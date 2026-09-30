use axum::body::Body;
use axum::extract::{Path, Request, State};
use axum::http::{header, request::Parts, StatusCode};
use axum::middleware::Next;
use axum::response::Response;
use axum::{middleware, Router};
use base64::Engine;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{CallToolResult, Content, Implementation, ServerCapabilities, ServerInfo};
use rmcp::service::RequestContext;
use rmcp::transport::streamable_http_server::{
    session::local::LocalSessionManager, StreamableHttpServerConfig, StreamableHttpService,
};
use rmcp::{tool, tool_handler, tool_router, ErrorData, RoleServer, ServerHandler};
use schemars::JsonSchema;
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
use serde::Deserialize;
use serde_json::json;

use crate::attachments::{self, CreateUploadLinkBody, MoveAttachmentBody, UploadFromUrlBody};
use crate::entities::{oauth_tokens, remote_cursors, users};
use crate::error::AppError;
use crate::notes::{
    self, CreateNoteBody, MoveNoteBody, PatchFrontmatterBody, PatchNoteBody, PeriodicAppendBody,
    PeriodicBody, ReplaceNoteBody,
};
use crate::search;
use crate::session::{bearer_token, hash_token, now_millis, ApiActor, ApiPrincipal};
use crate::state::AppState;
use crate::structured::{
    self, BaseViewBody, BaseViewPatchBody, CanvasEdgeBody, CanvasEdgePatchBody, CanvasNodeBody,
    CanvasNodePatchBody, CanvasOperation, CanvasOperationBatchBody, CreateStructuredBody,
    MoveStructuredBody,
};
use crate::{SERVER_NAME, SERVER_SLUG};

#[derive(Clone)]
pub(crate) struct ToolCtx {
    pub state: AppState,
    pub vault_id: String,
    pub principal: ApiPrincipal,
}

#[derive(Clone)]
pub struct InstaMcp;

pub fn router(state: AppState) -> Router<AppState> {
    let svc = StreamableHttpService::new(
        || Ok(InstaMcp),
        LocalSessionManager::default().into(),
        StreamableHttpServerConfig::default()
            .disable_allowed_hosts()
            .with_stateful_mode(false)
            .with_json_response(true),
    );
    Router::new()
        .route_service("/mcp/i/{app_id}", svc)
        .route_layer(middleware::from_fn_with_state(state, front_auth))
        .route_layer(middleware::from_fn(log_failures))
}

/// Transport-level rejections (405/406/400) are produced inside rmcp and
/// otherwise leave no trace, which makes client issues undiagnosable.
async fn log_failures(req: Request<Body>, next: Next) -> Response {
    fn header(req: &Request<Body>, name: &str) -> String {
        req.headers()
            .get(name)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_owned()
    }
    let method = req.method().clone();
    let uri = req.uri().clone();
    let accept = header(&req, "accept");
    let session_id = header(&req, "mcp-session-id");
    let protocol_version = header(&req, "mcp-protocol-version");
    let has_auth = req.headers().contains_key(header::AUTHORIZATION);
    let response = next.run(req).await;
    if response.status().is_client_error() || response.status().is_server_error() {
        tracing::warn!(
            %method,
            %uri,
            status = %response.status(),
            accept,
            session_id,
            protocol_version,
            has_auth,
            "mcp request rejected"
        );
    }
    response
}

async fn front_auth(
    State(state): State<AppState>,
    Path(app_id): Path<String>,
    mut req: Request<Body>,
    next: Next,
) -> Result<Response, Response> {
    match authenticate_mcp(
        &state,
        &app_id,
        req.headers()
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok()),
    )
    .await
    {
        Ok((vault_id, principal)) => {
            req.extensions_mut().insert(ToolCtx {
                state,
                vault_id,
                principal,
            });
            Ok(next.run(req).await)
        }
        Err(_) => Err(unauthorized(&state, &app_id)),
    }
}

fn unauthorized(state: &AppState, app_id: &str) -> Response {
    let metadata = format!(
        "{}/.well-known/oauth-protected-resource/mcp/i/{app_id}",
        state.config.public_base_url.trim_end_matches('/')
    );
    Response::builder()
        .status(StatusCode::UNAUTHORIZED)
        .header(
            header::WWW_AUTHENTICATE,
            format!("Bearer resource_metadata=\"{metadata}\""),
        )
        .body(Body::empty())
        .unwrap()
}

async fn authenticate_mcp(
    state: &AppState,
    app_id: &str,
    auth: Option<&str>,
) -> Result<(String, ApiPrincipal), AppError> {
    let token = auth.and_then(bearer_token).ok_or(AppError::Unauthorized)?;
    let token_hash = hash_token(token);
    let cursor = remote_cursors::Entity::find()
        .filter(remote_cursors::Column::AppId.eq(app_id))
        .one(&state.db)
        .await?
        .ok_or(AppError::Unauthorized)?;
    // The bearer secret may be the cursor's legacy token or one of its
    // plugin-issued tokens; either way it must resolve to this same cursor.
    if let Some(matched) = crate::session::cursor_by_token_hash(&state.db, &token_hash).await? {
        if matched.id == cursor.id {
            let vault_id = cursor.vault_id.clone();
            let principal = crate::session::cursor_principal(&state.db, cursor).await?;
            return Ok((vault_id, principal));
        }
        return Err(AppError::Unauthorized);
    }
    let oauth = oauth_tokens::Entity::find_by_id(token_hash)
        .one(&state.db)
        .await?
        .ok_or(AppError::Unauthorized)?;
    if oauth.access_expires_at < now_millis()
        || oauth.app_id != app_id
        || oauth.vault_id != cursor.vault_id
    {
        return Err(AppError::Unauthorized);
    }
    let user = users::Entity::find_by_id(cursor.created_by.clone())
        .one(&state.db)
        .await?
        .ok_or(AppError::Unauthorized)?;
    Ok((
        cursor.vault_id.clone(),
        ApiPrincipal {
            user,
            actor: ApiActor::Cursor(cursor),
        },
    ))
}

fn ctx(context: &RequestContext<RoleServer>) -> Result<ToolCtx, ErrorData> {
    context
        .extensions
        .get::<Parts>()
        .and_then(|parts| parts.extensions.get::<ToolCtx>())
        .cloned()
        .ok_or_else(|| ErrorData::internal_error("missing tool context", None))
}

fn ok<T: serde::Serialize>(value: T) -> Result<CallToolResult, ErrorData> {
    Ok(CallToolResult::structured(
        json!({ "ok": true, "data": value }),
    ))
}

fn tool_result(
    result: crate::error::AppResult<impl serde::Serialize>,
) -> Result<CallToolResult, ErrorData> {
    match result {
        Ok(value) => ok(value),
        Err(err) => Ok(CallToolResult::structured_error(
            json!({ "ok": false, "reason": err.to_string() }),
        )),
    }
}

fn tool_unit(result: crate::error::AppResult<()>) -> Result<CallToolResult, ErrorData> {
    match result {
        Ok(()) => ok(json!({ "deleted": true })),
        Err(err) => Ok(CallToolResult::structured_error(
            json!({ "ok": false, "reason": err.to_string() }),
        )),
    }
}

#[derive(Deserialize, JsonSchema)]
struct PathArgs {
    path: String,
}

#[derive(Deserialize, JsonSchema)]
struct CreateArgs {
    path: String,
    #[serde(default)]
    content: String,
}

#[derive(Deserialize, JsonSchema)]
struct ContentArgs {
    path: String,
    content: String,
    /// Lowercase hex SHA-256 of the note content this replacement is based on.
    /// When set and the note changed since, the replace is refused as stale.
    #[serde(default)]
    expected_content_hash: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
struct BodyArgs {
    path: String,
    body: String,
}

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct MoveArgs {
    path: String,
    to_path: String,
}

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct StructuredMoveArgs {
    path: String,
    to_path: String,
    #[serde(default)]
    update_embeds: bool,
}

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct PatchArgs {
    path: String,
    old: String,
    new: String,
    #[serde(default)]
    replace_all: bool,
}

#[derive(Deserialize, JsonSchema)]
struct FrontmatterArgs {
    path: String,
    #[serde(default)]
    set: serde_json::Map<String, serde_json::Value>,
    #[serde(default)]
    unset: Vec<String>,
}

#[derive(Deserialize, JsonSchema)]
struct PeriodicArgs {
    period: String,
    date: Option<String>,
    #[serde(default)]
    content: String,
}

#[derive(Deserialize, JsonSchema)]
struct PeriodicAppendArgs {
    period: String,
    date: Option<String>,
    text: String,
}

#[derive(Deserialize, JsonSchema)]
struct AttachmentUploadArgs {
    path: String,
    base64: String,
}

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct AttachmentMoveArgs {
    path: String,
    to_path: String,
    #[serde(default)]
    update_embeds: bool,
}

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct AttachmentFromUrlArgs {
    source_url: String,
    path: String,
}

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct CreateUploadLinkArgs {
    landing_dir: Option<String>,
    expires_in_seconds: Option<i64>,
}

#[derive(Deserialize, JsonSchema)]
struct SearchArgs {
    query: String,
    limit: Option<u32>,
}

#[derive(Deserialize, JsonSchema)]
struct BacklinksArgs {
    path: String,
}

/// Schema helpers for `serde_json::Value` fields.
///
/// schemars renders a bare `serde_json::Value` as the boolean schema `true`
/// ("accept any JSON"). That is legal JSON Schema, so lenient MCP clients accept
/// it, but Claude Code validates each tool's input schema with a stricter model
/// that requires every property to be a schema *object* and rejects a bare
/// boolean (`{"code":"custom","message":"Invalid input"}`), which aborts the
/// whole `tools/list`. Emitting an object schema instead keeps these tools
/// loadable in Claude Code while remaining permissive everywhere else.
fn any_object_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
    schemars::json_schema!({ "type": "object" })
}
fn any_json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
    schemars::json_schema!({})
}

#[derive(Deserialize, JsonSchema)]
struct JsonPathArgs {
    path: String,
    #[schemars(schema_with = "any_json_schema")]
    value: serde_json::Value,
}

#[derive(Deserialize, JsonSchema)]
struct CanvasNodeArgs {
    path: String,
    id: Option<String>,
    #[serde(flatten)]
    fields: serde_json::Map<String, serde_json::Value>,
}
#[derive(Deserialize, JsonSchema)]
struct CanvasNodePatchArgs {
    path: String,
    id: String,
    #[serde(flatten)]
    patch: serde_json::Map<String, serde_json::Value>,
}
#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct CanvasEdgeArgs {
    path: String,
    id: Option<String>,
    from_node: String,
    to_node: String,
    #[serde(flatten)]
    fields: serde_json::Map<String, serde_json::Value>,
}
#[derive(Deserialize, JsonSchema)]
struct CanvasEdgePatchArgs {
    path: String,
    id: String,
    #[serde(flatten)]
    patch: serde_json::Map<String, serde_json::Value>,
}
#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct CanvasOperationBatchArgs {
    path: String,
    operations: Vec<serde_json::Value>,
    mutation_id: Option<String>,
}
#[derive(Deserialize, JsonSchema)]
struct IdPathArgs {
    path: String,
    id: String,
}
#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct BaseViewArgs {
    path: String,
    name: String,
    #[serde(rename = "type")]
    view_type: String,
    #[serde(default)]
    #[schemars(schema_with = "any_object_schema")]
    filters: Option<serde_json::Value>,
    order: Option<Vec<serde_json::Value>>,
    #[serde(default)]
    #[schemars(schema_with = "any_object_schema")]
    sort: Option<serde_json::Value>,
    #[serde(default)]
    #[schemars(schema_with = "any_object_schema")]
    group_by: Option<serde_json::Value>,
    columns: Option<Vec<serde_json::Value>>,
}
#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct BaseViewPatchArgs {
    path: String,
    name: String,
    #[serde(default)]
    #[schemars(schema_with = "any_object_schema")]
    filters: Option<serde_json::Value>,
    order: Option<Vec<serde_json::Value>>,
    #[serde(default)]
    #[schemars(schema_with = "any_object_schema")]
    sort: Option<serde_json::Value>,
    #[serde(default)]
    #[schemars(schema_with = "any_object_schema")]
    group_by: Option<serde_json::Value>,
    columns: Option<Vec<serde_json::Value>>,
}
#[derive(Deserialize, JsonSchema)]
struct NamePathArgs {
    path: String,
    name: String,
}
#[derive(Deserialize, JsonSchema)]
struct SetValueArgs {
    path: String,
    name: Option<String>,
    #[schemars(schema_with = "any_json_schema")]
    value: serde_json::Value,
}

// Schema helper for `Vec<serde_json::Value>` params: schemars renders a bare
// `Value` as boolean `true`, which Claude Code rejects (see `any_object_schema`).
fn any_json_array_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
    schemars::json_schema!({ "type": "array" })
}

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct PluginDbQueryArgs {
    plugin: String,
    name: String,
    sql: String,
    #[serde(default)]
    #[schemars(schema_with = "any_json_array_schema")]
    params: Vec<serde_json::Value>,
    limit: Option<usize>,
}

/// One write statement in a `write_plugin_database` batch.
#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct PluginDbStatementArg {
    sql: String,
    #[serde(default)]
    #[schemars(schema_with = "any_json_array_schema")]
    params: Vec<serde_json::Value>,
}

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct PluginDbExecuteArgs {
    plugin: String,
    name: String,
    /// Statements run in order inside one transaction.
    statements: Vec<PluginDbStatementArg>,
}

fn base_view_fields(
    filters: Option<serde_json::Value>,
    order: Option<Vec<serde_json::Value>>,
    sort: Option<serde_json::Value>,
    group_by: Option<serde_json::Value>,
    columns: Option<Vec<serde_json::Value>>,
) -> serde_json::Map<String, serde_json::Value> {
    let mut fields = serde_json::Map::new();
    if let Some(value) = filters {
        fields.insert("filters".into(), value);
    }
    if let Some(value) = order {
        fields.insert("order".into(), serde_json::Value::Array(value));
    }
    if let Some(value) = sort {
        fields.insert("sort".into(), value);
    }
    if let Some(value) = group_by {
        fields.insert("groupBy".into(), value);
    }
    if let Some(value) = columns {
        fields.insert("columns".into(), serde_json::Value::Array(value));
    }
    fields
}

#[tool_router]
impl InstaMcp {
    #[tool(
        description = "List Obsidian Canvas files in the cursor vault",
        annotations(
            title = "Canvas: List canvases",
            read_only_hint = true,
            open_world_hint = false
        )
    )]
    async fn list_canvases(
        &self,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = ctx(&context)?;
        tool_result(
            structured::list_structured_inner(&c.state, &c.principal, &c.vault_id, Some("canvas"))
                .await,
        )
    }
    #[tool(
        description = "List synced plugin SQLite databases in the cursor vault (server-side replicas)",
        annotations(
            title = "Plugin DB: List databases",
            read_only_hint = true,
            open_world_hint = false
        )
    )]
    async fn list_plugin_databases(
        &self,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = ctx(&context)?;
        tool_result(crate::plugindb::routes::list_inner(&c.state, &c.principal, &c.vault_id).await)
    }

    #[tool(
        description = "Run a read-only SELECT against a synced plugin SQLite database; params bind ?1..?N; returns columns + rows. Cell values use the tagged wire encoding ({$blob}/{$int}) for non-JSON-native values.",
        annotations(
            title = "Plugin DB: Query (read-only)",
            read_only_hint = true,
            open_world_hint = false
        )
    )]
    async fn query_plugin_database(
        &self,
        context: RequestContext<RoleServer>,
        Parameters(args): Parameters<PluginDbQueryArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = ctx(&context)?;
        tool_result(
            crate::plugindb::routes::query_inner(
                &c.state,
                &c.principal,
                &c.vault_id,
                &args.plugin,
                &args.name,
                &args.sql,
                &args.params,
                args.limit,
            )
            .await,
        )
    }

    #[tool(
        description = "Run one or more INSERT/UPDATE/DELETE/REPLACE statements (one transaction) against a synced plugin SQLite database; params bind ?1..?N per statement. The changes replicate to all vault devices (CRDT last-writer-wins). Schema changes are not allowed.",
        annotations(
            title = "Plugin DB: Execute write",
            read_only_hint = false,
            destructive_hint = true,
            open_world_hint = false
        )
    )]
    async fn write_plugin_database(
        &self,
        context: RequestContext<RoleServer>,
        Parameters(args): Parameters<PluginDbExecuteArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = ctx(&context)?;
        let stmts: Vec<crate::plugindb::ExecuteStatement> = args
            .statements
            .into_iter()
            .map(|s| crate::plugindb::ExecuteStatement {
                sql: s.sql,
                params: s.params,
            })
            .collect();
        tool_result(
            crate::plugindb::routes::execute_inner(
                &c.state,
                &c.principal,
                &c.vault_id,
                &args.plugin,
                &args.name,
                &stmts,
            )
            .await,
        )
    }

    #[tool(
        description = "Read an Obsidian Canvas as file-form arrays",
        annotations(
            title = "Canvas: Read canvas",
            read_only_hint = true,
            open_world_hint = false
        )
    )]
    async fn read_canvas(
        &self,
        context: RequestContext<RoleServer>,
        Parameters(args): Parameters<PathArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = ctx(&context)?;
        tool_result(
            structured::read_canvas_inner(&c.state, &c.principal, &c.vault_id, &args.path).await,
        )
    }

    #[tool(
        description = "Create an Obsidian Canvas; writes are CRDT-merged and appear live in open Canvas views",
        annotations(
            title = "Canvas: Create canvas",
            read_only_hint = false,
            destructive_hint = false,
            open_world_hint = false
        )
    )]
    async fn create_canvas(
        &self,
        context: RequestContext<RoleServer>,
        Parameters(args): Parameters<JsonPathArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = ctx(&context)?;
        tool_result(
            structured::create_canvas_inner(
                &c.state,
                &c.principal,
                &c.vault_id,
                CreateStructuredBody {
                    path: args.path,
                    value: args.value,
                },
            )
            .await,
        )
    }

    #[tool(
        description = "Add a Canvas node; writes are CRDT-merged and appear live in open Canvas views",
        annotations(
            title = "Canvas: Add node",
            read_only_hint = false,
            destructive_hint = false,
            open_world_hint = false
        )
    )]
    async fn add_canvas_node(
        &self,
        context: RequestContext<RoleServer>,
        Parameters(args): Parameters<CanvasNodeArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = ctx(&context)?;
        tool_result(
            structured::add_canvas_node_inner(
                &c.state,
                &c.principal,
                &c.vault_id,
                &args.path,
                CanvasNodeBody {
                    id: args.id,
                    fields: args.fields,
                },
            )
            .await,
        )
    }

    #[tool(
        description = "Update a Canvas node; writes are CRDT-merged and appear live in open Canvas views",
        annotations(
            title = "Canvas: Update node",
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn update_canvas_node(
        &self,
        context: RequestContext<RoleServer>,
        Parameters(args): Parameters<CanvasNodePatchArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = ctx(&context)?;
        tool_result(
            structured::update_canvas_node_inner(
                &c.state,
                &c.principal,
                &c.vault_id,
                &args.path,
                CanvasNodePatchBody {
                    id: args.id,
                    patch: args.patch,
                },
            )
            .await,
        )
    }

    #[tool(
        description = "Delete a Canvas node and connected edges; writes are CRDT-merged and appear live in open Canvas views",
        annotations(
            title = "Canvas: Delete node",
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn delete_canvas_node(
        &self,
        context: RequestContext<RoleServer>,
        Parameters(args): Parameters<IdPathArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = ctx(&context)?;
        tool_result(
            structured::delete_canvas_node_inner(
                &c.state,
                &c.principal,
                &c.vault_id,
                &args.path,
                &args.id,
            )
            .await,
        )
    }

    #[tool(
        description = "Add a Canvas edge; writes are CRDT-merged and appear live in open Canvas views",
        annotations(
            title = "Canvas: Add edge",
            read_only_hint = false,
            destructive_hint = false,
            open_world_hint = false
        )
    )]
    async fn add_canvas_edge(
        &self,
        context: RequestContext<RoleServer>,
        Parameters(args): Parameters<CanvasEdgeArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = ctx(&context)?;
        tool_result(
            structured::add_canvas_edge_inner(
                &c.state,
                &c.principal,
                &c.vault_id,
                &args.path,
                CanvasEdgeBody {
                    id: args.id,
                    from_node: args.from_node,
                    to_node: args.to_node,
                    fields: args.fields,
                },
            )
            .await,
        )
    }

    #[tool(
        description = "Update a Canvas edge; writes are CRDT-merged and appear live in open Canvas views",
        annotations(
            title = "Canvas: Update edge",
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn update_canvas_edge(
        &self,
        context: RequestContext<RoleServer>,
        Parameters(args): Parameters<CanvasEdgePatchArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = ctx(&context)?;
        tool_result(
            structured::update_canvas_edge_inner(
                &c.state,
                &c.principal,
                &c.vault_id,
                &args.path,
                CanvasEdgePatchBody {
                    id: args.id,
                    patch: args.patch,
                },
            )
            .await,
        )
    }

    #[tool(
        description = "Delete a Canvas edge; writes are CRDT-merged and appear live in open Canvas views",
        annotations(
            title = "Canvas: Delete edge",
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn delete_canvas_edge(
        &self,
        context: RequestContext<RoleServer>,
        Parameters(args): Parameters<IdPathArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = ctx(&context)?;
        tool_result(
            structured::delete_canvas_edge_inner(
                &c.state,
                &c.principal,
                &c.vault_id,
                &args.path,
                &args.id,
            )
            .await,
        )
    }

    #[tool(
        description = "Apply an atomic batch of Canvas node, edge, and ordering operations",
        annotations(
            title = "Canvas: Apply operation batch",
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn apply_canvas_operations(
        &self,
        context: RequestContext<RoleServer>,
        Parameters(args): Parameters<CanvasOperationBatchArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = ctx(&context)?;
        let operations: Result<Vec<CanvasOperation>, _> = args
            .operations
            .into_iter()
            .map(serde_json::from_value)
            .collect();
        let operations = operations.map_err(|error| {
            ErrorData::invalid_params(format!("invalid Canvas operation: {error}"), None)
        })?;
        tool_result(
            structured::apply_canvas_operations_inner(
                &c.state,
                &c.principal,
                &c.vault_id,
                &args.path,
                CanvasOperationBatchBody {
                    operations,
                    mutation_id: args.mutation_id,
                },
            )
            .await,
        )
    }

    #[tool(
        description = "Move or rename a Canvas; updateEmbeds rewrites ![[...]], [[...]], and [](...) references in other notes; writes are CRDT-merged and appear live in open Canvas views",
        annotations(
            title = "Canvas: Move canvas",
            read_only_hint = false,
            destructive_hint = false,
            open_world_hint = false
        )
    )]
    async fn move_canvas(
        &self,
        context: RequestContext<RoleServer>,
        Parameters(args): Parameters<StructuredMoveArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = ctx(&context)?;
        tool_result(
            structured::move_structured_inner(
                &c.state,
                &c.principal,
                &c.vault_id,
                &args.path,
                MoveStructuredBody {
                    to_path: args.to_path,
                    update_embeds: args.update_embeds,
                },
                "canvas",
            )
            .await,
        )
    }

    #[tool(
        description = "Delete a Canvas",
        annotations(
            title = "Canvas: Delete canvas",
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn delete_canvas(
        &self,
        context: RequestContext<RoleServer>,
        Parameters(args): Parameters<PathArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = ctx(&context)?;
        tool_unit(
            structured::delete_canvas_inner(&c.state, &c.principal, &c.vault_id, &args.path).await,
        )
    }

    #[tool(
        description = "List Obsidian Bases in the cursor vault",
        annotations(
            title = "Base: List bases",
            read_only_hint = true,
            open_world_hint = false
        )
    )]
    async fn list_bases(
        &self,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = ctx(&context)?;
        tool_result(
            structured::list_structured_inner(&c.state, &c.principal, &c.vault_id, Some("base"))
                .await,
        )
    }

    #[tool(
        description = "Read an Obsidian Base",
        annotations(
            title = "Base: Read base",
            read_only_hint = true,
            open_world_hint = false
        )
    )]
    async fn read_base(
        &self,
        context: RequestContext<RoleServer>,
        Parameters(args): Parameters<PathArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = ctx(&context)?;
        tool_result(
            structured::read_base_inner(&c.state, &c.principal, &c.vault_id, &args.path).await,
        )
    }

    #[tool(
        description = "Create an Obsidian Base; writes are CRDT-merged and appear live in open Base views",
        annotations(
            title = "Base: Create base",
            read_only_hint = false,
            destructive_hint = false,
            open_world_hint = false
        )
    )]
    async fn create_base(
        &self,
        context: RequestContext<RoleServer>,
        Parameters(args): Parameters<JsonPathArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = ctx(&context)?;
        tool_result(
            structured::create_base_inner(
                &c.state,
                &c.principal,
                &c.vault_id,
                CreateStructuredBody {
                    path: args.path,
                    value: args.value,
                },
            )
            .await,
        )
    }

    #[tool(
        description = "Add or replace a Base view by name; writes are CRDT-merged and appear live in open Base views",
        annotations(
            title = "Base: Add view",
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn add_base_view(
        &self,
        context: RequestContext<RoleServer>,
        Parameters(args): Parameters<BaseViewArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = ctx(&context)?;
        tool_result(
            structured::add_base_view_inner(
                &c.state,
                &c.principal,
                &c.vault_id,
                &args.path,
                BaseViewBody {
                    name: args.name,
                    view_type: args.view_type,
                    fields: base_view_fields(
                        args.filters,
                        args.order,
                        args.sort,
                        args.group_by,
                        args.columns,
                    ),
                },
            )
            .await,
        )
    }

    #[tool(
        description = "List views in an Obsidian Base",
        annotations(
            title = "Base: List views",
            read_only_hint = true,
            open_world_hint = false
        )
    )]
    async fn list_base_views(
        &self,
        context: RequestContext<RoleServer>,
        Parameters(args): Parameters<PathArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = ctx(&context)?;
        tool_result(
            structured::list_base_views_inner(&c.state, &c.principal, &c.vault_id, &args.path)
                .await,
        )
    }

    #[tool(
        description = "Update a Base view by name; writes are CRDT-merged and appear live in open Base views",
        annotations(
            title = "Base: Update view",
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn update_base_view(
        &self,
        context: RequestContext<RoleServer>,
        Parameters(args): Parameters<BaseViewPatchArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = ctx(&context)?;
        tool_result(
            structured::update_base_view_inner(
                &c.state,
                &c.principal,
                &c.vault_id,
                &args.path,
                BaseViewPatchBody {
                    name: args.name,
                    patch: base_view_fields(
                        args.filters,
                        args.order,
                        args.sort,
                        args.group_by,
                        args.columns,
                    ),
                },
            )
            .await,
        )
    }

    #[tool(
        description = "Delete a Base view by name; writes are CRDT-merged and appear live in open Base views",
        annotations(
            title = "Base: Delete view",
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn delete_base_view(
        &self,
        context: RequestContext<RoleServer>,
        Parameters(args): Parameters<NamePathArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = ctx(&context)?;
        tool_result(
            structured::delete_base_view_inner(
                &c.state,
                &c.principal,
                &c.vault_id,
                &args.path,
                &args.name,
            )
            .await,
        )
    }

    #[tool(
        description = "Set global Base filters; writes are CRDT-merged and appear live in open Base views",
        annotations(
            title = "Base: Set filters",
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn set_base_filters(
        &self,
        context: RequestContext<RoleServer>,
        Parameters(args): Parameters<SetValueArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = ctx(&context)?;
        tool_result(
            structured::set_base_filters_inner(
                &c.state,
                &c.principal,
                &c.vault_id,
                &args.path,
                args.value,
            )
            .await,
        )
    }

    #[tool(
        description = "Set Base view filters by view name; writes are CRDT-merged and appear live in open Base views",
        annotations(
            title = "Base: Set view filters",
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn set_base_view_filters(
        &self,
        context: RequestContext<RoleServer>,
        Parameters(args): Parameters<SetValueArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = ctx(&context)?;
        let Some(name) = args.name else {
            return Ok(CallToolResult::structured_error(
                json!({ "ok": false, "reason": "name is required" }),
            ));
        };
        tool_result(
            structured::set_base_view_filters_inner(
                &c.state,
                &c.principal,
                &c.vault_id,
                &args.path,
                &name,
                args.value,
            )
            .await,
        )
    }

    #[tool(
        description = "Set a Base formula by name; writes are CRDT-merged and appear live in open Base views",
        annotations(
            title = "Base: Set formula",
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn set_base_formula(
        &self,
        context: RequestContext<RoleServer>,
        Parameters(args): Parameters<SetValueArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = ctx(&context)?;
        let Some(name) = args.name else {
            return Ok(CallToolResult::structured_error(
                json!({ "ok": false, "reason": "name is required" }),
            ));
        };
        tool_result(
            structured::set_base_formula_inner(
                &c.state,
                &c.principal,
                &c.vault_id,
                &args.path,
                &name,
                args.value,
            )
            .await,
        )
    }

    #[tool(
        description = "Delete a Base formula by name; writes are CRDT-merged and appear live in open Base views",
        annotations(
            title = "Base: Delete formula",
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn delete_base_formula(
        &self,
        context: RequestContext<RoleServer>,
        Parameters(args): Parameters<NamePathArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = ctx(&context)?;
        tool_result(
            structured::delete_base_formula_inner(
                &c.state,
                &c.principal,
                &c.vault_id,
                &args.path,
                &args.name,
            )
            .await,
        )
    }

    #[tool(
        description = "Set a Base property by name; writes are CRDT-merged and appear live in open Base views",
        annotations(
            title = "Base: Set property",
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn set_base_property(
        &self,
        context: RequestContext<RoleServer>,
        Parameters(args): Parameters<SetValueArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = ctx(&context)?;
        let Some(name) = args.name else {
            return Ok(CallToolResult::structured_error(
                json!({ "ok": false, "reason": "name is required" }),
            ));
        };
        tool_result(
            structured::set_base_property_inner(
                &c.state,
                &c.principal,
                &c.vault_id,
                &args.path,
                &name,
                args.value,
            )
            .await,
        )
    }

    #[tool(
        description = "Delete a Base property by name; writes are CRDT-merged and appear live in open Base views",
        annotations(
            title = "Base: Delete property",
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn delete_base_property(
        &self,
        context: RequestContext<RoleServer>,
        Parameters(args): Parameters<NamePathArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = ctx(&context)?;
        tool_result(
            structured::delete_base_property_inner(
                &c.state,
                &c.principal,
                &c.vault_id,
                &args.path,
                &args.name,
            )
            .await,
        )
    }

    #[tool(
        description = "Move or rename a Base; updateEmbeds rewrites ![[...]], [[...]], and [](...) references in other notes; writes are CRDT-merged and appear live in open Base views",
        annotations(
            title = "Base: Move base",
            read_only_hint = false,
            destructive_hint = false,
            open_world_hint = false
        )
    )]
    async fn move_base(
        &self,
        context: RequestContext<RoleServer>,
        Parameters(args): Parameters<StructuredMoveArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = ctx(&context)?;
        tool_result(
            structured::move_structured_inner(
                &c.state,
                &c.principal,
                &c.vault_id,
                &args.path,
                MoveStructuredBody {
                    to_path: args.to_path,
                    update_embeds: args.update_embeds,
                },
                "base",
            )
            .await,
        )
    }

    #[tool(
        description = "Delete a Base",
        annotations(
            title = "Base: Delete base",
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn delete_base(
        &self,
        context: RequestContext<RoleServer>,
        Parameters(args): Parameters<PathArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = ctx(&context)?;
        tool_unit(
            structured::delete_base_inner(&c.state, &c.principal, &c.vault_id, &args.path).await,
        )
    }

    #[tool(
        description = "List notes in the cursor vault",
        annotations(
            title = "Note: List notes",
            read_only_hint = true,
            open_world_hint = false
        )
    )]
    async fn list_notes(
        &self,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = ctx(&context)?;
        tool_result(notes::list_notes_inner(&c.state, &c.principal, &c.vault_id).await)
    }

    #[tool(
        description = "Read a note by path",
        annotations(
            title = "Note: Read note",
            read_only_hint = true,
            open_world_hint = false
        )
    )]
    async fn read_note(
        &self,
        context: RequestContext<RoleServer>,
        Parameters(args): Parameters<PathArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = ctx(&context)?;
        tool_result(notes::read_note_inner(&c.state, &c.principal, &c.vault_id, &args.path).await)
    }

    #[tool(
        description = "Create a note",
        annotations(
            title = "Note: Create note",
            read_only_hint = false,
            destructive_hint = false,
            open_world_hint = false
        )
    )]
    async fn create_note(
        &self,
        context: RequestContext<RoleServer>,
        Parameters(args): Parameters<CreateArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = ctx(&context)?;
        tool_result(
            notes::create_note_inner(
                &c.state,
                &c.principal,
                &c.vault_id,
                CreateNoteBody {
                    path: args.path,
                    content: args.content,
                },
            )
            .await,
        )
    }

    #[tool(
        description = "Replace a full note including frontmatter",
        annotations(
            title = "Note: Replace note",
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn replace_note(
        &self,
        context: RequestContext<RoleServer>,
        Parameters(args): Parameters<ContentArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = ctx(&context)?;
        tool_result(
            notes::replace_note_inner(
                &c.state,
                &c.principal,
                &c.vault_id,
                &args.path,
                ReplaceNoteBody {
                    content: args.content,
                    expected_content_hash: args.expected_content_hash,
                },
            )
            .await,
        )
    }

    #[tool(
        description = "Replace only the body of a note, preserving existing frontmatter",
        annotations(
            title = "Note: Replace body",
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn replace_body(
        &self,
        context: RequestContext<RoleServer>,
        Parameters(args): Parameters<BodyArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = ctx(&context)?;
        tool_result(
            notes::replace_body_inner(&c.state, &c.principal, &c.vault_id, &args.path, args.body)
                .await,
        )
    }

    #[tool(
        description = "Patch note text",
        annotations(
            title = "Note: Patch note",
            read_only_hint = false,
            destructive_hint = true,
            open_world_hint = false
        )
    )]
    async fn patch_note(
        &self,
        context: RequestContext<RoleServer>,
        Parameters(args): Parameters<PatchArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = ctx(&context)?;
        tool_result(
            notes::patch_note_inner(
                &c.state,
                &c.principal,
                &c.vault_id,
                &args.path,
                PatchNoteBody {
                    old: args.old,
                    new: args.new,
                    replace_all: args.replace_all,
                },
            )
            .await,
        )
    }

    #[tool(
        description = "Move or rename a note",
        annotations(
            title = "Note: Move note",
            read_only_hint = false,
            destructive_hint = false,
            open_world_hint = false
        )
    )]
    async fn move_note(
        &self,
        context: RequestContext<RoleServer>,
        Parameters(args): Parameters<MoveArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = ctx(&context)?;
        tool_result(
            notes::move_note_inner(
                &c.state,
                &c.principal,
                &c.vault_id,
                &args.path,
                MoveNoteBody {
                    to_path: args.to_path,
                },
            )
            .await,
        )
    }

    #[tool(
        description = "Delete a note",
        annotations(
            title = "Note: Delete note",
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn delete_note(
        &self,
        context: RequestContext<RoleServer>,
        Parameters(args): Parameters<PathArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = ctx(&context)?;
        tool_unit(notes::delete_note_inner(&c.state, &c.principal, &c.vault_id, &args.path).await)
    }

    #[tool(
        description = "Parse note frontmatter",
        annotations(
            title = "Note: Parse frontmatter",
            read_only_hint = true,
            open_world_hint = false
        )
    )]
    async fn parse_frontmatter(
        &self,
        context: RequestContext<RoleServer>,
        Parameters(args): Parameters<PathArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = ctx(&context)?;
        tool_result(
            notes::parse_frontmatter_inner(&c.state, &c.principal, &c.vault_id, &args.path).await,
        )
    }

    #[tool(
        description = "Patch note frontmatter keys",
        annotations(
            title = "Note: Patch frontmatter",
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn patch_frontmatter(
        &self,
        context: RequestContext<RoleServer>,
        Parameters(args): Parameters<FrontmatterArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = ctx(&context)?;
        tool_result(
            notes::patch_frontmatter_inner(
                &c.state,
                &c.principal,
                &c.vault_id,
                &args.path,
                PatchFrontmatterBody {
                    set: args.set,
                    unset: args.unset,
                },
            )
            .await,
        )
    }

    #[tool(
        description = "Generate a stable permalink for a note. Returns a URL of the form <public-base>/n/<guid>. Opening it in a browser issues an HTTP redirect to an obsidian://realtime-open deeplink, which opens the note in the user's Obsidian vault via the Realtime plugin. The permalink tracks the note by stable guid, so it keeps working after the note is renamed or moved. Share this URL as-is; do not rewrite it into an obsidian:// link yourself.",
        annotations(
            title = "Note: Generate permalink",
            read_only_hint = true,
            open_world_hint = false
        )
    )]
    async fn generate_permalink(
        &self,
        context: RequestContext<RoleServer>,
        Parameters(args): Parameters<PathArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = ctx(&context)?;
        tool_result(
            notes::note_permalink_inner(&c.state, &c.principal, &c.vault_id, &args.path).await,
        )
    }

    #[tool(
        description = "Get or create a periodic note",
        annotations(
            title = "Note: Get or create periodic note",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn periodic_note_get_or_create(
        &self,
        context: RequestContext<RoleServer>,
        Parameters(args): Parameters<PeriodicArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = ctx(&context)?;
        tool_result(
            notes::periodic_note_get_or_create_inner(
                &c.state,
                &c.principal,
                &c.vault_id,
                &args.period,
                PeriodicBody {
                    date: args.date,
                    content: args.content,
                },
            )
            .await,
        )
    }

    #[tool(
        description = "Append text to a periodic note",
        annotations(
            title = "Note: Append to periodic note",
            read_only_hint = false,
            destructive_hint = false,
            open_world_hint = false
        )
    )]
    async fn periodic_note_append(
        &self,
        context: RequestContext<RoleServer>,
        Parameters(args): Parameters<PeriodicAppendArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = ctx(&context)?;
        tool_result(
            notes::periodic_note_append_inner(
                &c.state,
                &c.principal,
                &c.vault_id,
                &args.period,
                PeriodicAppendBody {
                    date: args.date,
                    text: args.text,
                },
            )
            .await,
        )
    }

    #[tool(
        description = "List attachments in the cursor vault",
        annotations(
            title = "Attachment: List attachments",
            read_only_hint = true,
            open_world_hint = false
        )
    )]
    async fn list_attachments(
        &self,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = ctx(&context)?;
        tool_result(attachments::list_attachments_inner(&c.state, &c.principal, &c.vault_id).await)
    }

    #[tool(
        description = "Check whether an attachment exists",
        annotations(
            title = "Attachment: Check attachment",
            read_only_hint = true,
            open_world_hint = false
        )
    )]
    async fn head_attachment(
        &self,
        context: RequestContext<RoleServer>,
        Parameters(args): Parameters<PathArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = ctx(&context)?;
        tool_result(
            attachments::head_attachment_inner(&c.state, &c.principal, &c.vault_id, &args.path)
                .await,
        )
    }

    #[tool(
        description = "Read an attachment; images are returned as image content, other files as base64",
        annotations(
            title = "Attachment: Read attachment",
            read_only_hint = true,
            open_world_hint = false
        )
    )]
    async fn read_attachment(
        &self,
        context: RequestContext<RoleServer>,
        Parameters(args): Parameters<PathArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = ctx(&context)?;
        match attachments::read_attachment_inner(&c.state, &c.principal, &c.vault_id, &args.path)
            .await
        {
            Ok(data) if data.content_type.starts_with("image/") => {
                Ok(CallToolResult::success(vec![Content::image(
                    base64::engine::general_purpose::STANDARD.encode(&data.bytes),
                    data.content_type,
                )]))
            }
            Ok(data) => ok(json!({
                "path": data.meta.path,
                "hash": data.meta.hash,
                "size": data.meta.size,
                "contentType": data.content_type,
                "base64": base64::engine::general_purpose::STANDARD.encode(&data.bytes),
            })),
            Err(err) => Ok(CallToolResult::structured_error(
                json!({ "ok": false, "reason": err.to_string() }),
            )),
        }
    }

    #[tool(
        description = "Upload an attachment from base64 content",
        annotations(
            title = "Attachment: Upload attachment",
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn upload_attachment(
        &self,
        context: RequestContext<RoleServer>,
        Parameters(args): Parameters<AttachmentUploadArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = ctx(&context)?;
        let bytes = match base64::engine::general_purpose::STANDARD.decode(args.base64.as_bytes()) {
            Ok(bytes) => bytes,
            Err(_) => {
                return Ok(CallToolResult::structured_error(
                    json!({ "ok": false, "reason": "invalid_base64" }),
                ));
            }
        };
        tool_result(
            attachments::upload_attachment_bytes_inner(
                &c.state,
                &c.principal,
                &c.vault_id,
                &args.path,
                &bytes,
                crate::ydoc::AttachmentPrecondition::None,
            )
            .await,
        )
    }

    #[tool(
        description = "Delete an attachment",
        annotations(
            title = "Attachment: Delete attachment",
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn delete_attachment(
        &self,
        context: RequestContext<RoleServer>,
        Parameters(args): Parameters<PathArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = ctx(&context)?;
        tool_unit(
            attachments::delete_attachment_inner(&c.state, &c.principal, &c.vault_id, &args.path)
                .await,
        )
    }

    #[tool(
        description = "Move or rename an attachment; updateEmbeds rewrites ![[...]], [[...]], and [](...) references in other notes",
        annotations(
            title = "Attachment: Move attachment",
            read_only_hint = false,
            destructive_hint = false,
            open_world_hint = false
        )
    )]
    async fn move_attachment(
        &self,
        context: RequestContext<RoleServer>,
        Parameters(args): Parameters<AttachmentMoveArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = ctx(&context)?;
        tool_result(
            attachments::move_attachment_inner(
                &c.state,
                &c.principal,
                &c.vault_id,
                &args.path,
                MoveAttachmentBody {
                    to_path: args.to_path,
                    update_embeds: args.update_embeds,
                },
            )
            .await,
        )
    }

    #[tool(
        description = "Fetch an attachment from an allowlisted URL",
        annotations(
            title = "Attachment: Upload from URL",
            read_only_hint = false,
            destructive_hint = true,
            open_world_hint = true
        )
    )]
    async fn upload_attachment_url(
        &self,
        context: RequestContext<RoleServer>,
        Parameters(args): Parameters<AttachmentFromUrlArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = ctx(&context)?;
        tool_result(
            attachments::upload_attachment_url_inner(
                &c.state,
                &c.principal,
                &c.vault_id,
                UploadFromUrlBody {
                    source_url: args.source_url,
                    path: args.path,
                },
            )
            .await,
        )
    }

    #[tool(
        description = "Create a signed single-use browser upload link",
        annotations(
            title = "Attachment: Create upload link",
            read_only_hint = false,
            destructive_hint = false,
            open_world_hint = false
        )
    )]
    async fn create_upload_link(
        &self,
        context: RequestContext<RoleServer>,
        Parameters(args): Parameters<CreateUploadLinkArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = ctx(&context)?;
        tool_result(
            attachments::create_upload_link_inner(
                &c.state,
                &c.principal,
                &c.vault_id,
                CreateUploadLinkBody {
                    landing_dir: args.landing_dir,
                    expires_in_seconds: args.expires_in_seconds,
                },
            )
            .await,
        )
    }

    #[tool(
        description = "Search notes",
        annotations(
            title = "Search: Search notes",
            read_only_hint = true,
            open_world_hint = false
        )
    )]
    async fn search_notes(
        &self,
        context: RequestContext<RoleServer>,
        Parameters(args): Parameters<SearchArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = ctx(&context)?;
        tool_result(
            search::search_notes_inner(
                &c.state,
                &c.principal,
                &c.vault_id,
                &args.query,
                args.limit,
            )
            .await,
        )
    }

    #[tool(
        description = "List tags",
        annotations(
            title = "Search: List tags",
            read_only_hint = true,
            open_world_hint = false
        )
    )]
    async fn list_tags(
        &self,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = ctx(&context)?;
        tool_result(search::list_tags_inner(&c.state, &c.principal, &c.vault_id).await)
    }

    #[tool(
        description = "List backlinks",
        annotations(
            title = "Search: List backlinks",
            read_only_hint = true,
            open_world_hint = false
        )
    )]
    async fn list_backlinks(
        &self,
        context: RequestContext<RoleServer>,
        Parameters(args): Parameters<BacklinksArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = ctx(&context)?;
        tool_result(
            search::list_backlinks_inner(&c.state, &c.principal, &c.vault_id, &args.path).await,
        )
    }

    #[tool(
        description = "Backfill note ids",
        annotations(
            title = "Search: Backfill note ids",
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn backfill_ids(
        &self,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = ctx(&context)?;
        tool_result(search::reindex_inner(&c.state, &c.principal, &c.vault_id).await)
    }
}

#[tool_handler]
impl ServerHandler for InstaMcp {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new(SERVER_SLUG, env!("CARGO_PKG_VERSION")))
            .with_instructions(format!("{SERVER_NAME} vault tools"))
    }
}
