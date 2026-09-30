use axum::extract::{Path, State};
use axum::Json;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map as JsonMap, Value as JsonValue};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, OnceLock, Weak};
use tokio::sync::Mutex;
use utoipa::ToSchema;

use crate::audit::{self, AuditEntry};
use crate::crdt::Level;
use crate::error::{AppError, AppResult};
use crate::routes::{authorize_path, require_member};
use crate::session::{now_millis, ApiPrincipal};
use crate::state::AppState;
use crate::ydoc::{self, StructuredIndexEntry};

static CANVAS_MUTATION_LOCKS: OnceLock<Mutex<HashMap<String, Weak<Mutex<()>>>>> = OnceLock::new();

async fn canvas_mutation_lock(document_id: &str) -> Arc<Mutex<()>> {
    let locks = CANVAS_MUTATION_LOCKS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut locks = locks.lock().await;
    locks.retain(|_, lock| lock.strong_count() > 0);
    if let Some(lock) = locks.get(document_id).and_then(Weak::upgrade) {
        return lock;
    }
    let lock = Arc::new(Mutex::new(()));
    locks.insert(document_id.to_string(), Arc::downgrade(&lock));
    lock
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StructuredSummary {
    pub path: String,
    pub guid: String,
    pub kind: String,
    pub permalink: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StructuredResponse {
    pub path: String,
    pub guid: String,
    pub kind: String,
    pub value: JsonValue,
    pub permalink: String,
    /// Opaque version of the stored value. Send it back as `If-Match` on a
    /// replace to refuse the write if the file changed since it was read.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value_hash: Option<String>,
}

/// Hash of a stored structured value, independent of object key order.
pub(crate) fn structured_value_hash(value: &JsonValue) -> String {
    use sha2::{Digest, Sha256};
    fn canonical(value: &JsonValue, out: &mut String) {
        match value {
            JsonValue::Object(map) => {
                let mut keys: Vec<&String> = map.keys().collect();
                keys.sort();
                out.push('{');
                for (index, key) in keys.into_iter().enumerate() {
                    if index > 0 {
                        out.push(',');
                    }
                    out.push_str(&JsonValue::String(key.clone()).to_string());
                    out.push(':');
                    canonical(&map[key], out);
                }
                out.push('}');
            }
            JsonValue::Array(items) => {
                out.push('[');
                for (index, item) in items.iter().enumerate() {
                    if index > 0 {
                        out.push(',');
                    }
                    canonical(item, out);
                }
                out.push(']');
            }
            other => out.push_str(&other.to_string()),
        }
    }
    let mut encoded = String::new();
    canonical(value, &mut encoded);
    format!("{:x}", Sha256::digest(encoded.as_bytes()))
}

/// The value hash an `If-Match` header names, if any (quotes are optional).
pub(crate) fn if_match_hash(headers: &axum::http::HeaderMap) -> Option<String> {
    let raw = headers
        .get(axum::http::header::IF_MATCH)?
        .to_str()
        .ok()?
        .trim();
    let raw = raw.strip_prefix("W/").unwrap_or(raw);
    Some(raw.trim_matches('"').to_string()).filter(|hash| !hash.is_empty())
}

#[derive(Deserialize)]
pub struct CreateStructuredBody {
    pub path: String,
    #[serde(default)]
    pub value: JsonValue,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MoveStructuredBody {
    pub to_path: String,
    #[serde(default)]
    pub update_embeds: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CanvasNodeBody {
    pub id: Option<String>,
    #[serde(flatten)]
    pub fields: JsonMap<String, JsonValue>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CanvasNodePatchBody {
    pub id: String,
    #[serde(flatten)]
    pub patch: JsonMap<String, JsonValue>,
}

#[derive(Deserialize)]
pub struct IdBody {
    pub id: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CanvasEdgeBody {
    pub id: Option<String>,
    pub from_node: String,
    pub to_node: String,
    #[serde(flatten)]
    pub fields: JsonMap<String, JsonValue>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CanvasEdgePatchBody {
    pub id: String,
    #[serde(flatten)]
    pub patch: JsonMap<String, JsonValue>,
}

#[derive(Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CanvasOperationBatchBody {
    pub operations: Vec<CanvasOperation>,
    pub mutation_id: Option<String>,
}

#[derive(Deserialize, ToSchema)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum CanvasOperation {
    NodeCreate {
        #[schema(value_type = Object)]
        node: JsonMap<String, JsonValue>,
    },
    NodePatch {
        id: String,
        patch: CanvasFieldPatch,
    },
    NodeDelete {
        id: String,
    },
    NodeRestore {
        #[schema(value_type = Object)]
        node: JsonMap<String, JsonValue>,
    },
    EdgeCreate {
        #[schema(value_type = Object)]
        edge: JsonMap<String, JsonValue>,
    },
    EdgePatch {
        id: String,
        patch: CanvasFieldPatch,
    },
    EdgeDelete {
        id: String,
    },
    EdgeRestore {
        #[schema(value_type = Object)]
        edge: JsonMap<String, JsonValue>,
    },
    NodeOrder {
        order: Vec<String>,
    },
    EdgeOrder {
        order: Vec<String>,
    },
}

#[derive(Deserialize, ToSchema)]
pub struct CanvasFieldPatch {
    #[serde(default)]
    #[schema(value_type = Object)]
    pub set: JsonMap<String, JsonValue>,
    #[serde(default)]
    pub remove: Vec<String>,
}

#[derive(Deserialize)]
pub struct BaseViewBody {
    pub name: String,
    #[serde(rename = "type")]
    pub view_type: String,
    #[serde(flatten)]
    pub fields: JsonMap<String, JsonValue>,
}

#[derive(Deserialize)]
pub struct BaseViewPatchBody {
    pub name: String,
    #[serde(flatten)]
    pub patch: JsonMap<String, JsonValue>,
}

#[derive(Deserialize)]
pub struct NameBody {
    pub name: String,
}

#[derive(Deserialize)]
pub struct SetValueBody {
    pub name: Option<String>,
    pub value: JsonValue,
}

pub fn canvas_to_file_json(value: JsonValue) -> JsonValue {
    let Some(root) = value.as_object() else {
        return json!({ "nodes": [], "edges": [] });
    };
    let nodes = root.get("nodes").and_then(JsonValue::as_object);
    let edges = root.get("edges").and_then(JsonValue::as_object);
    let node_order = root.get("nodeOrder").and_then(JsonValue::as_array);
    let edge_order = root.get("edgeOrder").and_then(JsonValue::as_array);
    json!({
        "nodes": ordered_canvas_items(nodes, node_order),
        "edges": ordered_canvas_items(edges, edge_order),
    })
}

pub fn ordered_canvas_items(
    items: Option<&JsonMap<String, JsonValue>>,
    order: Option<&Vec<JsonValue>>,
) -> Vec<JsonValue> {
    let Some(items) = items else {
        return Vec::new();
    };
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    if let Some(order) = order {
        for id in order.iter().filter_map(JsonValue::as_str) {
            if seen.insert(id.to_string()) {
                let Some(item) = items.get(id) else {
                    continue;
                };
                out.push(item.clone());
            }
        }
    }
    for (id, item) in items {
        if !seen.contains(id) {
            out.push(item.clone());
        }
    }
    out
}

pub async fn list_canvases(
    State(state): State<AppState>,
    principal: ApiPrincipal,
    Path(vault_id): Path<String>,
) -> AppResult<Json<Vec<StructuredSummary>>> {
    Ok(Json(
        list_structured_inner(&state, &principal, &vault_id, Some("canvas")).await?,
    ))
}

pub async fn list_bases(
    State(state): State<AppState>,
    principal: ApiPrincipal,
    Path(vault_id): Path<String>,
) -> AppResult<Json<Vec<StructuredSummary>>> {
    Ok(Json(
        list_structured_inner(&state, &principal, &vault_id, Some("base")).await?,
    ))
}

pub(crate) async fn list_structured_inner(
    state: &AppState,
    principal: &ApiPrincipal,
    vault_id: &str,
    kind_filter: Option<&str>,
) -> AppResult<Vec<StructuredSummary>> {
    principal.require_vault(vault_id)?;
    require_member(state, &principal.user.id, vault_id).await?;
    let update = ydoc::read_update(state, vault_id).await?;
    let mut entries =
        ydoc::decode_structured_index(&update).map_err(|e| AppError::Internal(e.to_string()))?;
    entries.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(entries
        .into_iter()
        .filter(|e| kind_filter.is_none_or(|k| e.kind == k))
        .map(|e| StructuredSummary {
            permalink: permalink_for_guid(state, &e.guid),
            path: e.path,
            guid: e.guid,
            kind: e.kind,
        })
        .collect())
}

pub async fn read_canvas(
    State(state): State<AppState>,
    principal: ApiPrincipal,
    Path((vault_id, path)): Path<(String, String)>,
) -> AppResult<Json<StructuredResponse>> {
    Ok(Json(
        read_canvas_inner(&state, &principal, &vault_id, &path).await?,
    ))
}

pub(crate) async fn read_canvas_inner(
    state: &AppState,
    principal: &ApiPrincipal,
    vault_id: &str,
    path: &str,
) -> AppResult<StructuredResponse> {
    let mut response = read_structured_json(state, principal, vault_id, path, "canvas").await?;
    response.value = canvas_to_file_json(response.value);
    Ok(response)
}

pub async fn create_canvas(
    State(state): State<AppState>,
    principal: ApiPrincipal,
    Path(vault_id): Path<String>,
    Json(body): Json<CreateStructuredBody>,
) -> AppResult<Json<StructuredResponse>> {
    Ok(Json(
        create_canvas_inner(&state, &principal, &vault_id, body).await?,
    ))
}

pub(crate) async fn create_canvas_inner(
    state: &AppState,
    principal: &ApiPrincipal,
    vault_id: &str,
    body: CreateStructuredBody,
) -> AppResult<StructuredResponse> {
    create_structured(
        state,
        principal,
        vault_id,
        &body.path,
        "canvas",
        canvas_file_to_map(body.value),
    )
    .await
}

pub async fn replace_canvas(
    State(state): State<AppState>,
    principal: ApiPrincipal,
    Path((vault_id, path)): Path<(String, String)>,
    headers: axum::http::HeaderMap,
    Json(body): Json<JsonValue>,
) -> AppResult<Json<StructuredResponse>> {
    let value = canvas_file_to_map(body);
    let expected = if_match_hash(&headers);
    Ok(Json(
        write_structured_json(
            &state,
            &principal,
            &vault_id,
            &path,
            "canvas",
            value,
            expected.as_deref(),
        )
        .await?,
    ))
}

pub async fn delete_canvas(
    State(state): State<AppState>,
    principal: ApiPrincipal,
    Path((vault_id, path)): Path<(String, String)>,
) -> AppResult<Json<JsonValue>> {
    delete_structured_inner(&state, &principal, &vault_id, &path, "canvas").await?;
    Ok(Json(json!({ "deleted": true })))
}

pub(crate) async fn delete_canvas_inner(
    state: &AppState,
    principal: &ApiPrincipal,
    vault_id: &str,
    path: &str,
) -> AppResult<()> {
    delete_structured_inner(state, principal, vault_id, path, "canvas").await
}

pub async fn move_canvas(
    State(state): State<AppState>,
    principal: ApiPrincipal,
    Path((vault_id, path)): Path<(String, String)>,
    Json(body): Json<MoveStructuredBody>,
) -> AppResult<Json<StructuredResponse>> {
    Ok(Json(
        move_structured_inner(&state, &principal, &vault_id, &path, body, "canvas").await?,
    ))
}

pub async fn add_canvas_node(
    State(state): State<AppState>,
    principal: ApiPrincipal,
    Path((vault_id, path)): Path<(String, String)>,
    Json(body): Json<CanvasNodeBody>,
) -> AppResult<Json<StructuredResponse>> {
    Ok(Json(
        update_canvas_doc(&state, &principal, &vault_id, &path, |root| {
            add_node(root, body)
        })
        .await?,
    ))
}

pub(crate) async fn add_canvas_node_inner(
    state: &AppState,
    principal: &ApiPrincipal,
    vault_id: &str,
    path: &str,
    body: CanvasNodeBody,
) -> AppResult<StructuredResponse> {
    update_canvas_doc(state, principal, vault_id, path, |root| {
        add_node(root, body)
    })
    .await
}

pub(crate) async fn update_canvas_node_inner(
    state: &AppState,
    principal: &ApiPrincipal,
    vault_id: &str,
    path: &str,
    body: CanvasNodePatchBody,
) -> AppResult<StructuredResponse> {
    update_canvas_doc(state, principal, vault_id, path, |root| {
        patch_item(root, "nodes", &body.id, body.patch)
    })
    .await
}

pub(crate) async fn delete_canvas_node_inner(
    state: &AppState,
    principal: &ApiPrincipal,
    vault_id: &str,
    path: &str,
    id: &str,
) -> AppResult<StructuredResponse> {
    update_canvas_doc(state, principal, vault_id, path, |root| {
        delete_node(root, id)
    })
    .await
}

pub async fn update_canvas_node(
    State(state): State<AppState>,
    principal: ApiPrincipal,
    Path((vault_id, path)): Path<(String, String)>,
    Json(body): Json<CanvasNodePatchBody>,
) -> AppResult<Json<StructuredResponse>> {
    Ok(Json(
        update_canvas_doc(&state, &principal, &vault_id, &path, |root| {
            patch_item(root, "nodes", &body.id, body.patch)
        })
        .await?,
    ))
}

pub async fn delete_canvas_node(
    State(state): State<AppState>,
    principal: ApiPrincipal,
    Path((vault_id, path)): Path<(String, String)>,
    Json(body): Json<IdBody>,
) -> AppResult<Json<StructuredResponse>> {
    Ok(Json(
        update_canvas_doc(&state, &principal, &vault_id, &path, |root| {
            delete_node(root, &body.id)
        })
        .await?,
    ))
}

pub async fn add_canvas_edge(
    State(state): State<AppState>,
    principal: ApiPrincipal,
    Path((vault_id, path)): Path<(String, String)>,
    Json(body): Json<CanvasEdgeBody>,
) -> AppResult<Json<StructuredResponse>> {
    Ok(Json(
        update_canvas_doc(&state, &principal, &vault_id, &path, |root| {
            add_edge(root, body)
        })
        .await?,
    ))
}

pub(crate) async fn add_canvas_edge_inner(
    state: &AppState,
    principal: &ApiPrincipal,
    vault_id: &str,
    path: &str,
    body: CanvasEdgeBody,
) -> AppResult<StructuredResponse> {
    update_canvas_doc(state, principal, vault_id, path, |root| {
        add_edge(root, body)
    })
    .await
}

pub(crate) async fn update_canvas_edge_inner(
    state: &AppState,
    principal: &ApiPrincipal,
    vault_id: &str,
    path: &str,
    body: CanvasEdgePatchBody,
) -> AppResult<StructuredResponse> {
    update_canvas_doc(state, principal, vault_id, path, |root| {
        patch_item(root, "edges", &body.id, body.patch)
    })
    .await
}

pub(crate) async fn delete_canvas_edge_inner(
    state: &AppState,
    principal: &ApiPrincipal,
    vault_id: &str,
    path: &str,
    id: &str,
) -> AppResult<StructuredResponse> {
    update_canvas_doc(state, principal, vault_id, path, |root| {
        delete_item(root, "edges", "edgeOrder", id)
    })
    .await
}

pub async fn update_canvas_edge(
    State(state): State<AppState>,
    principal: ApiPrincipal,
    Path((vault_id, path)): Path<(String, String)>,
    Json(body): Json<CanvasEdgePatchBody>,
) -> AppResult<Json<StructuredResponse>> {
    Ok(Json(
        update_canvas_doc(&state, &principal, &vault_id, &path, |root| {
            patch_item(root, "edges", &body.id, body.patch)
        })
        .await?,
    ))
}

pub async fn delete_canvas_edge(
    State(state): State<AppState>,
    principal: ApiPrincipal,
    Path((vault_id, path)): Path<(String, String)>,
    Json(body): Json<IdBody>,
) -> AppResult<Json<StructuredResponse>> {
    Ok(Json(
        update_canvas_doc(&state, &principal, &vault_id, &path, |root| {
            delete_item(root, "edges", "edgeOrder", &body.id)
        })
        .await?,
    ))
}

pub async fn apply_canvas_operations(
    State(state): State<AppState>,
    principal: ApiPrincipal,
    Path((vault_id, path)): Path<(String, String)>,
    Json(body): Json<CanvasOperationBatchBody>,
) -> AppResult<Json<StructuredResponse>> {
    match apply_canvas_operations_inner(&state, &principal, &vault_id, &path, body).await {
        Ok(response) => Ok(Json(response)),
        Err(error) => {
            tracing::warn!(vault_id, path, error = %error, "Canvas operation batch failed");
            Err(error)
        }
    }
}

pub(crate) async fn apply_canvas_operations_inner(
    state: &AppState,
    principal: &ApiPrincipal,
    vault_id: &str,
    path: &str,
    body: CanvasOperationBatchBody,
) -> AppResult<StructuredResponse> {
    if body.operations.is_empty() {
        return Err(AppError::BadRequest("operations must not be empty".into()));
    }
    if body
        .mutation_id
        .as_ref()
        .is_some_and(|id| id.is_empty() || id.len() > 128)
    {
        return Err(AppError::BadRequest("invalid mutationId".into()));
    }
    let entry = require_structured_access(state, principal, vault_id, path, "canvas", true).await?;
    let document_id = doc_id(vault_id, &entry.guid);
    let mutation_lock = canvas_mutation_lock(&document_id).await;
    let _mutation_guard = mutation_lock.lock().await;
    let (epoch, current) = ydoc::read_update_for_write(state, &document_id).await?;
    let mut value =
        ydoc::decode_structured(&current).map_err(|error| AppError::Internal(error.to_string()))?;
    let before = value.clone();
    let root = value
        .as_object_mut()
        .ok_or_else(|| AppError::Internal("structured root is not object".into()))?;

    if let Some(mutation_id) = body.mutation_id.as_ref() {
        let seen = ensure_array(root, "canvasMutationIds");
        if seen.iter().any(|value| value.as_str() == Some(mutation_id)) {
            return Ok(StructuredResponse {
                permalink: permalink_for_guid(state, &entry.guid),
                path: entry.path,
                guid: entry.guid,
                kind: entry.kind,
                value_hash: Some(structured_value_hash(&value)),
                value: canvas_to_file_json(value),
            });
        }
    }

    let operation_count = body.operations.len();
    apply_canvas_operation_batch(root, body.operations)?;
    if let Some(mutation_id) = body.mutation_id {
        let seen = ensure_array(root, "canvasMutationIds");
        if seen.len() >= 256 {
            seen.remove(0);
        }
        seen.push(json!(mutation_id));
    }

    let update = ydoc::build_structured_update(&current, &value)?;
    if !update.is_empty() {
        ydoc::write_update_at_epoch(state, &document_id, epoch, update).await?;
        mark_structured_write(state, vault_id, principal).await;
    }
    if audit::is_cursor(principal) {
        audit::record(
            state,
            principal,
            vault_id,
            AuditEntry::new("canvas_operations", path)
                .before(pretty_json(&canvas_to_file_json(before)))
                .after(pretty_json(&canvas_to_file_json(value.clone())))
                .details(json!({ "operationCount": operation_count })),
        )
        .await;
    }
    Ok(StructuredResponse {
        permalink: permalink_for_guid(state, &entry.guid),
        path: entry.path,
        guid: entry.guid,
        kind: entry.kind,
        value_hash: Some(structured_value_hash(&value)),
        value: canvas_to_file_json(value),
    })
}

pub async fn read_base(
    State(state): State<AppState>,
    principal: ApiPrincipal,
    Path((vault_id, path)): Path<(String, String)>,
) -> AppResult<Json<StructuredResponse>> {
    Ok(Json(
        read_base_inner(&state, &principal, &vault_id, &path).await?,
    ))
}

pub(crate) async fn read_base_inner(
    state: &AppState,
    principal: &ApiPrincipal,
    vault_id: &str,
    path: &str,
) -> AppResult<StructuredResponse> {
    read_structured_json(state, principal, vault_id, path, "base").await
}

pub async fn create_base(
    State(state): State<AppState>,
    principal: ApiPrincipal,
    Path(vault_id): Path<String>,
    Json(body): Json<CreateStructuredBody>,
) -> AppResult<Json<StructuredResponse>> {
    Ok(Json(
        create_base_inner(&state, &principal, &vault_id, body).await?,
    ))
}

pub(crate) async fn create_base_inner(
    state: &AppState,
    principal: &ApiPrincipal,
    vault_id: &str,
    body: CreateStructuredBody,
) -> AppResult<StructuredResponse> {
    let value = base_value_to_json(body.value)?;
    create_structured(state, principal, vault_id, &body.path, "base", value).await
}

pub async fn replace_base(
    State(state): State<AppState>,
    principal: ApiPrincipal,
    Path((vault_id, path)): Path<(String, String)>,
    headers: axum::http::HeaderMap,
    Json(body): Json<JsonValue>,
) -> AppResult<Json<StructuredResponse>> {
    let body = base_value_to_json(body)?;
    let expected = if_match_hash(&headers);
    Ok(Json(
        write_structured_json(
            &state,
            &principal,
            &vault_id,
            &path,
            "base",
            body,
            expected.as_deref(),
        )
        .await?,
    ))
}

pub async fn delete_base(
    State(state): State<AppState>,
    principal: ApiPrincipal,
    Path((vault_id, path)): Path<(String, String)>,
) -> AppResult<Json<JsonValue>> {
    delete_structured_inner(&state, &principal, &vault_id, &path, "base").await?;
    Ok(Json(json!({ "deleted": true })))
}

pub(crate) async fn delete_base_inner(
    state: &AppState,
    principal: &ApiPrincipal,
    vault_id: &str,
    path: &str,
) -> AppResult<()> {
    delete_structured_inner(state, principal, vault_id, path, "base").await
}

pub async fn move_base(
    State(state): State<AppState>,
    principal: ApiPrincipal,
    Path((vault_id, path)): Path<(String, String)>,
    Json(body): Json<MoveStructuredBody>,
) -> AppResult<Json<StructuredResponse>> {
    Ok(Json(
        move_structured_inner(&state, &principal, &vault_id, &path, body, "base").await?,
    ))
}

pub async fn list_base_views(
    State(state): State<AppState>,
    principal: ApiPrincipal,
    Path((vault_id, path)): Path<(String, String)>,
) -> AppResult<Json<JsonValue>> {
    let value = read_structured_json(&state, &principal, &vault_id, &path, "base")
        .await?
        .value;
    Ok(Json(base_views_value(value)))
}

pub(crate) async fn list_base_views_inner(
    state: &AppState,
    principal: &ApiPrincipal,
    vault_id: &str,
    path: &str,
) -> AppResult<JsonValue> {
    Ok(base_views_value(
        read_structured_json(state, principal, vault_id, path, "base")
            .await?
            .value,
    ))
}

pub async fn add_base_view(
    State(state): State<AppState>,
    principal: ApiPrincipal,
    Path((vault_id, path)): Path<(String, String)>,
    Json(body): Json<BaseViewBody>,
) -> AppResult<Json<StructuredResponse>> {
    Ok(Json(
        add_base_view_inner(&state, &principal, &vault_id, &path, body).await?,
    ))
}

pub(crate) async fn add_base_view_inner(
    state: &AppState,
    principal: &ApiPrincipal,
    vault_id: &str,
    path: &str,
    body: BaseViewBody,
) -> AppResult<StructuredResponse> {
    update_base_doc(state, principal, vault_id, path, |root| {
        upsert_view(root, body)
    })
    .await
}

pub(crate) async fn update_base_view_inner(
    state: &AppState,
    principal: &ApiPrincipal,
    vault_id: &str,
    path: &str,
    body: BaseViewPatchBody,
) -> AppResult<StructuredResponse> {
    update_base_doc(state, principal, vault_id, path, |root| {
        patch_view(root, body)
    })
    .await
}

pub(crate) async fn delete_base_view_inner(
    state: &AppState,
    principal: &ApiPrincipal,
    vault_id: &str,
    path: &str,
    name: &str,
) -> AppResult<StructuredResponse> {
    update_base_doc(state, principal, vault_id, path, |root| {
        delete_view(root, name)
    })
    .await
}

pub(crate) async fn set_base_filters_inner(
    state: &AppState,
    principal: &ApiPrincipal,
    vault_id: &str,
    path: &str,
    value: JsonValue,
) -> AppResult<StructuredResponse> {
    update_base_doc(state, principal, vault_id, path, |root| {
        root.insert("filters".into(), value);
        Ok(())
    })
    .await
}

pub(crate) async fn set_base_view_filters_inner(
    state: &AppState,
    principal: &ApiPrincipal,
    vault_id: &str,
    path: &str,
    name: &str,
    value: JsonValue,
) -> AppResult<StructuredResponse> {
    update_base_doc(state, principal, vault_id, path, |root| {
        set_view_field(root, name, "filters", value)
    })
    .await
}

pub(crate) async fn set_base_formula_inner(
    state: &AppState,
    principal: &ApiPrincipal,
    vault_id: &str,
    path: &str,
    name: &str,
    value: JsonValue,
) -> AppResult<StructuredResponse> {
    update_base_doc(state, principal, vault_id, path, |root| {
        set_named(root, "formulas", name, value)
    })
    .await
}

pub(crate) async fn delete_base_formula_inner(
    state: &AppState,
    principal: &ApiPrincipal,
    vault_id: &str,
    path: &str,
    name: &str,
) -> AppResult<StructuredResponse> {
    update_base_doc(state, principal, vault_id, path, |root| {
        delete_named(root, "formulas", name)
    })
    .await
}

pub(crate) async fn set_base_property_inner(
    state: &AppState,
    principal: &ApiPrincipal,
    vault_id: &str,
    path: &str,
    name: &str,
    value: JsonValue,
) -> AppResult<StructuredResponse> {
    update_base_doc(state, principal, vault_id, path, |root| {
        set_named(root, "properties", name, value)
    })
    .await
}

pub(crate) async fn delete_base_property_inner(
    state: &AppState,
    principal: &ApiPrincipal,
    vault_id: &str,
    path: &str,
    name: &str,
) -> AppResult<StructuredResponse> {
    update_base_doc(state, principal, vault_id, path, |root| {
        delete_named(root, "properties", name)
    })
    .await
}

pub async fn update_base_view(
    State(state): State<AppState>,
    principal: ApiPrincipal,
    Path((vault_id, path)): Path<(String, String)>,
    Json(body): Json<BaseViewPatchBody>,
) -> AppResult<Json<StructuredResponse>> {
    Ok(Json(
        update_base_doc(&state, &principal, &vault_id, &path, |root| {
            patch_view(root, body)
        })
        .await?,
    ))
}

pub async fn delete_base_view(
    State(state): State<AppState>,
    principal: ApiPrincipal,
    Path((vault_id, path)): Path<(String, String)>,
    Json(body): Json<NameBody>,
) -> AppResult<Json<StructuredResponse>> {
    Ok(Json(
        update_base_doc(&state, &principal, &vault_id, &path, |root| {
            delete_view(root, &body.name)
        })
        .await?,
    ))
}

pub async fn set_base_filters(
    State(state): State<AppState>,
    principal: ApiPrincipal,
    Path((vault_id, path)): Path<(String, String)>,
    Json(body): Json<SetValueBody>,
) -> AppResult<Json<StructuredResponse>> {
    Ok(Json(
        update_base_doc(&state, &principal, &vault_id, &path, |root| {
            root.insert("filters".into(), body.value);
            Ok(())
        })
        .await?,
    ))
}

pub async fn set_base_view_filters(
    State(state): State<AppState>,
    principal: ApiPrincipal,
    Path((vault_id, path)): Path<(String, String)>,
    Json(body): Json<SetValueBody>,
) -> AppResult<Json<StructuredResponse>> {
    let name = body
        .name
        .ok_or_else(|| AppError::BadRequest("name is required".into()))?;
    Ok(Json(
        update_base_doc(&state, &principal, &vault_id, &path, |root| {
            set_view_field(root, &name, "filters", body.value)
        })
        .await?,
    ))
}

pub async fn set_base_formula(
    State(state): State<AppState>,
    principal: ApiPrincipal,
    Path((vault_id, path)): Path<(String, String)>,
    Json(body): Json<SetValueBody>,
) -> AppResult<Json<StructuredResponse>> {
    let name = body
        .name
        .ok_or_else(|| AppError::BadRequest("name is required".into()))?;
    Ok(Json(
        update_base_doc(&state, &principal, &vault_id, &path, |root| {
            set_named(root, "formulas", &name, body.value)
        })
        .await?,
    ))
}

pub async fn delete_base_formula(
    State(state): State<AppState>,
    principal: ApiPrincipal,
    Path((vault_id, path)): Path<(String, String)>,
    Json(body): Json<NameBody>,
) -> AppResult<Json<StructuredResponse>> {
    Ok(Json(
        update_base_doc(&state, &principal, &vault_id, &path, |root| {
            delete_named(root, "formulas", &body.name)
        })
        .await?,
    ))
}

pub async fn set_base_property(
    State(state): State<AppState>,
    principal: ApiPrincipal,
    Path((vault_id, path)): Path<(String, String)>,
    Json(body): Json<SetValueBody>,
) -> AppResult<Json<StructuredResponse>> {
    let name = body
        .name
        .ok_or_else(|| AppError::BadRequest("name is required".into()))?;
    Ok(Json(
        update_base_doc(&state, &principal, &vault_id, &path, |root| {
            set_named(root, "properties", &name, body.value)
        })
        .await?,
    ))
}

pub async fn delete_base_property(
    State(state): State<AppState>,
    principal: ApiPrincipal,
    Path((vault_id, path)): Path<(String, String)>,
    Json(body): Json<NameBody>,
) -> AppResult<Json<StructuredResponse>> {
    Ok(Json(
        update_base_doc(&state, &principal, &vault_id, &path, |root| {
            delete_named(root, "properties", &body.name)
        })
        .await?,
    ))
}

async fn update_canvas_doc<F>(
    state: &AppState,
    principal: &ApiPrincipal,
    vault_id: &str,
    path: &str,
    edit: F,
) -> AppResult<StructuredResponse>
where
    F: FnOnce(&mut JsonMap<String, JsonValue>) -> AppResult<()>,
{
    let mut response =
        mutate_structured_doc(state, principal, vault_id, path, "canvas", edit).await?;
    response.value = canvas_to_file_json(response.value);
    Ok(response)
}

async fn update_base_doc<F>(
    state: &AppState,
    principal: &ApiPrincipal,
    vault_id: &str,
    path: &str,
    edit: F,
) -> AppResult<StructuredResponse>
where
    F: FnOnce(&mut JsonMap<String, JsonValue>) -> AppResult<()>,
{
    mutate_structured_doc(state, principal, vault_id, path, "base", edit).await
}

async fn mutate_structured_doc<F>(
    state: &AppState,
    principal: &ApiPrincipal,
    vault_id: &str,
    path: &str,
    kind: &str,
    edit: F,
) -> AppResult<StructuredResponse>
where
    F: FnOnce(&mut JsonMap<String, JsonValue>) -> AppResult<()>,
{
    let entry = require_structured_access(state, principal, vault_id, path, kind, true).await?;
    let document_id = doc_id(vault_id, &entry.guid);
    let mutation_lock = canvas_mutation_lock(&document_id).await;
    let _mutation_guard = mutation_lock.lock().await;
    let (epoch, current) = ydoc::read_update_for_write(state, &document_id).await?;
    let mut value =
        ydoc::decode_structured(&current).map_err(|error| AppError::Internal(error.to_string()))?;
    let before = value.clone();
    let root = value
        .as_object_mut()
        .ok_or_else(|| AppError::Internal("structured root is not object".into()))?;
    edit(root)?;
    let update = ydoc::build_structured_update(&current, &value)?;
    if !update.is_empty() {
        ydoc::write_update_at_epoch(state, &document_id, epoch, update).await?;
        mark_structured_write(state, vault_id, principal).await;
        if audit::is_cursor(principal) {
            audit::record(
                state,
                principal,
                vault_id,
                AuditEntry::new("structured_set", path)
                    .before(pretty_json(&before))
                    .after(pretty_json(&value))
                    .details(json!({ "kind": kind })),
            )
            .await;
        }
    }
    Ok(StructuredResponse {
        permalink: permalink_for_guid(state, &entry.guid),
        path: entry.path,
        guid: entry.guid,
        kind: entry.kind,
        value_hash: Some(structured_value_hash(&value)),
        value,
    })
}

pub(crate) async fn read_structured_json(
    state: &AppState,
    principal: &ApiPrincipal,
    vault_id: &str,
    path: &str,
    kind: &str,
) -> AppResult<StructuredResponse> {
    let entry = require_structured_access(state, principal, vault_id, path, kind, false).await?;
    let update = ydoc::read_update(state, &doc_id(vault_id, &entry.guid)).await?;
    let value = ydoc::decode_structured(&update).map_err(|e| AppError::Internal(e.to_string()))?;
    Ok(StructuredResponse {
        permalink: permalink_for_guid(state, &entry.guid),
        path: entry.path,
        guid: entry.guid,
        kind: entry.kind,
        value_hash: Some(structured_value_hash(&value)),
        value,
    })
}

pub(crate) async fn write_structured_json(
    state: &AppState,
    principal: &ApiPrincipal,
    vault_id: &str,
    path: &str,
    kind: &str,
    value: JsonValue,
    expected_hash: Option<&str>,
) -> AppResult<StructuredResponse> {
    let entry = require_structured_access(state, principal, vault_id, path, kind, true).await?;
    let before = ydoc::update_structured(state, &doc_id(vault_id, &entry.guid), |current| {
        if expected_hash
            .is_some_and(|expected| !expected.eq_ignore_ascii_case(&structured_value_hash(current)))
        {
            return Err(AppError::Conflict("stale".into()));
        }
        Ok(value.clone())
    })
    .await?;
    // The before-image is only needed for the cursor audit trail.
    let before = audit::is_cursor(principal).then_some(before);
    mark_structured_write(state, vault_id, principal).await;
    if let Some(before) = before {
        audit::record(
            state,
            principal,
            vault_id,
            AuditEntry::new("structured_set", path)
                .before(pretty_json(&before))
                .after(pretty_json(&value))
                .details(json!({ "kind": kind })),
        )
        .await;
    }
    Ok(StructuredResponse {
        permalink: permalink_for_guid(state, &entry.guid),
        path: entry.path,
        guid: entry.guid,
        kind: entry.kind,
        value_hash: Some(structured_value_hash(&value)),
        value,
    })
}

pub(crate) async fn create_structured(
    state: &AppState,
    principal: &ApiPrincipal,
    vault_id: &str,
    path: &str,
    kind: &str,
    value: JsonValue,
) -> AppResult<StructuredResponse> {
    principal.require_vault(vault_id)?;
    require_member(state, &principal.user.id, vault_id).await?;
    validate_structured_path(path, kind)?;
    if structured_entry_by_path(state, vault_id, path)
        .await?
        .is_some()
    {
        return Err(AppError::Conflict("structured file already exists".into()));
    }
    let level = authorize_path(state, &principal.user, vault_id, path).await?;
    if level == Level::ReadOnly {
        return Err(AppError::Forbidden);
    }
    let guid = uuid::Uuid::new_v4().to_string();
    state
        .ensure_vault_document(vault_id, &doc_id(vault_id, &guid))
        .await?;
    ydoc::set_structured(state, &doc_id(vault_id, &guid), &value).await?;
    ydoc::index_set_structured(state, vault_id, path, &guid, kind).await?;
    mark_structured_write(state, vault_id, principal).await;
    audit::record(
        state,
        principal,
        vault_id,
        AuditEntry::new("structured_create", path)
            .after(pretty_json(&value))
            .details(json!({ "kind": kind })),
    )
    .await;
    Ok(StructuredResponse {
        permalink: permalink_for_guid(state, &guid),
        path: path.into(),
        guid,
        kind: kind.into(),
        value_hash: Some(structured_value_hash(&value)),
        value,
    })
}

pub(crate) async fn move_structured_inner(
    state: &AppState,
    principal: &ApiPrincipal,
    vault_id: &str,
    path: &str,
    body: MoveStructuredBody,
    kind: &str,
) -> AppResult<StructuredResponse> {
    validate_structured_path(&body.to_path, kind)?;
    let entry = require_structured_access(state, principal, vault_id, path, kind, true).await?;
    if structured_entry_by_path(state, vault_id, &body.to_path)
        .await?
        .is_some()
    {
        return Err(AppError::Conflict("exists".into()));
    }
    let level = authorize_path(state, &principal.user, vault_id, &body.to_path).await?;
    if level == Level::ReadOnly {
        return Err(AppError::Forbidden);
    }
    ydoc::index_rename_structured(state, vault_id, &entry.path, &body.to_path).await?;
    if body.update_embeds {
        crate::notes::rewrite_references_after_move(
            state,
            vault_id,
            None,
            &entry.path,
            &body.to_path,
        )
        .await;
    }
    mark_structured_write(state, vault_id, principal).await;
    audit::record(
        state,
        principal,
        vault_id,
        AuditEntry::new("structured_move", &entry.path)
            .to_path(&body.to_path)
            .details(json!({ "kind": kind, "updateEmbeds": body.update_embeds })),
    )
    .await;
    let mut out = read_structured_json(state, principal, vault_id, &body.to_path, kind).await?;
    if kind == "canvas" {
        out.value = canvas_to_file_json(out.value);
    }
    Ok(out)
}

pub(crate) async fn delete_structured_inner(
    state: &AppState,
    principal: &ApiPrincipal,
    vault_id: &str,
    path: &str,
    kind: &str,
) -> AppResult<()> {
    let entry = require_structured_access(state, principal, vault_id, path, kind, true).await?;
    // Capture the document for the cursor audit trail so the delete can be
    // undone; human deletes skip the extra read.
    let before = if audit::is_cursor(principal) {
        let update = ydoc::read_update(state, &doc_id(vault_id, &entry.guid)).await?;
        Some(ydoc::decode_structured(&update).map_err(|e| AppError::Internal(e.to_string()))?)
    } else {
        None
    };
    ydoc::index_remove_structured(state, vault_id, &entry.path).await?;
    mark_structured_write(state, vault_id, principal).await;
    if let Some(before) = before {
        audit::record(
            state,
            principal,
            vault_id,
            AuditEntry::new("structured_delete", &entry.path)
                .before(pretty_json(&before))
                .details(json!({ "kind": kind })),
        )
        .await;
    }
    Ok(())
}

fn pretty_json(value: &JsonValue) -> String {
    serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string())
}

async fn require_structured_access(
    state: &AppState,
    principal: &ApiPrincipal,
    vault_id: &str,
    path: &str,
    expect_kind: &str,
    write: bool,
) -> AppResult<StructuredIndexEntry> {
    principal.require_vault(vault_id)?;
    require_member(state, &principal.user.id, vault_id).await?;
    validate_structured_path(path, expect_kind)?;
    let entry = structured_entry_by_path(state, vault_id, path)
        .await?
        .ok_or(AppError::NotFound)?;
    if entry.kind != expect_kind {
        return Err(AppError::BadRequest("wrong structured kind".into()));
    }
    let level = authorize_path(state, &principal.user, vault_id, path).await?;
    if write && level == Level::ReadOnly {
        return Err(AppError::Forbidden);
    }
    Ok(entry)
}

async fn structured_entry_by_path(
    state: &AppState,
    vault_id: &str,
    path: &str,
) -> AppResult<Option<StructuredIndexEntry>> {
    let update = ydoc::read_update(state, vault_id).await?;
    Ok(ydoc::decode_structured_index(&update)
        .map_err(|e| AppError::Internal(e.to_string()))?
        .into_iter()
        .find(|e| e.path == path))
}

pub(crate) fn canvas_file_to_map(value: JsonValue) -> JsonValue {
    let mut root = JsonMap::new();
    let nodes = value
        .get("nodes")
        .and_then(JsonValue::as_array)
        .cloned()
        .unwrap_or_default();
    let edges = value
        .get("edges")
        .and_then(JsonValue::as_array)
        .cloned()
        .unwrap_or_default();
    let mut node_map = JsonMap::new();
    let mut edge_map = JsonMap::new();
    let mut node_order = Vec::new();
    let mut edge_order = Vec::new();
    for item in nodes {
        if let Some(id) = item.get("id").and_then(JsonValue::as_str) {
            node_order.push(json!(id));
            node_map.insert(id.into(), item);
        }
    }
    for item in edges {
        if let Some(id) = item.get("id").and_then(JsonValue::as_str) {
            edge_order.push(json!(id));
            edge_map.insert(id.into(), item);
        }
    }
    root.insert("nodes".into(), JsonValue::Object(node_map));
    root.insert("edges".into(), JsonValue::Object(edge_map));
    root.insert("nodeOrder".into(), JsonValue::Array(node_order));
    root.insert("edgeOrder".into(), JsonValue::Array(edge_order));
    JsonValue::Object(root)
}

fn add_node(root: &mut JsonMap<String, JsonValue>, body: CanvasNodeBody) -> AppResult<()> {
    let id = body.id.unwrap_or_else(gen_canvas_id);
    let mut node = body.fields;
    node.insert("id".into(), json!(id.clone()));
    apply_canvas_operation_batch(root, vec![CanvasOperation::NodeCreate { node }])
}

fn add_edge(root: &mut JsonMap<String, JsonValue>, body: CanvasEdgeBody) -> AppResult<()> {
    let id = body.id.unwrap_or_else(gen_canvas_id);
    let mut edge = body.fields;
    edge.insert("id".into(), json!(id.clone()));
    edge.insert("fromNode".into(), json!(body.from_node));
    edge.insert("toNode".into(), json!(body.to_node));
    apply_canvas_operation_batch(root, vec![CanvasOperation::EdgeCreate { edge }])
}

fn patch_item(
    root: &mut JsonMap<String, JsonValue>,
    map_key: &str,
    id: &str,
    patch: JsonMap<String, JsonValue>,
) -> AppResult<()> {
    let patch = CanvasFieldPatch {
        set: patch,
        remove: Vec::new(),
    };
    let operation = if map_key == "nodes" {
        CanvasOperation::NodePatch {
            id: id.into(),
            patch,
        }
    } else {
        CanvasOperation::EdgePatch {
            id: id.into(),
            patch,
        }
    };
    apply_canvas_operation_batch(root, vec![operation])
}

fn delete_node(root: &mut JsonMap<String, JsonValue>, id: &str) -> AppResult<()> {
    apply_canvas_operation_batch(root, vec![CanvasOperation::NodeDelete { id: id.into() }])
}

fn delete_item(
    root: &mut JsonMap<String, JsonValue>,
    map_key: &str,
    order_key: &str,
    id: &str,
) -> AppResult<()> {
    let operation = if map_key == "nodes" {
        CanvasOperation::NodeDelete { id: id.into() }
    } else {
        CanvasOperation::EdgeDelete { id: id.into() }
    };
    let _ = order_key;
    apply_canvas_operation_batch(root, vec![operation])
}

fn apply_canvas_operation_batch(
    root: &mut JsonMap<String, JsonValue>,
    operations: Vec<CanvasOperation>,
) -> AppResult<()> {
    for operation in operations {
        match operation {
            CanvasOperation::NodeCreate { node } => {
                create_canvas_item(root, "nodes", "nodeOrder", "deletedNodeIds", node, false)?
            }
            CanvasOperation::NodeRestore { node } => {
                create_canvas_item(root, "nodes", "nodeOrder", "deletedNodeIds", node, true)?
            }
            CanvasOperation::EdgeCreate { edge } => {
                create_canvas_item(root, "edges", "edgeOrder", "deletedEdgeIds", edge, false)?
            }
            CanvasOperation::EdgeRestore { edge } => {
                create_canvas_item(root, "edges", "edgeOrder", "deletedEdgeIds", edge, true)?
            }
            CanvasOperation::NodePatch { id, patch } => {
                apply_canvas_patch(root, "nodes", &id, patch)?
            }
            CanvasOperation::EdgePatch { id, patch } => {
                apply_canvas_patch(root, "edges", &id, patch)?
            }
            CanvasOperation::NodeDelete { id } => {
                tombstone_canvas_item(root, "nodes", "nodeOrder", "deletedNodeIds", &id);
                let connected: Vec<String> = ensure_object(root, "edges")
                    .iter()
                    .filter(|(_, edge)| {
                        edge.get("fromNode").and_then(JsonValue::as_str) == Some(id.as_str())
                            || edge.get("toNode").and_then(JsonValue::as_str) == Some(id.as_str())
                    })
                    .map(|(edge_id, _)| edge_id.clone())
                    .collect();
                for edge_id in connected {
                    tombstone_canvas_item(root, "edges", "edgeOrder", "deletedEdgeIds", &edge_id);
                }
            }
            CanvasOperation::EdgeDelete { id } => {
                tombstone_canvas_item(root, "edges", "edgeOrder", "deletedEdgeIds", &id)
            }
            CanvasOperation::NodeOrder { order } => {
                set_canvas_order(root, "nodes", "nodeOrder", order)?
            }
            CanvasOperation::EdgeOrder { order } => {
                set_canvas_order(root, "edges", "edgeOrder", order)?
            }
        }
    }
    validate_canvas_edges(root)
}

fn create_canvas_item(
    root: &mut JsonMap<String, JsonValue>,
    map_key: &str,
    order_key: &str,
    tombstone_key: &str,
    mut item: JsonMap<String, JsonValue>,
    restore: bool,
) -> AppResult<()> {
    let id = item
        .get("id")
        .and_then(JsonValue::as_str)
        .filter(|id| !id.is_empty())
        .ok_or_else(|| AppError::BadRequest("Canvas item id is required".into()))?
        .to_string();
    if ensure_object(root, tombstone_key).contains_key(&id) {
        if !restore {
            return Err(AppError::Conflict("Canvas item is tombstoned".into()));
        }
        ensure_object(root, tombstone_key).remove(&id);
    }
    if ensure_object(root, map_key).contains_key(&id) {
        return Err(AppError::Conflict("Canvas item exists".into()));
    }
    item.insert("id".into(), json!(id.clone()));
    ensure_object(root, map_key).insert(id.clone(), JsonValue::Object(item));
    ensure_order(root, order_key, &id);
    Ok(())
}

fn apply_canvas_patch(
    root: &mut JsonMap<String, JsonValue>,
    map_key: &str,
    id: &str,
    patch: CanvasFieldPatch,
) -> AppResult<()> {
    if id.is_empty() {
        return Err(AppError::BadRequest("Canvas item id is required".into()));
    }
    let item = ensure_object(root, map_key)
        .get_mut(id)
        .and_then(JsonValue::as_object_mut)
        .ok_or(AppError::NotFound)?;
    for key in patch.remove {
        if key != "id" {
            item.remove(&key);
        }
    }
    for (key, value) in patch.set {
        if key != "id" {
            item.insert(key, value);
        }
    }
    item.insert("id".into(), json!(id));
    Ok(())
}

fn tombstone_canvas_item(
    root: &mut JsonMap<String, JsonValue>,
    map_key: &str,
    order_key: &str,
    tombstone_key: &str,
    id: &str,
) {
    ensure_object(root, map_key).remove(id);
    ensure_object(root, tombstone_key).insert(id.into(), json!(-1));
    if let Some(order) = root.get_mut(order_key).and_then(JsonValue::as_array_mut) {
        order.retain(|value| value.as_str() != Some(id));
    }
}

fn set_canvas_order(
    root: &mut JsonMap<String, JsonValue>,
    map_key: &str,
    order_key: &str,
    order: Vec<String>,
) -> AppResult<()> {
    let items = ensure_object(root, map_key);
    let mut seen = HashSet::new();
    let mut normalized = Vec::new();
    for id in order {
        if !items.contains_key(&id) {
            return Err(AppError::BadRequest(format!(
                "unknown Canvas item id: {id}"
            )));
        }
        if seen.insert(id.clone()) {
            normalized.push(json!(id));
        }
    }
    for id in items.keys() {
        if seen.insert(id.clone()) {
            normalized.push(json!(id));
        }
    }
    root.insert(order_key.into(), JsonValue::Array(normalized));
    Ok(())
}

fn validate_canvas_edges(root: &mut JsonMap<String, JsonValue>) -> AppResult<()> {
    let node_ids: HashSet<String> = ensure_object(root, "nodes").keys().cloned().collect();
    for edge in ensure_object(root, "edges").values() {
        let edge = edge
            .as_object()
            .ok_or_else(|| AppError::BadRequest("Canvas edge must be an object".into()))?;
        for key in ["fromNode", "toNode"] {
            let id = edge
                .get(key)
                .and_then(JsonValue::as_str)
                .ok_or_else(|| AppError::BadRequest(format!("Canvas edge {key} is required")))?;
            if !node_ids.contains(id) {
                return Err(AppError::BadRequest(format!(
                    "Canvas edge references unknown node: {id}"
                )));
            }
        }
    }
    Ok(())
}

fn upsert_view(root: &mut JsonMap<String, JsonValue>, body: BaseViewBody) -> AppResult<()> {
    let mut view = body.fields;
    view.insert("name".into(), json!(body.name));
    view.insert("type".into(), json!(body.view_type));
    upsert_by_name(root, "views", JsonValue::Object(view))
}

fn base_views_value(value: JsonValue) -> JsonValue {
    value.get("views").cloned().unwrap_or_else(|| json!([]))
}

fn patch_view(root: &mut JsonMap<String, JsonValue>, body: BaseViewPatchBody) -> AppResult<()> {
    let views = ensure_array(root, "views");
    let view = views
        .iter_mut()
        .find(|v| v.get("name").and_then(JsonValue::as_str) == Some(&body.name))
        .and_then(JsonValue::as_object_mut)
        .ok_or(AppError::NotFound)?;
    for (key, value) in body.patch {
        view.insert(key, value);
    }
    view.insert("name".into(), json!(body.name));
    Ok(())
}

fn delete_view(root: &mut JsonMap<String, JsonValue>, name: &str) -> AppResult<()> {
    let views = ensure_array(root, "views");
    let old_len = views.len();
    views.retain(|v| v.get("name").and_then(JsonValue::as_str) != Some(name));
    if old_len == views.len() {
        return Err(AppError::NotFound);
    }
    Ok(())
}

fn set_view_field(
    root: &mut JsonMap<String, JsonValue>,
    name: &str,
    field: &str,
    value: JsonValue,
) -> AppResult<()> {
    let views = ensure_array(root, "views");
    let view = views
        .iter_mut()
        .find(|v| v.get("name").and_then(JsonValue::as_str) == Some(name))
        .and_then(JsonValue::as_object_mut)
        .ok_or(AppError::NotFound)?;
    view.insert(field.into(), value);
    Ok(())
}

fn set_named(
    root: &mut JsonMap<String, JsonValue>,
    key: &str,
    name: &str,
    value: JsonValue,
) -> AppResult<()> {
    ensure_object(root, key).insert(name.into(), value);
    Ok(())
}

fn delete_named(root: &mut JsonMap<String, JsonValue>, key: &str, name: &str) -> AppResult<()> {
    ensure_object(root, key)
        .remove(name)
        .ok_or(AppError::NotFound)?;
    Ok(())
}

fn upsert_by_name(
    root: &mut JsonMap<String, JsonValue>,
    key: &str,
    value: JsonValue,
) -> AppResult<()> {
    let name = value
        .get("name")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| AppError::BadRequest("name is required".into()))?
        .to_string();
    let values = ensure_array(root, key);
    if let Some(slot) = values
        .iter_mut()
        .find(|v| v.get("name").and_then(JsonValue::as_str) == Some(&name))
    {
        *slot = value;
    } else {
        values.push(value);
    }
    Ok(())
}

fn ensure_object<'a>(
    root: &'a mut JsonMap<String, JsonValue>,
    key: &str,
) -> &'a mut JsonMap<String, JsonValue> {
    if !root.get(key).is_some_and(JsonValue::is_object) {
        root.insert(key.into(), json!({}));
    }
    root.get_mut(key).unwrap().as_object_mut().unwrap()
}

fn ensure_array<'a>(root: &'a mut JsonMap<String, JsonValue>, key: &str) -> &'a mut Vec<JsonValue> {
    if !root.get(key).is_some_and(JsonValue::is_array) {
        root.insert(key.into(), json!([]));
    }
    root.get_mut(key).unwrap().as_array_mut().unwrap()
}

fn ensure_order(root: &mut JsonMap<String, JsonValue>, key: &str, id: &str) {
    let order = ensure_array(root, key);
    if !order.iter().any(|v| v.as_str() == Some(id)) {
        order.push(json!(id));
    }
}

fn base_value_to_json(value: JsonValue) -> AppResult<JsonValue> {
    let value = if let Some(text) = value.as_str() {
        serde_yaml::from_str::<JsonValue>(text)
            .map_err(|e| AppError::BadRequest(format!("invalid base YAML: {e}")))?
    } else {
        value
    };
    match value {
        JsonValue::Object(_) => Ok(value),
        JsonValue::Null => Ok(json!({})),
        _ => Err(AppError::BadRequest("base value must be an object".into())),
    }
}

fn validate_structured_path(path: &str, kind: &str) -> AppResult<()> {
    if path.is_empty() || path.contains('\\') {
        return Err(AppError::BadRequest("invalid structured path".into()));
    }
    for component in path.split('/') {
        if component.is_empty() || component == "." || component == ".." {
            return Err(AppError::BadRequest("invalid structured path".into()));
        }
    }
    let ext = match kind {
        "canvas" => ".canvas",
        "base" => ".base",
        _ => return Err(AppError::BadRequest("invalid structured kind".into())),
    };
    if !path.ends_with(ext) {
        return Err(AppError::BadRequest("invalid structured path".into()));
    }
    Ok(())
}

fn gen_canvas_id() -> String {
    nanoid::nanoid!(16, &nanoid::alphabet::SAFE)
}

fn doc_id(vault_id: &str, guid: &str) -> String {
    format!("{vault_id}__{guid}")
}

fn permalink_for_guid(state: &AppState, guid: &str) -> String {
    format!(
        "{}/n/{guid}",
        state.config.public_base_url.trim_end_matches('/')
    )
}

async fn mark_structured_write(state: &AppState, vault_id: &str, principal: &ApiPrincipal) {
    state
        .git
        .mark_write(
            vault_id,
            &principal.to_git_principal(now_millis() + 24 * 60 * 60 * 1000),
        )
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_add_sets_id_and_order() {
        let mut root = JsonMap::new();
        add_node(
            &mut root,
            CanvasNodeBody {
                id: Some("abc".into()),
                fields: JsonMap::from_iter([("type".into(), json!("text"))]),
            },
        )
        .unwrap();
        assert_eq!(root["nodeOrder"], json!(["abc"]));
        assert_eq!(root["nodes"]["abc"]["id"], json!("abc"));
    }

    #[test]
    fn canvas_file_order_ignores_duplicate_ids() {
        let value = json!({
            "nodes": {"a": {"id": "a"}, "b": {"id": "b"}},
            "edges": {},
            "nodeOrder": ["a", "a", "b", "a"],
            "edgeOrder": []
        });
        let file = canvas_to_file_json(value);
        assert_eq!(file["nodes"], json!([{"id": "a"}, {"id": "b"}]));
    }

    #[test]
    fn node_delete_cascades_edges() {
        let mut root = canvas_file_to_map(json!({ "nodes": [{"id":"a"},{"id":"b"}], "edges": [{"id":"e", "fromNode":"a", "toNode":"b"}] })).as_object().unwrap().clone();
        delete_node(&mut root, "a").unwrap();
        assert_eq!(root["edges"], json!({}));
        assert_eq!(root["edgeOrder"], json!([]));
        assert_eq!(root["deletedNodeIds"]["a"], json!(-1));
        assert_eq!(root["deletedEdgeIds"]["e"], json!(-1));
    }

    #[test]
    fn canvas_batch_patches_fields_removes_fields_and_restores_tombstones() {
        let mut root = canvas_file_to_map(json!({
            "nodes": [{"id":"a","type":"text","text":"keep","x":0,"color":"1"}],
            "edges": []
        }))
        .as_object()
        .unwrap()
        .clone();
        apply_canvas_operation_batch(
            &mut root,
            vec![CanvasOperation::NodePatch {
                id: "a".into(),
                patch: CanvasFieldPatch {
                    set: JsonMap::from_iter([("x".into(), json!(42))]),
                    remove: vec!["color".into(), "id".into()],
                },
            }],
        )
        .unwrap();
        assert_eq!(root["nodes"]["a"]["text"], json!("keep"));
        assert_eq!(root["nodes"]["a"]["x"], json!(42));
        assert!(root["nodes"]["a"].get("color").is_none());
        assert_eq!(root["nodes"]["a"]["id"], json!("a"));

        apply_canvas_operation_batch(
            &mut root,
            vec![CanvasOperation::NodeDelete { id: "a".into() }],
        )
        .unwrap();
        assert!(apply_canvas_operation_batch(
            &mut root,
            vec![CanvasOperation::NodeCreate {
                node: JsonMap::from_iter([("id".into(), json!("a"))]),
            }],
        )
        .is_err());
        apply_canvas_operation_batch(
            &mut root,
            vec![CanvasOperation::NodeRestore {
                node: JsonMap::from_iter([
                    ("id".into(), json!("a")),
                    ("type".into(), json!("text")),
                ]),
            }],
        )
        .unwrap();
        assert_eq!(root["nodes"]["a"]["id"], json!("a"));
        assert!(root["deletedNodeIds"].get("a").is_none());
    }

    #[test]
    fn canvas_batch_validates_edges_after_atomic_node_creation() {
        let mut root = canvas_file_to_map(json!({"nodes":[],"edges":[]}))
            .as_object()
            .unwrap()
            .clone();
        apply_canvas_operation_batch(
            &mut root,
            vec![
                CanvasOperation::NodeCreate {
                    node: JsonMap::from_iter([("id".into(), json!("a"))]),
                },
                CanvasOperation::NodeCreate {
                    node: JsonMap::from_iter([("id".into(), json!("b"))]),
                },
                CanvasOperation::EdgeCreate {
                    edge: JsonMap::from_iter([
                        ("id".into(), json!("e")),
                        ("fromNode".into(), json!("a")),
                        ("toNode".into(), json!("b")),
                    ]),
                },
            ],
        )
        .unwrap();
        assert_eq!(root["edges"]["e"]["toNode"], json!("b"));
    }

    #[test]
    fn view_upserts_by_name() {
        let mut root = JsonMap::new();
        upsert_view(
            &mut root,
            BaseViewBody {
                name: "Main".into(),
                view_type: "table".into(),
                fields: JsonMap::new(),
            },
        )
        .unwrap();
        upsert_view(
            &mut root,
            BaseViewBody {
                name: "Main".into(),
                view_type: "cards".into(),
                fields: JsonMap::new(),
            },
        )
        .unwrap();
        assert_eq!(root["views"].as_array().unwrap().len(), 1);
        assert_eq!(root["views"][0]["type"], json!("cards"));
    }

    #[test]
    fn base_value_parses_yaml_string() {
        let value = base_value_to_json(json!("views:\n  - name: Main\n    type: table\n")).unwrap();
        assert_eq!(value["views"][0]["name"], json!("Main"));
        assert_eq!(value["views"][0]["type"], json!("table"));
    }

    #[test]
    fn base_value_rejects_scalar_to_avoid_empty_roundtrip() {
        assert!(base_value_to_json(json!("not-a-map")).is_err());
        assert!(base_value_to_json(json!(["not-a-map"])).is_err());
        assert_eq!(base_value_to_json(JsonValue::Null).unwrap(), json!({}));
    }

    #[test]
    fn formula_property_set_unset() {
        let mut root = JsonMap::new();
        set_named(&mut root, "formulas", "f", json!("1+1")).unwrap();
        set_named(&mut root, "properties", "p", json!({"type":"text"})).unwrap();
        delete_named(&mut root, "formulas", "f").unwrap();
        assert!(root["formulas"].as_object().unwrap().is_empty());
        assert_eq!(root["properties"]["p"]["type"], json!("text"));
    }
}
