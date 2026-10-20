use axum::extract::{Form, Path, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Redirect, Response};
use axum::Json;
use base64::Engine;
use sea_orm::{ActiveModelTrait, ColumnTrait, EntityTrait, QueryFilter, Set};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::entities::{oauth_clients, oauth_codes, oauth_tokens, remote_cursors, users};
use crate::error::{AppError, AppResult};
use crate::routes::mcp_url;
use crate::session::{hash_token, now_millis};
use crate::state::{AppState, OAuthFlow};

const CODE_TTL_MS: i64 = 5 * 60 * 1000;
const ACCESS_TTL_MS: i64 = 60 * 60 * 1000;
const REFRESH_TTL_MS: i64 = 30 * 24 * 60 * 60 * 1000;
const TOKEN_ENDPOINT_AUTH_METHODS: [&str; 3] =
    ["none", "client_secret_post", "client_secret_basic"];

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
pub struct RegisterResponse {
    client_id: String,
    client_id_issued_at: i64,

    // RFC 7591 §3.2.1: public clients get no secret field at all. Clients
    // such as ChatGPT reject `"client_secret": null`.
    #[serde(skip_serializing_if = "Option::is_none")]
    client_secret: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    client_secret_expires_at: Option<i64>,
    redirect_uris: Vec<String>,
    token_endpoint_auth_method: String,
}

#[derive(Deserialize)]
pub struct RegisterRequest {
    redirect_uris: Vec<String>,
    token_endpoint_auth_method: Option<String>,
}

#[derive(Deserialize)]
pub struct AuthorizeParams {
    response_type: String,
    client_id: String,
    redirect_uri: String,
    code_challenge: String,
    code_challenge_method: String,
    resource: String,
    scope: Option<String>,
    state: Option<String>,
    mock_sub: Option<String>,
    mock_email: Option<String>,
    mock_name: Option<String>,
    mock_picture: Option<String>,
}

#[derive(Deserialize)]
pub struct TokenRequest {
    grant_type: String,
    code: Option<String>,
    redirect_uri: Option<String>,
    client_id: Option<String>,
    client_secret: Option<String>,
    code_verifier: Option<String>,
    refresh_token: Option<String>,
}

#[derive(Serialize)]
pub struct TokenResponse {
    access_token: String,
    token_type: &'static str,
    expires_in: i64,
    refresh_token: String,
    scope: String,
}

pub async fn protected_resource(State(state): State<AppState>) -> Json<serde_json::Value> {
    Json(protected_resource_value(&state, None))
}

pub async fn protected_resource_app(
    State(state): State<AppState>,
    Path(app_id): Path<String>,
) -> Json<serde_json::Value> {
    Json(protected_resource_value(&state, Some(&app_id)))
}

fn protected_resource_value(state: &AppState, app_id: Option<&str>) -> serde_json::Value {
    let base = state.config.public_base_url.trim_end_matches('/');
    let resource = app_id
        .map(|id| format!("{base}/mcp/i/{id}"))
        .unwrap_or_else(|| base.to_string());
    json!({
        "resource": resource,
        "authorization_servers": [base],
        "bearer_methods_supported": ["header"],
    })
}

pub async fn authorization_server(State(state): State<AppState>) -> Json<serde_json::Value> {
    let base = state.config.public_base_url.trim_end_matches('/');
    Json(json!({
        "issuer": base,
        "authorization_endpoint": format!("{base}/oauth/authorize"),
        "token_endpoint": format!("{base}/oauth/token"),
        "registration_endpoint": format!("{base}/oauth/register"),
        "code_challenge_methods_supported": ["S256"],
        "grant_types_supported": ["authorization_code", "refresh_token"],
        "response_types_supported": ["code"],
        "token_endpoint_auth_methods_supported": TOKEN_ENDPOINT_AUTH_METHODS,
    }))
}

pub async fn register_client(
    State(state): State<AppState>,
    Json(body): Json<RegisterRequest>,
) -> AppResult<(StatusCode, Json<RegisterResponse>)> {
    if body.redirect_uris.is_empty() {
        return Err(AppError::BadRequest("redirect_uris is required".into()));
    }
    for uri in &body.redirect_uris {
        url::Url::parse(uri).map_err(|_| AppError::BadRequest("invalid redirect_uri".into()))?;
    }

    // An omitted method registers a public PKCE client. RFC 7591 defaults to
    // client_secret_basic, but existing MCP clients rely on the public default.
    let auth_method = body
        .token_endpoint_auth_method
        .unwrap_or_else(|| "none".to_string());
    if !TOKEN_ENDPOINT_AUTH_METHODS.contains(&auth_method.as_str()) {
        return Err(AppError::BadRequest(
            "unsupported token_endpoint_auth_method".into(),
        ));
    }
    let client_secret = (auth_method != "none").then(random_token);
    let now = now_millis();
    let model = oauth_clients::ActiveModel {
        id: Set(uuid::Uuid::new_v4().to_string()),
        client_secret_hash: Set(client_secret.as_deref().map(hash_token)),
        redirect_uris: Set(serde_json::to_string(&body.redirect_uris).unwrap_or_default()),
        app_id: Set(None),
        created_at: Set(now),
    };
    let client = model.insert(&state.db).await?;
    Ok((
        StatusCode::CREATED,
        Json(RegisterResponse {
            client_id: client.id,
            client_id_issued_at: now / 1000,
            client_secret_expires_at: client_secret.as_ref().map(|_| 0),
            client_secret,
            redirect_uris: body.redirect_uris,
            token_endpoint_auth_method: auth_method,
        }),
    ))
}

pub async fn authorize(
    State(state): State<AppState>,
    Query(params): Query<AuthorizeParams>,
) -> AppResult<Response> {
    if params.response_type != "code" || params.code_challenge_method != "S256" {
        return Err(AppError::BadRequest(
            "unsupported authorization request".into(),
        ));
    }
    let client = oauth_clients::Entity::find_by_id(params.client_id.clone())
        .one(&state.db)
        .await?
        .ok_or_else(|| AppError::BadRequest("unknown client_id".into()))?;
    let redirect_uris: Vec<String> = serde_json::from_str(&client.redirect_uris)
        .map_err(|_| AppError::Internal("invalid client redirect_uris".into()))?;
    if !redirect_uris.iter().any(|u| u == &params.redirect_uri) {
        return Err(AppError::BadRequest("redirect_uri mismatch".into()));
    }
    let cursor = cursor_for_resource(&state, &params.resource).await?;
    prune_oauth_flows(&state).await;
    let key = uuid::Uuid::new_v4().to_string();
    state.oauth_flows.lock().await.insert(
        key.clone(),
        OAuthFlow {
            client_id: client.id,
            redirect_uri: params.redirect_uri,
            code_challenge: params.code_challenge,
            scope: params.scope.unwrap_or_default(),
            state: params.state,
            app_id: cursor.app_id,
            created_at: now_millis(),
        },
    );
    crate::oidc::begin_login(
        state,
        String::new(),
        params.mock_sub,
        params.mock_email,
        params.mock_name,
        params.mock_picture,
        Some(key),
    )
    .await
}

/// Evict abandoned/expired authorize requests so the in-memory map can't grow
/// unbounded. Mirrors `oidc::prune_oidc_flows`; successful flows are removed in
/// `finish_authorize`.
async fn prune_oauth_flows(state: &AppState) {
    let mut flows = state.oauth_flows.lock().await;
    flows.retain(|_, flow| !flow.is_expired());
}

pub async fn finish_authorize(
    state: AppState,
    oauth_flow_key: String,
    user: users::Model,
) -> AppResult<Response> {
    prune_oauth_flows(&state).await;
    let flow = state
        .oauth_flows
        .lock()
        .await
        .remove(&oauth_flow_key)
        .ok_or_else(|| AppError::BadRequest("unknown or expired oauth flow".into()))?;
    if flow.is_expired() {
        return Err(AppError::BadRequest("unknown or expired oauth flow".into()));
    }
    let cursor = remote_cursors::Entity::find()
        .filter(remote_cursors::Column::AppId.eq(&flow.app_id))
        .one(&state.db)
        .await?
        .ok_or(AppError::NotFound)?;
    if cursor.created_by != user.id {
        return Err(AppError::Forbidden);
    }
    let code = random_token();
    let now = now_millis();
    oauth_codes::ActiveModel {
        code_hash: Set(hash_token(&code)),
        client_id: Set(flow.client_id),
        user_id: Set(user.id),
        app_id: Set(cursor.app_id),
        vault_id: Set(cursor.vault_id),
        code_challenge: Set(flow.code_challenge),
        redirect_uri: Set(flow.redirect_uri.clone()),
        scope: Set(flow.scope),
        expires_at: Set(now + CODE_TTL_MS),
        created_at: Set(now),
    }
    .insert(&state.db)
    .await?;
    let sep = if flow.redirect_uri.contains('?') {
        '&'
    } else {
        '?'
    };
    let state_param = flow
        .state
        .map(|s| format!("&state={}", urlencoding(s)))
        .unwrap_or_default();
    Ok(Redirect::to(&format!(
        "{}{}code={}{}",
        flow.redirect_uri, sep, code, state_param
    ))
    .into_response())
}

pub async fn token(
    State(state): State<AppState>,
    headers: HeaderMap,
    Form(mut body): Form<TokenRequest>,
) -> AppResult<Json<TokenResponse>> {
    prune_oauth_flows(&state).await;
    if let Some((client_id, client_secret)) = basic_client_credentials(&headers)? {
        // RFC 6749 §2.3: a client uses one authentication method per request.
        if body.client_secret.is_some()
            || body.client_id.as_deref().is_some_and(|id| id != client_id)
        {
            return Err(AppError::Unauthorized);
        }
        body.client_id = Some(client_id);
        body.client_secret = Some(client_secret);
    }
    match body.grant_type.as_str() {
        "authorization_code" => exchange_code(&state, body).await.map(Json),
        "refresh_token" => refresh(&state, body).await.map(Json),
        _ => Err(AppError::BadRequest("unsupported grant_type".into())),
    }
}

/// Parses `Authorization: Basic` client credentials (RFC 6749 §2.3.1).
fn basic_client_credentials(headers: &HeaderMap) -> AppResult<Option<(String, String)>> {
    let Some(value) = headers.get(header::AUTHORIZATION) else {
        return Ok(None);
    };
    let Some(encoded) = value.to_str().ok().and_then(|v| {
        v.strip_prefix("Basic ")
            .or_else(|| v.strip_prefix("basic "))
    }) else {
        return Ok(None);
    };
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(encoded.trim())
        .ok()
        .and_then(|bytes| String::from_utf8(bytes).ok())
        .ok_or(AppError::Unauthorized)?;
    let (id, secret) = decoded.split_once(':').ok_or(AppError::Unauthorized)?;
    let unescape = |s: &str| {
        percent_encoding::percent_decode_str(&s.replace('+', " "))
            .decode_utf8()
            .map(|s| s.into_owned())
            .map_err(|_| AppError::Unauthorized)
    };
    Ok(Some((unescape(id)?, unescape(secret)?)))
}

async fn exchange_code(state: &AppState, body: TokenRequest) -> AppResult<TokenResponse> {
    let code = body
        .code
        .ok_or_else(|| AppError::BadRequest("missing code".into()))?;
    let verifier = body
        .code_verifier
        .ok_or_else(|| AppError::BadRequest("missing code_verifier".into()))?;
    let client_id = body
        .client_id
        .ok_or_else(|| AppError::BadRequest("missing client_id".into()))?;
    verify_client(state, &client_id, body.client_secret.as_deref()).await?;
    let grant = oauth_codes::Entity::find_by_id(hash_token(&code))
        .one(&state.db)
        .await?
        .ok_or(AppError::Unauthorized)?;
    // Delete the authorization code before validation: this enforces one-time
    // use even if validation fails (e.g. wrong redirect_uri), preventing replay
    // attempts. The client must request a new code on any failure.
    oauth_codes::Entity::delete_by_id(hash_token(&code))
        .exec(&state.db)
        .await?;
    if grant.expires_at < now_millis()
        || grant.client_id != client_id
        || body.redirect_uri.as_deref() != Some(&grant.redirect_uri)
    {
        return Err(AppError::Unauthorized);
    }
    if pkce_s256(&verifier) != grant.code_challenge {
        return Err(AppError::Unauthorized);
    }
    issue_tokens(
        state,
        grant.client_id,
        grant.user_id,
        grant.app_id,
        grant.vault_id,
        grant.scope,
    )
    .await
}

async fn refresh(state: &AppState, body: TokenRequest) -> AppResult<TokenResponse> {
    let refresh_token = body
        .refresh_token
        .ok_or_else(|| AppError::BadRequest("missing refresh_token".into()))?;
    let token = oauth_tokens::Entity::find()
        .filter(oauth_tokens::Column::RefreshHash.eq(hash_token(&refresh_token)))
        .one(&state.db)
        .await?
        .ok_or(AppError::Unauthorized)?;
    verify_client(state, &token.client_id, body.client_secret.as_deref()).await?;
    if token.refresh_expires_at < now_millis() {
        return Err(AppError::Unauthorized);
    }
    oauth_tokens::Entity::delete_by_id(token.access_hash)
        .exec(&state.db)
        .await?;
    issue_tokens(
        state,
        token.client_id,
        token.user_id,
        token.app_id,
        token.vault_id,
        token.scope,
    )
    .await
}

async fn issue_tokens(
    state: &AppState,
    client_id: String,
    user_id: String,
    app_id: String,
    vault_id: String,
    scope: String,
) -> AppResult<TokenResponse> {
    let access_token = random_token();
    let refresh_token = random_token();
    let now = now_millis();
    oauth_tokens::ActiveModel {
        access_hash: Set(hash_token(&access_token)),
        refresh_hash: Set(Some(hash_token(&refresh_token))),
        client_id: Set(client_id),
        user_id: Set(user_id),
        app_id: Set(app_id),
        vault_id: Set(vault_id),
        scope: Set(scope.clone()),
        access_expires_at: Set(now + ACCESS_TTL_MS),
        refresh_expires_at: Set(now + REFRESH_TTL_MS),
        created_at: Set(now),
    }
    .insert(&state.db)
    .await?;
    Ok(TokenResponse {
        access_token,
        token_type: "Bearer",
        expires_in: ACCESS_TTL_MS / 1000,
        refresh_token,
        scope,
    })
}

async fn verify_client(
    state: &AppState,
    client_id: &str,
    client_secret: Option<&str>,
) -> AppResult<oauth_clients::Model> {
    let client = oauth_clients::Entity::find_by_id(client_id.to_string())
        .one(&state.db)
        .await?
        .ok_or(AppError::Unauthorized)?;
    if let Some(hash) = &client.client_secret_hash {
        if client_secret.map(hash_token).as_deref() != Some(hash) {
            return Err(AppError::Unauthorized);
        }
    }
    Ok(client)
}

async fn cursor_for_resource(state: &AppState, resource: &str) -> AppResult<remote_cursors::Model> {
    let app_id = resource
        .trim_end_matches('/')
        .rsplit('/')
        .next()
        .ok_or_else(|| AppError::BadRequest("invalid resource".into()))?;
    let cursor = remote_cursors::Entity::find()
        .filter(remote_cursors::Column::AppId.eq(app_id))
        .one(&state.db)
        .await?
        .ok_or_else(|| AppError::BadRequest("unknown resource".into()))?;
    if mcp_url(state, &cursor.app_id) != resource.trim_end_matches('/') {
        return Err(AppError::BadRequest("resource mismatch".into()));
    }
    Ok(cursor)
}

fn pkce_s256(verifier: &str) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()))
}

fn random_token() -> String {
    use rand::Rng;
    let bytes: [u8; 32] = rand::thread_rng().gen();
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn urlencoding(value: String) -> String {
    url::form_urlencoded::byte_serialize(value.as_bytes()).collect()
}
