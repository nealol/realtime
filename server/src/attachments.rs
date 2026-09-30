use std::net::IpAddr;
use std::path::PathBuf;

use axum::body::{Body, Bytes};
use axum::extract::{Multipart, Path, Query, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use base64::Engine;
use futures_util::StreamExt;
use hmac::{Hmac, Mac};
use sea_orm::{ActiveModelTrait, EntityTrait, Set};
use serde::Serialize;
use serde::{de::DeserializeOwned, Deserialize};
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;
use tokio_util::io::ReaderStream;
use url::Url;
use yrs::Any;

use crate::audit::{self, AuditEntry};
use crate::config::OidcMode;
use crate::crdt::Level;
use crate::entities::upload_jtis;
use crate::error::{AppError, AppResult};
use crate::routes::{authorize_path, require_member};
use crate::session::{now_millis, ApiPrincipal};
use crate::state::{AppState, Principal, PrincipalActor};
use crate::ydoc;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AttachmentSummary {
    pub path: String,
    pub hash: String,
    pub size: i64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UploadAttachmentResponse {
    pub path: String,
    pub hash: String,
    pub size: i64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MoveAttachmentBody {
    pub to_path: String,
    #[serde(default)]
    pub update_embeds: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UploadFromUrlBody {
    pub source_url: String,
    pub path: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateUploadLinkBody {
    pub landing_dir: Option<String>,
    pub expires_in_seconds: Option<i64>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateUploadLinkResponse {
    pub upload_url: String,
    pub expires_at: i64,
    pub landing_dir: String,
    pub token: String,
}

#[derive(Deserialize)]
pub struct UploadQuery {
    pub token: String,
}

pub(crate) struct AttachmentData {
    pub meta: AttachmentSummary,
    pub bytes: Vec<u8>,
    pub content_type: &'static str,
}

#[derive(Serialize, Deserialize)]
struct UploadTokenPayload {
    jti: String,
    vault_id: String,
    landing_dir: String,
    expires_at: i64,
    principal: UploadPrincipal,
}

#[derive(Serialize, Deserialize)]
struct UploadPrincipal {
    user_id: String,
    display_name: String,
    email: String,
    cursor_id: Option<String>,
    app_id: Option<String>,
    cursor_name: Option<String>,
}

pub async fn list_attachments(
    State(state): State<AppState>,
    principal: ApiPrincipal,
    Path(vault_id): Path<String>,
) -> AppResult<axum::Json<Vec<AttachmentSummary>>> {
    Ok(axum::Json(
        list_attachments_inner(&state, &principal, &vault_id).await?,
    ))
}

pub(crate) async fn list_attachments_inner(
    state: &AppState,
    principal: &ApiPrincipal,
    vault_id: &str,
) -> AppResult<Vec<AttachmentSummary>> {
    principal.require_vault(vault_id)?;
    require_member(state, &principal.user.id, vault_id).await?;
    let update = ydoc::read_update(state, vault_id).await?;
    let mut out = Vec::new();
    for (path, meta) in
        ydoc::decode_binaries_map(&update).map_err(|e| AppError::Internal(e.to_string()))?
    {
        if let Some((hash, size)) = binary_meta(meta) {
            out.push(AttachmentSummary { path, hash, size });
        }
    }
    out.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(out)
}

pub async fn head_attachment(
    State(state): State<AppState>,
    principal: ApiPrincipal,
    Path((vault_id, path)): Path<(String, String)>,
) -> AppResult<StatusCode> {
    let meta = head_attachment_inner(&state, &principal, &vault_id, &path).await?;
    if meta {
        Ok(StatusCode::OK)
    } else {
        Err(AppError::NotFound)
    }
}

pub(crate) async fn head_attachment_inner(
    state: &AppState,
    principal: &ApiPrincipal,
    vault_id: &str,
    path: &str,
) -> AppResult<bool> {
    let meta = require_attachment(state, principal, vault_id, path).await?;
    let blob = blob_path(state, vault_id, &meta.hash)?;
    Ok(tokio::fs::metadata(blob).await.is_ok())
}

pub async fn read_attachment(
    State(state): State<AppState>,
    principal: ApiPrincipal,
    Path((vault_id, path)): Path<(String, String)>,
) -> AppResult<Response> {
    let meta = require_attachment(&state, &principal, &vault_id, &path).await?;
    let file = tokio::fs::File::open(blob_path(&state, &vault_id, &meta.hash)?)
        .await
        .map_err(|_| AppError::NotFound)?;
    let body = Body::from_stream(ReaderStream::new(file));
    Ok(attachment_file_response(&path, meta.size, body))
}

/// Read an attachment without an authenticated principal. Used by the public
/// share viewer (`/api/view/{id}/attachments/...`), which authorizes by share
/// id before calling; this only validates the path and streams the blob.
pub(crate) async fn read_attachment_public(
    state: &AppState,
    vault_id: &str,
    path: &str,
) -> AppResult<Response> {
    validate_attachment_path(state, path)?;
    let update = ydoc::read_update(state, vault_id).await?;
    let meta = ydoc::decode_binaries_map(&update)
        .map_err(|e| AppError::Internal(e.to_string()))?
        .into_iter()
        .find_map(|(p, meta)| if p == path { binary_meta(meta) } else { None })
        .ok_or(AppError::NotFound)?;
    let (hash, size) = meta;
    let file = tokio::fs::File::open(blob_path(state, vault_id, &hash)?)
        .await
        .map_err(|_| AppError::NotFound)?;
    let body = Body::from_stream(ReaderStream::new(file));
    Ok(attachment_file_response(path, size, body))
}

/// Read a public attachment only while the live vault index still maps the
/// shared path to the exact hash captured when the link was created.
pub(crate) async fn read_attachment_public_exact(
    state: &AppState,
    vault_id: &str,
    path: &str,
    expected_hash: &str,
) -> AppResult<Response> {
    validate_attachment_path(state, path)?;
    let update = ydoc::read_update(state, vault_id).await?;
    let (hash, size) = ydoc::decode_binaries_map(&update)
        .map_err(|e| AppError::Internal(e.to_string()))?
        .into_iter()
        .find_map(|(p, meta)| if p == path { binary_meta(meta) } else { None })
        .filter(|(hash, _)| hash == expected_hash)
        .ok_or(AppError::NotFound)?;
    let file = tokio::fs::File::open(blob_path(state, vault_id, &hash)?)
        .await
        .map_err(|_| AppError::NotFound)?;
    let body = Body::from_stream(ReaderStream::new(file));
    Ok(attachment_file_response(path, size, body))
}

pub(crate) async fn read_attachment_inner(
    state: &AppState,
    principal: &ApiPrincipal,
    vault_id: &str,
    path: &str,
) -> AppResult<AttachmentData> {
    let meta = require_attachment(state, principal, vault_id, path).await?;
    let bytes = tokio::fs::read(blob_path(state, vault_id, &meta.hash)?)
        .await
        .map_err(|_| AppError::NotFound)?;
    Ok(AttachmentData {
        content_type: content_type_for_path(path),
        meta,
        bytes,
    })
}

pub async fn upload_attachment(
    State(state): State<AppState>,
    principal: ApiPrincipal,
    Path((vault_id, path)): Path<(String, String)>,
    headers: axum::http::HeaderMap,
    body: Body,
) -> AppResult<axum::Json<UploadAttachmentResponse>> {
    let expected = crate::structured::if_match_hash(&headers);
    let precondition = match (expected.as_deref(), require_absent(&headers)?) {
        (Some(_), true) => {
            return Err(AppError::BadRequest(
                "If-Match and If-None-Match cannot be combined".into(),
            ))
        }
        (Some(hash), false) => ydoc::AttachmentPrecondition::Hash(hash),
        (None, true) => ydoc::AttachmentPrecondition::Absent,
        (None, false) => ydoc::AttachmentPrecondition::None,
    };
    let bytes = body_to_bytes(body, state.config.attachment_max_bytes).await?;
    Ok(axum::Json(
        upload_attachment_bytes_inner(&state, &principal, &vault_id, &path, &bytes, precondition)
            .await?,
    ))
}

/// Whether the request sent `If-None-Match: *` (the path must be empty).
/// Entity tags are not supported there: an attachment is named by its hash,
/// which `If-Match` covers.
fn require_absent(headers: &axum::http::HeaderMap) -> AppResult<bool> {
    let Some(value) = headers.get(header::IF_NONE_MATCH) else {
        return Ok(false);
    };
    match value.to_str().map(str::trim) {
        Ok("*") => Ok(true),
        _ => Err(AppError::BadRequest(
            "If-None-Match supports only \"*\"".into(),
        )),
    }
}

pub(crate) async fn upload_attachment_bytes_inner(
    state: &AppState,
    principal: &ApiPrincipal,
    vault_id: &str,
    path: &str,
    bytes: &[u8],
    precondition: ydoc::AttachmentPrecondition<'_>,
) -> AppResult<UploadAttachmentResponse> {
    principal.require_vault(vault_id)?;
    require_member(state, &principal.user.id, vault_id).await?;
    validate_attachment_path(state, path)?;
    if bytes.len() as u64 > state.config.attachment_max_bytes {
        return Err(AppError::PayloadTooLarge);
    }
    require_attachment_write(state, principal, vault_id, path).await?;
    reject_html_upload(None, bytes)?;
    validate_magic_for_path(path, bytes)?;
    let (hash, size) = store_bytes(state, vault_id, bytes).await?;
    ydoc::index_set_binary_if(state, vault_id, path, &hash, size, precondition).await?;
    state
        .git
        .mark_write(
            vault_id,
            &principal.to_git_principal(now_millis() + 24 * 60 * 60 * 1000),
        )
        .await;
    audit::record(
        state,
        principal,
        vault_id,
        AuditEntry::new("attachment_upload", path)
            .details(serde_json::json!({ "hash": hash, "size": size })),
    )
    .await;
    Ok(UploadAttachmentResponse {
        path: path.to_string(),
        hash,
        size,
    })
}

pub async fn delete_attachment(
    State(state): State<AppState>,
    principal: ApiPrincipal,
    Path((vault_id, path)): Path<(String, String)>,
) -> AppResult<axum::Json<serde_json::Value>> {
    delete_attachment_inner(&state, &principal, &vault_id, &path).await?;
    Ok(axum::Json(serde_json::json!({ "ok": true })))
}

pub(crate) async fn delete_attachment_inner(
    state: &AppState,
    principal: &ApiPrincipal,
    vault_id: &str,
    path: &str,
) -> AppResult<()> {
    let meta = require_attachment(state, principal, vault_id, path).await?;
    ydoc::index_remove_binary(state, vault_id, path).await?;
    state
        .git
        .mark_write(
            vault_id,
            &principal.to_git_principal(now_millis() + 24 * 60 * 60 * 1000),
        )
        .await;
    // Blobs are content-addressed and unaffected by the index removal, so the
    // recorded hash/size are enough to restore the entry on undo.
    audit::record(
        state,
        principal,
        vault_id,
        AuditEntry::new("attachment_delete", path)
            .details(serde_json::json!({ "hash": meta.hash, "size": meta.size })),
    )
    .await;
    Ok(())
}

pub async fn move_attachment(
    State(state): State<AppState>,
    principal: ApiPrincipal,
    Path((vault_id, path)): Path<(String, String)>,
    axum::Json(body): axum::Json<MoveAttachmentBody>,
) -> AppResult<axum::Json<AttachmentSummary>> {
    Ok(axum::Json(
        move_attachment_inner(&state, &principal, &vault_id, &path, body).await?,
    ))
}

pub(crate) async fn move_attachment_inner(
    state: &AppState,
    principal: &ApiPrincipal,
    vault_id: &str,
    path: &str,
    body: MoveAttachmentBody,
) -> AppResult<AttachmentSummary> {
    let meta = require_attachment(state, principal, vault_id, path).await?;
    validate_attachment_path(state, &body.to_path)?;
    if path == body.to_path {
        return Err(AppError::Conflict("same_path".into()));
    }
    if attachment_exists(state, principal, vault_id, &body.to_path).await? {
        return Err(AppError::Conflict("exists".into()));
    }
    ydoc::index_rename_binary(state, vault_id, path, &body.to_path).await?;
    if body.update_embeds {
        crate::notes::rewrite_references_after_move(state, vault_id, None, path, &body.to_path)
            .await;
    }
    state
        .git
        .mark_write(
            vault_id,
            &principal.to_git_principal(now_millis() + 24 * 60 * 60 * 1000),
        )
        .await;
    audit::record(
        state,
        principal,
        vault_id,
        AuditEntry::new("attachment_move", path)
            .to_path(&body.to_path)
            .details(serde_json::json!({
                "hash": meta.hash,
                "size": meta.size,
                "updateEmbeds": body.update_embeds,
            })),
    )
    .await;
    Ok(AttachmentSummary {
        path: body.to_path,
        hash: meta.hash,
        size: meta.size,
    })
}

pub async fn upload_attachment_url(
    State(state): State<AppState>,
    principal: ApiPrincipal,
    Path(vault_id): Path<String>,
    axum::Json(body): axum::Json<UploadFromUrlBody>,
) -> AppResult<axum::Json<UploadAttachmentResponse>> {
    Ok(axum::Json(
        upload_attachment_url_inner(&state, &principal, &vault_id, body).await?,
    ))
}

pub(crate) async fn upload_attachment_url_inner(
    state: &AppState,
    principal: &ApiPrincipal,
    vault_id: &str,
    body: UploadFromUrlBody,
) -> AppResult<UploadAttachmentResponse> {
    principal.require_vault(vault_id)?;
    require_member(state, &principal.user.id, vault_id).await?;
    validate_external_upload_path(state, &body.path)?;
    require_attachment_write(state, principal, vault_id, &body.path).await?;
    let url = validate_source_url(state, &body.source_url)?;
    let res = state
        .http
        .get(url.clone())
        .send()
        .await
        .map_err(|e| AppError::BadRequest(format!("fetch failed: {e}")))?;
    if !res.status().is_success() {
        return Err(AppError::BadRequest(format!(
            "fetch returned {}",
            res.status()
        )));
    }
    if res
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|value| value.to_ascii_lowercase().contains("text/html"))
    {
        return Err(AppError::BadRequest("html_not_allowed".into()));
    }
    let bytes = res
        .bytes()
        .await
        .map_err(|e| AppError::BadRequest(format!("fetch body: {e}")))?;
    if bytes.len() as u64 > state.config.attachment_max_bytes {
        return Err(AppError::PayloadTooLarge);
    }
    reject_html_upload(None, &bytes)?;
    validate_magic_for_path(&body.path, &bytes)?;
    let (hash, size) = store_bytes(state, vault_id, &bytes).await?;
    ydoc::index_set_binary(state, vault_id, &body.path, &hash, size).await?;
    state
        .git
        .mark_write(
            vault_id,
            &principal.to_git_principal(now_millis() + 24 * 60 * 60 * 1000),
        )
        .await;
    audit::record(
        state,
        principal,
        vault_id,
        AuditEntry::new("attachment_upload", &body.path).details(
            serde_json::json!({ "hash": hash, "size": size, "sourceUrl": body.source_url }),
        ),
    )
    .await;
    Ok(UploadAttachmentResponse {
        path: body.path,
        hash,
        size,
    })
}

pub async fn create_upload_link(
    State(state): State<AppState>,
    principal: ApiPrincipal,
    Path(vault_id): Path<String>,
    axum::Json(body): axum::Json<CreateUploadLinkBody>,
) -> AppResult<axum::Json<CreateUploadLinkResponse>> {
    Ok(axum::Json(
        create_upload_link_inner(&state, &principal, &vault_id, body).await?,
    ))
}

pub(crate) async fn create_upload_link_inner(
    state: &AppState,
    principal: &ApiPrincipal,
    vault_id: &str,
    body: CreateUploadLinkBody,
) -> AppResult<CreateUploadLinkResponse> {
    principal.require_vault(vault_id)?;
    require_member(state, &principal.user.id, vault_id).await?;
    let landing_dir = normalize_landing_dir(state, body.landing_dir.as_deref())?;
    let ttl = body
        .expires_in_seconds
        .unwrap_or(15 * 60)
        .clamp(60, 24 * 60 * 60);
    let expires_at = now_millis() + ttl * 1000;
    let payload = UploadTokenPayload {
        jti: uuid::Uuid::new_v4().to_string(),
        vault_id: vault_id.to_string(),
        landing_dir: landing_dir.clone(),
        expires_at,
        principal: UploadPrincipal::from_api(principal),
    };
    let token = sign_payload(state, &payload)?;
    let upload_url = format!(
        "{}/upload?token={}",
        state.config.public_base_url.trim_end_matches('/'),
        token
    );
    Ok(CreateUploadLinkResponse {
        upload_url,
        expires_at,
        landing_dir,
        token,
    })
}

pub async fn public_upload(
    State(state): State<AppState>,
    Query(query): Query<UploadQuery>,
    multipart: Multipart,
) -> AppResult<axum::Json<UploadAttachmentResponse>> {
    Ok(axum::Json(
        public_upload_inner(&state, &query.token, multipart).await?,
    ))
}

async fn public_upload_inner(
    state: &AppState,
    token: &str,
    mut multipart: Multipart,
) -> AppResult<UploadAttachmentResponse> {
    let payload: UploadTokenPayload = verify_payload(state, token)?;
    if payload.expires_at < now_millis() {
        return Err(AppError::Unauthorized);
    }
    require_member(state, &payload.principal.user_id, &payload.vault_id).await?;
    if upload_jtis::Entity::find_by_id(payload.jti.clone())
        .one(&state.db)
        .await?
        .is_some()
    {
        return Err(AppError::Conflict("upload_token_used".into()));
    }

    let mut requested_path = None;
    let mut file_name = None;
    let mut content_type = None;
    let mut bytes = None;
    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|e| AppError::BadRequest(format!("multipart: {e}")))?
    {
        match field.name() {
            Some("path") => {
                requested_path = Some(
                    field
                        .text()
                        .await
                        .map_err(|e| AppError::BadRequest(format!("multipart path: {e}")))?,
                );
            }
            Some("file") => {
                file_name = field.file_name().map(str::to_string);
                content_type = field.content_type().map(str::to_string);
                let data = field
                    .bytes()
                    .await
                    .map_err(|e| AppError::BadRequest(format!("multipart file: {e}")))?;
                if data.len() as u64 > state.config.attachment_max_bytes {
                    return Err(AppError::PayloadTooLarge);
                }
                bytes = Some(data.to_vec());
            }
            _ => {}
        }
    }
    let bytes = bytes.ok_or_else(|| AppError::BadRequest("missing file".into()))?;
    reject_html_upload(content_type.as_deref(), &bytes)?;
    let path = upload_path(
        state,
        &payload.landing_dir,
        requested_path.as_deref(),
        file_name.as_deref(),
    )?;
    validate_external_upload_path(state, &path)?;
    validate_magic_for_path(&path, &bytes)?;

    upload_jtis::ActiveModel {
        jti: Set(payload.jti),
        expires_at: Set(payload.expires_at),
        created_at: Set(now_millis()),
    }
    .insert(&state.db)
    .await?;

    let (hash, size) = store_bytes(state, &payload.vault_id, &bytes).await?;
    ydoc::index_set_binary(state, &payload.vault_id, &path, &hash, size).await?;
    state
        .git
        .mark_write(
            &payload.vault_id,
            &payload.principal.to_principal(payload.expires_at),
        )
        .await;
    Ok(UploadAttachmentResponse { path, hash, size })
}

async fn body_to_bytes(body: Body, max_bytes: u64) -> AppResult<Vec<u8>> {
    let mut bytes = Vec::new();
    let mut stream = body.into_data_stream();
    while let Some(chunk) = stream.next().await {
        let chunk: Bytes =
            chunk.map_err(|e| AppError::BadRequest(format!("upload stream: {e}")))?;
        if bytes.len() as u64 + chunk.len() as u64 > max_bytes {
            return Err(AppError::PayloadTooLarge);
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

async fn attachment_exists(
    state: &AppState,
    principal: &ApiPrincipal,
    vault_id: &str,
    path: &str,
) -> AppResult<bool> {
    match require_attachment(state, principal, vault_id, path).await {
        Ok(_) => Ok(true),
        Err(AppError::NotFound) => Ok(false),
        Err(e) => Err(e),
    }
}

/// Resolve attachment metadata after enforcing vault scope, membership, and
/// path policy. This does not read the blob bytes.
pub(crate) async fn require_attachment(
    state: &AppState,
    principal: &ApiPrincipal,
    vault_id: &str,
    path: &str,
) -> AppResult<AttachmentSummary> {
    principal.require_vault(vault_id)?;
    require_member(state, &principal.user.id, vault_id).await?;
    validate_attachment_path(state, path)?;
    let update = ydoc::read_update(state, vault_id).await?;
    ydoc::decode_binaries_map(&update)
        .map_err(|e| AppError::Internal(e.to_string()))?
        .into_iter()
        .find_map(|(p, meta)| {
            if p == path {
                binary_meta(meta).map(|(hash, size)| AttachmentSummary {
                    path: p,
                    hash,
                    size,
                })
            } else {
                None
            }
        })
        .ok_or(AppError::NotFound)
}

async fn store_bytes(state: &AppState, vault_id: &str, bytes: &[u8]) -> AppResult<(String, i64)> {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let hash = hex_lower(&hasher.finalize());
    let size = bytes.len() as i64;
    let path = blob_path(state, vault_id, &hash)?;
    if tokio::fs::metadata(&path).await.is_err() {
        let dir = path
            .parent()
            .ok_or_else(|| AppError::Internal("blob path has no parent".into()))?;
        tokio::fs::create_dir_all(dir)
            .await
            .map_err(|e| AppError::Internal(format!("blob mkdir: {e}")))?;
        let tmp = dir.join(format!(".tmp-{}", uuid::Uuid::new_v4()));
        let mut file = tokio::fs::File::create(&tmp)
            .await
            .map_err(|e| AppError::Internal(format!("blob create: {e}")))?;
        file.write_all(bytes)
            .await
            .map_err(|e| AppError::Internal(format!("blob write: {e}")))?;
        file.flush()
            .await
            .map_err(|e| AppError::Internal(format!("blob flush: {e}")))?;
        tokio::fs::rename(&tmp, &path)
            .await
            .map_err(|e| AppError::Internal(format!("blob publish: {e}")))?;
    }
    Ok((hash, size))
}

/// SSRF guard for server-side attachment fetches. Layers, in order:
///   1. Default-closed host allowlist (`ATTACHMENT_FETCH_HOST_ALLOWLIST`) — the
///      primary control; nothing is fetchable unless an operator opts the host in.
///   2. IP-literal denylist (private/loopback/link-local/unspecified/ULA) for
///      hosts written as raw IPs.
///   3. HTTPS-only (HTTP permitted solely for loopback dev hosts).
///
/// Redirects are disabled globally (the reqwest client uses `Policy::none()`),
/// so there is no cross-redirect re-validation to perform.
///
/// Residual risk: an allowlisted *DNS name* that resolves to an internal IP
/// (DNS rebinding) is not re-resolved here. This is accepted because the
/// allowlist is operator-controlled and default-closed.
///
/// In `OidcMode::Mock` (tests/dev only; requires `ALLOW_MOCK_OIDC=1`) the
/// IP-literal denylist is intentionally bypassed so the in-process fake fetch
/// source on loopback can be reached. It is never relaxed in production mode.
fn validate_source_url(state: &AppState, raw: &str) -> AppResult<Url> {
    let url = Url::parse(raw).map_err(|_| AppError::BadRequest("invalid source_url".into()))?;
    let host = url
        .host_str()
        .ok_or_else(|| AppError::BadRequest("source_url missing host".into()))?;
    if !state
        .config
        .attachment_fetch_host_allowlist
        .iter()
        .any(|allowed| allowed == host)
    {
        return Err(AppError::Forbidden);
    }
    if let Ok(ip) = host.parse::<IpAddr>() {
        let denied = match ip {
            IpAddr::V4(ip) => {
                ip.is_private() || ip.is_loopback() || ip.is_link_local() || ip.is_unspecified()
            }
            IpAddr::V6(ip) => ip.is_loopback() || ip.is_unspecified() || ip.is_unique_local(),
        };
        // Block internal/loopback IP literals — except in mock mode, where the
        // in-process fake fetch source lives on loopback (see fn doc comment).
        if denied && state.config.oidc_mode != OidcMode::Mock {
            return Err(AppError::Forbidden);
        }
    }
    let loopback = matches!(host, "localhost" | "127.0.0.1" | "::1");
    if url.scheme() != "https" && !(url.scheme() == "http" && loopback) {
        return Err(AppError::BadRequest("source_url must be https".into()));
    }
    Ok(url)
}

fn binary_meta(value: Any) -> Option<(String, i64)> {
    let Any::Map(map) = value else { return None };
    let hash = match map.get("hash")? {
        Any::String(value) => value.to_string(),
        _ => return None,
    };
    let size = match map.get("size")? {
        Any::Number(value) => *value as i64,
        Any::BigInt(value) => *value,
        _ => return None,
    };
    Some((hash, size))
}

fn validate_attachment_path(state: &AppState, path: &str) -> AppResult<()> {
    validate_attachment_components(path)?;
    if state.config.attachments_path_mode != "relative"
        && state.config.attachments_path_mode != "subfolder"
    {
        return Err(AppError::Internal("invalid attachments path mode".into()));
    }
    if state.config.attachments_path_mode == "subfolder" {
        let subfolder = state
            .config
            .attachments_subfolder
            .as_deref()
            .ok_or_else(|| AppError::Internal("attachments_subfolder required".into()))?;
        let prefix = format!("{}/", subfolder.trim_matches('/'));
        if !path.starts_with(&prefix) {
            return Err(AppError::BadRequest(
                "attachment path must be inside attachments subfolder".into(),
            ));
        }
    }
    Ok(())
}

fn validate_external_upload_path(state: &AppState, path: &str) -> AppResult<()> {
    validate_attachment_path(state, path)?;
    if !attachment_extension_allowed(&state.config.attachment_allowed_extensions, path) {
        return Err(AppError::BadRequest(
            "attachment extension not allowed".into(),
        ));
    }
    Ok(())
}

fn attachment_extension_allowed(allowed_extensions: &[String], path: &str) -> bool {
    if allowed_extensions.iter().any(|allowed| allowed == "*") {
        return true;
    }
    let Some((_, ext)) = path.rsplit_once('.') else {
        return false;
    };
    !ext.is_empty()
        && allowed_extensions
            .iter()
            .any(|allowed| allowed == &ext.to_ascii_lowercase())
}

fn blob_path(state: &AppState, vault_id: &str, hash: &str) -> AppResult<PathBuf> {
    if hash.len() != 64
        || !hash
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(AppError::BadRequest("invalid blob hash".into()));
    }
    if vault_id.is_empty()
        || vault_id.contains('/')
        || vault_id.contains('\\')
        || vault_id.contains("..")
    {
        return Err(AppError::BadRequest("invalid vault id".into()));
    }
    Ok(PathBuf::from(&state.config.blob_dir)
        .join(vault_id)
        .join(hash))
}

fn normalize_landing_dir(state: &AppState, landing_dir: Option<&str>) -> AppResult<String> {
    let dir = landing_dir
        .or(state.config.attachments_subfolder.as_deref())
        .unwrap_or("")
        .trim_matches('/');
    if dir.is_empty() {
        return Ok(String::new());
    }
    validate_attachment_components(dir)?;
    Ok(dir.to_string())
}

fn upload_path(
    state: &AppState,
    landing_dir: &str,
    requested_path: Option<&str>,
    file_name: Option<&str>,
) -> AppResult<String> {
    let name = requested_path
        .filter(|p| !p.trim().is_empty())
        .or(file_name)
        .ok_or_else(|| AppError::BadRequest("missing upload path".into()))?
        .trim_matches('/');
    validate_attachment_components(name)?;
    let path = if landing_dir.is_empty() || name.starts_with(&format!("{landing_dir}/")) {
        name.to_string()
    } else {
        format!("{landing_dir}/{name}")
    };
    if state.config.attachments_path_mode == "subfolder" {
        let subfolder = state
            .config
            .attachments_subfolder
            .as_deref()
            .unwrap_or_default()
            .trim_matches('/');
        if !subfolder.is_empty() && !path.starts_with(&format!("{subfolder}/")) {
            return Err(AppError::BadRequest(
                "upload path must be inside attachments subfolder".into(),
            ));
        }
    }
    Ok(path)
}

fn validate_attachment_components(path: &str) -> AppResult<()> {
    if path.is_empty() || path.contains('\\') || path.ends_with(".md") {
        return Err(AppError::BadRequest("invalid attachment path".into()));
    }
    for component in path.split('/') {
        if component.is_empty() || component == "." || component == ".." {
            return Err(AppError::BadRequest("invalid attachment path".into()));
        }
    }
    Ok(())
}

fn reject_html_upload(content_type: Option<&str>, bytes: &[u8]) -> AppResult<()> {
    if content_type.is_some_and(|value| value.to_ascii_lowercase().contains("text/html"))
        || std::str::from_utf8(&bytes[..bytes.len().min(4096)])
            .ok()
            .is_some_and(|text| text.to_ascii_lowercase().contains("<html"))
    {
        return Err(AppError::BadRequest("html_not_allowed".into()));
    }
    Ok(())
}

fn validate_magic_for_path(path: &str, bytes: &[u8]) -> AppResult<()> {
    let ext = path
        .rsplit('.')
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase();
    let ok = match ext.as_str() {
        "png" => bytes.starts_with(b"\x89PNG\r\n\x1a\n"),
        "jpg" | "jpeg" => bytes.starts_with(&[0xff, 0xd8, 0xff]),
        "gif" => bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a"),
        "webp" => bytes.len() >= 12 && bytes.starts_with(b"RIFF") && &bytes[8..12] == b"WEBP",
        "pdf" => bytes.starts_with(b"%PDF-"),
        "svg" => svg_upload_allowed(bytes),
        _ => true,
    };
    if ok {
        Ok(())
    } else {
        Err(AppError::BadRequest(
            "file content does not match extension".into(),
        ))
    }
}

fn svg_upload_allowed(bytes: &[u8]) -> bool {
    let Ok(text) = std::str::from_utf8(bytes) else {
        return false;
    };
    let lower = text.to_ascii_lowercase();
    lower.contains("<svg") && !lower.contains("<script") && !lower.contains("javascript:")
}

fn attachment_file_response(path: &str, size: i64, body: Body) -> Response {
    Response::builder()
        .header(header::CONTENT_TYPE, content_type_for_path(path))
        .header(header::CONTENT_LENGTH, size.to_string())
        .header(header::X_CONTENT_TYPE_OPTIONS, "nosniff")
        .header(header::CONTENT_SECURITY_POLICY, "sandbox")
        .body(body)
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

async fn require_attachment_write(
    state: &AppState,
    principal: &ApiPrincipal,
    vault_id: &str,
    path: &str,
) -> AppResult<()> {
    let level = authorize_path(state, &principal.user, vault_id, path).await?;
    if level == Level::ReadOnly {
        return Err(AppError::Forbidden);
    }
    Ok(())
}

impl UploadPrincipal {
    fn from_api(principal: &ApiPrincipal) -> Self {
        match &principal.actor {
            crate::session::ApiActor::User => Self {
                user_id: principal.user.id.clone(),
                display_name: principal.user.display_name.clone(),
                email: principal.user.email.clone(),
                cursor_id: None,
                app_id: None,
                cursor_name: None,
            },
            crate::session::ApiActor::Cursor(cursor) => Self {
                user_id: principal.user.id.clone(),
                display_name: principal.user.display_name.clone(),
                email: principal.user.email.clone(),
                cursor_id: Some(cursor.id.clone()),
                app_id: Some(cursor.app_id.clone()),
                cursor_name: Some(cursor.name.clone()),
            },
        }
    }

    fn to_principal(&self, expires_at_ms: i64) -> Principal {
        let actor = match (&self.cursor_id, &self.app_id, &self.cursor_name) {
            (Some(cursor_id), Some(app_id), Some(cursor_name)) => PrincipalActor::Cursor {
                cursor_id: cursor_id.clone(),
                app_id: app_id.clone(),
                cursor_name: cursor_name.clone(),
            },
            _ => PrincipalActor::User,
        };
        Principal {
            user_id: self.user_id.clone(),
            display_name: self.display_name.clone(),
            email: self.email.clone(),
            git_email: None,
            actor,
            expires_at_ms,
        }
    }
}

fn sign_payload<T: Serialize>(state: &AppState, payload: &T) -> AppResult<String> {
    let payload = serde_json::to_vec(payload)
        .map_err(|e| AppError::Internal(format!("upload token serialize: {e}")))?;
    let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&payload);
    let sig = sign_bytes(state, encoded.as_bytes())?;
    Ok(format!("{encoded}.{sig}"))
}

fn verify_payload<T: DeserializeOwned>(state: &AppState, token: &str) -> AppResult<T> {
    let (payload, sig) = token.split_once('.').ok_or(AppError::Unauthorized)?;
    let expected = sign_bytes(state, payload.as_bytes())?;
    if !constant_time_eq(expected.as_bytes(), sig.as_bytes()) {
        return Err(AppError::Unauthorized);
    }
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload.as_bytes())
        .map_err(|_| AppError::Unauthorized)?;
    serde_json::from_slice(&bytes).map_err(|_| AppError::Unauthorized)
}

fn sign_bytes(state: &AppState, bytes: &[u8]) -> AppResult<String> {
    let mut mac = Hmac::<Sha256>::new_from_slice(state.config.upload_token.as_bytes())
        .map_err(|e| AppError::Internal(format!("upload hmac: {e}")))?;
    mac.update(bytes);
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes()))
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (a, b)| acc | (a ^ b)) == 0
}

pub(crate) fn content_type_for_path(path: &str) -> &'static str {
    match path
        .rsplit('.')
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "svg" => "image/svg+xml",
        "pdf" => "application/pdf",
        _ => "application/octet-stream",
    }
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

#[cfg(test)]
mod tests {
    use super::attachment_extension_allowed;

    #[test]
    fn wildcard_allows_arbitrary_and_extensionless_vault_files() {
        let allowed = vec!["*".to_string()];
        assert!(attachment_extension_allowed(&allowed, "data/config.json"));
        assert!(attachment_extension_allowed(&allowed, "assets/archive.zip"));
        assert!(attachment_extension_allowed(&allowed, "LICENSE"));
    }

    #[test]
    fn explicit_extension_allowlist_remains_restrictive() {
        let allowed = vec!["png".to_string(), "pdf".to_string()];
        assert!(attachment_extension_allowed(&allowed, "image.PNG"));
        assert!(!attachment_extension_allowed(&allowed, "data.json"));
        assert!(!attachment_extension_allowed(&allowed, "LICENSE"));
    }
}
