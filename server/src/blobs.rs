//! Content-addressed binary blob store.
//!
//! Markdown files sync through Yjs, but binary attachments (images, PDFs, …)
//! would bloat the text CRDT. Instead the plugin syncs only a `path -> sha256`
//! mapping through the index doc and stores the bytes here, keyed by their hash.
//!
//! Blobs live on the filesystem under `{blob_dir}/{vault_id}/{hash}`, alongside
//! the CRDT snapshots. Access is authorized against the attachment/config path
//! supplied by the client. Legacy unscoped requests are accepted only when the
//! principal has one uniform permission level across the entire vault.
//! Uploads are content-verified — the streamed bytes must hash to the claimed
//! `hash` — so a member cannot poison a hash with mismatched content.

use std::path::{Path as FsPath, PathBuf};

use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use futures_util::StreamExt;
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;
use tokio_util::io::ReaderStream;
use yrs::{Any, Map, Out, ReadTxn, Transact};

use crate::crdt::Level;
use crate::error::{AppError, AppResult};
use crate::routes::{authorize_path, authorize_uniform_vault, require_member};
use crate::session::{now_millis, AuthUser};
use crate::state::{AppState, Principal, PrincipalActor};

pub const MAX_BLOB_BYTES: u64 = 100 * 1024 * 1024;

/// A sha256 hex digest is exactly 64 lowercase hex characters. Validating this
/// before building any path is what keeps `hash` from escaping the blob dir.
pub(crate) fn valid_hash(hash: &str) -> bool {
    hash.len() == 64
        && hash
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// Resolve the on-disk path for a blob, after validating both segments.
pub(crate) fn blob_fs_path(blob_dir: &str, vault_id: &str, hash: &str) -> Result<PathBuf, String> {
    if !valid_hash(hash) {
        return Err("invalid blob hash".into());
    }
    // vault_id is a server-issued UUID, but guard against separators regardless.
    if vault_id.is_empty()
        || vault_id.contains('/')
        || vault_id.contains('\\')
        || vault_id.contains("..")
    {
        return Err("invalid vault id".into());
    }
    let mut p = PathBuf::from(blob_dir);
    p.push(vault_id);
    p.push(hash);
    Ok(p)
}

fn blob_path(state: &AppState, vault_id: &str, hash: &str) -> AppResult<PathBuf> {
    blob_fs_path(&state.config.blob_dir, vault_id, hash).map_err(AppError::BadRequest)
}

#[derive(Default, serde::Deserialize)]
pub struct BlobQuery {
    path: Option<String>,
}

async fn authorize_blob(
    state: &AppState,
    user: &crate::entities::users::Model,
    vault_id: &str,
    path: Option<&str>,
    write: bool,
) -> AppResult<()> {
    require_member(state, &user.id, vault_id).await?;
    let level = match path {
        Some(path) if !path.is_empty() => authorize_path(state, user, vault_id, path).await?,
        Some(_) => return Err(AppError::BadRequest("blob path must not be empty".into())),
        None => authorize_uniform_vault(state, user, vault_id).await?,
    };
    if write && level != Level::Full {
        return Err(AppError::Forbidden);
    }
    Ok(())
}

async fn blob_matches_path(
    state: &AppState,
    vault_id: &str,
    path: &str,
    hash: &str,
) -> AppResult<bool> {
    // Point lookup in the live index doc. Encoding and re-decoding the whole
    // index per request cost O(vault size) on every blob download.
    state
        .documents
        .read_with(vault_id, |doc| {
            let txn = doc.transact();
            ["binaries", "configFiles"].into_iter().any(|name| {
                let Some(map) = txn.get_map(name) else {
                    return false;
                };
                matches!(
                    map.get(&txn, path),
                    Some(Out::Any(Any::Map(meta)))
                        if matches!(meta.get("hash"), Some(Any::String(value)) if value.as_ref() == hash)
                )
            })
        })
        .await
        .map_err(AppError::from)
}

/// `HEAD /api/vaults/{id}/blobs/{hash}` — 200 if present, 404 otherwise. Lets the
/// client skip an upload when the server already has the content (dedup).
pub async fn head_blob(
    State(state): State<AppState>,
    AuthUser(user): AuthUser,
    Path((vault_id, hash)): Path<(String, String)>,
    Query(query): Query<BlobQuery>,
) -> AppResult<StatusCode> {
    authorize_blob(&state, &user, &vault_id, query.path.as_deref(), false).await?;
    let path = blob_path(&state, &vault_id, &hash)?;
    match tokio::fs::metadata(&path).await {
        Ok(_) => Ok(StatusCode::OK),
        Err(_) => Err(AppError::NotFound),
    }
}

/// `GET /api/vaults/{id}/blobs/{hash}` — stream the blob bytes back.
pub async fn get_blob(
    State(state): State<AppState>,
    AuthUser(user): AuthUser,
    Path((vault_id, hash)): Path<(String, String)>,
    Query(query): Query<BlobQuery>,
) -> AppResult<Response> {
    authorize_blob(&state, &user, &vault_id, query.path.as_deref(), false).await?;
    if let Some(path) = query.path.as_deref() {
        if !blob_matches_path(&state, &vault_id, path, &hash).await? {
            return Err(AppError::NotFound);
        }
    }
    let path = blob_path(&state, &vault_id, &hash)?;

    let file = tokio::fs::File::open(&path)
        .await
        .map_err(|_| AppError::NotFound)?;
    let stream = ReaderStream::new(file);
    let body = Body::from_stream(stream);

    Ok(Response::builder()
        .header(axum::http::header::CONTENT_TYPE, "application/octet-stream")
        .body(body)
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response()))
}

/// `PUT /api/vaults/{id}/blobs/{hash}` — stream the request body to disk, verify
/// it hashes to `hash`, then atomically publish it. Idempotent: an already-present
/// blob is accepted without rewriting (the body is still drained).
pub async fn put_blob(
    State(state): State<AppState>,
    AuthUser(user): AuthUser,
    Path((vault_id, hash)): Path<(String, String)>,
    Query(query): Query<BlobQuery>,
    body: Body,
) -> AppResult<StatusCode> {
    authorize_blob(&state, &user, &vault_id, query.path.as_deref(), true).await?;
    let path = blob_path(&state, &vault_id, &hash)?;

    // Already stored: short-circuit. Content addressing makes this safe.
    if tokio::fs::metadata(&path).await.is_ok() {
        return Ok(StatusCode::OK);
    }

    let dir = path
        .parent()
        .ok_or_else(|| AppError::Internal("blob path has no parent".into()))?;
    tokio::fs::create_dir_all(dir)
        .await
        .map_err(|e| AppError::Internal(format!("blob mkdir: {e}")))?;

    // Stream to a unique temp file in the same dir (so the rename is atomic),
    // hashing as we go.
    let tmp = dir.join(format!(".tmp-{}", uuid::Uuid::new_v4()));
    let actual = match stream_to_file(body, &tmp).await {
        Ok(h) => h,
        Err(e) => {
            let _ = tokio::fs::remove_file(&tmp).await;
            return Err(e);
        }
    };

    if actual != hash {
        let _ = tokio::fs::remove_file(&tmp).await;
        return Err(AppError::Conflict(format!(
            "content hash {actual} does not match {hash}"
        )));
    }

    // Atomic publish. A concurrent uploader of the same content is harmless: the
    // rename just replaces an identical file.
    tokio::fs::rename(&tmp, &path)
        .await
        .map_err(|e| AppError::Internal(format!("blob publish: {e}")))?;

    // A new blob may resolve a shim that git backup committed as a fallback
    // while the bytes were missing, so nudge a re-commit.
    let principal = Principal {
        user_id: user.id.clone(),
        display_name: user.display_name.clone(),
        email: user.email.clone(),
        git_email: user.git_email.clone(),
        actor: PrincipalActor::User,
        expires_at_ms: now_millis() + 24 * 60 * 60 * 1000,
    };
    state.git.mark_write(&vault_id, &principal).await;

    Ok(StatusCode::OK)
}

/// Stream `body` into `tmp`, returning the lowercase hex sha256 of the bytes.
async fn stream_to_file(body: Body, tmp: &FsPath) -> AppResult<String> {
    let mut file = tokio::fs::File::create(tmp)
        .await
        .map_err(|e| AppError::Internal(format!("blob create: {e}")))?;
    let mut hasher = Sha256::new();
    let mut stream = body.into_data_stream();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| AppError::BadRequest(format!("upload stream: {e}")))?;
        if file.metadata().await.map(|m| m.len()).unwrap_or(0) + chunk.len() as u64 > MAX_BLOB_BYTES
        {
            return Err(AppError::PayloadTooLarge);
        }
        hasher.update(&chunk);
        file.write_all(&chunk)
            .await
            .map_err(|e| AppError::Internal(format!("blob write: {e}")))?;
    }
    file.flush()
        .await
        .map_err(|e| AppError::Internal(format!("blob flush: {e}")))?;

    Ok(hex_lower(&hasher.finalize()))
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
    use super::*;

    #[test]
    fn valid_hash_accepts_64_hex() {
        assert!(valid_hash(&"a".repeat(64)));
        assert!(valid_hash(
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        ));
    }

    #[test]
    fn valid_hash_rejects_bad_input() {
        assert!(!valid_hash(""));
        assert!(!valid_hash(&"a".repeat(63)));
        assert!(!valid_hash(&"a".repeat(65)));
        assert!(!valid_hash(&"A".repeat(64))); // uppercase
        assert!(!valid_hash("../../etc/passwd"));
        assert!(!valid_hash(&"g".repeat(64))); // non-hex
    }
}
