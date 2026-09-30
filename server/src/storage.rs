//! Per-vault storage accounting + orphaned-blob cleanup.
//!
//! Surfaces the storage a vault occupies on the server, split into three
//! buckets for the plugin's "Storage Management" panel:
//!  - **current** binary attachments — blobs referenced by the live `binaries`
//!    or `configFiles` index maps;
//!  - **previous** binary attachments — orphaned blobs no longer referenced by
//!    the live map (older versions and the content behind trashed/deleted files);
//!  - **plain vault** — native persisted CRDT generations for this vault.
//!
//! The cleanup endpoint deletes orphaned ("previous") blobs. It deliberately
//! never touches blobs referenced by the live map, but removing previous
//! versions does forfeit the ability to restore those older/deleted versions —
//! the plugin warns the user before calling it.

use std::collections::HashSet;
use std::path::PathBuf;

use axum::extract::{Path, Query, State};
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::crdt::Level;
use crate::error::{AppError, AppResult};
use crate::routes::{authorize_uniform_vault, require_admin, require_member};
use crate::session::AuthUser;
use crate::state::AppState;
use crate::ydoc;

/// Orphaned blobs newer than this are kept. A client uploads a blob before it
/// publishes the index entry that references it, and a re-upload of existing
/// content only refreshes the file's mtime, so a young "orphan" is usually a
/// publish still in flight.
const BLOB_GC_GRACE: std::time::Duration = std::time::Duration::from_secs(60 * 60);

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StorageUsage {
    pub blobs_current_bytes: u64,
    pub blobs_previous_bytes: u64,
    pub current_blob_count: u64,
    pub previous_blob_count: u64,
    /// `None` when the native CRDT store cannot be read.
    pub plain_vault_bytes: Option<u64>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GcBlobsBody {
    /// Only delete orphaned blobs at least this large (bytes). Defaults to 0.
    pub min_bytes: Option<u64>,
    /// Only delete orphaned blobs written at least this long ago (seconds).
    /// Defaults to an hour, so uploads whose index entry is still on its way
    /// are not reclaimed.
    pub min_age_seconds: Option<u64>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GcBlobsResult {
    pub removed: u64,
    pub freed_bytes: u64,
}

/// A sha256 hex digest is exactly 64 lowercase hex characters. Filters out
/// in-flight `.tmp-*` files and anything that isn't a content blob.
fn is_blob_name(name: &str) -> bool {
    name.len() == 64
        && name
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn vault_blob_dir(state: &AppState, vault_id: &str) -> PathBuf {
    let mut p = PathBuf::from(&state.config.blob_dir);
    p.push(vault_id);
    p
}

/// Hashes referenced by the live `binaries` and `configFiles` index maps (the
/// "current" set). Config-folder files share the attachment blob store, so
/// they must count as live or GC would delete them out from under peers.
async fn live_blob_hashes(state: &AppState, vault_id: &str) -> AppResult<HashSet<String>> {
    let current = ydoc::read_update(state, vault_id).await?;
    let mut hashes = HashSet::new();
    let entries = ydoc::decode_binaries_map(&current)
        .map_err(|e| crate::error::AppError::Internal(e.to_string()))?;
    for (_path, value) in entries {
        if let yrs::Any::Map(meta) = value {
            if let Some(yrs::Any::String(hash)) = meta.get("hash") {
                hashes.insert(hash.to_string());
            }
        }
    }
    let config_entries = ydoc::decode_config_entries(&current)
        .map_err(|e| crate::error::AppError::Internal(e.to_string()))?;
    for entry in config_entries {
        hashes.insert(entry.hash);
    }
    Ok(hashes)
}

/// `GET /api/vaults/{id}/storage` — admin-only storage breakdown for the vault.
pub async fn get_storage(
    State(state): State<AppState>,
    AuthUser(user): AuthUser,
    Path(vault_id): Path<String>,
) -> AppResult<Json<StorageUsage>> {
    require_admin(&state, &user.id, &vault_id).await?;

    let live = live_blob_hashes(&state, &vault_id).await?;

    let mut current_bytes = 0u64;
    let mut previous_bytes = 0u64;
    let mut current_count = 0u64;
    let mut previous_count = 0u64;

    let dir = vault_blob_dir(&state, &vault_id);
    if let Ok(mut rd) = tokio::fs::read_dir(&dir).await {
        while let Ok(Some(entry)) = rd.next_entry().await {
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if !is_blob_name(name) {
                continue;
            }
            let Ok(meta) = entry.metadata().await else {
                continue;
            };
            if !meta.is_file() {
                continue;
            }
            let len = meta.len();
            if live.contains(name) {
                current_bytes += len;
                current_count += 1;
            } else {
                previous_bytes += len;
                previous_count += 1;
            }
        }
    }

    let plain_vault_bytes = plain_vault_bytes(&state, &vault_id).await;

    Ok(Json(StorageUsage {
        blobs_current_bytes: current_bytes,
        blobs_previous_bytes: previous_bytes,
        current_blob_count: current_count,
        previous_blob_count: previous_count,
        plain_vault_bytes,
    }))
}

/// Blob hashes still referenced by `trash` entries (recoverable deletions),
/// keyed by entry id.
async fn trashed_blob_hashes(state: &AppState, vault_id: &str) -> AppResult<Vec<(String, String)>> {
    use yrs::{Any, Map, Out, ReadTxn, Transact};
    state
        .documents
        .read_with(vault_id, |doc| {
            let txn = doc.transact();
            let Some(trash) = txn.get_map("trash") else {
                return Vec::new();
            };
            trash
                .iter(&txn)
                .filter_map(|(id, value)| {
                    let Out::Any(Any::Map(entry)) = value else {
                        return None;
                    };
                    match entry.get("hash") {
                        Some(Any::String(hash)) => Some((id.to_string(), hash.to_string())),
                        _ => None,
                    }
                })
                .collect()
        })
        .await
        .map_err(AppError::from)
}

/// True when a blob file was written (or re-uploaded) within `grace`.
fn written_within(meta: &std::fs::Metadata, grace: std::time::Duration) -> bool {
    meta.modified()
        .ok()
        .and_then(|modified| modified.elapsed().ok())
        .is_some_and(|age| age < grace)
}

/// `POST /api/vaults/{id}/storage/gc-blobs` — delete orphaned ("previous") blobs.
pub async fn gc_blobs(
    State(state): State<AppState>,
    AuthUser(user): AuthUser,
    Path(vault_id): Path<String>,
    Json(body): Json<GcBlobsBody>,
) -> AppResult<Json<GcBlobsResult>> {
    require_admin(&state, &user.id, &vault_id).await?;

    let live = live_blob_hashes(&state, &vault_id).await?;
    let min_bytes = body.min_bytes.unwrap_or(0);
    let grace = body
        .min_age_seconds
        .map_or(BLOB_GC_GRACE, std::time::Duration::from_secs);

    let mut removed = 0u64;
    let mut freed_bytes = 0u64;

    let dir = vault_blob_dir(&state, &vault_id);
    if let Ok(mut rd) = tokio::fs::read_dir(&dir).await {
        while let Ok(Some(entry)) = rd.next_entry().await {
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if !is_blob_name(name) || live.contains(name) {
                continue;
            }
            let Ok(meta) = entry.metadata().await else {
                continue;
            };
            if !meta.is_file() || meta.len() < min_bytes || written_within(&meta, grace) {
                continue;
            }
            let len = meta.len();
            if tokio::fs::remove_file(entry.path()).await.is_ok() {
                removed += 1;
                freed_bytes += len;
            }
        }
    }

    Ok(Json(GcBlobsResult {
        removed,
        freed_bytes,
    }))
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeleteBlobResult {
    pub deleted: bool,
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeleteBlobQuery {
    /// The trash entry being permanently deleted; its own reference to the
    /// blob does not keep it alive.
    pub trash_entry: Option<String>,
}

/// `DELETE /api/vaults/{id}/blobs/{hash}` — reclaim a single orphaned blob (used
/// when permanently deleting a trashed attachment). Refuses to delete a blob that
/// is still referenced by the live `binaries` map or by another trash entry, or
/// that was written recently (an upload whose index entry is still in flight).
/// Idempotent: a missing blob reports `deleted: true`.
pub async fn delete_blob(
    State(state): State<AppState>,
    AuthUser(user): AuthUser,
    Path((vault_id, hash)): Path<(String, String)>,
    Query(query): Query<DeleteBlobQuery>,
) -> AppResult<Json<DeleteBlobResult>> {
    require_member(&state, &user.id, &vault_id).await?;
    if authorize_uniform_vault(&state, &user, &vault_id).await? != Level::Full {
        return Err(AppError::Forbidden);
    }
    if !is_blob_name(&hash) {
        return Err(AppError::BadRequest("invalid blob hash".into()));
    }
    let live = live_blob_hashes(&state, &vault_id).await?;
    if live.contains(&hash) {
        return Ok(Json(DeleteBlobResult { deleted: false }));
    }
    let still_trashed = trashed_blob_hashes(&state, &vault_id)
        .await?
        .into_iter()
        .any(|(id, trashed)| trashed == hash && query.trash_entry.as_deref() != Some(id.as_str()));
    if still_trashed {
        return Ok(Json(DeleteBlobResult { deleted: false }));
    }
    let mut path = vault_blob_dir(&state, &vault_id);
    path.push(&hash);
    if tokio::fs::metadata(&path)
        .await
        .is_ok_and(|meta| written_within(&meta, BLOB_GC_GRACE))
    {
        return Ok(Json(DeleteBlobResult { deleted: false }));
    }
    match tokio::fs::remove_file(&path).await {
        Ok(_) => Ok(Json(DeleteBlobResult { deleted: true })),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            Ok(Json(DeleteBlobResult { deleted: true }))
        }
        Err(e) => Err(AppError::Internal(format!("blob delete: {e}"))),
    }
}

/// Sum the on-disk size of this vault's native CRDT generations.
async fn plain_vault_bytes(state: &AppState, vault_id: &str) -> Option<u64> {
    state
        .documents
        .document_bytes_for_vault(vault_id)
        .await
        .ok()
}
