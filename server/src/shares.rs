//! Public read-only note sharing.
//!
//! A share row maps a nanoid to a `(vault_id, guid)` pair; the SPA served at
//! `/view/{id}` fetches the note snapshot from `GET /api/view/{id}` and then
//! listens on `GET /api/view/{id}/events` (SSE) for incremental Yjs updates,
//! which it applies to a local `Y.Doc` and re-renders. Diffs are computed
//! server-side by polling the native document snapshot and encoding state since
//! the previously observed state vector, so clients receive minimal
//! CRDT-correct updates rather than text diffs.

use std::convert::Infallible;
use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::Response;
use axum::Json;
use base64::Engine;
use futures_util::stream::Stream;
use futures_util::StreamExt;
use sea_orm::{ActiveModelTrait, ColumnTrait, EntityTrait, QueryFilter, Set};
use serde::{Deserialize, Serialize};
use yrs::{Doc, ReadTxn, StateVector, Transact, Update};

use crate::attachments;
use crate::entities::{public_attachment_shares, public_shares, vault_files};
use crate::error::{AppError, AppResult};
use crate::routes::require_member;
use crate::session::{now_millis, ApiPrincipal};
use crate::state::AppState;
use crate::ydoc;

/// How often an SSE connection polls the document store for changes.
const POLL_INTERVAL: Duration = Duration::from_millis(1500);

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ShareResponse {
    pub id: String,
    pub url: String,
    pub path: String,
    pub guid: String,
    pub created_at: i64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AttachmentShareResponse {
    pub id: String,
    pub url: String,
    pub path: String,
    pub hash: String,
    pub size: i64,
    pub created_at: i64,
}

#[derive(Deserialize)]
pub struct CreateShareBody {
    pub path: String,
}

#[derive(Deserialize)]
pub struct SharePathQuery {
    pub path: String,
}

#[derive(Deserialize)]
pub struct ViewEventsQuery {
    pub epoch: Option<u64>,
}

fn initial_snapshot_event(
    _client_epoch: Option<u64>,
    stream_epoch: u64,
    update: &[u8],
) -> Result<Event, Infallible> {
    let payload = serde_json::json!({
        "epoch": stream_epoch,
        "update": base64::engine::general_purpose::STANDARD.encode(update),
    });
    Ok(Event::default().event("snapshot").data(payload.to_string()))
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ViewResponse {
    pub title: String,
    pub path: String,
    pub epoch: u64,
    /// Full Yjs document state (base64, v1 encoding); the note text lives in
    /// the root `Y.Text("contents")`.
    pub update_b64: String,
    pub updated_at: i64,
}

#[derive(Deserialize)]
pub struct ResolveQuery {
    pub target: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResolveResponse {
    pub share_id: String,
    pub title: String,
    pub path: String,
}

fn doc_id(vault_id: &str, guid: &str) -> String {
    format!("{vault_id}__{guid}")
}

fn share_url(state: &AppState, id: &str) -> String {
    format!(
        "{}/view/{id}",
        state.config.public_base_url.trim_end_matches('/')
    )
}

fn attachment_share_url(state: &AppState, id: &str) -> String {
    format!(
        "{}/a/{id}",
        state.config.public_base_url.trim_end_matches('/')
    )
}

fn title_for_path(path: &str) -> String {
    let name = path.rsplit('/').next().unwrap_or(path);
    name.strip_suffix(".md").unwrap_or(name).to_string()
}

async fn file_by_path(
    state: &AppState,
    vault_id: &str,
    path: &str,
) -> AppResult<Option<vault_files::Model>> {
    Ok(vault_files::Entity::find()
        .filter(vault_files::Column::VaultId.eq(vault_id))
        .filter(vault_files::Column::Path.eq(path))
        .one(&state.db)
        .await?)
}

async fn share_by_guid(
    state: &AppState,
    vault_id: &str,
    guid: &str,
) -> AppResult<Option<public_shares::Model>> {
    Ok(public_shares::Entity::find()
        .filter(public_shares::Column::VaultId.eq(vault_id))
        .filter(public_shares::Column::Guid.eq(guid))
        .one(&state.db)
        .await?)
}

async fn attachment_share_by_path(
    state: &AppState,
    vault_id: &str,
    path: &str,
) -> AppResult<Option<public_attachment_shares::Model>> {
    Ok(public_attachment_shares::Entity::find()
        .filter(public_attachment_shares::Column::VaultId.eq(vault_id))
        .filter(public_attachment_shares::Column::Path.eq(path))
        .one(&state.db)
        .await?)
}

fn attachment_share_response(
    state: &AppState,
    share: public_attachment_shares::Model,
) -> AttachmentShareResponse {
    AttachmentShareResponse {
        url: attachment_share_url(state, &share.id),
        id: share.id,
        path: share.path,
        hash: share.hash,
        size: share.size,
        created_at: share.created_at,
    }
}

/// Look up a live share and the current path of the note it points at.
/// Returns 404 if the share is gone or its note no longer exists.
async fn resolve_share(
    state: &AppState,
    share_id: &str,
) -> AppResult<(public_shares::Model, vault_files::Model)> {
    let share = public_shares::Entity::find_by_id(share_id.to_string())
        .one(&state.db)
        .await?
        .ok_or(AppError::NotFound)?;
    let file = vault_files::Entity::find()
        .filter(vault_files::Column::VaultId.eq(&share.vault_id))
        .filter(vault_files::Column::Guid.eq(&share.guid))
        .one(&state.db)
        .await?
        .ok_or(AppError::NotFound)?;
    Ok((share, file))
}

// ---------------------------------------------------------------------------
// Authenticated share management
// ---------------------------------------------------------------------------

pub async fn create_share(
    State(state): State<AppState>,
    principal: ApiPrincipal,
    Path(vault_id): Path<String>,
    Json(body): Json<CreateShareBody>,
) -> AppResult<Json<ShareResponse>> {
    principal.require_vault(&vault_id)?;
    require_member(&state, &principal.user.id, &vault_id).await?;
    let file = file_by_path(&state, &vault_id, &body.path)
        .await?
        .ok_or(AppError::NotFound)?;

    // Idempotent: re-sharing an already-shared note returns the existing link.
    if let Some(existing) = share_by_guid(&state, &vault_id, &file.guid).await? {
        return Ok(Json(ShareResponse {
            url: share_url(&state, &existing.id),
            id: existing.id,
            path: file.path,
            guid: file.guid,
            created_at: existing.created_at,
        }));
    }

    let id = nanoid::nanoid!(16, &nanoid::alphabet::SAFE);
    let created_at = now_millis();
    public_shares::ActiveModel {
        id: Set(id.clone()),
        vault_id: Set(vault_id.clone()),
        guid: Set(file.guid.clone()),
        created_by: Set(principal.user.id.clone()),
        created_at: Set(created_at),
    }
    .insert(&state.db)
    .await?;

    Ok(Json(ShareResponse {
        url: share_url(&state, &id),
        id,
        path: file.path,
        guid: file.guid,
        created_at,
    }))
}

pub async fn get_share(
    State(state): State<AppState>,
    principal: ApiPrincipal,
    Path(vault_id): Path<String>,
    Query(query): Query<SharePathQuery>,
) -> AppResult<Json<serde_json::Value>> {
    principal.require_vault(&vault_id)?;
    require_member(&state, &principal.user.id, &vault_id).await?;
    let Some(file) = file_by_path(&state, &vault_id, &query.path).await? else {
        return Ok(Json(serde_json::json!({ "share": null })));
    };
    let share = share_by_guid(&state, &vault_id, &file.guid).await?;
    Ok(Json(match share {
        Some(share) => serde_json::json!({
            "share": ShareResponse {
                url: share_url(&state, &share.id),
                id: share.id,
                path: file.path,
                guid: file.guid,
                created_at: share.created_at,
            }
        }),
        None => serde_json::json!({ "share": null }),
    }))
}

pub async fn delete_share(
    State(state): State<AppState>,
    principal: ApiPrincipal,
    Path(vault_id): Path<String>,
    Query(query): Query<SharePathQuery>,
) -> AppResult<Json<serde_json::Value>> {
    principal.require_vault(&vault_id)?;
    require_member(&state, &principal.user.id, &vault_id).await?;
    let file = file_by_path(&state, &vault_id, &query.path)
        .await?
        .ok_or(AppError::NotFound)?;
    let share = share_by_guid(&state, &vault_id, &file.guid)
        .await?
        .ok_or(AppError::NotFound)?;
    public_shares::Entity::delete_by_id(share.id)
        .exec(&state.db)
        .await?;
    Ok(Json(serde_json::json!({ "ok": true })))
}

pub async fn create_attachment_share(
    State(state): State<AppState>,
    principal: ApiPrincipal,
    Path(vault_id): Path<String>,
    Json(body): Json<CreateShareBody>,
) -> AppResult<Json<AttachmentShareResponse>> {
    let attachment =
        attachments::require_attachment(&state, &principal, &vault_id, &body.path).await?;

    if let Some(existing) = attachment_share_by_path(&state, &vault_id, &body.path).await? {
        if existing.hash == attachment.hash {
            return Ok(Json(attachment_share_response(&state, existing)));
        }

        // Keep the existing id and update its captured version atomically. This
        // avoids a revoke/recreate gap and makes concurrent re-shares converge.
        let mut updated: public_attachment_shares::ActiveModel = existing.into();
        updated.hash = Set(attachment.hash.clone());
        updated.size = Set(attachment.size);
        return Ok(Json(attachment_share_response(
            &state,
            updated.update(&state.db).await?,
        )));
    }

    let id = nanoid::nanoid!(16, &nanoid::alphabet::SAFE);
    let created_at = now_millis();
    let insert = public_attachment_shares::ActiveModel {
        id: Set(id.clone()),
        vault_id: Set(vault_id.clone()),
        path: Set(attachment.path.clone()),
        hash: Set(attachment.hash.clone()),
        size: Set(attachment.size),
        created_by: Set(principal.user.id.clone()),
        created_at: Set(created_at),
    }
    .insert(&state.db)
    .await;

    match insert {
        Ok(created) => Ok(Json(attachment_share_response(&state, created))),
        Err(error) => {
            // Another request may have inserted the same (vault, path) after
            // our lookup. Re-read the unique-index winner so concurrent
            // idempotent creates return the same link instead of a 500.
            if let Some(winner) =
                attachment_share_by_path(&state, &vault_id, &attachment.path).await?
            {
                if winner.hash == attachment.hash {
                    return Ok(Json(attachment_share_response(&state, winner)));
                }
            }
            Err(error.into())
        }
    }
}

pub async fn get_attachment_share(
    State(state): State<AppState>,
    principal: ApiPrincipal,
    Path(vault_id): Path<String>,
    Query(query): Query<SharePathQuery>,
) -> AppResult<Json<serde_json::Value>> {
    principal.require_vault(&vault_id)?;
    require_member(&state, &principal.user.id, &vault_id).await?;
    let share = attachment_share_by_path(&state, &vault_id, &query.path).await?;
    Ok(Json(match share {
        Some(share) => serde_json::json!({ "share": attachment_share_response(&state, share) }),
        None => serde_json::json!({ "share": null }),
    }))
}

pub async fn delete_attachment_share(
    State(state): State<AppState>,
    principal: ApiPrincipal,
    Path(vault_id): Path<String>,
    Query(query): Query<SharePathQuery>,
) -> AppResult<Json<serde_json::Value>> {
    principal.require_vault(&vault_id)?;
    require_member(&state, &principal.user.id, &vault_id).await?;
    let share = attachment_share_by_path(&state, &vault_id, &query.path)
        .await?
        .ok_or(AppError::NotFound)?;
    public_attachment_shares::Entity::delete_by_id(share.id)
        .exec(&state.db)
        .await?;
    Ok(Json(serde_json::json!({ "ok": true })))
}

// ---------------------------------------------------------------------------
// Public view API (no auth)
// ---------------------------------------------------------------------------

pub async fn view_shared_attachment(
    State(state): State<AppState>,
    Path(share_id): Path<String>,
) -> AppResult<Response> {
    let share = public_attachment_shares::Entity::find_by_id(share_id)
        .one(&state.db)
        .await?
        .ok_or(AppError::NotFound)?;
    attachments::read_attachment_public_exact(&state, &share.vault_id, &share.path, &share.hash)
        .await
}

pub async fn view_share(
    State(state): State<AppState>,
    Path(share_id): Path<String>,
) -> AppResult<Json<ViewResponse>> {
    let (share, file) = resolve_share(&state, &share_id).await?;
    let note_doc_id = doc_id(&share.vault_id, &share.guid);
    let (epoch, update) = state.documents.read_update_with_epoch(&note_doc_id).await?;
    Ok(Json(ViewResponse {
        title: title_for_path(&file.path),
        epoch,
        update_b64: base64::engine::general_purpose::STANDARD.encode(&update),
        path: file.path,
        updated_at: file.updated_at,
    }))
}

pub async fn view_events(
    State(state): State<AppState>,
    Path(share_id): Path<String>,
    Query(query): Query<ViewEventsQuery>,
) -> AppResult<Sse<impl Stream<Item = Result<Event, Infallible>>>> {
    let (share, _) = resolve_share(&state, &share_id).await?;
    let note_doc_id = doc_id(&share.vault_id, &share.guid);

    // Establish a stream baseline, then emit that full state as the first SSE
    // event. This closes the GET-to-EventSource race: edits made after the GET
    // but before this read are included here, and replaying the GET state is
    // harmless because Yjs full updates are idempotent.
    let (initial_epoch, initial) = state.documents.read_update_with_epoch(&note_doc_id).await?;
    let last_sv = state_vector_of(&initial)?;

    struct Poller {
        state: AppState,
        share_id: String,
        note_doc_id: String,
        epoch: u64,
        sv: StateVector,
        done: bool,
    }

    let poller = Poller {
        state,
        share_id,
        note_doc_id,
        epoch: initial_epoch,
        sv: last_sv,
        done: false,
    };

    let stream = futures_util::stream::unfold(poller, |mut p| async move {
        if p.done {
            return None;
        }
        loop {
            tokio::time::sleep(POLL_INTERVAL).await;

            // Stop streaming as soon as the share is revoked.
            match public_shares::Entity::find_by_id(p.share_id.clone())
                .one(&p.state.db)
                .await
            {
                Ok(Some(_)) => {}
                _ => {
                    p.done = true;
                    return Some((Ok(Event::default().event("revoked").data("{}")), p));
                }
            }

            // Transient document read failures just skip a tick.
            let Ok((epoch, update)) = p
                .state
                .documents
                .read_update_with_epoch(&p.note_doc_id)
                .await
            else {
                continue;
            };
            if epoch != p.epoch {
                p.epoch = epoch;
                let Ok(new_sv) = state_vector_of(&update) else {
                    continue;
                };
                p.sv = new_sv;
                let payload = serde_json::json!({
                    "epoch": epoch,
                    "update": base64::engine::general_purpose::STANDARD.encode(&update),
                });
                return Some((
                    Ok(Event::default().event("snapshot").data(payload.to_string())),
                    p,
                ));
            }
            let Ok((delta, new_sv)) = delta_since(&update, &p.sv) else {
                continue;
            };
            if new_sv == p.sv {
                continue;
            }
            p.sv = new_sv;
            let payload = serde_json::json!({
                "epoch": epoch,
                "update": base64::engine::general_purpose::STANDARD.encode(&delta),
            });
            return Some((
                Ok(Event::default().event("update").data(payload.to_string())),
                p,
            ));
        }
    });

    let initial_snapshot = initial_snapshot_event(query.epoch, initial_epoch, &initial);
    let stream = futures_util::stream::once(async move { initial_snapshot }).chain(stream);

    Ok(Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(25))))
}

/// Decode a full-state v1 update into its state vector.
fn state_vector_of(update: &[u8]) -> AppResult<StateVector> {
    let doc = Doc::new();
    let decoded = crate::safe_yrs::decode_v1::<Update>(update)
        .map_err(|e| AppError::Internal(format!("decode update: {e:?}")))?;
    {
        let mut txn = doc.transact_mut();
        txn.apply_update(decoded);
    }
    let sv = doc.transact().state_vector();
    Ok(sv)
}

/// Compute the minimal Yjs update covering everything in `update` that is not
/// yet covered by `since`, plus the new full state vector.
fn delta_since(update: &[u8], since: &StateVector) -> AppResult<(Vec<u8>, StateVector)> {
    let doc = Doc::new();
    let decoded = crate::safe_yrs::decode_v1::<Update>(update)
        .map_err(|e| AppError::Internal(format!("decode update: {e:?}")))?;
    {
        let mut txn = doc.transact_mut();
        txn.apply_update(decoded);
    }
    let txn = doc.transact();
    Ok((txn.encode_state_as_update_v1(since), txn.state_vector()))
}

pub async fn view_attachment(
    State(state): State<AppState>,
    Path((share_id, path)): Path<(String, String)>,
) -> AppResult<Response> {
    let (share, file) = resolve_share(&state, &share_id).await?;
    let note_doc_id = format!("{}__{}", share.vault_id, share.guid);
    let update = ydoc::read_update(&state, &note_doc_id).await?;
    let content =
        ydoc::decode_text(&update, "contents").map_err(|e| AppError::Internal(e.to_string()))?;
    if !note_references_attachment(&content, &file.path, &path) {
        return Err(AppError::NotFound);
    }
    attachments::read_attachment_public(&state, &share.vault_id, &path).await
}

pub async fn view_resolve(
    State(state): State<AppState>,
    Path(share_id): Path<String>,
    Query(query): Query<ResolveQuery>,
) -> AppResult<Json<ResolveResponse>> {
    let (share, _) = resolve_share(&state, &share_id).await?;
    let target = query.target.trim().trim_matches('/');
    if target.is_empty() {
        return Err(AppError::BadRequest("missing target".into()));
    }

    let files = vault_files::Entity::find()
        .filter(vault_files::Column::VaultId.eq(&share.vault_id))
        .all(&state.db)
        .await?;
    let file = resolve_wikilink_target(&files, target).ok_or(AppError::NotFound)?;

    // Only reveal notes that are themselves publicly shared.
    let other = share_by_guid(&state, &share.vault_id, &file.guid)
        .await?
        .ok_or(AppError::NotFound)?;
    Ok(Json(ResolveResponse {
        share_id: other.id,
        title: title_for_path(&file.path),
        path: file.path.clone(),
    }))
}

/// Resolve a wikilink target the way Obsidian does, approximately: exact path,
/// path with `.md` appended, then unique basename match (case-insensitive).
fn resolve_wikilink_target<'a>(
    files: &'a [vault_files::Model],
    target: &str,
) -> Option<&'a vault_files::Model> {
    let with_md = format!("{target}.md");
    if let Some(file) = files.iter().find(|f| f.path == target || f.path == with_md) {
        return Some(file);
    }
    let lower = target.to_lowercase();
    files.iter().find(|f| {
        let name = f.path.rsplit('/').next().unwrap_or(&f.path);
        let stem = name.strip_suffix(".md").unwrap_or(name);
        stem.to_lowercase() == lower
    })
}

/// True when `requested` is an embed/image target of `note_content`.
fn note_references_attachment(note_content: &str, note_path: &str, requested: &str) -> bool {
    let requested = requested.trim().trim_matches('/');
    if requested.is_empty() {
        return false;
    }
    let note_dir = note_path.rsplit_once('/').map(|(dir, _)| dir).unwrap_or("");
    for target in extract_attachment_targets(note_content) {
        if attachment_target_matches(&target, requested, note_dir) {
            return true;
        }
    }
    false
}

fn extract_attachment_targets(content: &str) -> Vec<String> {
    let mut targets = Vec::new();
    let bytes = content.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'!' && i + 1 < bytes.len() && bytes[i + 1] == b'[' {
            if i + 2 < bytes.len() && bytes[i + 2] == b'[' {
                if let Some(end) = content[i + 3..].find("]]") {
                    let inner = content[i + 3..i + 3 + end].trim();
                    let target = inner.split('|').next().unwrap_or(inner).trim();
                    if !target.is_empty() {
                        targets.push(target.to_string());
                    }
                    i += 3 + end + 2;
                    continue;
                }
            } else if let Some(close) = content[i + 2..].find("](") {
                let after = i + 2 + close + 2;
                if let Some(end) = content[after..].find(')') {
                    let target = content[after..after + end].trim();
                    if !target.is_empty() {
                        targets.push(target.to_string());
                    }
                    i = after + end + 1;
                    continue;
                }
            }
        }
        i += 1;
    }
    targets
}

fn attachment_target_matches(target: &str, requested: &str, note_dir: &str) -> bool {
    let Some(requested) = normalize_attachment_path("", requested) else {
        return false;
    };
    let Some(vault_target) = normalize_attachment_path("", target) else {
        return false;
    };
    if vault_target.eq_ignore_ascii_case(&requested) {
        return true;
    }
    let target_base = vault_target.rsplit('/').next().unwrap_or(&vault_target);
    let requested_base = requested.rsplit('/').next().unwrap_or(&requested);
    if target_base.eq_ignore_ascii_case(requested_base) && !vault_target.contains('/') {
        return true;
    }
    if let Some(relative) = normalize_attachment_path(note_dir, target) {
        if relative.eq_ignore_ascii_case(&requested) {
            return true;
        }
    }
    false
}

fn normalize_attachment_path(base_dir: &str, path: &str) -> Option<String> {
    let path = path.trim().split(['?', '#']).next().unwrap_or("");
    let decoded = percent_encoding::percent_decode_str(path)
        .decode_utf8()
        .ok()?;
    let mut segments: Vec<&str> = if decoded.starts_with('/') {
        Vec::new()
    } else {
        base_dir
            .split('/')
            .filter(|segment| !segment.is_empty())
            .collect()
    };
    for segment in decoded.split(['/', '\\']) {
        match segment {
            "" | "." => {}
            ".." => {
                segments.pop();
            }
            segment => segments.push(segment),
        }
    }
    (!segments.is_empty()).then(|| segments.join("/"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use yrs::{GetString, Text};

    fn text_doc(value: &str) -> Doc {
        let doc = Doc::new();
        let text = doc.get_or_insert_text("contents");
        {
            let mut txn = doc.transact_mut();
            text.insert(&mut txn, 0, value);
        }
        doc
    }

    fn full_update(doc: &Doc) -> Vec<u8> {
        doc.transact()
            .encode_state_as_update_v1(&StateVector::default())
    }

    #[test]
    fn delta_since_carries_only_new_changes_and_applies_cleanly() {
        let doc = text_doc("hello");
        let base = full_update(&doc);
        let base_sv = state_vector_of(&base).unwrap();

        let text = doc.get_or_insert_text("contents");
        {
            let mut txn = doc.transact_mut();
            text.insert(&mut txn, 5, " world");
        }
        let next = full_update(&doc);

        let (delta, new_sv) = delta_since(&next, &base_sv).unwrap();
        assert_ne!(new_sv, base_sv);
        assert!(delta.len() < next.len());

        // A client doc holding only the snapshot ends up with the new text.
        let client = Doc::new();
        {
            let mut txn = client.transact_mut();
            txn.apply_update(crate::safe_yrs::decode_v1::<Update>(&base).unwrap());
            txn.apply_update(crate::safe_yrs::decode_v1::<Update>(&delta).unwrap());
        }
        let text = client.get_or_insert_text("contents");
        assert_eq!(text.get_string(&client.transact()), "hello world");
    }

    #[test]
    fn delta_since_reports_unchanged_state_vector_when_idle() {
        let doc = text_doc("hello");
        let base = full_update(&doc);
        let sv = state_vector_of(&base).unwrap();
        let (_, new_sv) = delta_since(&base, &sv).unwrap();
        assert_eq!(new_sv, sv);
    }

    #[test]
    fn stream_always_starts_with_snapshot_to_cover_get_eventsource_race() {
        let update = full_update(&text_doc("replacement"));
        assert!(initial_snapshot_event(Some(7), 8, &update).is_ok());
        assert!(initial_snapshot_event(Some(8), 8, &update).is_ok());
        assert!(initial_snapshot_event(None, 8, &update).is_ok());
    }

    #[test]
    fn title_strips_directories_and_extension() {
        assert_eq!(title_for_path("Folder/Sub/My Note.md"), "My Note");
        assert_eq!(title_for_path("Plain.md"), "Plain");
        assert_eq!(title_for_path("noext"), "noext");
    }

    fn file(path: &str, guid: &str) -> vault_files::Model {
        vault_files::Model {
            id: guid.to_string(),
            vault_id: "v".into(),
            guid: guid.to_string(),
            path: path.to_string(),
            updated_at: 0,
        }
    }

    #[test]
    fn wikilink_resolution_prefers_exact_path_then_basename() {
        let files = vec![
            file("A/Note.md", "g1"),
            file("B/Other.md", "g2"),
            file("Exact.md", "g3"),
        ];
        assert_eq!(resolve_wikilink_target(&files, "Exact").unwrap().guid, "g3");
        assert_eq!(
            resolve_wikilink_target(&files, "A/Note").unwrap().guid,
            "g1"
        );
        assert_eq!(resolve_wikilink_target(&files, "other").unwrap().guid, "g2");
        assert!(resolve_wikilink_target(&files, "Missing").is_none());
    }

    #[test]
    fn share_attachment_must_be_referenced_by_note() {
        let note = "See ![[HR/offer-letters.pdf]] and ![chart](images/chart.png)";
        assert!(note_references_attachment(
            note,
            "Notes/hiring.md",
            "HR/offer-letters.pdf"
        ));
        assert!(note_references_attachment(
            note,
            "Notes/hiring.md",
            "images/chart.png"
        ));
        assert!(!note_references_attachment(
            note,
            "Notes/hiring.md",
            "HR/salary.pdf"
        ));
        assert!(note_references_attachment(
            "![[chart.png]]",
            "Notes/hiring.md",
            "Notes/chart.png"
        ));
        assert!(note_references_attachment(
            "![chart](../images/chart%20one.png)",
            "Notes/Published/hiring.md",
            "Notes/images/chart one.png"
        ));
    }
}
