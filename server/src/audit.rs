//! Short-lived audit trail for remote-cursor mutations.
//!
//! Every document mutation performed by a remote cursor (MCP, REST or the
//! streaming API) is recorded here with enough before/after state to render a
//! diff and to undo the operation. This is separate from the permanent Git
//! audit log: entries expire after [`RETENTION_MS`] (~3 days) and exist so
//! vault admins can review and revert what an automation did recently.

use sea_orm::{
    ActiveModelTrait, ColumnTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect, Set,
};
use serde_json::{json, Value as JsonValue};

use crate::entities::{cursor_audit_log, remote_cursor_tokens};
use crate::error::{AppError, AppResult};
use crate::notes;
use crate::session::{now_millis, ApiActor, ApiPrincipal};
use crate::state::AppState;
use crate::structured;

pub const RETENTION_MS: i64 = 72 * 60 * 60 * 1000;
/// Cap stored note contents; entries above this are kept (for the log view)
/// but truncated, which disables undo for that entry.
const MAX_CONTENT_BYTES: usize = 1_000_000;
const RETENTION_SWEEP_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60 * 60);

/// Whether operations by this principal should be audited.
pub fn is_cursor(principal: &ApiPrincipal) -> bool {
    matches!(principal.actor, ApiActor::Cursor(_))
}

/// One operation to record. `operation` doubles as the undo dispatch key.
pub struct AuditEntry {
    pub operation: &'static str,
    pub path: String,
    pub to_path: Option<String>,
    pub before_content: Option<String>,
    pub after_content: Option<String>,
    pub details: Option<JsonValue>,
}

impl AuditEntry {
    pub fn new(operation: &'static str, path: impl Into<String>) -> Self {
        AuditEntry {
            operation,
            path: path.into(),
            to_path: None,
            before_content: None,
            after_content: None,
            details: None,
        }
    }

    pub fn to_path(mut self, to_path: impl Into<String>) -> Self {
        self.to_path = Some(to_path.into());
        self
    }

    pub fn before(mut self, content: impl Into<String>) -> Self {
        self.before_content = Some(content.into());
        self
    }

    pub fn after(mut self, content: impl Into<String>) -> Self {
        self.after_content = Some(content.into());
        self
    }

    pub fn details(mut self, details: JsonValue) -> Self {
        self.details = Some(details);
        self
    }
}

/// Truncate to `MAX_CONTENT_BYTES` on a char boundary; true if truncated.
fn clamp_content(content: String) -> (String, bool) {
    if content.len() <= MAX_CONTENT_BYTES {
        return (content, false);
    }
    let mut end = MAX_CONTENT_BYTES;
    while !content.is_char_boundary(end) {
        end -= 1;
    }
    (content[..end].to_string(), true)
}

/// Record an audit entry for a cursor-actor mutation. No-op for human actors.
/// Failures are logged and swallowed: auditing must never fail the edit.
/// Returns the entry id when one was written.
pub async fn record(
    state: &AppState,
    principal: &ApiPrincipal,
    vault_id: &str,
    entry: AuditEntry,
) -> Option<String> {
    let ApiActor::Cursor(cursor) = &principal.actor else {
        return None;
    };
    let mut truncated = false;
    let before = entry.before_content.map(|c| {
        let (c, t) = clamp_content(c);
        truncated |= t;
        c
    });
    let after = entry.after_content.map(|c| {
        let (c, t) = clamp_content(c);
        truncated |= t;
        c
    });
    let mut details = entry.details.unwrap_or_else(|| json!({}));
    if truncated {
        details["truncated"] = json!(true);
    }
    let id = uuid::Uuid::new_v4().to_string();
    let model = cursor_audit_log::ActiveModel {
        id: Set(id.clone()),
        vault_id: Set(vault_id.to_string()),
        cursor_id: Set(cursor.id.clone()),
        created_at: Set(now_millis()),
        operation: Set(entry.operation.to_string()),
        path: Set(entry.path),
        to_path: Set(entry.to_path),
        before_content: Set(before),
        after_content: Set(after),
        details: Set(Some(details.to_string())),
        undone_at: Set(None),
    };
    match model.insert(&state.db).await {
        Ok(_) => Some(id),
        Err(e) => {
            tracing::warn!("cursor audit insert failed: {e}");
            None
        }
    }
}

/// Newest-first keyset pagination over a cursor's audit entries.
pub async fn list(
    state: &AppState,
    cursor_id: &str,
    before: Option<i64>,
    limit: u64,
) -> AppResult<Vec<cursor_audit_log::Model>> {
    let mut query = cursor_audit_log::Entity::find()
        .filter(cursor_audit_log::Column::CursorId.eq(cursor_id))
        .filter(cursor_audit_log::Column::CreatedAt.gt(now_millis() - RETENTION_MS));
    if let Some(before) = before {
        query = query.filter(cursor_audit_log::Column::CreatedAt.lt(before));
    }
    Ok(query
        .order_by_desc(cursor_audit_log::Column::CreatedAt)
        .limit(limit)
        .all(&state.db)
        .await?)
}

fn entry_details(entry: &cursor_audit_log::Model) -> JsonValue {
    entry
        .details
        .as_deref()
        .and_then(|d| serde_json::from_str(d).ok())
        .unwrap_or_else(|| json!({}))
}

fn entry_is_truncated(entry: &cursor_audit_log::Model) -> bool {
    entry_details(entry)["truncated"] == json!(true)
}

/// Undo a single audit entry by applying the inverse operation, attributed to
/// the (human) `undoer` principal. Best-effort, single-entry: when the target
/// changed since the operation, a `changed_since` conflict is returned unless
/// `force` is set.
pub async fn undo(
    state: &AppState,
    undoer: &ApiPrincipal,
    vault_id: &str,
    cursor_id: &str,
    entry_id: &str,
    force: bool,
) -> AppResult<()> {
    let entry = cursor_audit_log::Entity::find_by_id(entry_id.to_string())
        .filter(cursor_audit_log::Column::VaultId.eq(vault_id))
        .filter(cursor_audit_log::Column::CursorId.eq(cursor_id))
        .one(&state.db)
        .await?
        .ok_or(AppError::NotFound)?;
    if entry.undone_at.is_some() {
        return Err(AppError::Conflict("already_undone".into()));
    }
    if entry_is_truncated(&entry) {
        return Err(AppError::BadRequest("undo_unavailable".into()));
    }

    match entry.operation.as_str() {
        "note_create" => {
            check_note_unchanged(state, undoer, vault_id, &entry.path, &entry, force).await?;
            notes::delete_note_inner(state, undoer, vault_id, &entry.path).await?;
        }
        "note_replace"
        | "note_patch"
        | "note_replace_body"
        | "note_frontmatter"
        | "note_periodic_append"
        | "stream" => {
            let before = entry
                .before_content
                .clone()
                .ok_or_else(|| AppError::BadRequest("undo_unavailable".into()))?;
            check_note_unchanged(state, undoer, vault_id, &entry.path, &entry, force).await?;
            notes::replace_note_inner(
                state,
                undoer,
                vault_id,
                &entry.path,
                notes::ReplaceNoteBody {
                    content: before,
                    expected_content_hash: None,
                },
            )
            .await?;
        }
        "note_move" => {
            let to_path = entry
                .to_path
                .clone()
                .ok_or_else(|| AppError::BadRequest("undo_unavailable".into()))?;
            notes::move_note_inner(
                state,
                undoer,
                vault_id,
                &to_path,
                notes::MoveNoteBody {
                    to_path: entry.path.clone(),
                },
            )
            .await?;
        }
        "note_delete" => {
            let before = entry
                .before_content
                .clone()
                .ok_or_else(|| AppError::BadRequest("undo_unavailable".into()))?;
            notes::create_note_inner(
                state,
                undoer,
                vault_id,
                notes::CreateNoteBody {
                    path: entry.path.clone(),
                    content: before,
                },
            )
            .await?;
        }
        "structured_set" => {
            let kind = structured_kind(&entry)?;
            let before = structured_json(entry.before_content.as_deref())?;
            let current =
                structured::read_structured_json(state, undoer, vault_id, &entry.path, &kind)
                    .await?;
            let after = structured_json(entry.after_content.as_deref())?;
            if current.value != after && !force {
                return Err(AppError::Conflict("changed_since".into()));
            }
            // Refuse the undo if the file changes between that check and the write.
            let expected = if force { None } else { current.value_hash };
            structured::write_structured_json(
                state,
                undoer,
                vault_id,
                &entry.path,
                &kind,
                before,
                expected.as_deref(),
            )
            .await?;
        }
        "structured_create" => {
            let kind = structured_kind(&entry)?;
            structured::delete_structured_inner(state, undoer, vault_id, &entry.path, &kind)
                .await?;
        }
        "structured_delete" => {
            let kind = structured_kind(&entry)?;
            let before = structured_json(entry.before_content.as_deref())?;
            structured::create_structured(state, undoer, vault_id, &entry.path, &kind, before)
                .await?;
        }
        "structured_move" => {
            let kind = structured_kind(&entry)?;
            let update_embeds = entry_details(&entry)["updateEmbeds"]
                .as_bool()
                .unwrap_or(false);
            let to_path = entry
                .to_path
                .clone()
                .ok_or_else(|| AppError::BadRequest("undo_unavailable".into()))?;
            structured::move_structured_inner(
                state,
                undoer,
                vault_id,
                &to_path,
                structured::MoveStructuredBody {
                    to_path: entry.path.clone(),
                    update_embeds,
                },
                &kind,
            )
            .await?;
        }
        "attachment_upload" => {
            crate::attachments::delete_attachment_inner(state, undoer, vault_id, &entry.path)
                .await?;
        }
        "attachment_delete" => {
            let details = entry_details(&entry);
            let hash = details["hash"]
                .as_str()
                .ok_or_else(|| AppError::BadRequest("undo_unavailable".into()))?
                .to_string();
            let size = details["size"].as_i64().unwrap_or(0);
            undoer.require_vault(vault_id)?;
            crate::routes::require_member(state, &undoer.user.id, vault_id).await?;
            crate::ydoc::index_set_binary(state, vault_id, &entry.path, &hash, size).await?;
            state
                .git
                .mark_write(
                    vault_id,
                    &undoer.to_git_principal(now_millis() + 24 * 60 * 60 * 1000),
                )
                .await;
        }
        "attachment_move" => {
            let update_embeds = entry_details(&entry)["updateEmbeds"]
                .as_bool()
                .unwrap_or(false);
            let to_path = entry
                .to_path
                .clone()
                .ok_or_else(|| AppError::BadRequest("undo_unavailable".into()))?;
            crate::attachments::move_attachment_inner(
                state,
                undoer,
                vault_id,
                &to_path,
                crate::attachments::MoveAttachmentBody {
                    to_path: entry.path.clone(),
                    update_embeds,
                },
            )
            .await?;
        }
        other => {
            return Err(AppError::BadRequest(format!(
                "cannot undo operation {other}"
            )));
        }
    }

    let mut active: cursor_audit_log::ActiveModel = entry.into();
    active.undone_at = Set(Some(now_millis()));
    active.update(&state.db).await?;
    Ok(())
}

/// 409 `changed_since` when the note's current content differs from the
/// entry's recorded `after_content` (and the caller didn't force). A missing
/// note surfaces as a `missing` conflict so the client can explain it.
async fn check_note_unchanged(
    state: &AppState,
    undoer: &ApiPrincipal,
    vault_id: &str,
    path: &str,
    entry: &cursor_audit_log::Model,
    force: bool,
) -> AppResult<()> {
    if force {
        return Ok(());
    }
    let current = match notes::read_note_inner(state, undoer, vault_id, path).await {
        Ok(note) => note.content,
        Err(AppError::NotFound) => return Err(AppError::Conflict("missing".into())),
        Err(e) => return Err(e),
    };
    if Some(&current) != entry.after_content.as_ref() {
        return Err(AppError::Conflict("changed_since".into()));
    }
    Ok(())
}

fn structured_kind(entry: &cursor_audit_log::Model) -> AppResult<String> {
    entry_details(entry)["kind"]
        .as_str()
        .map(ToString::to_string)
        .ok_or_else(|| AppError::BadRequest("undo_unavailable".into()))
}

fn structured_json(content: Option<&str>) -> AppResult<JsonValue> {
    serde_json::from_str(content.ok_or_else(|| AppError::BadRequest("undo_unavailable".into()))?)
        .map_err(|e| AppError::Internal(format!("audit structured payload: {e}")))
}

/// Hourly sweep: expire audit entries past retention and stale cursor tokens.
pub fn spawn_retention_task(state: AppState) {
    tokio::spawn(async move {
        loop {
            let now = now_millis();
            if let Err(e) = cursor_audit_log::Entity::delete_many()
                .filter(cursor_audit_log::Column::CreatedAt.lt(now - RETENTION_MS))
                .exec(&state.db)
                .await
            {
                tracing::warn!("audit retention sweep failed: {e}");
            }
            if let Err(e) = remote_cursor_tokens::Entity::delete_many()
                .filter(remote_cursor_tokens::Column::ExpiresAt.lt(now))
                .exec(&state.db)
                .await
            {
                tracing::warn!("cursor token sweep failed: {e}");
            }
            tokio::time::sleep(RETENTION_SWEEP_INTERVAL).await;
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clamp_content_truncates_on_char_boundary() {
        let (kept, truncated) = clamp_content("short".to_string());
        assert_eq!(kept, "short");
        assert!(!truncated);

        // A crab is 4 bytes; build a string straddling the limit.
        let crabs = "🦀".repeat(MAX_CONTENT_BYTES / 4 + 2);
        let (kept, truncated) = clamp_content(crabs);
        assert!(truncated);
        assert!(kept.len() <= MAX_CONTENT_BYTES);
        assert!(kept.chars().all(|c| c == '🦀'));
    }

    fn entry_with(details: Option<&str>) -> cursor_audit_log::Model {
        cursor_audit_log::Model {
            id: "e1".into(),
            vault_id: "v1".into(),
            cursor_id: "c1".into(),
            created_at: 0,
            operation: "note_replace".into(),
            path: "a.md".into(),
            to_path: None,
            before_content: None,
            after_content: None,
            details: details.map(ToString::to_string),
            undone_at: None,
        }
    }

    #[test]
    fn truncated_flag_is_read_from_details() {
        assert!(entry_is_truncated(&entry_with(Some(
            r#"{"truncated":true}"#
        ))));
        assert!(!entry_is_truncated(&entry_with(Some(r#"{}"#))));
        assert!(!entry_is_truncated(&entry_with(None)));
    }

    #[test]
    fn structured_kind_requires_details() {
        let mut entry = entry_with(Some(r#"{"kind":"canvas"}"#));
        assert_eq!(structured_kind(&entry).unwrap(), "canvas");
        entry.details = None;
        assert!(structured_kind(&entry).is_err());
    }
}
