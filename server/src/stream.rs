//! Streaming API for remote cursors.
//!
//! `GET /api/vaults/{vault_id}/stream` upgrades to a WebSocket on which an
//! external app (holding a remote cursor bearer token) streams LLM tokens into
//! a note. The server joins the note's native CRDT document as a peer: it applies
//! token batches as incremental Yjs updates and publishes a Yjs awareness state
//! for the cursor, so connected editors render the streamed text *and* a named
//! caret in realtime (see `src/editor/RemoteSelections.ts` — no client changes
//! needed).
//!
//! Client-facing protocol (JSON text frames):
//!   → {"type":"start","path":"Notes/x.md","anchor":{"mode":"append"}}
//!     anchor: {"mode":"append"} | {"mode":"after","text":"## Draft"}
//!             | {"mode":"offset","offset":42}  (UTF-8 byte offset)
//!   → {"type":"text","text":"chunk"}   (repeat per token/chunk)
//!   → {"type":"end"}
//!   ← {"type":"started","guid":…,"position":…}
//!   ← {"type":"ack","applied":N}      (per flush, N = bytes applied)
//!   ← {"type":"error","code":…,"message":…} (fatal; connection closes)
//!   ← {"type":"done","auditId":…,"inserted":N}
//!
//! The insertion point is tracked as a yrs sticky index (relative position), so
//! concurrent human edits move the stream position instead of corrupting it.
//! Auth: `Authorization: Bearer <cursor token>` or `?token=` (browser clients).

use axum::extract::ws::{Message as AxumMsg, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{json, Value as JsonValue};
use tokio::time::{interval, timeout, Duration, Instant};
use yrs::encoding::write::Write;
use yrs::sync::protocol::MSG_AWARENESS;
use yrs::sync::{Message as YMsg, SyncMessage};
use yrs::updates::encoder::{Encode, Encoder, EncoderV1};
use yrs::{
    Assoc, Doc, GetString, IndexScope, IndexedSequence, ReadTxn, StickyIndex, Text, Transact,
};

use crate::audit::{self, AuditEntry};
use crate::crdt::{CrdtConnection, Level};
use crate::error::AppError;
use crate::notes;
use crate::session::{
    bearer_token, cursor_by_token_hash, cursor_principal, hash_token, ApiActor, ApiPrincipal,
};
use crate::state::AppState;

/// Batch pending tokens and flush on this cadence (or on `FLUSH_BYTES`).
const FLUSH_INTERVAL: Duration = Duration::from_millis(150);
const FLUSH_BYTES: usize = 4 * 1024;
/// Commit and close when no client frame arrives for this long.
const IDLE_TIMEOUT: Duration = Duration::from_secs(60);
/// Hard cap on a single session's lifetime.
const MAX_SESSION: Duration = Duration::from_secs(15 * 60);
/// Hard cap on text inserted per session.
const MAX_INSERTED_BYTES: usize = 2 * 1024 * 1024;
/// How long we wait for the `start` frame and initial Yjs sync.
const SETUP_TIMEOUT: Duration = Duration::from_secs(30);
/// Longest a session waits for the server to activate a proposed epoch it has
/// acknowledged. Activation waits at most the acknowledgement timeout.
const EPOCH_ACTIVATION_TIMEOUT: Duration = Duration::from_secs(90);

#[derive(Deserialize)]
pub struct StreamQuery {
    token: Option<String>,
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ClientFrame {
    Start {
        path: String,
        #[serde(default)]
        anchor: Anchor,
    },
    Text {
        text: String,
    },
    End,
}

#[derive(Deserialize, Default)]
#[serde(tag = "mode", rename_all = "snake_case")]
enum Anchor {
    #[default]
    Append,
    After {
        text: String,
    },
    Offset {
        offset: usize,
    },
}

pub async fn stream_ws(
    State(state): State<AppState>,
    Path(vault_id): Path<String>,
    Query(query): Query<StreamQuery>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    let token = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(bearer_token)
        .map(str::to_string)
        .or(query.token);
    let Some(token) = token else {
        return (StatusCode::UNAUTHORIZED, "missing bearer token").into_response();
    };
    let cursor = match cursor_by_token_hash(&state.db, &hash_token(&token)).await {
        Ok(Some(cursor)) => cursor,
        Ok(None) => return (StatusCode::UNAUTHORIZED, "unknown token").into_response(),
        Err(e) => return e.into_response(),
    };
    if cursor.vault_id != vault_id {
        return (StatusCode::FORBIDDEN, "cursor is bound to another vault").into_response();
    }
    let principal = match cursor_principal(&state.db, cursor).await {
        Ok(principal) => principal,
        Err(e) => return e.into_response(),
    };
    ws.on_upgrade(move |socket| async move {
        if let Err(e) = run_session(state, vault_id, principal, socket).await {
            tracing::debug!("stream session closed: {e}");
        }
    })
}

async fn run_session(
    state: AppState,
    vault_id: String,
    principal: ApiPrincipal,
    mut client: WebSocket,
) -> anyhow::Result<()> {
    // 1. The first frame must be `start`, naming the note and anchor.
    let (path, anchor) = match timeout(SETUP_TIMEOUT, next_client_frame(&mut client)).await {
        Ok(Ok(Some(ClientFrame::Start { path, anchor }))) => (path, anchor),
        Ok(Ok(Some(_))) => {
            return fail(&mut client, "expected_start", "first frame must be start").await
        }
        Ok(Ok(None)) | Ok(Err(_)) => return Ok(()),
        Err(_) => return fail(&mut client, "timeout", "no start frame received").await,
    };

    // 2. Resolve the note with full membership/ACL checks.
    let file = match notes::require_note_access(&state, &principal, &vault_id, &path, true).await {
        Ok(file) => file,
        Err(e) => return fail(&mut client, error_code(&e), &e.to_string()).await,
    };
    let doc_id = format!("{vault_id}__{}", file.guid);

    // 3. Join the native document as a peer and sync the current state.
    let mut document = match state
        .documents
        .connect(&doc_id, Level::Full, None, None)
        .await
    {
        Ok(connection) => connection,
        Err(e) => {
            tracing::warn!("stream: document connect failed: {e}");
            return fail(&mut client, "sync_failed", "could not join document").await;
        }
    };
    let mut session = DocSession::new(&principal);
    if let Err(e) = timeout(SETUP_TIMEOUT, initial_sync(&mut session, &mut document))
        .await
        .map_err(|_| anyhow::anyhow!("initial sync timed out"))
        .and_then(|r| r)
    {
        tracing::warn!("stream: initial sync failed: {e}");
        return fail(&mut client, "sync_failed", "could not sync document").await;
    }
    let before_content = session.content();

    // 4. Pin the insertion point and show the caret.
    let position = match session.resolve_anchor(&anchor) {
        Ok(position) => position,
        Err(code) => return fail(&mut client, code, "could not resolve anchor").await,
    };
    if let Some(msg) = session.awareness_message() {
        document.send(msg).await?;
    }
    send_json(
        &mut client,
        json!({ "type": "started", "guid": file.guid, "position": position }),
    )
    .await?;

    // 5. Pump frames until end/disconnect/timeout.
    let (mut cl_tx, mut cl_rx) = client.split();
    let mut flush_tick = interval(FLUSH_INTERVAL);
    let deadline = Instant::now() + MAX_SESSION;
    let mut last_activity = Instant::now();
    let mut error: Option<(&str, String)> = None;

    'main: loop {
        tokio::select! {
            msg = cl_rx.next() => match msg {
                Some(Ok(AxumMsg::Text(text))) => {
                    last_activity = Instant::now();
                    match serde_json::from_str::<ClientFrame>(text.as_str()) {
                        Ok(ClientFrame::Text { text }) => {
                            if session.inserted + session.pending.len() + text.len() > MAX_INSERTED_BYTES {
                                error = Some(("too_large", "session insert limit exceeded".into()));
                                break 'main;
                            }
                            session.pending.push_str(&text);
                            if session.pending.len() >= FLUSH_BYTES {
                                flush(&mut session, &document, &mut cl_tx).await?;
                            }
                        }
                        Ok(ClientFrame::End) => break 'main,
                        Ok(ClientFrame::Start { .. }) => {
                            error = Some(("already_started", "session already started".into()));
                            break 'main;
                        }
                        Err(e) => {
                            error = Some(("bad_frame", e.to_string()));
                            break 'main;
                        }
                    }
                }
                Some(Ok(AxumMsg::Close(_))) | None => break 'main,
                Some(Ok(_)) => {}
                Some(Err(_)) => break 'main,
            },
            msg = document.recv() => match msg {
                Some(data) => {
                    if let Some(epoch) = crate::crdt::epoch_proposal(&data) {
                        // Acknowledge at once (an unacknowledged peer holds the
                        // epoch back for everyone), then follow the document
                        // into the new epoch and keep streaming there.
                        flush(&mut session, &document, &mut cl_tx).await?;
                        let caret = session.caret_offset();
                        document.send(crate::crdt::epoch_acknowledgement(epoch)?).await?;
                        match rejoin_epoch(&state, &doc_id, epoch, &mut session).await {
                            Ok(rejoined) => {
                                document = rejoined;
                                session.pin_caret(caret);
                                if let Some(msg) = session.awareness_message() {
                                    document.send(msg).await?;
                                }
                            }
                            Err(e) => {
                                tracing::warn!("stream: could not follow document epoch: {e}");
                                error = Some(("sync_lost", "document epoch changed".into()));
                                break 'main;
                            }
                        }
                        continue;
                    }
                    if let Some(reply) = session.handle_upstream(&data) {
                        document.send(reply).await?;
                    }
                }
                None => {
                    error = Some(("sync_lost", "document connection closed".into()));
                    break 'main;
                }
            },
            _ = flush_tick.tick() => {
                flush(&mut session, &document, &mut cl_tx).await?;
                if last_activity.elapsed() > IDLE_TIMEOUT {
                    error = Some(("idle_timeout", "no frames received; committing".into()));
                    break 'main;
                }
                if Instant::now() > deadline {
                    error = Some(("session_timeout", "max session length reached; committing".into()));
                    break 'main;
                }
            },
        }
    }

    // 6. Commit: flush the tail, drop the caret, attribute + audit the write.
    flush(&mut session, &document, &mut cl_tx).await.ok();
    // `CrdtConnection::send` queues work into the document task. Before
    // returning `done`, round-trip an opaque sync-status marker so REST reads
    // are guaranteed to observe every queued stream update.
    let flush_barrier = uuid::Uuid::new_v4().as_bytes().to_vec();
    document
        .send(YMsg::Custom(crate::crdt::SYNC_STATUS_MESSAGE, flush_barrier.clone()).encode_v1())
        .await?;
    loop {
        let Some(message) = document.recv().await else {
            anyhow::bail!("document connection closed before stream flush acknowledgement");
        };
        if matches!(
            crate::safe_yrs::decode_v1::<YMsg>(&message),
            Ok(YMsg::Custom(crate::crdt::SYNC_STATUS_MESSAGE, payload))
                if payload == flush_barrier
        ) {
            break;
        }
    }
    if let Some(msg) = session.clear_awareness_message() {
        document.send(msg).await.ok();
    }

    let after_content = session.content();
    let mut audit_id = None;
    if session.inserted > 0 {
        notes::best_effort_index(&state, &vault_id, &file.guid, &path, &after_content).await;
        notes::mark_note_write(&state, &vault_id, &principal).await;
        audit_id = audit::record(
            &state,
            &principal,
            &vault_id,
            AuditEntry::new("stream", &path)
                .before(before_content)
                .after(after_content),
        )
        .await;
    }

    let mut client = cl_tx.reunite(cl_rx)?;
    if let Some((code, message)) = error {
        send_json(
            &mut client,
            json!({ "type": "error", "code": code, "message": message }),
        )
        .await
        .ok();
    }
    send_json(
        &mut client,
        json!({ "type": "done", "auditId": audit_id, "inserted": session.inserted }),
    )
    .await
    .ok();
    client.close().await.ok();
    Ok(())
}

/// Receive the next JSON text frame from the client, skipping control frames.
async fn next_client_frame(client: &mut WebSocket) -> anyhow::Result<Option<ClientFrame>> {
    while let Some(msg) = client.next().await {
        match msg? {
            AxumMsg::Text(text) => {
                return Ok(Some(serde_json::from_str::<ClientFrame>(text.as_str())?))
            }
            AxumMsg::Close(_) => return Ok(None),
            _ => {}
        }
    }
    Ok(None)
}

async fn send_json(client: &mut WebSocket, value: JsonValue) -> anyhow::Result<()> {
    client.send(AxumMsg::Text(value.to_string().into())).await?;
    Ok(())
}

async fn fail(client: &mut WebSocket, code: &str, message: &str) -> anyhow::Result<()> {
    send_json(
        client,
        json!({ "type": "error", "code": code, "message": message }),
    )
    .await
    .ok();
    client.close().await.ok();
    Ok(())
}

fn error_code(e: &AppError) -> &'static str {
    match e {
        AppError::NotFound => "not_found",
        AppError::Forbidden => "forbidden",
        AppError::Unauthorized => "unauthorized",
        AppError::BadRequest(_) => "bad_request",
        AppError::Conflict(_) => "conflict",
        _ => "internal",
    }
}

async fn flush(
    session: &mut DocSession,
    document: &CrdtConnection,
    cl_tx: &mut SplitSink<WebSocket, AxumMsg>,
) -> anyhow::Result<()> {
    let Some(out) = session.flush() else {
        return Ok(());
    };
    document.send(out.update_message).await?;
    if let Some(awareness) = out.awareness_message {
        document.send(awareness).await?;
    }
    cl_tx
        .send(AxumMsg::Text(
            json!({ "type": "ack", "applied": out.applied })
                .to_string()
                .into(),
        ))
        .await?;
    Ok(())
}

/// Drive the y-protocols handshake until we hold the doc's full state: send our
/// SyncStep1, answer the server's, and apply its SyncStep2.
async fn initial_sync(
    session: &mut DocSession,
    document: &mut CrdtConnection,
) -> anyhow::Result<()> {
    document.send(session.sync_step1_message()).await?;
    while !session.synced {
        let Some(data) = document.recv().await else {
            anyhow::bail!("document connection closed during initial sync");
        };
        if let Some(reply) = session.handle_upstream(&data) {
            document.send(reply).await?;
        }
    }
    Ok(())
}

/// Join `doc_id` again once `epoch` is active, with a fresh replica, because
/// the new epoch's items share no identity with the retired ones.
async fn rejoin_epoch(
    state: &AppState,
    doc_id: &str,
    epoch: u64,
    session: &mut DocSession,
) -> anyhow::Result<CrdtConnection> {
    let deadline = Instant::now() + EPOCH_ACTIVATION_TIMEOUT;
    while state.documents.current_epoch(doc_id).await? < epoch {
        if Instant::now() > deadline {
            anyhow::bail!("epoch {epoch} was not activated in time");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let mut document = state
        .documents
        .connect(doc_id, Level::Full, None, None)
        .await?;
    session.reset_replica();
    timeout(SETUP_TIMEOUT, initial_sync(session, &mut document))
        .await
        .map_err(|_| anyhow::anyhow!("initial sync timed out"))??;
    Ok(document)
}

struct FlushOut {
    update_message: Vec<u8>,
    awareness_message: Option<Vec<u8>>,
    applied: usize,
}

/// The server-side peer of one stream session: a local replica of the note's
/// Yjs doc, the sticky insertion position, and the cursor's awareness state.
/// All methods are synchronous; the network loop owns the awaits.
struct DocSession {
    doc: Doc,
    /// Insertion point; sticks to the last streamed character (Assoc::Before)
    /// so concurrent edits shift it instead of splitting the stream.
    pos: Option<StickyIndex>,
    pending: String,
    inserted: usize,
    synced: bool,
    /// Lamport-ish clock for our awareness entry (y-protocols requirement).
    awareness_clock: u32,
    user_json: JsonValue,
}

impl DocSession {
    fn new(principal: &ApiPrincipal) -> Self {
        let (name, color) = match &principal.actor {
            ApiActor::Cursor(cursor) => (cursor.name.clone(), cursor_color(&cursor.app_id)),
            ApiActor::User => (principal.user.display_name.clone(), cursor_color("user")),
        };
        let doc = Doc::new();
        doc.get_or_insert_text("contents");
        DocSession {
            doc,
            pos: None,
            pending: String::new(),
            inserted: 0,
            synced: false,
            awareness_clock: 0,
            user_json: json!({
                "name": name,
                "color": color,
                "colorLight": format!("{color}33"),
            }),
        }
    }

    fn content(&self) -> String {
        let text = self.doc.get_or_insert_text("contents");
        let txn = self.doc.transact();
        text.get_string(&txn)
    }

    /// Byte offset of the insertion point, if one is pinned.
    fn caret_offset(&self) -> Option<u32> {
        let txn = self.doc.transact();
        self.pos
            .as_ref()
            .and_then(|pos| pos.get_offset(&txn))
            .map(|offset| offset.index)
    }

    /// Start over with an empty replica for a new document epoch.
    fn reset_replica(&mut self) {
        self.doc = Doc::new();
        self.doc.get_or_insert_text("contents");
        self.pos = None;
        self.synced = false;
    }

    /// Pin the insertion point at `offset` in the current replica (moved back
    /// to a character boundary), or leave it unpinned to append.
    fn pin_caret(&mut self, offset: Option<u32>) {
        let Some(offset) = offset else {
            return;
        };
        let content = self.content();
        let mut index = (offset as usize).min(content.len());
        while !content.is_char_boundary(index) {
            index -= 1;
        }
        let text = self.doc.get_or_insert_text("contents");
        let mut txn = self.doc.transact_mut();
        self.pos = text.sticky_index(&mut txn, index as u32, Assoc::Before);
    }

    fn sync_step1_message(&self) -> Vec<u8> {
        let sv = self.doc.transact().state_vector();
        YMsg::Sync(SyncMessage::SyncStep1(sv)).encode_v1()
    }

    /// Handle one binary y-protocols frame from the document peer.
    fn handle_upstream(&mut self, data: &[u8]) -> Option<Vec<u8>> {
        let msg = match crate::safe_yrs::decode_message(data) {
            Ok(msg) => msg,
            Err(e) => {
                tracing::debug!("stream: undecodable Yjs frame: {e}");
                return None;
            }
        };
        match msg {
            YMsg::Sync(SyncMessage::SyncStep1(sv)) => {
                let update = self.doc.transact().encode_state_as_update_v1(&sv);
                Some(YMsg::Sync(SyncMessage::SyncStep2(update)).encode_v1())
            }
            YMsg::Sync(SyncMessage::SyncStep2(update)) => {
                self.apply_update(&update);
                self.synced = true;
                None
            }
            YMsg::Sync(SyncMessage::Update(update)) => {
                self.apply_update(&update);
                None
            }
            YMsg::AwarenessQuery => self.awareness_message(),
            YMsg::Awareness(_) | YMsg::Auth(_) | YMsg::Custom(..) => None,
        }
    }

    fn apply_update(&mut self, update: &[u8]) {
        let Ok(update) = crate::safe_yrs::validate_update(update) else {
            tracing::debug!("stream: undecodable doc update");
            return;
        };
        let mut txn = self.doc.transact_mut();
        txn.apply_update(update);
    }

    /// Pin the sticky insertion position from the requested anchor. Returns the
    /// resolved byte offset (for the `started` frame) or an error code.
    fn resolve_anchor(&mut self, anchor: &Anchor) -> Result<usize, &'static str> {
        let text = self.doc.get_or_insert_text("contents");
        let content = {
            let txn = self.doc.transact();
            text.get_string(&txn)
        };
        // The doc uses yrs' default OffsetKind::Bytes, so `str` byte offsets
        // are valid insert indices as long as they sit on a char boundary.
        let index = match anchor {
            Anchor::Append => content.len(),
            Anchor::After { text: anchor_text } => {
                if anchor_text.is_empty() {
                    return Err("anchor_empty");
                }
                match content.find(anchor_text.as_str()) {
                    Some(at) => at + anchor_text.len(),
                    None => return Err("anchor_not_found"),
                }
            }
            Anchor::Offset { offset } => {
                let offset = (*offset).min(content.len());
                if !content.is_char_boundary(offset) {
                    return Err("offset_not_char_boundary");
                }
                offset
            }
        };
        let mut txn = self.doc.transact_mut();
        self.pos = text.sticky_index(&mut txn, index as u32, Assoc::Before);
        Ok(index)
    }

    /// Apply the pending batch at the sticky position and emit the wire frames.
    fn flush(&mut self) -> Option<FlushOut> {
        if self.pending.is_empty() {
            return None;
        }
        let batch = std::mem::take(&mut self.pending);
        let text = self.doc.get_or_insert_text("contents");
        let before = {
            let mut txn = self.doc.transact_mut();
            let before = txn.state_vector();
            let index = self
                .pos
                .as_ref()
                .and_then(|pos| pos.get_offset(&txn))
                .map(|offset| offset.index)
                .unwrap_or_else(|| text.len(&txn));
            text.insert(&mut txn, index, &batch);
            // Re-pin to the end of what we just wrote so the next batch chains
            // after it even when humans type at the same spot.
            self.pos = text
                .sticky_index(&mut txn, index + batch.len() as u32, Assoc::Before)
                .or(self.pos.take());
            before
        };
        let update = self.doc.transact().encode_state_as_update_v1(&before);
        self.inserted += batch.len();
        Some(FlushOut {
            update_message: YMsg::Sync(SyncMessage::Update(update)).encode_v1(),
            awareness_message: self.awareness_message(),
            applied: batch.len(),
        })
    }

    /// Awareness frame publishing the cursor's name/color + caret position in
    /// the shape `RemoteSelections.ts` consumes from `awareness.getStates()`.
    fn awareness_message(&mut self) -> Option<Vec<u8>> {
        let pos = self.pos.as_ref()?;
        let caret = relative_position_json(pos);
        let state = json!({
            "user": self.user_json,
            "cursor": { "anchor": caret, "head": caret },
        });
        Some(self.encode_awareness(&state.to_string()))
    }

    /// Awareness removal frame (state "null"), clearing the caret on clients.
    fn clear_awareness_message(&mut self) -> Option<Vec<u8>> {
        if self.awareness_clock == 0 {
            return None; // never published
        }
        Some(self.encode_awareness("null"))
    }

    /// Hand-encoded y-protocols awareness message for our single client entry:
    /// varint client count, then (client id, clock, JSON string) per client.
    fn encode_awareness(&mut self, state_json: &str) -> Vec<u8> {
        self.awareness_clock += 1;
        let mut payload = EncoderV1::new();
        payload.write_var(1u32);
        payload.write_var(self.doc.client_id());
        payload.write_var(self.awareness_clock);
        payload.write_string(state_json);
        let mut msg = EncoderV1::new();
        msg.write_var(MSG_AWARENESS);
        msg.write_buf(payload.to_vec());
        msg.to_vec()
    }
}

/// Serialize a yrs sticky index in the JSON shape yjs'
/// `createRelativePositionFromJSON` expects: `{type, tname, item, assoc}` with
/// `{client, clock}` IDs. (yrs' own serde format is not yjs-compatible.)
fn relative_position_json(pos: &StickyIndex) -> JsonValue {
    let assoc = match pos.assoc {
        Assoc::After => 0,
        Assoc::Before => -1,
    };
    match pos.scope() {
        IndexScope::Relative(id) => json!({
            "type": null,
            "tname": null,
            "item": { "client": id.client, "clock": id.clock },
            "assoc": assoc,
        }),
        IndexScope::Nested(id) => json!({
            "type": { "client": id.client, "clock": id.clock },
            "tname": null,
            "item": null,
            "assoc": assoc,
        }),
        IndexScope::Root(name) => json!({
            "type": null,
            "tname": name.as_ref(),
            "item": null,
            "assoc": assoc,
        }),
    }
}

/// Stable caret color per cursor, derived from its app id.
fn cursor_color(app_id: &str) -> &'static str {
    const PALETTE: [&str; 8] = [
        "#e91e63", "#9c27b0", "#3f51b5", "#2196f3", "#009688", "#4caf50", "#ff9800", "#795548",
    ];
    let hash: u32 = app_id.bytes().fold(2166136261u32, |acc, b| {
        (acc ^ b as u32).wrapping_mul(16777619)
    });
    PALETTE[(hash % PALETTE.len() as u32) as usize]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entities::{remote_cursors, users};

    fn test_principal() -> ApiPrincipal {
        let now = 0;
        ApiPrincipal {
            user: users::Model {
                id: "u1".into(),
                oidc_issuer: "iss".into(),
                oidc_subject: "sub".into(),
                email: "a@x".into(),
                git_email: None,
                display_name: "Alice".into(),
                picture_url: None,
                avatar_url_override: None,
                created_at: now,
            },
            actor: ApiActor::Cursor(remote_cursors::Model {
                id: "c1".into(),
                vault_id: "v1".into(),
                app_id: "app123".into(),
                name: "Claude".into(),
                token_hash: "hash".into(),
                created_by: "u1".into(),
                plugin_id: None,
                created_at: now,
                updated_at: now,
            }),
        }
    }

    /// Simulate the remote document peer: apply updates and hand
    /// us its state as a SyncStep2.
    fn seeded_remote(content: &str) -> (Doc, Vec<u8>) {
        let doc = Doc::new();
        let text = doc.get_or_insert_text("contents");
        {
            let mut txn = doc.transact_mut();
            text.insert(&mut txn, 0, content);
        }
        let update = doc
            .transact()
            .encode_state_as_update_v1(&yrs::StateVector::default());
        let step2 = YMsg::Sync(SyncMessage::SyncStep2(update)).encode_v1();
        (doc, step2)
    }

    fn remote_content(doc: &Doc) -> String {
        let text = doc.get_or_insert_text("contents");
        let txn = doc.transact();
        text.get_string(&txn)
    }

    fn apply_flush(remote: &Doc, out: &FlushOut) {
        let msg = crate::safe_yrs::decode_v1::<YMsg>(&out.update_message).unwrap();
        let YMsg::Sync(SyncMessage::Update(update)) = msg else {
            panic!("expected sync update message");
        };
        let mut txn = remote.transact_mut();
        txn.apply_update(crate::safe_yrs::decode_v1::<yrs::Update>(&update).unwrap());
    }

    fn synced_session(content: &str) -> (DocSession, Doc) {
        let (remote, step2) = seeded_remote(content);
        let mut session = DocSession::new(&test_principal());
        assert!(session.handle_upstream(&step2).is_none());
        assert!(session.synced);
        assert_eq!(session.content(), content);
        (session, remote)
    }

    #[test]
    fn handshake_replies_step2_to_step1() {
        let mut session = DocSession::new(&test_principal());
        let step1 = YMsg::Sync(SyncMessage::SyncStep1(yrs::StateVector::default())).encode_v1();
        let reply = session.handle_upstream(&step1).expect("reply");
        assert!(matches!(
            crate::safe_yrs::decode_v1::<YMsg>(&reply).unwrap(),
            YMsg::Sync(SyncMessage::SyncStep2(_))
        ));
    }

    #[test]
    fn append_anchor_streams_to_end() {
        let (mut session, remote) = synced_session("# Title\n");
        assert_eq!(session.resolve_anchor(&Anchor::Append), Ok(8));
        session.pending.push_str("Hello ");
        apply_flush(&remote, &session.flush().unwrap());
        session.pending.push_str("world");
        apply_flush(&remote, &session.flush().unwrap());
        assert_eq!(remote_content(&remote), "# Title\nHello world");
        assert_eq!(session.inserted, 11);
    }

    #[test]
    fn after_anchor_inserts_behind_match() {
        let (mut session, remote) = synced_session("intro\n## Draft\noutro");
        let pos = session
            .resolve_anchor(&Anchor::After {
                text: "## Draft".into(),
            })
            .unwrap();
        assert_eq!(pos, "intro\n## Draft".len());
        session.pending.push_str("\nstreamed");
        apply_flush(&remote, &session.flush().unwrap());
        assert_eq!(remote_content(&remote), "intro\n## Draft\nstreamed\noutro");
    }

    #[test]
    fn anchor_errors() {
        let (mut session, _remote) = synced_session("hi 🦀 end");
        assert_eq!(
            session.resolve_anchor(&Anchor::After {
                text: "missing".into()
            }),
            Err("anchor_not_found")
        );
        // Offset inside the 4-byte crab is not a char boundary.
        assert_eq!(
            session.resolve_anchor(&Anchor::Offset { offset: 4 }),
            Err("offset_not_char_boundary")
        );
        // Past-the-end offsets clamp to append ("hi 🦀 end" is 11 bytes).
        assert_eq!(
            session.resolve_anchor(&Anchor::Offset { offset: 999 }),
            Ok(11)
        );
    }

    #[test]
    fn concurrent_remote_insert_shifts_stream_position() {
        let (mut session, remote) = synced_session("abc");
        session.resolve_anchor(&Anchor::Append).unwrap();
        session.pending.push_str("XY");
        apply_flush(&remote, &session.flush().unwrap());
        assert_eq!(remote_content(&remote), "abcXY");

        // A human prepends text concurrently; the session applies the update.
        let remote_update = {
            let text = remote.get_or_insert_text("contents");
            let mut txn = remote.transact_mut();
            let before = txn.state_vector();
            text.insert(&mut txn, 0, "0123");
            drop(txn);
            remote.transact().encode_state_as_update_v1(&before)
        };
        session.handle_upstream(&YMsg::Sync(SyncMessage::Update(remote_update)).encode_v1());
        assert_eq!(session.content(), "0123abcXY");

        // The stream keeps chaining after its own last character.
        session.pending.push('Z');
        apply_flush(&remote, &session.flush().unwrap());
        assert_eq!(remote_content(&remote), "0123abcXYZ");
    }

    #[test]
    fn awareness_message_has_remote_selections_shape() {
        let (mut session, _remote) = synced_session("abc");
        session.resolve_anchor(&Anchor::Append).unwrap();
        let msg = session.awareness_message().expect("awareness");

        // The frame must round-trip through the y-protocols decoder…
        let decoded = crate::safe_yrs::decode_v1::<YMsg>(&msg).unwrap();
        let YMsg::Awareness(_) = decoded else {
            panic!("expected awareness message");
        };
        // …and clocks must advance per publish.
        assert_eq!(session.awareness_clock, 1);
        session.awareness_message().unwrap();
        assert_eq!(session.awareness_clock, 2);

        // Clearing publishes the protocol's "null" state.
        let cleared = session.clear_awareness_message().unwrap();
        assert!(matches!(
            crate::safe_yrs::decode_v1::<YMsg>(&cleared).unwrap(),
            YMsg::Awareness(_)
        ));
    }

    #[test]
    fn relative_position_json_matches_yjs_shape() {
        let (mut session, _remote) = synced_session("abc");
        session.resolve_anchor(&Anchor::Append).unwrap();
        let value = relative_position_json(session.pos.as_ref().unwrap());
        // Anchored after "c": an item-relative position with left association.
        assert_eq!(value["assoc"], json!(-1));
        assert!(value["item"]["client"].is_u64());
        assert!(value["item"]["clock"].is_u64());
        assert!(value["type"].is_null());
        assert!(value["tname"].is_null());

        // Empty doc: falls back to the root type name.
        let (mut empty, _remote) = synced_session("");
        empty.resolve_anchor(&Anchor::Append).unwrap();
        let value = relative_position_json(empty.pos.as_ref().unwrap());
        assert_eq!(value["tname"], json!("contents"));
        assert!(value["item"].is_null());
    }

    #[test]
    fn cursor_color_is_stable() {
        assert_eq!(cursor_color("app123"), cursor_color("app123"));
        assert!(cursor_color("anything").starts_with('#'));
    }
}
