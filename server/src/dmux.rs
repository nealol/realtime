//! Bounded multiplexing endpoint for native CRDT sync.
//!
//! Each virtual channel is an in-process document session. Clients can therefore
//! sync a bounded shard of documents over one physical WebSocket without any
//! server-side socket fan-out.
//!
//! Each `OPEN` frame
//! carries the per-doc path+query (`/d/{docId}/ws/{docId}?token=…`) the client
//! would otherwise have connected to. The native engine validates the opaque,
//! document-scoped token before opening the channel.
//!
//! Wire frames mirror `src/sync/mux.ts` (ints are lib0 var-uints):
//!   OPEN     = [1][channel][varString pathAndQuery]   client -> server
//!   OPEN_OK  = [2][channel]                            server -> client
//!   OPEN_ERR = [3][channel][reason]                    server -> client
//!   DATA     = [4][channel][raw yjs bytes …]           both directions
//!   CLOSE    = [5][channel]                            both directions
//!
//! OPEN_ERR's trailing reason is optional on the wire: clients that predate
//! it stop reading after the channel id and treat every rejection alike.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use axum::{
    extract::{
        ws::{Message as AxumMsg, WebSocket, WebSocketUpgrade},
        State,
    },
    response::Response,
};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::time::{interval, Duration, Instant, MissedTickBehavior};
use url::Url;

use crate::crdt::{read_varint, Attribution, CrdtConnection, CrdtError};
use crate::state::AppState;

const FRAME_OPEN: u64 = 1;
const FRAME_OPEN_OK: u64 = 2;
const FRAME_OPEN_ERR: u64 = 3;
const FRAME_DATA: u64 = 4;
const FRAME_CLOSE: u64 = 5;
const FRAME_PING: u64 = 6;
const FRAME_PONG: u64 = 7;

/// Channel id reserved for connection-level control frames (PING/PONG).
const CONTROL_CHANNEL: u64 = 0;

/// OPEN_ERR reasons. Admission pressure is the only one that should slow a
/// client's other opens on this connection.
const OPEN_ERR_REJECTED: u64 = 1;
/// The token cannot open this document: unknown, expired (this process's
/// grants are in memory), minted for another document, or naming an epoch the
/// document is no longer at. A freshly minted token can succeed.
const OPEN_ERR_TOKEN: u64 = 2;
/// The document could not be opened right now; the same token may succeed later.
const OPEN_ERR_UNAVAILABLE: u64 = 3;

/// Outbound queue depth per direction; bounds memory if one side stalls.
const CHANNEL_CAPACITY: usize = 16;
/// Hard fan-out bound for one client connection.
const MAX_CHANNELS: usize = 512;
/// Bound OPEN parsing/allocation and the request forwarded to tungstenite.
const MAX_OPEN_PATH_BYTES: usize = 4_096;
/// Match the native CRDT protocol limit. Queue depth and channel fan-out are
/// deliberately small enough that backpressure resets happen before large
/// frames can accumulate across the physical connection.
const MAX_DATA_FRAME_BYTES: usize = crate::crdt::MAX_UPDATE_BYTES;
/// Bound dial-task creation even when a client immediately closes each channel.
const MAX_OPENS_PER_WINDOW: usize = 128;
/// A physical connection is cheap to replace. Cap its lifetime channel churn
/// so rejected/short-lived OPEN attempts cannot retain unbounded history.
const MAX_OPENS_PER_CONNECTION: usize = 4_096;
const OPEN_RATE_WINDOW: Duration = Duration::from_secs(10);
/// The browser client sends mux PING frames every five seconds.
const CLIENT_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
/// Longest the read loop waits to queue a control reply. A client that has
/// not drained its socket for this long is not reading, so the connection is
/// closed (the client reconnects) instead of leaving the loop parked.
const CONTROL_SEND_TIMEOUT: Duration = CLIENT_IDLE_TIMEOUT;

struct OpenLimiter {
    seen_channels: HashSet<u64>,
    window_started: Instant,
    opens_in_window: usize,
    opens_total: usize,
}

impl OpenLimiter {
    fn new(now: Instant) -> Self {
        Self {
            seen_channels: HashSet::new(),
            window_started: now,
            opens_in_window: 0,
            opens_total: 0,
        }
    }

    fn exhausted(&self) -> bool {
        self.opens_total >= MAX_OPENS_PER_CONNECTION
    }

    fn admit(&mut self, channel: u64, active_channels: usize, now: Instant) -> bool {
        if now.duration_since(self.window_started) >= OPEN_RATE_WINDOW {
            self.window_started = now;
            self.opens_in_window = 0;
        }
        if channel == CONTROL_CHANNEL
            || self.seen_channels.contains(&channel)
            || active_channels >= MAX_CHANNELS
            || self.opens_in_window >= MAX_OPENS_PER_WINDOW
            || self.opens_total >= MAX_OPENS_PER_CONNECTION
        {
            return false;
        }
        self.opens_in_window += 1;
        self.opens_total += 1;
        self.seen_channels.insert(channel);
        true
    }
}

/// Accept the multiplexed sync socket.
pub async fn dmux(State(state): State<AppState>, ws: WebSocketUpgrade) -> Response {
    ws.on_upgrade(move |socket| handle(socket, state))
}

async fn handle(client: WebSocket, state: AppState) {
    state.sync_metrics.physical_connection_opened();
    let (mut cl_sink, mut cl_stream) = client.split();

    // All writes to the single client socket are serialized through this queue,
    // since every document session plus control replies share it.
    let (out_tx, mut out_rx) = mpsc::channel::<Vec<u8>>(CHANNEL_CAPACITY);
    let writer = tokio::spawn(async move {
        while let Some(buf) = out_rx.recv().await {
            if cl_sink.send(AxumMsg::Binary(buf.into())).await.is_err() {
                break;
            }
        }
    });

    // channel id -> sender into that channel's native document session.
    let mut channels: HashMap<u64, mpsc::Sender<Vec<u8>>> = HashMap::new();
    let mut open_limiter = OpenLimiter::new(Instant::now());
    let mut last_client_activity = Instant::now();
    let mut idle_tick = interval(Duration::from_secs(5));
    idle_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    // Session tasks notify this loop when their channel has ended, so terminal
    // paths do not leave stale senders behind.
    let (done_tx, mut done_rx) = mpsc::unbounded_channel::<u64>();

    loop {
        tokio::select! {
            msg = cl_stream.next() => {
                let Some(msg) = msg else { break };
                let Ok(msg) = msg else { break };
                let AxumMsg::Binary(buf) = msg else {
                    // Text/ping/pong/close: ping/pong are handled by axum; ignore the rest.
                    continue;
                };
                last_client_activity = Instant::now();
                match parse_frame(&buf) {
                    Some(Frame::Open {
                        channel,
                        path_and_query,
                    }) => {
                        if !open_limiter.admit(channel, channels.len(), Instant::now()) {
                            state.sync_metrics.document_channel_rejected();
                            // Queued even when the outbound queue is full: a
                            // dropped OPEN_ERR leaves the client's channel
                            // waiting to open forever.
                            let rejected = encode_open_err(channel, OPEN_ERR_REJECTED);
                            if !send_control(&out_tx, rejected, CONTROL_SEND_TIMEOUT).await {
                                break;
                            }
                            tracing::warn!(
                                channel,
                                active_channels = channels.len(),
                                "dmux: rejected channel open"
                            );
                            if open_limiter.exhausted() {
                                break;
                            }
                            continue;
                        }
                        // Register synchronously, then load/connect the document in a
                        // task. Cold storage I/O must not block unrelated channels.
                        let (up_tx, up_rx) = mpsc::channel::<Vec<u8>>(CHANNEL_CAPACITY);
                        channels.insert(channel, up_tx);
                        state.sync_metrics.document_channel_opened();
                        let state = state.clone();
                        let out = out_tx.clone();
                        let done = done_tx.clone();
                        tokio::spawn(async move {
                            open_and_pump(state, channel, path_and_query, up_rx, out).await;
                            let _ = done.send(channel);
                        });
                    }
                    Some(Frame::Data { channel, payload }) => {
                        if let Some(up_tx) = channels.get(&channel) {
                            match up_tx.try_send(payload.to_vec()) {
                                Ok(()) => {}
                                // The document session is backed up. Silently dropping a Yjs update
                                // would diverge the doc (the provider thinks it sent it
                                // and won't resend until reconnect), so instead reset the
                                // channel: the provider reconnects and does a full resync.
                                Err(TrySendError::Full(_)) => {
                                    remove_channel(&mut channels, channel, &state);
                                    state.sync_metrics.document_channel_backpressure_reset();
                                    tracing::warn!(channel, "dmux: document backpressure; reset channel");
                                    let reset = encode_simple(FRAME_CLOSE, channel);
                                    if !send_control(&out_tx, reset, CONTROL_SEND_TIMEOUT).await {
                                        break;
                                    }
                                }
                                Err(TrySendError::Closed(_)) => {
                                    remove_channel(&mut channels, channel, &state);
                                }
                            }
                        }
                    }
                    Some(Frame::OversizedData { channel }) => {
                        remove_channel(&mut channels, channel, &state);
                        state.sync_metrics.document_channel_backpressure_reset();
                        tracing::warn!(
                            channel,
                            payload_limit = MAX_DATA_FRAME_BYTES,
                            "dmux: oversized document frame; reset channel"
                        );
                        let reset = encode_simple(FRAME_CLOSE, channel);
                        if !send_control(&out_tx, reset, CONTROL_SEND_TIMEOUT).await {
                            break;
                        }
                    }
                    Some(Frame::Close { channel }) => {
                        // Dropping the sender ends that channel's upstream pump.
                        remove_channel(&mut channels, channel, &state);
                    }
                    Some(Frame::Ping) => {
                        let _ = out_tx.try_send(encode_simple(FRAME_PONG, CONTROL_CHANNEL));
                    }
                    None => {}
                }
            }
            done = done_rx.recv() => {
                if let Some(channel) = done {
                    remove_channel(&mut channels, channel, &state);
                }
            }
            _ = idle_tick.tick() => {
                if last_client_activity.elapsed() > CLIENT_IDLE_TIMEOUT {
                    tracing::debug!("dmux: idle client connection closed");
                    break;
                }
            }
        }
    }

    // Client gone: drop every channel and the writer.
    state.sync_metrics.document_channels_closed(channels.len());
    channels.clear();
    drop(out_tx);
    writer.abort();
    let _ = writer.await;
    state.sync_metrics.physical_connection_closed();
}

/// Queue a control frame from the read loop, waiting at most `wait` for room.
/// `false` means it cannot be delivered: the writer is gone, or the client
/// has stopped reading.
async fn send_control(out_tx: &mpsc::Sender<Vec<u8>>, frame: Vec<u8>, wait: Duration) -> bool {
    matches!(
        tokio::time::timeout(wait, out_tx.send(frame)).await,
        Ok(Ok(()))
    )
}

fn remove_channel(
    channels: &mut HashMap<u64, mpsc::Sender<Vec<u8>>>,
    channel: u64,
    state: &AppState,
) {
    if channels.remove(&channel).is_some() {
        state.sync_metrics.document_channels_closed(1);
    }
}

/// Validate a document-scoped token, open a native session, then pump one
/// virtual channel. Runs in its own task so cold document I/O never blocks the
/// multiplexed client read loop.
async fn open_and_pump(
    state: AppState,
    channel: u64,
    path_and_query: String,
    up_rx: mpsc::Receiver<Vec<u8>>,
    out_tx: mpsc::Sender<Vec<u8>>,
) {
    let Some((document_id, token)) = parse_sync_target(&path_and_query) else {
        let _ = out_tx.send(encode_open_err(channel, OPEN_ERR_TOKEN)).await;
        return;
    };
    let Some(grant) = state.sync_grant(&token, &document_id).await else {
        let _ = out_tx.send(encode_open_err(channel, OPEN_ERR_TOKEN)).await;
        return;
    };
    let attribution = Arc::new(Attribution::new(
        state.clone(),
        &document_id,
        grant.principal,
    ));
    let connection = match state
        .documents
        .connect(
            &document_id,
            grant.level,
            Some(attribution),
            Some(grant.epoch),
        )
        .await
    {
        Ok(connection) => connection,
        Err(error) => {
            tracing::debug!("dmux: native document open failed: {error}");
            let reason = match error {
                CrdtError::EpochTransition { .. } | CrdtError::RetiredEpoch { .. } => {
                    OPEN_ERR_TOKEN
                }
                _ => OPEN_ERR_UNAVAILABLE,
            };
            let _ = out_tx.send(encode_open_err(channel, reason)).await;
            return;
        }
    };

    if out_tx
        .send(encode_simple(FRAME_OPEN_OK, channel))
        .await
        .is_err()
    {
        return;
    }

    pump_channel(channel, connection, up_rx, out_tx).await;
}

fn parse_sync_target(path_and_query: &str) -> Option<(String, String)> {
    let url = Url::parse(&format!("http://localhost{path_and_query}")).ok()?;
    let segments = url.path_segments()?.collect::<Vec<_>>();
    let ["d", document_id, "ws", repeated_id] = segments.as_slice() else {
        return None;
    };
    if document_id.is_empty() || document_id != repeated_id {
        return None;
    }
    let token = url
        .query_pairs()
        .find(|(key, _)| key == "token")
        .map(|(_, value)| value.into_owned())?;
    Some(((*document_id).to_string(), token))
}

/// Pump one channel's frames in both directions until either side closes.
/// Ends when `up_rx` is dropped or the native session closes.
async fn pump_channel(
    channel: u64,
    mut connection: CrdtConnection,
    mut up_rx: mpsc::Receiver<Vec<u8>>,
    out_tx: mpsc::Sender<Vec<u8>>,
) {
    loop {
        tokio::select! {
            from_client = up_rx.recv() => {
                match from_client {
                    Some(bytes) => {
                        if connection.send(bytes).await.is_err() {
                            break;
                        }
                    }
                    None => break, // channel closed or client gone
                }
            }
            from_document = connection.recv() => {
                match from_document {
                    Some(bytes) => {
                        if out_tx.send(encode_data(channel, &bytes)).await.is_err() {
                            break;
                        }
                    }
                    None => break,
                }
            }
        }
    }
    let _ = out_tx.send(encode_simple(FRAME_CLOSE, channel)).await;
}

// --- framing ---------------------------------------------------------------

enum Frame<'a> {
    Open {
        channel: u64,
        path_and_query: String,
    },
    Data {
        channel: u64,
        payload: &'a [u8],
    },
    OversizedData {
        channel: u64,
    },
    Close {
        channel: u64,
    },
    Ping,
}

fn parse_frame(buf: &[u8]) -> Option<Frame<'_>> {
    let (frame_type, rest) = read_varint(buf)?;
    let (channel, rest) = read_varint(rest)?;
    match frame_type {
        t if t == FRAME_OPEN => {
            let (len, rest) = read_varint(rest)?;
            let len = usize::try_from(len).ok()?;
            if len > MAX_OPEN_PATH_BYTES || rest.len() != len {
                return None;
            }
            let path_and_query = String::from_utf8(rest.to_vec()).ok()?;
            Some(Frame::Open {
                channel,
                path_and_query,
            })
        }
        t if t == FRAME_DATA => {
            if rest.len() > MAX_DATA_FRAME_BYTES {
                return Some(Frame::OversizedData { channel });
            }
            Some(Frame::Data {
                channel,
                payload: rest,
            })
        }
        t if t == FRAME_CLOSE && rest.is_empty() => Some(Frame::Close { channel }),
        t if t == FRAME_PING && channel == CONTROL_CHANNEL && rest.is_empty() => Some(Frame::Ping),
        _ => None,
    }
}

/// Parser entry point used by the out-of-process fuzz target.
#[cfg(feature = "fuzzing")]
pub fn fuzz_parse_frame(bytes: &[u8]) {
    if let Some(frame) = parse_frame(bytes) {
        match frame {
            Frame::Open {
                channel,
                path_and_query,
            } => {
                std::hint::black_box((channel, path_and_query));
            }
            Frame::Data { channel, payload } => {
                std::hint::black_box((channel, payload));
            }
            Frame::OversizedData { channel } => {
                std::hint::black_box(channel);
            }
            Frame::Close { channel } => {
                std::hint::black_box(channel);
            }
            Frame::Ping => {}
        }
    }
}

/// Append an unsigned LEB128 var-uint (lib0 encoding).
fn write_varint(buf: &mut Vec<u8>, mut n: u64) {
    loop {
        let mut byte = (n & 0x7f) as u8;
        n >>= 7;
        if n != 0 {
            byte |= 0x80;
        }
        buf.push(byte);
        if n == 0 {
            break;
        }
    }
}

/// A frame with just a type and channel id (OPEN_OK / OPEN_ERR / CLOSE).
fn encode_simple(frame_type: u64, channel: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(4);
    write_varint(&mut buf, frame_type);
    write_varint(&mut buf, channel);
    buf
}

/// OPEN_ERR with its reason code.
fn encode_open_err(channel: u64, reason: u64) -> Vec<u8> {
    let mut buf = encode_simple(FRAME_OPEN_ERR, channel);
    write_varint(&mut buf, reason);
    buf
}

fn encode_data(channel: u64, payload: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(payload.len() + 4);
    write_varint(&mut buf, FRAME_DATA);
    write_varint(&mut buf, channel);
    buf.extend_from_slice(payload);
    buf
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[tokio::test]
    async fn control_frames_wait_for_room_but_not_forever() {
        let (out_tx, mut out_rx) = mpsc::channel::<Vec<u8>>(1);
        out_tx.send(vec![1]).await.unwrap();
        // A client that drains its queue still gets the frame...
        let drain = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            let first = out_rx.recv().await;
            let second = out_rx.recv().await;
            (first, second, out_rx)
        });
        assert!(send_control(&out_tx, vec![2], Duration::from_secs(5)).await);
        let (first, second, out_rx) = drain.await.unwrap();
        assert_eq!((first, second), (Some(vec![1]), Some(vec![2])));
        // ...one that stops reading ends the wait instead of parking it.
        out_tx.send(vec![3]).await.unwrap();
        assert!(!send_control(&out_tx, vec![4], Duration::from_millis(20)).await);
        drop(out_rx);
        assert!(!send_control(&out_tx, vec![5], Duration::from_secs(5)).await);
    }

    #[test]
    fn varint_roundtrips() {
        for n in [0u64, 1, 127, 128, 300, 16384, u32::MAX as u64, u64::MAX] {
            let mut buf = Vec::new();
            write_varint(&mut buf, n);
            let (got, rest) = read_varint(&buf).unwrap();
            assert_eq!(got, n);
            assert!(rest.is_empty());
        }
    }

    #[test]
    fn parses_open_frame() {
        // [type=1][channel=7][len=3]["abc"]
        let mut buf = Vec::new();
        write_varint(&mut buf, FRAME_OPEN);
        write_varint(&mut buf, 7);
        write_varint(&mut buf, 3);
        buf.extend_from_slice(b"abc");
        match parse_frame(&buf) {
            Some(Frame::Open {
                channel,
                path_and_query,
            }) => {
                assert_eq!(channel, 7);
                assert_eq!(path_and_query, "abc");
            }
            _ => panic!("expected open frame"),
        }
    }

    #[test]
    fn parses_data_and_close() {
        let data = encode_data(9, b"hello");
        match parse_frame(&data) {
            Some(Frame::Data { channel, payload }) => {
                assert_eq!(channel, 9);
                assert_eq!(payload, b"hello");
            }
            _ => panic!("expected data frame"),
        }

        let close = encode_simple(FRAME_CLOSE, 4);
        assert!(matches!(
            parse_frame(&close),
            Some(Frame::Close { channel: 4 })
        ));
    }

    #[test]
    fn open_err_carries_a_trailing_reason() {
        let frame = encode_open_err(12, OPEN_ERR_TOKEN);
        let (frame_type, rest) = read_varint(&frame).unwrap();
        let (channel, rest) = read_varint(rest).unwrap();
        let (reason, rest) = read_varint(rest).unwrap();
        assert_eq!(
            (frame_type, channel, reason),
            (FRAME_OPEN_ERR, 12, OPEN_ERR_TOKEN)
        );
        assert!(rest.is_empty());
    }

    #[test]
    fn parses_ping() {
        let ping = encode_simple(FRAME_PING, CONTROL_CHANNEL);
        assert!(matches!(parse_frame(&ping), Some(Frame::Ping)));
    }

    #[test]
    fn rejects_truncated_open() {
        let mut buf = Vec::new();
        write_varint(&mut buf, FRAME_OPEN);
        write_varint(&mut buf, 1);
        write_varint(&mut buf, 10); // claims 10 bytes
        buf.extend_from_slice(b"short");
        assert!(parse_frame(&buf).is_none());
    }

    #[test]
    fn rejects_overflowing_varints_and_trailing_control_bytes() {
        assert!(
            read_varint(&[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x02]).is_none()
        );

        let mut open = Vec::new();
        write_varint(&mut open, FRAME_OPEN);
        write_varint(&mut open, 1);
        write_varint(&mut open, 1);
        open.extend_from_slice(b"xy");
        assert!(parse_frame(&open).is_none());

        let mut close = encode_simple(FRAME_CLOSE, 1);
        close.push(0);
        assert!(parse_frame(&close).is_none());
    }

    #[test]
    fn rejects_oversized_open_path() {
        let mut buf = Vec::new();
        write_varint(&mut buf, FRAME_OPEN);
        write_varint(&mut buf, 1);
        write_varint(&mut buf, (MAX_OPEN_PATH_BYTES + 1) as u64);
        buf.resize(buf.len() + MAX_OPEN_PATH_BYTES + 1, b'x');
        assert!(parse_frame(&buf).is_none());
    }

    #[test]
    fn rejects_oversized_data_payload() {
        let payload = vec![0u8; MAX_DATA_FRAME_BYTES + 1];
        let data = encode_data(1, &payload);
        assert!(matches!(
            parse_frame(&data),
            Some(Frame::OversizedData { channel: 1 })
        ));
    }

    #[test]
    fn accepts_large_native_protocol_frames_without_forcing_reconnect() {
        let payload = vec![7u8; 2 * 1024 * 1024];
        let data = encode_data(9, &payload);
        match parse_frame(&data) {
            Some(Frame::Data {
                channel: 9,
                payload: decoded,
            }) => assert_eq!(decoded, payload),
            _ => panic!("expected a complete large DATA frame"),
        }
    }

    #[test]
    fn sync_target_requires_matching_document_ids_and_token() {
        assert_eq!(
            parse_sync_target("/d/vault__doc/ws/vault__doc?token=secret"),
            Some(("vault__doc".to_string(), "secret".to_string()))
        );
        assert!(parse_sync_target("/d/a/ws/b?token=secret").is_none());
        assert!(parse_sync_target("/d/a/ws/a").is_none());
        assert!(parse_sync_target("/other/a/ws/a?token=secret").is_none());
        assert!(parse_sync_target("//attacker.example/d/a/ws/a?token=secret").is_none());
    }

    #[test]
    fn open_limiter_rejects_control_duplicates_rate_and_fanout() {
        let start = Instant::now();
        let mut limiter = OpenLimiter::new(start);
        assert!(!limiter.admit(CONTROL_CHANNEL, 0, start));
        assert!(limiter.admit(1, 0, start));
        assert!(!limiter.admit(1, 0, start));

        let mut rate_limiter = OpenLimiter::new(start);
        for channel in 1..=MAX_OPENS_PER_WINDOW as u64 {
            assert!(rate_limiter.admit(channel, 0, start));
        }
        assert!(!rate_limiter.admit((MAX_OPENS_PER_WINDOW + 1) as u64, 0, start));
        for channel in (MAX_OPENS_PER_WINDOW + 2)..=(MAX_OPENS_PER_WINDOW + 100) {
            assert!(!rate_limiter.admit(channel as u64, 0, start));
        }
        assert_eq!(
            rate_limiter.opens_in_window, MAX_OPENS_PER_WINDOW,
            "rejected retries must not consume admission slots"
        );
        assert!(rate_limiter.admit(
            (MAX_OPENS_PER_WINDOW + 101) as u64,
            0,
            start + OPEN_RATE_WINDOW
        ));

        let mut fanout_limiter = OpenLimiter::new(start);
        let mut now = start;
        for active in 0..MAX_CHANNELS {
            if active > 0 && active % MAX_OPENS_PER_WINDOW == 0 {
                now += OPEN_RATE_WINDOW;
            }
            assert!(fanout_limiter.admit((active + 1) as u64, active, now));
        }
        now += OPEN_RATE_WINDOW;
        assert!(!fanout_limiter.admit((MAX_CHANNELS + 1) as u64, MAX_CHANNELS, now));

        let mut lifetime_limiter = OpenLimiter::new(start);
        let mut now = start;
        for channel in 1..=MAX_OPENS_PER_CONNECTION as u64 {
            if channel > 1 && (channel - 1) % MAX_OPENS_PER_WINDOW as u64 == 0 {
                now += OPEN_RATE_WINDOW;
            }
            assert!(lifetime_limiter.admit(channel, 0, now));
        }
        now += OPEN_RATE_WINDOW;
        assert!(lifetime_limiter.exhausted());
        assert!(!lifetime_limiter.admit(MAX_OPENS_PER_CONNECTION as u64 + 1, 0, now));
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(1_024))]

        #[test]
        fn fuzz_every_u64_has_one_bounded_varint_roundtrip(value in any::<u64>()) {
            let mut encoded = Vec::new();
            write_varint(&mut encoded, value);
            prop_assert!(encoded.len() <= 10);
            let (decoded, rest) = read_varint(&encoded).expect("writer output must decode");
            prop_assert_eq!(decoded, value);
            prop_assert!(rest.is_empty());
        }

        #[test]
        fn fuzz_arbitrary_mux_frames_never_escape_parser_bounds(
            bytes in prop::collection::vec(any::<u8>(), 0..8_192),
        ) {
            if let Some(frame) = parse_frame(&bytes) {
                match frame {
                    Frame::Open { path_and_query, .. } => {
                        prop_assert!(path_and_query.len() <= MAX_OPEN_PATH_BYTES);
                    }
                    Frame::Data { payload, .. } => {
                        prop_assert!(payload.len() <= bytes.len().min(MAX_DATA_FRAME_BYTES));
                    }
                    Frame::OversizedData { .. } | Frame::Close { .. } | Frame::Ping => {}
                }
            }
        }
    }
}
