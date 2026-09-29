import * as encoding from "lib0/encoding";
import * as decoding from "lib0/decoding";
import { handleEpochProposal } from "../documentEpoch";
import type { SyncSocket } from "./RealtimeProvider";

/**
 * Bounded WebSocket multiplexing for Realtime document sync.
 *
 * Each Yjs document has its own `RealtimeProvider`, which would normally open
 * its own WebSocket. With the whole vault synced that is one socket per
 * file. This module packs them into bounded real-socket shards per server origin:
 *
 *  - {@link MuxWebSocket} implements the provider's small `SyncSocket`
 *    contract. Instead of opening a socket it registers a *channel* on a
 *    shared {@link MuxConnection}.
 *  - Each {@link MuxConnection} owns one real socket to `wss://{host}/dmux`,
 *    carries at most 512 channels, and routes frames for that shard.
 *
 * The server (`server/src/dmux.rs`) demultiplexes each channel into an
 * in-process native CRDT session while preserving the provider's wire format
 * and write attribution.
 *
 * Wire frames (binary; ints are lib0 var-uints):
 *   OPEN     = [1][channelId][varString pathAndQuery]   client -> server
 *   OPEN_OK  = [2][channelId]                            server -> client
 *   OPEN_ERR = [3][channelId]                            server -> client
 *   DATA     = [4][channelId][raw yjs bytes …]           both directions
 *   CLOSE    = [5][channelId]                            both directions
 *   PING     = [6][0]                                    client -> server
 *   PONG     = [7][0]                                    server -> client
 *
 * `pathAndQuery` is the per-document URL the provider would otherwise use
 * (`/d/{docId}/ws/{docId}?token=…`); the server validates the same per-document
 * token before opening the in-process CRDT connection.
 *
 * PING/PONG are a mux-level heartbeat on the shared socket (channel 0). The
 * provider has its own per-channel heartbeat, but it can only close the
 * *virtual* socket; a silently-dead real socket (mobile sleep, NAT timeout)
 * keeps `readyState === OPEN`, so every reconnect would re-attach to a dead
 * socket. This heartbeat detects that and tears the real socket down so every
 * channel's provider reconnects and rebuilds it.
 */

const FRAME_OPEN = 1;
const FRAME_OPEN_OK = 2;
const FRAME_OPEN_ERR = 3;
const FRAME_DATA = 4;
const FRAME_CLOSE = 5;
const FRAME_PING = 6;
const FRAME_PONG = 7;
const EPOCH_PROPOSAL_MESSAGE = 103;
const EPOCH_ACK_MESSAGE = 104;

/** Channel id reserved for control frames that are not tied to a channel. */
const CONTROL_CHANNEL = 0;

/** Send a PING this often while the real socket is open. */
const HEARTBEAT_INTERVAL_MS = 5_000;
/** Tear the socket down if no PONG (or any frame) arrives within this window. */
const HEARTBEAT_TIMEOUT_MS = 12_000;
/** Back off virtual reconnects after the server rejects an OPEN. */
const OPEN_RETRY_INITIAL_MS = 1_000;
const OPEN_RETRY_MAX_MS = 10_000;
/** Stay below the server's 128 OPENs / 10s admission window. */
const OPEN_BURST_SIZE = 8;
const OPEN_PACE_INTERVAL_MS = 100;
const OPEN_PACE_JITTER_MS = 25;
/** Keep each client shard comfortably below the server's 1,024-channel ceiling. */
const MAX_CHANNELS_PER_CONNECTION = 512;

/** Close code reported to providers when the shared socket drops. */
const CLOSE_CODE_TRANSPORT = 1006;

const WS_CONNECTING = 0;
const WS_OPEN = 1;
const WS_CLOSING = 2;
const WS_CLOSED = 3;

export function encodeOpen(channelId: number, pathAndQuery: string): Uint8Array {
  const enc = encoding.createEncoder();
  encoding.writeVarUint(enc, FRAME_OPEN);
  encoding.writeVarUint(enc, channelId);
  encoding.writeVarString(enc, pathAndQuery);
  return encoding.toUint8Array(enc);
}

function decodeEpochProposal(payload: Uint8Array): number | null {
  const decoder = decoding.createDecoder(payload);
  if (decoding.readVarUint(decoder) !== EPOCH_PROPOSAL_MESSAGE) return null;
  const body = decoding.readVarUint8Array(decoder);
  if (decoding.hasContent(decoder)) throw new Error("trailing bytes in document epoch proposal");
  const parsed = JSON.parse(new TextDecoder().decode(body)) as { epoch?: unknown };
  if (!Number.isSafeInteger(parsed.epoch) || (parsed.epoch as number) < 0) {
    throw new Error("invalid document epoch proposal");
  }
  return parsed.epoch as number;
}

function encodeEpochAcknowledgement(epoch: number): Uint8Array {
  const encoder = encoding.createEncoder();
  encoding.writeVarUint(encoder, EPOCH_ACK_MESSAGE);
  encoding.writeVarUint8Array(encoder, new TextEncoder().encode(JSON.stringify({ epoch })));
  return encoding.toUint8Array(encoder);
}

export function encodeData(channelId: number, payload: Uint8Array): Uint8Array {
  const enc = encoding.createEncoder();
  encoding.writeVarUint(enc, FRAME_DATA);
  encoding.writeVarUint(enc, channelId);
  encoding.writeUint8Array(enc, payload);
  return encoding.toUint8Array(enc);
}

export function encodeClose(channelId: number): Uint8Array {
  const enc = encoding.createEncoder();
  encoding.writeVarUint(enc, FRAME_CLOSE);
  encoding.writeVarUint(enc, channelId);
  return encoding.toUint8Array(enc);
}

export function encodePing(): Uint8Array {
  const enc = encoding.createEncoder();
  encoding.writeVarUint(enc, FRAME_PING);
  encoding.writeVarUint(enc, CONTROL_CHANNEL);
  return encoding.toUint8Array(enc);
}

export function encodePong(): Uint8Array {
  const enc = encoding.createEncoder();
  encoding.writeVarUint(enc, FRAME_PONG);
  encoding.writeVarUint(enc, CONTROL_CHANNEL);
  return encoding.toUint8Array(enc);
}

export type DecodedFrame =
  | { type: "open"; channelId: number; pathAndQuery: string }
  | { type: "open_ok"; channelId: number }
  | { type: "open_err"; channelId: number }
  | { type: "data"; channelId: number; payload: Uint8Array }
  | { type: "close"; channelId: number }
  | { type: "ping" }
  | { type: "pong" }
  | null;

export function decodeFrame(buf: Uint8Array): DecodedFrame {
  try {
    const dec = decoding.createDecoder(buf);
    const type = decoding.readVarUint(dec);
    const channelId = decoding.readVarUint(dec);
    switch (type) {
      case FRAME_OPEN:
        return { type: "open", channelId, pathAndQuery: decoding.readVarString(dec) };
      case FRAME_OPEN_OK:
        return { type: "open_ok", channelId };
      case FRAME_OPEN_ERR:
        return { type: "open_err", channelId };
      case FRAME_DATA:
        return { type: "data", channelId, payload: decoding.readTailAsUint8Array(dec) };
      case FRAME_CLOSE:
        return { type: "close", channelId };
      case FRAME_PING:
        return { type: "ping" };
      case FRAME_PONG:
        return { type: "pong" };
      default:
        return null;
    }
  } catch {
    return null;
  }
}

/** Pluggable for tests; defaults to the platform `WebSocket`. */
type WebSocketCtor = { new (url: string): WebSocket };
let webSocketCtor: WebSocketCtor = globalThis.WebSocket as unknown as WebSocketCtor;

/** Test-only: swap the WebSocket implementation used for the real connection. */
export function setMuxWebSocketCtor(ctor: WebSocketCtor): void {
  webSocketCtor = ctor;
}

const connectionPools = new Map<string, MuxConnection[]>();

/** Test-only: tear down all shared connections and registries. */
export function resetMuxForTests(): void {
  for (const pool of connectionPools.values()) {
    for (const conn of [...pool]) conn.destroyForTests();
  }
  connectionPools.clear();
}

/** One bounded real-socket shard for a server origin. */
class MuxConnection {
  private ws: WebSocket | null = null;
  private readonly channels = new Map<number, MuxWebSocket>();
  /** OPEN frames awaiting a live socket: channelId -> pathAndQuery. */
  private readonly pendingOpens = new Map<number, string>();
  private nextChannelId = 1;
  private heartbeatTimer: ReturnType<typeof setInterval> | null = null;
  private openRetryTimer: ReturnType<typeof setTimeout> | null = null;
  private openPaceTimer: ReturnType<typeof setTimeout> | null = null;
  private openRetryMs = 0;
  private openBlockedUntil = 0;
  private openBurstRemaining = OPEN_BURST_SIZE;
  /** Timestamp of the last frame received on the real socket. */
  private lastActivity = 0;

  constructor(private readonly url: string) {}

  static acquire(url: string): MuxConnection {
    let pool = connectionPools.get(url);
    if (!pool) {
      pool = [];
      connectionPools.set(url, pool);
    }
    let conn = pool.find((candidate) => candidate.channels.size < MAX_CHANNELS_PER_CONNECTION);
    if (!conn) {
      conn = new MuxConnection(url);
      pool.push(conn);
    }
    return conn;
  }

  register(channel: MuxWebSocket): number {
    const id = this.nextChannelId++;
    this.channels.set(id, channel);
    return id;
  }

  openChannel(channelId: number, pathAndQuery: string): void {
    this.pendingOpens.set(channelId, pathAndQuery);
    this.ensureSocket();
    if (this.ws && this.ws.readyState === WS_OPEN) this.flushOpens();
  }

  sendData(channelId: number, payload: Uint8Array): void {
    if (this.ws && this.ws.readyState === WS_OPEN) {
      this.ws.send(encodeData(channelId, payload));
    }
  }

  closeChannel(channelId: number): void {
    this.pendingOpens.delete(channelId);
    this.channels.delete(channelId);
    if (this.ws && this.ws.readyState === WS_OPEN) {
      this.ws.send(encodeClose(channelId));
    }
    // Nothing left to carry: drop the idle real socket instead of leaking it.
    if (this.channels.size === 0 && this.pendingOpens.size === 0) {
      this.teardown();
    }
  }

  private ensureSocket(): void {
    if (this.ws && (this.ws.readyState === WS_OPEN || this.ws.readyState === WS_CONNECTING)) {
      return;
    }
    const ws = new webSocketCtor(this.url);
    ws.binaryType = "arraybuffer";
    this.ws = ws;
    ws.onopen = () => {
      this.lastActivity = Date.now();
      this.openBurstRemaining = OPEN_BURST_SIZE;
      this.startHeartbeat();
      this.flushOpens();
    };
    ws.onmessage = (event: MessageEvent) => this.receive(new Uint8Array(event.data as ArrayBuffer));
    ws.onclose = () => this.handleDown();
    ws.onerror = () => this.handleDown();
  }

  private flushOpens(): void {
    if (!this.ws || this.ws.readyState !== WS_OPEN) return;
    const waitMs = this.openBlockedUntil - Date.now();
    if (waitMs > 0) {
      this.scheduleOpenRetry(waitMs);
      return;
    }
    this.clearOpenRetryTimer();
    if (this.openPaceTimer !== null) return;

    while (this.openBurstRemaining > 0 && this.sendNextOpen()) {
      this.openBurstRemaining--;
    }
    if (this.pendingOpens.size > 0) this.schedulePacedOpen();
  }

  private sendNextOpen(): boolean {
    if (!this.ws || this.ws.readyState !== WS_OPEN) return false;
    const next = this.pendingOpens.entries().next();
    if (next.done) return false;
    const [channelId, pathAndQuery] = next.value;
    this.pendingOpens.delete(channelId);
    this.ws.send(encodeOpen(channelId, pathAndQuery));
    return true;
  }

  private schedulePacedOpen(): void {
    if (this.openPaceTimer !== null) return;
    const jitter = Math.floor(Math.random() * (OPEN_PACE_JITTER_MS + 1));
    this.openPaceTimer = setTimeout(() => {
      this.openPaceTimer = null;
      if (Date.now() < this.openBlockedUntil) {
        this.flushOpens();
        return;
      }
      this.sendNextOpen();
      if (this.pendingOpens.size > 0) this.schedulePacedOpen();
    }, OPEN_PACE_INTERVAL_MS + jitter);
  }

  private scheduleOpenRetry(waitMs: number): void {
    if (this.openRetryTimer !== null) return;
    this.openRetryTimer = setTimeout(() => {
      this.openRetryTimer = null;
      this.flushOpens();
      this.teardownIfIdle();
    }, waitMs);
  }

  private clearOpenRetryTimer(): void {
    if (this.openRetryTimer !== null) {
      clearTimeout(this.openRetryTimer);
      this.openRetryTimer = null;
    }
  }

  private clearOpenPaceTimer(): void {
    if (this.openPaceTimer !== null) {
      clearTimeout(this.openPaceTimer);
      this.openPaceTimer = null;
    }
  }

  private backOffOpens(): void {
    this.openRetryMs =
      this.openRetryMs === 0
        ? OPEN_RETRY_INITIAL_MS
        : Math.min(this.openRetryMs * 2, OPEN_RETRY_MAX_MS);
    this.openBlockedUntil = Date.now() + this.openRetryMs;
  }

  private resetOpenBackoff(): void {
    this.openRetryMs = 0;
    this.openBlockedUntil = 0;
    this.clearOpenRetryTimer();
  }

  private receive(buf: Uint8Array): void {
    this.lastActivity = Date.now();
    const frame = decodeFrame(buf);
    if (!frame) return;
    switch (frame.type) {
      case "pong":
      case "ping":
        // Liveness only; `lastActivity` is already refreshed above.
        break;
      case "data":
        this.channels.get(frame.channelId)?.deliverData(frame.payload);
        break;
      case "open_ok":
        // OPEN_OK for an earlier admitted channel can arrive after a later
        // OPEN_ERR because upstream dials complete out of order. Only a success
        // after the current cooldown proves the server is admitting opens again.
        if (Date.now() >= this.openBlockedUntil) {
          this.resetOpenBackoff();
          this.flushOpens();
        }
        this.channels.get(frame.channelId)?.deliverOpen();
        break;
      case "open_err": {
        this.backOffOpens();
        const channel = this.channels.get(frame.channelId);
        this.channels.delete(frame.channelId);
        channel?.deliverError();
        channel?.deliverClose(CLOSE_CODE_TRANSPORT);
        this.teardownIfIdle();
        break;
      }
      case "close": {
        const channel = this.channels.get(frame.channelId);
        this.channels.delete(frame.channelId);
        channel?.deliverClose(CLOSE_CODE_TRANSPORT);
        this.teardownIfIdle();
        break;
      }
      default:
        break;
    }
  }

  private teardownIfIdle(): void {
    if (this.channels.size !== 0 || this.pendingOpens.size !== 0) return;
    const waitMs = this.openBlockedUntil - Date.now();
    if (waitMs <= 0) {
      this.teardown();
      return;
    }
    // Keep the socket through the OPEN backoff so a reconnecting channel still
    // honours it; the retry timer drops the socket afterwards if still idle.
    this.scheduleOpenRetry(waitMs);
  }

  private startHeartbeat(): void {
    this.stopHeartbeat();
    this.heartbeatTimer = setInterval(() => this.heartbeatTick(), HEARTBEAT_INTERVAL_MS);
  }

  private stopHeartbeat(): void {
    if (this.heartbeatTimer !== null) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
  }

  /** Periodically ping the real socket; if it goes silent past the timeout,
   * treat it as dead even when no `close` event ever fired (mobile/NAT). */
  private heartbeatTick(): void {
    if (!this.ws || this.ws.readyState !== WS_OPEN) return;
    if (Date.now() - this.lastActivity > HEARTBEAT_TIMEOUT_MS) {
      this.handleDown();
      return;
    }
    try {
      this.ws.send(encodePing());
    } catch {
      this.handleDown();
    }
  }

  /** Real socket dropped: fail every channel so each provider reconnects (which
   * recreates a channel and reopens the shared socket). */
  private handleDown(): void {
    this.stopHeartbeat();
    this.resetOpenBackoff();
    this.clearOpenPaceTimer();
    const channels = [...this.channels.values()];
    this.channels.clear();
    this.pendingOpens.clear();
    const ws = this.ws;
    this.ws = null;
    if (ws) {
      ws.onopen = ws.onmessage = ws.onclose = ws.onerror = null;
      try {
        ws.close();
      } catch {
        /* ignore */
      }
    }
    for (const channel of channels) {
      channel.deliverError();
      channel.deliverClose(CLOSE_CODE_TRANSPORT);
    }
    this.release();
  }

  /** Close the idle real socket without failing channels (there are none). */
  private teardown(): void {
    this.stopHeartbeat();
    this.resetOpenBackoff();
    this.clearOpenPaceTimer();
    const ws = this.ws;
    this.ws = null;
    if (ws) {
      ws.onopen = ws.onmessage = ws.onclose = ws.onerror = null;
      try {
        ws.close();
      } catch {
        /* ignore */
      }
    }
    this.release();
  }

  private release(): void {
    // A provider close callback can synchronously allocate a replacement
    // channel on this connection. Do not unpublish a shard that was reused.
    if (this.channels.size > 0 || this.pendingOpens.size > 0) return;
    const pool = connectionPools.get(this.url);
    if (!pool) return;
    const index = pool.indexOf(this);
    if (index !== -1) pool.splice(index, 1);
    if (pool.length === 0) connectionPools.delete(this.url);
  }

  destroyForTests(): void {
    this.teardown();
    this.channels.clear();
    this.pendingOpens.clear();
  }
}

type WsHandler = ((event: unknown) => void) | null;
type WsMessageHandler = ((event: { data: unknown }) => void) | null;

/**
 * A virtual socket implementing the small surface used by
 * {@link RealtimeProvider}: `binaryType`, `readyState`, `send`, `close`, and
 * the four event handlers. Frames route over a shared {@link MuxConnection}.
 */
export class MuxWebSocket implements SyncSocket {
  static readonly CONNECTING = WS_CONNECTING;
  static readonly OPEN = WS_OPEN;
  static readonly CLOSING = WS_CLOSING;
  static readonly CLOSED = WS_CLOSED;

  readonly CONNECTING = WS_CONNECTING;
  readonly OPEN = WS_OPEN;
  readonly CLOSING = WS_CLOSING;
  readonly CLOSED = WS_CLOSED;

  binaryType = "arraybuffer";
  readyState: number = WS_CONNECTING;
  onopen: WsHandler = null;
  onmessage: WsMessageHandler = null;
  onclose: WsHandler = null;
  onerror: WsHandler = null;

  private readonly conn: MuxConnection;
  private readonly channelId: number;
  private readonly documentId: string;

  constructor(url: string) {
    const parsed = new URL(url);
    const segments = parsed.pathname.split("/");
    this.documentId = decodeURIComponent(segments[2] ?? "");
    const pathAndQuery = parsed.pathname + parsed.search;
    const muxUrl = `${parsed.protocol}//${parsed.host}/dmux`;
    this.conn = MuxConnection.acquire(muxUrl);
    this.channelId = this.conn.register(this);
    this.conn.openChannel(this.channelId, pathAndQuery);
  }

  send(data: Uint8Array | ArrayBuffer): void {
    if (this.readyState !== WS_OPEN) return;
    const bytes = data instanceof Uint8Array ? data : new Uint8Array(data);
    this.conn.sendData(this.channelId, bytes);
  }

  close(): void {
    if (this.readyState === WS_CLOSED) return;
    this.readyState = WS_CLOSED;
    this.conn.closeChannel(this.channelId);
  }

  // --- called by MuxConnection ---------------------------------------------

  deliverOpen(): void {
    if (this.readyState !== WS_CONNECTING) return;
    this.readyState = WS_OPEN;
    this.onopen?.({});
  }

  deliverData(payload: Uint8Array): void {
    try {
      const epoch = decodeEpochProposal(payload);
      if (epoch !== null) {
        handleEpochProposal(this.documentId, epoch);
        this.conn.sendData(this.channelId, encodeEpochAcknowledgement(epoch));
        return;
      }
    } catch {
      this.deliverError();
      this.close();
      return;
    }
    this.onmessage?.({ data: payload });
  }

  deliverError(): void {
    this.onerror?.({});
  }

  deliverClose(code: number = CLOSE_CODE_TRANSPORT): void {
    if (this.readyState === WS_CLOSED) return;
    this.readyState = WS_CLOSED;
    // The provider ignores the code today, but carry it for parity/diagnostics.
    this.onclose?.({ code, reason: "", wasClean: false });
  }
}

export function createMuxSocket(url: string): SyncSocket {
  return new MuxWebSocket(url);
}
