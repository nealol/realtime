import * as Y from "yjs";
import * as encoding from "lib0/encoding";
import * as decoding from "lib0/decoding";
import * as syncProtocol from "y-protocols/sync";
import * as awarenessProtocol from "y-protocols/awareness";
import { DocumentEpochPendingError, clientTokenExpired, type ClientToken } from "./clientToken";
import { handleEpochProposal } from "../documentEpoch";

const MESSAGE_SYNC = 0;
const MESSAGE_AWARENESS = 1;
const MESSAGE_QUERY_AWARENESS = 3;
const MESSAGE_SYNC_STATUS = 102;
const MESSAGE_EPOCH_PROPOSAL = 103;
const MESSAGE_EPOCH_ACKNOWLEDGEMENT = 104;
const MESSAGE_DOCUMENT_INVALIDATED = 105;

const SOCKET_OPEN = 1;

const SOCKET_RETRIES_PER_TOKEN = 3;
const RECONNECT_INITIAL_MS = 500;
const TOKEN_RETRY_MS = 3_000;
/** Cap for the per-document backoff after repeated token failures. */
const MAX_TOKEN_RETRY_MS = 60_000;
/** Retry cadence while the server finishes activating a pending epoch. */
const EPOCH_PENDING_RETRY_MS = 2_000;
const MAX_RECONNECT_MS = 5_000;
/** Probe interval while local changes await the server's durable acknowledgement. */
const HEARTBEAT_MS = 2_000;
/**
 * Probe interval for an idle document. Every document in the vault holds a
 * channel, so a 2s probe cost thousands of round trips a second in a large
 * vault while nothing changed; a dead shared socket is caught by the mux's
 * own 5s ping instead.
 */
const IDLE_HEARTBEAT_MS = 30_000;
/**
 * How long a probe may go unanswered. Channels share one socket, so a large
 * sync message ahead of the reply can legitimately delay it by seconds.
 */
const RESPONSE_TIMEOUT_MS = 10_000;
/**
 * A socket (or mux channel) that never opens must not stall the connect loop.
 * Generous, because mux OPENs are paced client-side: after a network drop a
 * large vault queues hundreds of channels that legitimately open over ~a minute.
 */
const CONNECT_TIMEOUT_MS = 60_000;
const HANDSHAKE_TIMEOUT_MS = 10_000;

export const SYNC_EVENT_STATUS = "connection-status";
export const SYNC_EVENT_LOCAL_CHANGES = "local-changes";
export const SYNC_EVENT_SYNCED = "synced";
export const SYNC_EVENT_DOCUMENT_INVALIDATED = "document-invalidated";

export const SYNC_STATUS_OFFLINE = "offline";
export const SYNC_STATUS_CONNECTING = "connecting";
export const SYNC_STATUS_ERROR = "error";
export const SYNC_STATUS_HANDSHAKING = "handshaking";
export const SYNC_STATUS_CONNECTED = "connected";

export type SyncStatus =
  | typeof SYNC_STATUS_OFFLINE
  | typeof SYNC_STATUS_CONNECTING
  | typeof SYNC_STATUS_ERROR
  | typeof SYNC_STATUS_HANDSHAKING
  | typeof SYNC_STATUS_CONNECTED;

export interface SyncSocket {
  binaryType: string;
  readyState: number;
  onopen: ((event: unknown) => void) | null;
  onmessage: ((event: { data: unknown }) => void) | null;
  onclose: ((event: unknown) => void) | null;
  onerror: ((event: unknown) => void) | null;
  send(data: Uint8Array | ArrayBuffer): void;
  close(): void;
}

export type SyncSocketFactory = (url: string) => SyncSocket;

/**
 * `reason` on a socket's error event when the server refused its token
 * (expired, from before a server restart, or for a retired epoch). A freshly
 * minted token can open the document, so retrying this one is pointless.
 */
export const SOCKET_ERROR_TOKEN_REJECTED = "token-rejected";

export interface RealtimeProviderOptions {
  connect?: boolean;
  awareness?: awarenessProtocol.Awareness;
  socketFactory?: SyncSocketFactory;
  onReadOnlyUpdate?: (update: Uint8Array) => void;
}

type TokenSource = () => Promise<ClientToken>;

type ProviderEventMap = {
  [SYNC_EVENT_STATUS]: SyncStatus;
  [SYNC_EVENT_LOCAL_CHANGES]: boolean;
  [SYNC_EVENT_SYNCED]: boolean;
  [SYNC_EVENT_DOCUMENT_INVALIDATED]: string;
};

type ProviderEvent = keyof ProviderEventMap;
type ProviderListener<K extends ProviderEvent> = (value: ProviderEventMap[K]) => void;
type AnyProviderListener = (value: never) => void;
interface AwarenessChanges {
  added: number[];
  updated: number[];
  removed: number[];
}

/**
 * Realtime's Yjs document transport.
 *
 * The server persists every update before echoing message 102. Consequently,
 * `hasLocalChanges` means more than "sent": it remains true until the server
 * has durably acknowledged the corresponding local document version.
 */
export class RealtimeProvider {
  readonly awareness: awarenessProtocol.Awareness;

  status: SyncStatus = SYNC_STATUS_OFFLINE;
  clientToken: ClientToken | null = null;
  lastConnectionError: string | null = null;

  private readonly listeners = new Map<ProviderEvent, Set<AnyProviderListener>>();
  private readonly socketFactory: SyncSocketFactory;
  private readonly onReadOnlyUpdate?: (update: Uint8Array) => void;
  private socket: SyncSocket | null = null;
  private connectTask: Promise<void> | null = null;
  private connectTaskLifecycle = -1;
  private completeAttempt: ((connected: boolean) => void) | null = null;
  private lifecycle = 0;
  private shouldConnect = false;
  private destroyed = false;
  private retries = 0;
  private tokenFailures = 0;
  /** The server refused the current attempt's token; it will never open this document. */
  private tokenRejected = false;
  /** Tokens refused in a row, so a server that refuses every token is not hammered. */
  private tokenRejections = 0;
  private localVersion = 0;
  private acknowledgedVersion = -1;
  /**
   * Local content a read-only connection holds that the server does not have
   * (and will never accept). It keeps `hasLocalChanges` true: the server
   * still answers sync-status probes on read-only connections, but those
   * answers do not mean this content was saved.
   */
  private readOnlyDivergence = false;
  private receivedServerSyncStep1 = false;
  private heartbeatTimer: number | null = null;
  private responseTimer: number | null = null;
  private handshakeTimer: number | null = null;
  private retryTimer: number | null = null;
  private wakeRetry: (() => void) | null = null;

  private readonly documentUpdateListener = (update: Uint8Array, origin: unknown): void => {
    if (origin === this || this.destroyed) return;
    if (this.clientToken?.authorization === "read-only") {
      this.onReadOnlyUpdate?.(update.slice());
      console.warn(
        "Realtime: a read-only document was changed locally; the server will not accept the change.",
      );
      return;
    }

    const encoder = encoding.createEncoder();
    encoding.writeVarUint(encoder, MESSAGE_SYNC);
    syncProtocol.writeUpdate(encoder, update);
    this.send(encoding.toUint8Array(encoder));
    this.incrementLocalVersion();
    this.checkSync();
  };

  private readonly awarenessUpdateListener = (
    { added, updated, removed }: AwarenessChanges,
    origin: unknown,
  ): void => {
    if (origin === this || this.destroyed) return;
    // Only this client's own presence is ours to publish. Remote states that
    // expire locally (origin "timeout") are bookkeeping: the server removes a
    // peer when its connection closes, and it rejects updates that name a
    // client other than the connection's own.
    const local = this.document.clientID;
    if (!added.includes(local) && !updated.includes(local) && !removed.includes(local)) return;
    this.sendAwareness([local]);
  };

  constructor(
    private readonly documentId: string,
    private readonly document: Y.Doc,
    private readonly tokenSource: TokenSource,
    options: RealtimeProviderOptions = {},
  ) {
    this.awareness = options.awareness ?? new awarenessProtocol.Awareness(document);
    this.socketFactory = options.socketFactory ?? defaultSocketFactory;
    this.onReadOnlyUpdate = options.onReadOnlyUpdate;
    this.document.on("update", this.documentUpdateListener);
    this.awareness.on("update", this.awarenessUpdateListener);
    if (options.connect !== false) void this.connect();
  }

  get hasLocalChanges(): boolean {
    return this.readOnlyDivergence || this.acknowledgedVersion < this.localVersion;
  }

  /** Start one idempotent reconnect loop and resolve after connect or cancellation. */
  connect(): Promise<void> {
    if (this.destroyed || this.status === SYNC_STATUS_CONNECTED) return Promise.resolve();
    this.shouldConnect = true;
    this.wakeRetry?.();
    if (this.connectTask && this.connectTaskLifecycle === this.lifecycle) return this.connectTask;

    const lifecycle = this.lifecycle;
    const task = this.runConnectLoop(lifecycle).finally(() => {
      if (this.connectTask === task) this.connectTask = null;
      if (
        this.shouldConnect &&
        !this.destroyed &&
        this.status !== SYNC_STATUS_CONNECTED &&
        this.lifecycle === lifecycle
      ) {
        queueMicrotask(() => void this.connect());
      }
    });
    this.connectTask = task;
    this.connectTaskLifecycle = lifecycle;
    return task;
  }

  disconnect(): void {
    if (this.destroyed) return;
    this.shouldConnect = false;
    this.lifecycle += 1;
    this.wakeRetry?.();
    this.closeSocket();
    this.setStatus(SYNC_STATUS_OFFLINE);
  }

  destroy(): void {
    if (this.destroyed) return;
    this.destroyed = true;
    this.shouldConnect = false;
    this.lifecycle += 1;
    this.wakeRetry?.();
    this.closeSocket();
    this.document.off("update", this.documentUpdateListener);
    this.awareness.off("update", this.awarenessUpdateListener);
    awarenessProtocol.removeAwarenessStates(
      this.awareness,
      [this.document.clientID],
      "provider destroyed",
    );
    this.listeners.clear();
    this.setStatus(SYNC_STATUS_OFFLINE);
  }

  on<K extends ProviderEvent>(type: K, listener: ProviderListener<K>): void {
    let listeners = this.listeners.get(type);
    if (!listeners) {
      listeners = new Set();
      this.listeners.set(type, listeners);
    }
    listeners.add(listener as AnyProviderListener);
  }

  off<K extends ProviderEvent>(type: K, listener: ProviderListener<K>): void {
    this.listeners.get(type)?.delete(listener as AnyProviderListener);
  }

  once<K extends ProviderEvent>(type: K, listener: ProviderListener<K>): void {
    const onceListener: ProviderListener<K> = (value) => {
      this.off(type, onceListener);
      listener(value);
    };
    this.on(type, onceListener);
  }

  private emit<K extends ProviderEvent>(type: K, value: ProviderEventMap[K]): void {
    const listeners = this.listeners.get(type);
    if (!listeners) return;
    for (const listener of [...listeners]) listener(value as never);
  }

  private setStatus(status: SyncStatus): void {
    if (this.status === status) return;
    const wasSynced = this.status === SYNC_STATUS_CONNECTED;
    this.status = status;
    if (status === SYNC_STATUS_CONNECTED) this.lastConnectionError = null;
    this.emit(SYNC_EVENT_STATUS, status);
    const isSynced = status === SYNC_STATUS_CONNECTED;
    if (wasSynced !== isSynced) this.emit(SYNC_EVENT_SYNCED, isSynced);
  }

  private async runConnectLoop(lifecycle: number): Promise<void> {
    this.setStatus(SYNC_STATUS_CONNECTING);
    while (this.canContinue(lifecycle) && this.status !== SYNC_STATUS_CONNECTED) {
      let token: ClientToken;
      try {
        token = await this.ensureClientToken();
        this.tokenFailures = 0;
      } catch (error) {
        if (!this.canContinue(lifecycle)) return;
        this.setStatus(SYNC_STATUS_ERROR);
        this.retries += 1;
        if (error instanceof DocumentEpochPendingError) {
          // The server is still collecting acknowledgements for the epoch this
          // client already accepted; poll until it activates.
          this.lastConnectionError = error.message;
          await this.sleep(EPOCH_PENDING_RETRY_MS, lifecycle);
          continue;
        }
        console.warn("Realtime: failed to get a document token", error);
        this.lastConnectionError = error instanceof Error ? error.message : String(error);
        // Back off per document so one persistently failing document cannot
        // keep hammering the token endpoint every few seconds.
        const delay = Math.min(MAX_TOKEN_RETRY_MS, TOKEN_RETRY_MS * 2 ** this.tokenFailures);
        this.tokenFailures += 1;
        await this.sleep(delay, lifecycle);
        continue;
      }
      if (!this.canContinue(lifecycle)) return;

      for (let attempt = 0; attempt < SOCKET_RETRIES_PER_TOKEN; attempt += 1) {
        this.tokenRejected = false;
        const connected = await this.attemptConnection(token, lifecycle);
        if (!this.canContinue(lifecycle) || connected) {
          if (connected) {
            this.retries = 0;
            this.tokenRejections = 0;
          }
          return;
        }
        this.retries += 1;
        // The server will not accept this token again (it expired, the server
        // restarted, or the document moved epochs): mint a new one now instead
        // of spending the remaining attempts on it.
        if (this.tokenRejected) break;
        await this.sleep(this.reconnectDelay(), lifecycle);
        if (!this.canContinue(lifecycle)) return;
      }
      this.clientToken = null;
      if (this.tokenRejected) {
        this.tokenRejections += 1;
        // One refusal is routine (an expired token); a fresh token refused
        // again means something else is wrong, so back off like a failed mint.
        if (this.tokenRejections > 1) {
          const delay = Math.min(
            MAX_TOKEN_RETRY_MS,
            TOKEN_RETRY_MS * 2 ** (this.tokenRejections - 2),
          );
          await this.sleep(delay, lifecycle);
        }
      }
    }
  }

  private canContinue(lifecycle: number): boolean {
    return this.shouldConnect && !this.destroyed && this.lifecycle === lifecycle;
  }

  private async ensureClientToken(): Promise<ClientToken> {
    // A reconnect after a long connection (or sleep) must not present a
    // token the server has already forgotten.
    if (this.clientToken && clientTokenExpired(this.clientToken)) this.clientToken = null;
    if (!this.clientToken) {
      const token = await this.tokenSource();
      if (!token || token.docId !== this.documentId || !token.url) {
        throw new Error(`invalid document token for "${this.documentId}"`);
      }
      this.clientToken = token;
    }
    return this.clientToken;
  }

  private attemptConnection(token: ClientToken, lifecycle: number): Promise<boolean> {
    this.closeSocket();
    this.setStatus(SYNC_STATUS_CONNECTING);

    let socket: SyncSocket;
    try {
      socket = this.socketFactory(this.socketUrl(token));
    } catch (error) {
      console.warn("Realtime: failed to create a document socket", error);
      this.setStatus(SYNC_STATUS_ERROR);
      return Promise.resolve(false);
    }
    if (!this.canContinue(lifecycle)) {
      detachSocket(socket);
      socket.close();
      return Promise.resolve(false);
    }

    this.socket = socket;
    socket.binaryType = "arraybuffer";
    this.receivedServerSyncStep1 = false;

    let resolve!: (connected: boolean) => void;
    const result = new Promise<boolean>((done) => {
      resolve = done;
    });
    let settled = false;
    this.completeAttempt = (connected) => {
      if (settled) return;
      settled = true;
      this.completeAttempt = null;
      resolve(connected);
    };

    // Bound the time to open as well as the handshake: a socket (or mux
    // channel whose open reply was lost) that never opens would otherwise
    // leave this attempt, and the whole connect loop, waiting forever.
    this.handshakeTimer = window.setTimeout(() => {
      if (this.status !== SYNC_STATUS_CONNECTED)
        this.failSocket(socket, lifecycle, "connect timeout");
    }, CONNECT_TIMEOUT_MS);

    socket.onopen = () => {
      if (!this.isCurrentSocket(socket, lifecycle)) return;
      this.setStatus(SYNC_STATUS_HANDSHAKING);
      this.sendSyncStep1();
      this.broadcastAwareness();
      this.resetHeartbeat();
      this.clearHandshakeTimeout();
      this.handshakeTimer = window.setTimeout(() => {
        if (this.status !== SYNC_STATUS_CONNECTED) {
          this.failSocket(socket, lifecycle, "handshake timeout");
        }
      }, HANDSHAKE_TIMEOUT_MS);
    };
    socket.onmessage = (event) => {
      if (!this.isCurrentSocket(socket, lifecycle)) return;
      try {
        this.receiveMessage(toUint8Array(event.data));
        this.clearResponseTimeout();
        this.resetHeartbeat();
      } catch (error) {
        console.warn("Realtime: invalid document sync message", error);
        this.failSocket(
          socket,
          lifecycle,
          `invalid message: ${error instanceof Error ? error.message : String(error)}`,
        );
      }
    };
    socket.onerror = (event) => {
      const rejected =
        (event as { reason?: unknown } | null)?.reason === SOCKET_ERROR_TOKEN_REJECTED;
      if (rejected && this.isCurrentSocket(socket, lifecycle)) this.tokenRejected = true;
      this.failSocket(socket, lifecycle, rejected ? "token rejected" : "socket error");
    };
    socket.onclose = (event) => {
      const close = event as { code?: unknown; reason?: unknown };
      const detail =
        typeof close.code === "number"
          ? `socket closed (${close.code}${
              typeof close.reason === "string" && close.reason ? `: ${close.reason}` : ""
            })`
          : "socket closed";
      this.failSocket(socket, lifecycle, detail);
    };
    return result;
  }

  private isCurrentSocket(socket: SyncSocket, lifecycle: number): boolean {
    return this.socket === socket && this.canContinue(lifecycle);
  }

  private failSocket(socket: SyncSocket, lifecycle: number, reason: string): void {
    if (this.socket !== socket) return;
    this.lastConnectionError = reason;
    this.socket = null;
    detachSocket(socket);
    try {
      socket.close();
    } catch {
      // The transport has already failed.
    }
    this.clearSocketTimers();
    this.removeRemoteAwareness();
    if (!this.destroyed && this.lifecycle === lifecycle && this.shouldConnect) {
      this.setStatus(SYNC_STATUS_ERROR);
    }
    this.completeAttempt?.(false);
    if (!this.connectTask && this.canContinue(lifecycle)) queueMicrotask(() => void this.connect());
  }

  private closeSocket(): void {
    const socket = this.socket;
    this.socket = null;
    this.clearSocketTimers();
    this.completeAttempt?.(false);
    this.removeRemoteAwareness();
    if (!socket) return;
    detachSocket(socket);
    try {
      socket.close();
    } catch {
      // The transport is already gone.
    }
  }

  private socketUrl(token: ClientToken): string {
    const url = new URL(token.url);
    if (!url.pathname.endsWith("/")) url.pathname += "/";
    url.pathname += encodeURIComponent(token.docId);
    if (token.token) url.searchParams.set("token", token.token);
    return url.toString();
  }

  private send(message: Uint8Array): boolean {
    const socket = this.socket;
    if (!socket || socket.readyState !== SOCKET_OPEN) return false;
    socket.send(message);
    return true;
  }

  private sendSyncStep1(): void {
    const encoder = encoding.createEncoder();
    encoding.writeVarUint(encoder, MESSAGE_SYNC);
    syncProtocol.writeSyncStep1(encoder, this.document);
    this.send(encoding.toUint8Array(encoder));
  }

  private receiveSyncMessage(decoder: decoding.Decoder): void {
    const encoder = encoding.createEncoder();
    encoding.writeVarUint(encoder, MESSAGE_SYNC);
    const messageType = syncProtocol.readSyncMessage(decoder, encoder, this.document, this);
    const reply = encoding.toUint8Array(encoder);
    if (reply.length > 1) this.send(reply);

    if (messageType === syncProtocol.messageYjsSyncStep1) {
      this.receivedServerSyncStep1 = true;
      if (this.clientToken?.authorization === "read-only") this.noteReadOnlyDivergence(reply);
      this.checkSync();
    } else if (messageType === syncProtocol.messageYjsSyncStep2) {
      this.clearHandshakeTimeout();
      this.setStatus(SYNC_STATUS_CONNECTED);
      this.completeAttempt?.(true);
    }
  }

  private receiveMessage(message: Uint8Array): void {
    const decoder = decoding.createDecoder(message);
    const messageType = decoding.readVarUint(decoder);
    switch (messageType) {
      case MESSAGE_SYNC:
        this.receiveSyncMessage(decoder);
        break;
      case MESSAGE_AWARENESS:
        awarenessProtocol.applyAwarenessUpdate(
          this.awareness,
          decoding.readVarUint8Array(decoder),
          this,
        );
        break;
      case MESSAGE_QUERY_AWARENESS:
        // Answer for this client only (see awarenessUpdateListener).
        this.broadcastAwareness();
        break;
      case MESSAGE_SYNC_STATUS: {
        const versionDecoder = decoding.createDecoder(decoding.readVarUint8Array(decoder));
        this.acknowledgeVersion(decoding.readVarUint(versionDecoder));
        break;
      }
      case MESSAGE_EPOCH_PROPOSAL:
        this.acknowledgeEpoch(decoding.readVarUint8Array(decoder));
        break;
      case MESSAGE_DOCUMENT_INVALIDATED:
        this.receiveDocumentInvalidation(decoding.readVarUint8Array(decoder));
        break;
      default:
        // New optional server messages are ignored for forward compatibility.
        break;
    }
  }

  private receiveDocumentInvalidation(payload: Uint8Array): void {
    const parsed = JSON.parse(new TextDecoder().decode(payload)) as { documentId?: unknown };
    if (typeof parsed.documentId !== "string" || parsed.documentId.length === 0) {
      throw new Error("invalid document invalidation");
    }
    this.emit(SYNC_EVENT_DOCUMENT_INVALIDATED, parsed.documentId);
  }

  private acknowledgeEpoch(payload: Uint8Array): void {
    const parsed = JSON.parse(new TextDecoder().decode(payload)) as { epoch?: unknown };
    if (!Number.isSafeInteger(parsed.epoch) || (parsed.epoch as number) < 0) {
      throw new Error("invalid document epoch proposal");
    }
    const epoch = parsed.epoch as number;
    handleEpochProposal(this.documentId, epoch);

    const encoder = encoding.createEncoder();
    encoding.writeVarUint(encoder, MESSAGE_EPOCH_ACKNOWLEDGEMENT);
    encoding.writeVarUint8Array(encoder, new TextEncoder().encode(JSON.stringify({ epoch })));
    this.send(encoding.toUint8Array(encoder));
  }

  private broadcastAwareness(): void {
    if (this.awareness.getLocalState() !== null) this.sendAwareness([this.document.clientID]);
  }

  private sendAwareness(clients: number[]): void {
    const encoder = encoding.createEncoder();
    encoding.writeVarUint(encoder, MESSAGE_AWARENESS);
    encoding.writeVarUint8Array(
      encoder,
      awarenessProtocol.encodeAwarenessUpdate(this.awareness, clients),
    );
    this.send(encoding.toUint8Array(encoder));
  }

  private incrementLocalVersion(): void {
    const wasPending = this.hasLocalChanges;
    this.localVersion += 1;
    if (!wasPending) this.emit(SYNC_EVENT_LOCAL_CHANGES, true);
  }

  private acknowledgeVersion(version: number): void {
    const wasPending = this.hasLocalChanges;
    // Over a full-access connection the acknowledgement also covers whatever
    // this connection's handshake sent, including earlier read-only divergence.
    if (this.clientToken?.authorization !== "read-only") this.readOnlyDivergence = false;
    this.acknowledgedVersion = Math.max(this.acknowledgedVersion, version);
    if (wasPending && !this.hasLocalChanges) this.emit(SYNC_EVENT_LOCAL_CHANGES, false);
  }

  /**
   * A read-only server never accepts this client's content. If the handshake
   * shows local items the server lacks (edits persisted before the grant
   * turned out to be read-only), keep them pending: the server still answers
   * sync-status probes, but those replies must not mark this content saved.
   */
  private noteReadOnlyDivergence(reply: Uint8Array): void {
    const decoder = decoding.createDecoder(reply);
    decoding.readVarUint(decoder);
    if (decoding.readVarUint(decoder) !== syncProtocol.messageYjsSyncStep2) return;
    const update = decoding.readVarUint8Array(decoder);
    // Only inserted structs are evidence: Yjs always encodes the whole delete
    // set, so deletions cannot be compared against the server's state vector.
    if (Y.decodeUpdate(update).structs.length === 0) return;
    const wasPending = this.hasLocalChanges;
    this.readOnlyDivergence = true;
    if (!wasPending) this.emit(SYNC_EVENT_LOCAL_CHANGES, true);
  }

  private checkSync(): void {
    if (!this.receivedServerSyncStep1) return;
    const encoder = encoding.createEncoder();
    encoding.writeVarUint(encoder, MESSAGE_SYNC_STATUS);
    const versionEncoder = encoding.createEncoder();
    encoding.writeVarUint(versionEncoder, this.localVersion);
    encoding.writeVarUint8Array(encoder, encoding.toUint8Array(versionEncoder));
    if (!this.send(encoding.toUint8Array(encoder))) return;

    if (!this.responseTimer) {
      this.responseTimer = window.setTimeout(() => {
        const socket = this.socket;
        if (socket) {
          this.failSocket(socket, this.lifecycle, "heartbeat timeout");
        }
      }, RESPONSE_TIMEOUT_MS);
    }
  }

  private resetHeartbeat(): void {
    window.clearTimeout(this.heartbeatTimer ?? undefined);
    const interval =
      this.receivedServerSyncStep1 && !this.hasLocalChanges ? IDLE_HEARTBEAT_MS : HEARTBEAT_MS;
    this.heartbeatTimer = window.setTimeout(() => {
      this.heartbeatTimer = null;
      if (this.receivedServerSyncStep1) this.checkSync();
      else this.sendSyncStep1();
    }, interval);
  }

  private clearResponseTimeout(): void {
    window.clearTimeout(this.responseTimer ?? undefined);
    this.responseTimer = null;
  }

  private clearHandshakeTimeout(): void {
    window.clearTimeout(this.handshakeTimer ?? undefined);
    this.handshakeTimer = null;
  }

  private clearSocketTimers(): void {
    window.clearTimeout(this.heartbeatTimer ?? undefined);
    window.clearTimeout(this.responseTimer ?? undefined);
    window.clearTimeout(this.handshakeTimer ?? undefined);
    this.heartbeatTimer = null;
    this.responseTimer = null;
    this.handshakeTimer = null;
  }

  private removeRemoteAwareness(): void {
    const remoteClients = Array.from(this.awareness.getStates().keys()).filter(
      (clientId) => clientId !== this.document.clientID,
    );
    if (remoteClients.length > 0) {
      awarenessProtocol.removeAwarenessStates(this.awareness, remoteClients, this);
    }
  }

  private reconnectDelay(): number {
    return Math.min(MAX_RECONNECT_MS, RECONNECT_INITIAL_MS * Math.pow(1.1, this.retries));
  }

  private sleep(ms: number, lifecycle: number): Promise<void> {
    if (!this.canContinue(lifecycle)) return Promise.resolve();
    let resolve!: () => void;
    const promise = new Promise<void>((done) => {
      resolve = done;
    });
    const finish = () => {
      window.clearTimeout(this.retryTimer ?? undefined);
      this.retryTimer = null;
      this.wakeRetry = null;
      resolve();
    };
    this.wakeRetry = finish;
    this.retryTimer = window.setTimeout(finish, ms);
    return promise;
  }
}

function defaultSocketFactory(url: string): SyncSocket {
  return new WebSocket(url) as unknown as SyncSocket;
}

/**
 * Stop reacting to a socket about to be closed. Closing one that is still
 * connecting reports an error event afterwards, and Node's `ws` throws that
 * error when nothing listens, so a no-op handler stays attached.
 */
function detachSocket(socket: SyncSocket): void {
  socket.onopen = null;
  socket.onmessage = null;
  socket.onclose = null;
  socket.onerror = () => {};
}

function toUint8Array(data: unknown): Uint8Array {
  if (data instanceof Uint8Array) return data;
  if (data instanceof ArrayBuffer) return new Uint8Array(data);
  // WebSocket implementations can create the buffer in another JavaScript
  // realm, where `instanceof ArrayBuffer` is false.
  if (Object.prototype.toString.call(data) === "[object ArrayBuffer]") {
    return new Uint8Array(data as ArrayBuffer);
  }
  if (ArrayBuffer.isView(data)) {
    return new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
  }
  throw new Error("document socket returned a non-binary message");
}
