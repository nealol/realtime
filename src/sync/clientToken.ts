import type RealtimePlugin from "../main";
import { getDocumentEpoch } from "../documentEpoch";
import { HttpError, isServerUnavailableStatus } from "../httpError";

/** Token fields used by Realtime's native document provider. */
export type ClientToken = {
  url: string;
  baseUrl: string;
  docId: string;
  token?: string;
  authorization?: "full" | "read-only";
  epoch?: number;
  /** The server is still activating `epoch`; it accepts connections once active. */
  epochPending?: boolean;
  /** When the server stops accepting `token` (ms since the Unix epoch); absent on older servers. */
  expiresAt?: number;
};

/** Refresh a token this long before it expires, so a reconnect never presents a dead one. */
const TOKEN_EXPIRY_MARGIN_MS = 60_000;

/** Whether a cached token can no longer be trusted to open its document. */
export function clientTokenExpired(token: ClientToken, now = Date.now()): boolean {
  return typeof token.expiresAt === "number" && now >= token.expiresAt - TOKEN_EXPIRY_MARGIN_MS;
}

export class DocumentEpochChangedError extends Error {
  constructor(
    readonly documentId: string,
    readonly epoch: number,
  ) {
    super(`Realtime: document "${documentId}" moved to epoch ${epoch}; reconnecting.`);
    this.name = "DocumentEpochChangedError";
  }
}

export class DocumentEpochPendingError extends Error {
  constructor(
    readonly documentId: string,
    readonly localEpoch: number,
    readonly serverEpoch: number,
  ) {
    super(
      `Realtime: document "${documentId}" is waiting for epoch ${localEpoch} to activate; retrying.`,
    );
    this.name = "DocumentEpochPendingError";
  }
}

const TOKEN_RETRY_DELAY_MS = 30_000;
/**
 * Server errors on this many token requests in a row (with no success in
 * between) mean the server is failing generally rather than for one document.
 */
const SERVER_FAILURES_BEFORE_BACKOFF = 3;

/**
 * Token requests in flight at once. Reconnecting a large vault mints one token
 * per document; strictly one at a time, a few thousand notes took minutes.
 */
const MAX_CONCURRENT_TOKEN_REQUESTS = 6;

let tokenRetryDelayMs = TOKEN_RETRY_DELAY_MS;
let nextTokenAttemptAt = 0;
let consecutiveServerFailures = 0;
let activeTokenRequests = 0;
let tokenRequestWaiters: Array<() => void> = [];

/**
 * Test-only: clear the module-global token backoff/queue and optionally shrink
 * the retry delay. The backoff state is shared by every provider in the
 * process, so without a reset one transient token failure in a test run stalls
 * every later connection for 30s.
 */
export function resetTokenRetryStateForTests(delayMs = TOKEN_RETRY_DELAY_MS): void {
  tokenRetryDelayMs = delayMs;
  nextTokenAttemptAt = 0;
  consecutiveServerFailures = 0;
  activeTokenRequests = 0;
  tokenRequestWaiters = [];
}

/**
 * Whether a failed token request should delay every document's next request.
 * Only failures that say the server or the session is unavailable qualify: an
 * error specific to one document (a 4xx, or one document's 500) must not stall
 * the rest of the vault behind it.
 */
function backsOffEveryDocument(error: unknown): boolean {
  if (error instanceof DocumentEpochChangedError || error instanceof DocumentEpochPendingError) {
    return false;
  }
  if (error instanceof HttpError) {
    if (isServerUnavailableStatus(error.status)) return true;
    if (error.status >= 500) {
      consecutiveServerFailures += 1;
      return consecutiveServerFailures >= SERVER_FAILURES_BEFORE_BACKOFF;
    }
    return false;
  }
  // No HTTP response (offline, DNS, TLS), a rejected session, or a malformed
  // token: nothing else can succeed right now either.
  return true;
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => window.setTimeout(resolve, ms));
}

/** Wait for a free token-request slot; the returned function releases it. */
function waitForTokenAttemptSlot(): Promise<() => void> {
  return new Promise((resolve) => {
    const grant = () => {
      activeTokenRequests += 1;
      let released = false;
      resolve(() => {
        if (released) return;
        released = true;
        activeTokenRequests -= 1;
        tokenRequestWaiters.shift()?.();
      });
    };
    if (activeTokenRequests < MAX_CONCURRENT_TOKEN_REQUESTS) grant();
    else tokenRequestWaiters.push(grant);
  });
}

/**
 * Obtains a {@link ClientToken} for a document from the Realtime server. The
 * server performs the access check and terminates the Yjs connection itself.
 *
 * `docId` is the *namespaced* id (`{vaultId}` for the index, `{vaultId}__{guid}`
 * for a file); the vault is always the active vault.
 *
 * `expectedEpoch` is the epoch the caller's local Y.Doc and persistence were
 * built for. Once the durable epoch moves past it, that instance holds state
 * from a retired epoch and must never connect again: its items would be new
 * to the replacement document and duplicate its content.
 */
export async function getClientToken(
  plugin: RealtimePlugin,
  docId: string,
  path?: string,
  expectedEpoch?: number,
): Promise<ClientToken> {
  const vaultId = plugin.settings.activeVaultId;
  if (!vaultId) {
    throw new Error("Realtime: no active vault; sign in and set up a vault before syncing.");
  }
  const staleEpoch = () => {
    const localEpoch = getDocumentEpoch(plugin, docId);
    return expectedEpoch !== undefined && localEpoch !== expectedEpoch ? localEpoch : null;
  };
  const staleBeforeRequest = staleEpoch();
  if (staleBeforeRequest !== null) throw new DocumentEpochChangedError(docId, staleBeforeRequest);

  const release = await waitForTokenAttemptSlot();
  try {
    const waitMs = nextTokenAttemptAt - Date.now();
    if (waitMs > 0) await delay(waitMs);

    const token = await plugin.auth.docToken(vaultId, docId, path);
    nextTokenAttemptAt = 0;
    consecutiveServerFailures = 0;
    if (!token || !token.url) {
      throw new Error(`Realtime: auth server returned an invalid token for "${docId}".`);
    }
    const serverEpoch = token.epoch ?? 0;
    const localEpoch = getDocumentEpoch(plugin, docId);
    if (serverEpoch > localEpoch) {
      plugin.acceptDocumentEpoch(docId, serverEpoch);
      throw new DocumentEpochChangedError(docId, serverEpoch);
    }
    if (serverEpoch < localEpoch) {
      // This client already accepted a proposed epoch that the server has not
      // activated yet (it is waiting for other peers to acknowledge, for at
      // most its acknowledgement timeout). Wait for it: connecting to the
      // retiring epoch would load its items into the fresh local document,
      // and the next full-access connection would upload them into the
      // replacement as duplicate content.
      throw new DocumentEpochPendingError(docId, localEpoch, serverEpoch);
    }
    if (token.epochPending) {
      // Newer servers name the pending epoch in tokens while it activates.
      throw new DocumentEpochPendingError(docId, localEpoch, serverEpoch);
    }
    const staleAfterRequest = staleEpoch();
    if (staleAfterRequest !== null) throw new DocumentEpochChangedError(docId, staleAfterRequest);
    return token;
  } catch (e) {
    if (backsOffEveryDocument(e)) {
      nextTokenAttemptAt = Date.now() + tokenRetryDelayMs;
    }
    throw e;
  } finally {
    release();
  }
}
