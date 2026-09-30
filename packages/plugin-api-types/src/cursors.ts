/**
 * Types for the plugin-managed remote cursors API, reachable as
 * `app.plugins.plugins["realtime"].cursors`.
 *
 * A plugin acquires a remote cursor identity for the active vault; edits made
 * with the returned bearer token (REST, MCP, or the streaming WebSocket) are
 * attributed in Git to the cursor robot on behalf of the current user, instead
 * of looking like ordinary human edits.
 *
 * The handle's `notes` methods perform those edits locally — plain function
 * calls, no WebSocket or token plumbing — and every mutation lands in the
 * cursor's audit log (undoable by admins for ~3 days) with robot Git
 * attribution, exactly like MCP and streaming edits.
 */

export interface AcquireCursorOptions {
  /** The acquiring plugin's manifest id (same trust model as `sql.open`). */
  pluginId: string;
  /** Display name for the cursor; defaults to the plugin id on first acquire. */
  name?: string;
}

export interface CursorNoteSummary {
  path: string;
  guid: string;
  permalink: string;
}

export interface CursorNote {
  path: string;
  guid: string;
  content: string;
  permalink: string;
}

/**
 * Note operations executed *as the cursor*. Each mutation is recorded in the
 * cursor's audit log and attributed to the cursor robot in Git. Token expiry
 * and renewal are handled internally (one transparent re-acquire on 401).
 */
export interface CursorNotesApi {
  list(): Promise<CursorNoteSummary[]>;
  read(path: string): Promise<CursorNote>;
  create(path: string, content?: string): Promise<CursorNote>;
  replace(path: string, content: string): Promise<CursorNote>;
  patch(
    path: string,
    edit: { old: string; new: string; replaceAll?: boolean },
  ): Promise<CursorNote>;
  /**
   * Convenience read-then-replace appending `text` on a fresh line. If the
   * note changes between the read and the write, the write is refused and
   * `append` re-reads rather than reverting that change; it gives up with the
   * server's `stale` error after a few attempts on a note under heavy edits.
   */
  append(path: string, text: string): Promise<CursorNote>;
  move(path: string, toPath: string): Promise<CursorNote>;
  delete(path: string): Promise<void>;
}

export interface RemoteCursorHandle {
  cursorId: string;
  appId: string;
  vaultId: string;
  name: string;
  /** Bearer token for REST/MCP/streaming calls. Expires; re-acquire on 401. */
  token: string;
  /** Server base URL, e.g. `https://host` (REST routes live under /api). */
  baseUrl: string;
  mcpUrl: string;
  /** WebSocket endpoint for streaming tokens into a note. */
  streamUrl: string;
  expiresAt: number;
  /** Audited, robot-attributed note edits — no token handling needed. */
  notes: CursorNotesApi;
}

/** The remote cursors API surface (`app.plugins.plugins["realtime"].cursors`). */
export interface RealtimeCursors {
  /**
   * Get-or-create this plugin's remote cursor for the active vault and return
   * a handle with a live bearer token. Tokens are cached per session and
   * re-minted when close to expiry; call {@link invalidate} after a 401.
   */
  acquire(opts: AcquireCursorOptions): Promise<RemoteCursorHandle>;
  /** Drop the cached token for a plugin (e.g. after a 401) so the next acquire re-mints. */
  invalidate(pluginId: string): void;
}
