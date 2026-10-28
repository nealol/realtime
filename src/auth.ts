import { requestUrl } from "obsidian";
import type RealtimePlugin from "./main";
import type { ClientToken } from "./sync/clientToken";
import {
  checkServerCaps,
  CompatibilityError,
  serverSupportsCapability,
  type CapName,
} from "./caps";
import { HttpError } from "./httpError";

export { HttpError, isServerUnavailableStatus } from "./httpError";

/** Identity returned by `GET /api/me`. */
export interface MeResponse {
  userId: string;
  email: string;
  gitEmail?: string;
  displayName: string;
  pictureUrl?: string | null;
  avatarUrlOverride?: string | null;
  avatarUrl?: string | null;
}

export interface KnownSession extends MeResponse {
  serverUrl: string;
  serverId: string;
  tokenKey: string;
}

/**
 * A token validated by SSO against `serverUrl`, not yet active. Produced by
 * {@link AuthClient.authenticateAt}; becomes active via
 * {@link AuthClient.commitSession} only while its attempt is still current.
 */
export interface AuthenticatedSession {
  serverUrl: string;
  serverId: string;
  token: string;
  me: MeResponse;
  /** SSO attempt that produced this session. */
  attempt: number;
}

/** Server identity returned by the public `GET /api/server-info`. */
export interface ServerInfoResponse {
  serverId: string;
  /** Server release semver (operator-facing; not used for gating). */
  version?: string;
  /** Named capability versions per surface. Required by this plugin for compatibility gating. */
  caps?: Record<string, string>;
  /** Cap names the client must understand. Optional; empty in v1. */
  requiredCaps?: string[];
}

export interface VaultInfo {
  id: string;
  name: string;
  role: "admin" | "member";
  createdBy?: string;
  owner?: boolean;
}

export interface MemberInfo {
  userId: string;
  email: string;
  displayName: string;
  role: "admin" | "member";
  owner?: boolean;
  avatarUrl?: string | null;
}

export interface RemoteCursorInfo {
  id: string;
  appId: string;
  name: string;
  mcpUrl: string;
  createdAt: number;
  /** Manifest id of the managing plugin; absent for admin-created cursors. */
  pluginId?: string | null;
}

export interface PluginCursorGrant {
  id: string;
  appId: string;
  name: string;
  vaultId: string;
  pluginId: string;
  mcpUrl: string;
  streamUrl: string;
  secretToken: string;
  expiresAt: number;
}

export interface CursorAuditEntry {
  id: string;
  createdAt: number;
  operation: string;
  path: string;
  toPath?: string | null;
  beforeContent?: string | null;
  afterContent?: string | null;
  details?: Record<string, unknown> | null;
  undoneAt?: number | null;
}

export interface CursorAuditPage {
  entries: CursorAuditEntry[];
  hasMore: boolean;
}

export interface GitBackupConfig {
  configured: boolean;
  remoteUrl?: string;
  authMethod?: "ssh" | "https";
  branch?: string;
  sshPublicKey?: string;
  hasHttpsToken: boolean;
  enabled: boolean;
  lastPushAt?: number;
  lastPushError?: string;
}

export interface PutGitBackupBody {
  remoteUrl: string;
  authMethod: "ssh" | "https";
  branch?: string;
  httpsToken?: string;
  regenerateKey?: boolean;
  enabled: boolean;
}

export interface SearchHit {
  path: string;
  guid: string;
  title: string;
  permalink: string;
  snippet: string;
}

export interface TagCount {
  tag: string;
  count: number;
}

/** Stable permalink for a note, returned by the note-permalinks endpoint. */
export interface PermalinkResponse {
  kind: string;
  url: string;
}

/** A public read-only share link for a note (rendered at `/view/{id}`). */
export interface PublicShareResponse {
  id: string;
  url: string;
  path: string;
  guid: string;
  createdAt: number;
}

/** A public link to one exact version of a binary attachment. */
export interface PublicAttachmentShareResponse {
  id: string;
  url: string;
  path: string;
  hash: string;
  size: number;
  createdAt: number;
}

/** Per-vault storage breakdown, from `GET /api/vaults/{id}/storage`. */
export interface StorageUsage {
  blobsCurrentBytes: number;
  blobsPreviousBytes: number;
  currentBlobCount: number;
  previousBlobCount: number;
  /** null when the CRDT store is not readable server-side. */
  plainVaultBytes: number | null;
}

/** Result of an orphaned-blob cleanup. */
export interface GcBlobsResult {
  removed: number;
  freedBytes: number;
}

/** Thrown when the server rejects the session; callers should prompt re-login. */
export class AuthError extends Error {}

/**
 * Validate a self-settable git author email. Mirrors the server-side
 * `validate_git_email` (server/src/routes.rs): the value flows unescaped into
 * `Name <email>` passed to `git commit --author` and into `Co-authored-by`
 * trailers, so it must not contain control chars, whitespace, or angle
 * brackets, and must have a basic email shape. Returns a human-readable error
 * message, or `null` when the value is valid — including the empty string,
 * which clears the field (falling back to the login email).
 */
export function validateGitEmail(value: string): string | null {
  const trimmed = value.trim();
  if (trimmed === "") return null;
  if (trimmed.length > 254) return "Git author email is too long.";
  if (/[\x00-\x1f\x7f<>\s]/.test(trimmed)) {
    return "Git author email must not contain spaces, angle brackets, or line breaks.";
  }
  const at = trimmed.indexOf("@");
  if (at < 0 || at !== trimmed.lastIndexOf("@")) {
    return "Enter a valid email address.";
  }
  const local = trimmed.slice(0, at);
  const domain = trimmed.slice(at + 1);
  if (local === "" || domain === "" || !domain.includes(".")) {
    return "Enter a valid email address.";
  }
  return null;
}

/**
 * Validate a self-settable avatar URL. Mirrors the server-side
 * `validate_avatar_url` (server/src/routes.rs): must be a parseable `http` or
 * `https` URL, at most 2048 bytes, with no ASCII control characters or
 * whitespace. Returns a human-readable error message, or `null` when the value
 * is valid — including the empty string, which clears the override (falling
 * back to the OpenID profile picture).
 */
export function validateAvatarUrl(value: string): string | null {
  const trimmed = value.trim();
  if (trimmed === "") return null;
  if (trimmed.length > 2048) return "Avatar URL is too long.";
  if (/[\x00-\x1f\x7f\s]/.test(trimmed)) {
    return "Avatar URL must not contain spaces or line breaks.";
  }
  try {
    const url = new URL(trimmed);
    if (url.protocol !== "http:" && url.protocol !== "https:") {
      return "Avatar URL must start with http: or https:.";
    }
  } catch {
    return "Enter a valid http(s) URL.";
  }
  return null;
}

/**
 * Talks to the Realtime auth server: SSO login (via an `obsidian://` deep link,
 * with a paste-code fallback), session management, and the vault/sharing/token
 * endpoints. Uses Obsidian's `requestUrl` so it works around desktop CORS.
 */
export class AuthClient {
  private readonly serverCapsByUrl = new Map<string, Record<string, string>>();
  private plugin: RealtimePlugin;
  /** Resolver for an in-flight login call awaiting the deep link / paste code. */
  private pendingLogin: ((token: string) => Promise<MeResponse>) | null = null;
  /** Rejecter paired with {@link pendingLogin}, so the wait can be cancelled. */
  private pendingReject: ((err: Error) => void) | null = null;
  private pendingTimer: number | null = null;
  /** Current SSO attempt; bumping it invalidates older attempts and their sessions. */
  private loginAttempt = 0;

  constructor(plugin: RealtimePlugin) {
    this.plugin = plugin;
  }

  private get baseUrl(): string {
    return normalizeServerUrl(this.plugin.settings.authServerUrl);
  }

  /**
   * SecretStorage key for the current server's session token. Obsidian's
   * SecretStorage is shared across local vaults, so the key is namespaced by
   * server host + the server's stable id (`/api/server-info`) to let one client
   * hold tokens for multiple servers at once. Falls back to the legacy global
   * key when the server id isn't known yet (pre-migration installs).
   */
  private tokenKey(userId = this.plugin.settings.userId): string {
    const serverId = this.plugin.settings.authServerId;
    if (!serverId) return LEGACY_TOKEN_KEY;
    if (!userId) return serverSessionTokenKey(this.plugin.settings.authServerUrl, serverId);
    return sessionTokenKey(this.plugin.settings.authServerUrl, serverId, userId);
  }

  private tokenRecord(userId = this.plugin.settings.userId): { key: string; token: string } | null {
    const keys = [this.tokenKey(userId), this.tokenKey("")];
    if (userId) {
      for (const session of this.knownSessions()) {
        let sameServer = false;
        try {
          sameServer = normalizeServerUrl(session.serverUrl) === this.baseUrl;
        } catch {
          continue;
        }
        if (sameServer && session.userId === userId) keys.push(session.tokenKey);
      }
    }
    keys.push(LEGACY_TOKEN_KEY);

    for (const key of new Set(keys)) {
      const token = this.plugin.app.secretStorage.getSecret(key);
      if (token) return { key, token };
    }
    return null;
  }

  private getToken(): string {
    return this.tokenRecord()?.token ?? "";
  }

  private setToken(value: string, userId = this.plugin.settings.userId): void {
    this.plugin.app.secretStorage.setSecret(this.tokenKey(userId), value);
  }

  private deleteToken(): void {
    const record = this.tokenRecord();
    const keys = [
      record?.key,
      this.tokenKey(this.plugin.settings.userId),
      this.tokenKey(""),
      LEGACY_TOKEN_KEY,
    ];
    // SecretStorage has no delete; clear each exact fallback that could
    // otherwise make this account appear signed in again after logout.
    for (const key of new Set(keys.filter((key): key is string => !!key))) {
      this.plugin.app.secretStorage.setSecret(key, "");
    }
  }

  get isLoggedIn(): boolean {
    return !!this.getToken();
  }

  /** Whether the active server advertises a supported optional capability. */
  supportsCapability(name: CapName): boolean {
    return serverSupportsCapability(this.serverCapsByUrl.get(this.baseUrl), name);
  }

  // --- server identity -------------------------------------------------------

  /**
   * Fetch a server's stable id, release version, and advertised caps (public
   * endpoint; no session required). Raw fetch with no compatibility check —
   * kept private so all callers go through {@link serverInfoChecked} and cannot
   * bypass cap gating.
   */
  private async rawServerInfo(baseUrl: string): Promise<ServerInfoResponse> {
    return this.apiAt<ServerInfoResponse>(normalizeServerUrl(baseUrl), "/api/server-info");
  }

  /**
   * Fetch `/api/server-info` and enforce capability compatibility. Returns the
   * full response so callers can read `serverId`; throws
   * {@link CompatibilityError} on a cap mismatch or missing caps.
   *
   * Only a check of the ACTIVE server updates `plugin.lastCompatibilityError`
   * (set on failure, cleared on success). Testing a candidate server during
   * sign-in or migration must not hide or invent the active server's state.
   *
   * Network/offline errors propagate as normal exceptions (not
   * `CompatibilityError`); callers tolerate them as today.
   */
  async serverInfoChecked(baseUrl: string): Promise<ServerInfoResponse> {
    const normalizedBaseUrl = normalizeServerUrl(baseUrl);
    const info = await this.rawServerInfo(normalizedBaseUrl);
    const result = checkServerCaps(info.caps, info.requiredCaps);
    const active = normalizedBaseUrl === this.baseUrl;
    if (result.ok) {
      this.serverCapsByUrl.set(normalizedBaseUrl, info.caps ?? {});
      if (active) this.plugin.lastCompatibilityError = null;
      return info;
    }
    if (active) {
      this.plugin.lastCompatibilityError = {
        reason: result.reason,
        detail: result.detail,
        serverVersion: info.version,
      };
    }
    throw new CompatibilityError(result.reason, result.detail, info.version);
  }

  /** Ensure the current server id is known and migrate recognized saved sessions. */
  async ensureServerId(): Promise<string> {
    const baseUrl = this.baseUrl;
    const existing = this.plugin.settings.authServerId;
    const active = this.tokenRecord();
    const sessionsBefore = this.knownSessions();
    // A sign-out or a server/account switch while a request below is in
    // flight owns the session state; never rebind credentials it replaced.
    const superseded = () =>
      this.baseUrl !== baseUrl || this.tokenRecord()?.token !== active?.token;
    let serverId: string;
    try {
      ({ serverId } = await this.serverInfoChecked(baseUrl));
    } catch (e) {
      // Existing installs should still start while offline, but a cap failure
      // means the server explicitly advertised an incompatible protocol.
      if (e instanceof CompatibilityError || !existing) throw e;
      return existing;
    }
    if (superseded()) return serverId;

    const serverOnlyKey = serverSessionTokenKey(baseUrl, serverId);
    const activeNeedsMigration =
      active !== null &&
      (active.key === LEGACY_TOKEN_KEY ||
        active.key === serverOnlyKey ||
        !existing ||
        existing !== serverId);

    if (activeNeedsMigration) {
      let me: MeResponse;
      try {
        me = await this.apiAt<MeResponse>(baseUrl, "/api/me", active.token);
      } catch (e) {
        if (!(e instanceof AuthError)) {
          // Do not bind credentials to an unverified server after a transient
          // failure. Keeping the old id makes the next startup retry.
          if (existing) return existing;
          throw e;
        }
        if (this.baseUrl !== baseUrl) return serverId;
        // A rejected active token is already cleared by the unauthorized
        // helper. Accept the fetched identity so startup can show sign-in.
        this.plugin.settings.authServerId = serverId;
        await this.plugin.saveSettings();
        return serverId;
      }
      if (superseded()) return serverId;
      this.plugin.settings.authServerId = serverId;
      const destination = sessionTokenKey(baseUrl, serverId, me.userId);
      this.plugin.app.secretStorage.setSecret(destination, active.token);
      this.saveKnownSessions([
        this.knownSession(me, destination, baseUrl, serverId),
        ...sessionsBefore.filter(
          (session) => session.tokenKey !== active.key && session.tokenKey !== destination,
        ),
      ]);
      if (active.key !== destination) {
        this.plugin.app.secretStorage.setSecret(active.key, "");
      }
      this.applyIdentity(me);
      this.plugin.updateLocalAwareness();
    } else {
      this.plugin.settings.authServerId = serverId;
    }

    // A repaired server id can leave multiple saved accounts under the old
    // namespace. Validate and migrate each independently.
    for (const candidate of sessionsBefore) {
      if (this.baseUrl !== baseUrl) break;
      let sameServer = false;
      try {
        sameServer = normalizeServerUrl(candidate.serverUrl) === baseUrl;
      } catch {
        continue;
      }
      if (!sameServer || candidate.serverId === serverId || candidate.tokenKey === active?.key) {
        continue;
      }
      const token = this.plugin.app.secretStorage.getSecret(candidate.tokenKey);
      if (!token) continue;

      try {
        const me = await this.apiAt<MeResponse>(baseUrl, "/api/me", token);
        const destination = sessionTokenKey(baseUrl, serverId, me.userId);
        this.plugin.app.secretStorage.setSecret(destination, token);
        this.saveKnownSessions([
          this.knownSession(me, destination, baseUrl, serverId),
          ...this.knownSessions().filter(
            (session) =>
              session.tokenKey !== candidate.tokenKey && session.tokenKey !== destination,
          ),
        ]);
        this.plugin.app.secretStorage.setSecret(candidate.tokenKey, "");
      } catch (e) {
        if (e instanceof AuthError) {
          this.plugin.app.secretStorage.setSecret(candidate.tokenKey, "");
          this.forgetSession(candidate.tokenKey);
        }
        // Network and server failures leave the candidate untouched so a later
        // startup can retry it.
      }
    }

    await this.plugin.saveSettings();
    return serverId;
  }

  // --- low-level request -----------------------------------------------------

  private async api<T>(path: string, init?: { method?: string; body?: unknown }): Promise<T> {
    return this.apiAt<T>(this.baseUrl, path, this.getToken(), init);
  }

  async apiAt<T>(
    baseUrl: string,
    path: string,
    token?: string,
    init?: { method?: string; body?: unknown },
  ): Promise<T> {
    const res = await requestUrl({
      url: `${normalizeServerUrl(baseUrl)}${path}`,
      method: init?.method ?? "GET",
      contentType: init?.body !== undefined ? "application/json" : undefined,
      headers: token ? { Authorization: `Bearer ${token}` } : undefined,
      body: init?.body !== undefined ? JSON.stringify(init.body) : undefined,
      throw: false,
    });

    if (res.status === 401) await this.unauthorized(baseUrl, token ?? "");
    if (res.status < 200 || res.status >= 300) {
      let msg = `HTTP ${res.status}`;
      try {
        msg = (res.json as { error?: string })?.error ?? msg;
      } catch {
        // Non-JSON error bodies (e.g. from a proxy) keep the status message.
      }
      throw new HttpError(msg, res.status);
    }
    return res.json as T;
  }

  private async unauthorized(baseUrl: string, token: string): Promise<never> {
    if (
      normalizeServerUrl(baseUrl) === this.baseUrl &&
      token !== "" &&
      token === this.tokenRecord()?.token
    ) {
      await this.clearSession();
    }
    throw new AuthError("Session expired. Please sign in again.");
  }

  // --- session ---------------------------------------------------------------

  /**
   * Deep-link fallback when no sign-in is waiting (e.g. Obsidian restarted
   * between opening the browser and the callback): validate the token against
   * the active server, then commit it. Nothing is stored unless it validates.
   */
  async setSession(token: string): Promise<MeResponse> {
    const serverUrl = this.baseUrl;
    const attempt = this.loginAttempt;
    const { serverId } = await this.serverInfoChecked(serverUrl);
    this.assertLoginAttempt(attempt);
    const me = await this.apiAt<MeResponse>(serverUrl, "/api/me", token);
    this.assertLoginAttempt(attempt);
    if (serverUrl !== this.baseUrl) throw new Error("Sign-in cancelled: active server changed.");
    await this.applySessionAt(serverUrl, serverId, token, me);
    return me;
  }

  /**
   * Throw unless `session` came from the current SSO attempt. Cancelling,
   * timing out, unloading, or starting another attempt invalidates it.
   */
  assertCurrentLogin(session: AuthenticatedSession): void {
    this.assertLoginAttempt(session.attempt);
  }

  private assertLoginAttempt(attempt: number): void {
    if (attempt !== this.loginAttempt) {
      throw new Error("Sign-in was cancelled or replaced by a newer sign-in.");
    }
  }

  /**
   * Make a validated SSO session the active one: server URL, server id,
   * account, token, and (when given) the vault binding are committed together
   * in one settings save. Rejects a session from a stale attempt.
   */
  async commitSession(session: AuthenticatedSession, activeVaultId?: string): Promise<void> {
    this.assertCurrentLogin(session);
    await this.applySessionAt(
      session.serverUrl,
      session.serverId,
      session.token,
      session.me,
      activeVaultId,
    );
  }

  /**
   * Single commit point for every sign-in path. All inputs were validated
   * against `serverUrl` (caps + `/api/me`) before this runs, so the active
   * compatibility state is the committed server's (compatible).
   *
   * Vault ids belong to one server: without an explicit `activeVaultId`, a
   * binding to a different server's vault is dropped rather than carried over.
   */
  private async applySessionAt(
    serverUrl: string,
    serverId: string,
    token: string,
    me: MeResponse,
    activeVaultId?: string,
  ): Promise<void> {
    this.cancelPendingLogin("Sign-in cancelled: the active session changed.");
    const settings = this.plugin.settings;
    if (activeVaultId !== undefined) {
      settings.activeVaultId = activeVaultId;
    } else if (settings.authServerId && settings.authServerId !== serverId) {
      settings.activeVaultId = "";
    }
    settings.authServerUrl = serverUrl;
    settings.authServerId = serverId;
    this.plugin.lastCompatibilityError = null;
    settings.userId = me.userId;
    this.setToken(token, me.userId);
    this.rememberSession(me, this.tokenKey(me.userId));
    this.applyIdentity(me);
    await this.plugin.saveSettings();
    this.plugin.updateLocalAwareness();
  }

  private applyIdentity(me: MeResponse): void {
    this.plugin.settings.userId = me.userId;
    this.plugin.settings.userDisplayName = me.displayName;
    this.plugin.settings.userEmail = me.email;
    this.plugin.settings.gitEmail = me.gitEmail ?? "";
    this.plugin.settings.userPictureUrl = me.pictureUrl ?? "";
    this.plugin.settings.userAvatarUrlOverride = me.avatarUrlOverride ?? "";
    this.plugin.settings.userAvatarUrl = me.avatarUrl ?? "";
    // Default the cursor name to the SSO display name. defaultSettings()
    // pre-fills a random two-word placeholder, so a falsy check alone would
    // never adopt the account name; replace the name unless the user has
    // explicitly customized it in settings.
    if (me.displayName && !this.plugin.settings.clientNameCustomized) {
      this.plugin.settings.clientName = me.displayName;
    }
  }

  private async clearSession(): Promise<void> {
    this.deleteToken();
    this.plugin.settings.userId = "";
    this.plugin.settings.userDisplayName = "";
    this.plugin.settings.userEmail = "";
    this.plugin.settings.gitEmail = "";
    this.plugin.settings.userPictureUrl = "";
    this.plugin.settings.userAvatarUrlOverride = "";
    this.plugin.settings.userAvatarUrl = "";
    await this.plugin.saveSettings();
  }

  destroy(): void {
    this.cancelPendingLogin("Sign-in cancelled: plugin unloaded.");
  }

  /**
   * Log out: drop the session and the active vault binding locally first, so
   * nothing can start against them while the server revocation is in flight.
   */
  async logout(): Promise<void> {
    this.cancelPendingLogin("Sign-in cancelled: signed out.");
    const baseUrl = this.baseUrl;
    const token = this.getToken();
    this.plugin.settings.activeVaultId = "";
    await this.clearSession();
    try {
      if (token) await this.apiAt(baseUrl, "/api/logout", token, { method: "POST", body: {} });
    } catch (e) {
      console.warn("[Realtime] server logout failed", e);
    }
  }

  me(): Promise<MeResponse> {
    return this.api<MeResponse>("/api/me");
  }

  async updateMe(body: {
    gitEmail?: string | null;
    avatarUrlOverride?: string | null;
  }): Promise<MeResponse> {
    const me = await this.api<MeResponse>("/api/me", { method: "PATCH", body });
    this.plugin.settings.gitEmail = me.gitEmail ?? "";
    this.plugin.settings.userPictureUrl = me.pictureUrl ?? "";
    this.plugin.settings.userAvatarUrlOverride = me.avatarUrlOverride ?? "";
    this.plugin.settings.userAvatarUrl = me.avatarUrl ?? "";
    await this.plugin.saveSettings();
    return me;
  }

  // --- login flow ------------------------------------------------------------

  /**
   * Sign in to `baseUrl` through SSO and make it the active server and account.
   * A vault binding is retained only when it belongs to the same server.
   */
  async loginToServer(baseUrl: string): Promise<MeResponse> {
    const session = await this.authenticateAt(baseUrl);
    await this.commitSession(session);
    return session.me;
  }

  async validSessionsForServer(baseUrl: string): Promise<KnownSession[]> {
    const normalized = normalizeServerUrl(baseUrl);
    const { serverId } = await this.serverInfoChecked(normalized);
    let sessions = this.knownSessions().filter((session) => {
      return (
        session.serverUrl === normalized &&
        session.serverId === serverId &&
        !!this.plugin.app.secretStorage.getSecret(session.tokenKey)
      );
    });
    const oldServerKey = serverSessionTokenKey(normalized, serverId);
    if (
      this.plugin.app.secretStorage.getSecret(oldServerKey) &&
      !sessions.some((session) => session.tokenKey === oldServerKey)
    ) {
      sessions = [
        ...sessions,
        {
          serverUrl: normalized,
          serverId,
          userId: "",
          email: "",
          displayName: "",
          tokenKey: oldServerKey,
        },
      ];
    }
    const valid: KnownSession[] = [];
    for (const session of sessions) {
      const token = this.plugin.app.secretStorage.getSecret(session.tokenKey);
      if (!token) continue;
      try {
        const me = await this.apiAt<MeResponse>(normalized, "/api/me", token);
        const updated = {
          ...session,
          ...me,
          serverUrl: normalized,
          serverId,
          tokenKey: session.tokenKey,
        };
        valid.push(updated);
        this.rememberSession(updated, session.tokenKey, normalized, serverId);
      } catch (e) {
        if (e instanceof AuthError) this.forgetSession(session.tokenKey);
      }
    }
    return valid;
  }

  async useKnownSession(session: KnownSession): Promise<MeResponse> {
    const token = this.plugin.app.secretStorage.getSecret(session.tokenKey);
    if (!token) throw new AuthError("Saved session not found. Please sign in again.");
    this.cancelPendingLogin("Sign-in cancelled: a saved session was selected.");
    const attempt = this.loginAttempt;
    // Verify compatibility and the token before trusting the saved session's
    // server. A server upgrade that broke a cap, or a revoked token, must not
    // replace the active session.
    const serverUrl = normalizeServerUrl(session.serverUrl);
    const { serverId } = await this.serverInfoChecked(serverUrl);
    this.assertLoginAttempt(attempt);
    const me = await this.apiAt<MeResponse>(serverUrl, "/api/me", token);
    this.assertLoginAttempt(attempt);
    await this.applySessionAt(serverUrl, serverId, token, me);
    return me;
  }

  /**
   * Open the browser to `baseUrl`'s SSO page and resolve once the token from
   * the `obsidian://realtime-auth?token=…` deep link (or the paste-code
   * fallback) validates against that server. Changes no active session,
   * server, or vault state: commit the result with {@link commitSession}.
   *
   * Starting another attempt, aborting `signal`, the 5 minute timeout, or
   * unloading cancels this attempt, before or after it resolves.
   */
  async authenticateAt(baseUrl: string, signal?: AbortSignal): Promise<AuthenticatedSession> {
    const serverUrl = normalizeServerUrl(baseUrl);
    signal?.throwIfAborted();
    this.cancelPendingLogin("Sign-in cancelled: a newer sign-in started.");
    const attempt = this.loginAttempt;
    signal?.addEventListener(
      "abort",
      () => {
        if (attempt === this.loginAttempt) this.cancelPendingLogin("Sign-in cancelled.");
      },
      { once: true },
    );
    // Verify compatibility before opening the SSO browser flow — a cap
    // mismatch should hard-block before the user is sent to the browser.
    const { serverId } = await this.serverInfoChecked(serverUrl);
    signal?.throwIfAborted();
    this.assertLoginAttempt(attempt);
    const redirect = encodeURIComponent("obsidian://realtime-auth");
    window.open(`${serverUrl}/auth/login?redirect=${redirect}`);
    return new Promise<AuthenticatedSession>((resolve, reject) => {
      this.pendingReject = reject;
      this.pendingLogin = async (token: string) => {
        this.pendingLogin = null;
        try {
          const me = await this.apiAt<MeResponse>(serverUrl, "/api/me", token);
          this.assertLoginAttempt(attempt);
          resolve({ serverUrl, serverId, token, me, attempt });
          return me;
        } catch (e) {
          reject(e);
          throw e;
        } finally {
          if (attempt === this.loginAttempt) {
            this.pendingReject = null;
            if (this.pendingTimer !== null) {
              window.clearTimeout(this.pendingTimer);
              this.pendingTimer = null;
            }
          }
        }
      };
      // Abandon the wait after 5 minutes so the promise can't leak forever.
      // Guard on the attempt so a stale timer can't cancel a newer attempt.
      this.pendingTimer = window.setTimeout(
        () => {
          if (attempt === this.loginAttempt) this.cancelPendingLogin("Login timed out.");
        },
        5 * 60 * 1000,
      );
    });
  }

  /**
   * Invalidate the current SSO attempt: reject its wait if still pending, and
   * make any session it already produced (or is validating) uncommittable.
   */
  private cancelPendingLogin(reason: string): void {
    const reject = this.pendingReject;
    this.loginAttempt++;
    this.pendingLogin = null;
    this.pendingReject = null;
    if (this.pendingTimer !== null) {
      window.clearTimeout(this.pendingTimer);
      this.pendingTimer = null;
    }
    reject?.(new Error(reason));
  }

  /**
   * Called by the registered protocol handler in main.ts. A token for an
   * in-flight sign-in is handed to that flow, which owns committing it and
   * restarting sync; resolves `null` then. Otherwise (e.g. Obsidian restarted
   * after opening the browser) the token is validated and committed for the
   * active server, and the new identity is returned.
   */
  async handleProtocol(params: Record<string, string>): Promise<MeResponse | null> {
    const token = params.token;
    if (!token) return null;
    if (this.pendingLogin) {
      await this.pendingLogin(token);
      return null;
    }
    // Only a fresh plugin instance may accept an orphaned post-restart link.
    // Late or duplicate callbacks from a staged/cancelled flow never commit.
    if (this.loginAttempt !== 0) return null;
    return this.setSession(token);
  }

  /**
   * Paste-code fallback for when the `obsidian://` deep link doesn't fire:
   * feed the token shown in the browser to the in-flight sign-in, whose caller
   * reports the outcome. Returns false when no sign-in is waiting for a code.
   */
  submitPastedCode(token: string): boolean {
    const trimmed = token.trim();
    if (!trimmed || !this.pendingLogin) return false;
    this.pendingLogin(trimmed).catch(() => {
      // The awaiting authenticateAt caller surfaces the failure.
    });
    return true;
  }

  private rememberSession(
    me: MeResponse,
    tokenKey: string,
    serverUrl = this.baseUrl,
    serverId = this.plugin.settings.authServerId,
  ): void {
    const session = this.knownSession(me, tokenKey, serverUrl, serverId);
    this.saveKnownSessions([
      session,
      ...this.knownSessions().filter((existing) => existing.tokenKey !== tokenKey),
    ]);
  }

  private knownSession(
    me: MeResponse,
    tokenKey: string,
    serverUrl: string,
    serverId: string,
  ): KnownSession {
    return {
      serverUrl,
      serverId,
      userId: me.userId,
      email: me.email,
      gitEmail: me.gitEmail,
      displayName: me.displayName,
      pictureUrl: me.pictureUrl,
      avatarUrlOverride: me.avatarUrlOverride,
      avatarUrl: me.avatarUrl,
      tokenKey,
    };
  }

  private forgetSession(tokenKey: string): void {
    this.saveKnownSessions(this.knownSessions().filter((session) => session.tokenKey !== tokenKey));
  }

  private knownSessions(): KnownSession[] {
    try {
      const raw = window.localStorage.getItem(KNOWN_SESSIONS_KEY);
      const parsed = raw ? JSON.parse(raw) : [];
      if (!Array.isArray(parsed)) return [];
      return parsed.filter((session): session is KnownSession => {
        if (
          !session ||
          typeof session !== "object" ||
          typeof session.serverUrl !== "string" ||
          typeof session.serverId !== "string" ||
          typeof session.userId !== "string" ||
          typeof session.email !== "string" ||
          typeof session.displayName !== "string" ||
          typeof session.tokenKey !== "string"
        ) {
          return false;
        }
        // Avatar fields are optional on old sessions; when present they must
        // be a string or null.
        const pic = session.pictureUrl;
        const override = session.avatarUrlOverride;
        const avatar = session.avatarUrl;
        if (
          (pic !== undefined && pic !== null && typeof pic !== "string") ||
          (override !== undefined && override !== null && typeof override !== "string") ||
          (avatar !== undefined && avatar !== null && typeof avatar !== "string")
        ) {
          return false;
        }
        return true;
      });
    } catch {
      return [];
    }
  }

  private saveKnownSessions(sessions: KnownSession[]): void {
    try {
      window.localStorage.setItem(KNOWN_SESSIONS_KEY, JSON.stringify(sessions));
    } catch {
      // Token discovery is an enhancement; auth still works without the index.
    }
  }

  // --- vaults / sharing ------------------------------------------------------

  listVaults(): Promise<VaultInfo[]> {
    return this.api<VaultInfo[]>("/api/vaults");
  }

  listVaultsAt(baseUrl: string, token: string): Promise<VaultInfo[]> {
    return this.apiAt<VaultInfo[]>(baseUrl, "/api/vaults", token);
  }

  createVault(name: string): Promise<VaultInfo> {
    return this.api<VaultInfo>("/api/vaults", { method: "POST", body: { name } });
  }

  createInvite(vaultId: string, role?: "admin" | "member"): Promise<{ code: string }> {
    return this.api<{ code: string }>(`/api/vaults/${vaultId}/invites`, {
      method: "POST",
      body: { role },
    });
  }

  redeemInvite(code: string): Promise<{ vaultId: string; name: string }> {
    return this.api<{ vaultId: string; name: string }>("/api/invites/redeem", {
      method: "POST",
      body: { code },
    });
  }

  listMembers(vaultId: string): Promise<MemberInfo[]> {
    return this.api<MemberInfo[]>(`/api/vaults/${vaultId}/members`);
  }

  promoteMember(vaultId: string, userId: string): Promise<MemberInfo> {
    return this.api<MemberInfo>(`/api/vaults/${vaultId}/members/${userId}/promote`, {
      method: "POST",
      body: {},
    });
  }

  async removeMember(vaultId: string, userId: string): Promise<void> {
    await this.api(`/api/vaults/${vaultId}/members/${userId}`, { method: "DELETE" });
  }

  listCursors(vaultId: string): Promise<RemoteCursorInfo[]> {
    return this.api<RemoteCursorInfo[]>(`/api/vaults/${vaultId}/cursors`);
  }

  createCursor(vaultId: string, name: string): Promise<RemoteCursorInfo & { secretToken: string }> {
    return this.api<RemoteCursorInfo & { secretToken: string }>(`/api/vaults/${vaultId}/cursors`, {
      method: "POST",
      body: { name },
    });
  }

  renameCursor(vaultId: string, cursorId: string, name: string): Promise<RemoteCursorInfo> {
    return this.api<RemoteCursorInfo>(`/api/vaults/${vaultId}/cursors/${cursorId}`, {
      method: "POST",
      body: { name },
    });
  }

  regenerateCursorToken(vaultId: string, cursorId: string): Promise<{ secretToken: string }> {
    return this.api<{ secretToken: string }>(`/api/vaults/${vaultId}/cursors/${cursorId}/token`, {
      method: "POST",
      body: {},
    });
  }

  async deleteCursor(vaultId: string, cursorId: string): Promise<void> {
    await this.api(`/api/vaults/${vaultId}/cursors/${cursorId}`, { method: "DELETE" });
  }

  /** Get-or-create the plugin-managed cursor for (vault, plugin) and mint a fresh token. */
  acquirePluginCursor(
    vaultId: string,
    pluginId: string,
    name?: string,
  ): Promise<PluginCursorGrant> {
    return this.api<PluginCursorGrant>(`/api/vaults/${vaultId}/cursors/plugin`, {
      method: "POST",
      body: { pluginId, name },
    });
  }

  listCursorAudit(
    vaultId: string,
    cursorId: string,
    before?: number,
    limit?: number,
  ): Promise<CursorAuditPage> {
    const params = new URLSearchParams();
    if (before !== undefined) params.set("before", String(before));
    if (limit !== undefined) params.set("limit", String(limit));
    const query = params.size ? `?${params.toString()}` : "";
    return this.api<CursorAuditPage>(`/api/vaults/${vaultId}/cursors/${cursorId}/audit${query}`);
  }

  async undoCursorAudit(
    vaultId: string,
    cursorId: string,
    entryId: string,
    force = false,
  ): Promise<void> {
    await this.api(`/api/vaults/${vaultId}/cursors/${cursorId}/audit/${entryId}/undo`, {
      method: "POST",
      body: { force },
    });
  }

  getGitBackup(vaultId: string): Promise<GitBackupConfig> {
    return this.api<GitBackupConfig>(`/api/vaults/${vaultId}/backup`);
  }

  putGitBackup(vaultId: string, body: PutGitBackupBody): Promise<GitBackupConfig> {
    return this.api<GitBackupConfig>(`/api/vaults/${vaultId}/backup`, {
      method: "PUT",
      body,
    });
  }

  async deleteGitBackup(vaultId: string): Promise<void> {
    await this.api(`/api/vaults/${vaultId}/backup`, { method: "DELETE" });
  }

  testGitBackup(vaultId: string): Promise<{ ok: boolean; error?: string }> {
    return this.api<{ ok: boolean; error?: string }>(`/api/vaults/${vaultId}/backup/test`, {
      method: "POST",
      body: {},
    });
  }

  /** Resolve a stable, shareable permalink (`…/n/{guid}`) for a note by path. */
  notePermalink(vaultId: string, path: string): Promise<PermalinkResponse> {
    const encoded = path.split("/").map(encodeURIComponent).join("/");
    return this.api<PermalinkResponse>(`/api/vaults/${vaultId}/note-permalinks/${encoded}`, {
      method: "POST",
      body: {},
    });
  }

  /**
   * Create (or return the existing) public read-only share link for a note.
   * Idempotent server-side; the returned `url` points at `/view/{id}`.
   */
  createPublicShare(vaultId: string, path: string): Promise<PublicShareResponse> {
    return this.api<PublicShareResponse>(`/api/vaults/${vaultId}/shares`, {
      method: "POST",
      body: { path },
    });
  }

  /** Stop publicly sharing a note. Rejects with a 404 error if not shared. */
  async deletePublicShare(vaultId: string, path: string): Promise<void> {
    const params = new URLSearchParams({ path });
    await this.api(`/api/vaults/${vaultId}/shares?${params.toString()}`, { method: "DELETE" });
  }

  /** Create (or return the existing) public link for a binary attachment. */
  createPublicAttachmentShare(
    vaultId: string,
    path: string,
  ): Promise<PublicAttachmentShareResponse> {
    return this.api<PublicAttachmentShareResponse>(`/api/vaults/${vaultId}/attachment-shares`, {
      method: "POST",
      body: { path },
    });
  }

  /** Stop publicly sharing a binary attachment. */
  async deletePublicAttachmentShare(vaultId: string, path: string): Promise<void> {
    const params = new URLSearchParams({ path });
    await this.api(`/api/vaults/${vaultId}/attachment-shares?${params.toString()}`, {
      method: "DELETE",
    });
  }

  search(vaultId: string, q: string, limit?: number): Promise<SearchHit[]> {
    const params = new URLSearchParams({ q });
    if (limit !== undefined) params.set("limit", String(limit));
    return this.api<SearchHit[]>(`/api/vaults/${vaultId}/search?${params.toString()}`);
  }

  listTags(vaultId: string): Promise<TagCount[]> {
    return this.api<TagCount[]>(`/api/vaults/${vaultId}/tags`);
  }

  backlinks(vaultId: string, path: string): Promise<SearchHit[]> {
    const encoded = path.split("/").map(encodeURIComponent).join("/");
    return this.api<SearchHit[]>(`/api/vaults/${vaultId}/backlinks/${encoded}`);
  }

  /** Best-effort registry update so the server can resolve doc → path for ACLs. */
  async registerFile(vaultId: string, guid: string, path: string): Promise<void> {
    try {
      await this.api(`/api/vaults/${vaultId}/files`, {
        method: "POST",
        body: { guid, path },
      });
    } catch (e) {
      console.warn("[Realtime] file registry update failed", e);
    }
  }

  /** Mint a sync token for a namespaced document in the active vault. */
  docToken(
    vaultId: string,
    docId: string,
    path?: string,
    authorization?: "full" | "read-only",
  ): Promise<ClientToken> {
    return this.api<ClientToken>("/api/doc-token", {
      method: "POST",
      body: {
        vaultId,
        docId,
        ...(path ? { path } : {}),
        ...(authorization ? { authorization } : {}),
      },
    });
  }

  // --- binary blob store -----------------------------------------------------
  //
  // Binary file contents are stored content-addressed by sha256 hash, separate
  // from the JSON API (these carry raw bytes, not JSON). All three are vault
  // scoped: the server requires membership of `vaultId`.

  /**
   * `path` scopes the request to one vault path (the server then serves only
   * the hash that path currently maps to). `null` requests the blob by hash
   * alone, which the server permits for principals with uniform vault access.
   */
  private blobUrl(
    vaultId: string,
    path: string | null,
    hash: string,
    extra: Record<string, string> = {},
  ): string {
    const params = new URLSearchParams(extra);
    if (path !== null) params.set("path", path);
    const query = params.toString();
    return `${this.baseUrl}/api/vaults/${vaultId}/blobs/${hash}${query ? `?${query}` : ""}`;
  }

  /**
   * True if the server already holds the blob (lets callers skip re-upload).
   * With `size`, a stored copy of a different length (a torn write) does not
   * count, so the caller re-uploads and repairs it.
   */
  async blobExists(vaultId: string, path: string, hash: string, size?: number): Promise<boolean> {
    const token = this.getToken();
    const res = await requestUrl({
      url: this.blobUrl(vaultId, path, hash, size === undefined ? {} : { size: String(size) }),
      method: "HEAD",
      headers: token ? { Authorization: `Bearer ${token}` } : {},
      throw: false,
    });
    if (res.status === 401) await this.unauthorized(this.baseUrl, token);
    if (res.status >= 200 && res.status < 300) return true;
    if (res.status === 404) return false;
    throw new HttpError(`blob lookup failed: HTTP ${res.status}`, res.status);
  }

  /** Download blob bytes by hash (see {@link blobUrl} for `path`). */
  async getBlob(vaultId: string, path: string | null, hash: string): Promise<ArrayBuffer> {
    const token = this.getToken();
    const res = await requestUrl({
      url: this.blobUrl(vaultId, path, hash),
      method: "GET",
      headers: token ? { Authorization: `Bearer ${token}` } : {},
      throw: false,
    });
    if (res.status === 401) await this.unauthorized(this.baseUrl, token);
    if (res.status < 200 || res.status >= 300) {
      throw new HttpError(`blob download failed: ${blobErrorMessage(res)}`, res.status);
    }
    return res.arrayBuffer;
  }

  /** Upload blob bytes; idempotent and content-verified server-side. */
  async putBlob(vaultId: string, path: string, hash: string, data: ArrayBuffer): Promise<void> {
    const token = this.getToken();
    const res = await requestUrl({
      url: this.blobUrl(vaultId, path, hash),
      method: "PUT",
      headers: token ? { Authorization: `Bearer ${token}` } : {},
      contentType: "application/octet-stream",
      body: data,
      throw: false,
    });
    if (res.status === 401) await this.unauthorized(this.baseUrl, token);
    if (res.status < 200 || res.status >= 300) {
      throw new HttpError(`blob upload failed: ${blobErrorMessage(res)}`, res.status);
    }
  }

  // --- storage management ----------------------------------------------------

  /** Per-vault storage breakdown (admin only). */
  getStorageUsage(vaultId: string): Promise<StorageUsage> {
    return this.api<StorageUsage>(`/api/vaults/${vaultId}/storage`);
  }

  /** Delete orphaned ("previous") blobs, optionally only those ≥ `minBytes`. */
  gcBlobs(vaultId: string, minBytes?: number): Promise<GcBlobsResult> {
    return this.api<GcBlobsResult>(`/api/vaults/${vaultId}/storage/gc-blobs`, {
      method: "POST",
      body: minBytes !== undefined ? { minBytes } : {},
    });
  }

  /**
   * Reclaim a single orphaned blob (no-op server-side while still referenced).
   * `trashEntryId` names the trash entry being deleted, whose own reference to
   * the blob must not keep it alive; other trash entries still do.
   */
  async deleteBlob(vaultId: string, hash: string, trashEntryId?: string): Promise<void> {
    const query = trashEntryId ? `?trashEntry=${encodeURIComponent(trashEntryId)}` : "";
    await this.api(`/api/vaults/${vaultId}/blobs/${hash}${query}`, { method: "DELETE" });
  }

  // --- plugin databases (synced SQLite) --------------------------------------

  private pluginDbPath(vaultId: string, pluginId: string, name: string, suffix = ""): string {
    const p = encodeURIComponent(pluginId);
    const n = encodeURIComponent(name);
    return `/api/vaults/${vaultId}/plugin-dbs/${p}/${n}${suffix}`;
  }

  /**
   * Pull the server replica's changeset past `cursor` (bootstrap / rebase),
   * with the per-site cursor the changeset covers (absent on older servers).
   */
  async pluginDbChanges(
    vaultId: string,
    pluginId: string,
    name: string,
    cursor: Record<string, number>,
  ): Promise<{ changes: import("./pluginDb/types").ChangeRow[]; cursor?: Record<string, number> }> {
    const since = encodeURIComponent(JSON.stringify(cursor ?? {}));
    const res = await this.api<{
      changes?: import("./pluginDb/types").ChangeRow[];
      cursor?: Record<string, number>;
    }>(this.pluginDbPath(vaultId, pluginId, name, `/changes?since=${since}`));
    return { changes: res.changes ?? [], cursor: res.cursor };
  }

  /** Tell the server a publish happened, so it replicates + commits to git. */
  async touchPluginDb(vaultId: string, pluginId: string, name: string): Promise<void> {
    await this.api(this.pluginDbPath(vaultId, pluginId, name, "/touch"), {
      method: "POST",
      body: {},
    });
  }

  /** Purge a plugin database: delete the server replica + git dump (irreversible). */
  async deletePluginDb(vaultId: string, pluginId: string, name: string): Promise<void> {
    await this.api(this.pluginDbPath(vaultId, pluginId, name), { method: "DELETE" });
  }

  // --- git history + rollback --------------------------------------------------

  /** Page through the vault's git history (optionally one file's, `--follow`). */
  listHistoryCommits(
    vaultId: string,
    opts?: { limit?: number; before?: string; path?: string },
  ): Promise<import("./history/types").CommitListPage> {
    const params = new URLSearchParams();
    if (opts?.limit) params.set("limit", String(opts.limit));
    if (opts?.before) params.set("before", opts.before);
    if (opts?.path) params.set("path", opts.path);
    const qs = params.toString();
    return this.api(`/api/vaults/${vaultId}/history/commits${qs ? `?${qs}` : ""}`);
  }

  /** Commit metadata plus its change list. */
  getHistoryCommit(vaultId: string, hash: string): Promise<import("./history/types").CommitDetail> {
    return this.api(`/api/vaults/${vaultId}/history/commits/${hash}`);
  }

  /** Full file tree at a commit. */
  getHistoryTree(vaultId: string, hash: string): Promise<import("./history/types").HistoryTree> {
    return this.api(`/api/vaults/${vaultId}/history/commits/${hash}/tree`);
  }

  /** One path's content at a commit (text, binary metadata, or absent). */
  getHistoryFile(
    vaultId: string,
    hash: string,
    path: string,
  ): Promise<import("./history/types").FileAtCommit> {
    return this.api(
      `/api/vaults/${vaultId}/history/commits/${hash}/file?path=${encodeURIComponent(path)}`,
    );
  }

  /** Raw bytes of a path at a commit; throws "blob no longer available" on 410. */
  async getHistoryBlob(vaultId: string, hash: string, path: string): Promise<ArrayBuffer> {
    const token = this.getToken();
    const res = await requestUrl({
      url: `${this.baseUrl}/api/vaults/${vaultId}/history/commits/${hash}/blob?path=${encodeURIComponent(path)}`,
      method: "GET",
      headers: token ? { Authorization: `Bearer ${token}` } : {},
      throw: false,
    });
    if (res.status === 401) await this.unauthorized(this.baseUrl, token);
    if (res.status === 410) throw new Error("blob no longer available");
    if (res.status < 200 || res.status >= 300) {
      throw new Error(`history blob download failed: ${blobErrorMessage(res)}`);
    }
    return res.arrayBuffer;
  }

  /** Dry-run a rollback (admin only). Pass `path` to scope to a single file. */
  rollbackPreview(
    vaultId: string,
    hash: string,
    opts?: { path?: string; targetPath?: string },
  ): Promise<import("./history/types").RollbackPlan> {
    if (opts?.targetPath && !opts.path) {
      throw new Error("targetPath requires path");
    }
    const qs = new URLSearchParams();
    if (opts?.path) qs.set("path", opts.path);
    if (opts?.targetPath) qs.set("targetPath", opts.targetPath);
    const suffix = qs.toString();
    return this.api(
      `/api/vaults/${vaultId}/history/commits/${hash}/rollback/preview${suffix ? `?${suffix}` : ""}`,
      { method: "POST", body: {} },
    );
  }

  /** Execute a rollback (admin only); `pluginDbs` opts databases in (vault scope only). */
  rollbackVault(
    vaultId: string,
    hash: string,
    opts?: { path?: string; targetPath?: string; pluginDbs?: { plugin: string; name: string }[] },
  ): Promise<import("./history/types").RollbackResult> {
    if (opts?.targetPath && !opts.path) {
      throw new Error("targetPath requires path");
    }
    if (opts?.path && opts.pluginDbs && opts.pluginDbs.length > 0) {
      throw new Error("pluginDbs cannot be combined with path");
    }
    const qs = new URLSearchParams();
    if (opts?.path) qs.set("path", opts.path);
    if (opts?.targetPath) qs.set("targetPath", opts.targetPath);
    const suffix = qs.toString();
    return this.api(
      `/api/vaults/${vaultId}/history/commits/${hash}/rollback${suffix ? `?${suffix}` : ""}`,
      { method: "POST", body: { pluginDbs: opts?.pluginDbs ?? [] } },
    );
  }
}

/**
 * Build an error message from a failed blob response without touching the
 * response's lazy `.json` getter, which throws on an empty body (e.g. axum's
 * default 404, which has no body — the symptom when the server lacks the blob
 * routes). Falls back to the bare status code.
 */
function blobErrorMessage(res: { status: number; text?: string }): string {
  const text = (res.text ?? "").trim();
  if (text) {
    try {
      const parsed = JSON.parse(text) as { error?: string };
      if (parsed?.error) return parsed.error;
    } catch {
      return `HTTP ${res.status}: ${text.slice(0, 200)}`;
    }
  }
  return `HTTP ${res.status}`;
}

/** Legacy, un-namespaced SecretStorage key (single global session token). */
const LEGACY_TOKEN_KEY = "realtime-session-token";
const KNOWN_SESSIONS_KEY = "realtime-known-sessions";

/**
 * SecretStorage ids must be lowercase alphanumeric/dashes and 64 chars max.
 * Use only hashes in the variable portion so arbitrary UUID/user id formats
 * cannot push the id over the limit or leave invalid punctuation behind.
 */
function serverSessionTokenKey(serverUrl: string, serverId: string): string {
  const hash = shortHash(`${normalizeServerUrl(serverUrl)}\n${serverId}`);
  return `${LEGACY_TOKEN_KEY}-server-${hash}`;
}

function sessionTokenKey(serverUrl: string, serverId: string, userId: string): string {
  const serverHash = shortHash(`${normalizeServerUrl(serverUrl)}\n${serverId}`);
  const userHash = shortHash(`${normalizeServerUrl(serverUrl)}\n${serverId}\n${userId}`);
  return `${LEGACY_TOKEN_KEY}-${serverHash}-${userHash}`;
}

function shortHash(value: string): string {
  let hash = 0x811c9dc5;
  for (let i = 0; i < value.length; i++) {
    hash ^= value.charCodeAt(i);
    hash = Math.imul(hash, 0x01000193);
  }
  return (hash >>> 0).toString(36).padStart(7, "0");
}

export function normalizeServerUrl(url: string): string {
  const trimmed = url.trim().replace(/\/+$/, "");
  if (!trimmed) throw new Error("Enter a server URL.");
  const parsed = new URL(trimmed);
  if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
    throw new Error("Server URL must start with http:// or https://.");
  }
  return parsed.toString().replace(/\/$/, "");
}
