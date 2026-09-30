/**
 * Wire types for the Realtime server's REST API. Field names mirror the
 * server's serde serializations (camelCase) exactly; see server/src in the
 * Realtime repository (routes.rs, notes.rs, structured.rs, attachments.rs,
 * search.rs, storage.rs) and the served /openapi.json.
 */

// ---------- auth & identity ----------

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

/** Server identity returned by the public `GET /api/server-info`. */
export interface ServerInfoResponse {
  serverId: string;
  /** Server release semver (operator-facing; not used for gating). Optional for old servers. */
  version?: string;
  /** Named capability versions per surface. Optional for old servers. SDK/CLI consumers can self-gate; only the Obsidian plugin enforces caps. */
  caps?: Record<string, string>;
  /** Cap names the client must understand. Optional on older servers. */
  requiredCaps?: string[];
}

// ---------- vaults, invites, members ----------

export interface VaultInfo {
  id: string;
  name: string;
  role: "admin" | "member";
  createdBy?: string;
  owner?: boolean;
}

export interface CreateVaultBody {
  name: string;
}

export interface CreateInviteBody {
  role?: "admin" | "member";
}

export interface InviteResponse {
  code: string;
}

export interface RedeemResponse {
  vaultId: string;
  name: string;
}

export interface MemberInfo {
  userId: string;
  email: string;
  displayName: string;
  role: "admin" | "member";
  owner?: boolean;
  avatarUrl?: string | null;
}

// ---------- remote cursors ----------

export interface RemoteCursorInfo {
  id: string;
  appId: string;
  name: string;
  mcpUrl: string;
  createdAt: number;
  /** Manifest id of the managing plugin; absent for admin-created cursors. */
  pluginId?: string | null;
}

/** `POST /api/vaults/{id}/cursors` — includes the copy-once secret. */
export interface CreatedRemoteCursor {
  id: string;
  appId: string;
  name: string;
  mcpUrl: string;
  createdAt: number;
  secretToken: string;
}

export interface SecretTokenResponse {
  secretToken: string;
}

/** `POST /api/vaults/{id}/cursors/plugin` — a plugin-managed cursor grant. */
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

// ---------- notes ----------

export interface NoteSummary {
  path: string;
  guid: string;
  permalink: string;
}

export interface Note {
  path: string;
  guid: string;
  content: string;
  permalink: string;
}

export interface PatchNoteBody {
  old: string;
  new: string;
  replaceAll?: boolean;
}

/** A public read-only share link for a note (rendered at `/view/{id}`). */
export interface PublicShare {
  id: string;
  /** Absolute URL of the public viewer page. */
  url: string;
  path: string;
  guid: string;
  createdAt: number;
}

/** A public link to one exact version of a binary attachment. */
export interface PublicAttachmentShare {
  id: string;
  /** Absolute URL of the public attachment. */
  url: string;
  path: string;
  hash: string;
  size: number;
  createdAt: number;
}

/** Stable permalink for a note, returned by the note-permalinks endpoint. */
export interface PermalinkResponse {
  kind: string;
  url: string;
}

export interface FrontmatterResponse {
  path: string;
  frontmatter: unknown;
}

export interface PatchFrontmatterBody {
  set?: Record<string, unknown>;
  unset?: string[];
}

/** Periodic note granularity (`/api/vaults/{id}/periodic/{period}`). */
export type PeriodicPeriod = "daily" | "weekly" | "monthly" | "quarterly" | "yearly";

// ---------- search ----------

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

export interface ReindexResponse {
  count: number;
}

// ---------- canvases & bases (structured docs) ----------

export interface StructuredSummary {
  path: string;
  guid: string;
  kind: string;
  permalink: string;
}

export interface StructuredResponse {
  path: string;
  guid: string;
  kind: string;
  value: unknown;
  permalink: string;
  /**
   * Opaque version of the stored value (newer servers). Pass it as `ifMatch`
   * to `replace` so the write is refused if the file changed since.
   */
  valueHash?: string;
}

/** Preconditions for a full note replace. */
export interface ReplaceNoteOptions {
  /**
   * Lowercase hex SHA-256 of the note content the replacement is based on.
   * The server refuses the write (409) if the note changed since.
   */
  expectedContentHash?: string;
}

/** Preconditions for a full Canvas/Base replace. */
export interface ReplaceStructuredOptions {
  /** `valueHash` from the read the replacement is based on. */
  ifMatch?: string;
}

/** Preconditions for an attachment upload. */
export interface UploadAttachmentOptions {
  /**
   * Content hash (from `list` or an earlier upload) the path must still hold.
   * The server refuses the upload (409 `stale`) if the attachment changed.
   */
  ifMatch?: string;
  /** `"*"`: refuse the upload (409 `exists`) if the path already holds an attachment. */
  ifNoneMatch?: "*";
}

/** Free-form node fields (Obsidian Canvas spec) plus an optional stable id. */
export interface CanvasNodeBody {
  id?: string;
  [field: string]: unknown;
}

export interface CanvasEdgeBody {
  id?: string;
  fromNode: string;
  toNode: string;
  [field: string]: unknown;
}

export interface CanvasFieldPatch {
  set?: Record<string, unknown>;
  remove?: string[];
}

export type CanvasOperation =
  | { type: "node-create"; node: Record<string, unknown> }
  | { type: "node-patch"; id: string; patch: CanvasFieldPatch }
  | { type: "node-delete"; id: string }
  | { type: "node-restore"; node: Record<string, unknown> }
  | { type: "edge-create"; edge: Record<string, unknown> }
  | { type: "edge-patch"; id: string; patch: CanvasFieldPatch }
  | { type: "edge-delete"; id: string }
  | { type: "edge-restore"; edge: Record<string, unknown> }
  | { type: "node-order"; order: string[] }
  | { type: "edge-order"; order: string[] };

export interface CanvasOperationBatch {
  operations: CanvasOperation[];
  mutationId?: string;
}

export interface BaseViewBody {
  name: string;
  type: string;
  [field: string]: unknown;
}

// ---------- attachments & blobs ----------

export interface AttachmentSummary {
  path: string;
  hash: string;
  size: number;
}

export interface UploadAttachmentResponse {
  path: string;
  hash: string;
  size: number;
}

export interface CreateUploadLinkResponse {
  uploadUrl: string;
  expiresAt: number;
  landingDir: string;
  token: string;
}

// ---------- storage & backup ----------

/** Per-vault storage breakdown, from `GET /api/vaults/{id}/storage`. */
export interface StorageUsage {
  blobsCurrentBytes: number;
  blobsPreviousBytes: number;
  currentBlobCount: number;
  previousBlobCount: number;
  /** null when the CRDT store is not readable server-side. */
  plainVaultBytes: number | null;
}

export interface GcBlobsResult {
  removed: number;
  freedBytes: number;
}

export interface DeleteBlobResult {
  deleted: boolean;
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

// ---------- plugin databases (cr-sqlite replication) ----------

/** Tagged, JSON-safe encoding of a single `crsql_changes.val`. */
export type PluginDbEncodedVal =
  | null
  | number
  | string
  | boolean
  | { $blob: string }
  | { $int: string };

export interface PluginDbChangeRow {
  table: string;
  pk: string;
  cid: string;
  val: PluginDbEncodedVal;
  col_version: number;
  db_version: number;
  site_id: string;
  cl: number;
  seq: number;
}

/** Per-device applied cursor: max db_version seen per origin site id (hex). */
export type PluginDbCursor = Record<string, number>;

/** A synced plugin database the server holds a replica for. */
export interface PluginDbInfo {
  plugin: string;
  name: string;
  updatedAt: number;
}

/** Result of a read-only `PluginDbResource.query` call. */
export interface PluginDbQueryResult {
  columns: string[];
  /** Cell values use the tagged wire encoding ({$blob}/{$int}) for non-JSON-native values. */
  rows: PluginDbEncodedVal[][];
  truncated: boolean;
}

/** Result of an `PluginDbResource.execute` call. */
export interface PluginDbExecuteResult {
  rowsAffected: number;
  dbVersion: number;
}

/** One write statement in an `execute` batch (one transaction). */
export interface PluginDbStatement {
  sql: string;
  params?: PluginDbEncodedVal[];
}

// ---------- doc tokens ----------

/** Yjs document token minted by `POST /api/doc-token`. */
export interface DocTokenResponse {
  url?: string;
  baseUrl?: string;
  docId?: string;
  token?: string;
  /** Logical CRDT epoch. A changed value requires a fresh local Y.Doc. */
  epoch?: number;
  /**
   * `epoch` is still being activated; connections to it are refused until it
   * is. Retry the token instead of connecting to an older epoch.
   */
  epochPending?: boolean;
  /**
   * When the server stops accepting `token` (ms since the Unix epoch). Mint a
   * new token before reconnecting after this; absent on older servers.
   */
  expiresAt?: number;
  [key: string]: unknown;
}

// ---------- OAuth 2.1 ----------

export interface OAuthServerMetadata {
  issuer: string;
  authorization_endpoint: string;
  token_endpoint: string;
  registration_endpoint: string;
  code_challenge_methods_supported: string[];
  grant_types_supported: string[];
  response_types_supported: string[];
}

export interface OAuthRegisteredClient {
  client_id: string;
  client_secret?: string | null;
  redirect_uris: string[];
}

/** Tokens from `POST /oauth/token`, with camelCase mapping applied. */
export interface OAuthTokens {
  accessToken: string;
  tokenType: string;
  /** Lifetime in seconds (access tokens live 1 hour). */
  expiresIn: number;
  refreshToken: string;
  scope: string;
}

// ---------- streaming ----------

export type StreamAnchor =
  | { mode: "append" }
  | { mode: "after"; text: string }
  | { mode: "offset"; offset: number };

export interface StreamResult {
  /** Audit-log entry covering the whole streamed insert; null if nothing was inserted. */
  auditId: string | null;
  /** Total UTF-8 bytes inserted into the note. */
  inserted: number;
}

// ---------- git history + rollback ----------

export interface HistoryCommit {
  hash: string;
  shortHash: string;
  parents: string[];
  authorName: string;
  authorEmail: string;
  timestampMs: number;
  subject: string;
  principalId?: string;
  principalType?: string;
  cursorId?: string;
  cursorName?: string;
  onBehalfOf?: string;
  rollbackOf?: string;
  /** When the list was path-filtered, the followed file's path at this commit. */
  pathAtCommit?: string;
}

export interface HistoryCommitListPage {
  commits: HistoryCommit[];
  hasMore: boolean;
}

export interface HistoryCommitChange {
  path: string;
  status: "added" | "modified" | "deleted" | "renamed" | "other";
  renamedTo?: string;
  kind: string;
}

export interface HistoryCommitDetail {
  commit: HistoryCommit;
  changes: HistoryCommitChange[];
}

export interface HistoryTreeEntry {
  path: string;
  size: number;
  kind: string;
}

export interface HistoryTreeResponse {
  entries: HistoryTreeEntry[];
}

export type HistoryFileAtCommit =
  | { type: "text"; content: string; lang: string }
  | { type: "binary"; hash: string; size: number; inline: boolean; blobAvailable: boolean }
  | { type: "absent" };

export interface HistoryPlannedChange {
  path: string;
  kind: string;
  action: "create" | "modify" | "delete" | "restoreBlob";
}

export interface HistoryUnrecoverableBinary {
  path: string;
  hash: string;
  currentKept: boolean;
}

export interface HistoryPluginDbPlan {
  plugin: string;
  name: string;
  changed: boolean;
  rollbackable: boolean;
  reason?: string;
}

export interface HistoryRollbackPlan {
  targetCommit: string;
  changes: HistoryPlannedChange[];
  unrecoverableBinaries: HistoryUnrecoverableBinary[];
  pluginDbs: HistoryPluginDbPlan[];
}

export interface HistoryRollbackResult {
  commit: string | null;
  applied: number;
  deleted: number;
  blobsRestored: number;
  pluginDbsRolledBack: number;
  unrecoverableBinaries: HistoryUnrecoverableBinary[];
}
