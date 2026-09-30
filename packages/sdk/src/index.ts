// Isomorphic entry point. Node-only helpers (browser login, loopback OAuth)
// live in "@realtime-md/sdk/node".

export * from "./types";
export { ApiError, AuthError, ConflictError, NotFoundError, ValidationError } from "./errors";
export {
  Http,
  staticToken,
  encodePath,
  normalizeBaseUrl,
  type TokenProvider,
  type HttpOptions,
  type Query,
} from "./http";
export { RealtimeClient, VaultHandle, type RealtimeClientOptions } from "./client";
export { CursorClient, type CursorClientOptions } from "./cursorClient";
export {
  NoteStream,
  StreamError,
  streamText,
  streamUrlFor,
  MAX_STREAM_BYTES,
  type NoteStreamOptions,
  type WebSocketConstructor,
  type WebSocketLike,
} from "./stream";
export { generateVerifier, challengeS256, generateState } from "./auth/pkce";
export {
  OAuthClient,
  OAuthTokenProvider,
  type OAuthClientOptions,
  type AuthorizeUrlOptions,
  type ExchangeCodeOptions,
} from "./auth/oauth";
export {
  NotesResource,
  FrontmatterResource,
  PeriodicNotesResource,
  noteContentHash,
} from "./resources/notes";
export {
  VaultsResource,
  InvitesResource,
  MembersResource,
  CursorsResource,
  CursorAuditResource,
} from "./resources/vaults";
export { AttachmentsResource, BlobsResource } from "./resources/attachments";
export { CanvasesResource, BasesResource } from "./resources/structured";
export {
  SearchResource,
  StorageResource,
  BackupResource,
  PluginDbResource,
} from "./resources/misc";
export { HistoryResource } from "./resources/history";
export { SharesResource } from "./resources/shares";
