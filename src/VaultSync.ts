import * as Y from "yjs";
import {
  RealtimeProvider,
  SYNC_EVENT_DOCUMENT_INVALIDATED,
  SYNC_EVENT_STATUS,
  SYNC_STATUS_CONNECTED,
  SYNC_STATUS_OFFLINE,
  SYNC_STATUS_ERROR,
  type SyncStatus,
} from "./sync/RealtimeProvider";
import { IndexeddbPersistence, clearDocument } from "y-indexeddb";
import { TFile, TAbstractFile, Notice, Platform, type EventRef } from "obsidian";
import type RealtimePlugin from "./main";
import { getClientToken } from "./sync/clientToken";
import { createMuxSocket } from "./sync/mux";
import { epochPersistenceName, getDocumentEpoch } from "./documentEpoch";
import { Document } from "./Document";
import { CanvasDocument } from "./CanvasDocument";
import { BaseDocument } from "./BaseDocument";
import type { StructuredDocument } from "./StructuredDocument";
import type { DocumentBootstrapOptions, SyncedDoc } from "./SyncedDoc";
import { BinarySync, type BinaryMeta } from "./BinarySync";
import { ConfigSync, type ConfigMeta } from "./ConfigSync";
import { categoryForConfigPath, enabledConfigCategories } from "./configCategories";
import { matchesAnyGlob, parseGlobs } from "./glob";
import { LocalSyncState, type MaterializedKind } from "./localSyncState";
import { isConflictCopy, preserveTextConflict } from "./conflictRecovery";
export { isConflictCopy } from "./conflictRecovery";
import { sha256Text } from "./hash";
import { ensureParentFolder } from "./vaultHelpers";
import { embeddedLinkpaths, resolveAttachmentLink } from "./noteAttachments";
import { BootstrapEventLog } from "./bootstrapEvents";

type FileKind = "text" | "structured" | "binary" | "ignore";
type StructuredKind = "canvas" | "base";
type QueueKind = "text" | StructuredKind;
interface QueueItem {
  path: string;
  guid: string;
  kind: QueueKind;
  reconnect?: boolean;
  /** Content of the document this one replaces after an epoch change. */
  retiredBaseline?: string | null;
}
interface StructuredMeta {
  guid: string;
  kind: StructuredKind;
}

/** Longest a queued document may hold a connection slot before the next starts. */
const QUEUE_SLOT_TIMEOUT_MS = 30_000;
/** Reload an evicted document for invalidations at most this often. */
const INVALIDATION_RELOAD_WINDOW_MS = 5_000;

function isStructuredMeta(value: unknown): value is StructuredMeta {
  if (!value || typeof value !== "object") return false;
  const meta = value as Partial<StructuredMeta>;
  return (
    typeof meta.guid === "string" &&
    meta.guid.length > 0 &&
    (meta.kind === "canvas" || meta.kind === "base")
  );
}

/**
 * Document kind recorded for a trashed (deleted but recoverable) file.
 * `plugindb` is a third-party plugin's synced-SQLite database (see src/pluginDb).
 */
export type TrashKind = "text" | "canvas" | "base" | "binary" | "plugindb";

/** Stored value of a `trash` map entry (the map key is the entry id). */
export interface TrashEntryValue {
  /**
   * Vault-relative path the file had when it was deleted. For `plugindb`
   * entries this is a human-readable label (`${pluginId}/${name}`) so the
   * existing path filter/column keeps working.
   */
  path: string;
  kind: TrashKind;
  /** Document guid for text/structured kinds; lets restore re-pull content. */
  guid?: string;
  /** Blob hash + byte size for binary kinds; lets restore re-download bytes. */
  hash?: string;
  size?: number;
  /** Owning plugin id (only for `plugindb` kind). */
  pluginId?: string;
  /** Database name (only for `plugindb` kind). */
  name?: string;
  /** Deletion time (ms epoch), for newest-first ordering. */
  deletedAt: number;
}

/** A trash entry with its map-key id attached, as surfaced to the UI. */
export interface TrashEntry extends TrashEntryValue {
  id: string;
}

function newGuid(): string {
  if (typeof crypto !== "undefined" && "randomUUID" in crypto) {
    return crypto.randomUUID();
  }
  return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    const v = c === "x" ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}

/**
 * Coordinates collaborative sync for the whole vault:
 *  - maintains a shared "file index" (path -> doc guid) in its own Y.Doc, so
 *    file creations/deletions propagate between clients;
 *  - owns a {@link Document} per Markdown file.
 *
 * One vault maps to one sync server (configured by URL + vault id).
 */
export class VaultSync {
  private plugin: RealtimePlugin;
  private indexDoc: Y.Doc;
  private indexProvider: RealtimeProvider;
  private indexPersistence: IndexeddbPersistence;
  /** Shared map of vault-relative path -> document guid. */
  private files: Y.Map<string>;
  /** Shared map of vault-relative structured path -> document metadata. */
  private structured: Y.Map<StructuredMeta>;
  /** Shared map of trash entry id -> deleted-file metadata (recoverable). */
  private trash: Y.Map<TrashEntryValue>;
  /** Sibling sync path for binary (non-Markdown) files; shares the index doc. */
  private binarySync: BinarySync;
  /** Per-device opt-in sync for whitelisted files under `.obsidian`. */
  private configSync: ConfigSync;
  /** Device-local proof that a path was previously materialized on disk. */
  private localSyncState: LocalSyncState;

  private documents = new Map<string, Document>();
  private structuredDocuments = new Map<string, StructuredDocument>();
  private destroyed = false;
  /** Mobile-only lifecycle gate: retain CRDT state while releasing network channels. */
  private mobileSuspended = false;
  private initialSynced = false;
  private initialSyncRunning = false;
  private bootstrapPrepared = false;
  private startupFiles = new Map<string, string>();
  private startupStructured = new Map<string, StructuredMeta>();
  /** Last time a text document synced (ms epoch); gates background blob uploads. */
  private lastTextActivityAt = 0;
  /** Tracks the live connection so we only notify on a connected→dropped edge. */
  private wasConnected = false;

  private filesObserver: (event: Y.YMapEvent<string>) => void;
  /** Attachments embedded in open notes, currently prioritized for download. */
  private noteAttachmentPaths = new Set<string>();
  /** Attachments currently prioritized by open Canvas views. */
  private canvasAttachmentPaths = new Set<string>();
  private noteAttachmentTimer: number | null = null;
  private noteAttachmentGeneration = 0;
  /** An open note embeds an attachment the binary index does not list yet. */
  private noteAttachmentsUnresolved = false;
  private binaryBaselineSeeded = false;
  private readonly binariesObserver = (): void => {
    if (this.noteAttachmentsUnresolved) this.prioritizeOpenNoteAttachments();
  };
  private structuredObserver: (event: Y.YMapEvent<StructuredMeta>) => void;
  private statusListener: (status: SyncStatus) => void;
  private vaultEvents: EventRef[] = [];
  private docQueue = new Map<string, QueueItem>();
  private activeDocConnections = 0;
  private docConnectionGeneration = 0;
  private highPriorityDrained = false;
  private backgroundSyncStarted = false;
  private prioritizedPaths = new Set<string>();
  private prioritizedGuids = new Set<string>();
  /** Monotonic per-path version used to abort stale async create/rename work. */
  private pathVersions = new Map<string, number>();
  /** Net effect of the vault events captured before the initial sync. */
  private bootstrapLog = new BootstrapEventLog();
  /** Paths this plugin is writing; their vault events are not user edits. */
  private ownWrites = new Map<string, number>();
  private remoteDeleteInFlight = new Set<string>();
  private remoteDeletePending = new Map<
    string,
    { kind: "text" | StructuredKind; deletedIdentity: string | null }
  >();
  private remoteDeletePreserved = new Set<string>();
  private remoteDeletesApplying = new Set<string>();
  /** Old path → new path for remote renames being applied to the vault. */
  private remoteRenamesApplying = new Map<string, string>();
  /** Last explicit user/local activity per path; used for mobile LRU eviction. */
  private mobileLastUsedAt = new Map<string, number>();
  private mobileTrimTimer: number | null = null;
  private mobileTrimRunning = false;
  private mobileCatchUpPending = false;
  private mobileResumeInProgress = false;
  /** Evicted documents recently reloaded because of an invalidation. */
  private invalidationReloads = new Set<string>();
  private invalidationReloadPending = new Set<string>();
  private invalidationListener: (documentId: string) => void;

  constructor(plugin: RealtimePlugin) {
    this.plugin = plugin;
    this.mobileSuspended = Platform?.isMobile === true && document.visibilityState === "hidden";

    this.indexDoc = new Y.Doc();
    this.files = this.indexDoc.getMap("files");
    this.structured = this.indexDoc.getMap("structured");
    this.trash = this.indexDoc.getMap("trash");

    const vaultId = plugin.settings.activeVaultId;
    const serverScope = plugin.settings.authServerId || plugin.settings.authServerUrl;
    const localScope = `${serverScope}:${vaultId}`;
    this.localSyncState = new LocalSyncState(localScope);
    this.binarySync = new BinarySync(
      plugin,
      this,
      this.indexDoc,
      this.localSyncState,
      (path) =>
        this.structuredKindForPath(path) !== null ||
        shouldSyncCanvasBinaryPath(
          path,
          this.plugin.settings.syncBinaries,
          this.plugin.settings.binaryExcludeGlobs,
        ),
    );
    if (this.mobileSuspended) this.binarySync.setPaused(true);
    this.configSync = new ConfigSync(plugin, this.indexDoc, this.localSyncState);
    if (this.mobileSuspended) this.configSync.setPaused(true);

    // Connect only after the persisted index has loaded (see init()), so local
    // offline map changes merge with the server instead of racing it. The index
    // doc keeps the bare vault id; file docs are namespaced as `${vaultId}__${guid}`.
    // Pin the epoch: an index epoch change restarts sync with a fresh store.
    const indexEpoch = getDocumentEpoch(plugin, vaultId);
    this.indexProvider = new RealtimeProvider(
      vaultId,
      this.indexDoc,
      () => getClientToken(plugin, vaultId, undefined, indexEpoch),
      { connect: false, socketFactory: createMuxSocket },
    );
    // Nobody views the index: publishing presence on it is pure overhead.
    this.indexProvider.awareness.setLocalState(null);
    this.indexPersistence = new IndexeddbPersistence(
      epochPersistenceName(plugin, vaultId, `realtime:index:${localScope}`, indexEpoch),
      this.indexDoc,
    );
    if (indexEpoch > 0) {
      // A retired epoch's index store is never read again.
      void clearDocument(
        epochPersistenceName(plugin, vaultId, `realtime:index:${localScope}`, indexEpoch - 1),
      ).catch(() => {});
    }

    this.filesObserver = this.onFilesChanged.bind(this);
    this.files.observe(this.filesObserver);
    this.indexDoc.getMap("binaries").observe(this.binariesObserver);
    this.structuredObserver = this.onStructuredChanged.bind(this);
    this.structured.observe(this.structuredObserver);

    this.statusListener = this.handleIndexStatus.bind(this);
    this.indexProvider.on(SYNC_EVENT_STATUS, this.statusListener);
    this.invalidationListener = (documentId) => this.onDocumentInvalidated(documentId);
    this.indexProvider.on(SYNC_EVENT_DOCUMENT_INVALIDATED, this.invalidationListener);

    this.registerVaultEvents();
    void this.init();
  }

  private handleIndexStatus(status: SyncStatus): void {
    if (this.mobileSuspended) {
      if (status === SYNC_STATUS_OFFLINE) {
        this.wasConnected = false;
        this.plugin.setStatus("offline");
      }
      return;
    }
    if (status === SYNC_STATUS_CONNECTED) {
      this.wasConnected = true;
      this.plugin.setStatus("connected");
      void this.runInitialSync();
      this.binarySync.retryNow();
      if (Platform?.isMobile && this.mobileCatchUpPending && !this.mobileResumeInProgress) {
        this.mobileCatchUpPending = false;
        this.binarySync.setPaused(false);
        this.configSync.setPaused(false);
        this.queueMobileReconnects();
      }
    } else if (status === "connecting" || status === "handshaking") {
      this.notifyDisconnected();
      this.plugin.setStatus("connecting");
    } else if (status === "error") {
      if (Platform?.isMobile && this.initialSynced) this.mobileCatchUpPending = true;
      this.notifyDisconnected();
      this.plugin.setStatus("error");
    } else {
      if (Platform?.isMobile && this.initialSynced) this.mobileCatchUpPending = true;
      this.notifyDisconnected();
    }
  }

  /** Load the persisted index, then connect so local offline changes sync. */
  private async init(): Promise<void> {
    try {
      await Promise.all([this.indexPersistence.whenSynced, this.localSyncState.whenSynced]);
    } catch (e) {
      console.error("[Realtime] index persistence failed to load", e);
    }
    if (this.destroyed) return;
    this.migrateLegacyLocalState();
    this.startupFiles = new Map(this.files.entries());
    this.startupStructured = new Map(
      [...this.structured.entries()].filter((entry): entry is [string, StructuredMeta] =>
        isStructuredMeta(entry[1]),
      ),
    );
    // Capture the persisted (pre-remote-merge) binary baseline before connecting.
    this.binarySync.seedBaseline();
    this.configSync.seedBaseline();
    this.binaryBaselineSeeded = true;
    this.prioritizeOpenNoteAttachments();
    if (!this.mobileSuspended) void this.connectIndexAfterCompatibilityCheck();
  }

  private async connectIndexAfterCompatibilityCheck(): Promise<void> {
    try {
      await this.plugin.auth.serverInfoChecked(this.plugin.settings.authServerUrl);
      if (this.destroyed || this.mobileSuspended) return;
      this.scheduleMobileWorkingSetTrim();
      await this.indexProvider.connect();
    } catch {
      if (this.destroyed) return;
      // Local documents remain available offline, but no provider may open
      // until this server has proved the mandatory capability contract.
      this.indexProvider.disconnect();
      this.plugin.setStatus("offline");
    }
  }

  private migrateLegacyLocalState(): void {
    for (const [path, guid] of this.files.entries()) {
      this.localSyncState.migrateLegacyIdentity(path, "text", guid);
    }
    for (const [path, meta] of this.structured.entries()) {
      if (isStructuredMeta(meta)) {
        this.localSyncState.migrateLegacyIdentity(path, meta.kind, meta.guid);
      }
    }
    const binaries = this.indexDoc.getMap<{ hash?: string }>("binaries");
    for (const [path, meta] of binaries.entries()) {
      if (meta?.hash) this.localSyncState.migrateLegacyIdentity(path, "binary", meta.hash);
    }
    if (this.plugin.settings.syncConfigEnabled) {
      const categories = enabledConfigCategories(this.plugin.settings.configSyncCategories);
      const configFiles = this.indexDoc.getMap<{ hash?: string }>("configFiles");
      for (const [path, meta] of configFiles.entries()) {
        const prefix = `${this.plugin.app.vault.configDir}/`;
        const category = path.startsWith(prefix)
          ? categoryForConfigPath(path.slice(prefix.length))
          : null;
        if (meta?.hash && category !== null && categories.has(category)) {
          this.localSyncState.migrateLegacyIdentity(path, "config", meta.hash);
        }
      }
    }
  }

  /**
   * Mark a vault write the plugin itself is about to make at `path`, so the
   * resulting vault event is not taken for a user edit. Call the returned
   * function once the write settles.
   */
  beginOwnWrite(path: string): () => void {
    this.ownWrites.set(path, (this.ownWrites.get(path) ?? 0) + 1);
    let released = false;
    return () => {
      if (released) return;
      released = true;
      // Release on the next tick so the write's own vault event is still ours.
      window.setTimeout(() => {
        const count = (this.ownWrites.get(path) ?? 1) - 1;
        if (count > 0) this.ownWrites.set(path, count);
        else this.ownWrites.delete(path);
      }, 0);
    };
  }

  private isOwnWrite(path: string): boolean {
    return (
      this.ownWrites.has(path) ||
      this.remoteDeletesApplying.has(path) ||
      this.binarySync.isWriting(path)
    );
  }

  /**
   * The user deleted or moved away the file at `path` before the initial
   * sync finished: restoring it from the index must wait for that change to
   * be published instead of undoing it.
   */
  isPendingLocalRemoval(path: string): boolean {
    return this.bootstrapLog.removed(path);
  }

  noteMaterialized(path: string, kind: MaterializedKind, identity?: string): void {
    if (identity) this.localSyncState.commit(path, kind, identity);
    else this.localSyncState.mark(path, kind);
  }

  noteContentAcknowledged(
    path: string,
    kind: MaterializedKind,
    identity: string,
    fingerprint: string,
    reconciled = false,
  ): void {
    this.localSyncState.markSynced(path, kind, identity, fingerprint, reconciled);
  }

  /** Record disk content the document for `identity` durably contains. */
  noteDiskContent(
    path: string,
    kind: MaterializedKind,
    identity: string,
    fingerprint: string,
  ): void {
    if (this.destroyed) return;
    this.localSyncState.markDisk(path, kind, identity, fingerprint);
  }

  /** Fingerprint of the content the server last acknowledged for `identity` at `path`. */
  acknowledgedFingerprint(path: string, identity: string): string | null {
    if (this.destroyed) return null;
    return this.localSyncState.acknowledgedFingerprint(path, identity);
  }

  /** Fingerprint of disk content the document for `identity` last contained. */
  async diskFingerprint(path: string, identity: string): Promise<string | null> {
    await this.localSyncState.whenSynced;
    if (this.destroyed) return null;
    return this.localSyncState.diskFingerprint(path, identity);
  }

  /** Nudge the index provider and every document to reconnect if stalled. */
  reconnectAll(): void {
    if (this.destroyed || this.mobileSuspended) return;
    const status = this.indexProvider.status;
    if (Platform?.isMobile && status !== SYNC_STATUS_CONNECTED) {
      if (status === SYNC_STATUS_OFFLINE || status === SYNC_STATUS_ERROR) {
        if (this.initialSynced) this.mobileCatchUpPending = true;
        void this.connectIndexAfterCompatibilityCheck();
      }
      return;
    }
    if (status === SYNC_STATUS_OFFLINE || status === SYNC_STATUS_ERROR) {
      void this.connectIndexAfterCompatibilityCheck();
    }
    if (Platform?.isMobile) return;
    for (const doc of this.documents.values()) doc.ensureConnected();
    for (const doc of this.structuredDocuments.values()) doc.ensureConnected();
    this.pumpDocQueue();
  }

  /**
   * Release mobile network channels while Obsidian is backgrounded. Y.Docs and
   * IndexedDB persistence stay alive, so local edits remain durable and merge
   * after the next foreground handshake.
   */
  suspendForBackground(): void {
    if (!Platform?.isMobile || this.destroyed || this.mobileSuspended) return;
    this.mobileSuspended = true;
    this.wasConnected = false;
    this.docConnectionGeneration++;
    this.activeDocConnections = 0;
    this.binarySync.setPaused(true);
    this.configSync.setPaused(true);
    for (const doc of this.documents.values()) doc.disconnect();
    for (const doc of this.structuredDocuments.values()) doc.disconnect();
    this.indexProvider.disconnect();
    this.plugin.setStatus("offline");
    this.scheduleMobileWorkingSetTrim();
  }

  /** Resume mobile sync with active/open/recent documents ahead of the backlog. */
  async resumeFromBackground(): Promise<void> {
    if (this.destroyed) return;
    if (!Platform?.isMobile) {
      this.reconnectAll();
      return;
    }
    this.mobileSuspended = false;
    const generation = this.docConnectionGeneration;
    this.mobileResumeInProgress = true;
    try {
      if (this.indexProvider.status !== SYNC_STATUS_CONNECTED) {
        await this.connectIndexAfterCompatibilityCheck();
      }
    } finally {
      this.mobileResumeInProgress = false;
    }
    if (
      this.destroyed ||
      this.mobileSuspended ||
      generation !== this.docConnectionGeneration ||
      this.indexProvider.status !== SYNC_STATUS_CONNECTED
    ) {
      if (!this.destroyed && !this.mobileSuspended && generation === this.docConnectionGeneration) {
        this.mobileCatchUpPending = true;
      }
      return;
    }
    this.binarySync.setPaused(false);
    this.configSync.setPaused(false);
    this.mobileCatchUpPending = false;
    this.queueMobileReconnects();
  }

  prioritizeItem(opts: { path?: string; guid?: string }): void {
    const path = opts.path?.trim() || (opts.guid ? this.pathForGuid(opts.guid) : null);
    if (opts.guid) this.prioritizedGuids.add(opts.guid);
    if (!path) return;
    this.markMobileDocumentUsed(path);
    this.enqueueKnownPath(path, true, Platform?.isMobile === true);
    this.pumpDocQueue();
  }

  /**
   * A file document moved to a new epoch: its resident Y.Doc and IndexedDB
   * store hold retired-epoch state and must not reconnect. Replace just that
   * document; every other document keeps syncing undisturbed.
   */
  rebuildDocumentForEpoch(documentId: string): void {
    if (this.destroyed) return;
    const prefix = `${this.plugin.settings.activeVaultId}__`;
    if (!documentId.startsWith(prefix)) return;
    const guid = documentId.slice(prefix.length);
    for (const [path, doc] of [...this.documents]) {
      if (doc.guid !== guid) continue;
      const retiredBaseline = doc.retiredBaseline();
      this.removeDocument(path, { clearLocalStore: true });
      this.enqueueDoc({ path, guid, kind: "text", retiredBaseline }, true);
    }
    for (const [path, doc] of [...this.structuredDocuments]) {
      if (doc.guid !== guid) continue;
      const kind: StructuredKind = doc instanceof CanvasDocument ? "canvas" : "base";
      const retiredBaseline = doc.retiredBaseline();
      this.removeStructuredDocument(path, { clearLocalStore: true });
      this.enqueueDoc({ path, guid, kind, retiredBaseline }, true);
    }
    this.pumpDocQueue();
  }

  private onDocumentInvalidated(documentId: string): void {
    if (!Platform?.isMobile || this.destroyed) return;
    const prefix = `${this.plugin.settings.activeVaultId}__`;
    if (!documentId.startsWith(prefix)) return;
    const guid = documentId.slice(prefix.length);
    if (!guid) return;
    const path = this.pathForGuid(guid);
    if (!path) return;
    // The server sends one invalidation per remote write. Count that activity
    // as use, so a document being edited elsewhere stays resident (and live)
    // instead of being evicted right after each reload and reloaded again.
    this.mobileLastUsedAt.set(path, Date.now());
    const resident = this.documents.has(path) || this.structuredDocuments.has(path);
    if (!resident) {
      // Reload an evicted document at most once per window; a later write in
      // the window is picked up by one trailing reload.
      if (this.invalidationReloads.has(path)) {
        this.invalidationReloadPending.add(path);
        return;
      }
      this.invalidationReloads.add(path);
      window.setTimeout(() => {
        this.invalidationReloads.delete(path);
        if (this.invalidationReloadPending.delete(path) && !this.destroyed) {
          this.onDocumentInvalidated(documentId);
        }
      }, INVALIDATION_RELOAD_WINDOW_MS);
    }
    this.enqueueKnownPath(path, true, true);
    this.pumpDocQueue();
  }

  prioritizeCanvasAttachments(paths: Iterable<string>): void {
    const added = [...paths];
    for (const path of added) this.canvasAttachmentPaths.add(path);
    this.binarySync.prioritizePaths(added);
  }

  unprioritizeCanvasAttachments(paths: Iterable<string>): void {
    const removed = [...paths];
    for (const path of removed) this.canvasAttachmentPaths.delete(path);
    this.binarySync.unprioritizePaths(
      removed.filter((path) => !this.noteAttachmentPaths.has(path)),
    );
  }

  /**
   * Download the attachments embedded in open notes ahead of the startup
   * binary pass, which otherwise waits for every prioritized note (on a fresh
   * device, all of them) and leaves the open note's images broken meanwhile.
   * Debounced; safe to call on every workspace or open-note change.
   */
  prioritizeOpenNoteAttachments(): void {
    if (this.destroyed || this.noteAttachmentTimer !== null) return;
    this.noteAttachmentTimer = window.setTimeout(() => {
      this.noteAttachmentTimer = null;
      void this.refreshNoteAttachments().catch((error) => {
        console.error("[Realtime] failed to prioritize note attachments", error);
      });
    }, 150);
  }

  private openMarkdownPaths(): Set<string> {
    const paths = new Set<string>();
    const add = (file: TFile | null | undefined) => {
      if (file?.extension === "md") paths.add(file.path);
    };
    add((this.plugin.app.workspace as any).getActiveFile?.());
    (this.plugin.app.workspace as any).iterateAllLeaves?.((leaf: any) => add(leaf?.view?.file));
    return paths;
  }

  private async refreshNoteAttachments(): Promise<void> {
    if (this.destroyed || !this.binaryBaselineSeeded) return;
    const generation = ++this.noteAttachmentGeneration;
    const candidates = this.canvasBinaryPaths(this.binarySync.remotePaths());
    const next = new Set<string>();
    let unresolved = false;
    for (const notePath of this.openMarkdownPaths()) {
      const resident = this.documents.get(notePath);
      const file = this.plugin.app.vault.getAbstractFileByPath(notePath);
      const text = resident
        ? resident.content
        : file instanceof TFile
          ? await this.plugin.app.vault.read(file)
          : "";
      if (this.destroyed || generation !== this.noteAttachmentGeneration) return;
      for (const link of embeddedLinkpaths(text)) {
        const target = resolveAttachmentLink(link, notePath, candidates);
        if (target) {
          next.add(target);
        } else if (
          !/\.md$/i.test(link) &&
          /\.[a-z0-9]+$/i.test(link) &&
          !this.plugin.app.metadataCache?.getFirstLinkpathDest?.(link, notePath)
        ) {
          // Likely an attachment whose index entry has not arrived yet.
          unresolved = true;
        }
      }
    }
    this.noteAttachmentsUnresolved = unresolved;
    const added = [...next].filter((path) => !this.noteAttachmentPaths.has(path));
    const removed = [...this.noteAttachmentPaths].filter(
      (path) => !next.has(path) && !this.canvasAttachmentPaths.has(path),
    );
    this.noteAttachmentPaths = next;
    this.binarySync.unprioritizePaths(removed);
    this.binarySync.prioritizePaths(added);
  }

  canvasBinaryPaths(paths: Iterable<string>): string[] {
    const binaryPaths: string[] = [];
    for (const path of paths) {
      if (
        this.isConfigPath(path) ||
        !shouldSyncCanvasBinaryPath(
          path,
          this.plugin.settings.syncBinaries,
          this.plugin.settings.binaryExcludeGlobs,
        )
      ) {
        continue;
      }
      const file = this.plugin.app.vault.getAbstractFileByPath(path);
      if ((file && this.classify(file) === "binary") || (!file && this.binarySync.hasPath(path))) {
        binaryPaths.push(path);
      }
    }
    return binaryPaths;
  }

  pathForGuid(guid: string): string | null {
    for (const [path, value] of this.files.entries()) {
      if (value === guid) return path;
    }
    for (const [path, meta] of this.structured.entries()) {
      if (!isStructuredMeta(meta)) continue;
      if (meta.guid === guid) return path;
    }
    return null;
  }

  /**
   * Wait until a vault item (located by `path` and/or `guid`) is resolvable on
   * disk as a {@link TFile}, or until `timeoutMs` (default 15s) elapses.
   *
   * Re-resolves the path on every check so a guid that the index only merges
   * after the wait starts is still picked up. Both conditions must hold: the
   * shared index must map the guid → path (or a path was given), AND the file
   * must exist on disk (the index entry alone is insufficient — the Document
   * pipeline writes the file via {@link Document#writeToDisk} asynchronously).
   * This method never creates documents or triggers sync; it only re-checks.
   *
   * Responsive without polling latency: a ~250ms interval is the backstop, and
   * observers on `files`/`structured` re-check immediately on any
   * remote merge. Tolerates the IndexedDB-loading startup phase — it does not
   * depend on `reconnectAll`'s timing, just on the index merge + disk write
   * landing. Resolves `null` on timeout or if this sync is destroyed mid-wait.
   */
  async waitForItem(opts: {
    guid?: string;
    path?: string;
    timeoutMs?: number;
  }): Promise<TFile | null> {
    const timeoutMs = opts.timeoutMs ?? 15_000;
    const deadline = Date.now() + timeoutMs;

    return new Promise<TFile | null>((resolve) => {
      let settled = false;
      let attached = false;
      let timer: ReturnType<typeof setInterval> | null = null;
      let timeoutTimer: ReturnType<typeof setTimeout> | null = null;

      const filesObserver = (): void => check();
      const structuredObserver = (): void => check();

      // Single, idempotent cleanup/exit path. Safe to call from every exit
      // (immediate success, observer hit, timer tick, timeout, destroy).
      const finish = (result: TFile | null): void => {
        if (settled) return;
        settled = true;
        if (timer !== null) clearInterval(timer);
        if (timeoutTimer !== null) clearTimeout(timeoutTimer);
        // Only unobserve what we actually attached; unobserving a handler that
        // was never registered logs a yjs console.error.
        if (attached) {
          this.files.unobserve(filesObserver);
          this.structured.unobserve(structuredObserver);
        }
        resolve(result);
      };

      const check = (): void => {
        if (this.destroyed) {
          finish(null);
          return;
        }
        // Re-resolve every tick: a guid may map to a path only after the wait
        // starts (index merge lands mid-wait). An explicit path takes precedence.
        const path = opts.path?.trim() || (opts.guid ? this.pathForGuid(opts.guid) : null);
        if (!path) return; // not ready yet — keep waiting (don't stop at "guid unknown")
        const file = this.plugin.app.vault.getAbstractFileByPath(path);
        if (file instanceof TFile) finish(file);
        // Otherwise keep waiting: the index entry alone is insufficient.
      };

      // Synchronous happy path before wiring up observers/timer (no cleanup to
      // do if this settles immediately).
      check();
      if (settled) return;

      // Re-check immediately on any remote index merge (responsive without
      // waiting for the next poll tick).
      this.files.observe(filesObserver);
      this.structured.observe(structuredObserver);
      attached = true;

      // Poll backstop covers the disk-write lag, destroy, and any miss the
      // observers don't see (e.g. the file landing on disk without an index
      // change because the index already had the entry).
      timer = setInterval(() => {
        check();
      }, 250);
      timeoutTimer = setTimeout(() => finish(null), Math.max(0, deadline - Date.now()));
    });
  }

  /** Notify once when a live connection drops; silent while already offline. */
  private notifyDisconnected(): void {
    if (!this.wasConnected) return;
    this.wasConnected = false;
    new Notice("Realtime: disconnected — reconnecting…");
  }

  // --- Index synchronisation -------------------------------------------------

  private async runInitialSync(): Promise<void> {
    if (this.initialSynced || this.initialSyncRunning || this.destroyed) return;
    this.initialSyncRunning = true;
    try {
      await this.runInitialSyncPass();
      if (!this.destroyed) {
        await this.runInitialSyncPass();
        await this.replayBootstrapVaultEvents();
        this.initialSynced = true;
        this.startBackgroundSyncAfterPriorityDrain();
      }
    } catch (error) {
      console.error("[Realtime] vault bootstrap failed; retrying", error);
      if (!this.destroyed) window.setTimeout(() => void this.runInitialSync(), 2_000);
    } finally {
      this.initialSyncRunning = false;
    }
  }

  private async runInitialSyncPass(): Promise<void> {
    const syncFiles = this.plugin.app.vault.getFiles().filter((file) => {
      const kind = this.classify(file);
      return kind === "text" || kind === "structured";
    });
    for (const file of syncFiles) {
      const kind = this.classify(file);
      if (kind === "text" && !this.localSyncState.has(file.path)) {
        const fingerprint = await sha256Text(await this.plugin.app.vault.read(file));
        this.beginUntrackedCandidate(file.path, "text", fingerprint);
      } else if (kind === "structured" && !this.localSyncState.has(file.path)) {
        const structuredKind = this.structuredKindForExtension(file.extension);
        if (structuredKind) {
          const fingerprint = await sha256Text(await this.plugin.app.vault.read(file));
          this.beginUntrackedCandidate(file.path, structuredKind, fingerprint);
        }
      }
    }
    this.bootstrapPrepared = true;

    // Index entries present in this device's last durable snapshot but absent
    // after the remote merge are remote deletions. Resolve them before treating
    // remaining local files as new candidates.
    for (const [path, guid] of this.startupFiles) {
      if (!this.files.has(path)) await this.handleRemoteDelete(path, "text", guid);
      if (this.destroyed) return;
    }
    for (const [path, meta] of this.startupStructured) {
      if (!this.structured.has(path)) {
        await this.handleRemoteDelete(path, meta.kind, meta.guid);
      }
      if (this.destroyed) return;
    }
    for (const file of syncFiles) {
      const state = this.localSyncState.get(file.path);
      if (!state || state.candidate || !state.identity) continue;
      const kind = this.classify(file);
      if (kind === "text" && !this.files.has(file.path)) {
        await this.handleRemoteDelete(file.path, "text", state.identity);
      } else if (kind === "structured" && !this.structured.has(file.path)) {
        const structuredKind = this.structuredKindForExtension(file.extension);
        if (structuredKind) {
          await this.handleRemoteDelete(file.path, structuredKind, state.identity);
        }
      }
      if (this.destroyed) return;
    }

    const priorityPaths = this.startupPriorityPaths();

    // Register entries that already exist in the shared index, but connect lazily.
    // A path the user removed during startup is left alone: materializing it
    // would undo the delete or rename the replay is about to publish.
    for (const [path, guid] of this.files.entries()) {
      if (this.bootstrapLog.removed(path)) continue;
      this.registerFile(path, guid);
      this.enqueueDoc(
        { path, guid, kind: "text" },
        priorityPaths.has(path) || !this.localFileExists(path),
      );
    }
    for (const [path, meta] of this.structured.entries()) {
      if (!isStructuredMeta(meta)) {
        console.warn(`[Realtime] ignoring malformed structured index entry for ${path}`);
        continue;
      }
      if (this.bootstrapLog.removed(path)) continue;
      this.registerFile(path, meta.guid);
      this.enqueueDoc(
        { path, guid: meta.guid, kind: meta.kind },
        priorityPaths.has(path) || !this.localFileExists(path),
      );
    }

    // Add any local text or structured files that aren't tracked yet.
    for (const file of syncFiles) {
      const path = file.path;
      if (!this.localFileExists(path)) continue;
      const pathVersion = this.currentPathVersion(path);
      if (isConflictCopy(path)) continue;
      // Renamed here from a tracked path during startup: the replay moves
      // that identity over; publishing it as new now would orphan it.
      const renamedFrom = this.bootstrapLog.renamedFrom(path);
      if (
        renamedFrom !== null &&
        (this.files.has(renamedFrom) || this.structured.has(renamedFrom))
      ) {
        continue;
      }
      const kind = this.classify(file);
      if (kind === "text") {
        if (!this.localSyncState.has(path)) {
          this.localSyncState.beginCandidate(path, "text", null);
        }
      } else if (kind === "structured") {
        const structuredKind = this.structuredKindForExtension(file.extension);
        if (structuredKind && !this.localSyncState.has(path)) {
          this.localSyncState.beginCandidate(path, structuredKind, null);
        }
      }
      if (kind === "structured") {
        const structuredKind = this.structuredKindForExtension(file.extension);
        if (!structuredKind) continue;
        if (!this.structured.has(path)) {
          const guid = this.localSyncState.candidateIdentity(path, structuredKind) ?? newGuid();
          this.localSyncState.beginCandidate(path, structuredKind, guid);
          const doc = this.ensureStructuredDocument(path, guid, structuredKind, true);
          await this.publishWhenReady(doc, () =>
            this.publishLocalStructured(path, pathVersion, doc, guid, structuredKind),
          );
          if (this.destroyed) return;
        } else {
          const meta = this.structured.get(path);
          if (!isStructuredMeta(meta)) continue;
          this.registerFile(path, meta.guid);
          this.enqueueDoc({ path, guid: meta.guid, kind: meta.kind }, priorityPaths.has(path));
        }
        continue;
      }
      if (!this.files.has(path)) {
        const guid = this.localSyncState.candidateIdentity(path, "text") ?? newGuid();
        this.localSyncState.beginCandidate(path, "text", guid);
        const doc = this.ensureDocument(path, guid, true);
        await this.publishWhenReady(doc, () => this.publishLocalText(path, pathVersion, doc, guid));
        if (this.destroyed) return;
      } else {
        this.registerFile(path, this.files.get(path)!);
        this.enqueueDoc(
          { path, guid: this.files.get(path)!, kind: "text" },
          priorityPaths.has(path),
        );
      }
    }

    this.pumpDocQueue();
    if (this.prioritizedPaths.size === 0) {
      this.highPriorityDrained = true;
      this.startBackgroundSyncAfterPriorityDrain();
    }

    new Notice(
      `Realtime: connected, syncing ${this.documents.size + this.structuredDocuments.size} files.`,
    );

    // Reconcile binary files against the blob store (after the text pass so
    // note sync wins the bandwidth while binaries settle in the background).
    this.startBackgroundSyncAfterPriorityDrain();
  }

  private startupPriorityPaths(): Set<string> {
    const paths = new Set<string>();
    const add = (file: TFile | null | undefined) => {
      if (file && (file.extension === "md" || this.structuredKindForExtension(file.extension))) {
        paths.add(file.path);
      }
    };
    add((this.plugin.app.workspace as any).getActiveFile?.());
    (this.plugin.app.workspace as any).iterateAllLeaves?.((leaf: any) => add(leaf?.view?.file));
    const recentPaths = Array.isArray(this.plugin.settings.recentPaths)
      ? this.plugin.settings.recentPaths
      : [];
    const recentLimit = Platform?.isMobile
      ? this.plugin.settings.mobileRecentResidentDocs
      : recentPaths.length;
    for (const path of recentPaths.slice(0, recentLimit)) paths.add(path);
    return paths;
  }

  private markMobileDocumentUsed(path: string): void {
    if (!Platform?.isMobile) return;
    this.mobileLastUsedAt.set(path, Date.now());
    this.scheduleMobileWorkingSetTrim(1_000);
  }

  private scheduleMobileWorkingSetTrim(delayMs = 0): void {
    if (
      !Platform?.isMobile ||
      !this.plugin.auth.supportsCapability("documentInvalidation") ||
      this.destroyed ||
      this.mobileTrimTimer !== null
    ) {
      return;
    }
    this.mobileTrimTimer = window.setTimeout(() => {
      this.mobileTrimTimer = null;
      void this.trimMobileWorkingSet();
    }, delayMs);
  }

  private async trimMobileWorkingSet(): Promise<void> {
    if (
      !Platform?.isMobile ||
      !this.plugin.auth.supportsCapability("documentInvalidation") ||
      this.destroyed
    ) {
      return;
    }
    if (this.mobileTrimRunning) {
      this.scheduleMobileWorkingSetTrim(1_000);
      return;
    }
    this.mobileTrimRunning = true;
    let retry = false;
    try {
      const retained = this.startupPriorityPaths();
      const maxResidents = this.plugin.settings.mobileMaxResidentDocs;
      const entries: Array<[string, Document | StructuredDocument]> = [
        ...this.documents.entries(),
        ...this.structuredDocuments.entries(),
      ];
      const retainedResidents = entries.reduce(
        (count, [path]) => count + (retained.has(path) ? 1 : 0),
        0,
      );
      const target = Math.max(maxResidents, retainedResidents);
      let residentCount = entries.length;
      const candidates = entries
        .filter(([path]) => !retained.has(path))
        .sort(
          ([left], [right]) =>
            (this.mobileLastUsedAt.get(left) ?? 0) - (this.mobileLastUsedAt.get(right) ?? 0),
        );

      for (const [path, doc] of candidates) {
        if (residentCount <= target) break;
        if (!this.documentMatchesIndex(path, doc)) continue;
        if (!(await doc.prepareForHibernation())) {
          retry = true;
          continue;
        }
        if (
          this.destroyed ||
          this.startupPriorityPaths().has(path) ||
          !this.documentMatchesIndex(path, doc)
        ) {
          continue;
        }
        if (this.documents.get(path) === doc) this.documents.delete(path);
        else if (this.structuredDocuments.get(path) === doc) {
          this.structuredDocuments.delete(path);
        } else {
          continue;
        }
        doc.destroy();
        residentCount--;
      }
      if (residentCount > target) retry = true;
    } finally {
      this.mobileTrimRunning = false;
    }
    if (retry) this.scheduleMobileWorkingSetTrim(1_000);
  }

  private documentMatchesIndex(path: string, doc: Document | StructuredDocument): boolean {
    if (doc instanceof Document) return this.files.get(path) === doc.guid;
    const meta = this.structured.get(path);
    return isStructuredMeta(meta) && meta.guid === doc.guid;
  }

  private enqueueKnownPath(path: string, front = false, reconnect = false): void {
    const guid = this.files.get(path);
    if (guid) {
      const item = { path, guid, kind: "text" } satisfies QueueItem;
      if (reconnect) this.enqueueReconnect(item, front);
      else this.enqueueDoc(item, front);
    }
    const meta = this.structured.get(path);
    if (isStructuredMeta(meta)) {
      const item = { path, guid: meta.guid, kind: meta.kind } satisfies QueueItem;
      if (reconnect) this.enqueueReconnect(item, front);
      else this.enqueueDoc(item, front);
    }
  }

  private enqueueReconnect(item: QueueItem, front = false): void {
    if (this.destroyed) return;
    const doc =
      item.kind === "text"
        ? this.documents.get(item.path)
        : this.structuredDocuments.get(item.path);
    if (!doc || doc.guid !== item.guid) {
      this.enqueueDoc(item, front);
      return;
    }
    if (!doc.isReady()) {
      if (front) this.prioritizedPaths.add(item.path);
      this.docQueue.delete(item.path);
      if (front) this.docQueue = new Map([[item.path, item], ...this.docQueue]);
      else this.docQueue.set(item.path, item);
      return;
    }
    const status = doc.provider.status;
    if (status !== SYNC_STATUS_OFFLINE && status !== SYNC_STATUS_ERROR) return;

    const queued = { ...item, reconnect: true };
    if (front) this.prioritizedPaths.add(item.path);
    this.docQueue.delete(item.path);
    if (front) this.docQueue = new Map([[item.path, queued], ...this.docQueue]);
    else this.docQueue.set(item.path, queued);
  }

  private queueMobileReconnects(): void {
    // Catch up only the resident working set. Walking the full index here
    // recreates every hibernated document and defeats mobileMaxResidentDocs.
    for (const [path, doc] of this.documents.entries()) {
      const guid = this.files.get(path);
      if (guid === doc.guid) this.enqueueReconnect({ path, guid, kind: "text" });
    }
    for (const [path, doc] of this.structuredDocuments.entries()) {
      const meta = this.structured.get(path);
      if (isStructuredMeta(meta) && meta.guid === doc.guid) {
        this.enqueueReconnect({ path, guid: meta.guid, kind: meta.kind });
      }
    }

    // Prepending reverses order, so walk priorities backwards to retain
    // active → open → recent ordering from startupPriorityPaths().
    const priorities = [...this.startupPriorityPaths()];
    for (let i = priorities.length - 1; i >= 0; i--) {
      this.enqueueKnownPath(priorities[i], true, true);
    }
    this.pumpDocQueue();
  }

  private enqueueDoc(item: QueueItem, front = false): void {
    if (this.destroyed) return;
    if (item.kind === "text") {
      const existing = this.documents.get(item.path);
      if (existing?.guid === item.guid) return;
      if (existing) this.removeDocument(item.path);
    } else {
      const existing = this.structuredDocuments.get(item.path);
      if (existing?.guid === item.guid) return;
      if (existing) this.removeStructuredDocument(item.path);
    }
    if (front) this.prioritizedPaths.add(item.path);
    if (front) {
      this.docQueue.delete(item.path);
      this.docQueue = new Map([[item.path, item], ...this.docQueue]);
    } else {
      this.docQueue.set(item.path, item);
    }
  }

  private pumpDocQueue(): void {
    if (this.destroyed || this.mobileSuspended) return;
    const concurrency = Platform?.isMobile ? 1 : 2;
    while (this.activeDocConnections < concurrency && this.docQueue.size > 0) {
      const [path, item] = this.docQueue.entries().next().value as [string, QueueItem];
      this.docQueue.delete(path);
      const doc =
        item.kind === "text"
          ? this.ensureDocument(item.path, item.guid, false, false, item.retiredBaseline)
          : this.ensureStructuredDocument(
              item.path,
              item.guid,
              item.kind,
              false,
              false,
              item.retiredBaseline,
            );
      this.activeDocConnections++;
      const generation = this.docConnectionGeneration;
      const completion =
        item.reconnect && doc.provider.status !== SYNC_STATUS_CONNECTED
          ? doc.whenNextServerSync()
          : doc.whenReady();
      doc.connect();
      void this.whenQueueSlotFree(doc, completion).then(() => {
        if (generation !== this.docConnectionGeneration) return;
        this.activeDocConnections = Math.max(0, this.activeDocConnections - 1);
        this.prioritizedPaths.delete(item.path);
        this.prioritizedGuids.delete(item.guid);
        if (!this.highPriorityDrained && this.prioritizedPaths.size === 0) {
          this.highPriorityDrained = true;
          this.startBackgroundSyncAfterPriorityDrain();
        }
        this.scheduleMobileWorkingSetTrim();
        this.pumpDocQueue();
      });
    }
  }

  /**
   * The queue throttles concurrent handshakes; it must not wait on a document
   * that cannot connect (it keeps retrying on its own) or on one that is
   * waiting for the user to resolve a startup conflict. With one or two
   * slots, either would otherwise stop every other document from syncing.
   */
  private whenQueueSlotFree(doc: SyncedDoc, completion: Promise<void>): Promise<void> {
    return new Promise((resolve) => {
      let done = false;
      const finish = () => {
        if (done) return;
        done = true;
        window.clearTimeout(timer);
        doc.provider.off(SYNC_EVENT_STATUS, onStatus);
        resolve();
      };
      const onStatus = (status: SyncStatus) => {
        if (status === SYNC_STATUS_ERROR) finish();
      };
      const timer = window.setTimeout(finish, QUEUE_SLOT_TIMEOUT_MS);
      doc.provider.on(SYNC_EVENT_STATUS, onStatus);
      void completion.then(finish, finish);
    });
  }

  private startBackgroundSyncAfterPriorityDrain(): void {
    if (!this.initialSynced || this.destroyed || !this.highPriorityDrained) return;
    if (this.backgroundSyncStarted) return;
    this.backgroundSyncStarted = true;
    void this.reconcileBinariesAndMigrateStructured();
    if (this.plugin.settings.syncConfigEnabled) {
      this.configSync.start(enabledConfigCategories(this.plugin.settings.configSyncCategories));
    }
  }

  private async reconcileBinariesAndMigrateStructured(): Promise<void> {
    // Config files used to be routed through BinarySync. They are invisible to
    // Obsidian's indexed Vault API, so BinarySync would repeatedly call
    // createBinary for an adapter file that already exists. ConfigSync owns the
    // whole config directory now; remove legacy binary entries before any blob
    // reconciliation can try to materialize them.
    const legacyBinaries = this.indexDoc.getMap<BinaryMeta>("binaries");
    const configFiles = this.indexDoc.getMap<ConfigMeta>("configFiles");
    const configPrefix = `${this.plugin.app.vault.configDir.replace(/\/+$/, "")}/`;
    for (const path of this.binarySync.remotePaths()) {
      if (!this.isConfigPath(path)) continue;
      const legacy = legacyBinaries.get(path);
      const relativePath = path.slice(configPrefix.length);
      const isRealtimePluginFile =
        relativePath === "plugins/realtime" || relativePath.startsWith("plugins/realtime/");
      if (
        legacy?.hash &&
        !isRealtimePluginFile &&
        categoryForConfigPath(relativePath) !== null &&
        !configFiles.has(path)
      ) {
        // Keep the existing blob reachable during the ownership cutover.
        // Legacy binary metadata had no mtime; zero makes any later local
        // conflict newer without changing fresh-device remote-first behavior.
        configFiles.set(path, {
          hash: legacy.hash,
          size: legacy.size,
          mtime: 0,
        });
      }
      this.binarySync.stopTrackingPath(path);
      this.localSyncState.remove(path);
    }

    await this.binarySync.reconcileAll(this.localBinaryPaths());
    if (this.destroyed) return;

    for (const path of this.binarySync.remotePaths()) {
      const structuredKind = this.structuredKindForPath(path);
      if (!structuredKind || this.structured.has(path)) continue;

      // Legacy sync stored canvases/bases as binary blobs. Pull the blob first,
      // then promote the path into the structured CRDT index for live sync.
      await this.binarySync.reconcilePath(path);
      if (this.destroyed) return;

      const localFile = this.plugin.app.vault.getAbstractFileByPath(path);
      if (!(localFile instanceof TFile)) continue;

      const guid = newGuid();
      this.indexDoc.transact(() => {
        this.structured.set(path, { guid, kind: structuredKind });
      });
      this.registerFile(path, guid);
      this.ensureStructuredDocument(path, guid, structuredKind, true);
      this.binarySync.stopTrackingPath(path);
    }
  }

  /** Vault-relative paths of all local files classified as binary. */
  private localBinaryPaths(): string[] {
    return this.plugin.app.vault
      .getFiles()
      .filter((f) => this.classify(f) === "binary")
      .map((f) => f.path);
  }

  private onFilesChanged(event: Y.YMapEvent<string>): void {
    if (this.destroyed || !this.bootstrapPrepared) return;
    event.changes.keys.forEach((change, path) => {
      if (change.action === "add" || change.action === "update") {
        const guid = this.files.get(path);
        if (guid && !this.bootstrapLog.removed(path)) {
          this.remoteDeletePreserved.delete(path);
          this.registerFile(path, guid);
          this.enqueueDoc(
            { path, guid, kind: "text" },
            this.prioritizedPaths.has(path) ||
              this.prioritizedGuids.has(guid) ||
              !this.localFileExists(path),
          );
          this.pumpDocQueue();
        }
      } else if (change.action === "delete") {
        void this.handleRemoteDelete(
          path,
          "text",
          typeof change.oldValue === "string" ? change.oldValue : null,
        );
      }
    });
  }

  private onStructuredChanged(event: Y.YMapEvent<StructuredMeta>): void {
    if (this.destroyed || !this.bootstrapPrepared) return;
    event.changes.keys.forEach((change, path) => {
      if (change.action === "add" || change.action === "update") {
        const meta = this.structured.get(path);
        if (isStructuredMeta(meta) && !this.bootstrapLog.removed(path)) {
          this.remoteDeletePreserved.delete(path);
          this.registerFile(path, meta.guid);
          this.enqueueDoc(
            { path, guid: meta.guid, kind: meta.kind },
            this.prioritizedPaths.has(path) ||
              this.prioritizedGuids.has(meta.guid) ||
              !this.localFileExists(path),
          );
          this.pumpDocQueue();
        }
      } else if (change.action === "delete") {
        const oldMeta = change.oldValue;
        void this.handleRemoteDelete(
          path,
          isStructuredMeta(oldMeta) ? oldMeta.kind : "canvas",
          isStructuredMeta(oldMeta) ? oldMeta.guid : null,
        );
      }
    });
  }

  private async handleRemoteDelete(
    path: string,
    kind: "text" | StructuredKind,
    deletedIdentity: string | null,
  ): Promise<void> {
    if (this.destroyed) return;
    if (this.remoteDeleteInFlight.has(path)) {
      this.remoteDeletePending.set(path, { kind, deletedIdentity });
      return;
    }
    this.remoteDeleteInFlight.add(path);
    let currentKind = kind;
    let currentIdentity = deletedIdentity;
    try {
      do {
        this.remoteDeletePending.delete(path);
        await this.reconcileRemoteDelete(path, currentKind, currentIdentity);
        const pending = this.remoteDeletePending.get(path);
        if (pending) {
          currentKind = pending.kind;
          currentIdentity = pending.deletedIdentity;
          continue;
        }
        const stillDeleted =
          currentKind === "text" ? !this.files.has(path) : !this.structured.has(path);
        if (!stillDeleted || this.destroyed) break;
        // A delete may have landed while reconciliation was in flight without
        // changing the coalesced payload. One final pass observes the current
        // index and disk state.
        break;
      } while (true);
    } finally {
      this.remoteDeleteInFlight.delete(path);
    }
  }

  private async reconcileRemoteDelete(
    path: string,
    kind: "text" | StructuredKind,
    deletedIdentity: string | null,
  ): Promise<void> {
    try {
      const wasReintroduced = () =>
        kind === "text" ? this.files.has(path) : this.structured.has(path);
      if (wasReintroduced()) return;
      const file = this.plugin.app.vault.getAbstractFileByPath(path);
      if (
        file instanceof TFile &&
        !this.remoteDeletePreserved.has(path) &&
        deletedIdentity &&
        (await this.applyRemoteRename(path, kind, deletedIdentity, wasReintroduced))
      ) {
        return;
      }
      if (file instanceof TFile && !this.remoteDeletePreserved.has(path)) {
        const textDocument = this.documents.get(path);
        const structuredDocument = this.structuredDocuments.get(path);
        const content =
          kind === "text" && textDocument
            ? textDocument.content
            : kind === "canvas" && structuredDocument instanceof CanvasDocument
              ? structuredDocument.canvasText()
              : kind === "base" && structuredDocument instanceof BaseDocument
                ? structuredDocument.baseData()
                : await this.plugin.app.vault.read(file);
        if (this.destroyed || wasReintroduced()) return;
        const fingerprint = await sha256Text(content);
        if (this.destroyed || wasReintroduced()) return;
        const acknowledged = deletedIdentity
          ? this.localSyncState.acknowledgedFingerprint(path, deletedIdentity)
          : null;
        if (fingerprint !== acknowledged) {
          const preservedPath = await preserveTextConflict(this.plugin, path, content, "local");
          if (this.destroyed || wasReintroduced()) return;
          this.remoteDeletePreserved.add(path);
          new Notice(
            `Realtime: "${path}" was deleted remotely; preserved your local version as "${preservedPath}".`,
          );
        }
      }

      if (this.destroyed || wasReintroduced()) return;
      const clearLocalStore = this.deletedStoreDisposable(path);
      this.removeDocument(path, { clearLocalStore });
      this.removeStructuredDocument(path, { clearLocalStore });
      const current = this.plugin.app.vault.getAbstractFileByPath(path);
      if (current instanceof TFile) {
        if (this.destroyed || wasReintroduced()) return;
        this.remoteDeletesApplying.add(path);
        try {
          await this.plugin.app.vault.delete(current);
        } finally {
          this.remoteDeletesApplying.delete(path);
        }
      }
      if (this.destroyed) return;
      if (wasReintroduced()) {
        this.rematerializeReintroducedPath(path);
        return;
      }
      this.localSyncState.remove(path);
      this.remoteDeletePreserved.delete(path);
    } catch (e) {
      console.error(`[Realtime] failed to apply remote delete for ${path}`, e);
      if (!this.destroyed) {
        window.setTimeout(() => void this.handleRemoteDelete(path, kind, deletedIdentity), 2_000);
      }
    }
  }

  /**
   * The index publishes a rename as delete(old) + set(new) with the same guid.
   * Applying that literally deletes the old file and materializes the new one
   * from the CRDT, so any local edits the server has not acknowledged (e.g.
   * made while sync was stopped) land in a conflict copy. When the guid now
   * lives at a path that does not exist locally, move the file instead: the
   * document at the new path then merges those edits against its baseline.
   */
  private async applyRemoteRename(
    from: string,
    kind: "text" | StructuredKind,
    guid: string,
    wasReintroduced: () => boolean,
  ): Promise<boolean> {
    const to = this.pathForGuid(guid);
    if (!to || to === from || this.localFileExists(to)) return false;
    const target = kind === "text" ? this.files.get(to) : this.structured.get(to);
    const targetKind = kind === "text" ? "text" : isStructuredMeta(target) ? target.kind : null;
    if (targetKind !== kind) return false;
    const pending = kind === "text" ? this.documents.get(to) : this.structuredDocuments.get(to);
    // A ready document at the target has already materialized its content.
    if (pending?.isReady()) return false;

    await ensureParentFolder(this.plugin.app, to);
    const current = this.plugin.app.vault.getAbstractFileByPath(from);
    if (
      this.destroyed ||
      wasReintroduced() ||
      this.localFileExists(to) ||
      !(current instanceof TFile) ||
      this.pathForGuid(guid) !== to
    ) {
      return false;
    }
    this.removeDocument(from);
    this.removeStructuredDocument(from);
    // A target document that already read its (then missing) disk file must
    // start over so it sees the moved content.
    if (pending) {
      if (kind === "text") this.removeDocument(to);
      else this.removeStructuredDocument(to);
    }
    this.bumpPathVersion(from);
    this.bumpPathVersion(to);
    this.localSyncState.move(from, to, kind);
    this.remoteRenamesApplying.set(from, to);
    try {
      await this.plugin.app.vault.rename(current, to);
    } catch (error) {
      this.remoteRenamesApplying.delete(from);
      this.localSyncState.move(to, from, kind);
      throw error;
    }
    // Obsidian reports the rename synchronously; the timeout only bounds a
    // late event so it can never suppress a later user rename.
    window.setTimeout(() => {
      if (this.remoteRenamesApplying.get(from) === to) this.remoteRenamesApplying.delete(from);
    }, 1_000);
    if (this.destroyed) return true;
    this.enqueueDoc({ path: to, guid, kind }, true);
    this.pumpDocQueue();
    return true;
  }

  private rematerializeReintroducedPath(path: string): void {
    const guid = this.files.get(path);
    if (guid) {
      this.removeDocument(path);
      this.enqueueDoc({ path, guid, kind: "text" });
      return;
    }
    const meta = this.structured.get(path);
    if (!isStructuredMeta(meta)) return;
    this.removeStructuredDocument(path);
    const doc = this.ensureStructuredDocument(path, meta.guid, meta.kind, false);
    void doc.whenReady();
  }

  // --- Document registry -----------------------------------------------------

  private ensureDocument(
    path: string,
    guid: string,
    isCreator: boolean,
    autoConnect = true,
    retiredBaseline: string | null = null,
  ): Document {
    const existing = this.documents.get(path);
    if (existing && existing.guid === guid) return existing;
    if (existing) this.removeDocument(path);

    const serverDocId = `${this.plugin.settings.activeVaultId}__${guid}`;
    const doc = new Document(this.plugin, path, guid, serverDocId, isCreator, {
      ...this.bootstrapOptions(path, guid, autoConnect),
      retiredBaseline,
    });
    this.documents.set(path, doc);
    this.plugin.applyAwarenessTo(doc);
    this.scheduleMobileWorkingSetTrim(1_000);
    return doc;
  }

  private bootstrapOptions(
    path: string,
    guid: string,
    autoConnect: boolean,
  ): DocumentBootstrapOptions {
    const identityConflict =
      this.localFileExists(path) && this.localSyncState.hasIdentityConflict(path, guid);
    const state = identityConflict ? this.localSyncState.get(path) : null;
    // The previous identity was acknowledged and has left the index (deleted,
    // not moved): an unchanged local copy is safe to replace.
    const staleLocalFingerprint =
      state?.identity && !state.candidate && state.fingerprint && !this.pathForGuid(state.identity)
        ? state.fingerprint
        : null;
    return {
      autoConnect: autoConnect && !this.mobileSuspended,
      forceBootstrapConflict: identityConflict,
      staleLocalFingerprint,
    };
  }

  /**
   * Publish a new local file's index entry once its creator document is
   * ready. Waits at most QUEUE_SLOT_TIMEOUT_MS: a document that cannot
   * connect (it keeps retrying on its own, e.g. at the server's document
   * quota) must not hold up the bootstrap or every file after it. It then
   * publishes whenever it does become ready. `publish` must re-check that
   * publishing is still wanted; it may run long after the caller moved on.
   */
  private async publishWhenReady(doc: SyncedDoc, publish: () => void): Promise<void> {
    let timer = 0;
    const timedOut = new Promise<false>((resolve) => {
      timer = window.setTimeout(() => resolve(false), QUEUE_SLOT_TIMEOUT_MS);
    });
    const ready = doc.whenReady().then(() => true as const);
    const inTime = await Promise.race([ready, timedOut]);
    window.clearTimeout(timer);
    if (inTime) publish();
    else void ready.then(() => publish());
  }

  /** Best-effort: keep the server's guid → path registry current (for ACLs). */
  private registerFile(path: string, guid: string): void {
    void this.plugin.auth.registerFile(this.plugin.settings.activeVaultId, guid, path);
  }

  private removeDocument(path: string, options: { clearLocalStore?: boolean } = {}): void {
    const doc = this.documents.get(path);
    if (doc) {
      this.documents.delete(path);
      doc.destroy({ clearLocalStore: options.clearLocalStore && !this.hasResidentGuid(doc.guid) });
    }
    this.mobileLastUsedAt?.delete(path);
  }

  /** Whether another resident document (e.g. at a second path) uses `guid`'s store. */
  private hasResidentGuid(guid: string): boolean {
    return this.allDocuments().some((doc) => doc.guid === guid);
  }

  /**
   * The local store of a document whose file is being deleted is only worth
   * keeping while it holds changes the server never acknowledged (restoring
   * the file from the trash on this device would then upload them).
   */
  private deletedStoreDisposable(path: string): boolean {
    const doc = this.documents.get(path) ?? this.structuredDocuments.get(path);
    return doc !== undefined && !doc.provider.hasLocalChanges;
  }

  private ensureStructuredDocument(
    path: string,
    guid: string,
    kind: StructuredKind,
    isCreator: boolean,
    autoConnect = true,
    retiredBaseline: string | null = null,
  ): StructuredDocument {
    const existing = this.structuredDocuments.get(path);
    if (existing && existing.guid === guid) return existing;
    if (existing) this.removeStructuredDocument(path);

    const serverDocId = `${this.plugin.settings.activeVaultId}__${guid}`;
    const options = { ...this.bootstrapOptions(path, guid, autoConnect), retiredBaseline };
    const doc =
      kind === "canvas"
        ? new CanvasDocument(this.plugin, path, guid, serverDocId, isCreator, options)
        : new BaseDocument(this.plugin, path, guid, serverDocId, isCreator, options);
    this.structuredDocuments.set(path, doc);
    this.plugin.applyAwarenessTo(doc);
    this.scheduleMobileWorkingSetTrim(1_000);
    return doc;
  }

  private removeStructuredDocument(
    path: string,
    options: { clearLocalStore?: boolean } = {},
  ): void {
    const doc = this.structuredDocuments.get(path);
    if (doc) {
      this.structuredDocuments.delete(path);
      doc.destroy({ clearLocalStore: options.clearLocalStore && !this.hasResidentGuid(doc.guid) });
    }
    this.mobileLastUsedAt?.delete(path);
  }

  /** Record that a text document just synced (called by {@link Document}). */
  noteTextActivity(): void {
    this.lastTextActivityAt = Date.now();
  }

  /**
   * True while notes are actively syncing — the index isn't connected/synced yet,
   * or a text document synced within the last {@link quietMs}. The binary upload
   * queue uses this to hold large transfers back until things are quiet.
   */
  isTextSyncBusy(quietMs = 2000): boolean {
    if (!this.initialSynced) return true;
    return Date.now() - this.lastTextActivityAt < quietMs;
  }

  getDocumentForPath(path: string): Document | undefined {
    return this.documents.get(path);
  }

  ensureDocumentForPath(path: string): Document | undefined {
    this.markMobileDocumentUsed(path);
    const guid = this.files.get(path);
    if (!guid) return this.documents.get(path);
    const doc = this.ensureDocument(path, guid, false, false);
    if (!this.mobileSuspended) doc.connect();
    return doc;
  }

  allDocuments(): Array<Document | StructuredDocument> {
    return [...this.documents.values(), ...this.structuredDocuments.values()];
  }

  private currentPathVersion(path: string): number {
    return this.pathVersions.get(path) ?? 0;
  }

  private bumpPathVersion(path: string): number {
    const next = this.currentPathVersion(path) + 1;
    this.pathVersions.set(path, next);
    return next;
  }

  private localFileExists(path: string): boolean {
    return this.plugin.app.vault.getAbstractFileByPath(path) instanceof TFile;
  }

  private canPublishLocalPath(
    path: string,
    pathVersion: number,
    doc: Document | StructuredDocument,
  ): boolean {
    if (doc.isDestroyed()) return false;
    if (this.currentPathVersion(path) !== pathVersion) return false;
    if (!this.localFileExists(path)) return false;
    if (doc instanceof Document) {
      return this.documents.get(path) === doc && !this.files.has(path);
    }
    return this.structuredDocuments.get(path) === doc && !this.structured.has(path);
  }

  // Two passes: unbind every stale binding before binding new ones. Obsidian
  // reuses view/canvas instances across files in a leaf, so binding a new doc
  // while a stale sibling is still patched would wrap the stale patch and the
  // later unbind could clobber the fresh one.
  bindOpenCanvases(): void {
    for (const doc of this.structuredDocuments.values()) {
      if (doc instanceof CanvasDocument) doc.unbindStaleCanvas();
    }
    for (const doc of this.structuredDocuments.values()) {
      if (doc instanceof CanvasDocument) doc.tryBindLiveCanvas();
    }
  }

  bindOpenBases(): void {
    for (const doc of this.structuredDocuments.values()) {
      if (doc instanceof BaseDocument) doc.unbindStaleBase();
    }
    for (const doc of this.structuredDocuments.values()) {
      if (doc instanceof BaseDocument) doc.tryBindLiveBase();
    }
  }

  // --- Local vault events ----------------------------------------------------

  private registerVaultEvents(): void {
    const vault = this.plugin.app.vault;

    this.vaultEvents = [
      vault.on("create", (file) => this.onLocalCreate(file)),
      vault.on("delete", (file) => this.onLocalDelete(file)),
      vault.on("rename", (file, oldPath) => this.onLocalRename(file, oldPath)),
      vault.on("modify", (file) => this.onLocalModify(file)),
    ];
  }

  /**
   * Decide how a file syncs: `text` (Markdown, via a {@link Document} CRDT),
   * `binary` (everything else, via {@link BinarySync}'s blob store), or `ignore`
   * (folders, conflict copies, and — when binary sync is off or excluded — the
   * non-Markdown files).
   */
  private classify(file: TAbstractFile): FileKind {
    if (!(file instanceof TFile)) return "ignore";
    if (isConflictCopy(file.path)) return "ignore";
    if (this.isConfigPath(file.path)) return "ignore";
    if (file.extension === "md") return "text";
    if (file.extension === "canvas" && this.plugin.settings.syncCanvases) return "structured";
    if (file.extension === "base" && this.plugin.settings.syncBases) return "structured";
    if (!this.plugin.settings.syncBinaries) return "ignore";
    if (matchesAnyGlob(file.path, parseGlobs(this.plugin.settings.binaryExcludeGlobs))) {
      return "ignore";
    }
    return "binary";
  }

  private isConfigPath(path: string): boolean {
    const configDir = this.plugin.app.vault.configDir.replace(/\/+$/, "");
    return configDir.length > 0 && path.startsWith(`${configDir}/`);
  }

  private structuredKindForExtension(extension: string): StructuredKind | null {
    if (extension === "canvas") return "canvas";
    if (extension === "base") return "base";
    return null;
  }

  private structuredKindForPath(path: string): StructuredKind | null {
    const dot = path.lastIndexOf(".");
    if (dot < 0 || dot < path.lastIndexOf("/")) return null;
    const extension = path.slice(dot + 1);
    const kind = this.structuredKindForExtension(extension);
    if (kind === "canvas" && !this.plugin.settings.syncCanvases) return null;
    if (kind === "base" && !this.plugin.settings.syncBases) return null;
    return kind;
  }

  /**
   * Record a local file with no accepted identity yet. Callers check
   * `has(path)` before hashing, but the hash awaits a disk read: a document
   * materializing a remote file (whose vault.create echoes back here) commits
   * its identity meanwhile. Overwriting that with `null` would turn every later
   * remote change into a forced bootstrap conflict for the file.
   */
  private beginUntrackedCandidate(path: string, kind: MaterializedKind, fingerprint: string): void {
    if (this.localSyncState.has(path)) return;
    this.localSyncState.beginCandidate(path, kind, null, fingerprint);
  }

  private onLocalCreate(file: TAbstractFile): void {
    if (this.destroyed || this.isOwnWrite(file.path)) return;
    if (!this.initialSynced) {
      this.bumpPathVersion(file.path);
      this.bootstrapLog.create(file.path, file);
      return;
    }
    void this.handleLocalCreate(file);
  }

  private async handleLocalCreate(file: TAbstractFile): Promise<void> {
    if (this.destroyed || !this.initialSynced) return;
    const path = file.path;
    const pathVersion = this.bumpPathVersion(path);
    // Created and then removed again before this ran (e.g. within one
    // offline session): there is nothing left to publish.
    if (file instanceof TFile && !this.localFileExists(path)) return;
    const kind = this.classify(file);
    if (kind === "text" && file instanceof TFile) {
      if (!this.localSyncState.has(path)) {
        const fingerprint = await sha256Text(await this.plugin.app.vault.read(file));
        if (
          this.destroyed ||
          this.currentPathVersion(path) !== pathVersion ||
          !this.localFileExists(path)
        ) {
          return;
        }
        this.beginUntrackedCandidate(path, "text", fingerprint);
      }
    } else if (kind === "structured" && file instanceof TFile) {
      const structuredKind = this.structuredKindForExtension(file.extension);
      if (structuredKind && !this.localSyncState.has(path)) {
        const fingerprint = await sha256Text(await this.plugin.app.vault.read(file));
        if (
          this.destroyed ||
          this.currentPathVersion(path) !== pathVersion ||
          !this.localFileExists(path)
        ) {
          return;
        }
        this.beginUntrackedCandidate(path, structuredKind, fingerprint);
      }
    }
    if (kind === "binary") {
      this.binarySync.onLocalChanged(path);
      return;
    }
    if (kind === "structured" && file instanceof TFile) {
      const structuredKind = this.structuredKindForExtension(file.extension);
      if (!structuredKind) return;
      if (this.structured.has(path)) {
        const meta = this.structured.get(path);
        if (!isStructuredMeta(meta)) return;
        this.registerFile(path, meta.guid);
        this.ensureStructuredDocument(path, meta.guid, meta.kind, false);
        return;
      }
      const guid = this.localSyncState.candidateIdentity(path, structuredKind) ?? newGuid();
      this.localSyncState.beginCandidate(path, structuredKind, guid);
      const doc = this.ensureStructuredDocument(path, guid, structuredKind, true);
      await this.publishWhenReady(doc, () =>
        this.publishLocalStructured(path, pathVersion, doc, guid, structuredKind),
      );
      return;
    }
    if (kind !== "text") return;
    if (this.files.has(path)) {
      // Created locally because a remote entry arrived; Document handles it.
      this.registerFile(path, this.files.get(path)!);
      this.ensureDocument(path, this.files.get(path)!, false);
      return;
    }
    const guid = this.localSyncState.candidateIdentity(path, "text") ?? newGuid();
    this.localSyncState.beginCandidate(path, "text", guid);
    const doc = this.ensureDocument(path, guid, true);
    // Publish the index entry only after the creator's file doc has seeded and
    // synced, so peers do not materialize an empty remote-created note.
    await this.publishWhenReady(doc, () => this.publishLocalText(path, pathVersion, doc, guid));
  }

  /** Publish a new local note's index entry, unless something changed the path meanwhile. */
  private publishLocalText(path: string, pathVersion: number, doc: Document, guid: string): void {
    if (this.destroyed || !this.canPublishLocalPath(path, pathVersion, doc)) return;
    this.indexDoc.transact(() => {
      this.files.set(path, guid);
    });
    this.localSyncState.commit(path, "text", guid);
    this.registerFile(path, guid);
  }

  /** Publish a new local Canvas/Base's index entry (see {@link publishLocalText}). */
  private publishLocalStructured(
    path: string,
    pathVersion: number,
    doc: StructuredDocument,
    guid: string,
    kind: StructuredKind,
  ): void {
    if (this.destroyed || !this.canPublishLocalPath(path, pathVersion, doc)) return;
    this.indexDoc.transact(() => {
      this.structured.set(path, { guid, kind });
    });
    this.localSyncState.commit(path, kind, guid);
    this.registerFile(path, guid);
  }

  private onLocalDelete(file: TAbstractFile): void {
    if (this.destroyed || this.isOwnWrite(file.path)) return;
    if (!this.initialSynced) {
      this.bumpPathVersion(file.path);
      this.bootstrapLog.delete(file.path);
      return;
    }
    this.applyLocalDeletion(file.path);
  }

  /** Publish the user's deletion of the file at `path`. */
  private applyLocalDeletion(path: string): void {
    this.localSyncState.remove(path);
    this.bumpPathVersion(path);
    const clearLocalStore = this.deletedStoreDisposable(path);
    if (this.documents.has(path) || this.files.has(path)) {
      this.removeDocument(path, { clearLocalStore });
      if (this.files.has(path)) {
        const guid = this.files.get(path)!;
        this.indexDoc.transact(() => {
          this.recordTrashIn({ path, kind: "text", guid });
          this.files.delete(path);
        });
      }
      return;
    }
    if (this.structuredDocuments.has(path) || this.structured.has(path)) {
      this.removeStructuredDocument(path, { clearLocalStore });
      if (this.structured.has(path)) {
        const meta = this.structured.get(path);
        if (!isStructuredMeta(meta)) return;
        this.indexDoc.transact(() => {
          this.recordTrashIn({ path, kind: meta.kind, guid: meta.guid });
          this.structured.delete(path);
        });
      }
      return;
    }
    // Binary (or formerly-binary) file: let BinarySync propagate the delete.
    this.binarySync.onLocalDeleted(path);
  }

  private onLocalRename(file: TAbstractFile, oldPath: string): void {
    if (this.remoteRenamesApplying.get(oldPath) === file.path) {
      // Our own application of a remote rename; the index already has it.
      this.remoteRenamesApplying.delete(oldPath);
      return;
    }
    if (!this.initialSynced) {
      this.bumpPathVersion(oldPath);
      this.bumpPathVersion(file.path);
      this.bootstrapLog.rename(oldPath, file.path, file);
      return;
    }
    void this.handleLocalRename(file, oldPath);
  }

  private async handleLocalRename(file: TAbstractFile, oldPath: string): Promise<void> {
    if (this.destroyed || !this.initialSynced || !(file instanceof TFile)) return;
    const newPath = file.path;
    this.bumpPathVersion(oldPath);
    const newPathVersion = this.bumpPathVersion(newPath);

    // Drop local tracking of the old path; publish the index move atomically
    // below so peers never observe a transient delete with no replacement path.
    const wasTracked = this.files.has(oldPath);
    const guid = this.files.get(oldPath) ?? this.documents.get(oldPath)?.guid;
    const wasStructuredTracked = this.structured.has(oldPath);
    const oldStructuredMeta = this.structured.get(oldPath);
    const oldStructured = isStructuredMeta(oldStructuredMeta)
      ? oldStructuredMeta
      : this.structuredDocuments.get(oldPath);
    this.removeDocument(oldPath);
    this.removeStructuredDocument(oldPath);

    const kind = this.classify(file);
    const oldLocalState = this.localSyncState.get(oldPath);
    if (kind === "text") {
      this.localSyncState.move(oldPath, newPath, "text");
    } else if (kind === "structured") {
      const structuredKind = this.structuredKindForExtension(file.extension);
      if (structuredKind) this.localSyncState.move(oldPath, newPath, structuredKind);
      else this.localSyncState.remove(oldPath);
    } else if (kind === "binary") {
      // Keep a binary baseline on the old path until BinarySync publishes the
      // delete. The new path is an independent first-write candidate.
      if (oldLocalState?.kind !== "binary") this.localSyncState.remove(oldPath);
    } else {
      // A binary renamed out of the sync set still needs its old baseline if
      // the process stops before BinarySync publishes the delete.
      if (oldLocalState?.kind !== "binary") this.localSyncState.remove(oldPath);
    }
    if (kind === "text") {
      const finalGuid = guid ?? newGuid();
      const doc = this.ensureDocument(newPath, finalGuid, !wasTracked);
      const publish = () => {
        this.indexDoc.transact(() => {
          if (wasTracked) this.files.delete(oldPath);
          if (wasStructuredTracked) this.structured.delete(oldPath);
          this.files.set(newPath, finalGuid);
        });
        this.registerFile(newPath, finalGuid);
      };
      if (!wasTracked) {
        if (wasStructuredTracked) {
          this.indexDoc.transact(() => this.structured.delete(oldPath));
        }
        await this.publishWhenReady(doc, () => {
          if (!this.destroyed && this.canPublishLocalPath(newPath, newPathVersion, doc)) publish();
        });
        return;
      }
      publish();
    } else if (kind === "structured") {
      const structuredKind = this.structuredKindForExtension(file.extension);
      if (!structuredKind) {
        if (wasTracked || wasStructuredTracked) {
          this.indexDoc.transact(() => {
            if (wasTracked) this.files.delete(oldPath);
            if (wasStructuredTracked) this.structured.delete(oldPath);
          });
        }
        return;
      }
      const finalGuid = oldStructured?.guid ?? newGuid();
      const doc = this.ensureStructuredDocument(
        newPath,
        finalGuid,
        structuredKind,
        !wasStructuredTracked,
      );
      const publish = () => {
        this.indexDoc.transact(() => {
          if (wasTracked) this.files.delete(oldPath);
          if (wasStructuredTracked) this.structured.delete(oldPath);
          this.structured.set(newPath, { guid: finalGuid, kind: structuredKind });
        });
        this.registerFile(newPath, finalGuid);
      };
      if (!wasStructuredTracked) {
        if (wasTracked) {
          this.indexDoc.transact(() => {
            if (guid) this.recordTrashIn({ path: oldPath, kind: "text", guid });
            this.files.delete(oldPath);
          });
        }
        await this.publishWhenReady(doc, () => {
          if (!this.destroyed && this.canPublishLocalPath(newPath, newPathVersion, doc)) publish();
        });
        return;
      }
      publish();
    } else if (kind === "binary") {
      if (wasTracked || wasStructuredTracked) {
        this.indexDoc.transact(() => {
          if (wasTracked) this.files.delete(oldPath);
          if (wasStructuredTracked) this.structured.delete(oldPath);
        });
      }
      // Reconcile both old (now gone) and new paths on the binary side.
      this.binarySync.onLocalRenamed(file, oldPath);
    } else {
      if (wasTracked || wasStructuredTracked) {
        this.indexDoc.transact(() => {
          if (wasTracked) this.files.delete(oldPath);
          if (wasStructuredTracked) this.structured.delete(oldPath);
        });
      }
      // Renamed to an ignored path: ensure any old binary entry is dropped.
      this.binarySync.onLocalDeleted(oldPath);
    }
  }

  private onLocalModify(file: TAbstractFile): void {
    if (this.destroyed) return;
    // An open note's embeds can change as the user edits it, or as its synced
    // content first reaches disk on a fresh device.
    if (
      file instanceof TFile &&
      file.extension === "md" &&
      this.openMarkdownPaths().has(file.path)
    ) {
      this.prioritizeOpenNoteAttachments();
    }
    if (!this.initialSynced) {
      // An edit does not change which files exist, so it neither bumps the
      // path version nor supersedes the create or rename that brought the
      // file here; the replay folds it in after those.
      if (!this.isOwnWrite(file.path)) this.bootstrapLog.modify(file.path, file);
      return;
    }
    const kind = this.classify(file);
    if (kind === "binary") {
      this.binarySync.onLocalChanged(file.path);
      return;
    }
    if (kind === "structured") {
      this.markMobileDocumentUsed(file.path);
      const meta = this.structured.get(file.path);
      const existing = this.structuredDocuments.get(file.path);
      const doc =
        existing ??
        (isStructuredMeta(meta)
          ? this.ensureStructuredDocument(file.path, meta.guid, meta.kind, false)
          : undefined);
      if (doc) {
        if (existing) {
          if (!this.mobileSuspended) doc.ensureConnected();
          void doc.onDiskChanged();
        }
      }
      return;
    }
    if (kind !== "text") return;
    this.markMobileDocumentUsed(file.path);
    const existing = this.documents.get(file.path);
    const guid = this.files.get(file.path);
    const doc = existing ?? (guid ? this.ensureDocument(file.path, guid, false) : undefined);
    if (doc) {
      if (existing) {
        if (!this.mobileSuspended) doc.ensureConnected();
        void doc.onDiskChanged();
      }
    }
  }

  /**
   * Publish what the user did to the vault before the initial sync finished,
   * as its net effect (see {@link BootstrapEventLog}). A step that fails is
   * logged and skipped rather than dropping every step after it.
   */
  private async replayBootstrapVaultEvents(): Promise<void> {
    this.initialSynced = true;
    const log = this.bootstrapLog;
    try {
      for (const step of log.plan()) {
        if (this.destroyed) return;
        try {
          switch (step.type) {
            case "delete":
              this.applyLocalDeletion(step.path);
              break;
            case "rename":
              // A rename from a path the index never tracked is just a new file.
              if (this.files.has(step.from) || this.structured.has(step.from)) {
                await this.handleLocalRename(step.file, step.from);
              } else {
                await this.handleLocalCreate(step.file);
              }
              break;
            case "create":
              await this.handleLocalCreate(step.file);
              break;
            case "modify":
              this.onLocalModify(step.file);
              break;
          }
        } catch (error) {
          console.error(`[Realtime] failed to publish a startup ${step.type}`, error);
        }
      }
    } finally {
      // Events arriving from here on are handled live; nothing is withheld.
      if (this.bootstrapLog === log) this.bootstrapLog = new BootstrapEventLog();
    }
  }

  // --- Trash -----------------------------------------------------------------

  /** Write a trash entry inside an already-open index transaction. */
  private recordTrashIn(entry: {
    path: string;
    kind: TrashKind;
    guid?: string;
    hash?: string;
    size?: number;
    pluginId?: string;
    name?: string;
  }): void {
    const value: TrashEntryValue = { path: entry.path, kind: entry.kind, deletedAt: Date.now() };
    if (entry.guid !== undefined) value.guid = entry.guid;
    if (entry.hash !== undefined) value.hash = entry.hash;
    if (entry.size !== undefined) value.size = entry.size;
    if (entry.pluginId !== undefined) value.pluginId = entry.pluginId;
    if (entry.name !== undefined) value.name = entry.name;
    this.trash.set(newGuid(), value);
  }

  /** Record a deleted file in the shared trash (its own transaction). */
  recordTrash(entry: {
    path: string;
    kind: TrashKind;
    guid?: string;
    hash?: string;
    size?: number;
    pluginId?: string;
    name?: string;
  }): void {
    if (this.destroyed) return;
    this.indexDoc.transact(() => this.recordTrashIn(entry));
  }

  /** Current trash entries, newest deletion first. */
  listTrash(): TrashEntry[] {
    const out: TrashEntry[] = [];
    for (const [id, value] of this.trash.entries()) {
      if (value) out.push({ id, ...value });
    }
    out.sort((a, b) => b.deletedAt - a.deletedAt);
    return out;
  }

  /** Subscribe to trash changes; returns an unsubscribe function. */
  observeTrash(cb: () => void): () => void {
    const observer = () => cb();
    this.trash.observe(observer);
    return () => this.trash.unobserve(observer);
  }

  /** True when a path is occupied by a tracked doc, blob, or local file. */
  private pathInUse(path: string): boolean {
    if (this.files.has(path) || this.structured.has(path)) return true;
    if (this.binarySync.hasPath(path)) return true;
    return !!this.plugin.app.vault.getAbstractFileByPath(path);
  }

  /**
   * Restore a trashed file. Re-adds the index entry (content re-materializes
   * from the retained CRDT document/blob) at the original path, or `targetPath`
   * when supplied. Throws if the destination is already occupied.
   */
  async restoreTrashEntry(id: string, targetPath?: string): Promise<void> {
    if (this.destroyed) return;
    const value = this.trash.get(id);
    if (!value) throw new Error("This trash entry no longer exists.");

    // Plugin databases have no destination path to retarget: restore re-opens
    // the per-DB doc by clearing its `meta.deletedAt` tombstone. We must not
    // touch `files`/`structured` or run the `pathInUse` guard for these.
    if (value.kind === "plugindb") {
      const pluginId = value.pluginId;
      const name = value.name;
      if (!pluginId || !name) throw new Error("Trash entry is missing its database identity.");
      const sqlApi = this.plugin.sqlApi;
      if (!sqlApi) throw new Error("The SQL API is not available.");
      if (await sqlApi.isLive({ pluginId, name })) {
        throw new Error(`A database "${pluginId}/${name}" is already active — delete it first.`);
      }
      await sqlApi.restore({ pluginId, name });
      this.indexDoc.transact(() => this.trash.delete(id));
      return;
    }

    const path = (targetPath ?? "").trim() || value.path;
    if (this.pathInUse(path))
      throw new Error(`"${path}" already exists — restore under a different name.`);

    if (value.kind === "binary") {
      if (!value.hash) throw new Error("Trash entry is missing its attachment data.");
      if (!(await this.binarySync.restoreEntry(path, value.hash, value.size ?? 0))) {
        // Storage cleanup reclaimed the bytes: restoring would publish an
        // attachment every device then fails to download forever.
        this.indexDoc.transact(() => this.trash.delete(id));
        throw new Error(
          `The contents of "${value.path}" were removed by storage cleanup and cannot be restored.`,
        );
      }
    } else if (value.kind === "text") {
      if (!value.guid) throw new Error("Trash entry is missing its document id.");
      const guid = value.guid;
      this.indexDoc.transact(() => this.files.set(path, guid));
      this.registerFile(path, guid);
      this.ensureDocument(path, guid, false);
    } else {
      if (!value.guid) throw new Error("Trash entry is missing its document id.");
      const guid = value.guid;
      const kind = value.kind;
      this.indexDoc.transact(() => this.structured.set(path, { guid, kind }));
      this.registerFile(path, guid);
      this.ensureStructuredDocument(path, guid, kind, false);
    }

    this.indexDoc.transact(() => this.trash.delete(id));
  }

  /**
   * Permanently drop a trash entry. For binaries this also reclaims the orphaned
   * blob (skipped server-side if still referenced). Text/structured docs are
   * orphaned: their CRDT content lingers but is no longer reachable.
   */
  async permanentlyDeleteTrashEntry(id: string): Promise<void> {
    if (this.destroyed) return;
    const value = this.trash.get(id);
    if (!value) return;
    if (value.kind === "binary" && value.hash && !this.binarySync.hasHash(value.hash)) {
      try {
        await this.plugin.auth.deleteBlob(this.plugin.settings.activeVaultId, value.hash, id);
      } catch (e) {
        console.error(`[Realtime] failed to delete blob for trashed ${value.path}`, e);
      }
    } else if (value.kind === "plugindb" && value.pluginId && value.name) {
      // Purge the server replica + git dump and trim the per-DB doc. Mirror
      // the binary branch: log a server failure but still drop the entry.
      try {
        await this.plugin.auth.deletePluginDb(
          this.plugin.settings.activeVaultId,
          value.pluginId,
          value.name,
        );
      } catch (e) {
        console.error(`[Realtime] failed to purge plugin db for trashed ${value.path}`, e);
      }
    }
    this.indexDoc.transact(() => this.trash.delete(id));
  }

  // --- Lifecycle -------------------------------------------------------------

  destroy(): void {
    if (this.destroyed) return;
    this.destroyed = true;
    const vault = this.plugin.app.vault;
    for (const ref of this.vaultEvents) vault.offref(ref);
    this.vaultEvents = [];
    this.binarySync.destroy();
    this.configSync.destroy();
    this.localSyncState.destroy();
    this.files.unobserve(this.filesObserver);
    this.structured.unobserve(this.structuredObserver);
    this.indexDoc.getMap("binaries").unobserve(this.binariesObserver);
    if (this.noteAttachmentTimer !== null) {
      window.clearTimeout(this.noteAttachmentTimer);
      this.noteAttachmentTimer = null;
    }
    this.indexProvider.off(SYNC_EVENT_STATUS, this.statusListener);
    this.indexProvider.off(SYNC_EVENT_DOCUMENT_INVALIDATED, this.invalidationListener);
    if (this.mobileTrimTimer !== null) {
      window.clearTimeout(this.mobileTrimTimer);
      this.mobileTrimTimer = null;
    }
    for (const doc of this.documents.values()) doc.destroy();
    this.documents.clear();
    for (const doc of this.structuredDocuments.values()) doc.destroy();
    this.structuredDocuments.clear();
    this.indexProvider.destroy();
    void this.indexPersistence.destroy();
    this.indexDoc.destroy();
  }
}

export function shouldSyncCanvasBinaryPath(
  path: string,
  syncBinaries: boolean,
  excludeGlobs: string,
): boolean {
  if (!syncBinaries || isConflictCopy(path)) return false;
  const extension = path.split(".").pop()?.toLowerCase();
  if (extension === "md" || extension === "canvas" || extension === "base") return false;
  return !matchesAnyGlob(path, parseGlobs(excludeGlobs));
}
