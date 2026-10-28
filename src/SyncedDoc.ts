import * as Y from "yjs";
import type { Awareness } from "y-protocols/awareness";
import { IndexeddbPersistence, clearDocument, storeState } from "y-indexeddb";
import type RealtimePlugin from "./main";
import { getClientToken } from "./sync/clientToken";
import {
  RealtimeProvider,
  SYNC_EVENT_LOCAL_CHANGES,
  SYNC_STATUS_ERROR,
  SYNC_STATUS_OFFLINE,
} from "./sync/RealtimeProvider";
import { createMuxSocket } from "./sync/mux";
import { compatibilityGateFor } from "./sync/compatibilityGate";
import { epochPersistenceName, epochStoreBaseName, getDocumentEpoch } from "./documentEpoch";
import { updateTextConflict } from "./conflictRecovery";
import { sha256Text } from "./hash";
import { registerPresenceOwner, type PresenceOwner } from "./presenceOwners";
import type { MaterializedKind } from "./localSyncState";

/**
 * How long an acknowledgement waits before its content is fingerprinted. A
 * burst of acknowledgements (continuous typing) is recorded once per window;
 * later ones never push the deadline back, so recording cannot starve.
 */
const ACKNOWLEDGE_DELAY_MS = 250;

/**
 * Fingerprint recording that a document's local store contains `text`.
 * Scoped to the epoch: each epoch starts from an empty store, so content a
 * retired instance held says nothing about its replacement.
 */
export async function storedContentFingerprint(epoch: number, text: string): Promise<string> {
  return `${epoch}:${await sha256Text(text)}`;
}

export interface DocumentBootstrapOptions {
  autoConnect?: boolean;
  /** The local file's durable identity is a different guid than this doc's. */
  forceBootstrapConflict?: boolean;
  /**
   * Acknowledged fingerprint of the local file under an identity that has
   * since been removed from the index. A disk file still matching it holds no
   * unsynced edits, so it is replaced by this document instead of conflicting.
   */
  staleLocalFingerprint?: string | null;
  /**
   * Content of the document this one replaces after an epoch rollover, when
   * the server had acknowledged all of it (see {@link SyncedDoc.retiredBaseline}).
   * The new epoch starts from an empty local store, so this is the only
   * baseline that tells local edits apart from remote ones.
   */
  retiredBaseline?: string | null;
}

export abstract class SyncedDoc implements PresenceOwner {
  readonly path: string;
  readonly guid: string;
  readonly serverDocId: string;
  /** Epoch this instance's Y.Doc and persistence belong to; never changes. */
  readonly epoch: number;
  readonly ydoc: Y.Doc;
  readonly provider: RealtimeProvider;
  readonly awareness: Awareness;
  isCreator: boolean;

  protected readonly plugin: RealtimePlugin;
  protected destroyed = false;
  /** True only after startup content is durably present on this device. */
  protected startupReady = false;
  protected readonly persistence: IndexeddbPersistence;

  private readyPromise: Promise<void>;
  private resolveReady!: () => void;
  private ready = false;
  private syncedListener: (synced: boolean) => void;
  private localChangesListener: (hasLocalChanges: boolean) => void;
  private readonly autoConnect: boolean;
  private persistenceReady = false;
  /** Whether IndexedDB held any state for this document when it loaded. */
  protected loadedStoredState = false;
  private connectRequested = false;
  /** True once the provider has reported a successful server sync at least once. */
  private syncedOnce = false;
  private nextServerSyncWaiters = new Set<() => void>();
  private readOnlyRecoveryPending = false;
  private readOnlyRecoveryRequested = false;
  private readOnlyRecoveryBaseline: string | null = null;
  /** The one recovery copy this document keeps its read-only edits in. */
  private readOnlyRecoveryPath: string | null = null;
  private destroyListeners = new Set<() => void>();
  /** This client's presence fields (name, colors, avatar). */
  private presenceUser: Record<string, unknown> | null = null;
  /** Open views of this document; presence is published only while one exists. */
  private presenceHolds = 0;
  /** Bumped by every content change; see {@link noteContentChanged}. */
  private contentRevision = 0;
  /** Fingerprint of the content at `revision`, hashed once per revision. */
  private contentFingerprint: { revision: number; fingerprint: Promise<string> } | null = null;
  private acknowledgeTimer: number | null = null;

  protected constructor(
    plugin: RealtimePlugin,
    path: string,
    guid: string,
    serverDocId: string,
    isCreator: boolean,
    opts: { autoConnect?: boolean } = {},
  ) {
    this.plugin = plugin;
    this.path = path;
    this.guid = guid;
    this.serverDocId = serverDocId;
    this.isCreator = isCreator;
    this.autoConnect = opts.autoConnect ?? true;
    this.epoch = getDocumentEpoch(plugin, serverDocId);
    this.ydoc = new Y.Doc();

    this.readyPromise = new Promise((resolve) => {
      this.resolveReady = resolve;
    });

    this.provider = new RealtimeProvider(
      serverDocId,
      this.ydoc,
      // Pin the epoch: once it moves on, this instance holds retired-epoch
      // state and must be rebuilt rather than reconnected.
      () => getClientToken(plugin, serverDocId, path, this.epoch),
      {
        connect: false,
        socketFactory: createMuxSocket,
        onReadOnlyUpdate: () => void this.preserveReadOnlyRecovery(),
        connectionGate: compatibilityGateFor(plugin),
      },
    );
    this.awareness = this.provider.awareness;
    // y-protocols starts every Awareness with an (empty) local state, which it
    // renews and every peer re-broadcasts every 15s. Most documents are never
    // on screen, so publish presence only while a view holds it.
    this.awareness.setLocalState(null);
    registerPresenceOwner(this.awareness, this);
    const store = epochStoreBaseName(plugin, serverDocId);
    this.persistence = new IndexeddbPersistence(
      epochPersistenceName(plugin, serverDocId, store, this.epoch),
      this.ydoc,
    );
    // The previous epoch's store can never be connected again; drop it even
    // when the document was not resident (or the app was closed) at rollover.
    if (this.epoch > 0) {
      void clearDocument(epochPersistenceName(plugin, serverDocId, store, this.epoch - 1)).catch(
        () => {},
      );
    }
    const storeUpdate = this.persistence._storeUpdate;
    this.ydoc.off("update", storeUpdate);
    const filteredStoreUpdate = (update: Uint8Array, origin: unknown) => {
      if (this.provider.clientToken?.authorization === "read-only" && origin !== this.provider) {
        return;
      }
      storeUpdate(update, origin);
    };
    this.persistence._storeUpdate = filteredStoreUpdate;
    this.ydoc.on("update", filteredStoreUpdate);

    this.syncedListener = (synced) => {
      if (synced) {
        this.syncedOnce = true;
        this.resolveNextServerSyncWaiters();
        void this.finishStartupReconcile();
      }
    };
    this.provider.on("synced", this.syncedListener);
    this.localChangesListener = (hasLocalChanges) => {
      if (!hasLocalChanges) this.scheduleAcknowledgedContent();
    };
    this.provider.on(SYNC_EVENT_LOCAL_CHANGES, this.localChangesListener);

    void this.init();
  }

  whenReady(): Promise<void> {
    return this.readyPromise;
  }

  isReady(): boolean {
    return this.ready;
  }

  /** True once the first successful server sync has been observed. */
  get hasSyncedOnce(): boolean {
    return this.syncedOnce;
  }

  /**
   * True while the provider is online (or trying to be) — i.e. a server sync is
   * expected that could deliver content we don't yet have locally. When offline
   * or errored, no sync can arrive, so local content must be persisted as-is.
   */
  get isProviderOnline(): boolean {
    const status = this.provider.status;
    return status !== SYNC_STATUS_OFFLINE && status !== SYNC_STATUS_ERROR;
  }

  isDestroyed(): boolean {
    return this.destroyed;
  }

  ensureConnected(): void {
    if (this.destroyed) return;
    this.connectRequested = true;
    if (!this.persistenceReady) return;
    this.connectProvider();
  }

  private connectProvider(): void {
    const status = this.provider.status;
    if (status === SYNC_STATUS_OFFLINE || status === SYNC_STATUS_ERROR) {
      void this.provider.connect();
    }
  }

  connect(): void {
    this.ensureConnected();
  }

  disconnect(): void {
    if (this.destroyed) return;
    this.provider.disconnect();
  }

  /**
   * Flush pending IndexedDB writes before releasing this document's in-memory
   * state. A document with unacknowledged server changes is never eligible.
   */
  async prepareForHibernation(): Promise<boolean> {
    if (
      this.destroyed ||
      !this.ready ||
      this.provider.hasLocalChanges ||
      !this.canHibernateLocally()
    ) {
      return false;
    }
    await storeState(this.persistence, false);
    return (
      !this.destroyed && this.ready && !this.provider.hasLocalChanges && this.canHibernateLocally()
    );
  }

  protected canHibernateLocally(): boolean {
    return true;
  }

  /**
   * Resolve once every update applied so far is committed to IndexedDB.
   * y-indexeddb stores updates in their own write transactions; a later
   * transaction on the same store cannot run until those have committed.
   */
  protected async flushPersistence(): Promise<void> {
    if (this.destroyed || !this.persistenceReady) return;
    await storeState(this.persistence, false);
  }

  /**
   * Record that the file holds `text`, which this document contains, so a
   * restart can tell a file that merely lags the document from a local edit.
   * Recorded only once IndexedDB holds the updates that produced `text`.
   */
  protected async recordStoredDiskContent(kind: MaterializedKind, text: string): Promise<void> {
    try {
      // Before the store has loaded there is nothing to flush against.
      if (!this.persistenceReady) return;
      const fingerprint = await storedContentFingerprint(this.epoch, text);
      await this.flushPersistence();
      if (this.destroyed) return;
      this.plugin.vaultSync?.noteDiskContent(this.path, kind, this.guid, fingerprint);
    } catch (error) {
      console.warn(`[Realtime] could not record disk state for ${this.path}`, error);
    }
  }

  /**
   * Whether disk `text` holds no local edits: it is content this document's
   * local store already contains (see {@link recordStoredDiskContent}), so the
   * file merely lags the document — or, when the store loaded nothing (a new
   * epoch, or a lost database), it is exactly the content the server last
   * acknowledged for this identity.
   */
  protected async storeContainsDiskContent(text: string): Promise<boolean> {
    const vaultSync = this.plugin.vaultSync;
    if (!this.loadedStoredState) {
      const acknowledged = vaultSync?.acknowledgedFingerprint?.(this.path, this.guid);
      return acknowledged != null && acknowledged === (await sha256Text(text));
    }
    const known = await vaultSync?.diskFingerprint(this.path, this.guid);
    return known != null && known === (await storedContentFingerprint(this.epoch, text));
  }

  /**
   * This document's content as a merge baseline for the one replacing it in
   * a new epoch, or null while the server has not acknowledged all of it:
   * changes it never persisted may be missing from the new epoch, and using
   * them as the baseline would read their absence as a remote deletion.
   */
  retiredBaseline(): string | null {
    if (this.destroyed || !this.ready || this.provider.hasLocalChanges) return null;
    return this.serializeRecoveryContent();
  }

  /** Set this client's presence fields; published while a view holds presence. */
  setPresenceUser(user: Record<string, unknown>): void {
    this.presenceUser = user;
    if (this.presenceHolds > 0 && !this.destroyed) this.awareness.setLocalStateField("user", user);
  }

  holdPresence(): () => void {
    this.presenceHolds += 1;
    if (this.presenceHolds === 1 && !this.destroyed) {
      this.awareness.setLocalState(this.presenceUser ? { user: this.presenceUser } : {});
    }
    let released = false;
    return () => {
      if (released) return;
      released = true;
      this.presenceHolds -= 1;
      if (this.presenceHolds === 0 && !this.destroyed) this.awareness.setLocalState(null);
    };
  }

  /** Run `listener` once this instance is destroyed; returns an unsubscribe. */
  onDestroy(listener: () => void): () => void {
    if (this.destroyed) {
      listener();
      return () => {};
    }
    this.destroyListeners.add(listener);
    return () => this.destroyListeners.delete(listener);
  }

  /** Resolve after the next successful server handshake. */
  whenNextServerSync(): Promise<void> {
    if (this.destroyed) return Promise.resolve();
    let resolve!: () => void;
    const promise = new Promise<void>((done) => {
      resolve = done;
    });
    this.nextServerSyncWaiters.add(resolve);
    return promise;
  }

  private resolveNextServerSyncWaiters(): void {
    for (const resolve of this.nextServerSyncWaiters) resolve();
    this.nextServerSyncWaiters.clear();
  }

  protected resolveWhenReady(): void {
    if (this.ready) return;
    this.ready = true;
    this.resolveReady();
  }

  protected async init(): Promise<void> {
    try {
      await this.persistence.whenSynced;
      this.loadedStoredState = this.ydoc.store.clients.size > 0;
      if (!this.destroyed) await this.afterPersistenceSynced();
    } catch (e) {
      console.error(`[Realtime] init failed for ${this.path}`, e);
      this.resolveWhenReady();
    }

    this.persistenceReady = true;
    if (!this.destroyed && (this.autoConnect || this.connectRequested)) {
      this.connectProvider();
    }
  }

  protected abstract afterPersistenceSynced(): Promise<void> | void;
  protected abstract finishStartupReconcile(): Promise<void>;
  /** The kind of file this document materializes as. */
  protected abstract materializedKind(): MaterializedKind;

  /** Subclasses call this from their content observers for every content change. */
  protected noteContentChanged(): void {
    this.contentRevision += 1;
  }

  /**
   * Record, after {@link ACKNOWLEDGE_DELAY_MS}, that the server holds the
   * current content. Requests made while one is pending join it.
   */
  private scheduleAcknowledgedContent(): void {
    if (this.destroyed || !this.startupReady || this.acknowledgeTimer !== null) return;
    this.acknowledgeTimer = window.setTimeout(() => {
      this.acknowledgeTimer = null;
      this.recordAcknowledgedContent().catch((error: unknown) => {
        console.warn(`[Realtime] could not record acknowledged content for ${this.path}`, error);
      });
    }, ACKNOWLEDGE_DELAY_MS);
  }

  /**
   * Record that the server holds this document's current content, when it is
   * ready and holds no unacknowledged local change. Content that changes
   * while it is being fingerprinted is not recorded; a later record retries.
   */
  protected async recordAcknowledgedContent(): Promise<void> {
    if (this.destroyed || !this.startupReady || this.provider.hasLocalChanges) return;
    const revision = this.contentRevision;
    let entry = this.contentFingerprint;
    if (!entry || entry.revision !== revision) {
      entry = { revision, fingerprint: sha256Text(this.serializeRecoveryContent()) };
      this.contentFingerprint = entry;
    }
    let fingerprint: string;
    try {
      fingerprint = await entry.fingerprint;
    } catch (error) {
      if (this.contentFingerprint === entry) this.contentFingerprint = null;
      throw error;
    }
    if (this.destroyed || !this.startupReady || this.provider.hasLocalChanges) return;
    if (this.contentRevision !== revision) {
      // The hash describes content the document no longer holds. A remote
      // change raises no new acknowledgement, so retry on our own.
      this.scheduleAcknowledgedContent();
      return;
    }
    // Reconciled: this content is what the document settled on, not a
    // pending local candidate.
    this.plugin.vaultSync?.noteContentAcknowledged(
      this.path,
      this.materializedKind(),
      this.guid,
      fingerprint,
      true,
    );
  }

  protected abstract serializeRecoveryContent(): string;
  protected abstract destroySubclass(): void;

  private async preserveReadOnlyRecovery(): Promise<void> {
    if (this.destroyed) return;
    this.readOnlyRecoveryRequested = true;
    if (this.readOnlyRecoveryPending) return;
    this.readOnlyRecoveryPending = true;
    try {
      while (this.readOnlyRecoveryRequested && !this.destroyed) {
        this.readOnlyRecoveryRequested = false;
        const value = this.serializeRecoveryContent();
        if (value === this.readOnlyRecoveryBaseline) continue;
        const recoveryPath = await updateTextConflict(
          this.plugin,
          this.path,
          value,
          "local",
          this.readOnlyRecoveryPath,
        );
        if (!this.destroyed) {
          this.readOnlyRecoveryBaseline = value;
          this.readOnlyRecoveryPath = recoveryPath ?? this.readOnlyRecoveryPath;
        }
      }
    } catch (error) {
      console.warn(`[Realtime] failed to preserve read-only edit for ${this.path}`, error);
    } finally {
      this.readOnlyRecoveryPending = false;
    }
  }

  /**
   * Release this document. `clearLocalStore` also deletes its IndexedDB store
   * (a retired epoch, or a deleted file whose changes all reached the server)
   * so stores do not accumulate on the device.
   */
  destroy(options: { clearLocalStore?: boolean } = {}): void {
    if (this.destroyed) return;
    this.destroyed = true;
    this.destroySubclass();
    if (this.acknowledgeTimer !== null) {
      window.clearTimeout(this.acknowledgeTimer);
      this.acknowledgeTimer = null;
    }
    this.provider.off("synced", this.syncedListener);
    this.provider.off(SYNC_EVENT_LOCAL_CHANGES, this.localChangesListener);
    this.provider.destroy();
    if (options.clearLocalStore) void this.persistence.clearData();
    else void this.persistence.destroy();
    this.ydoc.destroy();
    this.resolveNextServerSyncWaiters();
    this.resolveWhenReady();
    const listeners = [...this.destroyListeners];
    this.destroyListeners.clear();
    for (const listener of listeners) {
      try {
        listener();
      } catch (error) {
        console.error(`[Realtime] destroy listener failed for ${this.path}`, error);
      }
    }
  }
}
