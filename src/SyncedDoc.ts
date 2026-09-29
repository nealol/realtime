import * as Y from "yjs";
import type { Awareness } from "y-protocols/awareness";
import { IndexeddbPersistence, storeState } from "y-indexeddb";
import type RealtimePlugin from "./main";
import { getClientToken } from "./sync/clientToken";
import {
  RealtimeProvider,
  SYNC_EVENT_LOCAL_CHANGES,
  SYNC_STATUS_ERROR,
  SYNC_STATUS_OFFLINE,
} from "./sync/RealtimeProvider";
import { createMuxSocket } from "./sync/mux";
import { epochPersistenceName } from "./documentEpoch";
import { preserveTextConflict } from "./conflictRecovery";

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
}

export abstract class SyncedDoc {
  readonly path: string;
  readonly guid: string;
  readonly serverDocId: string;
  readonly ydoc: Y.Doc;
  readonly provider: RealtimeProvider;
  readonly awareness: Awareness;
  isCreator: boolean;

  protected readonly plugin: RealtimePlugin;
  protected destroyed = false;
  protected readonly persistence: IndexeddbPersistence;

  private readyPromise: Promise<void>;
  private resolveReady!: () => void;
  private ready = false;
  private syncedListener: (synced: boolean) => void;
  private localChangesListener: (hasLocalChanges: boolean) => void;
  private readonly autoConnect: boolean;
  private persistenceReady = false;
  private connectRequested = false;
  /** True once the provider has reported a successful server sync at least once. */
  private syncedOnce = false;
  private nextServerSyncWaiters = new Set<() => void>();
  private readOnlyRecoveryPending = false;
  private readOnlyRecoveryRequested = false;
  private readOnlyRecoveryBaseline: string | null = null;

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
    this.ydoc = new Y.Doc();

    this.readyPromise = new Promise((resolve) => {
      this.resolveReady = resolve;
    });

    this.provider = new RealtimeProvider(
      serverDocId,
      this.ydoc,
      () => getClientToken(plugin, serverDocId, path),
      {
        connect: false,
        socketFactory: createMuxSocket,
        onReadOnlyUpdate: () => void this.preserveReadOnlyRecovery(),
      },
    );
    this.awareness = this.provider.awareness;
    this.persistence = new IndexeddbPersistence(
      epochPersistenceName(plugin, serverDocId, serverDocId),
      this.ydoc,
    );
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
      if (!hasLocalChanges && !this.destroyed) void this.afterChangesSynced();
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
  protected afterChangesSynced(): Promise<void> | void {}
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
        await preserveTextConflict(this.plugin, this.path, value, "local");
        if (!this.destroyed) {
          this.readOnlyRecoveryBaseline = value;
        }
      }
    } catch (error) {
      console.warn(`[Realtime] failed to preserve read-only edit for ${this.path}`, error);
    } finally {
      this.readOnlyRecoveryPending = false;
    }
  }

  destroy(): void {
    if (this.destroyed) return;
    this.destroyed = true;
    this.destroySubclass();
    this.provider.off("synced", this.syncedListener);
    this.provider.off(SYNC_EVENT_LOCAL_CHANGES, this.localChangesListener);
    this.provider.destroy();
    void this.persistence.destroy();
    this.ydoc.destroy();
    this.resolveNextServerSyncWaiters();
    this.resolveWhenReady();
  }
}
