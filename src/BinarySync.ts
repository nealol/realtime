import * as Y from "yjs";
import { TFile, Notice, Platform } from "obsidian";
import type RealtimePlugin from "./main";
import type { VaultSync } from "./VaultSync";
import { sha256Hex } from "./hash";
import { dbg } from "./debug";
import { ensureParentFolder, getFileByPath, isOpenInWorkspace } from "./vaultHelpers";
import { openBinaryConflictModal, type ConflictChoice } from "./BinaryConflictModal";
import type { LocalSyncState } from "./localSyncState";
import { preserveBinaryConflict } from "./conflictRecovery";
import { HttpError, isServerUnavailableStatus } from "./httpError";

/**
 * Upload-queue state surfaced to the status bar:
 *  - `uploading` — a blob transfer is in flight;
 *  - `pending`   — blobs are queued but deferred (e.g. a large file waiting for
 *                  note sync to quiet down);
 *  - `idle`      — nothing queued or in flight.
 */
export type UploadStatus = "idle" | "uploading" | "pending";

/** Metadata tracked per binary file in the shared index doc. */
export interface BinaryMeta {
  /** Lowercase hex sha256 of the file's bytes — the blob store key. */
  hash: string;
  /** Byte length, for display and upload scheduling. */
  size: number;
  /**
   * The hash this version replaced (null for a new file) — the version its
   * publisher had agreed on. Lets a device tell an update built on its own
   * publish from a concurrent one that last-writer-wins chose over it.
   * Absent on entries from older clients.
   */
  prev?: string | null;
}

/** Files at or above this size are uploaded in the background, deferred while
 *  notes are actively syncing, so a large attachment can't stall note sync. */
const LARGE_FILE_BYTES = 5 * 1024 * 1024;
/** Max attempts for an upload the server rejects outright (with a Notice). */
const MAX_UPLOAD_ATTEMPTS = 5;
/** Cap for the backoff between retries while the server is unreachable. */
const MAX_RETRY_MS = 60_000;
/** Startup reconcile pool: parallel paths, and bytes held in memory at once. */
const RECONCILE_CONCURRENCY = 6;
const MOBILE_RECONCILE_CONCURRENCY = 3;
const RECONCILE_MAX_BYTES_IN_FLIGHT = 32 * 1024 * 1024;
/** Delay before re-draining the upload queue when deferred / after a failure. */
const DRAIN_RETRY_MS = 2000;
/** A large upload waits at most this long for note sync to quiet down. */
const MAX_LARGE_UPLOAD_DEFERRAL_MS = 60_000;
/** Live (non-startup) transfers: parallel jobs and bytes held in memory at once. */
const TRANSFER_CONCURRENCY = 4;
const MOBILE_TRANSFER_CONCURRENCY = 2;
const TRANSFER_MAX_BYTES_IN_FLIGHT = 32 * 1024 * 1024;
const MOBILE_TRANSFER_MAX_BYTES_IN_FLIGHT = 16 * 1024 * 1024;

/** Exponential backoff from DRAIN_RETRY_MS, capped at MAX_RETRY_MS. */
function retryDelay(failures: number): number {
  return Math.min(MAX_RETRY_MS, DRAIN_RETRY_MS * 2 ** Math.max(0, Math.min(failures - 1, 5)));
}

/**
 * Failures that do not say the server rejected this transfer: no response at
 * all (offline), the server being unavailable, or a rejected session (kept
 * until the user signs in again). Retry them for as long as it takes.
 */
function isTransientTransferError(error: unknown): boolean {
  if (!(error instanceof HttpError)) return true;
  return isServerUnavailableStatus(error.status) || error.status >= 500;
}

/**
 * A queued upload. Holds no bytes: the file is read when its transfer starts,
 * so a large import queues paths rather than the vault's attachments in memory.
 */
interface UploadJob {
  path: string;
  hash: string;
  size: number;
  attempts: number;
  urgent: boolean;
  expectedRemoteHash?: string | null;
  diskVersion: number;
  /** When note sync first held this (large) upload back. */
  deferredSince?: number;
}

/** Bounds concurrent transfers and the bytes they hold in memory at once. */
class TransferLimiter {
  private active = 0;
  private bytes = 0;
  private waiters: Array<() => void> = [];

  constructor(
    private readonly maxJobs: number,
    private readonly maxBytes: number,
  ) {}

  async run<T>(size: number, work: () => Promise<T>): Promise<T> {
    // A job larger than the whole budget still runs, alone.
    while (this.active > 0 && (this.active >= this.maxJobs || this.bytes + size > this.maxBytes)) {
      await new Promise<void>((resolve) => this.waiters.push(resolve));
    }
    this.active += 1;
    this.bytes += size;
    try {
      return await work();
    } finally {
      this.active -= 1;
      this.bytes -= size;
      for (const wake of this.waiters.splice(0)) wake();
    }
  }
}

/**
 * Syncs binary (non-Markdown) files via a content-addressed blob store instead of
 * the text CRDT. Only a `path -> { hash, size }` mapping travels through the shared
 * index Y.Doc (the `binaries` map); the bytes live on the server, keyed by hash.
 *
 * Reconciliation is a single idempotent {@link reconcile} per path, driven from
 * three sources — local vault events, remote `binaries` map changes, and the
 * one-shot startup pass — comparing three hashes: the local disk, the remote map
 * entry, and {@link lastSyncedHash} (what this device last agreed on). When both
 * the local and remote sides moved off that baseline, it's a true conflict and we
 * ask the user (keep local / keep remote). Map entries are published only *after*
 * the blob upload succeeds, so peers never see a hash whose bytes aren't on the
 * server yet.
 */
export class BinarySync {
  private plugin: RealtimePlugin;
  private vaultSync: VaultSync;
  private binaries: Y.Map<BinaryMeta>;
  private indexDoc: Y.Doc;
  private localSyncState?: LocalSyncState;
  /** Device-local filter; excluded paths stay in the shared index for peers. */
  private shouldTrack: (path: string) => boolean;

  /** Per-path hash this device has reconciled with disk (the merge baseline). */
  private lastSyncedHash = new Map<string, string>();
  /** Serializes reconcile() per path so concurrent triggers can't interleave. */
  private chains = new Map<string, Promise<void>>();
  private pendingReconcile = new Set<string>();
  private deferredReconciles = new Set<string>();
  private deferredInitialPulls = new Set<string>();
  /** Remote files whose disk materialization has not succeeded yet. */
  private pendingDownloads = new Set<string>();
  /** Consecutive failed downloads per path, for retry backoff. */
  private downloadFailures = new Map<string, number>();
  /** Paths we are currently writing to disk, to ignore the resulting vault event. */
  private writing = new Set<string>();
  /**
   * Paths the user deleted in a live vault event. That is proof of a delete,
   * so it overrides the "missing means not pulled yet" startup semantics.
   */
  private liveDeletes = new Set<string>();
  /** Paths migrated away from binary sync during this session. */
  private ignoredPaths = new Set<string>();
  /** Startup pulls should restore missing files from the moved/persisted index. */
  private pullingMissingRemote = false;

  /** Background upload queue (latest job per path wins). */
  private uploadQueue: UploadJob[] = [];
  private urgentPaths = new Set<string>();
  private diskVersions = new Map<string, number>();
  private draining = false;
  /** True only while a single blob transfer is actually in flight. */
  private activeUpload = false;
  private drainTimer: number | null = null;
  /** Last reported upload status (for the status bar); only emit on change. */
  private uploadStatus: UploadStatus = "idle";

  /** Serializes conflict modals so only one is shown at a time. */
  private conflictChain: Promise<void> = Promise.resolve();

  /** Disk reads for hashing, which hold a whole file in memory. */
  private readonly readLimiter = new TransferLimiter(
    Platform?.isMobile ? MOBILE_TRANSFER_CONCURRENCY : TRANSFER_CONCURRENCY,
    Platform?.isMobile ? MOBILE_TRANSFER_MAX_BYTES_IN_FLIGHT : TRANSFER_MAX_BYTES_IN_FLIGHT,
  );
  /** Blob downloads; a remote bulk import must not start them all at once. */
  private readonly downloadLimiter = new TransferLimiter(
    Platform?.isMobile ? MOBILE_TRANSFER_CONCURRENCY : TRANSFER_CONCURRENCY,
    Platform?.isMobile ? MOBILE_TRANSFER_MAX_BYTES_IN_FLIGHT : TRANSFER_MAX_BYTES_IN_FLIGHT,
  );

  /** Observer is a no-op until the startup pass runs (see {@link reconcileAll}). */
  private started = false;
  private paused = false;
  private destroyed = false;
  private observer: (event: Y.YMapEvent<BinaryMeta>) => void;

  constructor(
    plugin: RealtimePlugin,
    vaultSync: VaultSync,
    indexDoc: Y.Doc,
    localSyncState?: LocalSyncState,
    shouldTrack: (path: string) => boolean = () => true,
  ) {
    this.plugin = plugin;
    this.vaultSync = vaultSync;
    this.indexDoc = indexDoc;
    this.localSyncState = localSyncState;
    this.shouldTrack = shouldTrack;
    this.binaries = indexDoc.getMap<BinaryMeta>("binaries");
    this.observer = this.onBinariesChanged.bind(this);
    this.binaries.observe(this.observer);
  }

  private get vaultId(): string {
    return this.plugin.settings.activeVaultId;
  }

  // --- startup ---------------------------------------------------------------

  /**
   * Capture the persisted (pre-remote-merge) map as the sync baseline. Must run
   * after the index persistence loads but *before* the provider connects, so we
   * record what this device last agreed on rather than the merged result.
   */
  seedBaseline(): void {
    for (const [path, meta] of this.binaries.entries()) {
      if (!meta?.hash) continue;
      if (!this.localSyncState) {
        this.lastSyncedHash.set(path, meta.hash);
        continue;
      }
      this.localSyncState.migrateLegacyIdentity(path, "binary", meta.hash);
      const state = this.localSyncState?.get(path);
      if (state?.identity === meta.hash && !state.candidate) {
        this.lastSyncedHash.set(path, meta.hash);
      }
    }
    for (const [path, state] of this.localSyncState?.entries() ?? []) {
      if (
        state.kind === "binary" &&
        !state.candidate &&
        state.identity &&
        state.fingerprint === state.identity
      ) {
        this.lastSyncedHash.set(path, state.identity);
      }
    }
    dbg("BinarySync seedBaseline", this.lastSyncedHash.size, "entries");
  }

  /**
   * One-shot reconcile after the first server sync: walk every path known to the
   * remote map or present locally, then enable the live observer.
   */
  async reconcileAll(localBinaryPaths: string[]): Promise<void> {
    if (this.destroyed) return;
    this.started = true;

    const paths = new Set<string>(this.binaries.keys());
    for (const p of localBinaryPaths) paths.add(p);

    this.pullingMissingRemote = true;
    try {
      await this.reconcileConcurrently([...paths]);
    } finally {
      this.pullingMissingRemote = false;
    }
  }

  /**
   * Reconcile many paths with a small bounded pool. The startup pass used to
   * await each path in turn, so a fresh device paid one full blob round trip
   * per attachment back to back. Each download is held in memory whole, so
   * the pool is also capped by the bytes it has in flight (a single file
   * larger than the cap still runs, alone).
   */
  private async reconcileConcurrently(paths: string[]): Promise<void> {
    const maxJobs = Platform?.isMobile ? MOBILE_RECONCILE_CONCURRENCY : RECONCILE_CONCURRENCY;
    const running = new Set<Promise<void>>();
    let bytesInFlight = 0;
    for (const path of paths) {
      if (this.destroyed) break;
      // A local-only file is read (hashed and uploaded), so it costs its size too.
      const size =
        this.binaries.get(path)?.size ?? getFileByPath(this.plugin.app, path)?.stat?.size ?? 0;
      while (
        running.size > 0 &&
        (running.size >= maxJobs || bytesInFlight + size > RECONCILE_MAX_BYTES_IN_FLIGHT)
      ) {
        await Promise.race(running);
      }
      if (this.destroyed) break;
      bytesInFlight += size;
      const job: Promise<void> = this.reconcile(path).finally(() => {
        bytesInFlight -= size;
        running.delete(job);
      });
      running.add(job);
    }
    await Promise.all(running);
  }

  remotePaths(): string[] {
    return [...this.binaries.keys()];
  }

  async reconcilePath(path: string): Promise<void> {
    await this.reconcile(path);
  }

  /** Prioritize Canvas-referenced files without creating a second transfer path. */
  prioritizePaths(paths: Iterable<string>): void {
    if (this.destroyed) return;
    const added: string[] = [];
    for (const path of paths) {
      if (!this.shouldTrack(path)) continue;
      if (!this.urgentPaths.has(path)) added.push(path);
      this.urgentPaths.add(path);
    }
    for (const job of this.uploadQueue) {
      if (this.urgentPaths.has(job.path)) job.urgent = true;
    }
    this.uploadQueue.sort((left, right) => Number(right.urgent) - Number(left.urgent));
    for (const path of added) {
      // Until the startup pass has run, a file missing locally means "not
      // pulled yet", never "deleted here": give the early reconcile the same
      // semantics instead of publishing a delete from a stale baseline.
      if (!this.started || this.pullingMissingRemote) this.deferredInitialPulls.add(path);
      void this.reconcile(path);
    }
    this.scheduleDrain();
  }

  unprioritizePaths(paths: Iterable<string>): void {
    for (const path of paths) {
      this.urgentPaths.delete(path);
      const job = this.uploadQueue.find((candidate) => candidate.path === path);
      if (job) job.urgent = false;
    }
  }

  stopTrackingPath(path: string): void {
    this.ignoredPaths.add(path);
    this.lastSyncedHash.delete(path);
    this.indexDoc.transact(() => {
      this.binaries.delete(path);
    });
  }

  // --- live triggers ---------------------------------------------------------

  /** A local binary file was created or modified. */
  onLocalChanged(path: string): void {
    if (!this.shouldTrack(path)) return;
    if (this.ignoredPaths.has(path)) return;
    if (this.writing.has(path)) return;
    this.bumpDiskVersion(path);
    void this.reconcile(path);
  }

  /** A local binary file was deleted. */
  onLocalDeleted(path: string): void {
    if (!this.shouldTrack(path)) return;
    if (this.ignoredPaths.has(path)) return;
    if (this.writing.has(path)) return;
    this.bumpDiskVersion(path);
    this.liveDeletes.add(path);
    this.deferredInitialPulls.delete(path);
    void this.reconcile(path);
  }

  /** Whether this sync is writing `path` itself (its vault event is not a user edit). */
  isWriting(path: string): boolean {
    return this.writing.has(path);
  }

  /** A local binary file was renamed: treat as a delete of old + change of new. */
  onLocalRenamed(file: TFile, oldPath: string): void {
    if (!this.shouldTrack(oldPath) && !this.shouldTrack(file.path)) return;
    if (this.ignoredPaths.has(oldPath) || this.ignoredPaths.has(file.path)) return;
    this.bumpDiskVersion(oldPath);
    this.bumpDiskVersion(file.path);
    void this.reconcile(oldPath);
    void this.reconcile(file.path);
  }

  private onBinariesChanged(event: Y.YMapEvent<BinaryMeta>): void {
    if (this.destroyed || !this.started) return;
    event.changes.keys.forEach((_change, path) => {
      if (!this.shouldTrack(path)) return;
      if (this.ignoredPaths.has(path)) return;
      void this.reconcile(path);
    });
  }

  // --- core reconcile --------------------------------------------------------

  /** Run `fn` exclusively for `path`, chaining after any in-flight reconcile. */
  private reconcile(path: string): Promise<void> {
    const active = this.chains.get(path);
    if (active) {
      this.pendingReconcile.add(path);
      return active;
    }
    const next = Promise.resolve()
      .then(async () => {
        do {
          this.pendingReconcile.delete(path);
          await this.reconcileNow(path);
        } while (this.pendingReconcile.delete(path) && !this.destroyed);
      })
      .catch((e) => {
        console.error(`[Realtime] binary reconcile failed for ${path}`, e);
      });
    this.chains.set(path, next);
    void next.finally(() => {
      if (this.chains.get(path) === next) this.chains.delete(path);
    });
    return next;
  }

  private async reconcileNow(path: string): Promise<void> {
    if (this.destroyed) return;
    if (!this.shouldTrack(path)) return;
    if (this.ignoredPaths.has(path)) return;
    const liveDelete = this.liveDeletes.delete(path);
    const initialPull =
      !liveDelete && (this.pullingMissingRemote || this.deferredInitialPulls.delete(path));
    if (this.paused) {
      if (liveDelete) this.liveDeletes.add(path);
      this.deferReconcile(path, initialPull);
      return;
    }

    const localHash = await this.hashDisk(path);
    if (this.destroyed) return;
    if (this.paused) {
      if (liveDelete) this.liveDeletes.add(path);
      this.deferReconcile(path, initialPull);
      return;
    }
    if (localHash && !this.localSyncState?.has(path)) {
      this.localSyncState?.beginCandidate(path, "binary", localHash, localHash);
    }
    const remote = this.binaries.get(path);
    const remoteHash = remote?.hash ?? null;
    const base = this.lastSyncedHash.get(path) ?? null;

    // Already in agreement (both sides equal, or both absent).
    if (localHash === remoteHash) {
      if (remoteHash) {
        this.lastSyncedHash.set(path, remoteHash);
        this.localSyncState?.markSynced(path, "binary", remoteHash, remoteHash, true);
      } else {
        this.lastSyncedHash.delete(path);
      }
      this.pendingDownloads.delete(path);
      this.urgentPaths.delete(path);
      return;
    }

    if (localHash === undefined) {
      // Could not read the local file. Do not treat that as a delete.
      return;
    }

    // Local present, remote absent.
    if (localHash && !remoteHash) {
      if (base === null) {
        // Brand-new local file → publish it.
        await this.queueLocalUpload(path, null, localHash);
      } else if (base === localHash) {
        // Cleanly deleted remotely and we have no unsynced local changes →
        // the remote delete is authoritative, so remove the local copy.
        await this.deleteLocal(path);
      } else {
        // Deleted remotely but we also have unsynced local edits → conflict.
        await this.resolveConflict(path);
      }
      return;
    }

    // Local absent, remote present.
    if (!localHash && remoteHash) {
      if (base === remoteHash && !initialPull && (liveDelete || !this.pendingDownloads.has(path))) {
        // We deleted it locally → propagate the delete to the index.
        this.publishDelete(path);
      } else if (this.vaultSync.isPendingLocalRemoval?.(path)) {
        // The user removed it before the initial sync finished; that change
        // is about to be published, so do not restore the file meanwhile.
        return;
      } else {
        // New remote file, or remote moved while we had no local copy → pull.
        await this.downloadToDisk(path, remoteHash, localHash);
      }
      return;
    }

    // Both present and different.
    if (localHash && remoteHash && localHash !== remoteHash) {
      if (base === remoteHash) {
        // Clean local edit: remote hasn't moved since our baseline.
        await this.queueLocalUpload(path, remoteHash, localHash);
      } else if (base === localHash) {
        // Local matches the baseline and the remote moved. That is a clean
        // remote update only if the remote version was built on our baseline.
        const remotePrev = remote && "prev" in remote ? (remote.prev ?? null) : undefined;
        if (remotePrev === base) {
          await this.downloadToDisk(path, remoteHash, localHash);
        } else if (remoteHash === this.localSyncState?.get(path)?.replacedIdentity) {
          // The remote is still exactly the version our last publish replaced:
          // that publish never reached the shared index (e.g. the index moved
          // to a new epoch first). Publish it again rather than revert it.
          await this.queueLocalUpload(path, remoteHash, localHash);
        } else if (remotePrev === undefined) {
          // Published by an older client that records no parent: assume, as
          // before, that it was built on ours.
          await this.downloadToDisk(path, remoteHash, localHash);
        } else {
          // Published concurrently with ours, and last-writer-wins kept
          // theirs: both versions are real edits.
          await this.resolveConflict(path);
        }
      } else {
        // Both diverged from the baseline (or no baseline) → conflict.
        await this.resolveConflict(path);
      }
    }
  }

  // --- disk I/O --------------------------------------------------------------

  private async hashDisk(path: string): Promise<string | null | undefined> {
    const file = getFileByPath(this.plugin.app, path);
    if (!file) return null;
    try {
      return await this.readLimiter.run(file.stat?.size ?? 0, async () =>
        sha256Hex(await this.plugin.app.vault.readBinary(file)),
      );
    } catch (e) {
      console.error(`[Realtime] failed to read binary ${path}`, e);
      return undefined;
    }
  }

  private async readDisk(path: string): Promise<ArrayBuffer | null> {
    const file = getFileByPath(this.plugin.app, path);
    if (!file) return null;
    return await this.plugin.app.vault.readBinary(file);
  }

  private async downloadToDisk(
    path: string,
    hash: string,
    expectedLocalHash?: string | null,
  ): Promise<boolean> {
    if (this.paused) {
      this.deferReconcile(path);
      return false;
    }
    this.pendingDownloads.add(path);
    if (isOpenInWorkspace(this.plugin.app, path)) {
      // Don't overwrite a file the user is viewing; retry shortly.
      dbg("binary download deferred (open)", path);
      window.setTimeout(() => void this.reconcile(path), DRAIN_RETRY_MS);
      return false;
    }
    const size = this.binaries.get(path)?.size ?? 0;
    return this.downloadLimiter.run(size, () => this.transferToDisk(path, hash, expectedLocalHash));
  }

  private async transferToDisk(
    path: string,
    hash: string,
    expectedLocalHash?: string | null,
  ): Promise<boolean> {
    if (this.destroyed) return false;
    let bytes: ArrayBuffer;
    try {
      bytes = await this.plugin.auth.getBlob(this.vaultId, path, hash);
    } catch (e) {
      if (this.destroyed) return false;
      console.error(`[Realtime] blob download failed for ${path}`, e);
      this.retryDownloadLater(path);
      return false;
    }
    if (this.destroyed) return false;
    // Never write bytes that are not the blob the index names (e.g. a copy
    // the server stored torn): the disk copy would then be corrupt too.
    if ((await sha256Hex(bytes)) !== hash) {
      if (this.destroyed) return false;
      console.error(`[Realtime] downloaded bytes for ${path} do not match blob ${hash}`);
      this.retryDownloadLater(path);
      return false;
    }
    if (this.paused) {
      this.pendingDownloads.delete(path);
      this.deferReconcile(path);
      return false;
    }
    const currentLocal = await this.hashDisk(path);
    if (this.destroyed) return false;
    if (
      (this.binaries.get(path)?.hash ?? null) !== hash ||
      (expectedLocalHash !== undefined && currentLocal !== expectedLocalHash)
    ) {
      this.pendingDownloads.delete(path);
      void this.reconcile(path);
      return false;
    }
    if (!(await this.writeDisk(path, bytes))) {
      if (!this.destroyed) this.retryDownloadLater(path);
      return false;
    }
    if (this.destroyed) return false;
    const writtenHash = await this.hashDisk(path);
    if (writtenHash !== hash) {
      if (!this.destroyed) {
        console.error(`[Realtime] binary write verification failed for ${path}`);
        this.retryDownloadLater(path);
      }
      return false;
    }
    this.downloadFailures.delete(path);
    this.lastSyncedHash.set(path, hash);
    this.pendingDownloads.delete(path);
    this.localSyncState?.markSynced(path, "binary", hash, hash, true);
    this.urgentPaths.delete(path);
    dbg("binary downloaded", path, hash, bytes.byteLength);
    return true;
  }

  /** Retry a failed download with exponential backoff instead of every 2s. */
  private retryDownloadLater(path: string): void {
    const failures = (this.downloadFailures.get(path) ?? 0) + 1;
    this.downloadFailures.set(path, failures);
    window.setTimeout(() => void this.reconcile(path), retryDelay(failures));
  }

  private async writeDisk(path: string, bytes: ArrayBuffer): Promise<boolean> {
    if (this.destroyed) return false;
    this.writing.add(path);
    try {
      const file = getFileByPath(this.plugin.app, path);
      if (file) {
        if (isOpenInWorkspace(this.plugin.app, path)) return false;
        await this.plugin.app.vault.modifyBinary(file, bytes);
      } else {
        await ensureParentFolder(this.plugin.app, path);
        await this.plugin.app.vault.createBinary(path, bytes);
      }
      return true;
    } catch (e) {
      console.error(`[Realtime] writeDisk failed for ${path}`, e);
      return false;
    } finally {
      // Release on the next tick so the resulting vault event is still ours.
      window.setTimeout(() => this.writing.delete(path), 0);
    }
  }

  private async deleteLocal(path: string): Promise<void> {
    if (this.destroyed) return;
    if (this.paused) {
      this.deferReconcile(path);
      return;
    }
    const file = getFileByPath(this.plugin.app, path);
    if (!file) {
      this.lastSyncedHash.delete(path);
      this.localSyncState?.remove(path);
      return;
    }
    this.writing.add(path);
    try {
      await this.plugin.app.vault.delete(file);
      this.lastSyncedHash.delete(path);
      this.localSyncState?.remove(path);
    } catch (e) {
      console.error(`[Realtime] failed to delete binary ${path}`, e);
      if (!this.destroyed) window.setTimeout(() => void this.reconcile(path), DRAIN_RETRY_MS);
    } finally {
      window.setTimeout(() => this.writing.delete(path), 0);
    }
  }

  // --- index mutations -------------------------------------------------------

  private publishMeta(path: string, meta: BinaryMeta): void {
    this.indexDoc.transact(() => {
      this.binaries.set(path, meta);
    });
    this.lastSyncedHash.set(path, meta.hash);
    this.localSyncState?.markPublished(path, "binary", meta.hash, meta.prev ?? null);
  }

  private publishDelete(path: string): void {
    // Record the deletion in the shared trash before dropping the index entry,
    // so the attachment stays recoverable (its blob is retained until GC).
    const meta = this.binaries.get(path);
    if (meta?.hash) {
      this.vaultSync.recordTrash({ path, kind: "binary", hash: meta.hash, size: meta.size });
    }
    this.indexDoc.transact(() => {
      this.binaries.delete(path);
    });
    this.lastSyncedHash.delete(path);
    this.localSyncState?.remove(path);
  }

  /** True if a path currently has a live binary index entry. */
  hasPath(path: string): boolean {
    return this.binaries.has(path);
  }

  /** True if any live binary entry references this blob hash. */
  hasHash(hash: string): boolean {
    for (const meta of this.binaries.values()) {
      if (meta?.hash === hash) return true;
    }
    return false;
  }

  /**
   * Restore a trashed binary: re-publish its index entry and pull the blob.
   * Resolves false (publishing nothing) when the server no longer has the
   * bytes, e.g. after a storage cleanup reclaimed them.
   */
  async restoreEntry(path: string, hash: string, size: number): Promise<boolean> {
    if (this.destroyed) return false;
    if (!(await this.plugin.auth.blobExists(this.vaultId, path, hash, size))) return false;
    if (this.destroyed) return false;
    this.ignoredPaths.delete(path);
    this.lastSyncedHash.delete(path);
    this.indexDoc.transact(() => {
      this.binaries.set(path, { hash, size, prev: this.binaries.get(path)?.hash ?? null });
    });
    void this.reconcile(path);
    return true;
  }

  // --- upload queue ----------------------------------------------------------

  /**
   * Enqueue the local file at `path` (whose bytes hash to `localHash`) for
   * upload. The bytes are read, and checked against `localHash`, when the
   * transfer starts.
   */
  private async queueLocalUpload(
    path: string,
    expectedRemoteHash: string | null,
    localHash: string,
  ): Promise<boolean> {
    const file = getFileByPath(this.plugin.app, path);
    if (this.destroyed || !file) return false;
    this.enqueueUpload({
      path,
      hash: localHash,
      size: file.stat?.size ?? 0,
      attempts: 0,
      urgent: this.urgentPaths.has(path),
      expectedRemoteHash,
      diskVersion: this.diskVersions.get(path) ?? 0,
    });
    return true;
  }

  private enqueueUpload(job: UploadJob): void {
    // Latest job per path wins.
    this.uploadQueue = this.uploadQueue.filter((j) => j.path !== job.path);
    if (job.urgent) this.uploadQueue.unshift(job);
    else this.uploadQueue.push(job);
    this.refreshUploadStatus();
    this.scheduleDrain();
  }

  /** Notify the plugin when the upload status (idle/pending/uploading) changes. */
  private refreshUploadStatus(): void {
    let status: UploadStatus;
    if (this.destroyed) status = "idle";
    else if (this.activeUpload) status = "uploading";
    else if (this.uploadQueue.length > 0) status = "pending";
    else status = "idle";
    if (status === this.uploadStatus) return;
    this.uploadStatus = status;
    this.plugin.setUploadStatus(status);
  }

  private scheduleDrain(delay = 0): void {
    if (this.paused || this.drainTimer !== null) return;
    this.drainTimer = window.setTimeout(() => {
      this.drainTimer = null;
      void this.drain();
    }, delay);
  }

  private async drain(): Promise<void> {
    if (this.draining || this.destroyed || this.paused) return;
    this.draining = true;
    this.refreshUploadStatus();
    try {
      while (this.uploadQueue.length && !this.destroyed && !this.paused) {
        const index = this.nextUploadIndex();
        if (index < 0) {
          // Only large transfers remain and notes are actively syncing: they
          // stay queued and surface as "pending" rather than "uploading".
          this.scheduleDrain(DRAIN_RETRY_MS);
          break;
        }
        const [job] = this.uploadQueue.splice(index, 1);
        this.activeUpload = true;
        this.refreshUploadStatus();
        try {
          const completed = await this.doUpload(job);
          if (!completed) {
            this.uploadQueue.unshift(job);
            break;
          }
        } catch (e) {
          job.attempts++;
          if (isTransientTransferError(e)) {
            // Offline or the server is down: keep the upload and back off.
            // Giving up here would strand a file added offline until the
            // next restart; a reconnect also retries immediately.
            this.uploadQueue.push(job);
            this.scheduleDrain(retryDelay(job.attempts));
          } else if (job.attempts < MAX_UPLOAD_ATTEMPTS) {
            this.uploadQueue.push(job);
            this.scheduleDrain(DRAIN_RETRY_MS);
          } else {
            console.error(`[Realtime] giving up uploading ${job.path}`, e);
            new Notice(`Realtime: failed to upload "${job.path}".`);
          }
          break;
        } finally {
          this.activeUpload = false;
          this.refreshUploadStatus();
        }
      }
    } finally {
      this.draining = false;
      this.refreshUploadStatus();
      if (!this.paused && this.uploadQueue.length > 0) this.scheduleDrain();
    }
  }

  /**
   * Index of the next job to upload, or -1 when every queued job is a large
   * transfer being held back while notes sync. A held-back job never blocks
   * the smaller ones behind it, and waits at most MAX_LARGE_UPLOAD_DEFERRAL_MS
   * so continuous note activity cannot starve it.
   */
  private nextUploadIndex(): number {
    const busy = this.vaultSync.isTextSyncBusy();
    const now = Date.now();
    return this.uploadQueue.findIndex((job) => {
      if (job.urgent || job.size < LARGE_FILE_BYTES || !busy) return true;
      job.deferredSince ??= now;
      return now - job.deferredSince >= MAX_LARGE_UPLOAD_DEFERRAL_MS;
    });
  }

  private async doUpload(job: UploadJob): Promise<boolean> {
    // Read the bytes only now: a queued job holds none.
    const bytes = await this.readDisk(job.path);
    if (this.destroyed) return true;
    if (this.paused) return false;
    if (!bytes || (await sha256Hex(bytes)) !== job.hash) {
      // The file changed or vanished since it was queued; reconcile anew.
      void this.reconcile(job.path);
      return true;
    }
    // With the size, a stored copy of the wrong length (a torn write) counts
    // as missing, so this upload repairs it.
    const size = bytes.byteLength;
    const exists = await this.plugin.auth.blobExists(this.vaultId, job.path, job.hash, size);
    if (this.destroyed) return true;
    if (this.paused) return false;
    if (!exists) {
      await this.plugin.auth.putBlob(this.vaultId, job.path, job.hash, bytes);
    }
    if (this.destroyed) return true;
    if (this.paused) return false;
    if ((this.diskVersions.get(job.path) ?? 0) !== job.diskVersion) {
      void this.reconcile(job.path);
      return true;
    }
    if (
      job.expectedRemoteHash !== undefined &&
      (this.binaries.get(job.path)?.hash ?? null) !== job.expectedRemoteHash
    ) {
      void this.reconcile(job.path);
      return true;
    }
    // Publish only now that the bytes are on the server.
    this.publishMeta(job.path, {
      hash: job.hash,
      size,
      prev: job.expectedRemoteHash ?? null,
    });
    this.urgentPaths.delete(job.path);
    dbg("binary uploaded+published", job.path, job.hash, job.size);
    return true;
  }

  private bumpDiskVersion(path: string): void {
    this.diskVersions.set(path, (this.diskVersions.get(path) ?? 0) + 1);
  }

  // --- conflicts -------------------------------------------------------------

  /**
   * Queue a keep-local / keep-remote modal (serialized). `localHash` is the
   * current disk hash; `remoteHash` is null when the remote side deleted the file.
   */
  private resolveConflict(path: string): Promise<void> {
    const run = this.conflictChain.then(async () => {
      while (!this.destroyed) {
        if (this.paused) {
          this.deferReconcile(path);
          return;
        }
        // Re-check state at prompt time — it may have converged while queued.
        const nowLocal = await this.hashDisk(path);
        if (nowLocal === undefined) return;
        if (this.paused) {
          this.deferReconcile(path);
          return;
        }
        const nowRemote = this.binaries.get(path)?.hash ?? null;
        if (nowLocal === nowRemote) {
          if (nowRemote) this.lastSyncedHash.set(path, nowRemote);
          return;
        }
        const choice = await openBinaryConflictModal(this.plugin, {
          path,
          remoteDeleted: nowRemote === null,
        });
        if (this.destroyed) return;
        if (this.paused) {
          this.deferReconcile(path);
          return;
        }
        if (!(await this.conflictStateMatches(path, nowLocal, nowRemote))) continue;
        if (await this.applyConflictChoice(path, choice, nowLocal, nowRemote)) return;
      }
    });
    this.conflictChain = run.catch((e) => {
      console.error(`[Realtime] conflict resolution failed for ${path}`, e);
      if (!this.destroyed) window.setTimeout(() => void this.reconcile(path), DRAIN_RETRY_MS);
    });
    return this.conflictChain;
  }

  private async applyConflictChoice(
    path: string,
    choice: ConflictChoice,
    localHash: string | null,
    remoteHash: string | null,
  ): Promise<boolean> {
    if (choice === "local") {
      if (remoteHash) {
        const remoteBytes = await this.plugin.auth.getBlob(this.vaultId, path, remoteHash);
        if (!(await this.conflictStateMatches(path, localHash, remoteHash))) return false;
        const preservedPath = await preserveBinaryConflict(
          this.plugin,
          path,
          remoteBytes,
          "remote",
        );
        if (!(await this.conflictStateMatches(path, localHash, remoteHash))) return false;
        new Notice(`Realtime: preserved the remote copy as "${preservedPath}".`);
      }
      if (localHash) {
        const queued = await this.queueLocalUpload(path, remoteHash, localHash);
        if (!queued) return false;
      }
      new Notice(`Realtime: kept your local copy of "${path}".`);
      return true;
    } else {
      if (localHash) {
        const localBytes = await this.readDisk(path);
        if (!localBytes) throw new Error(`Local conflict content disappeared for "${path}".`);
        if ((await sha256Hex(localBytes)) !== localHash) return false;
        if (!(await this.conflictStateMatches(path, localHash, remoteHash))) return false;
        const preservedPath = await preserveBinaryConflict(this.plugin, path, localBytes, "local");
        if (!(await this.conflictStateMatches(path, localHash, remoteHash))) return false;
        new Notice(`Realtime: preserved your local copy as "${preservedPath}".`);
      }
      if (remoteHash === null) {
        // Remote deleted, user keeps remote → delete local.
        await this.deleteLocal(path);
      } else {
        if (!(await this.downloadToDisk(path, remoteHash, localHash))) return false;
      }
      new Notice(`Realtime: replaced "${path}" with the remote copy.`);
      return true;
    }
  }

  private async conflictStateMatches(
    path: string,
    localHash: string | null,
    remoteHash: string | null,
  ): Promise<boolean> {
    const currentLocal = await this.hashDisk(path);
    return (
      !this.destroyed &&
      currentLocal !== undefined &&
      currentLocal === localHash &&
      (this.binaries.get(path)?.hash ?? null) === remoteHash
    );
  }

  // --- lifecycle -------------------------------------------------------------

  private deferReconcile(path: string, initialPull = this.pullingMissingRemote): void {
    this.deferredReconciles.add(path);
    if (initialPull) this.deferredInitialPulls.add(path);
  }

  /**
   * Connectivity is back: retry queued uploads and failed downloads now
   * instead of waiting out their backoff.
   */
  retryNow(): void {
    if (this.destroyed || this.paused) return;
    if (this.drainTimer !== null) {
      window.clearTimeout(this.drainTimer);
      this.drainTimer = null;
    }
    if (this.uploadQueue.length > 0) this.scheduleDrain();
    const failed = [...this.downloadFailures.keys()];
    this.downloadFailures.clear();
    for (const path of failed) void this.reconcile(path);
  }

  setPaused(paused: boolean): void {
    if (this.destroyed || this.paused === paused) return;
    this.paused = paused;
    if (paused && this.drainTimer !== null) {
      window.clearTimeout(this.drainTimer);
      this.drainTimer = null;
    } else if (!paused) {
      if (this.uploadQueue.length > 0) this.scheduleDrain();
      const deferred = [...this.deferredReconciles];
      this.deferredReconciles.clear();
      for (const path of deferred) void this.reconcile(path);
    }
  }

  destroy(): void {
    if (this.destroyed) return;
    this.destroyed = true;
    this.binaries.unobserve(this.observer);
    if (this.drainTimer !== null) {
      window.clearTimeout(this.drainTimer);
      this.drainTimer = null;
    }
    this.uploadQueue = [];
    this.chains.clear();
    this.pendingReconcile.clear();
    this.deferredReconciles.clear();
    this.deferredInitialPulls.clear();
    this.pendingDownloads.clear();
    this.urgentPaths.clear();
    this.refreshUploadStatus();
  }
}
