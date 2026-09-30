import * as Y from "yjs";
import { Notice, normalizePath, TFile } from "obsidian";
import type RealtimePlugin from "./main";
import { SyncedDoc, type DocumentBootstrapOptions } from "./SyncedDoc";
import { ensureParentFolder, getFileByPath, isOpenInWorkspace } from "./vaultHelpers";
import {
  jsonContains,
  mergeStructuredStartupResult,
  reconcileInto,
  toValue,
  type JsonValue,
} from "./structured/reconcile";
import { preserveTextConflict } from "./conflictRecovery";
import { sha256Text } from "./hash";
import { getDocumentEpoch } from "./documentEpoch";

export const DISK_ORIGIN = Symbol("realtime-structured-disk");
const DISK_WRITE_RETRY_MS = 2_000;

export abstract class StructuredDocument extends SyncedDoc {
  readonly root: Y.Map<any>;
  private rootObserver: (events: Array<Y.YEvent<any>>, txn: Y.Transaction) => void;
  /** Serialized value currently being written, used to identify our own echo. */
  private writingTextToDisk: string | null = null;
  /**
   * Serialized file content this device last wrote or read. A disk read that
   * still matches it (e.g. the late event of an older write) is not an edit.
   */
  private diskKnownText: string | null = null;
  /** Disk writes run one at a time; a request during one coalesces behind it. */
  private writeRunning: Promise<boolean> | null = null;
  private writeQueued: Promise<boolean> | null = null;
  private writeTimer: number | null = null;
  private startupReconciled = false;
  /** True only after startup content is durably present on this device. */
  private startupReady = false;
  private startupReconciling = false;
  private forceBootstrapConflict: boolean;
  private readonly staleLocalFingerprint: string | null;
  private readonly retiredBaselineText: string | null;
  private baselineAtStartup: JsonValue = {};
  private baselineTextAtStartup = "";
  private diskAtStartup: JsonValue | null = null;
  private localChangedAtStartup = false;
  /** True when the on-disk file exists but could not be parsed. */
  private diskParseFailed = false;
  /** Serialized remote version already preserved by the startup reconcile. */
  private preservedStartupRemote: string | null = null;

  protected constructor(
    plugin: RealtimePlugin,
    path: string,
    guid: string,
    serverDocId: string,
    isCreator: boolean,
    opts: DocumentBootstrapOptions = {},
  ) {
    super(plugin, path, guid, serverDocId, isCreator, opts);
    this.forceBootstrapConflict = opts.forceBootstrapConflict ?? false;
    this.staleLocalFingerprint = opts.staleLocalFingerprint ?? null;
    this.retiredBaselineText = opts.retiredBaseline ?? null;
    this.root = this.ydoc.getMap("root");
    this.rootObserver = (_events, txn) => this.onRootChanged(txn?.origin);
    this.root.observeDeep(this.rootObserver);
  }

  get value(): JsonValue {
    return toValue(this.root);
  }

  protected abstract parse(text: string): JsonValue;
  protected abstract serialize(value: JsonValue): string;

  protected serializeRecoveryContent(): string {
    return this.serialize(this.value);
  }

  /**
   * When the file is open in the workspace, should we defer to a live editor
   * binding instead of writing through to disk? Canvas overrides this to `true`
   * because {@link CanvasBinding} owns the live view while it's open; writing to
   * an open file would surface as an external change and thrash the view.
   *
   * Document types without a live binding (e.g. Bases) must return `false` so the
   * disk write-through path stays active — otherwise they never sync while open,
   * which is exactly when they're being edited.
   */
  protected shouldDeferToLiveBinding(): boolean {
    return false;
  }

  /** True only when an open file should suppress disk write-through. */
  private suppressedWhileOpen(): boolean {
    return this.isOpen() && this.shouldDeferToLiveBinding();
  }

  protected async afterPersistenceSynced(): Promise<void> {
    // A new epoch starts from an empty store; the content it replaced is
    // then the baseline local and remote changes are measured from.
    this.baselineAtStartup =
      !this.loadedStoredState && this.retiredBaselineText !== null
        ? this.parse(this.retiredBaselineText)
        : this.value;
    this.baselineTextAtStartup = this.serialize(this.baselineAtStartup);
    const disk = await this.readParsedFromDisk();
    let stale = false;
    let unchangedSinceSync = false;
    const diskText = disk === null ? null : this.serialize(disk);
    if (diskText !== null) {
      const fingerprint = await sha256Text(diskText);
      stale = this.staleLocalFingerprint !== null && fingerprint === this.staleLocalFingerprint;
      // The file still holds content this document's store contains: it
      // merely lags the document, so it is not a local edit.
      unchangedSinceSync = await this.storeContainsDiskContent(diskText);
    }
    this.diskAtStartup = disk;
    this.diskKnownText = diskText;
    // An untouched copy of a file deleted remotely (and since re-created at
    // this path) is not a local change: let the new document replace it.
    if (stale) this.forceBootstrapConflict = false;
    this.localChangedAtStartup =
      diskText !== null && !stale && !unchangedSinceSync && diskText !== this.baselineTextAtStartup;
  }

  protected async finishStartupReconcile(): Promise<void> {
    if (this.startupReady || this.startupReconciling || this.destroyed) return;
    this.startupReconciling = true;
    let completed = false;
    try {
      if (!this.startupReconciled) {
        const remote = this.value;
        const disk = this.diskAtStartup;
        if (disk !== null && (this.localChangedAtStartup || this.forceBootstrapConflict)) {
          let merge = this.startupMerge(disk, remote);
          if (merge.conflicted) {
            // One recovery copy per remote version: a remote that keeps
            // changing while this retries must not leave a trail of copies.
            const remoteText = this.serialize(remote);
            if (remoteText !== this.preservedStartupRemote) {
              const preservedPath = await preserveTextConflict(
                this.plugin,
                this.path,
                remoteText,
                "remote",
              );
              this.preservedStartupRemote = remoteText;
              new Notice(
                `Realtime: merged "${this.path}" and preserved the conflicting remote version as "${preservedPath}".`,
              );
            }
            const latestDisk = await this.readParsedFromDisk();
            if (this.destroyed) return;
            if (latestDisk === null || this.serialize(latestDisk) !== this.serialize(disk)) {
              this.diskAtStartup = latestDisk;
              return;
            }
            // The remote may have moved on while the copy was written. Merge
            // against it now, with no await before applying, rather than
            // applying a merge that would revert those changes.
            const current = this.value;
            if (this.serialize(current) !== remoteText) merge = this.startupMerge(disk, current);
          }
          if (this.destroyed) return;
          this.applyValue(merge.value, DISK_ORIGIN);
        }
        this.startupReconciled = true;
      }

      let materialized = this.getFile() !== null;
      if (!this.suppressedWhileOpen() && !this.diskParseFailed) {
        materialized = await this.writeToDisk();
      } else if (materialized) {
        this.plugin.vaultSync?.noteMaterialized(
          this.path,
          this.path.endsWith(".canvas") ? "canvas" : "base",
          this.guid,
        );
      }
      if (!materialized || this.destroyed) return;

      this.startupReady = true;
      if (!this.provider.hasLocalChanges) await this.recordAcknowledgedContent(true);
      completed = true;
    } catch (e) {
      console.error(`[Realtime] structured startup reconcile failed for ${this.path}`, e);
    } finally {
      this.startupReconciling = false;
      if (completed) {
        this.resolveWhenReady();
      } else if (!this.destroyed) {
        window.setTimeout(() => void this.finishStartupReconcile(), 2_000);
      }
    }
  }

  /** The startup merge of local `disk` with `remote`, and whether they conflict. */
  private startupMerge(
    disk: JsonValue,
    remote: JsonValue,
  ): { value: JsonValue; conflicted: boolean } {
    return (
      this.mergeWithoutSharedBaseline(disk, remote) ??
      (this.forceBootstrapConflict
        ? { value: disk, conflicted: this.serialize(disk) !== this.serialize(remote) }
        : mergeStructuredStartupResult(this.baselineAtStartup, disk, remote))
    );
  }

  /**
   * Without a shared baseline (an unrelated local file, or the same file
   * created independently on two devices, e.g. an empty canvas from a plugin),
   * take whichever side already contains the other instead of letting the
   * local copy overwrite the remote. The local superset is not taken after an
   * epoch rollover, where the remote may have removed content on purpose.
   */
  private mergeWithoutSharedBaseline(
    disk: JsonValue,
    remote: JsonValue,
  ): { value: JsonValue; conflicted: boolean } | null {
    const baselineEmpty = this.baselineTextAtStartup === this.serialize(this.parse(""));
    if (!this.forceBootstrapConflict && !baselineEmpty) return null;
    // Compare file-level shapes; the CRDT value also carries internal state
    // (e.g. canvas tombstone maps) that never reaches disk.
    const diskShape = this.parse(this.serialize(disk));
    const remoteShape = this.parse(this.serialize(remote));
    if (jsonContains(remoteShape, diskShape)) return { value: remote, conflicted: false };
    if (
      getDocumentEpoch(this.plugin, this.serverDocId) === 0 &&
      jsonContains(diskShape, remoteShape)
    ) {
      return { value: disk, conflicted: false };
    }
    return null;
  }

  protected async afterChangesSynced(): Promise<void> {
    if (!this.startupReady || this.destroyed) return;
    await this.recordAcknowledgedContent(true);
  }

  private async recordAcknowledgedContent(reconciled = false): Promise<void> {
    const content = this.serialize(this.value);
    const fingerprint = await sha256Text(content);
    if (
      this.destroyed ||
      this.provider.hasLocalChanges ||
      this.serialize(this.value) !== content ||
      !this.startupReady
    ) {
      return;
    }
    this.plugin.vaultSync?.noteContentAcknowledged(
      this.path,
      this.path.endsWith(".canvas") ? "canvas" : "base",
      this.guid,
      fingerprint,
      reconciled,
    );
  }

  /**
   * Called by VaultSync when the file changed on disk. Our own writes (and
   * late events for older ones) are recognised by content, not timing; any
   * other content is a real edit, merged with changes the document gained
   * since the file last matched it.
   */
  async onDiskChanged(): Promise<void> {
    if (this.destroyed) return;
    const disk = await this.readParsedFromDisk();
    if (disk === null) return;
    if (this.destroyed) return;
    const serialized = this.serialize(disk);
    if (this.suppressedWhileOpen()) {
      // The live view owns the file while bound, and the binding keeps it in
      // step with the document: its saves are not edits to fold.
      if (serialized === this.serialize(this.value)) this.noteDiskContent(serialized);
      else this.diskKnownText = serialized;
      return;
    }
    if (serialized === this.diskKnownText || serialized === this.writingTextToDisk) return;
    if (serialized === this.serialize(this.value)) {
      this.noteDiskContent(serialized);
      return;
    }
    await this.foldDiskEdit(disk);
  }

  /**
   * Fold a disk edit into the document, merging it against the content the
   * file last matched so changes the document gained since (typically remote
   * edits whose disk write was pending) survive.
   */
  private async foldDiskEdit(disk: JsonValue): Promise<void> {
    this.plugin.vaultSync?.noteTextActivity();
    const diskText = this.serialize(disk);
    // Compare file-level shapes: the CRDT value also carries internal state
    // (e.g. canvas tombstone maps) that never reaches disk.
    const sharedText = this.serialize(this.value);
    const shared = this.parse(sharedText);
    const base = this.diskKnownText === null ? null : this.parse(this.diskKnownText);
    let next: JsonValue = disk;
    if (base !== null) {
      const merge = mergeStructuredStartupResult(base, disk, shared);
      next = merge.value;
      if (merge.conflicted && sharedText !== this.diskKnownText) {
        const preservedPath = await preserveTextConflict(
          this.plugin,
          this.path,
          sharedText,
          "remote",
        );
        if (this.destroyed) return;
        new Notice(
          `Realtime: "${this.path}" was edited on disk while newer changes were arriving; ` +
            `merged them and preserved the other version as "${preservedPath}".`,
        );
      }
    }
    const latest = await this.readParsedFromDisk();
    if (this.destroyed || latest === null || this.serialize(latest) !== diskText) return;
    this.applyValue(next, DISK_ORIGIN);
    this.noteDiskContent(diskText);
  }

  /** The file is known to hold `text`, which this document now contains. */
  private noteDiskContent(text: string): void {
    this.diskKnownText = text;
    if (this.provider.clientToken?.authorization === "read-only") return;
    void this.recordStoredDiskContent(this.materializedKind(), text);
  }

  private materializedKind(): "canvas" | "base" {
    return this.path.endsWith(".canvas") ? "canvas" : "base";
  }

  protected applyValue(value: JsonValue, origin: unknown = DISK_ORIGIN): void {
    if (this.destroyed) return;
    this.ydoc.transact(() => reconcileInto(this.root, value), origin);
  }

  protected onRootChanged(_origin?: unknown): void {
    if (this.destroyed || !this.startupReady) return;
    this.plugin.vaultSync?.noteTextActivity();
    if (this.suppressedWhileOpen()) return;
    this.scheduleWriteToDisk();
  }

  private scheduleWriteToDisk(delayMs = 100): void {
    if (this.writeTimer !== null) window.clearTimeout(this.writeTimer);
    this.writeTimer = window.setTimeout(() => {
      this.writeTimer = null;
      if (this.destroyed || !this.startupReady || this.suppressedWhileOpen()) return;
      void this.writeToDisk();
    }, delayMs);
  }

  protected getFile(): TFile | null {
    return getFileByPath(this.plugin.app, this.path);
  }

  protected isOpen(): boolean {
    return isOpenInWorkspace(this.plugin.app, this.path);
  }

  protected canHibernateLocally(): boolean {
    return (
      !this.isOpen() &&
      this.writeRunning === null &&
      this.writeQueued === null &&
      this.writeTimer === null
    );
  }

  private async readParsedFromDisk(): Promise<JsonValue | null> {
    const file = this.getFile();
    if (!file) {
      this.diskParseFailed = false;
      return null;
    }
    try {
      const parsed = this.parse(await this.plugin.app.vault.read(file));
      this.diskParseFailed = false;
      return parsed;
    } catch (e) {
      console.error(`[Realtime] failed to parse ${this.path}`, e);
      new Notice(`Realtime: could not parse ${this.path}; keeping the last synced version.`);
      this.diskParseFailed = true;
      return null;
    }
  }

  /**
   * Write the current value to disk. Writes never overlap: a request made
   * while one is running waits for it and then writes whatever the value is
   * by then, so an older write can never land after a newer one.
   */
  protected writeToDisk(): Promise<boolean> {
    if (this.writeQueued) return this.writeQueued;
    const running = this.writeRunning;
    if (!running) return this.startDiskWrite();
    const queued = running.then(() => {
      this.writeQueued = null;
      return this.startDiskWrite();
    });
    this.writeQueued = queued;
    return queued;
  }

  private startDiskWrite(): Promise<boolean> {
    const running: Promise<boolean> = this.writeCurrentValue().finally(() => {
      if (this.writeRunning === running) this.writeRunning = null;
    });
    this.writeRunning = running;
    return running;
  }

  private async writeCurrentValue(): Promise<boolean> {
    if (this.destroyed) return false;
    const text = this.serialize(this.value);
    this.writingTextToDisk = text;
    try {
      const file = this.getFile();
      if (file) {
        if (this.suppressedWhileOpen()) {
          this.plugin.vaultSync?.noteMaterialized(this.path, this.materializedKind(), this.guid);
          return true;
        }
        const raw = await this.plugin.app.vault.read(file);
        // Re-check destroyed after the await: a doc replaced mid-write (rename,
        // guid change) must not clobber the file its successor now owns.
        if (this.destroyed) return false;
        if (raw === text) {
          this.noteDiskContent(text);
          this.plugin.vaultSync?.noteMaterialized(this.path, this.materializedKind(), this.guid);
          if (this.startupReady && !this.provider.hasLocalChanges) {
            await this.recordAcknowledgedContent(true);
          }
          return true;
        }
        if (this.suppressedWhileOpen()) {
          this.plugin.vaultSync?.noteMaterialized(this.path, this.materializedKind(), this.guid);
          return true;
        }
        if (this.serialize(this.value) !== text) return false;
        if (this.diskKnownText !== null && !this.diskParseFailed) {
          let current: JsonValue | null = null;
          try {
            current = this.parse(raw);
          } catch {
            current = null;
          }
          if (current !== null && this.serialize(current) !== this.diskKnownText) {
            // Someone edited the file and its modify event has not been
            // handled yet: fold that edit in instead of overwriting it.
            await this.foldDiskEdit(current);
            return false;
          }
        }
        const releaseWrite = this.plugin.vaultSync?.beginOwnWrite(file.path);
        try {
          await this.plugin.app.vault.modify(file, text);
        } finally {
          releaseWrite?.();
        }
      } else {
        const path = normalizePath(this.path);
        // Removed by the user before the initial sync finished: publish
        // that rather than bring the file back.
        if (this.plugin.vaultSync?.isPendingLocalRemoval(path)) return false;
        await ensureParentFolder(this.plugin.app, path);
        if (this.destroyed) return false;
        if (this.serialize(this.value) !== text) return false;
        const releaseWrite = this.plugin.vaultSync?.beginOwnWrite(path);
        try {
          await this.plugin.app.vault.create(path, text);
        } finally {
          releaseWrite?.();
        }
      }
      if (this.destroyed) return false;
      this.noteDiskContent(text);
      this.plugin.vaultSync?.noteMaterialized(this.path, this.materializedKind(), this.guid);
      if (this.startupReady && !this.provider.hasLocalChanges) {
        await this.recordAcknowledgedContent(true);
      }
      return true;
    } catch (e) {
      console.error(`[Realtime] structured writeToDisk failed for ${this.path}`, e);
      if (!this.destroyed) {
        const file = this.getFile();
        if (file) {
          try {
            this.diskKnownText = this.serialize(this.parse(await this.plugin.app.vault.read(file)));
          } catch {
            // Keep the previous belief about the file.
          }
        }
        this.scheduleWriteToDisk(DISK_WRITE_RETRY_MS);
      }
      return false;
    } finally {
      this.writingTextToDisk = null;
    }
  }

  protected destroySubclass(): void {
    if (this.writeTimer !== null) {
      window.clearTimeout(this.writeTimer);
      this.writeTimer = null;
    }
    this.root.unobserveDeep(this.rootObserver);
  }
}
