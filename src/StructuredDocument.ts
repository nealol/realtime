import * as Y from "yjs";
import { Notice, normalizePath, TFile } from "obsidian";
import type RealtimePlugin from "./main";
import { SyncedDoc } from "./SyncedDoc";
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

export abstract class StructuredDocument extends SyncedDoc {
  readonly root: Y.Map<any>;
  private rootObserver: (events: Array<Y.YEvent<any>>, txn: Y.Transaction) => void;
  /** Serialized value currently being written, used to identify our own echo. */
  private writingTextToDisk: string | null = null;
  private writeTimer: number | null = null;
  private startupReconciled = false;
  /** True only after startup content is durably present on this device. */
  private startupReady = false;
  private startupReconciling = false;
  private readonly forceBootstrapConflict: boolean;
  private baselineAtStartup: JsonValue = {};
  private baselineTextAtStartup = "";
  private diskAtStartup: JsonValue | null = null;
  private localChangedAtStartup = false;
  /** True when the on-disk file exists but could not be parsed. */
  private diskParseFailed = false;

  protected constructor(
    plugin: RealtimePlugin,
    path: string,
    guid: string,
    serverDocId: string,
    isCreator: boolean,
    opts: { autoConnect?: boolean; forceBootstrapConflict?: boolean } = {},
  ) {
    super(plugin, path, guid, serverDocId, isCreator, opts);
    this.forceBootstrapConflict = opts.forceBootstrapConflict ?? false;
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
    this.baselineAtStartup = this.value;
    this.baselineTextAtStartup = this.serialize(this.baselineAtStartup);
    const disk = await this.readParsedFromDisk();
    this.diskAtStartup = disk;
    this.localChangedAtStartup =
      disk !== null && this.serialize(disk) !== this.baselineTextAtStartup;
  }

  protected async finishStartupReconcile(): Promise<void> {
    if (this.startupReady || this.startupReconciling || this.destroyed) return;
    this.startupReconciling = true;
    let completed = false;
    try {
      if (!this.startupReconciled) {
        const remote = this.value;
        if (
          this.diskAtStartup !== null &&
          (this.localChangedAtStartup || this.forceBootstrapConflict)
        ) {
          const merge =
            this.mergeWithoutSharedBaseline(this.diskAtStartup, remote) ??
            (this.forceBootstrapConflict
              ? {
                  value: this.diskAtStartup,
                  conflicted: this.serialize(this.diskAtStartup) !== this.serialize(remote),
                }
              : mergeStructuredStartupResult(this.baselineAtStartup, this.diskAtStartup, remote));
          if (merge.conflicted) {
            const preservedPath = await preserveTextConflict(
              this.plugin,
              this.path,
              this.serialize(remote),
              "remote",
            );
            new Notice(
              `Realtime: merged "${this.path}" and preserved the conflicting remote version as "${preservedPath}".`,
            );
            const latestDisk = await this.readParsedFromDisk();
            if (this.destroyed) return;
            if (
              latestDisk === null ||
              this.serialize(latestDisk) !== this.serialize(this.diskAtStartup) ||
              this.serialize(this.value) !== this.serialize(remote)
            ) {
              this.diskAtStartup = latestDisk;
              return;
            }
          }
          if (this.destroyed) return;
          this.applyValue(merge.value, DISK_ORIGIN);
        }
        this.startupReconciled = true;
      }

      let materialized = this.getFile() !== null;
      if (!this.suppressedWhileOpen() && !this.diskParseFailed) {
        materialized = await this.writeToDisk(this.serialize(this.value));
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

  async onDiskChanged(): Promise<void> {
    if (this.destroyed || this.suppressedWhileOpen()) return;
    const disk = await this.readParsedFromDisk();
    if (disk === null) return;
    if (this.destroyed) return;
    const serialized = this.serialize(disk);
    if (serialized === this.writingTextToDisk || serialized === this.serialize(this.value)) return;
    this.plugin.vaultSync?.noteTextActivity();
    this.applyValue(disk, DISK_ORIGIN);
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

  private scheduleWriteToDisk(): void {
    if (this.writeTimer !== null) window.clearTimeout(this.writeTimer);
    this.writeTimer = window.setTimeout(() => {
      this.writeTimer = null;
      if (this.destroyed || !this.startupReady || this.suppressedWhileOpen()) return;
      void this.writeToDisk(this.serialize(this.value));
    }, 100);
  }

  protected getFile(): TFile | null {
    return getFileByPath(this.plugin.app, this.path);
  }

  protected isOpen(): boolean {
    return isOpenInWorkspace(this.plugin.app, this.path);
  }

  protected canHibernateLocally(): boolean {
    return !this.isOpen() && this.writingTextToDisk === null && this.writeTimer === null;
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

  protected async writeToDisk(text: string): Promise<boolean> {
    if (this.destroyed) return false;
    this.writingTextToDisk = text;
    try {
      const file = this.getFile();
      if (file) {
        if (this.suppressedWhileOpen()) {
          this.plugin.vaultSync?.noteMaterialized(
            this.path,
            this.path.endsWith(".canvas") ? "canvas" : "base",
            this.guid,
          );
          return true;
        }
        if ((await this.plugin.app.vault.read(file)) === text) {
          this.plugin.vaultSync?.noteMaterialized(
            this.path,
            this.path.endsWith(".canvas") ? "canvas" : "base",
            this.guid,
          );
          if (this.startupReady && !this.provider.hasLocalChanges) {
            await this.recordAcknowledgedContent(true);
          }
          return true;
        }
        // Re-check destroyed after the await: a doc replaced mid-write (rename,
        // guid change) must not clobber the file its successor now owns.
        if (this.destroyed) return false;
        if (this.suppressedWhileOpen()) {
          this.plugin.vaultSync?.noteMaterialized(
            this.path,
            this.path.endsWith(".canvas") ? "canvas" : "base",
            this.guid,
          );
          return true;
        }
        if (this.serialize(this.value) !== text) return false;
        await this.plugin.app.vault.modify(file, text);
      } else {
        const path = normalizePath(this.path);
        await ensureParentFolder(this.plugin.app, path);
        if (this.destroyed) return false;
        if (this.serialize(this.value) !== text) return false;
        await this.plugin.app.vault.create(path, text);
      }
      this.plugin.vaultSync?.noteMaterialized(
        this.path,
        this.path.endsWith(".canvas") ? "canvas" : "base",
        this.guid,
      );
      if (this.startupReady && !this.provider.hasLocalChanges) {
        await this.recordAcknowledgedContent(true);
      }
      return true;
    } catch (e) {
      console.error(`[Realtime] structured writeToDisk failed for ${this.path}`, e);
      return false;
    } finally {
      window.setTimeout(() => {
        this.writingTextToDisk = null;
      }, 250);
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
