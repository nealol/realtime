import * as Y from "yjs";
import { Notice } from "obsidian";
import type RealtimePlugin from "./main";
import { sha256Hex } from "./hash";
import {
  categoryForVaultPath,
  categoryRequiresReload,
  type ConfigCategoryId,
} from "./configCategories";
import { dbg } from "./debug";
import type { LocalSyncState } from "./localSyncState";
import { isConflictCopy, preserveAdapterConflict } from "./conflictRecovery";
import { mergeStructuredStartupResult, type JsonValue } from "./structured/reconcile";

export interface ConfigMeta {
  hash: string;
  size: number;
  mtime: number;
}

export type ConfigReconcileAction =
  | "none"
  | "upload"
  | "download"
  | "deleteLocal"
  | "deleteRemote"
  | "merge";

/**
 * Three-way reconcile decision, mirroring Obsidian Sync's documented behavior
 * for settings files: deletes propagate within a profile, and a true conflict
 * on a JSON settings file merges the objects with local keys applied on top
 * of remote keys (`canMerge`); non-JSON conflicts fall back to newest-wins.
 *
 * `initialPull` guards the delete-propagation branch during the first pass
 * after start(): baselines are seeded from the shared map before any local
 * download has happened, so "local missing + baseline matches remote" would
 * otherwise publish a bogus remote delete from a device that simply never
 * pulled the file (or just enabled its category).
 */
export function decideConfigReconcile(
  local: ConfigMeta | null,
  remote: ConfigMeta | null,
  base: string | null,
  opts: { initialPull?: boolean; canMerge?: boolean } = {},
): ConfigReconcileAction {
  const localHash = local?.hash ?? null;
  const remoteHash = remote?.hash ?? null;

  if (localHash === remoteHash) return "none";

  if (local && !remote) {
    if (base === null) return "upload";
    if (base === local.hash) return "deleteLocal";
    return "upload";
  }

  if (!local && remote) {
    if (base === remote.hash && !opts.initialPull) return "deleteRemote";
    return "download";
  }

  if (local && remote) {
    // Fresh device (no baseline): the profile's remote settings win, so a
    // newly joining device never clobbers the profile with its local defaults.
    if (base === null) return "download";
    if (base === remote.hash) return "upload";
    if (base === local.hash) return "download";
    if (opts.canMerge) return "merge";
    return local.mtime >= remote.mtime ? "upload" : "download";
  }

  return "none";
}

/**
 * Shallow-merge two JSON settings documents the way Obsidian Sync documents
 * it: "JSON objects are merged; local keys are applied on top of remote
 * keys". Returns `null` when either side is not a plain JSON object, in
 * which case the caller falls back to newest-wins.
 */
export function mergeJsonSettings(remoteText: string, localText: string): string | null {
  let remote: unknown;
  let local: unknown;
  try {
    remote = JSON.parse(remoteText);
    local = JSON.parse(localText);
  } catch {
    return null;
  }
  if (!isPlainObject(remote) || !isPlainObject(local)) return null;
  return JSON.stringify({ ...remote, ...local }, null, 2);
}

/**
 * Three-way merge of JSON settings against the last synced version. Plugins
 * and the workspace rewrite their JSON files on every device, so both sides
 * routinely diverge on disjoint keys; those merge without a recovery copy.
 * Returns `null` when any side is not a plain JSON object.
 */
export function mergeJsonSettingsThreeWay(
  baseText: string,
  remoteText: string,
  localText: string,
): { text: string; conflicted: boolean } | null {
  let base: unknown;
  let remote: unknown;
  let local: unknown;
  try {
    base = JSON.parse(baseText);
    remote = JSON.parse(remoteText);
    local = JSON.parse(localText);
  } catch {
    return null;
  }
  if (!isPlainObject(base) || !isPlainObject(remote) || !isPlainObject(local)) return null;
  const merged = mergeStructuredStartupResult(
    base as JsonValue,
    local as JsonValue,
    remote as JsonValue,
  );
  return { text: JSON.stringify(merged.value, null, 2), conflicted: merged.conflicted };
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

const POLL_MS = 15_000;
const RETRY_MS = 2_000;
const RELOAD_NOTICE_DEBOUNCE_MS = 2_000;

export class ConfigSync {
  private plugin: RealtimePlugin;
  private indexDoc: Y.Doc;
  private configFiles: Y.Map<ConfigMeta>;
  private localSyncState?: LocalSyncState;
  private enabledCategories = new Set<ConfigCategoryId>();
  private lastSyncedHash = new Map<string, string>();
  private chains = new Map<string, Promise<boolean>>();
  private writing = new Set<string>();
  private pollTimer: number | null = null;
  private started = false;
  private paused = false;
  private pauseGeneration = 0;
  private destroyed = false;
  /**
   * True while the reconcile pass triggered by start() runs. Baselines seeded
   * from the shared map don't prove this device ever downloaded the file, so
   * delete propagation is suppressed until the initial pull completes
   * (mirrors BinarySync's `pullingMissingRemote`).
   */
  private initialPull = false;
  private initialPullComplete = false;
  private reloadNoticeTimer: number | null = null;
  private observer: (event: Y.YMapEvent<ConfigMeta>) => void;
  private focusHandler = () => void this.retryInitialPullOrReconcile();

  constructor(plugin: RealtimePlugin, indexDoc: Y.Doc, localSyncState?: LocalSyncState) {
    this.plugin = plugin;
    this.indexDoc = indexDoc;
    this.localSyncState = localSyncState;
    this.configFiles = indexDoc.getMap<ConfigMeta>("configFiles");
    this.observer = this.onConfigFilesChanged.bind(this);
    this.configFiles.observe(this.observer);
  }

  private get vaultId(): string {
    return this.plugin.settings.activeVaultId;
  }

  /** Obsidian's config directory (user-configurable; usually `.obsidian`). */
  private get configRoot(): string {
    return this.plugin.app.vault.configDir;
  }

  /** The plugin's own folder, always excluded to avoid sync feedback loops. */
  private get hardExcludeDir(): string {
    return `${this.configRoot}/plugins/realtime`;
  }

  seedBaseline(): void {
    for (const [path, meta] of this.configFiles.entries()) {
      if (!meta?.hash) continue;
      if (!this.localSyncState) {
        this.lastSyncedHash.set(path, meta.hash);
        continue;
      }
      this.localSyncState.migrateLegacyIdentity(path, "config", meta.hash);
      const state = this.localSyncState.get(path);
      if (state?.kind === "config" && !state.candidate && state.identity === meta.hash) {
        this.lastSyncedHash.set(path, meta.hash);
      }
    }
    for (const [path, state] of this.localSyncState?.entries() ?? []) {
      if (state.kind === "config" && !state.candidate && state.identity) {
        this.lastSyncedHash.set(path, state.identity);
      }
    }
    dbg("ConfigSync seedBaseline", this.lastSyncedHash.size, "entries");
  }

  start(categories: Set<ConfigCategoryId>): void {
    if (this.destroyed) return;
    this.enabledCategories = new Set(categories);
    if (this.started) {
      void this.runInitialPull();
      return;
    }
    this.started = true;
    window.addEventListener("focus", this.focusHandler);
    void this.runInitialPull();
    this.schedulePoll();
  }

  private async runInitialPull(): Promise<void> {
    if (this.paused) return;
    this.initialPull = true;
    try {
      const complete = await this.reconcileAll();
      if (complete && !this.paused && !this.destroyed) this.initialPullComplete = true;
    } finally {
      this.initialPull = !this.initialPullComplete;
    }
  }

  private async retryInitialPullOrReconcile(): Promise<void> {
    if (this.initialPullComplete) {
      await this.reconcileAll();
    } else {
      await this.runInitialPull();
    }
  }

  async reconcileAll(): Promise<boolean> {
    if (this.destroyed || !this.started || this.paused) return false;
    const paths = new Set<string>();
    for (const path of this.configFiles.keys()) {
      if (!this.isHardExcluded(path)) paths.add(path);
    }
    const localPaths = await this.localConfigPaths();
    if (localPaths === undefined) return false;
    for (const path of localPaths) paths.add(path);
    let complete = true;
    for (const path of paths) {
      complete = (await this.reconcile(path)) && complete;
      if (this.destroyed) return false;
    }
    return complete;
  }

  private onConfigFilesChanged(event: Y.YMapEvent<ConfigMeta>): void {
    if (this.destroyed || !this.started || this.paused) return;
    event.changes.keys.forEach((_change, path) => {
      if (!this.isHardExcluded(path)) void this.reconcile(path);
    });
  }

  private reconcile(path: string): Promise<boolean> {
    if (this.syncableCategory(path) === null) return Promise.resolve(true);
    if (this.writing.has(path)) return Promise.resolve(true);
    const prev = this.chains.get(path) ?? Promise.resolve(true);
    const next = prev
      .then(() => this.reconcileNow(path))
      .catch((e) => {
        console.error(`[Realtime] config reconcile failed for ${path}`, e);
        return false;
      });
    this.chains.set(path, next);
    void next.finally(() => {
      if (this.chains.get(path) === next) this.chains.delete(path);
    });
    return next;
  }

  private async reconcileNow(path: string): Promise<boolean> {
    if (this.destroyed || this.paused) return false;
    const generation = this.pauseGeneration;
    const local = await this.localInfo(path);
    // Adapter failures are retried by the bounded poll/focus/resume lifecycle.
    // Re-enqueueing here resolves the current chain and immediately starts a
    // new one, producing an unbounded microtask + adapter-I/O loop.
    if (local === undefined || !this.canApply(generation)) return false;
    if (local && !this.localSyncState?.has(path)) {
      this.localSyncState?.beginCandidate(path, "config", local.hash, local.hash);
    }
    const remote = this.configFiles.get(path) ?? null;
    const base = this.lastSyncedHash.get(path) ?? null;
    const localState = this.localSyncState?.get(path);

    if (
      local &&
      remote &&
      local.hash !== remote.hash &&
      localState?.kind === "config" &&
      localState.candidate
    ) {
      const localBytes = await this.plugin.app.vault.adapter.readBinary(path);
      let remoteBytes: ArrayBuffer;
      try {
        remoteBytes = await this.plugin.auth.getBlob(this.vaultId, path, remote.hash);
      } catch (e) {
        if (this.destroyed) return false;
        console.error(`[Realtime] config blob download failed for ${path}`, e);
        window.setTimeout(() => void this.reconcile(path), RETRY_MS);
        return false;
      }
      if (!this.canApply(generation)) {
        void this.reconcile(path);
        return false;
      }
      const preservedPath = await preserveAdapterConflict(this.plugin, path, localBytes, "local");
      new Notice(
        `Realtime: kept the remote settings at "${path}" and preserved this device's unrelated file as "${preservedPath}".`,
      );
      if (!(await this.configStateMatches(path, local.hash, remote.hash))) {
        void this.reconcile(path);
        return false;
      }
      await this.materializeDownload(path, remote, remoteBytes, local.hash, generation);
      return this.canApply(generation);
    }
    if (local && !remote && base !== null && base !== local.hash) {
      const localBytes = await this.plugin.app.vault.adapter.readBinary(path);
      if (!this.canApply(generation)) {
        void this.reconcile(path);
        return false;
      }
      const preservedPath = await preserveAdapterConflict(this.plugin, path, localBytes, "local");
      new Notice(
        `Realtime: "${path}" was deleted remotely; preserved this device's edited settings as "${preservedPath}".`,
      );
      if (!(await this.configStateMatches(path, local.hash, null))) {
        void this.reconcile(path);
        return false;
      }
      await this.deleteLocal(path, generation);
      return this.canApply(generation);
    }

    const action = decideConfigReconcile(local, remote, base, {
      initialPull: this.initialPull,
      canMerge: path.endsWith(".json"),
    });
    if (action === "none") {
      if (remote?.hash) {
        this.lastSyncedHash.set(path, remote.hash);
        this.localSyncState?.markSynced(path, "config", remote.hash, remote.hash);
      } else this.lastSyncedHash.delete(path);
      return true;
    }
    if (action === "upload" && local) {
      await this.upload(path, local, remote?.hash ?? null, generation);
    } else if (action === "download" && remote) {
      await this.download(path, remote, local?.hash ?? null, generation);
    } else if (action === "deleteLocal") await this.deleteLocal(path, generation);
    else if (action === "deleteRemote") this.publishDelete(path, generation);
    else if (action === "merge" && local && remote)
      await this.mergeConflict(path, local, remote, generation);
    return this.canApply(generation);
  }

  private async localConfigPaths(): Promise<string[] | undefined> {
    const paths: string[] = [];
    if (!(await this.walk(this.configRoot, paths))) return undefined;
    return paths.filter((path) => this.syncableCategory(path) !== null);
  }

  private async walk(folder: string, paths: string[]): Promise<boolean> {
    let listed: { files: string[]; folders: string[] };
    try {
      listed = await this.plugin.app.vault.adapter.list(folder);
    } catch {
      return false;
    }
    for (const file of listed.files) paths.push(file);
    for (const child of listed.folders) {
      if (this.isHardExcluded(child)) continue;
      if (!(await this.walk(child, paths))) return false;
    }
    return true;
  }

  private async localInfo(path: string): Promise<ConfigMeta | null | undefined> {
    try {
      if (!(await this.plugin.app.vault.adapter.exists(path))) return null;
      const bytes = await this.plugin.app.vault.adapter.readBinary(path);
      const stat = await this.plugin.app.vault.adapter.stat(path);
      return {
        hash: await sha256Hex(bytes),
        size: bytes.byteLength,
        mtime: stat?.mtime ?? Date.now(),
      };
    } catch (e) {
      console.error(`[Realtime] failed to read config ${path}`, e);
      return undefined;
    }
  }

  private async upload(
    path: string,
    meta: ConfigMeta,
    expectedRemoteHash: string | null,
    generation = this.pauseGeneration,
  ): Promise<void> {
    const bytes = await this.plugin.app.vault.adapter.readBinary(path);
    if (!this.canApply(generation)) {
      if (!this.destroyed) void this.reconcile(path);
      return;
    }
    await this.uploadBytes(path, bytes, meta, expectedRemoteHash, generation);
  }

  private async uploadBytes(
    path: string,
    bytes: ArrayBuffer,
    meta: ConfigMeta | null,
    expectedRemoteHash: string | null,
    generation = this.pauseGeneration,
  ): Promise<void> {
    const hash = await sha256Hex(bytes);
    if (!this.canApply(generation)) {
      if (!this.destroyed) void this.reconcile(path);
      return;
    }
    const finalMeta =
      meta && hash === meta.hash ? meta : { hash, size: bytes.byteLength, mtime: Date.now() };
    if (!(await this.plugin.auth.blobExists(this.vaultId, path, finalMeta.hash))) {
      if (!this.canApply(generation)) {
        if (!this.destroyed) void this.reconcile(path);
        return;
      }
      await this.plugin.auth.putBlob(this.vaultId, path, finalMeta.hash, bytes);
    }
    if (!this.canApply(generation)) {
      if (!this.destroyed) void this.reconcile(path);
      return;
    }
    const currentLocal = await this.localInfo(path);
    if (
      !this.canApply(generation) ||
      !currentLocal ||
      currentLocal.hash !== finalMeta.hash ||
      (this.configFiles.get(path)?.hash ?? null) !== expectedRemoteHash
    ) {
      void this.reconcile(path);
      return;
    }
    this.indexDoc.transact(() => this.configFiles.set(path, finalMeta));
    this.lastSyncedHash.set(path, finalMeta.hash);
    this.localSyncState?.markSynced(path, "config", finalMeta.hash, finalMeta.hash, true);
    dbg("config uploaded+published", path, finalMeta.hash, finalMeta.size);
  }

  private async download(
    path: string,
    meta: ConfigMeta,
    expectedLocalHash?: string | null,
    generation = this.pauseGeneration,
  ): Promise<void> {
    let bytes: ArrayBuffer;
    try {
      bytes = await this.plugin.auth.getBlob(this.vaultId, path, meta.hash);
    } catch (e) {
      if (this.destroyed) return;
      console.error(`[Realtime] config blob download failed for ${path}`, e);
      window.setTimeout(() => void this.reconcile(path), RETRY_MS);
      return;
    }
    await this.materializeDownload(path, meta, bytes, expectedLocalHash, generation);
  }

  private async materializeDownload(
    path: string,
    meta: ConfigMeta,
    bytes: ArrayBuffer,
    expectedLocalHash?: string | null,
    generation = this.pauseGeneration,
  ): Promise<void> {
    if (!this.canApply(generation)) {
      if (!this.destroyed) void this.reconcile(path);
      return;
    }
    const currentLocal = await this.localInfo(path);
    if (
      !this.canApply(generation) ||
      (this.configFiles.get(path)?.hash ?? null) !== meta.hash ||
      (expectedLocalHash !== undefined && (currentLocal?.hash ?? null) !== expectedLocalHash)
    ) {
      void this.reconcile(path);
      return;
    }
    this.writing.add(path);
    try {
      await this.ensureParentFolders(path);
      if (!this.canApply(generation)) {
        void this.reconcile(path);
        return;
      }
      await this.plugin.app.vault.adapter.writeBinary(path, bytes);
      this.lastSyncedHash.set(path, meta.hash);
      this.localSyncState?.markSynced(path, "config", meta.hash, meta.hash, true);
      this.noteDownloaded(path);
      dbg("config downloaded", path, meta.hash, bytes.byteLength);
    } finally {
      window.setTimeout(() => this.writing.delete(path), 0);
    }
  }

  /**
   * Both sides changed the same JSON settings file since the shared baseline.
   * First try a three-way merge against the baseline blob; disjoint changes
   * publish silently. Otherwise mirror Obsidian Sync: merge the objects with
   * local keys on top of remote keys, preserve the remote as a recovery copy,
   * and publish. Falls back to newest-wins when either side isn't a plain
   * JSON object.
   */
  private async mergeConflict(
    path: string,
    local: ConfigMeta,
    remote: ConfigMeta,
    generation = this.pauseGeneration,
  ): Promise<void> {
    let remoteBytes: ArrayBuffer;
    try {
      remoteBytes = await this.plugin.auth.getBlob(this.vaultId, path, remote.hash);
    } catch (e) {
      if (this.destroyed) return;
      console.error(`[Realtime] config blob download failed for ${path}`, e);
      window.setTimeout(() => void this.reconcile(path), RETRY_MS);
      return;
    }
    const localBytes = await this.plugin.app.vault.adapter.readBinary(path);
    if (!this.canApply(generation)) {
      if (!this.destroyed) void this.reconcile(path);
      return;
    }
    if (!(await this.configStateMatches(path, local.hash, remote.hash))) {
      void this.reconcile(path);
      return;
    }

    const decoder = new TextDecoder();
    const baseHash = this.lastSyncedHash.get(path);
    if (baseHash) {
      const baseBytes = await this.plugin.auth
        .getBlob(this.vaultId, path, baseHash)
        .catch(() => null);
      if (!this.canApply(generation)) {
        if (!this.destroyed) void this.reconcile(path);
        return;
      }
      const threeWay = baseBytes
        ? mergeJsonSettingsThreeWay(
            decoder.decode(baseBytes),
            decoder.decode(remoteBytes),
            decoder.decode(localBytes),
          )
        : null;
      if (threeWay && !threeWay.conflicted) {
        if (!(await this.configStateMatches(path, local.hash, remote.hash))) {
          void this.reconcile(path);
          return;
        }
        await this.writeMergedAndUpload(path, threeWay.text, remote.hash, generation);
        return;
      }
    }
    const merged = mergeJsonSettings(decoder.decode(remoteBytes), decoder.decode(localBytes));
    if (merged === null) {
      // Not mergeable JSON → newest modified version wins.
      if (local.mtime >= remote.mtime) {
        if (!this.canApply(generation)) return;
        await preserveAdapterConflict(this.plugin, path, remoteBytes, "remote");
        if (!(await this.configStateMatches(path, local.hash, remote.hash))) {
          void this.reconcile(path);
          return;
        }
        await this.uploadBytes(path, localBytes, local, remote.hash, generation);
      } else {
        if (!this.canApply(generation)) return;
        await preserveAdapterConflict(this.plugin, path, localBytes, "local");
        if (!(await this.configStateMatches(path, local.hash, remote.hash))) {
          void this.reconcile(path);
          return;
        }
        await this.download(path, remote, local.hash, generation);
      }
      return;
    }

    if (!this.canApply(generation)) return;
    const preservedPath = await preserveAdapterConflict(this.plugin, path, remoteBytes, "remote");
    new Notice(
      `Realtime: merged settings at "${path}" and preserved the conflicting remote version as "${preservedPath}".`,
    );
    if (!(await this.configStateMatches(path, local.hash, remote.hash))) {
      void this.reconcile(path);
      return;
    }

    await this.writeMergedAndUpload(path, merged, remote.hash, generation);
  }

  private async writeMergedAndUpload(
    path: string,
    merged: string,
    remoteHash: string,
    generation: number,
  ): Promise<void> {
    const mergedBytes = new TextEncoder().encode(merged);
    const buffer = mergedBytes.buffer.slice(
      mergedBytes.byteOffset,
      mergedBytes.byteOffset + mergedBytes.byteLength,
    ) as ArrayBuffer;
    this.writing.add(path);
    try {
      if (!this.canApply(generation)) {
        void this.reconcile(path);
        return;
      }
      await this.plugin.app.vault.adapter.writeBinary(path, buffer);
    } finally {
      window.setTimeout(() => this.writing.delete(path), 0);
    }
    if (!this.canApply(generation)) return;
    await this.uploadBytes(path, buffer, null, remoteHash, generation);
    dbg("config merged", path);
  }

  private async configStateMatches(
    path: string,
    localHash: string,
    remoteHash: string | null,
  ): Promise<boolean> {
    const currentLocal = await this.localInfo(path);
    return (
      !this.destroyed &&
      currentLocal?.hash === localHash &&
      (this.configFiles.get(path)?.hash ?? null) === remoteHash
    );
  }

  private async deleteLocal(path: string, generation = this.pauseGeneration): Promise<void> {
    if (!this.canApply(generation)) return;
    this.writing.add(path);
    try {
      if (await this.plugin.app.vault.adapter.exists(path)) {
        if (!this.canApply(generation)) {
          void this.reconcile(path);
          return;
        }
        await this.plugin.app.vault.adapter.remove(path);
      }
      this.lastSyncedHash.delete(path);
      this.localSyncState?.remove(path);
      this.noteDownloaded(path);
    } finally {
      window.setTimeout(() => this.writing.delete(path), 0);
    }
  }

  private publishDelete(path: string, generation = this.pauseGeneration): void {
    if (!this.canApply(generation)) {
      if (!this.destroyed) void this.reconcile(path);
      return;
    }
    this.indexDoc.transact(() => this.configFiles.delete(path));
    this.lastSyncedHash.delete(path);
    this.localSyncState?.remove(path);
    dbg("config delete published", path);
  }

  private async ensureParentFolders(path: string): Promise<void> {
    const parts = path.split("/").slice(0, -1);
    let current = "";
    for (const part of parts) {
      current = current ? `${current}/${part}` : part;
      if (!(await this.plugin.app.vault.adapter.exists(current))) {
        try {
          await this.plugin.app.vault.adapter.mkdir(current);
        } catch (e) {
          // Folder may have been created concurrently; tolerate that case only.
          const msg = e instanceof Error ? e.message : String(e);
          if (!/already exist/i.test(msg)) throw e;
        }
      }
    }
  }

  /**
   * After remote changes land in categories Obsidian can't hot-reload, show
   * one debounced notice suggesting a reload (mirrors Obsidian Sync's
   * documented "restart after settings download" guidance).
   */
  private noteDownloaded(path: string): void {
    const category = this.syncableCategory(path);
    if (!category || !categoryRequiresReload(category)) return;
    if (this.reloadNoticeTimer !== null) window.clearTimeout(this.reloadNoticeTimer);
    this.reloadNoticeTimer = window.setTimeout(() => {
      this.reloadNoticeTimer = null;
      if (this.destroyed) return;
      new Notice("Realtime: synced Obsidian settings changed. Reload Obsidian to apply them.");
    }, RELOAD_NOTICE_DEBOUNCE_MS);
  }

  private schedulePoll(): void {
    if (this.destroyed || this.paused || this.pollTimer !== null) return;
    this.pollTimer = window.setTimeout(() => {
      this.pollTimer = null;
      void this.retryInitialPullOrReconcile().finally(() => this.schedulePoll());
    }, POLL_MS);
  }

  setPaused(paused: boolean): void {
    if (this.destroyed || this.paused === paused) return;
    this.paused = paused;
    this.pauseGeneration += 1;
    if (paused && this.pollTimer !== null) {
      window.clearTimeout(this.pollTimer);
      this.pollTimer = null;
    } else if (!paused && this.started) {
      if (this.initialPullComplete) void this.reconcileAll();
      else void this.runInitialPull();
      this.schedulePoll();
    }
  }

  private canApply(generation: number): boolean {
    return !this.destroyed && !this.paused && this.pauseGeneration === generation;
  }

  /**
   * The enabled sync category a path belongs to, or `null` when the path is
   * hard-excluded, outside this device's config folder (other profiles are
   * other devices' business), unclassifiable, or in a disabled category.
   */
  private syncableCategory(path: string): ConfigCategoryId | null {
    if (this.isHardExcluded(path) || isConflictCopy(path)) return null;
    const category = categoryForVaultPath(path, this.configRoot);
    if (category === null || !this.enabledCategories.has(category)) return null;
    return category;
  }

  private isHardExcluded(path: string): boolean {
    const dir = this.hardExcludeDir;
    return path === dir || path.startsWith(`${dir}/`) || path.split("/").includes("node_modules");
  }

  destroy(): void {
    if (this.destroyed) return;
    this.destroyed = true;
    this.started = false;
    this.configFiles.unobserve(this.observer);
    window.removeEventListener("focus", this.focusHandler);
    if (this.pollTimer !== null) {
      window.clearTimeout(this.pollTimer);
      this.pollTimer = null;
    }
    if (this.reloadNoticeTimer !== null) {
      window.clearTimeout(this.reloadNoticeTimer);
      this.reloadNoticeTimer = null;
    }
    this.chains.clear();
  }
}
