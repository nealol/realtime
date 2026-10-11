/**
 * Public, plugin-author-facing entry point for the synced-SQLite API,
 * reachable as `app.plugins.plugins["realtime"].sql`.
 *
 * See docs/plugin-sql for the full guide. The API is intentionally small:
 * `open`, `delete`, `restore`, `whenAvailable`, plus the returned database
 * handle.
 */

import type {
  DatabaseHandle,
  DeleteOrRestoreOptions,
  OpenOptions,
} from "@realtime-md/plugin-api-types";
import type RealtimePlugin from "../main";
import {
  SyncedPluginDatabase,
  makeMemoryDocHandle,
  type MigrateFn,
  type SqlTx,
} from "./SyncedPluginDatabase";
import { PluginDbSync } from "./PluginDbSync";
import { buildEngineDeps } from "./obsidianDeps";
import { isValidId } from "./types";
import { CompatibilityError } from "../caps";

// Public interfaces live in the published types package; re-export for
// internal callers and docs links.
export type { OpenOptions, DeleteOrRestoreOptions, DatabaseHandle };

export interface DebugDatabase {
  pluginId: string;
  name: string;
  state: SyncedPluginDatabase["state"];
}

export class RealtimeSqlAPI {
  private plugin: RealtimePlugin;
  private engines = new Map<string, SyncedPluginDatabase>();
  private activeScope: string | null = null;

  constructor(plugin: RealtimePlugin) {
    this.plugin = plugin;
  }

  /** Cancellers for pending whenAvailable() pollers (fired on destroy/abort). */
  private availabilityCancels = new Set<() => void>();

  /**
   * Resolves once Realtime is enabled, signed in, and bound to a vault.
   * Never resolves while the user stays signed out — pass an `AbortSignal`
   * to stop waiting early. Plugin unload rejects all pending waiters.
   */
  async whenAvailable(opts?: { signal?: AbortSignal }): Promise<void> {
    if (!this.basicAvailable()) {
      if (opts?.signal?.aborted) throw new Error("whenAvailable aborted");
      await new Promise<void>((resolve, reject) => {
        const timer = setInterval(() => {
          if (this.basicAvailable()) {
            cleanup();
            resolve();
          }
        }, 200);
        const cancel = () => {
          cleanup();
          reject(new Error("whenAvailable aborted"));
        };
        const cleanup = () => {
          clearInterval(timer);
          this.availabilityCancels.delete(cancel);
          opts?.signal?.removeEventListener("abort", cancel);
        };
        this.availabilityCancels.add(cancel);
        opts?.signal?.addEventListener("abort", cancel, { once: true });
      });
    }
    await this.ensureCompatible();
    await this.prepareScope();
  }

  private basicAvailable(): boolean {
    return (
      this.plugin.settings.enabled &&
      this.plugin.auth.isLoggedIn &&
      !!this.plugin.settings.activeVaultId
    );
  }

  private requireAvailable(): void {
    if (!this.plugin.settings.enabled) throw new Error("Realtime is disabled in settings.");
    if (!this.plugin.auth.isLoggedIn) throw new Error("Realtime is signed out — sign in first.");
    if (!this.plugin.settings.activeVaultId) throw new Error("Realtime has no active vault.");
  }

  private async ensureCompatible(): Promise<void> {
    try {
      await this.plugin.auth.ensureServerId();
    } catch (error) {
      if (error instanceof CompatibilityError) throw error;
      // Match the vault sync lifecycle: a network failure must not disable
      // already-persisted offline work.
    }
    const incompatible = this.plugin.lastCompatibilityError;
    if (incompatible) {
      throw new CompatibilityError(
        incompatible.reason,
        incompatible.detail,
        incompatible.serverVersion,
      );
    }
  }

  private scope(): string {
    return JSON.stringify([
      this.plugin.settings.authServerUrl,
      this.plugin.settings.authServerId,
      this.plugin.settings.userId,
      this.plugin.settings.activeVaultId,
    ]);
  }

  private key(scope: string, pluginId: string, name: string): string {
    return JSON.stringify([scope, pluginId, name]);
  }

  private async prepareScope(): Promise<string> {
    const scope = this.scope();
    if (this.activeScope !== null && this.activeScope !== scope) {
      await this.closeEngines();
    }
    this.activeScope = scope;
    return scope;
  }

  private assertHandleScope(scope: string): void {
    if (!this.basicAvailable() || this.scope() !== scope || this.plugin.lastCompatibilityError) {
      throw new Error("Realtime database handle is no longer valid for the active session.");
    }
  }

  private async closeEngines(): Promise<void> {
    const engines = [...this.engines.values()];
    this.engines.clear();
    await Promise.all(engines.map((engine) => engine.close().catch(() => {})));
  }

  /** Close engines when settings/auth moved to another synchronization scope. */
  async reconcileLifecycle(): Promise<void> {
    if (
      !this.basicAvailable() ||
      this.plugin.lastCompatibilityError ||
      (this.activeScope !== null && this.activeScope !== this.scope())
    ) {
      this.activeScope = null;
      await this.closeEngines();
    }
  }

  /** Invalidate every handle before logout or an explicit vault replacement. */
  async resetForLifecycle(): Promise<void> {
    this.activeScope = null;
    await this.closeEngines();
  }

  async open(opts: OpenOptions): Promise<DatabaseHandle> {
    validateIds(opts.pluginId, opts.name);
    if (!Number.isInteger(opts.schemaVersion) || opts.schemaVersion < 1) {
      throw new Error("schemaVersion must be a positive integer.");
    }
    this.requireAvailable();
    await this.ensureCompatible();
    const scope = await this.prepareScope();
    await layoutReady(this.plugin);

    const key = this.key(scope, opts.pluginId, opts.name);
    let engine = this.engines.get(key);
    if (engine && engine.state === "error") {
      // A tombstoned/failed engine cannot be reused — rebuild from scratch
      // (e.g. the database was deleted and then restored from the trash on
      // another device; the fresh engine re-bootstraps).
      this.engines.delete(key);
      await engine.close().catch(() => {});
      engine = undefined;
    }
    if (!engine) {
      const deps = buildEngineDeps(this.plugin, opts.pluginId, opts.name);
      engine = new SyncedPluginDatabase({
        vaultId: this.plugin.settings.activeVaultId,
        pluginId: opts.pluginId,
        name: opts.name,
        schemaVersion: opts.schemaVersion,
        migrate: opts.migrate,
        onMergeReview: opts.onMergeReview,
        ...deps,
      });
      this.engines.set(key, engine);
    }
    engine.refcount++;
    try {
      await engine.start();
      // Re-opened with a newer schema: migrate the running engine in place
      // and drain any batches buffered while it was behind.
      if (opts.schemaVersion > engine.currentSchemaVersion) {
        await engine.upgradeSchema(opts.schemaVersion, opts.migrate);
      }
    } catch (e) {
      engine.refcount--;
      if (engine.refcount <= 0) {
        if (this.engines.get(key) === engine) this.engines.delete(key);
        await engine.close().catch(() => {});
      }
      throw e;
    }
    return this.makeHandle(key, scope, engine);
  }

  private makeHandle(key: string, scope: string, engine: SyncedPluginDatabase): DatabaseHandle {
    let closed = false;
    return {
      exec: (sql, bind) => {
        this.assertHandleScope(scope);
        return engine.exec(sql, bind);
      },
      query: (sql, bind) => {
        this.assertHandleScope(scope);
        return engine.query(sql, bind);
      },
      transaction: (cb) => {
        this.assertHandleScope(scope);
        return engine.transaction(cb);
      },
      onRemoteChange: (cb) => engine.onRemoteChange(cb),
      onStateChange: (cb) => engine.onStateChange(cb),
      get state() {
        return engine.state;
      },
      whenLive: () => {
        this.assertHandleScope(scope);
        return engine.whenLive();
      },
      rebaseFromServer: () => {
        this.assertHandleScope(scope);
        return engine.rebaseFromServer();
      },
      close: async () => {
        if (closed) return;
        closed = true;
        engine.refcount--;
        if (engine.refcount <= 0) {
          if (this.engines.get(key) === engine) this.engines.delete(key);
          await engine.close();
        }
      },
    };
  }

  /** Active local databases available to the internal SQL debugging view. */
  debugDatabases(): DebugDatabase[] {
    return [...this.engines.values()]
      .map((engine) => ({
        pluginId: engine.pluginId,
        name: engine.name,
        state: engine.state,
      }))
      .sort((a, b) =>
        a.pluginId === b.pluginId
          ? a.name.localeCompare(b.name)
          : a.pluginId.localeCompare(b.pluginId),
      );
  }

  /** Execute unrestricted SQL against one active local database. Internal UI only. */
  debugExecute(
    pluginId: string,
    name: string,
    sql: string,
  ): Promise<Record<string, import("./types").SqlValue>[]> {
    this.requireAvailable();
    const scope = this.scope();
    if (this.activeScope !== scope) {
      return Promise.reject(new Error("The selected local database is no longer available."));
    }
    const engine = this.engines.get(this.key(scope, pluginId, name));
    if (!engine) {
      return Promise.reject(new Error("The selected local database is no longer open."));
    }
    return engine.debugExecute(sql);
  }

  /** Soft-delete: tombstone the doc AND drop it into the vault trash bin. */
  async delete(opts: DeleteOrRestoreOptions): Promise<void> {
    validateIds(opts.pluginId, opts.name);
    this.requireAvailable();
    await this.ensureCompatible();
    const scope = await this.prepareScope();
    const key = this.key(scope, opts.pluginId, opts.name);
    const engine = this.engines.get(key);
    if (engine) {
      engine.markDeleted();
      // Drop the (now tombstoned) engine so a later open() rebuilds fresh.
      this.engines.delete(key);
      await engine.close().catch(() => {});
    } else {
      await this.withTransientDoc(opts.pluginId, opts.name, (sync) => {
        sync.setDeletedAt(Date.now());
      });
    }
    this.plugin.vaultSync?.recordTrash({
      path: `${opts.pluginId}/${opts.name}`,
      kind: "plugindb",
      pluginId: opts.pluginId,
      name: opts.name,
    });
  }

  /** Clear the tombstone so a fresh open() re-bootstraps. */
  async restore(opts: DeleteOrRestoreOptions): Promise<void> {
    validateIds(opts.pluginId, opts.name);
    this.requireAvailable();
    await this.ensureCompatible();
    const scope = await this.prepareScope();
    const key = this.key(scope, opts.pluginId, opts.name);
    const engine = this.engines.get(key);
    if (engine) {
      // The engine is likely in the terminal `deleted` error state; clear the
      // tombstone and drop it so the next open() re-bootstraps from scratch.
      engine.clearDeleted();
      this.engines.delete(key);
      await engine.close().catch(() => {});
      return;
    }
    await this.withTransientDoc(opts.pluginId, opts.name, (sync) => {
      sync.clearDeletedAt();
    });
  }

  /** True when a non-tombstoned database with this id currently exists. */
  async isLive(opts: DeleteOrRestoreOptions): Promise<boolean> {
    validateIds(opts.pluginId, opts.name);
    this.requireAvailable();
    await this.ensureCompatible();
    const scope = await this.prepareScope();
    const key = this.key(scope, opts.pluginId, opts.name);
    const engine = this.engines.get(key);
    if (engine) return engine.isLive();
    return this.withTransientDoc(opts.pluginId, opts.name, (sync) => {
      return (
        sync.getDeletedAt() === null &&
        (sync.listBatches().length > 0 || sync.getSchemaVersion() > 0)
      );
    });
  }

  /** Open just the per-DB doc (no cr-sqlite) to read/flip meta, then tear down. */
  private async withTransientDoc<T>(
    pluginId: string,
    name: string,
    fn: (sync: PluginDbSync) => T,
  ): Promise<T> {
    const vaultId = this.plugin.settings.activeVaultId;
    const docId = `${vaultId}__plugindb__${pluginId}__${name}`;
    const deps = buildEngineDeps(this.plugin, pluginId, name);
    const handle = deps.makeDoc(docId);
    const sync = new PluginDbSync(handle);
    try {
      await sync.whenSynced();
      return fn(sync);
    } finally {
      // Don't drop a just-written tombstone/restore with the connection.
      await sync.whenFlushed().catch(() => {});
      sync.destroy();
    }
  }

  /** Escape hatch: rebuild every currently-open database from the server replica. */
  async rebaseAll(): Promise<number> {
    this.requireAvailable();
    await this.ensureCompatible();
    await this.prepareScope();
    const engines = [...this.engines.values()];
    for (const engine of engines) await engine.rebaseFromServer().catch(() => {});
    return engines.length;
  }

  /**
   * A plugin database's document moved to a new epoch. Only that database
   * reconnects; its local data and every handle a plugin holds stay valid.
   * A database that is not open picks up the new epoch when next opened.
   */
  async rebuildDocumentForEpoch(documentId: string): Promise<void> {
    const engines = [...this.engines.values()].filter((engine) => engine.docId === documentId);
    await Promise.all(engines.map((engine) => engine.replaceTransport()));
  }

  /** Close every open database (plugin unload). */
  async destroy(): Promise<void> {
    for (const cancel of [...this.availabilityCancels]) cancel();
    this.availabilityCancels.clear();
    this.activeScope = null;
    await this.closeEngines();
  }
}

function validateIds(pluginId: string, name: string): void {
  if (!isValidId(pluginId)) {
    throw new Error(`pluginId must match [A-Za-z0-9_-]{1,80} without "__": ${pluginId}`);
  }
  if (!isValidId(name)) {
    throw new Error(`name must match [A-Za-z0-9_-]{1,80} without "__": ${name}`);
  }
}

function layoutReady(plugin: RealtimePlugin): Promise<void> {
  return new Promise<void>((resolve) => plugin.app.workspace.onLayoutReady(resolve));
}

// Re-export for the in-memory test harness and external typing.
export { makeMemoryDocHandle };
export type { MigrateFn, SqlTx };
