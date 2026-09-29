/**
 * The per-(pluginId,name) sync engine: an in-memory cr-sqlite database kept in
 * sync with peers through a Y.Doc batch log, persisted to a local snapshot, and
 * bootstrapped from / reconciled with the server replica.
 *
 * One instance is shared (refcounted) across all `open()` callers for the same
 * database; see {@link RealtimeSqlAPI} in api.ts.
 */

import * as Y from "yjs";
import type { DB, SQLite3 } from "./crsqlite";
import {
  openMemoryDb,
  readChangesForSite,
  applyChanges,
  currentDbVersion,
  hexToBytes,
  bytesToHex,
  base64ToBytes,
} from "./crsqlite";
import { PluginDbSync, type PluginDbDocHandle } from "./PluginDbSync";
import { captureSnapshot, parseSnapshot, serializeSnapshot, restoreSnapshot } from "./snapshot";
import type {
  Batch,
  ChangeRow,
  Cursor,
  DbState,
  DbErrorReason,
  RemoteChange,
  SqlValue,
} from "./types";
import { MAX_BATCH_ROWS, SYNC_FORMAT, isValidId } from "./types";

// `SqlTx` / `MigrateFn` are part of the published plugin API surface.
import type { MigrateFn, SqlTx } from "@realtime-md/plugin-api-types";
export type { MigrateFn, SqlTx };

export interface SyncedPluginDatabaseOptions {
  vaultId: string;
  pluginId: string;
  name: string;
  schemaVersion: number;
  migrate: MigrateFn;
  /**
   * Resolve the cr-sqlite WASM asset to a (same-origin) URL. Omit in production
   * to use the esbuild-inlined data URL; tests pass a node data URL.
   */
  locateWasm?: (file: string) => string;
  /** Build the per-DB Y.Doc transport handle. */
  makeDoc: (docId: string) => PluginDbDocHandle;
  /** Read the persisted snapshot text (null if none). */
  loadSnapshot: () => Promise<string | null>;
  /** Persist the snapshot text atomically. */
  saveSnapshot: (text: string) => Promise<void>;
  /** Remove the persisted snapshot. */
  deleteSnapshot: () => Promise<void>;
  /**
   * Pull the full server changeset for a cursor (bootstrap / rebase). Servers
   * also return the per-site cursor the changeset covers; older ones (and
   * simple stubs) return only the rows.
   */
  bootstrap?: (cursor: Cursor) => Promise<ChangeRow[] | BootstrapChangeset>;
  /** Notify the server that we published (debounced upstream). */
  touch?: () => void;
  /** Optional repair hook after remote applies. */
  onMergeReview?: (tables: string[]) => void | Promise<void>;
}

export interface BootstrapChangeset {
  changes: ChangeRow[];
  /** Per-site db_version the changeset covers (overwritten changes included). */
  cursor?: Cursor;
}

const PUBLISH_DEBOUNCE_MS = 250;
const SNAPSHOT_DEBOUNCE_MS = 1500;

export class SyncedPluginDatabase {
  readonly pluginId: string;
  readonly name: string;
  readonly vaultId: string;
  readonly docId: string;
  refcount = 0;

  private opts: SyncedPluginDatabaseOptions;
  private sqlite: SQLite3 | null = null;
  private db: DB | null = null;
  private sync: PluginDbSync | null = null;
  private siteHex = "";
  private schemaVersion: number;

  private cursor: Cursor = {};
  private published: Cursor = {};
  private appliedBatchIds = new Set<string>();
  private bufferedBatches: Batch[] = [];
  /** Serializes apply passes so two observer callbacks never interleave. */
  private applyChain: Promise<void> = Promise.resolve();

  private _state: DbState = "loading-wasm";
  private _errorReason: DbErrorReason | null = null;
  private stateListeners = new Set<(s: DbState) => void>();
  private remoteListeners = new Set<(c: RemoteChange) => void>();
  private liveWaiters: Array<{ resolve: () => void; reject: (e: Error) => void }> = [];

  private publishTimer: ReturnType<typeof setTimeout> | null = null;
  private snapshotTimer: ReturnType<typeof setTimeout> | null = null;
  private unobserveBatches: (() => void) | null = null;
  private unobserveMeta: (() => void) | null = null;
  private startPromise: Promise<void> | null = null;
  private closed = false;
  /** Set once the engine first reaches `live`: the local DB is authoritative. */
  private becameLive = false;

  constructor(opts: SyncedPluginDatabaseOptions) {
    if (!isValidId(opts.pluginId)) throw new Error(`invalid pluginId: ${opts.pluginId}`);
    if (!isValidId(opts.name)) throw new Error(`invalid database name: ${opts.name}`);
    this.opts = opts;
    this.pluginId = opts.pluginId;
    this.name = opts.name;
    this.vaultId = opts.vaultId;
    this.schemaVersion = opts.schemaVersion;
    this.docId = `${opts.vaultId}__plugindb__${opts.pluginId}__${opts.name}`;
  }

  // --- state -----------------------------------------------------------------

  get state(): DbState {
    return this._state;
  }

  get errorReason(): DbErrorReason | null {
    return this._errorReason;
  }

  private setState(state: DbState, reason: DbErrorReason | null = null): void {
    this._state = state;
    this._errorReason = reason;
    for (const cb of this.stateListeners) {
      try {
        cb(state);
      } catch (e) {
        console.error("[Realtime] sql onStateChange listener failed", e);
      }
    }
    if (state === "live") {
      this.becameLive = true;
      const waiters = this.liveWaiters;
      this.liveWaiters = [];
      for (const w of waiters) w.resolve();
    } else if (state === "error") {
      const err = new Error(
        `database ${this.pluginId}/${this.name} entered error state (${reason ?? "unknown"})`,
      );
      const waiters = this.liveWaiters;
      this.liveWaiters = [];
      for (const w of waiters) w.reject(err);
    }
  }

  onStateChange(cb: (s: DbState) => void): () => void {
    this.stateListeners.add(cb);
    return () => this.stateListeners.delete(cb);
  }

  onRemoteChange(cb: (c: RemoteChange) => void): () => void {
    this.remoteListeners.add(cb);
    return () => this.remoteListeners.delete(cb);
  }

  whenLive(): Promise<void> {
    if (this._state === "live") return Promise.resolve();
    if (this._state === "error") {
      return Promise.reject(
        new Error(`database ${this.pluginId}/${this.name} is in an error state`),
      );
    }
    return new Promise((resolve, reject) => this.liveWaiters.push({ resolve, reject }));
  }

  // --- lifecycle -------------------------------------------------------------

  start(): Promise<void> {
    if (!this.startPromise) this.startPromise = this.doStart();
    return this.startPromise;
  }

  private async doStart(): Promise<void> {
    try {
      const { getSqlite } = await import("./crsqlite");
      this.setState("loading-wasm");
      this.sqlite = await getSqlite(this.opts.locateWasm).catch((e) => {
        throw new WasmError(e instanceof Error ? e.message : String(e));
      });
      this.db = await openMemoryDb(this.sqlite);
      this.siteHex = this.db.siteid.toLowerCase();

      // Build the transport + Yjs view.
      this.sync = new PluginDbSync(this.opts.makeDoc(this.docId));

      this.setState("restoring");
      const restored = await this.tryRestoreSnapshot();
      if (!restored) {
        await this.runMigrate(0);
        await this.bootstrapFromServer();
      }

      await this.sync.whenSynced();
      if (this.closed) return;

      // Tombstone check: a deleted DB must not come back to life.
      if (this.sync.getDeletedAt() !== null) {
        await this.handleTombstoned();
        return;
      }

      this.setState("syncing");
      // Publish the CRR schema so the server can build a replica + git dump.
      await this.publishSchema();
      // Register the observers BEFORE the initial catch-up: the list covers
      // batches already present, the observer covers later arrivals — a batch
      // landing between the two would otherwise never be applied.
      this.unobserveBatches = this.sync.observeBatches((batches) => this.applyBatches(batches));
      this.unobserveMeta = this.sync.observeMeta(() => this.onMetaChanged());
      const initialBatches = this.sync.listBatches();
      await this.enqueueApply(initialBatches);
      // Clamp the restored published watermarks to what is actually durable
      // (in the log, or proven compacted), so the publish below re-reads any
      // range a stale snapshot claims was published but nobody received.
      this.reconcilePublished(initialBatches);
      // The log may have been compacted past our snapshot cursor while we were
      // away; a quiescent database gives no other trigger to catch up.
      if (this.needsServerRebase()) {
        await this.rebaseFromServer();
      } else {
        // Crash recovery: republish anything we produced (migrate/local
        // edits) that never made it into the log.
        await this.publishNow();
      }

      // The catch-up pass may have parked us in needs-migration; don't
      // override that (whenLive resolves only after the schema upgrade).
      if (this._state === "syncing") this.setState("live");
      this.scheduleSnapshot();
    } catch (e) {
      const reason: DbErrorReason = e instanceof WasmError ? "wasm" : "unknown";
      console.error(`[Realtime] failed to start plugin db ${this.docId}`, e);
      this.setState("error", reason);
      throw e;
    }
  }

  private async tryRestoreSnapshot(): Promise<boolean> {
    try {
      const text = await this.opts.loadSnapshot();
      if (!text) return false;
      const snap = parseSnapshot(text);
      if (!snap) return false;
      await restoreSnapshot(this.db!, snap);
      this.cursor = snap.cursors;
      this.published = snap.published;
      this.siteHex = this.db!.siteid.toLowerCase();
      if (snap.schemaVersion < this.schemaVersion) {
        await this.runMigrate(snap.schemaVersion);
      }
      return true;
    } catch (e) {
      console.warn(`[Realtime] snapshot restore failed for ${this.docId}; bootstrapping`, e);
      // Discard a corrupt snapshot and fall back to a fresh DB + bootstrap.
      await this.opts.deleteSnapshot().catch(() => {});
      await this.resetDb();
      return false;
    }
  }

  private async resetDb(): Promise<void> {
    if (this.db) await this.db.close().catch(() => {});
    this.db = await openMemoryDb(this.sqlite!);
    this.siteHex = this.db.siteid.toLowerCase();
    this.cursor = {};
    this.published = {};
    this.appliedBatchIds.clear();
    this.bufferedBatches = [];
  }

  private async runMigrate(fromVersion: number): Promise<void> {
    await this.runMigrateOn(this.db!, fromVersion);
  }

  private async runMigrateOn(db: DB, fromVersion: number): Promise<void> {
    await db.tx(async (tx) => {
      await this.opts.migrate(this.wrapTx(tx as unknown as DB, false), fromVersion);
    });
  }

  /** The schema version this engine is currently running at. */
  get currentSchemaVersion(): number {
    return this.schemaVersion;
  }

  /**
   * Upgrade a *running* engine to a newer schema version (a consumer re-opened
   * with a higher `schemaVersion`). Runs the new `migrate` from the old
   * version, republishes the schema, then drains any batches that were
   * buffered while we were behind (clearing `needs-migration`).
   */
  async upgradeSchema(newVersion: number, migrate: MigrateFn): Promise<void> {
    if (this.closed || !this.db) return;
    if (newVersion <= this.schemaVersion) return;
    const fromVersion = this.schemaVersion;
    this.opts.migrate = migrate;
    await this.runMigrate(fromVersion);
    this.schemaVersion = newVersion;
    await this.publishSchema();
    // The migration itself may have produced local changes.
    await this.publishNow();
    await this.drainBuffered();
    this.scheduleSnapshot();
  }

  /**
   * Re-apply batches buffered while our schema was behind. Anything still too
   * new is re-buffered by the apply pass; if the buffer fully drains, the
   * engine leaves `needs-migration`.
   */
  private async drainBuffered(): Promise<void> {
    if (this.bufferedBatches.length === 0) {
      if (this._state === "needs-migration") this.setState("live");
      return;
    }
    this.bufferedBatches = [];
    // Re-run a full pass, not just the buffered batches: batches that stalled
    // behind a buffered one (same site, later range) become applicable too.
    await this.enqueueApply(this.sync?.listBatches() ?? []);
    if (this.bufferedBatches.length === 0 && this._state === "needs-migration") {
      this.setState("live");
    }
  }

  private async bootstrapFromServer(): Promise<void> {
    if (!this.opts.bootstrap) return;
    this.setState("bootstrapping");
    this.cursor = await this.bootstrapInto(this.db!, this.cursor);
  }

  private async bootstrapInto(db: DB, cursor: Cursor): Promise<Cursor> {
    if (!this.opts.bootstrap) return { ...cursor };
    const result = await this.opts.bootstrap(cursor);
    const rows = Array.isArray(result) ? result : result.changes;
    const covered = Array.isArray(result) ? {} : (result.cursor ?? {});
    if (rows.length > 0) {
      await db.tx(async (tx) => {
        await applyChanges(tx as unknown as DB, rows);
      });
    }
    const advanced = { ...cursor };
    for (const row of rows) {
      const site = bytesToHex(base64ToBytes(row.site_id));
      advanced[site] = Math.max(advanced[site] ?? 0, row.db_version);
    }
    // The replica keeps only each cell's winning change, so the rows alone
    // understate how far a site's changes are covered once later writes from
    // other sites overwrote them; trust the server's cursor for that.
    for (const [site, version] of Object.entries(covered)) {
      const value = Number(version);
      if (Number.isSafeInteger(value) && value > 0) {
        advanced[site.toLowerCase()] = Math.max(advanced[site.toLowerCase()] ?? 0, value);
      }
    }
    return advanced;
  }

  private async handleTombstoned(): Promise<void> {
    await this.opts.deleteSnapshot().catch(() => {});
    if (this.db) await this.db.close().catch(() => {});
    this.db = null;
    this.setState("error", "deleted");
  }

  private onMetaChanged(): void {
    if (this.closed || !this.sync) return;
    if (this.sync.getDeletedAt() !== null && this._state !== "error") {
      void this.handleTombstoned();
      return;
    }
    // Compaction can trim away batches we still need without leaving any
    // batch from that site in the log to trip the per-batch gap check.
    if (this._state === "live" && this.needsServerRebase()) void this.rebaseFromServer();
  }

  /**
   * Clamp the persisted `published` watermarks to what is actually durable:
   * batches present in the log, or ranges the server compacted (which proves
   * they reached the replica). The snapshot file and the Y.Doc's IndexedDB
   * persistence are separate stores — if the doc store was lost (cleared,
   * evicted) after the snapshot recorded a publish, the watermark would
   * otherwise skip re-publishing rows nobody ever received.
   */
  private reconcilePublished(batches: Batch[]): void {
    if (!this.sync) return;
    const compacted = this.sync.getCompactedThrough();
    const inLog: Cursor = {};
    for (const b of batches) {
      inLog[b.siteId] = Math.max(inLog[b.siteId] ?? 0, b.toDbVersion);
    }
    for (const site of Object.keys(this.published)) {
      const durable = Math.max(compacted[site] ?? 0, inLog[site] ?? 0);
      if ((this.published[site] ?? 0) > durable) this.published[site] = durable;
    }
  }

  /**
   * True when the server compacted the log past our applied cursor for a
   * remote site: the missing range can no longer arrive through the batch
   * log, so the only way forward is a full server rebase. Own sites are
   * exempt — their rows live in our local crsql_changes by definition.
   */
  private needsServerRebase(): boolean {
    if (!this.sync || !this.opts.bootstrap) return false;
    const compacted = this.sync.getCompactedThrough();
    const ownSites = new Set<string>([this.siteHex, ...Object.keys(this.published)]);
    for (const [site, v] of Object.entries(compacted)) {
      if (ownSites.has(site)) continue;
      if (v > (this.cursor[site] ?? 0)) return true;
    }
    return false;
  }

  // --- queries ---------------------------------------------------------------

  private assertReady(): DB {
    if (this.closed || !this.db)
      throw new Error(`database ${this.pluginId}/${this.name} is not open`);
    if (this.rebasing) {
      throw new Error(`database ${this.pluginId}/${this.name} is rebasing`);
    }
    if (this._state === "error" && this._errorReason === "deleted") {
      throw new DeletedError(this.pluginId, this.name);
    }
    return this.db;
  }

  /**
   * Reject SQL referencing cr-sqlite/SQLite internals. Token-aware (mirrors
   * the server lint in `server/src/plugindb.rs`): single-quoted string
   * literals and comments are ignored, while bare and quoted identifiers
   * starting with `crsql_` or `sqlite_` are rejected.
   */
  private lint(sql: string): void {
    for (const ident of sqlIdentifiers(sql)) {
      const lower = ident.toLowerCase();
      if (lower.startsWith("crsql_") || lower.startsWith("sqlite_")) {
        throw new Error(
          `exec/query may not touch crsql_* or sqlite_* internals (${ident}); define schema in migrate()`,
        );
      }
    }
  }

  async exec(sql: string, bind: SqlValue[] = []): Promise<void> {
    const db = this.assertReady();
    this.lint(sql);
    await db.exec(sql, bind as never[]);
    this.schedulePublish();
    this.scheduleSnapshot();
  }

  async query<T = Record<string, SqlValue>>(sql: string, bind: SqlValue[] = []): Promise<T[]> {
    const db = this.assertReady();
    this.lint(sql);
    return db.execO<T & {}>(sql, bind as never[]);
  }

  /**
   * Execute SQL from Realtime's local debugging UI.
   *
   * Unlike the public plugin API, this intentionally permits SQLite and
   * cr-sqlite internal tables. Keep it off the public handle: malformed writes
   * here can corrupt the local replica and are only appropriate for debugging.
   */
  async debugExecute(sql: string): Promise<Record<string, SqlValue>[]> {
    const db = this.assertReady();
    // cr-sqlite returns null at runtime when the statement has no result
    // columns, despite execO's array-only declaration.
    const rows = (await db.execO<Record<string, SqlValue>>(sql)) ?? [];
    // The statement may have mutated user or internal tables. Preserve the
    // normal local-write behavior even though read-only statements make these
    // two calls no-ops beyond their debounced checks.
    this.schedulePublish();
    this.scheduleSnapshot();
    return rows;
  }

  async transaction<T>(cb: (tx: SqlTx) => Promise<T>): Promise<T> {
    const db = this.assertReady();
    let result!: T;
    await db.tx(async (tx) => {
      result = await cb(this.wrapTx(tx as unknown as DB, true));
    });
    this.schedulePublish();
    this.scheduleSnapshot();
    return result;
  }

  private wrapTx(tx: DB, lint: boolean): SqlTx {
    return {
      exec: async (sql, bind = []) => {
        if (lint) this.lint(sql);
        await tx.exec(sql, bind as never[]);
      },
      query: async <T = Record<string, SqlValue>>(sql: string, bind: SqlValue[] = []) => {
        if (lint) this.lint(sql);
        return (await tx.execO<Record<string, SqlValue>>(sql, bind as never[])) as T[];
      },
    };
  }

  // --- publish ---------------------------------------------------------------

  private async publishSchema(): Promise<void> {
    if (!this.db || !this.sync) return;
    try {
      const { collectSchema } = await import("./snapshot");
      const ddl = await collectSchema(this.db);
      if (ddl.length > 0) this.sync.setSchema(ddl, this.schemaVersion);
    } catch (e) {
      console.warn(`[Realtime] failed to publish schema for ${this.docId}`, e);
    }
  }

  private schedulePublish(): void {
    if (this.publishTimer || this.closed) return;
    this.publishTimer = setTimeout(() => {
      this.publishTimer = null;
      void this.publishNow();
    }, PUBLISH_DEBOUNCE_MS);
  }

  /** Append batches for every change this device has produced past its cursor. */
  private async publishNow(): Promise<void> {
    if (this.closed || !this.db || !this.sync) return;
    // Every site id we own: the current one plus any we have published before
    // (crash recovery across restarts that minted new site ids).
    const ownSites = new Set<string>([this.siteHex, ...Object.keys(this.published)]);
    let publishedAny = false;
    for (const site of ownSites) {
      const since = this.published[site] ?? 0;
      let rows: ChangeRow[];
      try {
        rows = await readChangesForSite(this.db, hexToBytes(site), since);
      } catch (e) {
        console.warn(`[Realtime] reading changes for ${site} failed`, e);
        continue;
      }
      if (rows.length === 0) continue;

      // A cursor contains only db_version, so rows from one committed version
      // are indivisible: splitting them would make the receiver advance to V
      // after the first chunk and skip every later chunk ending at the same V.
      // Keep versions whole even when one unusually large transaction exceeds
      // the target row count.
      const chunks: ChangeRow[][] = [];
      let chunk: ChangeRow[] = [];
      for (let start = 0; start < rows.length; ) {
        const version = rows[start].db_version;
        let end = start + 1;
        while (end < rows.length && rows[end].db_version === version) end++;
        if (chunk.length > 0 && chunk.length + (end - start) > MAX_BATCH_ROWS) {
          chunks.push(chunk);
          chunk = [];
        }
        for (let index = start; index < end; index++) chunk.push(rows[index]);
        start = end;
      }
      if (chunk.length > 0) chunks.push(chunk);

      let fromDbVersion = since;
      for (const changes of chunks) {
        const batch: Batch = {
          id: makeBatchId(),
          siteId: site,
          fromDbVersion,
          toDbVersion: changes[changes.length - 1].db_version,
          schemaVersion: this.schemaVersion,
          changes,
          createdAt: Date.now(),
          format: SYNC_FORMAT,
        };
        this.appliedBatchIds.add(batch.id);
        this.sync.appendBatch(batch);
        this.published[site] = batch.toDbVersion;
        fromDbVersion = batch.toDbVersion;
        publishedAny = true;
      }
    }
    if (publishedAny) {
      this.sync.setCursor(this.siteHex, this.cursor);
      this.opts.touch?.();
      this.scheduleSnapshot();
    }
  }

  // --- apply remote ----------------------------------------------------------

  private applyBatches(batches: Batch[]): void {
    void this.enqueueApply(batches);
  }

  /** Run an apply pass after every previously queued pass has finished. */
  private enqueueApply(batches: Batch[]): Promise<void> {
    const next = this.applyChain.then(() => this.applyBatchesAsync(batches));
    this.applyChain = next.catch((e) => {
      console.error(`[Realtime] apply pass failed for ${this.docId}`, e);
    });
    return next;
  }

  private async applyBatchesAsync(batches: Batch[]): Promise<void> {
    if (this.closed || !this.db || !this.sync) return;
    const compacted = this.sync.getCompactedThrough();
    const touchedTables = new Set<string>();
    let cursorChanged = false;
    let needsRebase = false;

    // Group by origin site and apply each site's batches in causal order.
    // The log is an append-only array, but republish-after-restore and lost
    // updates can leave a site's batches out of db_version order; applying
    // them in array order could advance the cursor past a range that then
    // never arrives (the `toDbVersion <= applied` check would skip it
    // forever). Cross-site order is irrelevant — cr-sqlite merge is a CRDT.
    const bySite = new Map<string, Batch[]>();
    for (const batch of batches) {
      const list = bySite.get(batch.siteId);
      if (list) list.push(batch);
      else bySite.set(batch.siteId, [batch]);
    }
    for (const list of bySite.values()) {
      list.sort((x, y) => x.fromDbVersion - y.fromDbVersion || x.toDbVersion - y.toDbVersion);
    }

    for (const siteBatches of bySite.values()) {
      // A stall (gap, apply failure, or a buffered newer-schema batch) ends
      // this site's pass: the stalled batch stays unapplied and unmarked so
      // the next pass retries it, and later batches must not jump the cursor
      // past it.
      for (const batch of siteBatches) {
        if (this.appliedBatchIds.has(batch.id)) continue;
        // Own-site batches: the applied cursor doesn't track our own sites
        // (it only records *remote* progress), so use `published` as the
        // contiguity baseline instead. That includes the site ids earlier
        // sessions of this device published under (each session opens a
        // fresh database with a new site id): their rows are already local,
        // and treating them as remote would restart at 0 and read a partly
        // compacted range as a gap that needs a full rebase.
        const ownSite = batch.siteId === this.siteHex || this.published[batch.siteId] !== undefined;
        const applied = ownSite
          ? (this.published[batch.siteId] ?? 0)
          : (this.cursor[batch.siteId] ?? 0);
        if (batch.toDbVersion <= applied) {
          this.appliedBatchIds.add(batch.id);
          continue;
        }
        if (batch.format !== SYNC_FORMAT) {
          // Unknown wire format: refuse rather than corrupt.
          this.appliedBatchIds.add(batch.id);
          continue;
        }
        // Schema too new for us: buffer and surface needs-migration.
        // drainBuffered() re-runs a full pass after the schema upgrade.
        if (batch.schemaVersion > this.schemaVersion) {
          if (!this.bufferedBatches.some((b) => b.id === batch.id))
            this.bufferedBatches.push(batch);
          if (this._state !== "needs-migration") this.setState("needs-migration");
          break;
        }
        // Contiguity: a batch may only apply on top of everything before it.
        if (batch.fromDbVersion > applied) {
          // The missing range was compacted away → rebuild from server.
          if ((compacted[batch.siteId] ?? 0) >= batch.fromDbVersion) {
            needsRebase = true;
            break;
          }
          // Not compacted, so the missing batches may still arrive
          // (out-of-order publish, lost update). Retry on the next pass.
          console.warn(
            `[Realtime] gap in batch log for ${this.docId}: site ${batch.siteId} applied to ` +
              `${applied}, batch starts at ${batch.fromDbVersion}; waiting for the missing range`,
          );
          break;
        }

        try {
          await this.db.tx(async (tx) => {
            await applyChanges(tx as unknown as DB, batch.changes);
          });
        } catch (e) {
          // Stall rather than skip: the batch stays unapplied and unmarked,
          // so the next pass retries it instead of jumping the cursor past it.
          console.error(`[Realtime] failed to apply batch ${batch.id} for ${this.docId}`, e);
          break;
        }
        for (const c of batch.changes) touchedTables.add(c.table);
        if (ownSite) {
          // Advance the published watermark so publishNow doesn't republish
          // this range (the rows are now either local or idempotently merged).
          this.published[batch.siteId] = Math.max(applied, batch.toDbVersion);
        } else {
          this.cursor[batch.siteId] = batch.toDbVersion;
        }
        this.appliedBatchIds.add(batch.id);
        cursorChanged = true;
      }
      if (needsRebase) break;
    }

    if (cursorChanged) {
      this.sync.setCursor(this.siteHex, this.cursor);
      this.scheduleSnapshot();
    }
    if (needsRebase) {
      void this.rebaseFromServer();
      return;
    }
    if (touchedTables.size > 0) {
      const tables = [...touchedTables];
      for (const cb of this.remoteListeners) {
        try {
          cb({ tables });
        } catch (e) {
          console.error("[Realtime] sql onRemoteChange listener failed", e);
        }
      }
      if (this.opts.onMergeReview) {
        try {
          await this.opts.onMergeReview(tables);
        } catch (e) {
          console.error("[Realtime] onMergeReview failed", e);
        }
      }
    }
  }

  // --- snapshot --------------------------------------------------------------

  private scheduleSnapshot(): void {
    if (this.snapshotTimer || this.closed) return;
    this.snapshotTimer = setTimeout(() => {
      this.snapshotTimer = null;
      void this.persistSnapshot();
    }, SNAPSHOT_DEBOUNCE_MS);
  }

  private async persistSnapshot(): Promise<void> {
    // Note: intentionally not guarded on `this.closed` — close() flips that
    // flag before flushing the final snapshot.
    if (!this.db) return;
    try {
      const snap = await captureSnapshot(this.db, this.schemaVersion, this.cursor, this.published);
      await this.opts.saveSnapshot(serializeSnapshot(snap));
    } catch (e) {
      console.warn(`[Realtime] failed to persist snapshot for ${this.docId}`, e);
    }
  }

  // --- escape hatch ----------------------------------------------------------

  private rebasing = false;

  /** Discard local DB + snapshot and rebuild from the server replica. */
  async rebaseFromServer(): Promise<void> {
    if (this.closed || !this.sqlite || this.rebasing) return;
    this.rebasing = true;
    const previousState = this._state;
    try {
      await this.doRebase();
    } catch (error) {
      if (!this.closed) this.setState(previousState);
      throw error;
    } finally {
      this.rebasing = false;
    }
  }

  private async doRebase(): Promise<void> {
    this.setState("bootstrapping");
    if (this.publishTimer) {
      clearTimeout(this.publishTimer);
      this.publishTimer = null;
    }
    await this.publishNow();
    if (!this.opts.bootstrap) {
      throw new Error(`database ${this.pluginId}/${this.name} has no server bootstrap source`);
    }

    // Build the replacement completely before touching the live database. A
    // failed request or decode therefore leaves both the current DB and its
    // snapshot intact.
    const replacement = await openMemoryDb(this.sqlite!);
    let replacementCursor: Cursor;
    try {
      await this.runMigrateOn(replacement, 0);
      replacementCursor = await this.bootstrapInto(replacement, {});
    } catch (error) {
      await replacement.close().catch(() => {});
      throw error;
    }

    const previous = this.db;
    this.db = replacement;
    this.siteHex = replacement.siteid.toLowerCase();
    this.cursor = replacementCursor;
    this.published = {};
    this.appliedBatchIds.clear();
    this.bufferedBatches = [];
    await previous?.close().catch(() => {});

    // Re-apply the log on top of the fresh bootstrap.
    await this.enqueueApply(this.sync?.listBatches() ?? []);
    await this.publishNow();
    if (this._state !== "needs-migration") this.setState("live");
    await this.persistSnapshot();
  }

  // --- delete / restore ------------------------------------------------------

  /** Soft-delete: set the doc tombstone (the trash entry is recorded by VaultSync). */
  markDeleted(): void {
    this.sync?.setDeletedAt(Date.now());
  }

  /** Clear the tombstone so a fresh open() re-bootstraps. */
  clearDeleted(): void {
    this.sync?.clearDeletedAt();
  }

  isLive(): boolean {
    return !!this.sync && this.sync.getDeletedAt() === null;
  }

  // --- teardown --------------------------------------------------------------

  async close(): Promise<void> {
    if (this.closed) return;
    const failed = this._state === "error";
    if (this.publishTimer) {
      clearTimeout(this.publishTimer);
      this.publishTimer = null;
    }
    // Best effort even after a post-live failure: unpublished local edits
    // and the snapshot are the only copies of those rows. An engine that
    // failed BEFORE going live (e.g. bootstrap failed) must not persist —
    // restoring that snapshot later would skip the bootstrap entirely.
    if (!failed || this.becameLive) await this.publishNow().catch(() => {});
    this.closed = true;
    if (this.snapshotTimer) clearTimeout(this.snapshotTimer);
    this.unobserveBatches?.();
    this.unobserveMeta?.();
    if (!failed || this.becameLive) {
      try {
        await this.persistSnapshot();
      } catch {
        /* best effort */
      }
    }
    if (this.db) await this.db.close().catch(() => {});
    this.db = null;
    // Let any just-written updates (final batches, the delete tombstone)
    // reach the server before tearing the transport down.
    await this.sync?.whenFlushed().catch(() => {});
    this.sync?.destroy();
    this.sync = null;
    this.setState("offline");
  }
}

class WasmError extends Error {}

export class DeletedError extends Error {
  readonly reason = "deleted" as const;
  constructor(pluginId: string, name: string) {
    super(`database ${pluginId}/${name} was deleted`);
  }
}

/**
 * Extract every identifier-like token from `sql`, skipping single-quoted
 * string literals (with `''` escapes), `--` line comments, and block
 * comments. Bare identifiers and quoted identifiers (`"…"`, `` `…` ``,
 * `[…]` — SQLite treats all three as identifiers) are both yielded.
 * Mirrors `sql_identifiers` in `server/src/plugindb.rs`.
 */
export function sqlIdentifiers(sql: string): string[] {
  const out: string[] = [];
  const n = sql.length;
  let i = 0;
  while (i < n) {
    const c = sql[i];
    if (c === "-" && sql[i + 1] === "-") {
      while (i < n && sql[i] !== "\n") i++;
    } else if (c === "/" && sql[i + 1] === "*") {
      i += 2;
      while (i + 1 < n && !(sql[i] === "*" && sql[i + 1] === "/")) i++;
      i = Math.min(i + 2, n);
    } else if (c === "'") {
      i++;
      while (i < n) {
        if (sql[i] === "'") {
          if (sql[i + 1] === "'") {
            i += 2;
            continue;
          }
          i++;
          break;
        }
        i++;
      }
    } else if (c === '"' || c === "`" || c === "[") {
      const close = c === "[" ? "]" : c;
      i++;
      let ident = "";
      while (i < n) {
        if (sql[i] === close) {
          if (close !== "]" && sql[i + 1] === close) {
            ident += close;
            i += 2;
            continue;
          }
          i++;
          break;
        }
        ident += sql[i];
        i++;
      }
      out.push(ident);
    } else if (/[A-Za-z_]/.test(c)) {
      const start = i;
      while (i < n && /[A-Za-z0-9_$]/.test(sql[i])) i++;
      out.push(sql.slice(start, i));
    } else {
      i++;
    }
  }
  return out;
}

/** Lexicographically-sortable-ish unique id (timestamp + random). */
function makeBatchId(): string {
  const t = Date.now().toString(36).padStart(9, "0");
  const r = Math.random().toString(36).slice(2, 10);
  return `${t}-${r}`;
}

/** Build a bare in-memory doc handle (tests / offline use). */
export function makeMemoryDocHandle(doc: Y.Doc): PluginDbDocHandle {
  return {
    doc,
    whenSynced: Promise.resolve(),
    isConnected: () => true,
    onStatus: () => () => {},
    destroy: () => {},
  };
}
