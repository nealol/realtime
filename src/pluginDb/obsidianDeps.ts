/**
 * Wiring that binds the (Obsidian-agnostic) {@link SyncedPluginDatabase} engine
 * to the live plugin: WASM resolution, snapshot persistence via the vault
 * adapter, the per-database sync transport, and the server bootstrap/touch calls.
 */

import * as Y from "yjs";
import {
  RealtimeProvider,
  SYNC_EVENT_STATUS,
  SYNC_EVENT_LOCAL_CHANGES,
  SYNC_STATUS_CONNECTED,
  type SyncStatus,
} from "../sync/RealtimeProvider";
import { IndexeddbPersistence, clearDocument } from "y-indexeddb";
import type RealtimePlugin from "../main";
import { getClientToken } from "../sync/clientToken";
import { createMuxSocket } from "../sync/mux";
import { epochPersistenceName, getDocumentEpoch } from "../documentEpoch";
import { CRSQLITE_WASM_DATA_URL } from "./wasmBinary";
import type { PluginDbDocHandle } from "./PluginDbSync";
import type { SyncedPluginDatabaseOptions } from "./SyncedPluginDatabase";
import type { Cursor } from "./types";

/**
 * Build the non-user-supplied engine deps for a given database.
 *
 * `locateWasm` returns the esbuild-inlined cr-sqlite WASM data URL — a single
 * synchronous value, so there is no separate asset, download, or async race.
 */
export function buildEngineDeps(
  plugin: RealtimePlugin,
  pluginId: string,
  name: string,
): Pick<
  SyncedPluginDatabaseOptions,
  | "locateWasm"
  | "makeDoc"
  | "loadSnapshot"
  | "saveSnapshot"
  | "deleteSnapshot"
  | "bootstrap"
  | "touch"
> {
  const vaultId = plugin.settings.activeVaultId;
  const adapter = plugin.app.vault.adapter;
  const dir = plugin.manifest.dir ?? `.obsidian/plugins/${plugin.manifest.id}`;
  const serverScope = encodeURIComponent(
    plugin.settings.authServerId || plugin.settings.authServerUrl,
  );
  const vaultScope = encodeURIComponent(vaultId);
  const scope = `server-${serverScope}/vault-${vaultScope}`;
  const dbDir = `${dir}/plugin-dbs/${scope}/${pluginId}`;
  const snapPath = `${dbDir}/${name}.snap`;
  const tmpPath = `${snapPath}.tmp`;

  return {
    locateWasm: () => CRSQLITE_WASM_DATA_URL,
    makeDoc: (docId) => makeDocHandle(plugin, docId),
    loadSnapshot: async () => {
      try {
        if (await adapter.exists(snapPath)) return await adapter.read(snapPath);
        // Crash recovery: the tmp file is only ever a fully-written snapshot
        // (write tmp → remove target → rename), so trust it if the final
        // file went missing mid-replace.
        if (await adapter.exists(tmpPath)) {
          console.warn("[Realtime] recovering plugin-db snapshot from tmp file");
          return await adapter.read(tmpPath);
        }
      } catch (e) {
        console.warn("[Realtime] snapshot read failed", e);
      }
      return null;
    },
    saveSnapshot: async (text) => {
      if (!(await adapter.exists(dbDir))) await adapter.mkdir(dbDir);
      // Write the full snapshot to a tmp file first, then swap it into
      // place with rename. A crash at any point leaves either the old
      // snapshot or a complete tmp file (recovered by loadSnapshot).
      await adapter.write(tmpPath, text);
      try {
        if (await adapter.exists(snapPath)) await adapter.remove(snapPath);
        await adapter.rename(tmpPath, snapPath);
      } catch {
        // Some adapters/platforms refuse the rename; fall back to a plain
        // write and keep the tmp file as the recovery copy until it lands.
        await adapter.write(snapPath, text);
        try {
          await adapter.remove(tmpPath);
        } catch {
          /* ignore */
        }
      }
    },
    deleteSnapshot: async () => {
      try {
        if (await adapter.exists(snapPath)) await adapter.remove(snapPath);
        if (await adapter.exists(tmpPath)) await adapter.remove(tmpPath);
      } catch (e) {
        console.warn("[Realtime] snapshot delete failed", e);
      }
    },
    bootstrap: (cursor: Cursor) => plugin.auth.pluginDbChanges(vaultId, pluginId, name, cursor),
    touch: () => {
      void plugin.auth.touchPluginDb(vaultId, pluginId, name).catch(() => {});
    },
  };
}

/** A per-database Realtime + IndexedDB transport handle. */
function makeDocHandle(plugin: RealtimePlugin, docId: string): PluginDbDocHandle {
  const doc = new Y.Doc();
  // Pin the epoch: after an epoch change this handle holds retired-epoch
  // state and must be replaced, never reconnected.
  const epoch = getDocumentEpoch(plugin, docId);
  const provider = new RealtimeProvider(
    docId,
    doc,
    () => getClientToken(plugin, docId, undefined, epoch),
    { connect: false, socketFactory: createMuxSocket },
  );
  // A database has no viewers to show presence to.
  provider.awareness.setLocalState(null);
  const serverScope = plugin.settings.authServerId || plugin.settings.authServerUrl;
  const storeBase = `realtime:plugindb:${serverScope}:${docId}`;
  const persistence = new IndexeddbPersistence(
    epochPersistenceName(plugin, docId, storeBase, epoch),
    doc,
  );
  if (epoch > 0) {
    // A retired epoch's store is never connected again.
    void clearDocument(epochPersistenceName(plugin, docId, storeBase, epoch - 1)).catch(() => {});
  }

  let connected = false;
  let destroyed = false;
  let reconnectTimer: number | null = null;
  const statusCbs = new Set<(c: boolean) => void>();
  const statusListener = (status: SyncStatus) => {
    const c = status === SYNC_STATUS_CONNECTED;
    if (c !== connected) {
      connected = c;
      for (const cb of statusCbs) cb(c);
    }
  };
  provider.on(SYNC_EVENT_STATUS, statusListener);

  const connectAfterCapabilityCheck = async (): Promise<void> => {
    if (destroyed) return;
    try {
      await plugin.auth.serverInfoChecked(plugin.settings.authServerUrl);
      if (destroyed) return;
      void provider.connect();
      await firstSyncedOrTimeout(provider, 3000);
    } catch {
      if (!destroyed && reconnectTimer === null) {
        reconnectTimer = window.setTimeout(() => {
          reconnectTimer = null;
          void connectAfterCapabilityCheck();
        }, 3000);
      }
    }
  };

  const whenSynced = persistence.whenSynced.catch(() => {}).then(connectAfterCapabilityCheck);

  return {
    doc,
    whenSynced,
    isConnected: () => connected,
    onStatus: (cb) => {
      statusCbs.add(cb);
      return () => statusCbs.delete(cb);
    },
    // Wait (bounded) until the provider has no unacknowledged local changes,
    // so a final write right before destroy() — e.g. the delete tombstone —
    // actually reaches the server instead of dying with the WebSocket.
    whenFlushed: () => {
      if (!provider.hasLocalChanges) return Promise.resolve();
      let resolve!: () => void;
      const promise = new Promise<void>((doneResolve) => {
        resolve = doneResolve;
      });
      let done = false;
      const finish = () => {
        if (done) return;
        done = true;
        provider.off(SYNC_EVENT_LOCAL_CHANGES, onChange);
        resolve();
      };
      const onChange = (has: boolean) => {
        if (!has) finish();
      };
      provider.on(SYNC_EVENT_LOCAL_CHANGES, onChange);
      window.setTimeout(finish, 3000);
      return promise;
    },
    destroy: () => {
      destroyed = true;
      window.clearTimeout(reconnectTimer ?? undefined);
      reconnectTimer = null;
      provider.off(SYNC_EVENT_STATUS, statusListener);
      provider.destroy();
      void persistence.destroy();
      doc.destroy();
    },
  };
}

/** Resolve on the provider's first server sync, or after `ms` to stay offline-tolerant. */
function firstSyncedOrTimeout(provider: RealtimeProvider, ms: number): Promise<void> {
  let resolve!: () => void;
  const promise = new Promise<void>((doneResolve) => {
    resolve = doneResolve;
  });
  let done = false;
  const finish = () => {
    if (done) return;
    done = true;
    provider.off("synced", onSynced);
    resolve();
  };
  const onSynced = (synced: boolean) => {
    if (synced) finish();
  };
  provider.on("synced", onSynced);
  window.setTimeout(finish, ms);
  return promise;
}
