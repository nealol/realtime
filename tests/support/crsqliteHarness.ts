// Shared helpers for plugin-db unit tests: a node-loadable cr-sqlite WASM data
// URL and an in-memory SyncedPluginDatabase factory wired through a Y.Doc.

import * as fs from "fs";
import * as Y from "yjs";
import {
  SyncedPluginDatabase,
  makeMemoryDocHandle,
  type SyncedPluginDatabaseOptions,
} from "../../src/pluginDb/SyncedPluginDatabase";

const wasmBytes = fs.readFileSync("node_modules/@vlcn.io/crsqlite-wasm/dist/crsqlite.wasm");
export const WASM_DATA_URL =
  "data:application/wasm;base64," + Buffer.from(wasmBytes).toString("base64");

export interface SnapStore {
  text: string | null;
}

export function newSnapStore(): SnapStore {
  return { text: null };
}

const TASKS_MIGRATE: SyncedPluginDatabaseOptions["migrate"] = async (tx, from) => {
  if (from < 1) {
    await tx.exec(`CREATE TABLE tasks (id PRIMARY KEY NOT NULL, title, done)`);
    await tx.exec(`SELECT crsql_as_crr('tasks')`);
  }
};

export interface MakeEngineOpts {
  /** The batch log's document, or a function naming the current one. */
  doc: Y.Doc | (() => Y.Doc);
  snap?: SnapStore;
  schemaVersion?: number;
  migrate?: SyncedPluginDatabaseOptions["migrate"];
  bootstrap?: SyncedPluginDatabaseOptions["bootstrap"];
  name?: string;
  pluginId?: string;
}

export function makeEngine(opts: MakeEngineOpts): SyncedPluginDatabase {
  const snap = opts.snap ?? newSnapStore();
  const doc = opts.doc;
  return new SyncedPluginDatabase({
    vaultId: "vault",
    pluginId: opts.pluginId ?? "test-plugin",
    name: opts.name ?? "tasks",
    schemaVersion: opts.schemaVersion ?? 1,
    migrate: opts.migrate ?? TASKS_MIGRATE,
    locateWasm: () => WASM_DATA_URL,
    makeDoc: () => makeMemoryDocHandle(typeof doc === "function" ? doc() : doc),
    loadSnapshot: async () => snap.text,
    saveSnapshot: async (text) => {
      snap.text = text;
    },
    deleteSnapshot: async () => {
      snap.text = null;
    },
    bootstrap: opts.bootstrap,
  });
}

/** Two Y.Docs joined by a relay that can be paused to simulate going offline. */
export function makeRelay(): {
  a: Y.Doc;
  b: Y.Doc;
  pause: () => void;
  resume: () => void;
} {
  const a = new Y.Doc();
  const b = new Y.Doc();
  let paused = false;
  a.on("update", (u: Uint8Array, origin: unknown) => {
    if (!paused && origin !== "relay") Y.applyUpdate(b, u, "relay");
  });
  b.on("update", (u: Uint8Array, origin: unknown) => {
    if (!paused && origin !== "relay") Y.applyUpdate(a, u, "relay");
  });
  return {
    a,
    b,
    pause: () => {
      paused = true;
    },
    resume: () => {
      paused = false;
      // Exchange full state to make up for updates dropped while paused.
      Y.applyUpdate(b, Y.encodeStateAsUpdate(a), "relay");
      Y.applyUpdate(a, Y.encodeStateAsUpdate(b), "relay");
    },
  };
}
