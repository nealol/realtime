import { afterAll, beforeAll, describe, expect, it } from "vitest";
import * as Y from "yjs";
import { BaseDocument } from "../../src/BaseDocument";
import { CanvasDocument } from "../../src/CanvasDocument";
import type RealtimePlugin from "../../src/main";
import { RealtimeProvider } from "../../src/sync/RealtimeProvider";
import { getClientToken } from "../../src/sync/clientToken";
import { parseBase, serializeBase } from "../../src/structured/base";
import { parseCanvas, reconcileCanvas, serializeCanvas } from "../../src/structured/canvas";
import { jsonEqual, reconcileInto, toValue, type JsonValue } from "../../src/structured/reconcile";
import { makeFakePlugin, type FakePlugin, type FakeVault } from "../support/fakePlugin";
import { startAuthHarness, type AuthHarness } from "../support/authServer";
import { freshGuid, waitFor } from "../support/util";

let harness: AuthHarness;
let token: string;
let vaultId: string;
let peerPlugin: FakePlugin;

beforeAll(async () => {
  harness = await startAuthHarness();
  token = await harness.loginUser("structured-live");
  vaultId = (await harness.createVault(token, "structured-live")).id;
  peerPlugin = makeFakePlugin(harness.authUrl, {
    sessionToken: token,
    activeVaultId: vaultId,
  }).plugin;
}, 180_000);

afterAll(async () => {
  await harness?.stop();
});

const docId = (guid: string) => `${vaultId}__${guid}`;

/** Test boundary: the fake implements the slice of the plugin documents use. */
const asPlugin = (plugin: FakePlugin): RealtimePlugin => plugin as unknown as RealtimePlugin;

function newPlugin(): { plugin: FakePlugin; vault: FakeVault } {
  return makeFakePlugin(harness.authUrl, { sessionToken: token, activeVaultId: vaultId });
}

/** Another device editing the structured root directly. */
class Peer {
  readonly doc = new Y.Doc();
  readonly provider: RealtimeProvider;

  constructor(serverDocId: string) {
    this.provider = new RealtimeProvider(serverDocId, this.doc, () =>
      getClientToken(asPlugin(peerPlugin), serverDocId),
    );
  }

  get value(): JsonValue {
    return toValue(this.doc.getMap("root"));
  }

  setCanvas(data: unknown): void {
    this.doc.transact(() =>
      reconcileCanvas(
        this.doc.getMap("root"),
        parseCanvas(JSON.stringify(data)),
        this.doc.clientID,
      ),
    );
  }

  setBase(value: JsonValue): void {
    this.doc.transact(() => reconcileInto(this.doc.getMap("root"), value));
  }

  destroy(): void {
    this.provider.destroy();
    this.doc.destroy();
  }
}

const node = (id: string, extra: Record<string, JsonValue> = {}) => ({
  id,
  type: "text",
  text: id,
  x: 0,
  y: 0,
  width: 120,
  height: 60,
  ...extra,
});

/** Sorted node ids of a CRDT value. */
const crdtNodeIds = (value: JsonValue) =>
  Object.keys(parseCanvas(serializeCanvas(value)).nodes).sort();
/** Sorted node ids of JSON Canvas data (view data or file content). */
const canvasNodeIds = (data: unknown) =>
  Object.keys(parseCanvas(typeof data === "string" ? data : JSON.stringify(data)).nodes).sort();
const crdtEdgeIds = (value: JsonValue) =>
  Object.keys(parseCanvas(serializeCanvas(value)).edges).sort();

/**
 * An open Canvas view shaped like Obsidian's: `requestSave` only schedules
 * the (debounced) file write; {@link flushSave} performs it and reports the
 * modify event the way VaultSync forwards it.
 */
function openCanvasView(vault: FakeVault, path: string, initial: unknown) {
  let data = structuredClone(initial);
  let savePending = false;
  const canvas = {
    getData: () => structuredClone(data),
    importData: (next: unknown) => {
      data = structuredClone(next);
    },
    requestSave: () => {
      savePending = true;
    },
    requestFrame: () => {},
  };
  const view = {
    getViewType: () => "canvas",
    file: { path },
    canvas,
    containerEl: document.createElement("div"),
  };
  const workspace = {
    on: () => ({}),
    iterateAllLeaves: (callback: (leaf: unknown) => void) => callback({ view }),
    getLeavesOfType: (type: string) => (type === "canvas" ? [{ view }] : []),
  };
  return {
    workspace,
    data: () => structuredClone(data),
    savePending: () => savePending,
    /** A local edit in the view, which calls (the patched) requestSave. */
    edit(next: unknown) {
      data = structuredClone(next);
      view.canvas.requestSave();
    },
    async flushSave(doc: CanvasDocument) {
      savePending = false;
      vault.files.set(path, `${JSON.stringify(data, null, "\t")}\n`);
      await doc.onDiskChanged();
    },
  };
}

function openBasesView(vault: FakeVault, path: string, initial: string) {
  let text = initial;
  let savePending = false;
  const pushed: string[] = [];
  const view = {
    getViewType: () => "bases",
    file: { path },
    getViewData: () => text,
    setViewData: (data: string, _clear: boolean) => {
      pushed.push(data);
      text = data;
    },
    requestSave: () => {
      savePending = true;
    },
  };
  const workspace = {
    on: () => ({}),
    iterateAllLeaves: (callback: (leaf: unknown) => void) => callback({ view }),
    getLeavesOfType: (type: string) => (type === "bases" ? [{ view }] : []),
  };
  return {
    workspace,
    text: () => text,
    pushed,
    savePending: () => savePending,
    edit(next: string) {
      text = next;
      view.requestSave();
    },
    async flushSave(doc: BaseDocument) {
      savePending = false;
      vault.files.set(path, text);
      await doc.onDiskChanged();
    },
  };
}

describe("Canvas edits made before the first sync", () => {
  it("keeps saved and unsaved edits of an open Canvas and merges remote changes", async () => {
    const guid = freshGuid();
    const path = "Board.canvas";
    const { plugin, vault } = newPlugin();
    const seed = { nodes: [node("a"), node("b")], edges: [] };
    vault.files.set(path, serializeCanvas(parseCanvas(JSON.stringify(seed))));
    const first = new CanvasDocument(asPlugin(plugin), path, guid, docId(guid), true);
    const peer = new Peer(docId(guid));
    let second: CanvasDocument | null = null;
    try {
      await first.whenReady();
      await waitFor(() => crdtNodeIds(peer.value).join() === "a,b", { label: "peer has seed" });
      first.destroy();

      // Another device adds a node while this one is closed.
      peer.setCanvas({ nodes: [node("a"), node("b"), node("r")], edges: [] });
      await waitFor(() => !peer.provider.hasLocalChanges, { label: "remote node stored" });

      // The Canvas is already open when the plugin starts again.
      const view = openCanvasView(vault, path, seed);
      plugin.app.workspace = view.workspace;
      second = new CanvasDocument(asPlugin(plugin), path, guid, docId(guid), false, {
        autoConnect: false,
      });
      const current = second;
      await waitFor(() => current.matchesStartupDisk(parseCanvas(vault.files.get(path)!)), {
        label: "startup disk read",
      });

      // A saved edit, then an unsaved one whose autosave has not run yet.
      view.edit({ nodes: [node("a"), node("b"), node("saved")], edges: [] });
      await view.flushSave(current);
      view.edit({
        nodes: [node("a", { x: 500 }), node("b"), node("saved"), node("unsaved")],
        edges: [],
      });
      // Nothing is folded into the stored state before startup completes.
      expect(crdtNodeIds(current.value)).toEqual(["a", "b"]);

      current.connect();
      await current.whenReady();
      const expected = ["a", "b", "r", "saved", "unsaved"];
      await waitFor(() => canvasNodeIds(view.data()).join() === expected.join(), {
        label: "view shows the merged canvas",
      });
      expect(crdtNodeIds(current.value)).toEqual(expected);
      expect(parseCanvas(serializeCanvas(current.value)).nodes.a).toMatchObject({ x: 500 });
      await view.flushSave(current);
      expect(canvasNodeIds(vault.files.get(path))).toEqual(expected);
      await waitFor(() => crdtNodeIds(peer.value).join() === expected.join(), {
        label: "peer receives the pre-startup edits",
      });
    } finally {
      second?.destroy();
      first.destroy();
      peer.destroy();
    }
  });

  it("never imports a reused view's leftover content from another file", async () => {
    const guid = freshGuid();
    const path = "Fresh.canvas";
    const { plugin, vault } = newPlugin();
    vault.files.set(path, serializeCanvas(parseCanvas(JSON.stringify({ nodes: [node("a")] }))));
    // Obsidian already points the view at this file but still shows the last one.
    const view = openCanvasView(vault, path, { nodes: [node("other")], edges: [] });
    plugin.app.workspace = view.workspace;
    const doc = new CanvasDocument(asPlugin(plugin), path, guid, docId(guid), true, {
      autoConnect: false,
    });
    const peer = new Peer(docId(guid));
    try {
      await waitFor(() => doc.matchesStartupDisk(parseCanvas(vault.files.get(path)!)), {
        label: "startup disk read",
      });
      view.edit({ nodes: [node("other", { x: 10 })], edges: [] });
      doc.connect();
      await doc.whenReady();
      await waitFor(() => canvasNodeIds(view.data()).join() === "a", {
        label: "view shows this file",
      });
      expect(crdtNodeIds(doc.value)).toEqual(["a"]);
      expect(canvasNodeIds(vault.files.get(path))).toEqual(["a"]);
      await waitFor(() => crdtNodeIds(peer.value).join() === "a", { label: "peer has the file" });
    } finally {
      doc.destroy();
      peer.destroy();
    }
  });
});

describe("Canvas deletions after a restart", () => {
  it("propagates disk deletions of nodes and edges created before the restart", async () => {
    const guid = freshGuid();
    const path = "Restart.canvas";
    const { plugin, vault } = newPlugin();
    const seed = {
      nodes: [node("a"), node("b"), node("c")],
      edges: [{ id: "e", fromNode: "a", toNode: "b" }],
    };
    vault.files.set(path, serializeCanvas(parseCanvas(JSON.stringify(seed))));
    const first = new CanvasDocument(asPlugin(plugin), path, guid, docId(guid), true);
    const peer = new Peer(docId(guid));
    let second: CanvasDocument | null = null;
    const collaborator = new Y.Doc();
    try {
      await first.whenReady();
      await waitFor(() => crdtEdgeIds(peer.value).join() === "e", { label: "peer has seed" });
      first.destroy();

      // While the app is closed, b and its edge are deleted on disk.
      vault.files.set(
        path,
        serializeCanvas(parseCanvas(JSON.stringify({ nodes: [node("a"), node("c")] }))),
      );
      second = new CanvasDocument(asPlugin(plugin), path, guid, docId(guid), false);
      const current = second;
      await current.whenReady();
      expect(crdtNodeIds(current.value)).toEqual(["a", "c"]);
      expect(crdtEdgeIds(current.value)).toEqual([]);
      // The startup reconcile wrote the merged file before reporting ready.
      expect(canvasNodeIds(vault.files.get(path))).toEqual(["a", "c"]);
      await waitFor(
        () => crdtNodeIds(peer.value).join() === "a,c" && crdtEdgeIds(peer.value).length === 0,
        { label: "peer receives the startup deletion" },
      );

      // Now c is deleted on disk while a collaborator's new node arrives
      // during the fold: the deletion applies, the concurrent node stays.
      Y.applyUpdate(collaborator, Y.encodeStateAsUpdate(current.ydoc));
      vault.files.set(path, serializeCanvas(parseCanvas(JSON.stringify({ nodes: [node("a")] }))));
      const read = vault.read.bind(vault);
      let reads = 0;
      vault.read = async (file) => {
        const text = await read(file);
        reads += 1;
        if (reads === 2) {
          const before = Y.encodeStateVector(current.ydoc);
          collaborator.transact(() =>
            reconcileCanvas(
              collaborator.getMap("root"),
              parseCanvas(JSON.stringify({ nodes: [node("a"), node("c"), node("r")] })),
              collaborator.clientID,
            ),
          );
          Y.applyUpdate(
            current.ydoc,
            Y.encodeStateAsUpdate(collaborator, before),
            current.provider,
          );
        }
        return text;
      };
      await current.onDiskChanged();
      vault.read = read;

      expect(crdtNodeIds(current.value)).toEqual(["a", "r"]);
      await waitFor(() => canvasNodeIds(vault.files.get(path)).join() === "a,r", {
        label: "disk holds the deletion and the concurrent node",
      });
      await waitFor(() => !crdtNodeIds(peer.value).includes("c"), {
        label: "peer receives the disk deletion",
      });
    } finally {
      second?.destroy();
      first.destroy();
      peer.destroy();
      collaborator.destroy();
    }
  });
});

describe("Bases edits made before the first sync", () => {
  it("never pushes pre-load content into an open Bases view and keeps its edits", async () => {
    const guid = freshGuid();
    const path = "views.base";
    const { plugin, vault } = newPlugin();
    const initial = serializeBase({ formulas: { a: "1" } });
    vault.files.set(path, initial);
    const first = new BaseDocument(asPlugin(plugin), path, guid, docId(guid), true);
    const peer = new Peer(docId(guid));
    let second: BaseDocument | null = null;
    try {
      await first.whenReady();
      await waitFor(() => JSON.stringify(peer.value) === '{"formulas":{"a":"1"}}', {
        label: "peer has the base",
      });
      first.destroy();
      peer.setBase({ formulas: { a: "1", c: "3" } });
      await waitFor(() => !peer.provider.hasLocalChanges, { label: "remote edit stored" });

      const view = openBasesView(vault, path, initial);
      plugin.app.workspace = view.workspace;
      second = new BaseDocument(asPlugin(plugin), path, guid, docId(guid), false, {
        autoConnect: false,
      });
      const current = second;
      await waitFor(() => current.matchesStartupDisk(parseBase(vault.files.get(path)!)), {
        label: "startup disk read",
      });
      expect(view.pushed).toEqual([]);
      expect(view.savePending()).toBe(false);

      // An unsaved edit in the view before startup completes.
      view.edit(serializeBase({ formulas: { a: "1", b: "2" } }));
      expect(current.value).toEqual({ formulas: { a: "1" } });

      current.connect();
      await current.whenReady();
      const expected = { formulas: { a: "1", b: "2", c: "3" } };
      await waitFor(() => jsonEqual(parseBase(view.text()), expected), {
        label: "view shows the merged base",
      });
      expect(view.pushed).toHaveLength(1);
      expect(parseBase(view.pushed[0])).toEqual(expected);
      expect(current.value).toEqual(expected);
      await view.flushSave(current);
      expect(parseBase(vault.files.get(path)!)).toEqual(expected);
      await waitFor(() => jsonEqual(peer.value, expected), {
        label: "peer receives the pre-startup edit",
      });
      expect(view.pushed).toHaveLength(1);
    } finally {
      second?.destroy();
      first.destroy();
      peer.destroy();
    }
  });
});
