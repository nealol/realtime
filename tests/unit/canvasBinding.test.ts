import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import * as Y from "yjs";
import { Awareness } from "y-protocols/awareness";
import {
  CANVAS_LOCAL_ORIGIN,
  CanvasBinding,
  canvasAttachmentPaths,
} from "../../src/editor/CanvasBinding";
import type { CanvasDocument } from "../../src/CanvasDocument";
import type RealtimePlugin from "../../src/main";
import { reconcileCanvas, serializeCanvas } from "../../src/structured/canvas";
import { applyCanvasOperations, type CanvasOperation } from "../../src/structured/canvasOperations";
import { toValue } from "../../src/structured/reconcile";
import { makeLiveCanvasFixture, normalizedCanvas } from "../support/canvasHarness";

/**
 * Test boundary: the binding reads only `app.workspace` from the plugin and
 * the startup/readiness methods the fixture document provides.
 */
function bindFixture(workspace: unknown, doc: object): CanvasBinding {
  const plugin = { app: { workspace } } as unknown as RealtimePlugin;
  return new CanvasBinding(plugin, doc as unknown as CanvasDocument);
}

function makeOperationalDoc(data: unknown) {
  let current = data;
  return {
    path: "Board.canvas",
    awareness: new Awareness(new Y.Doc()),
    isReady: () => true,
    canvasData: () => current,
    setRemote: (next: unknown) => {
      current = next;
    },
    applyCanvasOperations: vi.fn(),
    reconcileFromCanvasData: vi.fn(),
  };
}

interface CanvasFile {
  nodes: Array<Record<string, unknown>>;
  edges: Array<Record<string, unknown>>;
}

/**
 * A Yjs-backed document mirroring CanvasDocument's capture/apply surface, so
 * tests observe the CRDT value and its local transactions, not call wiring.
 */
function makeCrdtDoc(initial: unknown) {
  const ydoc = new Y.Doc();
  const root = ydoc.getMap("root");
  const value = (): CanvasFile => JSON.parse(serializeCanvas(toValue(root)));
  ydoc.transact(() => reconcileCanvas(root, normalizedCanvas(initial), ydoc.clientID));
  /** The CRDT value after every local transaction that changed it. */
  const localStates: CanvasFile[] = [];
  ydoc.on("afterTransaction", (transaction: Y.Transaction) => {
    if (transaction.origin === CANVAS_LOCAL_ORIGIN && transaction.changed.size) {
      localStates.push(value());
    }
  });
  return {
    path: "Board.canvas",
    awareness: new Awareness(new Y.Doc()),
    isReady: () => true,
    canvasData: value,
    applyCanvasOperations: (operations: readonly CanvasOperation[], origin: unknown) =>
      ydoc.transact(() => applyCanvasOperations(root, operations, ydoc.clientID), origin),
    reconcileFromCanvasData: (data: unknown, origin: unknown) =>
      ydoc.transact(
        () => reconcileCanvas(root, normalizedCanvas(data ?? {}), ydoc.clientID),
        origin,
      ),
    /** A peer's edit arriving through sync. */
    applyPeer: (data: unknown) =>
      ydoc.transact(() => reconcileCanvas(root, normalizedCanvas(data), ydoc.clientID), "peer"),
    value,
    localStates,
  };
}

const node = (x: number, text = "A") => ({ id: "a", type: "text", text, x, y: 0 });

describe("CanvasBinding startup", () => {
  it("falls back safely when Canvas data APIs move to the view", () => {
    let data = { nodes: [{ id: "a", type: "text", text: "old", x: 0 }], edges: [] };
    const rawCanvas = { getData: {}, importData: {}, requestSave: vi.fn() };
    const host = document.createElement("div");
    const view = {
      canvas: rawCanvas,
      containerEl: host,
      file: { path: "Board.canvas" },
      getViewType: () => "canvas",
      getViewData: () => JSON.stringify(data),
      setViewData: vi.fn((value: string) => {
        data = JSON.parse(value);
      }),
    };
    const workspace = {
      iterateAllLeaves: (callback: (leaf: unknown) => void) => callback({ view }),
      getLeavesOfType: () => [],
    };
    const doc = makeOperationalDoc(data);
    const binding = new CanvasBinding({ app: { workspace } } as any, doc as any);
    binding.tryBind();
    expect(binding.isActive()).toBe(false);
    rawCanvas.requestSave();
    expect(doc.applyCanvasOperations).not.toHaveBeenCalled();
    binding.destroy();
    doc.awareness.destroy();
  });

  it("discovers unique Canvas file-node attachment paths", () => {
    expect(
      canvasAttachmentPaths({
        nodes: {
          a: { id: "a", type: "file", file: "img/a.png" },
          b: { id: "b", type: "file", file: "img/a.png" },
          c: { id: "c", type: "text", text: "ignored" },
        },
        edges: {},
        nodeOrder: ["a", "b", "c"],
        edgeOrder: [],
      }),
    ).toEqual(["img/a.png"]);
  });

  it("prioritizes only binary Canvas references selected by VaultSync", () => {
    const initial = {
      nodes: [
        { id: "note", type: "file", file: "note.md" },
        { id: "image", type: "file", file: "image.png" },
      ],
      edges: [],
    };
    const fixture = makeLiveCanvasFixture("Board.canvas", initial);
    const doc = makeOperationalDoc(initial);
    const vaultSync = {
      canvasBinaryPaths: vi.fn(() => ["image.png"]),
      prioritizeCanvasAttachments: vi.fn(),
      unprioritizeCanvasAttachments: vi.fn(),
    };
    let attachmentExists = false;
    let refreshAttachments = () => {};
    const vault = {
      getAbstractFileByPath: vi.fn(() => (attachmentExists ? { path: "image.png" } : null)),
      on: vi.fn((_event: string, callback: () => void) => {
        refreshAttachments = callback;
        return {};
      }),
      offref: vi.fn(),
    };
    const binding = new CanvasBinding(
      {
        app: {
          workspace: fixture.workspace,
          vault,
        },
        vaultSync,
      } as any,
      doc as any,
    );
    binding.tryBind();
    expect(vaultSync.canvasBinaryPaths).toHaveBeenCalledWith(["image.png", "note.md"]);
    expect(vaultSync.prioritizeCanvasAttachments).toHaveBeenCalledWith(["image.png"]);
    expect(fixture.host.querySelector(".realtime-canvas-attachment-status")).not.toBeNull();
    attachmentExists = true;
    refreshAttachments();
    expect(fixture.host.querySelector(".realtime-canvas-attachment-status")).toBeNull();
    binding.destroy();
    expect(vault.offref).toHaveBeenCalledTimes(3);
  });

  it("captures edits from every open view of the same Canvas", () => {
    const initial = { nodes: [{ id: "a", type: "text", text: "initial" }], edges: [] };
    let firstData = structuredClone(initial);
    let secondData = structuredClone(initial);
    const makeCanvas = (read: () => unknown, write: (value: any) => void) => ({
      getData: vi.fn(() => structuredClone(read())),
      importData: vi.fn((value: unknown) => write(structuredClone(value))),
      requestSave: vi.fn(),
      requestFrame: vi.fn(),
    });
    const first = makeCanvas(
      () => firstData,
      (value) => (firstData = value),
    );
    const second = makeCanvas(
      () => secondData,
      (value) => (secondData = value),
    );
    const views = [first, second].map((canvas) => ({
      getViewType: () => "canvas",
      file: { path: "Board.canvas" },
      canvas,
      containerEl: document.createElement("div"),
    }));
    const workspace = {
      iterateAllLeaves: (callback: (leaf: unknown) => void) => {
        for (const view of views) callback({ view });
      },
      getLeavesOfType: () => views.map((view) => ({ view })),
    };
    const doc = makeOperationalDoc(initial);
    const binding = new CanvasBinding({ app: { workspace } } as any, doc as any);
    binding.tryBind();
    expect(binding.isActive()).toBe(true);
    doc.applyCanvasOperations.mockClear();

    secondData = { nodes: [{ id: "a", type: "text", text: "from second" }], edges: [] };
    second.requestSave();
    expect(doc.applyCanvasOperations).toHaveBeenCalledWith(
      [
        {
          type: "node-patch",
          id: "a",
          patch: { set: { text: "from second" }, remove: [] },
        },
      ],
      expect.anything(),
    );

    binding.destroy();
    doc.awareness.destroy();
  });

  it("keeps disk folding enabled when any open Canvas view is unsupported", () => {
    const initial = { nodes: [{ id: "a", type: "text", text: "initial" }], edges: [] };
    const fixture = makeLiveCanvasFixture("Board.canvas", initial);
    const unsupportedView = {
      getViewType: () => "canvas",
      file: { path: "Board.canvas" },
      canvas: { requestSave: vi.fn() },
      containerEl: document.createElement("div"),
    };
    const workspace = {
      iterateAllLeaves: (callback: (leaf: unknown) => void) => {
        fixture.workspace.iterateAllLeaves(callback);
        callback({ view: unsupportedView });
      },
      getLeavesOfType: () => [],
    };
    const doc = makeOperationalDoc(initial);
    const binding = new CanvasBinding({ app: { workspace } } as any, doc as any);
    binding.tryBind();
    expect(binding.isActive()).toBe(false);
    binding.destroy();
    doc.awareness.destroy();
  });

  it("does not import CRDT data into an open canvas before the document is ready", () => {
    const host = document.createElement("div");
    const originalRequestSave = vi.fn();
    const canvas = {
      getData: vi.fn(() => ({
        nodes: [{ id: "disk", type: "text", text: "from disk" }],
        edges: [],
      })),
      importData: vi.fn(),
      requestFrame: vi.fn(),
      requestSave: originalRequestSave,
    };
    const workspace = {
      iterateAllLeaves: (cb: (leaf: unknown) => void) =>
        cb({
          view: {
            getViewType: () => "canvas",
            file: { path: "Board.canvas" },
            canvas,
            containerEl: host,
          },
        }),
      getLeavesOfType: () => [],
    };
    const awareness = new Awareness(new Y.Doc());
    let ready = false;
    const doc = {
      path: "Board.canvas",
      awareness,
      isReady: () => ready,
      // The view's content is not this file's disk content.
      matchesStartupDisk: () => false,
      canvasData: () => ({ nodes: [], edges: [] }),
      reconcileFromCanvasData: vi.fn(),
    };

    const binding = new CanvasBinding({ app: { workspace } } as any, doc as any);
    binding.tryBind();

    expect(canvas.importData).not.toHaveBeenCalled();
    expect(originalRequestSave).not.toHaveBeenCalled();

    canvas.requestSave();

    expect(originalRequestSave).toHaveBeenCalledTimes(1);
    expect(doc.reconcileFromCanvasData).not.toHaveBeenCalled();
    expect(binding.startupLiveValue()).toBeNull();

    ready = true;
    binding.applyRemote();

    expect(canvas.importData).toHaveBeenCalledWith({ nodes: [], edges: [] }, true);
    expect(originalRequestSave).toHaveBeenCalledTimes(2);

    binding.destroy();
    awareness.destroy();
  });

  it("reports unsaved pre-startup edits only from a view seen showing this file", () => {
    const disk = { nodes: [{ id: "a", type: "text", text: "disk" }], edges: [] };
    const edited = {
      nodes: [
        { id: "a", type: "text", text: "disk" },
        { id: "b", type: "text", text: "unsaved" },
      ],
      edges: [],
    };
    const fixture = makeLiveCanvasFixture("Board.canvas", disk);
    let diskKnown = false;
    const doc = {
      path: "Board.canvas",
      awareness: new Awareness(new Y.Doc()),
      isReady: () => false,
      matchesStartupDisk: (value: unknown) =>
        diskKnown && JSON.stringify(value) === JSON.stringify(normalizedCanvas(disk)),
      canvasData: () => ({ nodes: [], edges: [] }),
      applyCanvasOperations: vi.fn(),
      reconcileFromCanvasData: vi.fn(),
    };
    const binding = bindFixture(fixture.workspace, doc);
    binding.tryBind();

    // The view was bound before the startup disk read finished, then edited.
    fixture.setData(edited);
    fixture.canvas.requestSave();
    expect(binding.startupLiveValue()).toBeNull();

    // Once the disk is known, the content the view showed when bound matches it.
    diskKnown = true;
    binding.checkStartupViews();
    expect(binding.startupLiveValue()).toEqual(normalizedCanvas(edited));
    expect(doc.applyCanvasOperations).not.toHaveBeenCalled();
    expect(doc.reconcileFromCanvasData).not.toHaveBeenCalled();
    expect(fixture.canvas.importData).not.toHaveBeenCalled();

    // Obsidian reuses the view for another file: its content is not ours.
    fixture.reuseFor("Other.canvas", disk);
    expect(binding.startupLiveValue()).toBeNull();

    binding.destroy();
    doc.awareness.destroy();
  });

  it("never reports a reused view's leftover content as a pre-startup edit", () => {
    const disk = { nodes: [{ id: "a", type: "text", text: "this file" }], edges: [] };
    const leftover = { nodes: [{ id: "x", type: "text", text: "previous file" }], edges: [] };
    const fixture = makeLiveCanvasFixture("Board.canvas", leftover);
    const doc = {
      path: "Board.canvas",
      awareness: new Awareness(new Y.Doc()),
      isReady: () => false,
      matchesStartupDisk: (value: unknown) =>
        JSON.stringify(value) === JSON.stringify(normalizedCanvas(disk)),
      canvasData: () => ({ nodes: [], edges: [] }),
      reconcileFromCanvasData: vi.fn(),
    };
    const binding = bindFixture(fixture.workspace, doc);
    binding.tryBind();
    fixture.canvas.requestSave();
    expect(binding.startupLiveValue()).toBeNull();

    // The view loads this file; a gesture then starts from its disk content.
    fixture.setData(disk);
    fixture.host.dispatchEvent(new PointerEvent("pointerdown", { bubbles: true }));
    const moved = { nodes: [{ id: "a", type: "text", text: "this file", x: 40 }], edges: [] };
    fixture.setData(moved);
    window.dispatchEvent(new PointerEvent("pointerup"));
    expect(binding.startupLiveValue()).toEqual(normalizedCanvas(moved));
    expect(doc.reconcileFromCanvasData).not.toHaveBeenCalled();

    binding.destroy();
    doc.awareness.destroy();
  });
});

/**
 * Obsidian reuses the same view (and Canvas instance) when a leaf switches to
 * another canvas file. A binding patched before the switch must neither fold
 * the other file's data into its CRDT nor import its CRDT into the other
 * file's view. Regression suite for the cross-file overwrite hazard.
 */
describe("CanvasBinding reused view (file switch)", () => {
  function makeCanvas(data: () => unknown) {
    return {
      getData: vi.fn(data),
      importData: vi.fn(),
      requestFrame: vi.fn(),
      requestSave: vi.fn(),
    };
  }

  function makeDoc(path: string, ready = true) {
    return {
      path,
      awareness: new Awareness(new Y.Doc()),
      isReady: () => ready,
      canvasData: () => ({ nodes: [{ id: "a", type: "text", text: "A" }], edges: [] }),
      reconcileFromCanvasData: vi.fn(),
    };
  }

  function makeWorkspace(view: any) {
    return {
      iterateAllLeaves: (cb: (leaf: unknown) => void) => cb({ view }),
      getLeavesOfType: () => [],
    };
  }

  it("does not capture the other file's data after the view switches files", () => {
    const canvas = makeCanvas(() => ({
      nodes: [{ id: "b", type: "text", text: "content of B" }],
      edges: [],
    }));
    const view = {
      getViewType: () => "canvas",
      file: { path: "A.canvas" },
      canvas,
      containerEl: document.createElement("div"),
    };
    const doc = makeDoc("A.canvas");
    const binding = new CanvasBinding(
      { app: { workspace: makeWorkspace(view) } } as any,
      doc as any,
    );
    binding.tryBind();
    doc.reconcileFromCanvasData.mockClear();

    // The leaf switches the reused view to another file.
    view.file = { path: "B.canvas" };
    canvas.getData.mockClear();

    // A local edit in B fires the still-patched requestSave.
    (canvas.requestSave as any)();

    // B's data must NOT be folded into A's CRDT, and the binding must detach.
    expect(doc.reconcileFromCanvasData).not.toHaveBeenCalled();
    expect(binding.isActive()).toBe(false);

    binding.destroy();
    doc.awareness.destroy();
  });

  it("does not import its CRDT into a view that now shows another file", () => {
    const canvas = makeCanvas(() => ({ nodes: [], edges: [] }));
    const view = {
      getViewType: () => "canvas",
      file: { path: "A.canvas" },
      canvas,
      containerEl: document.createElement("div"),
    };
    const doc = makeDoc("A.canvas");
    const binding = new CanvasBinding(
      { app: { workspace: makeWorkspace(view) } } as any,
      doc as any,
    );
    binding.tryBind();
    canvas.importData.mockClear();

    view.file = { path: "B.canvas" };

    // A remote update to A arrives while the view shows B.
    binding.applyRemote();

    // A's content must NOT be imported into (and saved over) B's view.
    expect(canvas.importData).not.toHaveBeenCalled();
    expect(binding.isActive()).toBe(false);

    binding.destroy();
    doc.awareness.destroy();
  });

  it("keeps the new file's binding working when the old binding unpatches later", () => {
    const canvas = makeCanvas(() => ({
      nodes: [{ id: "b", type: "text", text: "content of B" }],
      edges: [],
    }));
    const view = {
      getViewType: () => "canvas",
      file: { path: "A.canvas" },
      canvas,
      containerEl: document.createElement("div"),
    };
    const workspace = makeWorkspace(view);
    const docA = makeDoc("A.canvas");
    const bindingA = new CanvasBinding({ app: { workspace } } as any, docA as any);
    bindingA.tryBind();

    // Switch to B and bind B's doc BEFORE A is unbound (wraps A's patch).
    view.file = { path: "B.canvas" };
    const docB = makeDoc("B.canvas");
    const bindingB = new CanvasBinding({ app: { workspace } } as any, docB as any);
    bindingB.tryBind();

    // A's late unbind must not clobber B's patch.
    bindingA.unbindIfStale();
    expect(bindingA.isActive()).toBe(false);
    expect(bindingB.isActive()).toBe(true);

    docB.reconcileFromCanvasData.mockClear();
    docA.reconcileFromCanvasData.mockClear();
    (canvas.requestSave as any)();

    expect(docB.reconcileFromCanvasData).toHaveBeenCalled();
    expect(docA.reconcileFromCanvasData).not.toHaveBeenCalled();

    bindingA.destroy();
    bindingB.destroy();
    docA.awareness.destroy();
    docB.awareness.destroy();
  });
});

describe("CanvasBinding operation shadow", () => {
  it("captures only the local field changed against the displayed shadow", () => {
    const initial = { nodes: [{ id: "a", type: "text", text: "old", x: 0, y: 0 }], edges: [] };
    const fixture = makeLiveCanvasFixture("Board.canvas", initial);
    const doc = makeOperationalDoc(initial);
    const binding = new CanvasBinding({ app: { workspace: fixture.workspace } } as any, doc as any);
    binding.tryBind();

    // Yjs can already contain this remote text while the view still has the
    // prior snapshot. A local drag must publish x only, never stale text.
    doc.setRemote({ nodes: [{ id: "a", type: "text", text: "remote", x: 0, y: 0 }], edges: [] });
    fixture.setData({ nodes: [{ id: "a", type: "text", text: "old", x: 90, y: 0 }], edges: [] });
    fixture.canvas.requestSave();

    expect(doc.applyCanvasOperations).toHaveBeenCalledWith(
      [{ type: "node-patch", id: "a", patch: { set: { x: 90 }, remove: [] } }],
      expect.anything(),
    );
    expect(doc.reconcileFromCanvasData).not.toHaveBeenCalled();

    binding.destroy();
    doc.awareness.destroy();
  });

  it("updates the shadow on an equal remote state and suppresses delayed saves", () => {
    const initial = { nodes: [{ id: "a", type: "text", text: "same" }], edges: [] };
    const fixture = makeLiveCanvasFixture("Board.canvas", initial);
    const doc = makeOperationalDoc(initial);
    const binding = new CanvasBinding({ app: { workspace: fixture.workspace } } as any, doc as any);
    binding.tryBind();
    doc.applyCanvasOperations.mockClear();

    fixture.canvas.requestSave();
    expect(doc.applyCanvasOperations).not.toHaveBeenCalled();

    binding.destroy();
    doc.awareness.destroy();
  });
});

describe("CanvasBinding interaction scheduling", () => {
  it("samples bounded durable updates and captures the final drag state", () => {
    vi.useFakeTimers();
    const initial = { nodes: [{ id: "a", type: "text", text: "A", x: 0, y: 0 }], edges: [] };
    const fixture = makeLiveCanvasFixture("Board.canvas", initial);
    const awareness = new Awareness(new Y.Doc());
    const doc = {
      path: "Board.canvas",
      awareness,
      isReady: () => true,
      canvasData: () => initial,
      applyCanvasOperations: vi.fn(),
      reconcileFromCanvasData: vi.fn(),
    };
    const binding = new CanvasBinding({ app: { workspace: fixture.workspace } } as any, doc as any);
    binding.tryBind();

    fixture.host.dispatchEvent(new PointerEvent("pointerdown", { bubbles: true }));
    for (let index = 1; index <= 500; index++) {
      fixture.setData({ nodes: [{ id: "a", type: "text", text: "A", x: index, y: 0 }], edges: [] });
      fixture.host.dispatchEvent(new PointerEvent("pointermove", { bubbles: true }));
    }
    vi.advanceTimersByTime(300);
    window.dispatchEvent(new PointerEvent("pointerup"));

    expect(doc.applyCanvasOperations.mock.calls.length).toBeLessThanOrEqual(5);
    expect(doc.applyCanvasOperations).toHaveBeenLastCalledWith(
      [{ type: "node-patch", id: "a", patch: { set: { x: 500 }, remove: [] } }],
      expect.anything(),
    );

    binding.destroy();
    awareness.destroy();
    vi.useRealTimers();
  });

  it("defers remote deletion during a drag and flushes on pointer release", () => {
    vi.useFakeTimers();
    const initial = { nodes: [{ id: "a", type: "text", text: "A", x: 0, y: 0 }], edges: [] };
    const remote = { nodes: [], edges: [] };
    const fixture = makeLiveCanvasFixture("Board.canvas", initial);
    const doc = makeOperationalDoc(initial);
    const binding = new CanvasBinding({ app: { workspace: fixture.workspace } } as any, doc as any);
    binding.tryBind();
    fixture.canvas.importData.mockClear();

    fixture.host.dispatchEvent(new PointerEvent("pointerdown", { bubbles: true }));
    doc.setRemote(remote);
    binding.scheduleRemote();
    vi.advanceTimersByTime(100);
    expect(fixture.canvas.importData).not.toHaveBeenCalled();

    window.dispatchEvent(new PointerEvent("pointerup"));
    expect(fixture.canvas.importData).toHaveBeenCalledTimes(1);
    expect(fixture.canvas.importData).toHaveBeenCalledWith(remote, true);

    binding.destroy();
    doc.awareness.destroy();
    vi.useRealTimers();
  });

  it("removes timers and lifecycle listeners on destroy", () => {
    vi.useFakeTimers();
    const initial = { nodes: [{ id: "a", type: "text", text: "A", x: 0, y: 0 }], edges: [] };
    const fixture = makeLiveCanvasFixture("Board.canvas", initial);
    const doc = makeOperationalDoc(initial);
    const binding = new CanvasBinding({ app: { workspace: fixture.workspace } } as any, doc as any);
    binding.tryBind();
    expect(doc.awareness.getLocalState()?.canvasPresence).toMatchObject({ version: 1 });
    fixture.host.dispatchEvent(new PointerEvent("pointerdown", { bubbles: true }));
    binding.destroy();
    expect(doc.awareness.getLocalState()?.canvasPresence ?? null).toBeNull();
    doc.applyCanvasOperations.mockClear();
    vi.advanceTimersByTime(500);
    window.dispatchEvent(new PointerEvent("pointerup"));
    expect(doc.applyCanvasOperations).not.toHaveBeenCalled();
    doc.awareness.destroy();
    expect(vi.getTimerCount()).toBe(0);
    vi.useRealTimers();
  });

  it("leaves no timers after repeated Canvas mount and unmount cycles", () => {
    vi.useFakeTimers();
    for (let index = 0; index < 25; index++) {
      const initial = {
        nodes: [{ id: "a", type: "text", text: String(index), x: 0, y: 0 }],
        edges: [],
      };
      const fixture = makeLiveCanvasFixture(`Board-${index}.canvas`, initial);
      const doc = makeOperationalDoc(initial);
      doc.path = `Board-${index}.canvas`;
      const binding = new CanvasBinding(
        { app: { workspace: fixture.workspace } } as any,
        doc as any,
      );
      binding.tryBind();
      binding.destroy();
      doc.awareness.destroy();
    }
    expect(vi.getTimerCount()).toBe(0);
    vi.useRealTimers();
  });
});

describe("CanvasBinding gesture capture", () => {
  const cleanups: Array<() => void> = [];

  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    for (const cleanup of cleanups.splice(0)) cleanup();
    vi.useRealTimers();
  });

  function bindCrdt(initial: unknown, { unloadHook = true } = {}) {
    const fixture = makeLiveCanvasFixture("Board.canvas", initial);
    const doc = makeCrdtDoc(initial);
    // TextFileView.onUnloadFile saves what the view shows to the file.
    const unloadSaves: unknown[] = [];
    const onUnloadFile = vi.fn(async () => {
      unloadSaves.push(fixture.data());
    });
    if (unloadHook) fixture.view.onUnloadFile = onUnloadFile;
    const binding = bindFixture(fixture.workspace, doc);
    binding.tryBind();
    let tornDown = false;
    const teardown = () => {
      if (tornDown) return;
      tornDown = true;
      binding.destroy();
      doc.awareness.destroy();
    };
    cleanups.push(teardown);
    // Obsidian updates the view, then requests a save for the edit.
    const edit = (next: unknown) => {
      fixture.setData(next);
      fixture.canvas.requestSave();
    };
    const press = () =>
      fixture.host.dispatchEvent(new PointerEvent("pointerdown", { bubbles: true }));
    const release = () => window.dispatchEvent(new PointerEvent("pointerup"));
    // FileView.loadFile: unload the current file, then show the next one.
    const openInView = async (path: string, data: unknown) => {
      await fixture.view.onUnloadFile?.(fixture.view.file);
      fixture.reuseFor(path, data);
    };
    return {
      fixture,
      doc,
      binding,
      edit,
      press,
      release,
      teardown,
      openInView,
      onUnloadFile,
      unloadSaves,
    };
  }

  it("coalesces per-move save requests during a drag and keeps the final geometry", () => {
    const { doc, edit, press, release } = bindCrdt({ nodes: [node(0)], edges: [] });

    press();
    edit({ nodes: [node(1)], edges: [] });
    // A move's save request alone does not fold the whole board in.
    expect(doc.value().nodes[0]).toMatchObject({ x: 0 });
    for (let x = 2; x <= 300; x++) {
      vi.advanceTimersByTime(1);
      edit({ nodes: [node(x)], edges: [] });
    }
    // The interaction sampler bounds durable updates over the 300ms gesture.
    expect(doc.localStates.length).toBeGreaterThan(0);
    expect(doc.localStates.length).toBeLessThanOrEqual(300 / 75 + 1);
    const sampled = doc.localStates.map((state) => state.nodes[0]?.x as number);
    expect(sampled).toEqual([...sampled].sort((a, b) => a - b));

    edit({ nodes: [node(301)], edges: [] });
    release();
    expect(doc.value()).toEqual({ nodes: [node(301)], edges: [] });
    expect(doc.localStates[doc.localStates.length - 1]).toEqual({
      nodes: [node(301)],
      edges: [],
    });
  });

  const finishers: Array<[string, () => void]> = [
    ["pointer release", () => window.dispatchEvent(new PointerEvent("pointerup"))],
    ["pointer cancel", () => window.dispatchEvent(new PointerEvent("pointercancel"))],
    ["window blur", () => window.dispatchEvent(new Event("blur"))],
    [
      "hidden document",
      () => {
        Object.defineProperty(document, "hidden", { configurable: true, value: true });
        try {
          document.dispatchEvent(new Event("visibilitychange"));
        } finally {
          Reflect.deleteProperty(document, "hidden");
        }
      },
    ],
  ];

  it.each(finishers)("folds the latest geometry when a %s ends the gesture", (_name, finish) => {
    const { doc, edit, press } = bindCrdt({ nodes: [node(0)], edges: [] });

    press();
    edit({ nodes: [node(42)], edges: [] });
    expect(doc.value().nodes[0]).toMatchObject({ x: 0 });
    finish();
    expect(doc.value()).toEqual({ nodes: [node(42)], edges: [] });

    // Once the gesture ended, edits fold in as soon as they are saved.
    edit({ nodes: [node(43)], edges: [] });
    expect(doc.value()).toEqual({ nodes: [node(43)], edges: [] });
  });

  it("folds edits made outside a gesture immediately", () => {
    const { doc, edit } = bindCrdt({ nodes: [node(0)], edges: [] });

    edit({ nodes: [node(0, "typed")], edges: [] });
    expect(doc.value()).toEqual({ nodes: [node(0, "typed")], edges: [] });
    edit({ nodes: [node(0, "typed"), { id: "b", type: "text", text: "B" }], edges: [] });
    expect(doc.value()).toEqual({
      nodes: [node(0, "typed"), { id: "b", type: "text", text: "B" }],
      edges: [],
    });
    expect(doc.localStates).toHaveLength(2);
  });

  it("keeps a drag's latest geometry when the binding is torn down mid-gesture", () => {
    const { doc, edit, press, release, teardown } = bindCrdt({ nodes: [node(0)], edges: [] });

    press();
    edit({ nodes: [node(77)], edges: [] });
    teardown();
    expect(doc.value()).toEqual({ nodes: [node(77)], edges: [] });

    // Nothing reaches the CRDT once unbound.
    edit({ nodes: [node(99)], edges: [] });
    vi.advanceTimersByTime(500);
    release();
    expect(doc.value()).toEqual({ nodes: [node(77)], edges: [] });
    expect(vi.getTimerCount()).toBe(0);
  });

  it("flushes a gesture's pending geometry before Obsidian reuses the view", async () => {
    const { doc, edit, press, openInView, unloadSaves } = bindCrdt({
      nodes: [node(0)],
      edges: [],
    });

    press();
    edit({ nodes: [node(5)], edges: [] });
    expect(doc.value().nodes[0]).toMatchObject({ x: 0 });
    await openInView("Other.canvas", { nodes: [node(0, "other file")], edges: [] });
    expect(doc.value()).toEqual({ nodes: [node(5)], edges: [] });
    // Obsidian's own unload save still ran on this file's content.
    expect(unloadSaves).toEqual([{ nodes: [node(5)], edges: [] }]);

    // What the reused view shows next never reaches this document.
    edit({ nodes: [node(9, "other file")], edges: [] });
    vi.advanceTimersByTime(500);
    window.dispatchEvent(new PointerEvent("pointerup"));
    expect(doc.value()).toEqual({ nodes: [node(5)], edges: [] });
  });

  it("captures every gesture save when the view has no unload hook", () => {
    const { doc, edit, press, release } = bindCrdt(
      { nodes: [node(0)], edges: [] },
      { unloadHook: false },
    );

    press();
    edit({ nodes: [node(5)], edges: [] });
    expect(doc.value()).toEqual({ nodes: [node(5)], edges: [] });
    edit({ nodes: [node(6)], edges: [] });
    expect(doc.value()).toEqual({ nodes: [node(6)], edges: [] });
    release();
    expect(doc.value()).toEqual({ nodes: [node(6)], edges: [] });
  });

  it("hands the view's unload hook back on teardown", async () => {
    const { fixture, doc, edit, press, teardown, openInView, onUnloadFile, unloadSaves } = bindCrdt(
      { nodes: [node(0)], edges: [] },
    );

    teardown();
    expect(fixture.view.onUnloadFile).toBe(onUnloadFile);
    press();
    edit({ nodes: [node(5)], edges: [] });
    await openInView("Other.canvas", { nodes: [node(0, "other file")], edges: [] });
    expect(unloadSaves).toEqual([{ nodes: [node(5)], edges: [] }]);
    expect(doc.value()).toEqual({ nodes: [node(0)], edges: [] });
  });

  it("ignores the bounce-back of a remote import but folds later local edits", () => {
    const { fixture, doc, binding, edit, press, release } = bindCrdt({
      nodes: [node(0)],
      edges: [],
    });

    doc.applyPeer({ nodes: [node(0, "peer")], edges: [] });
    binding.applyRemote();
    expect(fixture.data()).toEqual({ nodes: [node(0, "peer")], edges: [] });

    // Obsidian's debounced save for the import fires after it completed.
    fixture.canvas.requestSave();
    // A gesture that has not moved anything yet samples the same content.
    press();
    vi.advanceTimersByTime(150);
    expect(doc.localStates).toEqual([]);
    expect(doc.value()).toEqual({ nodes: [node(0, "peer")], edges: [] });

    edit({ nodes: [node(25, "peer")], edges: [] });
    vi.advanceTimersByTime(75);
    expect(doc.value()).toEqual({ nodes: [node(25, "peer")], edges: [] });
    edit({ nodes: [node(30, "peer")], edges: [] });
    release();
    expect(doc.value()).toEqual({ nodes: [node(30, "peer")], edges: [] });

    // Outside the gesture, a further local edit folds in immediately.
    edit({ nodes: [node(30, "local")], edges: [] });
    expect(doc.value()).toEqual({ nodes: [node(30, "local")], edges: [] });
    expect(doc.localStates.map((state) => state.nodes[0])).toEqual([
      node(25, "peer"),
      node(30, "peer"),
      node(30, "local"),
    ]);
  });
});

describe("CanvasBinding presence publishing", () => {
  const initial = {
    nodes: [{ id: "a", type: "text", text: "A", x: 1, y: 2, width: 30, height: 40 }],
    edges: [{ id: "e", fromNode: "a", toNode: "a" }],
  };

  function startDrag(liveCanvas: Record<string, unknown>) {
    const fixture = makeLiveCanvasFixture("Board.canvas", initial);
    Object.assign(fixture.canvas, liveCanvas);
    const doc = makeOperationalDoc(initial);
    const binding = new CanvasBinding(
      { app: { workspace: fixture.workspace } } as never,
      doc as never,
    );
    binding.tryBind();
    fixture.canvas.getData.mockClear();
    fixture.host.dispatchEvent(new PointerEvent("pointerdown", { bubbles: true }));
    const presence = doc.awareness.getLocalState()?.canvasPresence;
    const getDataCalls = fixture.canvas.getData.mock.calls.length;
    window.dispatchEvent(new PointerEvent("pointerup"));
    binding.destroy();
    doc.awareness.destroy();
    return { presence, getDataCalls };
  }

  it("publishes drag geometry from live selected nodes without serializing the Canvas", () => {
    const node = { id: "a", x: 50, y: 60, width: 70, height: 80 };
    const edge = { id: "e" };
    const { presence, getDataCalls } = startDrag({
      nodes: new Map([["a", node]]),
      edges: new Map([["e", edge]]),
      selection: new Set([edge, node]),
    });
    expect(presence).toMatchObject({
      selectedNodeIds: ["a"],
      interaction: { kind: "drag", nodes: [{ id: "a", x: 50, y: 60, width: 70, height: 80 }] },
    });
    expect(getDataCalls).toBe(0);
  });

  it("falls back to Canvas data when live nodes cannot provide geometry", () => {
    const expected = {
      selectedNodeIds: ["a"],
      interaction: { kind: "drag", nodes: [{ id: "a", x: 1, y: 2, width: 30, height: 40 }] },
    };
    // Older shape: selection holds ids and there is no live node map.
    const byId = startDrag({ selection: new Set(["a", "e"]) });
    expect(byId.presence).toMatchObject(expected);
    expect(byId.getDataCalls).toBe(1);
    // Live node map whose node lacks numeric geometry.
    const partial = startDrag({
      nodes: new Map([["a", { id: "a" }]]),
      selection: new Set(["a", "e"]),
    });
    expect(partial.presence).toMatchObject(expected);
    expect(partial.getDataCalls).toBe(1);
  });
});
