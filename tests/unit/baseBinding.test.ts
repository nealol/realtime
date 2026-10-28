import { describe, expect, it, vi } from "vitest";
import { BaseBinding } from "../../src/editor/BaseBinding";
import type { BaseDocument } from "../../src/BaseDocument";
import type RealtimePlugin from "../../src/main";
import { parseBase } from "../../src/structured/base";

describe("BaseBinding startup", () => {
  it("pushes nothing into an open view before startup and reports only its own edits", () => {
    const disk = "formulas:\n  a: '1'\n";
    let text = disk;
    /** Obsidian's own (debounced) save, wrapped by the binding's patch. */
    const save = vi.fn();
    const view = {
      getViewType: () => "bases",
      file: { path: "views.base" },
      getViewData: () => text,
      setViewData: vi.fn((data: string) => {
        text = data;
      }),
      requestSave: save,
    };
    let ready = false;
    let crdt = "{}\n";
    const doc = {
      path: "views.base",
      isReady: () => ready,
      matchesStartupDisk: (value: unknown) =>
        JSON.stringify(value) === JSON.stringify(parseBase(disk)),
      baseData: () => crdt,
      reconcileFromBaseText: vi.fn(),
    };
    const workspace = {
      iterateAllLeaves: (cb: (leaf: unknown) => void) => cb({ view }),
      getLeavesOfType: () => [],
    };
    // Test boundary: the binding reads only `app.workspace` from the plugin
    // and the readiness/startup methods above from the document.
    const plugin = { app: { workspace } } as unknown as RealtimePlugin;
    const binding = new BaseBinding(plugin, doc as unknown as BaseDocument);
    binding.tryBind();

    // The constructor-time empty root never reaches the view or its save.
    expect(view.setViewData).not.toHaveBeenCalled();
    expect(save).not.toHaveBeenCalled();

    // An unsaved edit before startup is reported, not folded into the CRDT.
    text = "formulas:\n  a: '1'\n  b: '2'\n";
    view.requestSave();
    expect(doc.reconcileFromBaseText).not.toHaveBeenCalled();
    expect(binding.startupLiveValue()).toEqual({ formulas: { a: "1", b: "2" } });

    // After startup the reconciled value is pushed once; equal content is not re-pushed.
    ready = true;
    crdt = 'formulas:\n  a: "1"\n  b: "2"\n  c: "3"\n';
    binding.applyRemote();
    binding.applyRemote();
    expect(view.setViewData).toHaveBeenCalledTimes(1);
    // One save for the user's edit, one converging disk to the pushed value.
    expect(save).toHaveBeenCalledTimes(2);
    expect(parseBase(text)).toEqual({ formulas: { a: "1", b: "2", c: "3" } });
    binding.destroy();
  });

  it("does not report a reused view's leftover YAML as a startup edit", () => {
    const view = {
      getViewType: () => "bases",
      file: { path: "views.base" },
      getViewData: () => "formulas:\n  other: '1'\n",
      setViewData: vi.fn(),
      requestSave: vi.fn(),
    };
    const doc = {
      path: "views.base",
      isReady: () => false,
      matchesStartupDisk: (value: unknown) =>
        JSON.stringify(value) === JSON.stringify({ formulas: { a: "1" } }),
      baseData: () => "{}\n",
      reconcileFromBaseText: vi.fn(),
    };
    const workspace = {
      iterateAllLeaves: (cb: (leaf: unknown) => void) => cb({ view }),
      getLeavesOfType: () => [],
    };
    // Test boundary: see above.
    const plugin = { app: { workspace } } as unknown as RealtimePlugin;
    const binding = new BaseBinding(plugin, doc as unknown as BaseDocument);
    binding.tryBind();
    view.requestSave();
    expect(binding.startupLiveValue()).toBeNull();
    expect(doc.reconcileFromBaseText).not.toHaveBeenCalled();
    expect(view.setViewData).not.toHaveBeenCalled();
    binding.destroy();
  });
});

/**
 * Obsidian reuses the same TextFileView when a leaf switches to another
 * `.base` file. A binding patched before the switch must neither fold the
 * other file's YAML into its CRDT nor push its CRDT into the other file's
 * view. Regression suite for the cross-file overwrite hazard (mirrors the
 * CanvasBinding reused-view suite).
 */
describe("BaseBinding reused view (file switch)", () => {
  function makeView(path: string, data: () => string) {
    return {
      getViewType: () => "bases",
      file: { path } as { path: string },
      getViewData: vi.fn(data),
      setViewData: vi.fn(),
      requestSave: vi.fn(),
    };
  }

  function makeDoc(path: string) {
    return {
      path,
      isReady: () => true,
      baseData: () => "views:\n  - type: table\n    name: A\n",
      reconcileFromBaseText: vi.fn(),
    };
  }

  function makeWorkspace(view: any) {
    return {
      iterateAllLeaves: (cb: (leaf: unknown) => void) => cb({ view }),
      getLeavesOfType: () => [],
    };
  }

  it("does not capture the other file's YAML after the view switches files", () => {
    const view = makeView("A.base", () => "views:\n  - type: table\n    name: B\n");
    const doc = makeDoc("A.base");
    const binding = new BaseBinding({ app: { workspace: makeWorkspace(view) } } as any, doc as any);
    binding.tryBind();
    doc.reconcileFromBaseText.mockClear();

    // The leaf switches the reused view to another file.
    view.file = { path: "B.base" };

    // A local edit in B fires the still-patched requestSave.
    (view.requestSave as any)();

    expect(doc.reconcileFromBaseText).not.toHaveBeenCalled();
    expect(binding.isActive()).toBe(false);
  });

  it("does not push its CRDT into a view that now shows another file", () => {
    const view = makeView("A.base", () => "");
    const doc = makeDoc("A.base");
    const binding = new BaseBinding({ app: { workspace: makeWorkspace(view) } } as any, doc as any);
    binding.tryBind();
    view.setViewData.mockClear();

    view.file = { path: "B.base" };

    // A remote update to A arrives while the view shows B.
    binding.applyRemote();

    expect(view.setViewData).not.toHaveBeenCalled();
    expect(binding.isActive()).toBe(false);
  });

  it("keeps the new file's binding working when the old binding unpatches later", () => {
    const view = makeView("A.base", () => "views:\n  - type: table\n    name: B\n");
    const workspace = makeWorkspace(view);
    const docA = makeDoc("A.base");
    const bindingA = new BaseBinding({ app: { workspace } } as any, docA as any);
    bindingA.tryBind();

    // Switch to B and bind B's doc BEFORE A is unbound (wraps A's patch).
    view.file = { path: "B.base" };
    const docB = makeDoc("B.base");
    docB.baseData = () => "views:\n  - type: table\n    name: B\n";
    const bindingB = new BaseBinding({ app: { workspace } } as any, docB as any);
    bindingB.tryBind();

    // A's late unbind must not clobber B's patch.
    bindingA.unbindIfStale();
    expect(bindingA.isActive()).toBe(false);
    expect(bindingB.isActive()).toBe(true);

    docA.reconcileFromBaseText.mockClear();
    docB.reconcileFromBaseText.mockClear();
    (view.requestSave as any)();

    expect(docB.reconcileFromBaseText).toHaveBeenCalled();
    expect(docA.reconcileFromBaseText).not.toHaveBeenCalled();
  });
});
