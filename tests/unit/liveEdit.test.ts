import { describe, it, expect, beforeEach, vi } from "vitest";
import * as Y from "yjs";

const bind = vi.hoisted(() => ({ doc: null as any, requestSave: vi.fn() }));

vi.mock("../../src/editor/context", () => ({
  ensureDocumentForEditor: () => bind.doc,
  requestSaveForEditor: bind.requestSave,
}));

import { LiveEditPluginValue } from "../../src/editor/LiveEdit";

function makeEditor(text: string) {
  return {
    state: {
      doc: { toString: () => text },
    },
    setText(next: string) {
      text = next;
    },
    dispatch({ changes }: { changes: { from: number; to: number; insert: string } }) {
      text = text.slice(0, changes.from) + changes.insert + text.slice(changes.to);
    },
  };
}

describe("LiveEdit", () => {
  beforeEach(() => {
    bind.requestSave.mockClear();
  });

  it("does not push local editor changes until the document is ready, then preserves typed text", async () => {
    const ydoc = new Y.Doc();
    const ytext = ydoc.getText("contents");
    ytext.insert(0, "remote ready value");
    let resolveReady!: () => void;
    const ready = new Promise<void>((resolve) => {
      resolveReady = resolve;
    });
    let isReady = false;
    bind.doc = {
      path: "note.md",
      ytext,
      bindEditor: vi.fn(),
      unbindEditor: vi.fn(),
      whenReady: () => ready,
      isReady: () => isReady,
      isDestroyed: () => false,
      onDestroy: () => () => {},
    };

    const editor = makeEditor("remote ready value") as any;
    const live = new LiveEditPluginValue(editor);
    editor.setText("remote ready value plus local typing");

    live.update({ docChanged: true, state: editor.state } as any);
    expect(ytext.toString()).toBe("remote ready value");

    isReady = true;
    resolveReady();
    await Promise.resolve();

    expect(ytext.toString()).toBe("remote ready value plus local typing");
    live.destroy();
    ydoc.destroy();
  });

  it("detaches from a destroyed document and rebinds before pushing edits", async () => {
    const oldYdoc = new Y.Doc();
    const oldYtext = oldYdoc.getText("contents");
    oldYtext.insert(0, "old shared");
    let oldDestroyed = false;
    const oldDoc = {
      path: "note.md",
      ytext: oldYtext,
      bindEditor: vi.fn(),
      unbindEditor: vi.fn(),
      whenReady: () => Promise.resolve(),
      isReady: () => true,
      isDestroyed: () => oldDestroyed,
      onDestroy: () => () => {},
    };
    bind.doc = oldDoc;

    const editor = makeEditor("old shared") as any;
    const live = new LiveEditPluginValue(editor);
    expect(oldDoc.bindEditor).toHaveBeenCalledTimes(1);

    const newYdoc = new Y.Doc();
    const newYtext = newYdoc.getText("contents");
    const newDoc = {
      path: "note.md",
      ytext: newYtext,
      bindEditor: vi.fn(),
      unbindEditor: vi.fn(),
      whenReady: () => Promise.resolve(),
      isReady: () => true,
      isDestroyed: () => false,
      onDestroy: () => () => {},
    };

    oldDestroyed = true;
    bind.doc = newDoc;
    editor.setText("new editor text");
    live.update({ docChanged: true, state: editor.state } as any);
    await Promise.resolve();

    expect(oldDoc.unbindEditor).toHaveBeenCalledTimes(1);
    expect(newDoc.bindEditor).toHaveBeenCalledTimes(1);
    expect(oldYtext.toString()).toBe("old shared");
    expect(newYtext.toString()).toBe("new editor text");

    live.destroy();
    oldYdoc.destroy();
    newYdoc.destroy();
  });

  it("requests a save after applying a remote Y.Text change", async () => {
    const ydoc = new Y.Doc();
    const ytext = ydoc.getText("contents");
    ytext.insert(0, "initial");
    bind.doc = {
      path: "note.md",
      ytext,
      bindEditor: vi.fn(),
      unbindEditor: vi.fn(),
      whenReady: () => Promise.resolve(),
      isReady: () => true,
      isDestroyed: () => false,
      onDestroy: () => () => {},
    };

    const editor = makeEditor("initial") as any;
    const live = new LiveEditPluginValue(editor);
    await Promise.resolve();
    ydoc.transact(() => {
      ytext.delete(0, ytext.length);
      ytext.insert(0, "remote");
    });

    expect(editor.state.doc.toString()).toBe("remote");
    expect(bind.requestSave).toHaveBeenCalledTimes(1);
    expect(bind.requestSave).toHaveBeenCalledWith(editor);

    live.destroy();
    ydoc.destroy();
  });

  it("requests a save after delayed-bind reconciliation", async () => {
    const ydoc = new Y.Doc();
    const ytext = ydoc.getText("contents");
    ytext.insert(0, "remote ready");
    let resolveReady!: () => void;
    const ready = new Promise<void>((resolve) => {
      resolveReady = resolve;
    });
    bind.doc = {
      path: "note.md",
      ytext,
      bindEditor: vi.fn(),
      unbindEditor: vi.fn(),
      whenReady: () => ready,
      isReady: () => false,
      isDestroyed: () => false,
      onDestroy: () => () => {},
    };

    const editor = makeEditor("stale disk") as any;
    const live = new LiveEditPluginValue(editor);
    resolveReady();
    await Promise.resolve();

    expect(editor.state.doc.toString()).toBe("remote ready");
    expect(bind.requestSave).toHaveBeenCalledTimes(1);

    live.destroy();
    ydoc.destroy();
  });

  it("does not request a save for idempotent reconciliation or local edits", async () => {
    const ydoc = new Y.Doc();
    const ytext = ydoc.getText("contents");
    ytext.insert(0, "same");
    bind.doc = {
      path: "note.md",
      ytext,
      bindEditor: vi.fn(),
      unbindEditor: vi.fn(),
      whenReady: () => Promise.resolve(),
      isReady: () => true,
      isDestroyed: () => false,
      onDestroy: () => () => {},
      isCreator: true,
      hasSyncedOnce: true,
      isProviderOnline: true,
    };

    const editor = makeEditor("same") as any;
    const live = new LiveEditPluginValue(editor);
    await Promise.resolve();
    editor.setText("local");
    live.update({ docChanged: true, state: editor.state } as any);

    expect(ytext.toString()).toBe("local");
    expect(bind.requestSave).not.toHaveBeenCalled();

    live.destroy();
    ydoc.destroy();
  });

  it("rebinds as soon as its document is destroyed, without waiting for a keystroke", async () => {
    const listeners: Array<() => void> = [];
    const oldYdoc = new Y.Doc();
    const oldDoc = {
      path: "note.md",
      ytext: oldYdoc.getText("contents"),
      bindEditor: vi.fn(),
      unbindEditor: vi.fn(),
      whenReady: () => Promise.resolve(),
      isReady: () => true,
      isDestroyed: () => false,
      onDestroy: (listener: () => void) => {
        listeners.push(listener);
        return () => {};
      },
    };
    oldDoc.ytext.insert(0, "shared");
    bind.doc = oldDoc;
    const editor = makeEditor("shared") as any;
    const live = new LiveEditPluginValue(editor);
    await Promise.resolve();

    const newYdoc = new Y.Doc();
    const newYtext = newYdoc.getText("contents");
    newYtext.insert(0, "shared");
    bind.doc = {
      path: "note.md",
      ytext: newYtext,
      bindEditor: vi.fn(),
      unbindEditor: vi.fn(),
      whenReady: () => Promise.resolve(),
      isReady: () => true,
      isDestroyed: () => false,
      onDestroy: () => () => {},
    };
    for (const listener of listeners) listener();
    await Promise.resolve();
    await Promise.resolve();

    expect(oldDoc.unbindEditor).toHaveBeenCalledTimes(1);
    newYdoc.transact(() => newYtext.insert(newYtext.length, " + remote"));
    expect(editor.state.doc.toString()).toBe("shared + remote");
    live.destroy();
    oldYdoc.destroy();
    newYdoc.destroy();
  });

  it("pushes an edit typed while no document was bound", async () => {
    bind.doc = null;
    const editor = makeEditor("shared text") as any;
    const live = new LiveEditPluginValue(editor);
    editor.setText("shared text + typed");
    live.update({
      docChanged: true,
      state: editor.state,
      startState: { doc: { toString: () => "shared text" } },
    } as any);

    const ydoc = new Y.Doc();
    const ytext = ydoc.getText("contents");
    ytext.insert(0, "shared text");
    bind.doc = {
      path: "note.md",
      ytext,
      bindEditor: vi.fn(),
      unbindEditor: vi.fn(),
      whenReady: () => Promise.resolve(),
      isReady: () => true,
      isDestroyed: () => false,
      onDestroy: () => () => {},
    };
    live.update({ docChanged: false, state: editor.state } as any);
    await Promise.resolve();

    expect(ytext.toString()).toBe("shared text + typed");
    expect(editor.state.doc.toString()).toBe("shared text + typed");
    live.destroy();
    ydoc.destroy();
  });
});
