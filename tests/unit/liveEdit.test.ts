import { describe, it, expect, beforeEach, vi } from "vitest";
import * as Y from "yjs";
import { EditorState, type TransactionSpec } from "@codemirror/state";
import { EditorView, type ViewUpdate } from "@codemirror/view";
import type * as EditorContext from "../../src/editor/context";

const bind = vi.hoisted(() => ({ doc: null as any, requestSave: vi.fn() }));

vi.mock("../../src/editor/context", async (importOriginal) => ({
  ...(await importOriginal<typeof EditorContext>()),
  ensureDocumentForEditor: () => bind.doc,
  requestSaveForEditor: bind.requestSave,
}));

import { LiveEditPluginValue, liveEdit } from "../../src/editor/LiveEdit";

/** `owner` is the editorInfoField value: Obsidian's MarkdownView, embed, … */
function makeEditor(text: string, owner: object | null = null) {
  return {
    dom: document.createElement("div"),
    state: {
      doc: { toString: () => text },
      field: () => owner,
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

  it("merges text typed before the first sync with remote changes the sync delivered", async () => {
    const ydoc = new Y.Doc();
    const ytext = ydoc.getText("contents");
    ytext.insert(0, "line one\nline two\n");
    let resolveReady!: () => void;
    const ready = new Promise<void>((resolve) => {
      resolveReady = resolve;
    });
    let isReady = false;
    bind.doc = {
      path: "note.md",
      ytext,
      isCreator: false,
      hasSyncedOnce: false,
      isProviderOnline: true,
      bindEditor: vi.fn(),
      unbindEditor: vi.fn(),
      whenReady: () => ready,
      isReady: () => isReady,
      isDestroyed: () => false,
      onDestroy: () => () => {},
      preserveSharedConflict: vi.fn(),
    };

    const editor = makeEditor("line one\nline two\n") as any;
    const live = new LiveEditPluginValue(editor);
    editor.setText("line one\nline two\nmy local line\n");
    live.update({ docChanged: true, state: editor.state } as any);

    // The first sync delivers a collaborator's paragraph meanwhile.
    ytext.insert(0, "REMOTE PARAGRAPH\n");
    bind.doc.hasSyncedOnce = true;
    isReady = true;
    resolveReady();
    await Promise.resolve();

    const merged = "REMOTE PARAGRAPH\nline one\nline two\nmy local line\n";
    expect(ytext.toString()).toBe(merged);
    expect(editor.state.doc.toString()).toBe(merged);
    expect(bind.doc.preserveSharedConflict).not.toHaveBeenCalled();
    live.destroy();
    ydoc.destroy();
  });

  it("keeps overlapping pre-sync typing and preserves the shared version", async () => {
    const ydoc = new Y.Doc();
    const ytext = ydoc.getText("contents");
    ytext.insert(0, "title\n");
    let resolveReady!: () => void;
    const ready = new Promise<void>((resolve) => {
      resolveReady = resolve;
    });
    let isReady = false;
    bind.doc = {
      path: "note.md",
      ytext,
      isCreator: false,
      hasSyncedOnce: true,
      isProviderOnline: true,
      bindEditor: vi.fn(),
      unbindEditor: vi.fn(),
      whenReady: () => ready,
      isReady: () => isReady,
      isDestroyed: () => false,
      onDestroy: () => () => {},
      preserveSharedConflict: vi.fn(async () => {}),
    };

    const editor = makeEditor("title\n") as any;
    const live = new LiveEditPluginValue(editor);
    editor.setText("title edited here\n");
    live.update({ docChanged: true, state: editor.state } as any);
    ydoc.transact(() => {
      ytext.delete(0, ytext.length);
      ytext.insert(0, "title edited remotely\n");
    });
    isReady = true;
    resolveReady();
    await Promise.resolve();

    expect(ytext.toString()).toBe("title edited here\n");
    expect(editor.state.doc.toString()).toBe("title edited here\n");
    expect(bind.doc.preserveSharedConflict).toHaveBeenCalledWith("title edited remotely\n");
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

  describe("editors that hold only part of the note", () => {
    const note = "# Note\n\n## Table\n| a | b |\n| - | - |\n| 1 | 2 |\n";

    function bindSyncedNote() {
      const ydoc = new Y.Doc();
      const ytext = ydoc.getText("contents");
      ytext.insert(0, note);
      bind.doc = {
        path: "note.md",
        ytext,
        isCreator: false,
        hasSyncedOnce: true,
        isProviderOnline: true,
        bindEditor: vi.fn(),
        unbindEditor: vi.fn(),
        whenReady: () => Promise.resolve(),
        isReady: () => true,
        isDestroyed: () => false,
        onDestroy: () => () => {},
      };
      return { ydoc, ytext };
    }

    it.each([
      {
        name: "a Live Preview table cell",
        // Mounted inside the cell's `.table-cell-wrapper`; owned by the host view.
        makeFragmentEditor: () => {
          const editor = makeEditor("1", { file: { path: "note.md" } });
          const wrapper = document.createElement("div");
          wrapper.className = "table-cell-wrapper";
          wrapper.appendChild(editor.dom);
          return { editor, typed: "12" };
        },
      },
      {
        name: "a section embed such as a Canvas file node for note.md#Table",
        makeFragmentEditor: () => ({
          editor: makeEditor("| a | b |\n| - | - |\n| 1 | 2 |\n", { subpath: "#Table" }),
          typed: "| a | b |\n| - | - |\n| 1 | 2 |\n| 3 | 4 |\n",
        }),
      },
    ])("never syncs $name with the note's shared text", async ({ makeFragmentEditor }) => {
      const { ydoc, ytext } = bindSyncedNote();
      const { editor, typed } = makeFragmentEditor();
      // Stand-in: LiveEdit only reads `dom`, `state` and calls `dispatch`.
      const live = new LiveEditPluginValue(editor as unknown as EditorView);
      await Promise.resolve();

      editor.setText(typed);
      live.update({ docChanged: true, state: editor.state } as unknown as ViewUpdate);
      ydoc.transact(() => ytext.insert(0, "remote "));

      expect(bind.doc.bindEditor).not.toHaveBeenCalled();
      expect(ytext.toString()).toBe(`remote ${note}`);
      expect(editor.state.doc.toString()).toBe(typed);

      live.destroy();
      ydoc.destroy();
    });

    it("still syncs a whole-file embed, which holds the entire note", async () => {
      const { ydoc, ytext } = bindSyncedNote();
      const editor = makeEditor(note, { subpath: "" });
      const live = new LiveEditPluginValue(editor as unknown as EditorView);
      await Promise.resolve();

      editor.setText(`${note}more\n`);
      live.update({ docChanged: true, state: editor.state } as unknown as ViewUpdate);
      ydoc.transact(() => ytext.insert(0, "remote "));

      expect(ytext.toString()).toBe(`remote ${note}more\n`);
      expect(editor.state.doc.toString()).toBe(`remote ${note}more\n`);

      live.destroy();
      ydoc.destroy();
    });
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

  it("applies a note emptied elsewhere instead of pushing the editor's stale text", async () => {
    const ydoc = new Y.Doc();
    const ytext = ydoc.getText("contents");
    ytext.insert(0, "hello\n");
    const ready = Promise.withResolvers<void>();
    const doc = {
      path: "note.md",
      ytext,
      isCreator: false,
      hasSyncedOnce: false,
      isProviderOnline: true,
      readyNow: false,
      bindEditor: vi.fn(),
      unbindEditor: vi.fn(),
      whenReady: () => ready.promise,
      isReady: () => doc.readyNow,
      isDestroyed: () => false,
      onDestroy: () => () => {},
    };
    bind.doc = doc;
    const editor = makeEditor("hello\n");
    // LiveEdit reads `dom`, `state.doc` and calls `dispatch`, which the stand-in provides.
    const live = new LiveEditPluginValue(editor as unknown as EditorView);

    // The first sync delivers another device's "select all, delete".
    ytext.delete(0, ytext.length);
    doc.hasSyncedOnce = true;
    doc.readyNow = true;
    ready.resolve();
    await ready.promise;
    await Promise.resolve();

    expect(ytext.toString()).toBe("");
    expect(editor.state.doc.toString()).toBe("");
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

  describe("incremental edits from a synchronized baseline", () => {
    function sharedText(initial: string) {
      const ydoc = new Y.Doc();
      const ytext = ydoc.getText("contents");
      ytext.insert(0, initial);
      bind.doc = {
        path: "note.md",
        ytext,
        isCreator: false,
        hasSyncedOnce: true,
        isProviderOnline: true,
        bindEditor: () => {},
        unbindEditor: () => {},
        whenReady: () => Promise.resolve(),
        isReady: () => true,
        isDestroyed: () => false,
        onDestroy: () => () => {},
      };
      return { ydoc, ytext };
    }

    it("keeps multiple editors synchronized through multi-range edits and remote changes", async () => {
      const { ydoc, ytext } = sharedText("first\nmiddle 😀\nlast");
      const a = new EditorView({ doc: ytext.toString(), extensions: [liveEdit] });
      const b = new EditorView({ doc: ytext.toString(), extensions: [liveEdit] });
      try {
        await Promise.resolve();
        a.dispatch({
          changes: [
            { from: 0, to: 5, insert: "FIRST!" },
            { from: 16, to: 20, insert: "end" },
          ],
        });
        b.dispatch({ changes: { from: 7, insert: "remote " } });
        a.dispatch({ changes: { from: a.state.doc.length, insert: "\nmore" } });
        const expected = "FIRST!\nremote middle 😀\nend\nmore";
        expect(ytext.toString()).toBe(expected);
        expect(a.state.doc.toString()).toBe(expected);
        expect(b.state.doc.toString()).toBe(expected);
      } finally {
        a.destroy();
        b.destroy();
        ydoc.destroy();
      }
    });

    it("does not duplicate a remote edit bundled with local typing", async () => {
      const { ydoc, ytext } = sharedText("abc");
      const editor = {
        dom: document.createElement("div"),
        state: EditorState.create({ doc: "abc" }),
        dispatch(spec: TransactionSpec) {
          this.state = this.state.update(spec).state;
        },
      };
      const live = new LiveEditPluginValue(editor as unknown as EditorView);
      try {
        await Promise.resolve();
        const startState = editor.state;
        ytext.insert(0, "remote ");
        const local = editor.state.update({
          changes: { from: editor.state.doc.length, insert: "!" },
        });
        editor.state = local.state;
        live.update({
          docChanged: true,
          startState,
          state: local.state,
          changes: startState.changes({ from: 0, insert: "remote " }).compose(local.changes),
        } as ViewUpdate);
        expect(ytext.toString()).toBe("remote abc!");
        expect(editor.state.doc.toString()).toBe("remote abc!");
      } finally {
        live.destroy();
        ydoc.destroy();
      }
    });

    it("reconciles edits made inside a transaction whose shared changes are not observed yet", async () => {
      const { ydoc, ytext } = sharedText("abc");
      const editor = new EditorView({ doc: "abc", extensions: [liveEdit] });
      try {
        await Promise.resolve();
        ydoc.transact(() => {
          ytext.delete(0, 3);
          ytext.insert(0, "xyz");
          editor.dispatch({ changes: { from: 3, insert: "!" } });
        });
        expect(ytext.toString()).toBe("abc!");
        expect(editor.state.doc.toString()).toBe("abc!");
        editor.dispatch({ changes: { from: 0, insert: "next " } });
        expect(ytext.toString()).toBe("next abc!");
      } finally {
        editor.destroy();
        ydoc.destroy();
      }
    });

    it("preserves emoji when a replacement changes only one surrogate code unit", async () => {
      const { ydoc, ytext } = sharedText("x😀y");
      const editor = new EditorView({ doc: "x😀y", extensions: [liveEdit] });
      const peer = new Y.Doc();
      try {
        await Promise.resolve();
        editor.dispatch({ changes: { from: 2, to: 3, insert: "\ude03" } });
        editor.dispatch({ changes: { from: 4, insert: "!" } });
        Y.applyUpdate(peer, Y.encodeStateAsUpdate(ydoc));
        expect(editor.state.doc.toString()).toBe("x😃y!");
        expect(ytext.toString()).toBe("x😃y!");
        expect(peer.getText("contents").toString()).toBe("x😃y!");
      } finally {
        editor.destroy();
        peer.destroy();
        ydoc.destroy();
      }
    });
  });
});
