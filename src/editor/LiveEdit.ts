// Binds an Obsidian (CodeMirror 6) editor to a Yjs Y.Text, in both directions.
// Adapted from y-codemirror.next (MIT, © Kevin Jahns) and the Relay plugin.

import type { ChangeSpec, Text } from "@codemirror/state";
import { EditorView, ViewPlugin, type PluginValue, type ViewUpdate } from "@codemirror/view";
import type * as Y from "yjs";
import type { Document } from "../Document";
import { ensureDocumentForEditor, isNoteFragmentEditor, requestSaveForEditor } from "./context";
import { ySyncAnnotation } from "./annotations";
import { applyTextToYText } from "../diff";
import { mergeText } from "../textMerge";
import { dbg, dbgEnabled, snip } from "../debug";

/**
 * Decide whether a local editor change may be materialized into the shared
 * Y.Text. The dangerous case is turning a *pre-existing* editor buffer into
 * brand-new Yjs operations on an *empty* shared doc before our first server
 * sync: on clients whose local persistence was evicted (common on mobile) the
 * shared doc loads empty even though the server already holds this content, so
 * seeding it here duplicates the whole note once the server's operations
 * arrive. We therefore hold pushes back only while a sync that could deliver
 * that content is actually expected (provider online, not yet synced, and not
 * the doc's creator). Offline edits and post-sync edits always flow through:
 * no duplicate can arrive offline, and after the first sync `applyTextToYText`
 * produces a minimal, non-duplicating diff against the server's content.
 */
export function shouldPushEditorToShared(state: {
  sharedEmpty: boolean;
  isCreator: boolean;
  hasSyncedOnce: boolean;
  providerOnline: boolean;
}): boolean {
  const { sharedEmpty, isCreator, hasSyncedOnce, providerOnline } = state;
  if (sharedEmpty && !isCreator && !hasSyncedOnce && providerOnline) return false;
  return true;
}

export class LiveEditPluginValue implements PluginValue {
  private editor: EditorView;
  private doc: Document | null = null;
  private ytext: Y.Text | null = null;
  private observer: ((event: Y.YTextEvent, transaction: Y.Transaction) => void) | null = null;
  private unsubscribeDestroy: (() => void) | null = null;
  private destroyed = false;
  /** Edits only part of its note: never binds (see {@link isNoteFragmentEditor}). */
  private readonly inert: boolean;
  private editorTextAtBind: string | null = null;
  /**
   * Editor text before the first change made while no document was bound
   * (e.g. while one was being rebuilt), so binding still pushes that change.
   */
  private unpushedBaseline: string | null = null;
  /** Immutable editor snapshot whose contents are known to match Y.Text. */
  private synchronizedDoc: Text | null = null;
  private pushingEditorChanges = false;
  private sharedChangedWhilePushing = false;
  private sharedTransactionDepth = 0;
  private readonly beforeSharedTransaction = (transaction: Y.Transaction): void => {
    // An editor update can occur inside another transaction, before Y.Text
    // observers run. Empty acknowledgement transactions must not discard a
    // proven baseline, but forwarding offsets inside any foreign one is unsafe.
    if (transaction.origin !== this) this.sharedTransactionDepth++;
  };
  private readonly afterSharedTransaction = (transaction: Y.Transaction): void => {
    if (transaction.origin !== this) this.sharedTransactionDepth--;
  };

  constructor(editor: EditorView) {
    this.editor = editor;
    this.inert = isNoteFragmentEditor(editor);
    this.tryBind();
  }

  /** Attempt to find this file's Document and attach. Retries until sync is ready. */
  private tryBind(): void {
    if (this.destroyed || this.inert || this.doc) return;
    const doc = ensureDocumentForEditor(this.editor);
    if (!doc || doc.isDestroyed()) {
      window.setTimeout(() => this.tryBind(), 500);
      return;
    }
    this.doc = doc;
    this.ytext = doc.ytext;
    this.editorTextAtBind = this.unpushedBaseline ?? this.editor.state.doc.toString();
    this.unpushedBaseline = null;
    doc.bindEditor();
    // A document can be replaced while the editor stays open (e.g. rebuilt
    // for a new epoch): rebind right away instead of waiting for a keystroke,
    // so remote changes keep reaching the editor.
    this.unsubscribeDestroy = doc.onDestroy(() => {
      if (this.doc !== doc) return;
      this.detachDoc();
      queueMicrotask(() => this.tryBind());
    });

    const observer = (_event: Y.YTextEvent, transaction: Y.Transaction) =>
      this.onYTextChanged(transaction);
    this.observer = observer;
    this.ytext.observe(observer);
    this.ytext.doc?.on("beforeTransaction", this.beforeSharedTransaction);
    this.ytext.doc?.on("afterTransaction", this.afterSharedTransaction);

    // Reconcile only after initial sync, so we never act on an empty,
    // not-yet-synced shared document.
    void doc.whenReady().then(() => {
      if (this.destroyed || doc.isDestroyed()) return;
      this.reconcileOnAttach();
    });
  }

  /**
   * Align the editor and the shared text when an editor attaches:
   *  - text typed before the document was ready is merged into the shared text;
   *  - otherwise, once the document has synced with the server, the shared
   *    text is authoritative (even when empty) -> update editor;
   *  - else if the shared text has content, update the editor;
   *  - else if the editor has content, seed the shared text from it;
   *  - if both are empty, do nothing.
   * An empty shared document overwrites the editor only after a server sync,
   * because before one it may just be a store that has not loaded yet.
   */
  private reconcileOnAttach(): void {
    if (!this.ytext || this.destroyed) return;
    const ytext = this.ytext;
    const shared = ytext.toString();
    const current = this.editor.state.doc.toString();
    if (shared === current) {
      this.synchronizedDoc = this.editor.state.doc;
      return;
    }

    // The user typed while the document was still loading its local
    // persistence / first server sync (possibly a whole offline session).
    // Those keystrokes are edits to the text the editor showed when it bound;
    // the shared text may have gained other peers' changes meanwhile, so merge
    // the two instead of letting either side overwrite the other.
    const base = this.editorTextAtBind;
    if (base !== null && current !== base) {
      // ...unless a server sync that could deliver this content is still pending
      // (see {@link shouldPushEditorToShared}); seeding it now would duplicate.
      if (!this.mayPushToShared()) {
        if (dbgEnabled())
          dbg("reconcile push DEFERRED (pre-sync empty shared)", this.doc?.path, snip(current));
        return;
      }
      const merge = mergeText(base, current, shared);
      if (merge.kind === "merged") {
        ytext.doc?.transact(() => {
          applyTextToYText(ytext, merge.content);
        }, this);
        this.applyTextToEditor(merge.content);
        return;
      }
      // Both sides changed the same lines. Keep what the user is typing and
      // preserve the shared version beside the note rather than dropping it.
      const doc = this.doc;
      ytext.doc?.transact(() => {
        applyTextToYText(ytext, current);
      }, this);
      void doc?.preserveSharedConflict(shared);
      return;
    }

    if (shared.length > 0 || this.doc?.hasSyncedOnce) {
      // After a server sync an empty shared text is a deletion made
      // elsewhere; pushing the editor's stale text would undo it.
      this.applyTextToEditor(shared);
    } else if (current.length > 0 && this.mayPushToShared()) {
      // Seed the empty shared doc from the editor, unless a server sync that
      // could already hold this content is still pending (see
      // {@link shouldPushEditorToShared}) — seeding then would duplicate.
      const ytext = this.ytext;
      ytext.doc?.transact(() => {
        applyTextToYText(ytext, current);
      }, this);
    }
  }

  /**
   * Whether the current editor buffer may be pushed into the shared Y.Text.
   * Guards against the mobile-reopen duplication (see {@link shouldPushEditorToShared}).
   */
  private mayPushToShared(): boolean {
    if (!this.doc || !this.ytext) return false;
    return shouldPushEditorToShared({
      sharedEmpty: this.ytext.length === 0,
      isCreator: this.doc.isCreator,
      hasSyncedOnce: this.doc.hasSyncedOnce,
      providerOnline: this.doc.isProviderOnline,
    });
  }

  private detachDoc(): void {
    this.unsubscribeDestroy?.();
    this.unsubscribeDestroy = null;
    if (this.observer && this.ytext) this.ytext.unobserve(this.observer);
    this.ytext?.doc?.off("beforeTransaction", this.beforeSharedTransaction);
    this.ytext?.doc?.off("afterTransaction", this.afterSharedTransaction);
    if (this.doc) this.doc.unbindEditor();
    this.observer = null;
    this.ytext = null;
    this.doc = null;
    this.editorTextAtBind = null;
    this.synchronizedDoc = null;
    this.sharedTransactionDepth = 0;
  }

  /** Apply a remote Yjs change to the editor. */
  private onYTextChanged(transaction: Y.Transaction): void {
    if (this.pushingEditorChanges && transaction.origin === this) return;
    this.synchronizedDoc = null;
    if (this.pushingEditorChanges) this.sharedChangedWhilePushing = true;
    if (!this.ytext || this.destroyed) return;
    // Initial sync still streams into Y.Text before isReady(); applying it
    // would clobber text typed during that window. reconcileOnAttach runs
    // after ready and preserves those local edits.
    if (this.doc && !this.doc.isReady()) return;
    // Re-derive the editor content from the shared text using a minimal diff.
    // (Using delta would be more precise, but observe() here is invoked without
    // the event; we recompute against the current editor text instead.)
    this.applyTextToEditor(this.ytext.toString());
  }

  private applyTextToEditor(target: string): void {
    const current = this.editor.state.doc.toString();
    if (current === target) {
      this.synchronizedDoc = this.editor.state.doc;
      return;
    }
    if (dbgEnabled())
      dbg("applyTextToEditor", this.doc?.path, "editor", snip(current), "->ytext", snip(target));

    // Minimal prefix/suffix diff so we don't disturb the local selection.
    let start = 0;
    const max = Math.min(current.length, target.length);
    while (start < max && current[start] === target[start]) start++;
    let endCur = current.length;
    let endTar = target.length;
    while (endCur > start && endTar > start && current[endCur - 1] === target[endTar - 1]) {
      endCur--;
      endTar--;
    }
    const changes: ChangeSpec = {
      from: start,
      to: endCur,
      insert: target.slice(start, endTar),
    };
    this.editor.dispatch({
      changes,
      annotations: [ySyncAnnotation.of(this.editor)],
    });
    requestSaveForEditor(this.editor);
    // A transaction filter may have transformed the remote replacement.
    if (this.editor.state.doc.toString() === target) this.synchronizedDoc = this.editor.state.doc;
  }

  /** Push local editor edits into the shared Y.Text. */
  update(update: ViewUpdate): void {
    if (this.destroyed || this.inert) return;
    if (!this.doc || this.doc.isDestroyed()) {
      if (update.docChanged) this.unpushedBaseline ??= update.startState?.doc.toString() ?? null;
      if (this.doc) this.detachDoc();
      this.tryBind();
      return;
    }
    if (!this.ytext || !update.docChanged) return;
    if (this.doc && !this.doc.isReady()) return;

    // Only forward offsets from a proven shared baseline. Remote changes,
    // combined remote/local ViewUpdates and rebinds invalidate that baseline
    // and retain the self-healing full-text reconcile.
    const incremental = this.canApplyEditorChanges(update);
    let target: string | undefined;
    if (!incremental) {
      target = update.state.doc.toString();
      if (this.ytext.toString() === target) {
        this.synchronizedDoc = update.state.doc;
        return;
      }
    }
    if (!this.mayPushToShared()) {
      if (dbgEnabled())
        dbg(
          "local push DEFERRED (pre-sync empty shared)",
          this.doc.path,
          "->editor",
          snip(target ?? update.state.doc.toString()),
        );
      return;
    }
    if (dbgEnabled())
      dbg(
        "local push",
        this.doc.path,
        "ytext",
        snip(this.ytext.toString()),
        "->editor",
        snip(target ?? update.state.doc.toString()),
      );
    this.synchronizedDoc = null;
    this.pushingEditorChanges = true;
    this.sharedChangedWhilePushing = false;
    try {
      this.ytext.doc?.transact(() => {
        if (incremental) {
          update.changes.iterChanges((from, to, at, _end, inserted) => {
            if (to > from) this.ytext!.delete(at, to - from);
            if (inserted.length) this.ytext!.insert(at, inserted.toString());
          });
        } else {
          applyTextToYText(this.ytext!, target!);
        }
      }, this);
    } finally {
      this.pushingEditorChanges = false;
    }
    if (
      !this.sharedChangedWhilePushing &&
      this.ytext.doc &&
      (incremental || this.ytext.toString() === target)
    ) {
      this.synchronizedDoc = update.state.doc;
    }
  }

  private canApplyEditorChanges(update: ViewUpdate): boolean {
    const before = update.startState?.doc;
    if (
      !before ||
      this.sharedTransactionDepth !== 0 ||
      before !== this.synchronizedDoc ||
      !update.changes ||
      this.ytext?.length !== before.length
    )
      return false;
    // Yjs must never delete half of a stored surrogate pair. The full-text
    // fallback expands such replacements to cover both UTF-16 code units.
    const splitsSurrogate = (at: number): boolean => {
      if (at === 0 || at === before.length) return false;
      const pair = before.sliceString(at - 1, at + 1);
      return (
        pair.charCodeAt(0) >= 0xd800 &&
        pair.charCodeAt(0) <= 0xdbff &&
        pair.charCodeAt(1) >= 0xdc00 &&
        pair.charCodeAt(1) <= 0xdfff
      );
    };
    let safe = true;
    update.changes.iterChangedRanges((from, to) => {
      if (splitsSurrogate(from) || splitsSurrogate(to)) safe = false;
    });
    return safe;
  }

  destroy(): void {
    this.destroyed = true;
    this.detachDoc();
    this.editor = null as any;
  }
}

export const liveEdit = ViewPlugin.fromClass(LiveEditPluginValue);
