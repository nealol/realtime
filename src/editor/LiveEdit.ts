// Binds an Obsidian (CodeMirror 6) editor to a Yjs Y.Text, in both directions.
// Adapted from y-codemirror.next (MIT, © Kevin Jahns) and the Relay plugin.

import type { ChangeSpec } from "@codemirror/state";
import { EditorView, ViewPlugin, type PluginValue, type ViewUpdate } from "@codemirror/view";
import type * as Y from "yjs";
import type { Document } from "../Document";
import { ensureDocumentForEditor, requestSaveForEditor } from "./context";
import { ySyncAnnotation } from "./annotations";
import { applyTextToYText } from "../diff";
import { mergeText } from "../textMerge";
import { dbg, snip } from "../debug";

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
  private observer: (() => void) | null = null;
  private unsubscribeDestroy: (() => void) | null = null;
  private destroyed = false;
  private editorTextAtBind: string | null = null;
  /**
   * Editor text before the first change made while no document was bound
   * (e.g. while one was being rebuilt), so binding still pushes that change.
   */
  private unpushedBaseline: string | null = null;

  constructor(editor: EditorView) {
    this.editor = editor;
    this.tryBind();
  }

  /** Attempt to find this file's Document and attach. Retries until sync is ready. */
  private tryBind(): void {
    if (this.destroyed || this.doc) return;
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

    const observer = () => this.onYTextChanged();
    this.observer = observer;
    this.ytext.observe(observer);

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
   *  - otherwise, if the shared text has content, it is authoritative -> update editor;
   *  - else if the editor has content, seed the shared text from it;
   *  - if both are empty, do nothing.
   * We never overwrite editor content with an empty shared document.
   */
  private reconcileOnAttach(): void {
    if (!this.ytext || this.destroyed) return;
    const ytext = this.ytext;
    const shared = ytext.toString();
    const current = this.editor.state.doc.toString();
    if (shared === current) return;

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

    if (shared.length > 0) {
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
    if (this.doc) this.doc.unbindEditor();
    this.observer = null;
    this.ytext = null;
    this.doc = null;
    this.editorTextAtBind = null;
  }

  /** Apply a remote Yjs change to the editor. */
  private onYTextChanged(): void {
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
    if (current === target) return;
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
  }

  /** Push local editor edits into the shared Y.Text. */
  update(update: ViewUpdate): void {
    if (this.destroyed) return;
    if (!this.doc || this.doc.isDestroyed()) {
      if (update.docChanged) this.unpushedBaseline ??= update.startState?.doc.toString() ?? null;
      if (this.doc) this.detachDoc();
      this.tryBind();
      return;
    }
    if (!this.ytext || !update.docChanged) return;
    if (this.doc && !this.doc.isReady()) return;

    // Self-healing reconcile: make ytext match the editor's *current* text via a
    // minimal prefix/suffix diff, instead of mapping CodeMirror change offsets
    // onto ytext positions. Offset-mapping silently corrupts (and duplicates
    // characters) the moment ytext and the editor drift apart — e.g. when a
    // single ViewUpdate bundles an applied-remote change with a user keystroke.
    // This approach can't drift: it is idempotent for the remote changes we
    // ourselves applied (ytext already equals the editor, so it no-ops), which
    // is also why it needs no annotation filter.
    const target = update.state.doc.toString();
    if (this.ytext.toString() === target) return;

    // Hardening against the mobile-reopen duplication: never seed a pre-existing
    // buffer into an empty, not-yet-synced shared doc (the server may already
    // hold this content). See {@link shouldPushEditorToShared}.
    if (!this.mayPushToShared()) {
      dbg("local push DEFERRED (pre-sync empty shared)", this.doc.path, "->editor", snip(target));
      return;
    }
    dbg(
      "local push",
      this.doc?.path,
      "ytext",
      snip(this.ytext.toString()),
      "->editor",
      snip(target),
    );
    this.ytext.doc?.transact(() => {
      applyTextToYText(this.ytext!, target);
    }, this);
  }

  destroy(): void {
    this.destroyed = true;
    this.detachDoc();
    this.editor = null as any;
  }
}

export const liveEdit = ViewPlugin.fromClass(LiveEditPluginValue);
