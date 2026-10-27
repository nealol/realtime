// Mounts the top-right avatar stack into a CodeMirror editor's DOM, mirroring
// the binding pattern in RemoteSelections.ts: bind on first update when the
// document is available, retry until bound, clean up on destroy.

import { EditorView, ViewPlugin, type PluginValue, type ViewUpdate } from "@codemirror/view";
import type { Document } from "../Document";
import { getDocumentForEditor, isNoteFragmentEditor } from "./context";
import { mountPresenceStack } from "../presence";

class MarkdownPresencePluginValue implements PluginValue {
  private editor: EditorView;
  private doc: Document | null = null;
  private cleanup: (() => void) | null = null;
  private destroyed = false;
  /** Edits only part of its note: never mounts (see {@link isNoteFragmentEditor}). */
  private readonly inert: boolean;

  constructor(editor: EditorView) {
    this.editor = editor;
    this.inert = isNoteFragmentEditor(editor);
    this.bind();
  }

  private bind(): void {
    if (this.destroyed || this.inert || this.doc) return;
    const doc = getDocumentForEditor(this.editor);
    if (!doc) return;
    this.doc = doc;
    this.cleanup = mountPresenceStack(
      this.editor.dom,
      doc.awareness,
      "markdown",
      "realtime-markdown-presence-stack",
    );
  }

  update(_update: ViewUpdate): void {
    if (this.destroyed) return;
    if (!this.doc) this.bind();
  }

  destroy(): void {
    this.destroyed = true;
    this.cleanup?.();
    this.cleanup = null;
    this.doc = null;
    this.editor = null as unknown as EditorView;
  }
}

export const markdownPresence = ViewPlugin.fromClass(MarkdownPresencePluginValue);

export const markdownPresenceTheme = EditorView.baseTheme({
  ".cm-editor": { position: "relative" },
});
