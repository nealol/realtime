import type { EditorView } from "@codemirror/view";
import { editorInfoField } from "obsidian";
import type RealtimePlugin from "../main";
import type { Document } from "../Document";

export function getPlugin(editor: EditorView): RealtimePlugin | null {
  const info = editor.state.field(editorInfoField, false);
  // app is attached to the editor info in Obsidian.
  const app = (info as any)?.app;
  return app?.plugins?.plugins?.["realtime"] ?? null;
}

/**
 * Whether this editor's buffer holds only part of the note its
 * editorInfoField names. Such an editor must never bind to the note's shared
 * state: syncing it would replace the whole note with the fragment, or load
 * the whole note into the fragment's slot. Obsidian creates two kinds:
 *
 * - The Live Preview table widget edits each cell in a nested EditorView
 *   mounted inside a `.table-cell-wrapper`. It shares the host view's owner,
 *   so it names the note's file. The host editor already carries the cell's
 *   edits.
 * - A markdown embed scoped to a heading or block (e.g. a Canvas file node
 *   linking `note.md#Section`) is the owner, with a non-empty `subpath`. Its
 *   buffer is that section, and every edit to it, including ones we apply, is
 *   spliced back into the file around the section, so the note on disk would
 *   end up nested inside itself.
 *
 * Whole-file embeds (`subpath === ""`) hold the entire note and bind normally.
 * Both signals exist before Obsidian installs plugin extensions, so this is
 * reliable from a view plugin's constructor.
 */
export function isNoteFragmentEditor(editor: EditorView): boolean {
  if (editor.dom.closest(".table-cell-wrapper") !== null) return true;
  const info: unknown = editor.state.field(editorInfoField, false);
  if (!info || typeof info !== "object" || !("subpath" in info)) return false;
  return typeof info.subpath === "string" && info.subpath.length > 0;
}

export function requestSaveForEditor(editor: EditorView): void {
  const info = editor.state.field(editorInfoField, false);
  const file = info?.file;
  const app = (info as any)?.app;
  const workspace = app?.workspace as any;
  if (!file || !workspace) return;

  const views = new Set<any>();
  const collect = (leaf: any): void => {
    const view = leaf?.view;
    if (view?.file?.path === file.path) views.add(view);
  };

  if (typeof workspace.iterateAllLeaves === "function") {
    workspace.iterateAllLeaves(collect);
  }
  if (typeof workspace.getLeavesOfType === "function") {
    for (const leaf of workspace.getLeavesOfType("markdown") ?? []) collect(leaf);
  }

  for (const view of views) {
    if (typeof view.requestSave === "function") {
      view.requestSave();
    } else if (typeof view.save === "function") {
      void Promise.resolve(view.save()).catch((error) => {
        console.error(`[Realtime] editor save failed for ${file.path}`, error);
      });
    }
  }
}

export function getDocumentForEditor(editor: EditorView): Document | null {
  const info = editor.state.field(editorInfoField, false);
  const file = info?.file;
  if (!file) return null;
  const plugin = getPlugin(editor);
  if (!plugin || !plugin.vaultSync) return null;
  return plugin.vaultSync.getDocumentForPath(file.path) ?? null;
}

export function ensureDocumentForEditor(editor: EditorView): Document | null {
  const info = editor.state.field(editorInfoField, false);
  const file = info?.file;
  if (!file) return null;
  const plugin = getPlugin(editor);
  if (!plugin || !plugin.vaultSync) return null;
  return plugin.vaultSync.ensureDocumentForPath(file.path) ?? null;
}
