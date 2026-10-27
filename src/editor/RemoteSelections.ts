// Renders remote collaborators' cursors and selections.
// Adapted from y-codemirror.next (MIT, © Kevin Jahns) and the Relay plugin.

import { Annotation, RangeSet, type Range } from "@codemirror/state";
import {
  EditorView,
  ViewPlugin,
  Decoration,
  WidgetType,
  type PluginValue,
  type ViewUpdate,
  type DecorationSet,
} from "@codemirror/view";
import { Fragment, createElement } from "react";
import { flushSync } from "react-dom";
import { createRoot, type Root } from "react-dom/client";
import * as Y from "yjs";
import type { Awareness } from "y-protocols/awareness";
import type * as YType from "yjs";
import type { Document } from "../Document";
import { getDocumentForEditor, isNoteFragmentEditor } from "./context";

export const yRemoteSelectionsTheme = EditorView.baseTheme({
  ".cm-ySelection": {},
  ".cm-yLineSelection": { padding: 0, margin: "0px 2px 0px 4px" },
  ".cm-ySelectionCaret": {
    position: "relative",
    borderLeft: "1px solid black",
    borderRight: "1px solid black",
    marginLeft: "-1px",
    marginRight: "-1px",
    boxSizing: "border-box",
    display: "inline",
  },
  ".cm-ySelectionCaretDot": {
    borderRadius: "50%",
    position: "absolute",
    width: ".4em",
    height: ".4em",
    top: "-.2em",
    left: "-.2em",
    backgroundColor: "inherit",
    transition: "transform .3s ease-in-out",
    boxSizing: "border-box",
  },
  ".cm-ySelectionCaret:hover > .cm-ySelectionCaretDot": {
    transformOrigin: "bottom center",
    transform: "scale(0)",
  },
  ".cm-ySelectionInfo::after": {
    content: "attr(data-name)",
  },
  ".cm-ySelectionInfo": {
    position: "absolute",
    top: "-1.05em",
    left: "-1px",
    fontSize: ".75em",
    fontFamily: "sans-serif",
    fontStyle: "normal",
    fontWeight: "normal",
    lineHeight: "normal",
    userSelect: "none",
    color: "white",
    paddingLeft: "2px",
    paddingRight: "2px",
    zIndex: "101",
    transition: "opacity .3s ease-in-out",
    backgroundColor: "inherit",
    opacity: "0",
    transitionDelay: "0s",
    whiteSpace: "nowrap",
  },
  ".cm-ySelectionCaret:hover > .cm-ySelectionInfo": {
    opacity: "1",
    transitionDelay: "0s",
  },
});

const yRemoteSelectionsAnnotation = Annotation.define<Array<number>>();
const caretRoots = new WeakMap<HTMLElement, Root>();

class YRemoteCaretWidget extends WidgetType {
  constructor(
    readonly color: string,
    readonly name: string,
  ) {
    super();
  }

  toDOM(): HTMLElement {
    const span = document.createElement("span");
    span.className = "cm-ySelectionCaret";
    span.setAttribute("style", `background-color: ${this.color}; border-color: ${this.color}`);
    const root = createRoot(span);
    caretRoots.set(span, root);
    flushSync(() => {
      root.render(createElement(RemoteCaret, { name: this.name }));
    });
    return span;
  }

  eq(other: YRemoteCaretWidget): boolean {
    return other.color === this.color && other.name === this.name;
  }

  compare(other: YRemoteCaretWidget): boolean {
    return this.eq(other);
  }

  updateDOM(): boolean {
    return false;
  }

  destroy(dom: HTMLElement): void {
    caretRoots.get(dom)?.unmount();
    caretRoots.delete(dom);
  }

  get estimatedHeight(): number {
    return -1;
  }

  ignoreEvent(): boolean {
    return true;
  }
}

function RemoteCaret({ name }: { name: string }) {
  return createElement(
    Fragment,
    null,
    "⁠",
    createElement("div", { className: "cm-ySelectionCaretDot" }),
    "⁠",
    // Render the name via a CSS pseudo-element (content: attr(data-name))
    // rather than a real text node. A text node here lives inside CodeMirror's
    // contenteditable and can be absorbed into the document by the browser's
    // DOM observer (e.g. when typing/IME at a remote cursor), which then gets
    // forwarded into the shared Y.Text. An attribute is invisible to that path.
    createElement("div", { className: "cm-ySelectionInfo", "data-name": name }),
    "⁠",
  );
}

type AwarenessChange = { added: number[]; updated: number[]; removed: number[] };

class YRemoteSelectionsPluginValue implements PluginValue {
  private editor: EditorView;
  decorations: DecorationSet;
  private doc: Document | null = null;
  private awareness: Awareness | null = null;
  private listener: ((change: AwarenessChange) => void) | null = null;
  private destroyed = false;
  /** Edits only part of its note: never binds (see {@link isNoteFragmentEditor}). */
  private readonly inert: boolean;

  constructor(editor: EditorView) {
    this.editor = editor;
    this.decorations = RangeSet.of([]);
    this.inert = isNoteFragmentEditor(editor);
    this.bind();
  }

  private bind(): void {
    if (this.destroyed || this.inert || this.doc) return;
    const doc = getDocumentForEditor(this.editor);
    if (!doc) return;
    this.doc = doc;
    this.awareness = doc.awareness;
    this.listener = ({ added, updated, removed }) => {
      const clients = added.concat(updated).concat(removed);
      if (clients.some((id) => id !== this.awareness?.doc.clientID)) {
        this.editor.dispatch({ annotations: [yRemoteSelectionsAnnotation.of([])] });
      }
    };
    this.awareness.on("change", this.listener);
  }

  destroy(): void {
    this.destroyed = true;
    if (this.listener && this.awareness) {
      this.awareness.off("change", this.listener);
    }
    this.listener = null;
    this.awareness = null;
    this.doc = null;
    this.editor = null as any;
  }

  update(update: ViewUpdate): void {
    if (this.destroyed) return;
    if (!this.doc) {
      this.bind();
      if (!this.doc) return;
    }
    const ytext = this.doc.ytext;
    const ydoc = ytext.doc;
    const awareness = this.awareness;
    if (!ydoc || !awareness) return;

    const decorations: Array<Range<Decoration>> = [];
    const localState = awareness.getLocalState();

    // Publish our own cursor position into awareness.
    if (localState != null) {
      const hasFocus = update.view.hasFocus && update.view.dom.ownerDocument.hasFocus();
      const sel = hasFocus ? update.state.selection.main : null;
      const currentAnchor =
        localState.cursor == null
          ? null
          : Y.createRelativePositionFromJSON(localState.cursor.anchor);
      const currentHead =
        localState.cursor == null ? null : Y.createRelativePositionFromJSON(localState.cursor.head);

      if (sel != null) {
        const anchor = Y.createRelativePositionFromTypeIndex(ytext, sel.anchor);
        const head = Y.createRelativePositionFromTypeIndex(ytext, sel.head);
        if (
          localState.cursor == null ||
          !Y.compareRelativePositions(currentAnchor, anchor) ||
          !Y.compareRelativePositions(currentHead, head)
        ) {
          // Defer to avoid re-entrant editor dispatches inside update().
          queueMicrotask(() => {
            if (!this.destroyed) awareness.setLocalStateField("cursor", { anchor, head });
          });
        }
      } else if (localState.cursor != null && hasFocus) {
        queueMicrotask(() => {
          if (!this.destroyed) awareness.setLocalStateField("cursor", null);
        });
      }
    }

    // Render every remote cursor / selection.
    awareness.getStates().forEach((state, clientId) => {
      if (clientId === awareness.doc.clientID) return;
      const cursor = state.cursor;
      if (cursor == null || cursor.anchor == null || cursor.head == null) return;

      const anchor = Y.createAbsolutePositionFromRelativePosition(
        cursor.anchor as YType.RelativePosition,
        ydoc,
      );
      const head = Y.createAbsolutePositionFromRelativePosition(
        cursor.head as YType.RelativePosition,
        ydoc,
      );
      if (anchor == null || head == null || anchor.type !== ytext || head.type !== ytext) return;

      const docLen = update.state.doc.length;
      if (anchor.index > docLen || head.index > docLen) return;

      const { color = "#30bced", name = "Anonymous" } = state.user || {};
      const colorLight = (state.user && state.user.colorLight) || color + "33";
      const start = Math.min(anchor.index, head.index);
      const end = Math.max(anchor.index, head.index);
      const startLine = update.view.state.doc.lineAt(start);
      const endLine = update.view.state.doc.lineAt(end);

      if (startLine.number === endLine.number) {
        if (start !== end) {
          decorations.push({
            from: start,
            to: end,
            value: Decoration.mark({
              attributes: { style: `background-color: ${colorLight}` },
              class: "cm-ySelection",
            }),
          });
        }
      } else {
        decorations.push({
          from: start,
          to: startLine.from + startLine.length,
          value: Decoration.mark({
            attributes: { style: `background-color: ${colorLight}` },
            class: "cm-ySelection",
          }),
        });
        decorations.push({
          from: endLine.from,
          to: end,
          value: Decoration.mark({
            attributes: { style: `background-color: ${colorLight}` },
            class: "cm-ySelection",
          }),
        });
        for (let i = startLine.number + 1; i < endLine.number; i++) {
          const linePos = update.view.state.doc.line(i).from;
          decorations.push({
            from: linePos,
            to: linePos,
            value: Decoration.line({
              attributes: { class: "cm-yLineSelection", style: `background-color: ${colorLight}` },
            }),
          });
        }
      }

      decorations.push({
        from: head.index,
        to: head.index,
        value: Decoration.widget({
          side: head.index - anchor.index > 0 ? -1 : 1,
          block: false,
          widget: new YRemoteCaretWidget(color, name),
        }),
      });
    });

    this.decorations = Decoration.set(decorations, true);
  }
}

export const yRemoteSelections = ViewPlugin.fromClass(YRemoteSelectionsPluginValue, {
  decorations: (v) => v.decorations,
});
