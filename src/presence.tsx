// Shared presence UI helpers: avatar stacks and canvas cursor overlays.
//
// Both markdown editors and canvas views use the same Yjs awareness transport
// (per-document `awareness.setLocalStateField("user", ...)`), so this module
// provides the rendering and lifecycle glue that each mount point needs.

import { createElement, type ReactElement, type CSSProperties } from "react";
import { createRoot, type Root } from "react-dom/client";
import type { Awareness } from "y-protocols/awareness";
import { holdPresence } from "./presenceOwners";

// ---------- types ----------

export interface AwarenessUser {
  name?: string;
  color?: string;
  colorLight?: string;
  avatarUrl?: string | null;
}

export interface PresenceEntry {
  clientId: number;
  name: string;
  color: string;
  avatarUrl: string | null;
  isLocal: boolean;
}

/**
 * Shared cursor position published in canvas-world (graph) coordinates so it
 * stays consistent across devices with different viewport sizes, pan, or zoom.
 * `space` versions the coordinate system: `"canvas"` is world coords from
 * Obsidian's `Canvas.posFromDom`; a missing/other value is ignored as stale
 * screen-space data from an older plugin version.
 */
export interface CanvasCursorState {
  x: number;
  y: number;
  space: "canvas";
}

export interface CanvasPresenceState {
  version: 1;
  sequence: number;
  selectedNodeIds?: string[];
  interaction?: {
    kind: "drag" | "resize";
    nodes: Array<{ id: string; x: number; y: number; width: number; height: number }>;
  };
  editingNodeId?: string;
  viewport?: { x: number; y: number; scale: number };
}

export function readCanvasPresence(value: unknown): CanvasPresenceState | null {
  if (!value || typeof value !== "object") return null;
  const state = value as Record<string, unknown>;
  if (
    state.version !== 1 ||
    !Number.isSafeInteger(state.sequence) ||
    (state.sequence as number) < 0
  )
    return null;
  if (
    state.selectedNodeIds !== undefined &&
    (!Array.isArray(state.selectedNodeIds) ||
      state.selectedNodeIds.some((id) => typeof id !== "string"))
  )
    return null;
  if (state.viewport !== undefined) {
    const viewport = state.viewport as Record<string, unknown>;
    if (
      !viewport ||
      typeof viewport.x !== "number" ||
      typeof viewport.y !== "number" ||
      typeof viewport.scale !== "number" ||
      !Number.isFinite(viewport.scale) ||
      viewport.scale <= 0
    )
      return null;
  }
  if (state.interaction !== undefined) {
    const interaction = state.interaction as Record<string, unknown>;
    if (
      !interaction ||
      (interaction.kind !== "drag" && interaction.kind !== "resize") ||
      !Array.isArray(interaction.nodes) ||
      interaction.nodes.some((node) => !isCanvasPresenceNode(node))
    )
      return null;
  }
  if (state.editingNodeId !== undefined && typeof state.editingNodeId !== "string") return null;
  return state as unknown as CanvasPresenceState;
}

function isCanvasPresenceNode(value: unknown): boolean {
  if (!value || typeof value !== "object") return false;
  const node = value as Record<string, unknown>;
  return (
    typeof node.id === "string" &&
    ["x", "y", "width", "height"].every(
      (key) => typeof node[key] === "number" && Number.isFinite(node[key]),
    )
  );
}

// ---------- canvas viewport adapter ----------

/**
 * Minimal view of Obsidian's internal Canvas instance exposing the viewport
 * transform we need. Grounded in the Obsidian desktop build:
 * - `posFromDom({x,y}) = { x: canvas.x + x/scale, y: canvas.y + y/scale }`
 *   where `{x,y}` are DOM pixels relative to the canvas center.
 * - `domFromPos({x,y}) = { x: (x - canvas.x)*scale, y: (y - canvas.y)*scale }`.
 * - `scale` is the authoritative pixel scale (`2 ** zoom`), kept in sync via
 *   `setScale`; `zoom` itself is log2 and must not be used as a pixel multiplier.
 * `x/y` are the world coords at the viewport CENTER (not top-left), because
 * `domPosFromEvt` is relative to the wrapper element's center.
 *
 * `canvasRect` is derived LIVE from the canvas `wrapperEl` (mirroring Obsidian's
 * `p3()`), NOT from the Canvas instance's cached `canvasRect` field. That field
 * is only refreshed in `onResize`, so on mobile — where sidebars, toolbars, and
 * viewport insets shift layout without firing resize — a cached `canvasRect.cx`
 * drifts from the actual center and produces a constant horizontal cursor
 * offset. Computing the center fresh on each read keeps the cursor overlay
 * aligned with where Obsidian actually renders nodes.
 */
export interface CanvasViewport {
  x: number;
  y: number;
  scale: number;
  /** Live rect of the canvas wrapper, in client coordinates. */
  canvasRect: { cx: number; cy: number; left: number; top: number; width: number; height: number };
  posFromDom: (p: { x: number; y: number }) => { x: number; y: number };
  domFromPos: (p: { x: number; y: number }) => { x: number; y: number };
}

/**
 * Reconstruct `p3(wrapperEl)`: the canvas-writer rect Obsidian's own code uses
 * for `canvasRect` ({ left, top, width, height, cx, cy }), but read live so we
 * stay correct after layout shifts that don't fire `onResize`. Returns `null`
 * when no measurable wrapper element is available.
 */
function liveWrapperRect(canvas: Record<string, unknown>): CanvasViewport["canvasRect"] | null {
  const wrapper = canvas.wrapperEl;
  if (!wrapper || typeof (wrapper as any).getBoundingClientRect !== "function") return null;
  const el = wrapper as HTMLElement;
  const r = el.getBoundingClientRect();
  // Mirror p3(): account for the client border so cx/cy lands on the content
  // box center, not the border box.
  const left = r.left + el.clientLeft;
  const top = r.top + el.clientTop;
  const width = el.clientWidth;
  const height = el.clientHeight;
  return { left, top, width, height, cx: left + width / 2, cy: top + height / 2 };
}

/**
 * Extract a {@link CanvasViewport} from a live Obsidian Canvas instance, if it
 * has the private viewport properties we rely on. Returns `null` when the shape
 * is unsupported (old/changed Obsidian) so callers can fall back gracefully.
 *
 * The center rect is read live from `wrapperEl`; if that is unavailable the
 * Canvas instance's cached `canvasRect` is used as a best-effort fallback.
 */
export function readCanvasViewport(canvas: unknown): CanvasViewport | null {
  if (!canvas || typeof canvas !== "object") return null;
  const c = canvas as Record<string, unknown>;
  const scale = c.scale;
  const x = c.x;
  const y = c.y;
  const posFromDom = c.posFromDom;
  const domFromPos = c.domFromPos;
  if (typeof scale !== "number" || !Number.isFinite(scale) || scale <= 0) return null;
  if (typeof x !== "number" || typeof y !== "number") return null;
  if (typeof posFromDom !== "function" || typeof domFromPos !== "function") return null;

  // Prefer the live wrapper rect; fall back to the cached field, validating it.
  const canvasRect = liveWrapperRect(c) ?? cachedCanvasRect(c.canvasRect);
  if (!canvasRect) return null;

  return {
    x,
    y,
    scale,
    canvasRect,
    // Bind to the canvas instance: Obsidian's `posFromDom`/`domFromPos` read
    // `this.x`, `this.scale`, etc., so an unbound reference would lose `this`
    // and return NaN. The wrapper arrow preserves the binding for every call.
    posFromDom: (p) => (posFromDom as Function).call(canvas, p),
    domFromPos: (p) => (domFromPos as Function).call(canvas, p),
  };
}

/** Feature-detected viewport mutation used by optional local follow mode. */
export function writeCanvasViewport(
  canvas: unknown,
  viewport: { x: number; y: number; scale: number },
): boolean {
  if (!canvas || typeof canvas !== "object" || viewport.scale <= 0) return false;
  const target = canvas as Record<string, unknown>;
  try {
    if (typeof target.setViewport === "function") {
      (target.setViewport as Function).call(canvas, {
        x: viewport.x,
        y: viewport.y,
        scale: viewport.scale,
      });
    } else if ("x" in target && "y" in target && typeof target.setScale === "function") {
      target.x = viewport.x;
      target.y = viewport.y;
      (target.setScale as Function).call(canvas, viewport.scale);
    } else {
      return false;
    }
    (target.requestFrame as Function | undefined)?.call(canvas);
    return true;
  } catch {
    return false;
  }
}

/** Validate and adopt the Canvas instance's cached `canvasRect`, if usable. */
function cachedCanvasRect(rect: unknown): CanvasViewport["canvasRect"] | null {
  if (!rect || typeof rect !== "object") return null;
  const r = rect as Record<string, unknown>;
  if (
    typeof r.cx !== "number" ||
    typeof r.cy !== "number" ||
    typeof r.left !== "number" ||
    typeof r.top !== "number"
  )
    return null;
  return {
    cx: r.cx,
    cy: r.cy,
    left: r.left,
    top: r.top,
    width: typeof r.width === "number" ? r.width : 0,
    height: typeof r.height === "number" ? r.height : 0,
  };
}

// ---------- helpers ----------

/** Derive 1–2 character initials from a display name. */
export function initials(name: string): string {
  const parts = name.trim().split(/\s+/).filter(Boolean);
  if (parts.length === 0) return "?";
  if (parts.length === 1) return parts[0].slice(0, 2).toUpperCase();
  return (parts[0][0]! + parts[parts.length - 1]![0]!).toUpperCase();
}

/**
 * Collect presence entries from awareness states, filtered by `viewing.kind`.
 * Includes both local and remote clients whose awareness has `state.user` and
 * `state.viewing.kind === kind`. Sorts local client first, then ascending
 * `clientId` for deterministic z-order and tests.
 */
export function collectPresenceEntries(
  states: Map<number, unknown>,
  localClientId: number,
  kind: "markdown" | "canvas",
): PresenceEntry[] {
  const entries: PresenceEntry[] = [];
  states.forEach((rawState, clientId) => {
    if (!rawState || typeof rawState !== "object") return;
    const state = rawState as Record<string, unknown>;
    const user = state.user as AwarenessUser | undefined;
    if (!user) return;
    const viewing = state.viewing as { kind?: string } | undefined;
    if (!viewing || viewing.kind !== kind) return;
    entries.push({
      clientId,
      name: user.name ?? "Anonymous",
      color: user.color ?? "#30bced",
      avatarUrl: user.avatarUrl ?? null,
      isLocal: clientId === localClientId,
    });
  });
  entries.sort((a, b) => {
    if (a.isLocal !== b.isLocal) return a.isLocal ? -1 : 1;
    return a.clientId - b.clientId;
  });
  return entries;
}

// ---------- viewing reference counting ----------

const viewingRefs = new WeakMap<Awareness, Map<string, number>>();

/**
 * Mark this awareness as "viewing" a given kind (markdown or canvas).
 * Reference-counted per `(Awareness, kind)`: the first mark sets the
 * `viewing` awareness field; the last cleanup clears it (only if the current
 * `viewing.kind` still matches). This prevents one of two open editors for the
 * same note from clearing presence while another remains open.
 */
export function markViewing(awareness: Awareness, kind: "markdown" | "canvas"): () => void {
  // Presence is published only while some view of the document is open.
  const releasePresence = holdPresence(awareness);
  let byKind = viewingRefs.get(awareness);
  if (!byKind) {
    byKind = new Map();
    viewingRefs.set(awareness, byKind);
  }
  const count = byKind.get(kind) ?? 0;
  byKind.set(kind, count + 1);
  if (count === 0) {
    awareness.setLocalStateField("viewing", { kind });
  }
  let released = false;
  return () => {
    if (released) return;
    released = true;
    const current = byKind.get(kind) ?? 0;
    if (current <= 1) {
      byKind.delete(kind);
      const localState = awareness.getLocalState();
      const viewing = localState?.viewing as { kind?: string } | undefined;
      if (viewing?.kind === kind) {
        awareness.setLocalStateField("viewing", null);
      }
    } else {
      byKind.set(kind, current - 1);
    }
    releasePresence();
  };
}

// ---------- canvas cursor awareness ----------

/** Publish the local pointer position (or null to clear) into awareness. */
export function setCanvasCursor(awareness: Awareness, cursor: CanvasCursorState | null): void {
  awareness.setLocalStateField("canvasCursor", cursor);
}

export function setCanvasPresence(
  awareness: Awareness,
  presence: CanvasPresenceState | null,
): void {
  awareness.setLocalStateField("canvasPresence", presence);
}

// ---------- React avatar stack ----------

/**
 * Render a stack of avatar bubbles. Returns `null` for zero entries so the
 * mount point can skip the DOM entirely. Each bubble has a CSS variable
 * `--realtime-presence-color` set to the device's cursor color and a centered
 * border ring via `::after`.
 */
export function PresenceAvatarStack({
  entries,
}: {
  entries: PresenceEntry[];
}): ReactElement | null {
  if (entries.length === 0) return null;
  return createElement(
    "div",
    { className: "realtime-presence-stack" },
    entries.map((entry) =>
      createElement(
        "div",
        {
          key: entry.clientId,
          className: "realtime-presence-avatar-wrap",
          style: { "--realtime-presence-color": entry.color } as CSSProperties,
          "aria-label": `${entry.isLocal ? "You: " : ""}${entry.name}`,
        },
        entry.avatarUrl
          ? createElement("img", {
              className: "realtime-presence-avatar",
              src: entry.avatarUrl,
              alt: entry.name,
              // Some themes override inherited border radii on images. The
              // clip-path keeps note and canvas presence photos circular even
              // under an !important theme rule.
              style: { borderRadius: "50%", clipPath: "circle(50%)" },
            })
          : createElement(
              "div",
              { className: "realtime-presence-avatar-fallback" },
              initials(entry.name),
            ),
      ),
    ),
  );
}

// ---------- mount helpers ----------

type AwarenessChange = { added: number[]; updated: number[]; removed: number[] };

/**
 * Mount a React avatar stack into `host`, subscribed to `awareness` changes.
 * Returns a cleanup function that unsubscribes, unmounts, removes the child
 * div, and releases the viewing mark.
 */
export function mountPresenceStack(
  host: HTMLElement,
  awareness: Awareness,
  kind: "markdown" | "canvas",
  className?: string,
): () => void {
  const container = document.createElement("div");
  if (className) container.className = className;
  host.appendChild(container);
  const root = createRoot(container);

  const render = () => {
    const entries = collectPresenceEntries(awareness.getStates(), awareness.doc.clientID, kind);
    root.render(PresenceAvatarStack({ entries }));
  };

  const listener = () => render();
  awareness.on("change", listener);
  const releaseViewing = markViewing(awareness, kind);
  render();

  return () => {
    awareness.off("change", listener);
    releaseViewing();
    root.unmount();
    container.remove();
  };
}

/**
 * Mount a canvas cursor overlay into `host`. Subscribes to awareness changes
 * and renders remote cursor markers for states with `state.canvasCursor` and
 * `state.user`. The shared cursor position is published in canvas-world
 * (graph) coordinates via {@link readCanvasViewport}, so it stays consistent
 * across devices with different viewport sizes, pan, or zoom.
 *
 * Remote markers are rendered by converting the published world coords back to
 * DOM pixels with the *local* viewport's `domFromPos`. A rAF viewport watcher
 * re-renders whenever the local pan/zoom changes — Obsidian exposes no public
 * viewport-change event, so we poll `canvas.x/y/scale` each frame while remote
 * markers exist, gated to avoid work when no cursors are shown.
 *
 * Publishes on `pointermove` (throttled with rAF), clears on `pointerleave`,
 * `window.blur`, and hidden `visibilitychange`.
 */
export function mountCanvasCursorOverlay(
  host: HTMLElement,
  awareness: Awareness,
  getCanvas: () => unknown,
): () => void {
  const layer = document.createElement("div");
  layer.className = "realtime-canvas-presence-layer";
  host.appendChild(layer);
  const root = createRoot(layer);
  let followedClientId: number | null = null;

  /** Read the live viewport; null when the canvas private API is unavailable. */
  const viewport = () => readCanvasViewport(getCanvas());

  /**
   * Convert a canvas-world cursor position to DOM pixels relative to the
   * overlay layer (top-left origin). Returns null when the viewport is
   * unavailable. `domFromPos` yields pixels relative to the canvas center, so
   * we add the layer-to-center offset (canvasRect.cx - layer.left).
   */
  const worldToLayer = (wx: number, wy: number): { x: number; y: number } | null => {
    const vp = viewport();
    if (!vp) return null;
    const d = vp.domFromPos({ x: wx, y: wy });
    const layerRect = layer.getBoundingClientRect();
    return {
      x: d.x + (vp.canvasRect.cx - layerRect.left),
      y: d.y + (vp.canvasRect.cy - layerRect.top),
    };
  };

  const collectMarkers = () => {
    const localId = awareness.doc.clientID;
    const markers: Array<{
      key: number;
      name: string;
      color: string;
      x: number;
      y: number;
    }> = [];
    awareness.getStates().forEach((rawState, clientId) => {
      if (clientId === localId) return;
      if (!rawState || typeof rawState !== "object") return;
      const state = rawState as Record<string, unknown>;
      const user = state.user as AwarenessUser | undefined;
      if (!user) return;
      const cursor = state.canvasCursor as CanvasCursorState | null | undefined;
      // Ignore stale screen-space data from older plugin versions: only
      // canvas-world coordinates (`space: "canvas"`) are usable across devices.
      if (!cursor || cursor.space !== "canvas") return;
      if (typeof cursor.x !== "number" || typeof cursor.y !== "number") return;
      const pos = worldToLayer(cursor.x, cursor.y);
      if (!pos) return;
      markers.push({
        key: clientId,
        name: user.name ?? "Anonymous",
        color: user.color ?? "#30bced",
        x: pos.x,
        y: pos.y,
      });
    });
    return markers;
  };

  const lastSequences = new Map<number, number>();
  const canvasNodes = () => {
    const canvas = getCanvas() as { getData?: () => unknown } | null;
    const data = canvas?.getData?.() as { nodes?: unknown[] } | undefined;
    const result = new Map<
      string,
      { id: string; x: number; y: number; width: number; height: number }
    >();
    for (const node of data?.nodes ?? []) {
      if (isCanvasPresenceNode(node)) {
        const value = node as { id: string; x: number; y: number; width: number; height: number };
        result.set(value.id, value);
      }
    }
    return result;
  };

  const collectPresence = () => {
    const localId = awareness.doc.clientID;
    const nodes = canvasNodes();
    const entries: Array<{
      key: string;
      name: string;
      color: string;
      kind: "selection" | "drag" | "resize" | "editing";
      x: number;
      y: number;
      width: number;
      height: number;
    }> = [];
    awareness.getStates().forEach((rawState, clientId) => {
      if (clientId === localId || !rawState || typeof rawState !== "object") return;
      const state = rawState as Record<string, unknown>;
      const presence = readCanvasPresence(state.canvasPresence);
      if (!presence) return;
      const prior = lastSequences.get(clientId);
      if (prior !== undefined && presence.sequence < prior) return;
      lastSequences.set(clientId, presence.sequence);
      const user = state.user as AwarenessUser | undefined;
      const name = user?.name ?? "Anonymous";
      const color = user?.color ?? "#30bced";
      const interaction = new Map(presence.interaction?.nodes.map((node) => [node.id, node]));
      for (const id of presence.selectedNodeIds ?? []) {
        const node = interaction.get(id) ?? nodes.get(id);
        if (node)
          entries.push({
            key: `${clientId}:selection:${id}`,
            name,
            color,
            kind: "selection",
            ...node,
          });
      }
      for (const node of presence.interaction?.nodes ?? []) {
        entries.push({
          key: `${clientId}:${presence.interaction!.kind}:${node.id}`,
          name,
          color,
          kind: presence.interaction!.kind,
          ...node,
        });
      }
      if (presence.editingNodeId) {
        const node = nodes.get(presence.editingNodeId);
        if (node)
          entries.push({
            key: `${clientId}:editing:${node.id}`,
            name,
            color,
            kind: "editing",
            ...node,
          });
      }
    });
    return entries.flatMap((entry) => {
      const topLeft = worldToLayer(entry.x, entry.y);
      const bottomRight = worldToLayer(entry.x + entry.width, entry.y + entry.height);
      if (!topLeft || !bottomRight) return [];
      return [
        {
          ...entry,
          x: topLeft.x,
          y: topLeft.y,
          width: bottomRight.x - topLeft.x,
          height: bottomRight.y - topLeft.y,
        },
      ];
    });
  };

  const render = () => {
    const markers = collectMarkers();
    const presence = collectPresence();
    root.render(
      createElement(
        "div",
        null,
        presence.map((entry) =>
          createElement(
            "div",
            {
              key: entry.key,
              className: `realtime-canvas-presence-box is-${entry.kind}`,
              style: {
                "--realtime-presence-color": entry.color,
                "--realtime-canvas-x": `${entry.x}px`,
                "--realtime-canvas-y": `${entry.y}px`,
                "--realtime-canvas-width": `${entry.width}px`,
                "--realtime-canvas-height": `${entry.height}px`,
                position: "absolute",
                left: 0,
                top: 0,
                width: entry.width,
                height: entry.height,
                transform: `translate(${entry.x}px, ${entry.y}px)`,
                border: `2px ${entry.kind === "drag" || entry.kind === "resize" ? "dashed" : "solid"} ${entry.color}`,
                borderRadius: 6,
                boxSizing: "border-box",
                pointerEvents: "none",
              } as CSSProperties,
            },
            createElement(
              "div",
              {
                className: "realtime-canvas-presence-label",
                style: {
                  position: "absolute",
                  left: -2,
                  bottom: "100%",
                  padding: "1px 5px",
                  borderRadius: "4px 4px 4px 0",
                  background: entry.color,
                  color: "white",
                  fontSize: 11,
                  whiteSpace: "nowrap",
                },
              },
              entry.name,
            ),
          ),
        ),
        markers.map((m) =>
          createElement(
            "div",
            {
              key: m.key,
              className: "realtime-canvas-cursor",
              "data-client-id": m.key,
              style: {
                "--realtime-presence-color": m.color,
                "--realtime-canvas-x": `${m.x}px`,
                "--realtime-canvas-y": `${m.y}px`,
              } as CSSProperties,
            },
            createElement("div", { className: "realtime-canvas-cursor-dot" }),
            createElement(
              "button",
              {
                type: "button",
                className: "realtime-canvas-cursor-label",
                "aria-label": `Follow ${m.name}'s Canvas view`,
                style: {
                  display: "block",
                  border: 0,
                  lineHeight: "inherit",
                  pointerEvents: "auto",
                  cursor: "pointer",
                },
                onClick: (event) => {
                  event.stopPropagation();
                  followedClientId = followedClientId === m.key ? null : m.key;
                  applyFollowedViewport();
                },
              },
              m.name,
            ),
          ),
        ),
      ),
    );
  };

  const applyFollowedViewport = () => {
    if (followedClientId === null) return;
    const state = awareness.getStates().get(followedClientId) as
      | Record<string, unknown>
      | undefined;
    const viewing = state?.viewing as { kind?: string } | undefined;
    const presence = readCanvasPresence(state?.canvasPresence);
    if (!state || viewing?.kind !== "canvas" || !presence?.viewport) {
      followedClientId = null;
      return;
    }
    if (!writeCanvasViewport(getCanvas(), presence.viewport)) followedClientId = null;
  };
  const awarenessListener = () => {
    applyFollowedViewport();
    render();
  };
  awareness.on("change", awarenessListener);
  render();

  // Viewport watcher: re-render remote markers when the local pan/zoom moves.
  // Obsidian's Canvas sets `viewportChanged` + `requestFrame` internally but
  // emits no public event, so we poll on rAF only while remote markers exist.
  let viewRaf: number | null = null;
  let viewRunning = false;
  let lastSig = "";
  const viewportSignature = () => {
    const vp = viewport();
    if (!vp) return "";
    // Include pan/zoom AND the layer rect + canvasRect center, so cursors
    // stay positioned across window resize, sidebar toggles, and pane layout
    // shifts that move the canvas without changing pan/zoom.
    const lr = layer.getBoundingClientRect();
    return `${vp.x}|${vp.y}|${vp.scale}|${vp.canvasRect.cx}|${vp.canvasRect.cy}|${lr.left}|${lr.top}|${lr.width}|${lr.height}`;
  };
  const hasRemoteMarkers = () => {
    const localId = awareness.doc.clientID;
    let found = false;
    awareness.getStates().forEach((state, id) => {
      if (found || id === localId) return;
      if (!state || typeof state !== "object") return;
      const s = state as Record<string, unknown>;
      const c = s.canvasCursor as CanvasCursorState | undefined;
      if (c && c.space === "canvas") found = true;
    });
    return found;
  };
  const startWatcher = () => {
    if (viewRunning) return;
    viewRunning = true;
    lastSig = viewportSignature();
    const tick = () => {
      if (!viewRunning) return;
      if (!hasRemoteMarkers()) {
        viewRunning = false;
        viewRaf = null;
        return;
      }
      const sig = viewportSignature();
      if (sig !== "" && sig !== lastSig) {
        lastSig = sig;
        render();
      }
      viewRaf = requestAnimationFrame(tick);
    };
    viewRaf = requestAnimationFrame(tick);
  };

  let rafId: number | null = null;
  let pending: { x: number; y: number } | null = null;

  const onPointerMove = (event: PointerEvent) => {
    const vp = viewport();
    if (!vp) return; // cannot convert to world coords without the viewport
    // Map the client pointer to DOM pixels relative to the canvas center, then
    // to canvas-world coords via posFromDom.
    const domX = event.clientX - vp.canvasRect.cx;
    const domY = event.clientY - vp.canvasRect.cy;
    pending = vp.posFromDom({ x: domX, y: domY });
    if (rafId === null) {
      rafId = requestAnimationFrame(() => {
        rafId = null;
        if (pending) {
          setCanvasCursor(awareness, { ...pending, space: "canvas" });
          pending = null;
          // Ensure the remote-viewer loop is running while we have a cursor.
          startWatcher();
        }
      });
    }
  };

  const clearCursor = () => {
    if (rafId !== null) {
      cancelAnimationFrame(rafId);
      rafId = null;
    }
    pending = null;
    setCanvasCursor(awareness, null);
    render();
  };

  const onPointerLeave = () => clearCursor();
  const onBlur = () => clearCursor();
  const onVisibilityChange = () => {
    if (document.hidden) clearCursor();
  };

  host.addEventListener("pointermove", onPointerMove);
  host.addEventListener("pointerleave", onPointerLeave);
  window.addEventListener("blur", onBlur);
  document.addEventListener("visibilitychange", onVisibilityChange);
  const cancelFollow = () => {
    followedClientId = null;
  };
  host.addEventListener("pointerdown", cancelFollow, true);
  host.addEventListener("wheel", cancelFollow, true);

  // Start the viewport watcher whenever a remote state appears, so remote
  // cursors move in lockstep with the local pan/zoom even before we publish.
  const ensureWatcher = () => {
    if (hasRemoteMarkers()) startWatcher();
  };
  awareness.on("change", ensureWatcher);
  ensureWatcher();

  return () => {
    awareness.off("change", awarenessListener);
    awareness.off("change", ensureWatcher);
    host.removeEventListener("pointermove", onPointerMove);
    host.removeEventListener("pointerleave", onPointerLeave);
    window.removeEventListener("blur", onBlur);
    document.removeEventListener("visibilitychange", onVisibilityChange);
    host.removeEventListener("pointerdown", cancelFollow, true);
    host.removeEventListener("wheel", cancelFollow, true);
    if (rafId !== null) cancelAnimationFrame(rafId);
    viewRunning = false;
    if (viewRaf !== null) cancelAnimationFrame(viewRaf);
    setCanvasCursor(awareness, null);
    root.unmount();
    layer.remove();
  };
}
