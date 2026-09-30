import { describe, it, expect, vi } from "vitest";
import * as Y from "yjs";
import { Awareness } from "y-protocols/awareness";
import { createRoot } from "react-dom/client";
import { flushSync } from "react-dom";
import {
  collectPresenceEntries,
  markViewing,
  setCanvasCursor,
  readCanvasViewport,
  writeCanvasViewport,
  readCanvasPresence,
  setCanvasPresence,
  mountCanvasCursorOverlay,
  PresenceAvatarStack,
  initials,
  type PresenceEntry,
} from "../../src/presence";
import { registerPresenceOwner } from "../../src/presenceOwners";

function makeAwareness(): Awareness {
  const doc = new Y.Doc();
  return new Awareness(doc);
}

function set_user(aw: Awareness, user: Record<string, unknown>): void {
  aw.setLocalStateField("user", user);
}

describe("initials", () => {
  it("returns ? for empty string", () => {
    expect(initials("")).toBe("?");
  });
  it("returns first two chars for single word", () => {
    expect(initials("Alice")).toBe("AL");
  });
  it("returns first+last initials for multi-word", () => {
    expect(initials("Alice Bob")).toBe("AB");
    expect(initials("Alice Bob Charlie")).toBe("AC");
  });
});

describe("collectPresenceEntries", () => {
  it("filters by viewing.kind and includes local + remote, sorted local-first", () => {
    const aw = makeAwareness();
    const localId = aw.doc.clientID;

    // Local: viewing markdown
    set_user(aw, { name: "Local", color: "#ff0000", avatarUrl: "https://x/l.png" });
    aw.setLocalStateField("viewing", { kind: "markdown" });

    // Remote 1: viewing markdown
    const states = new Map<number, unknown>(aw.getStates());
    const remote1Id = localId + 1;
    const remote2Id = localId + 2;
    states.set(remote1Id, {
      user: { name: "Remote1", color: "#00ff00", avatarUrl: null },
      viewing: { kind: "markdown" },
    });
    // Remote 2: viewing canvas (should be filtered out for markdown)
    states.set(remote2Id, {
      user: { name: "Remote2", color: "#0000ff", avatarUrl: "https://x/r2.png" },
      viewing: { kind: "canvas" },
    });
    // Remote 3: no viewing field
    states.set(localId + 3, {
      user: { name: "NoViewing", color: "#aaaaaa" },
    });

    const entries = collectPresenceEntries(states, localId, "markdown");
    expect(entries).toHaveLength(2);
    expect(entries[0]!.isLocal).toBe(true);
    expect(entries[0]!.name).toBe("Local");
    expect(entries[0]!.avatarUrl).toBe("https://x/l.png");
    expect(entries[1]!.isLocal).toBe(false);
    expect(entries[1]!.clientId).toBe(remote1Id);
    expect(entries[1]!.avatarUrl).toBeNull();
  });

  it("defaults missing name/color", () => {
    const aw = makeAwareness();
    const localId = aw.doc.clientID;
    const states = new Map<number, unknown>(aw.getStates());
    states.set(localId, { user: {}, viewing: { kind: "canvas" } });
    const entries = collectPresenceEntries(states, localId, "canvas");
    expect(entries).toHaveLength(1);
    expect(entries[0]!.name).toBe("Anonymous");
    expect(entries[0]!.color).toBe("#30bced");
  });

  it("sorts remote clients by ascending clientId", () => {
    const aw = makeAwareness();
    const localId = aw.doc.clientID;
    const states = new Map<number, unknown>();
    states.set(localId + 5, { user: { name: "E" }, viewing: { kind: "markdown" } });
    states.set(localId + 2, { user: { name: "B" }, viewing: { kind: "markdown" } });
    states.set(localId, { user: { name: "Local" }, viewing: { kind: "markdown" } });
    const entries = collectPresenceEntries(states, localId, "markdown");
    expect(entries.map((e) => e.name)).toEqual(["Local", "B", "E"]);
  });
});

describe("markViewing", () => {
  it("sets viewing on first mark and clears on last release", () => {
    const aw = makeAwareness();
    const release1 = markViewing(aw, "markdown");
    expect(aw.getLocalState()?.viewing?.kind).toBe("markdown");
    const release2 = markViewing(aw, "markdown");
    // Still set after second mark
    expect(aw.getLocalState()?.viewing?.kind).toBe("markdown");
    // Release one — still set
    release1();
    expect(aw.getLocalState()?.viewing?.kind).toBe("markdown");
    // Release the last — cleared
    release2();
    expect(aw.getLocalState()?.viewing ?? null).toBeNull();
  });

  it("does not clear viewing if a different kind is current", () => {
    const aw = makeAwareness();
    const releaseMd = markViewing(aw, "markdown");
    // Simulate switching to canvas via another mark
    const releaseCanvas = markViewing(aw, "canvas");
    // Now viewing is canvas; releasing markdown should not clear it
    releaseMd();
    expect(aw.getLocalState()?.viewing?.kind).toBe("canvas");
    releaseCanvas();
    expect(aw.getLocalState()?.viewing ?? null).toBeNull();
  });
});

describe("presence holds", () => {
  it("publishes an owned awareness only while some view holds it", () => {
    const aw = makeAwareness();
    const user = { name: "A" };
    let holds = 0;
    aw.setLocalState(null);
    registerPresenceOwner(aw, {
      holdPresence: () => {
        if (holds++ === 0) aw.setLocalState({ user });
        return () => {
          if (--holds === 0) aw.setLocalState(null);
        };
      },
    });

    const first = markViewing(aw, "markdown");
    expect(aw.getLocalState()).toEqual({ user, viewing: { kind: "markdown" } });
    const second = markViewing(aw, "markdown");
    first();
    first();
    expect(aw.getLocalState()?.viewing).toEqual({ kind: "markdown" });
    second();
    expect(aw.getLocalState()).toBeNull();
  });
});

describe("setCanvasCursor", () => {
  it("writes { x, y, space:'canvas' } and clears to null without changing state.user", () => {
    const aw = makeAwareness();
    set_user(aw, { name: "Local", color: "#ff0000" });
    setCanvasCursor(aw, { x: 10, y: 20, space: "canvas" });
    expect(aw.getLocalState()?.canvasCursor).toEqual({ x: 10, y: 20, space: "canvas" });
    expect(aw.getLocalState()?.user?.name).toBe("Local");
    setCanvasCursor(aw, null);
    expect(aw.getLocalState()?.canvasCursor ?? null).toBeNull();
    expect(aw.getLocalState()?.user?.name).toBe("Local");
  });
});

describe("Canvas collaboration awareness", () => {
  it("round-trips versioned selection, drag, editing, and viewport state", () => {
    const aw = makeAwareness();
    const state = {
      version: 1 as const,
      sequence: 4,
      selectedNodeIds: ["a", "b"],
      interaction: {
        kind: "drag" as const,
        nodes: [{ id: "a", x: 10, y: 20, width: 100, height: 80 }],
      },
      editingNodeId: "b",
      viewport: { x: 5, y: 6, scale: 2 },
    };
    setCanvasPresence(aw, state);
    expect(readCanvasPresence(aw.getLocalState()?.canvasPresence)).toEqual(state);
    setCanvasPresence(aw, null);
    expect(aw.getLocalState()?.canvasPresence ?? null).toBeNull();
    aw.destroy();
  });

  it("rejects unsupported versions and malformed payloads", () => {
    expect(readCanvasPresence({ version: 2, sequence: 1 })).toBeNull();
    expect(readCanvasPresence({ version: 1, sequence: -1 })).toBeNull();
    expect(readCanvasPresence({ version: 1, sequence: 1, selectedNodeIds: [3] })).toBeNull();
    expect(
      readCanvasPresence({
        version: 1,
        sequence: 1,
        interaction: { kind: "drag", nodes: [{ id: "a", x: 0 }] },
      }),
    ).toBeNull();
  });

  it("renders remote selection labels and drag ghosts in local world coordinates", async () => {
    const aw = makeAwareness();
    const remoteId = aw.doc.clientID + 1;
    const states = aw.getStates() as Map<number, any>;
    states.set(remoteId, {
      user: { name: "Remote", color: "#ff0000" },
      viewing: { kind: "canvas" },
      canvasCursor: { x: 20, y: 30, space: "canvas" },
      canvasPresence: {
        version: 1,
        sequence: 2,
        selectedNodeIds: ["a"],
        interaction: {
          kind: "drag",
          nodes: [{ id: "a", x: 10, y: 20, width: 100, height: 50 }],
        },
        viewport: { x: 5, y: 6, scale: 2 },
      },
    });
    const host = document.createElement("div");
    document.body.appendChild(host);
    const wrapper = document.createElement("div");
    Object.defineProperties(wrapper, {
      clientWidth: { value: 800 },
      clientHeight: { value: 600 },
      clientLeft: { value: 0 },
      clientTop: { value: 0 },
    });
    wrapper.getBoundingClientRect = () => ({ left: 0, top: 0, width: 800, height: 600 }) as DOMRect;
    const canvas = {
      x: 0,
      y: 0,
      scale: 2,
      wrapperEl: wrapper,
      posFromDom: ({ x, y }: { x: number; y: number }) => ({ x: x / 2, y: y / 2 }),
      domFromPos: ({ x, y }: { x: number; y: number }) => ({ x: x * 2, y: y * 2 }),
      getData: () => ({
        nodes: [{ id: "a", type: "text", x: 10, y: 20, width: 100, height: 50 }],
        edges: [],
      }),
      setViewport: vi.fn(),
      requestFrame: vi.fn(),
    };
    const cleanup = mountCanvasCursorOverlay(host, aw, () => canvas);
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(host.querySelector(".realtime-canvas-presence-box.is-selection")).not.toBeNull();
    expect(host.querySelector(".realtime-canvas-presence-box.is-drag")).not.toBeNull();
    expect(host.textContent).toContain("Remote");
    expect(host.querySelector(".realtime-canvas-cursor")).not.toBeNull();
    const followButton = host.querySelector<HTMLButtonElement>(".realtime-canvas-cursor-label")!;
    expect(followButton.getAttribute("aria-label")).toContain("Follow Remote");
    followButton.click();
    expect(canvas.setViewport).toHaveBeenCalledWith({ x: 5, y: 6, scale: 2 });
    canvas.setViewport.mockClear();
    host.dispatchEvent(new PointerEvent("pointerdown", { bubbles: true }));
    states.get(remoteId).canvasPresence = {
      ...states.get(remoteId).canvasPresence,
      sequence: 3,
      viewport: { x: 50, y: 60, scale: 1 },
    };
    (aw as any).emit("change", [{ added: [], updated: [remoteId], removed: [] }, "remote"]);
    expect(canvas.setViewport).not.toHaveBeenCalled();
    cleanup();
    expect(aw.getLocalState()?.canvasPresence ?? null).toBeNull();
    expect(host.querySelector(".realtime-canvas-presence-layer")).toBeNull();
    aw.destroy();
    host.remove();
  });
});

describe("readCanvasViewport", () => {
  it("returns null for non-canvas objects", () => {
    expect(readCanvasViewport(null)).toBeNull();
    expect(readCanvasViewport({})).toBeNull();
    expect(readCanvasViewport({ scale: 1 })).toBeNull();
  });

  it("returns null when scale is missing, non-finite, or <= 0", () => {
    const base = makeCanvasLike();
    expect(readCanvasViewport({ ...base, scale: 0 })).toBeNull();
    expect(readCanvasViewport({ ...base, scale: NaN })).toBeNull();
    expect(readCanvasViewport({ ...base, scale: undefined })).toBeNull();
  });

  it("returns a viewport exposing posFromDom/domFromPos for a well-shaped canvas", () => {
    const c = makeCanvasLike();
    const vp = readCanvasViewport(c);
    expect(vp).not.toBeNull();
    // posFromDom: world = center.x + dom/scale
    expect(vp!.posFromDom({ x: 0, y: 0 })).toEqual({ x: 100, y: 50 });
    expect(vp!.posFromDom({ x: 50, y: 25 })).toEqual({ x: 150, y: 75 });
    // domFromPos is the inverse of posFromDom
    expect(vp!.domFromPos({ x: 150, y: 75 })).toEqual({ x: 50, y: 25 });
  });

  it("binds posFromDom/domFromPos so `this`-based methods work", () => {
    // Methods read `this.x/this.scale` like Obsidian's; without binding the
    // viewport wrappers would lose `this` and return NaN.
    const c = makeCanvasLikeThis();
    const vp = readCanvasViewport(c);
    expect(vp).not.toBeNull();
    expect(vp!.posFromDom({ x: 50, y: 25 })).toEqual({ x: 150, y: 75 });
    expect(vp!.domFromPos({ x: 150, y: 75 })).toEqual({ x: 50, y: 25 });
  });

  it("prefers the live wrapperEl rect over a stale cached canvasRect", () => {
    // Regression: mobile layout shifts leave Canvas.canvasRect stale, so the
    // wrapper-element center must win. The cached cx here is wildly wrong.
    const c = makeCanvasLikeWithWrapper({
      wrapperLeft: 10,
      wrapperTop: 20,
      wrapperWidth: 200,
      wrapperHeight: 100,
    });
    const vp = readCanvasViewport(c);
    expect(vp).not.toBeNull();
    // cx = 10 + clientLeft(0) + width/2 = 110; cy = 20 + 0 + 50 = 70
    expect(vp!.canvasRect.cx).toBe(110);
    expect(vp!.canvasRect.cy).toBe(70);
    expect(vp!.canvasRect.left).toBe(10);
    expect(vp!.canvasRect.top).toBe(20);
  });

  it("accounts for clientLeft/clientTop border when deriving the center", () => {
    const c = makeCanvasLikeWithWrapper({
      wrapperLeft: 5,
      wrapperTop: 8,
      wrapperWidth: 40,
      wrapperHeight: 20,
      clientLeft: 2,
      clientTop: 1,
    });
    const vp = readCanvasViewport(c);
    expect(vp).not.toBeNull();
    // left = 5 + 2 = 7; cx = 7 + 40/2 = 27; top = 8 + 1 = 9; cy = 9 + 20/2 = 19
    expect(vp!.canvasRect.left).toBe(7);
    expect(vp!.canvasRect.cx).toBe(27);
    expect(vp!.canvasRect.top).toBe(9);
    expect(vp!.canvasRect.cy).toBe(19);
  });
});

describe("Canvas viewport follow adapter", () => {
  it("applies viewport through a feature-detected setter", () => {
    const requestFrame = vi.fn();
    const setViewport = vi.fn();
    expect(writeCanvasViewport({ setViewport, requestFrame }, { x: 10, y: 20, scale: 1.5 })).toBe(
      true,
    );
    expect(setViewport).toHaveBeenCalledWith({ x: 10, y: 20, scale: 1.5 });
    expect(requestFrame).toHaveBeenCalledOnce();
  });

  it("falls back to x/y plus setScale and rejects unsupported shapes", () => {
    const canvas = { x: 0, y: 0, setScale: vi.fn() };
    expect(writeCanvasViewport(canvas, { x: 4, y: 5, scale: 2 })).toBe(true);
    expect(canvas).toMatchObject({ x: 4, y: 5 });
    expect(canvas.setScale).toHaveBeenCalledWith(2);
    expect(writeCanvasViewport({}, { x: 1, y: 2, scale: 1 })).toBe(false);
  });
});

function makeCanvasLike() {
  return {
    x: 100,
    y: 50,
    scale: 1,
    canvasRect: { cx: 400, cy: 300, left: 0, top: 0, width: 800, height: 600 },
    posFromDom: (p: { x: number; y: number }) => ({
      x: 100 + p.x / 1,
      y: 50 + p.y / 1,
    }),
    domFromPos: (p: { x: number; y: number }) => ({
      x: (p.x - 100) * 1,
      y: (p.y - 50) * 1,
    }),
  };
}

function makeCanvasLikeThis() {
  return {
    x: 100,
    y: 50,
    scale: 1,
    canvasRect: { cx: 400, cy: 300, left: 0, top: 0, width: 800, height: 600 },
    posFromDom: function (this: any, p: { x: number; y: number }) {
      return { x: this.x + p.x / this.scale, y: this.y + p.y / this.scale };
    },
    domFromPos: function (this: any, p: { x: number; y: number }) {
      return { x: (p.x - this.x) * this.scale, y: (p.y - this.y) * this.scale };
    },
  };
}

/**
 * Build a canvas-like object whose `wrapperEl` reports a live rect via a real
 * detached DOM element positioned with `left/top`/`width/height` styles. Lets
 * the live-rect path be exercised under jsdom.
 */
function makeCanvasLikeWithWrapper(opts: {
  x?: number;
  y?: number;
  scale?: number;
  wrapperLeft: number;
  wrapperTop: number;
  wrapperWidth: number;
  wrapperHeight: number;
  clientLeft?: number;
  clientTop?: number;
}) {
  const el = document.createElement("div");
  Object.defineProperty(el, "clientLeft", { value: opts.clientLeft ?? 0 });
  Object.defineProperty(el, "clientTop", { value: opts.clientTop ?? 0 });
  Object.defineProperty(el, "clientWidth", { value: opts.wrapperWidth });
  Object.defineProperty(el, "clientHeight", { value: opts.wrapperHeight });
  el.getBoundingClientRect = () =>
    ({
      left: opts.wrapperLeft,
      top: opts.wrapperTop,
      width: opts.wrapperWidth,
      height: opts.wrapperHeight,
      right: opts.wrapperLeft + opts.wrapperWidth,
      bottom: opts.wrapperTop + opts.wrapperHeight,
      x: 0,
      y: 0,
      toJSON: () => ({}),
    }) as DOMRect;
  const scale = opts.scale ?? 1;
  const x = opts.x ?? 100;
  const y = opts.y ?? 50;
  return {
    x,
    y,
    scale,
    wrapperEl: el,
    // A stale cached canvasRect that disagrees with the live wrapper; the live
    // path must win so we don't regress on mobile layout shifts.
    canvasRect: { cx: 40000, cy: 30000, left: 0, top: 0, width: 800, height: 600 },
    posFromDom: function (this: any, p: { x: number; y: number }) {
      return { x: this.x + p.x / this.scale, y: this.y + p.y / this.scale };
    },
    domFromPos: function (this: any, p: { x: number; y: number }) {
      return { x: (p.x - this.x) * this.scale, y: (p.y - this.y) * this.scale };
    },
  };
}
describe("PresenceAvatarStack", () => {
  it("renders nothing for zero entries", () => {
    const container = document.createElement("div");
    const root = createRoot(container);
    root.render(PresenceAvatarStack({ entries: [] }));
    // flushSync is not available in vitest jsdom; use act alternative
    container.remove();
    // The function returns null for zero entries
    const result = PresenceAvatarStack({ entries: [] });
    expect(result).toBeNull();
  });

  it("renders avatar wrappers with color CSS variable and img/fallback", () => {
    const entries: PresenceEntry[] = [
      {
        clientId: 1,
        name: "Alice",
        color: "#ff0000",
        avatarUrl: "https://example.com/a.png",
        isLocal: true,
      },
      {
        clientId: 2,
        name: "Bob",
        color: "#00ff00",
        avatarUrl: null,
        isLocal: false,
      },
    ];
    const container = document.createElement("div");
    document.body.appendChild(container);
    const root = createRoot(container);
    flushSync(() => {
      root.render(PresenceAvatarStack({ entries }));
    });
    container.querySelectorAll;
    const wraps = container.querySelectorAll(".realtime-presence-avatar-wrap");
    expect(wraps.length).toBe(2);
    const wrap0 = wraps[0] as HTMLElement;
    expect(wrap0.style.getPropertyValue("--realtime-presence-color")).toBe("#ff0000");
    expect(wrap0.querySelector("img")?.getAttribute("src")).toBe("https://example.com/a.png");
    const avatar = wrap0.querySelector(".realtime-presence-avatar") as HTMLImageElement;
    expect(avatar.style.borderRadius).toBe("50%");
    expect(avatar.style.clipPath).toBe("circle(50%)");
    const wrap1 = wraps[1] as HTMLElement;
    expect(wrap1.style.getPropertyValue("--realtime-presence-color")).toBe("#00ff00");
    expect(wrap1.querySelector(".realtime-presence-avatar-fallback")?.textContent).toBe("BO");
    root.unmount();
    container.remove();
  });
});
