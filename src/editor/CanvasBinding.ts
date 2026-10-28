import type RealtimePlugin from "../main";
import type { CanvasDocument } from "../CanvasDocument";
import type { StructuredCanvas } from "../structured/canvas";
import { diffCanvas, normalizeCanvas, type CanvasOperation } from "../structured/canvasOperations";
import {
  liveCanvasNodeGeometry,
  mountCanvasCursorOverlay,
  mountPresenceStack,
  readCanvasViewport,
  setCanvasPresence,
  type CanvasPresenceNode,
  type CanvasPresenceState,
} from "../presence";
import { CanvasTextCardBinding } from "./CanvasTextCardBinding";
import { CanvasViewAdapter } from "./CanvasViewAdapter";
import { dbg } from "../debug";
import type { EventRef } from "obsidian";

/**
 * Origin tag stamped on the Yjs transaction when we fold a local canvas edit
 * (captured via the patched `requestSave`) into the CRDT. The document's root
 * observer uses it to avoid re-importing our own edit back into the live canvas,
 * which would disrupt the in-progress selection/drag.
 */
export const CANVAS_LOCAL_ORIGIN = Symbol("realtime-canvas-request-save");
let loggedUnsupported = false;
/** Orders edits seen in views before startup, so the latest-edited view wins. */
let startupEditCounter = 0;

interface InternalCanvas {
  getData: () => unknown;
  importData: (data: unknown, clear?: boolean) => void;
  requestSave: (...args: unknown[]) => unknown;
  requestFrame?: () => void;
  selection?: Set<unknown> | Map<unknown, unknown>;
  nodes?: unknown;
  edges?: unknown;
  addNode?: (data: unknown) => unknown;
  removeNode?: (node: unknown) => unknown;
  addEdge?: (data: unknown) => unknown;
  removeEdge?: (edge: unknown) => unknown;
}

interface BoundCanvasView {
  canvas: InternalCanvas;
  host: HTMLElement;
  view: {
    file?: { path?: string } | null;
    getViewData?: () => unknown;
    setViewData?: (data: string, clear?: boolean) => void;
    /** Obsidian saves the view here before it loads another file or closes. */
    onUnloadFile?: (file: unknown) => Promise<void>;
  };
}

interface CanvasDiscovery {
  supported: BoundCanvasView[];
  unsupported: number;
}

/**
 * Owns one live binding per open view of a Canvas file. Disk write-through may
 * only be suppressed when every matching view exposes the private APIs needed
 * for bidirectional synchronization.
 */
export class CanvasBinding {
  private readonly views = new Map<InternalCanvas, CanvasViewBinding>();
  private readonly attachmentPaths = new Map<CanvasViewBinding, Set<string>>();
  private readonly presenceStates = new Map<CanvasViewBinding, CanvasPresenceState>();
  private prioritizedAttachments = new Set<string>();
  private activePresenceView: CanvasViewBinding | null = null;
  private unsupportedViews = 0;

  constructor(
    private readonly plugin: RealtimePlugin,
    private readonly doc: CanvasDocument,
  ) {}

  /** True only when every open view is patched and safe to use instead of disk folding. */
  isActive(): boolean {
    return this.views.size > 0 && this.unsupportedViews === 0;
  }

  tryBind(): void {
    this.reconcileViews(true);
  }

  unbindIfStale(): void {
    this.reconcileViews(false);
  }

  applyRemote(): void {
    for (const binding of this.views.values()) binding.applyRemote();
  }

  /**
   * Before the startup reconcile: the unsaved content of the open view edited
   * most recently, among views seen displaying this file, or null.
   */
  startupLiveValue(): StructuredCanvas | null {
    let latest: { value: StructuredCanvas; sequence: number } | null = null;
    for (const binding of this.views.values()) {
      const live = binding.startupLive();
      if (live && (!latest || live.sequence > latest.sequence)) latest = live;
    }
    return latest?.value ?? null;
  }

  /** Before startup: let each view recognize the file's (new) disk content. */
  checkStartupViews(): void {
    for (const binding of this.views.values()) binding.checkStartupAnchor();
  }

  scheduleRemote(): void {
    for (const binding of this.views.values()) binding.scheduleRemote();
  }

  destroy(): void {
    for (const binding of [...this.views.values()]) binding.destroy();
    this.views.clear();
    this.attachmentPaths.clear();
    this.presenceStates.clear();
    this.activePresenceView = null;
    this.updatePrioritizedAttachments(new Set());
    setCanvasPresence(this.doc.awareness, null);
  }

  private reconcileViews(createMissing: boolean): void {
    const discovery = findOpenCanvases(this.plugin, this.doc.path);
    this.unsupportedViews = discovery.unsupported;
    const live = new Map(discovery.supported.map((found) => [found.canvas, found]));

    for (const [canvas, binding] of [...this.views]) {
      if (!live.has(canvas) || binding.isStale()) {
        binding.destroy();
        this.views.delete(canvas);
      }
    }
    if (!createMissing) return;
    for (const found of discovery.supported) {
      if (this.views.has(found.canvas)) continue;
      try {
        const binding = new CanvasViewBinding(
          this.plugin,
          this.doc,
          found,
          (owner, paths) => this.onAttachmentPaths(owner, paths),
          (owner, state) => this.onPresence(owner, state),
          (owner) => this.removeView(owner),
        );
        this.views.set(found.canvas, binding);
      } catch (error) {
        this.unsupportedViews++;
        console.error(`[Realtime] failed to bind open canvas view for ${this.doc.path}`, error);
      }
    }
  }

  private removeView(binding: CanvasViewBinding): void {
    for (const [canvas, candidate] of this.views) {
      if (candidate !== binding) continue;
      binding.destroy();
      this.views.delete(canvas);
      return;
    }
    binding.destroy();
  }

  private onAttachmentPaths(binding: CanvasViewBinding, paths: Set<string>): void {
    if (paths.size) this.attachmentPaths.set(binding, paths);
    else this.attachmentPaths.delete(binding);
    const combined = new Set<string>();
    for (const values of this.attachmentPaths.values()) {
      for (const path of values) combined.add(path);
    }
    this.updatePrioritizedAttachments(combined);
  }

  private updatePrioritizedAttachments(next: Set<string>): void {
    const added = [...next].filter((path) => !this.prioritizedAttachments.has(path));
    const removed = [...this.prioritizedAttachments].filter((path) => !next.has(path));
    this.plugin.vaultSync?.unprioritizeCanvasAttachments(removed);
    this.plugin.vaultSync?.prioritizeCanvasAttachments(added);
    this.prioritizedAttachments = next;
  }

  private onPresence(binding: CanvasViewBinding, state: CanvasPresenceState | null): void {
    if (state) this.presenceStates.set(binding, state);
    else this.presenceStates.delete(binding);
    if (!state && this.activePresenceView === binding) this.activePresenceView = null;
    if (
      state &&
      (!this.activePresenceView || state.editingNodeId !== undefined || state.interaction)
    ) {
      this.activePresenceView = binding;
    }
    if (!this.activePresenceView || !this.presenceStates.has(this.activePresenceView)) {
      this.activePresenceView =
        [...this.presenceStates].find(
          ([, value]) => value.editingNodeId || value.interaction,
        )?.[0] ??
        this.presenceStates.keys().next().value ??
        null;
    }
    setCanvasPresence(
      this.doc.awareness,
      this.activePresenceView ? (this.presenceStates.get(this.activePresenceView) ?? null) : null,
    );
  }
}

class CanvasViewBinding {
  private plugin: RealtimePlugin;
  private doc: CanvasDocument;
  private canvas: InternalCanvas | null = null;
  private adapter: CanvasViewAdapter | null = null;
  /** The Obsidian view the canvas belongs to, kept to re-verify `file.path`. */
  private view: BoundCanvasView["view"] | null = null;
  private presenceCleanup: (() => void) | null = null;
  private originalRequestSave: InternalCanvas["requestSave"] | null = null;
  /** Our patch function, so unpatch can tell whether we're still the top patch. */
  private patchedRequestSave: InternalCanvas["requestSave"] | null = null;
  /**
   * Undoes our `onUnloadFile` patch while it is still the top patch; null when
   * the view has no unload hook, so gesture saves must be captured eagerly.
   */
  private restoreOnUnloadFile: (() => void) | null = null;
  private applyingRemote = false;
  /**
   * Normalized snapshot of the last data pushed into the view via
   * {@link applyRemote}. Obsidian's `requestSave` is debounced and may fire
   * *after* {@link applyingRemote} has been reset, so the flag alone can't
   * suppress that bounce-back. Comparing the captured data against this hash
   * ignores it precisely without muting a genuine concurrent local edit.
   */
  private lastImportedHash: string | null = null;
  /** Last normalized state accepted by this exact live view. */
  private shadow: ReturnType<typeof normalizeCanvas> | null = null;
  /**
   * Before startup: this view has been seen displaying this file's disk
   * content, so what it shows since is this file plus local edits, never
   * leftovers of a view Obsidian reused from another file.
   */
  private startupAnchored = false;
  /** Before startup: what the view showed when bound, matched once the disk is known. */
  private boundSnapshot: StructuredCanvas | null = null;
  /** Before startup: order of the latest edit seen in this view. */
  private startupEditSequence = 0;
  private interactionActive = false;
  private interactionTimer: ReturnType<typeof setInterval> | null = null;
  private remoteTimer: ReturnType<typeof setTimeout> | null = null;
  private remotePending = false;
  private lifecycleCleanup: (() => void) | null = null;
  private presenceSequence = 0;
  private presenceRaf: number | null = null;
  private textCardBinding: CanvasTextCardBinding | null = null;
  private editingNodeId: string | null = null;
  private attachmentIndicator: HTMLElement | null = null;
  private host: HTMLElement | null = null;
  private attachmentPaths = new Set<string>();
  private attachmentEventRefs: EventRef[] = [];

  constructor(
    plugin: RealtimePlugin,
    doc: CanvasDocument,
    found: BoundCanvasView,
    private readonly updateAttachmentPaths: (
      binding: CanvasViewBinding,
      paths: Set<string>,
    ) => void,
    private readonly updatePresence: (
      binding: CanvasViewBinding,
      state: CanvasPresenceState | null,
    ) => void,
    private readonly invalidate: (binding: CanvasViewBinding) => void,
  ) {
    this.plugin = plugin;
    this.doc = doc;
    const { canvas, host, view } = found;
    this.canvas = canvas;
    this.adapter = new CanvasViewAdapter(canvas, typeof view.setViewData !== "function");
    this.view = view;
    this.host = host;
    const vault = this.plugin.app.vault;
    if (vault && typeof vault.on === "function") {
      const refreshAttachments = () => this.renderAttachmentIndicator();
      this.attachmentEventRefs = [
        vault.on("create", refreshAttachments),
        vault.on("delete", refreshAttachments),
        vault.on("rename", refreshAttachments),
      ];
    }
    // Capture the original in the closure (not via the mutable field): a stale
    // copy of this patch can linger on a canvas instance that Obsidian reuses
    // for another file, and it must keep delegating to the save function it
    // wrapped even after this binding rebinds elsewhere.
    const original = canvas.requestSave.bind(canvas);
    this.originalRequestSave = original;
    this.patchedRequestSave = (...args: unknown[]) => {
      const result = original(...args);
      // Obsidian requests a save on every pointer move of a gesture. Capturing
      // each one serializes the whole board per frame; instead the bounded
      // interaction sampler and the release capture fold the gesture in. That
      // is only safe when the unload hook below can flush the gesture before
      // the view switches files; otherwise keep capturing every save. Before
      // startup a capture only orders this view's edit, so keep it.
      if (
        !this.applyingRemote &&
        (!this.interactionActive || !this.restoreOnUnloadFile || !this.doc.isReady())
      ) {
        this.captureLocal();
      }
      return result;
    };
    canvas.requestSave = this.patchedRequestSave;
    // Obsidian unloads the current file (with `view.file` still set to it)
    // before reusing the view for another file or closing it, and saves the
    // view there. Fold the in-progress gesture first, while the view still
    // shows this document. Like requestSave above, the closure delegates to
    // what it wrapped even if a lingering copy outlives this binding.
    const originalUnload = view.onUnloadFile;
    if (typeof originalUnload === "function") {
      const ownUnload = Object.prototype.hasOwnProperty.call(view, "onUnloadFile");
      const patchedUnload = (file: unknown) => {
        if (this.interactionActive && this.canvas && !this.isStale()) this.stopInteraction(true);
        return originalUnload.call(view, file);
      };
      view.onUnloadFile = patchedUnload;
      this.restoreOnUnloadFile = () => {
        if (view.onUnloadFile !== patchedUnload) return;
        if (ownUnload) view.onUnloadFile = originalUnload;
        else delete view.onUnloadFile;
      };
    }
    // Mount presence avatar stack + canvas cursor overlay. Both clean up via
    // presenceCleanup, which is called in unpatch().
    const stackCleanup = mountPresenceStack(
      host,
      this.doc.awareness,
      "canvas",
      "realtime-canvas-presence-stack",
    );
    const cursorCleanup = mountCanvasCursorOverlay(host, this.doc.awareness, () => this.canvas);
    this.presenceCleanup = () => {
      cursorCleanup();
      stackCleanup();
    };
    this.bindInteractionLifecycle(host);
    this.publishPresence(false);
    this.textCardBinding = new CanvasTextCardBinding(
      this.doc,
      () => this.canvas,
      (nodeId) => {
        this.editingNodeId = nodeId;
        this.publishPresence(this.interactionActive);
        if (nodeId) this.scheduleRemote();
      },
      host,
    );
    this.textCardBinding.start();
    if (!this.doc.isReady()) {
      this.boundSnapshot = this.readView();
      this.checkStartupAnchor();
    }
    this.applyRemote();
  }

  /**
   * Before startup, recognize that the view displays this file: it shows (or
   * showed when bound) exactly the disk content the startup reconcile merges.
   */
  checkStartupAnchor(): void {
    if (this.startupAnchored || !this.canvas || this.doc.isReady() || this.isStale()) return;
    const current = this.readView();
    if (
      (current !== null && this.doc.matchesStartupDisk(current)) ||
      (this.boundSnapshot !== null && this.doc.matchesStartupDisk(this.boundSnapshot))
    ) {
      this.startupAnchored = true;
      this.boundSnapshot = null;
    }
  }

  /** Before startup: this view's current content, if it displays this file. */
  startupLive(): { value: StructuredCanvas; sequence: number } | null {
    if (!this.canvas || this.isStale()) return null;
    this.checkStartupAnchor();
    const value = this.startupAnchored ? this.readView() : null;
    return value && { value, sequence: this.startupEditSequence };
  }

  /** The view's normalized content; null when the view cannot provide it. */
  private readView(): StructuredCanvas | null {
    try {
      return this.canvas ? normalizeCanvas(this.canvas.getData()) : null;
    } catch (error) {
      console.error(`[Realtime] failed to read canvas view for ${this.doc.path}`, error);
      return null;
    }
  }

  /**
   * True when the bound view no longer displays this document's file.
   * Obsidian reuses the same view — and the same Canvas instance — when a
   * leaf switches to a different canvas file, so the binding must re-verify
   * the path at every capture/import. Acting through a reused view would
   * read the *other* file's content into this CRDT, or write this CRDT's
   * content into the other file on disk.
   */
  isStale(): boolean {
    return this.view?.file?.path !== this.doc.path;
  }

  /** Push the CRDT's current value into the live view and converge disk. */
  applyRemote(): void {
    if (!this.canvas) return;
    // The leaf may have switched this reused view to a different file since we
    // bound; importing here would overwrite that file with this doc's content.
    if (this.isStale()) {
      this.invalidate(this);
      return;
    }
    // Do not import the constructor-time empty Y.Doc into an already-open
    // canvas. The real value arrives after IndexedDB/server startup reconcile.
    if (!this.doc.isReady()) return;
    this.boundSnapshot = null;
    if (this.textCardBinding?.hasUnboundActiveEditor()) {
      this.remotePending = true;
      return;
    }
    const next = this.doc.canvasData();
    this.prioritizeAttachments(normalizeCanvas(next));
    const nextHash = hashCanvasData(next);
    // Skip a pointless re-render (and the scroll/selection churn it brings)
    // when the live view already shows this content. Still stamp the hash so
    // a delayed requestSave triggered by the *previous* import is ignored.
    if (hashCanvasData(this.canvas.getData()) === nextHash) {
      this.lastImportedHash = nextHash;
      this.shadow = normalizeCanvas(this.canvas.getData());
      return;
    }
    this.applyingRemote = true;
    this.lastImportedHash = nextHash;
    try {
      if (this.shadow && this.adapter) {
        const operations = incrementalOperations(this.shadow, normalizeCanvas(next));
        if (operations) {
          const result = this.adapter.apply(operations, this.shadow);
          if (result.applied) {
            dbg("canvas incremental apply", this.doc.path, operations.length);
            this.shadow = normalizeCanvas(next);
            this.originalRequestSave?.();
            return;
          }
          dbg("canvas incremental fallback", this.doc.path, result.reason);
          logAdapterFallback(result.reason ?? "unsupported", result.error);
        } else logAdapterFallback("ordering");
      }
      // Pass clear=true so importData REMOVES nodes/edges absent from the
      // CRDT snapshot, not just adds/updates. Without it, deleted edges never
      // leave the live view — the original "deleted edges not propagating" bug.
      this.importSnapshot(next);
      this.shadow = normalizeCanvas(next);
      this.canvas.requestFrame?.();
      // Converge disk too: while the view is open the StructuredDocument
      // write-through is suppressed in favor of this binding, so a remote
      // change that we just pushed into the view must also be persisted —
      // otherwise the on-disk file stays stale and a later close/reopen or
      // onDiskChanged fold can resurrect deleted edges.
      this.originalRequestSave?.();
    } catch (e) {
      console.error(`[Realtime] failed to apply canvas update for ${this.doc.path}`, e);
    } finally {
      this.applyingRemote = false;
    }
  }

  private importSnapshot(data: unknown): void {
    if (typeof this.view?.setViewData === "function") {
      this.view.setViewData(JSON.stringify(data), true);
    } else {
      this.canvas?.importData(data, true);
    }
  }

  /** Coalesce observer bursts and defer disruptive imports during local gestures. */
  scheduleRemote(): void {
    if (!this.canvas) return;
    this.remotePending = true;
    if (this.interactionActive || this.remoteTimer) {
      dbg("canvas remote apply deferred", this.doc.path);
      return;
    }
    this.remoteTimer = setTimeout(() => {
      this.remoteTimer = null;
      if (!this.remotePending || this.interactionActive) return;
      this.remotePending = false;
      this.applyRemote();
    }, 16);
  }

  destroy(): void {
    this.unpatch();
  }

  /** Restore the canvas's original `requestSave` and forget the binding. */
  private unpatch(): void {
    // Saves requested during a gesture are folded in by the sampler, so a
    // binding torn down mid-gesture must still capture the latest state. A
    // view now showing another file holds none of this document's edits.
    this.stopInteraction(!this.isStale());
    if (this.remoteTimer) clearTimeout(this.remoteTimer);
    this.remoteTimer = null;
    this.remotePending = false;
    this.lifecycleCleanup?.();
    this.lifecycleCleanup = null;
    this.textCardBinding?.destroy();
    this.textCardBinding = null;
    this.editingNodeId = null;
    this.attachmentIndicator?.remove();
    this.attachmentIndicator = null;
    this.host = null;
    this.attachmentPaths.clear();
    this.updateAttachmentPaths(this, new Set());
    for (const eventRef of this.attachmentEventRefs) this.plugin.app.vault?.offref(eventRef);
    this.attachmentEventRefs = [];
    dbg("canvas presence cleanup", this.doc.path);
    this.presenceCleanup?.();
    this.presenceCleanup = null;
    this.updatePresence(this, null);
    // Only restore when we're still the top patch. If another binding patched
    // over us (the canvas instance is shared across files in a leaf), leave
    // the chain intact — its closure delegates to the save it captured, and
    // our capture path is disabled by nulling `canvas`/`view` below.
    if (this.canvas && this.originalRequestSave) {
      if (this.canvas.requestSave === this.patchedRequestSave) {
        this.canvas.requestSave = this.originalRequestSave;
      }
    }
    this.restoreOnUnloadFile?.();
    this.restoreOnUnloadFile = null;
    this.canvas = null;
    this.adapter = null;
    this.view = null;
    this.originalRequestSave = null;
    this.patchedRequestSave = null;
    this.lastImportedHash = null;
    this.shadow = null;
    this.startupAnchored = false;
    this.boundSnapshot = null;
  }

  private prioritizeAttachments(canvas: StructuredCanvas): void {
    const referencedPaths = canvasAttachmentPaths(canvas);
    const paths = this.plugin.vaultSync?.canvasBinaryPaths(referencedPaths) ?? [];
    const next = new Set(paths);
    this.attachmentPaths = next;
    this.updateAttachmentPaths(this, next);
    this.renderAttachmentIndicator();
  }

  private renderAttachmentIndicator(): void {
    const missing = [...this.attachmentPaths].filter(
      (path) => !this.plugin.app.vault?.getAbstractFileByPath(path),
    );
    if (!missing.length) {
      this.attachmentIndicator?.remove();
      this.attachmentIndicator = null;
      return;
    }
    const host = this.host;
    if (!this.attachmentIndicator && host) {
      const indicator = document.createElement("div");
      indicator.className = "realtime-canvas-attachment-status";
      Object.assign(indicator.style, {
        position: "absolute",
        left: "12px",
        bottom: "12px",
        zIndex: "50",
        padding: "4px 8px",
        borderRadius: "4px",
        background: "var(--background-secondary)",
        color: "var(--text-muted)",
        fontSize: "12px",
        pointerEvents: "none",
      });
      host.append(indicator);
      this.attachmentIndicator = indicator;
    }
    if (this.attachmentIndicator) {
      this.attachmentIndicator.textContent = `Loading ${missing.length} Canvas attachment${missing.length === 1 ? "" : "s"}…`;
    }
  }

  private bindInteractionLifecycle(host: HTMLElement): void {
    const start = () => this.startInteraction();
    const finish = () => this.stopInteraction(true);
    const move = () => {
      if (!this.interactionActive || this.presenceRaf !== null) return;
      this.presenceRaf = requestAnimationFrame(() => {
        this.presenceRaf = null;
        this.publishPresence(true);
      });
    };
    const onVisibility = () => {
      if (document.hidden) finish();
    };
    host.addEventListener("pointerdown", start, true);
    host.addEventListener("pointermove", move, true);
    window.addEventListener("pointerup", finish, true);
    window.addEventListener("pointercancel", finish, true);
    window.addEventListener("blur", finish);
    document.addEventListener("visibilitychange", onVisibility);
    this.lifecycleCleanup = () => {
      host.removeEventListener("pointerdown", start, true);
      host.removeEventListener("pointermove", move, true);
      window.removeEventListener("pointerup", finish, true);
      window.removeEventListener("pointercancel", finish, true);
      window.removeEventListener("blur", finish);
      document.removeEventListener("visibilitychange", onVisibility);
      if (this.presenceRaf !== null) cancelAnimationFrame(this.presenceRaf);
      this.presenceRaf = null;
    };
  }

  private startInteraction(): void {
    if (this.interactionActive || !this.canvas || this.isStale()) return;
    // A gesture starts from what the view shows now: before startup, that
    // may be the file's disk content, recognizing the view before it changes.
    this.checkStartupAnchor();
    this.interactionActive = true;
    this.publishPresence(true);
    // Sampling is durable but bounded. Awareness handles smooth pointer motion.
    this.interactionTimer = setInterval(() => this.captureLocal(), 75);
  }

  private stopInteraction(captureFinal: boolean): void {
    if (this.interactionTimer) clearInterval(this.interactionTimer);
    this.interactionTimer = null;
    const wasActive = this.interactionActive;
    this.interactionActive = false;
    if (captureFinal && wasActive) this.captureLocal();
    if (wasActive) this.publishPresence(false);
    if (this.remotePending) {
      this.remotePending = false;
      if (this.remoteTimer) clearTimeout(this.remoteTimer);
      this.remoteTimer = null;
      this.applyRemote();
    }
  }

  private publishPresence(interacting: boolean): void {
    const canvas = this.canvas;
    if (!canvas) {
      this.updatePresence(this, null);
      return;
    }
    const selection = selectedNodeGeometry(canvas);
    const selectedNodeIds = selection.map((node) => node.id);
    const interactionNodes = selection.flatMap((node) => (node.geometry ? [node.geometry] : []));
    const viewport = readCanvasViewport(canvas);
    this.updatePresence(this, {
      version: 1,
      sequence: ++this.presenceSequence,
      ...(selectedNodeIds.length ? { selectedNodeIds } : {}),
      ...(interacting && interactionNodes.length
        ? { interaction: { kind: "drag" as const, nodes: interactionNodes } }
        : {}),
      ...(this.editingNodeId ? { editingNodeId: this.editingNodeId } : {}),
      ...(viewport ? { viewport: { x: viewport.x, y: viewport.y, scale: viewport.scale } } : {}),
    });
  }

  private captureLocal(): void {
    try {
      if (!this.canvas) return;
      // The reused view may now show a different file: its data belongs to
      // that file, not this document. Folding it in would overwrite this
      // canvas everywhere with the other canvas's content.
      if (this.isStale()) {
        this.invalidate(this);
        return;
      }
      // Before startup reconcile the document holds only its stored state, and
      // the snapshot may be stale data of a reused view. Fold nothing: the
      // startup merge reads this view (startupLive) once it has been seen
      // showing this file, and saved edits reach it through the disk.
      if (!this.doc.isReady()) {
        this.checkStartupAnchor();
        if (this.startupAnchored) this.startupEditSequence = ++startupEditCounter;
        return;
      }
      const data = this.canvas.getData();
      // Normalize once: attachment discovery, the bounce-back check, and the
      // operation diff all read this snapshot.
      const next = normalizeCanvas(data);
      this.prioritizeAttachments(next);
      if (!data) return;
      // Ignore the bounce-back from our own importData(): if the view still
      // shows exactly what we just pushed, it's not a local edit. Obsidian's
      // requestSave is debounced and may fire after applyingRemote has been
      // reset, so the flag alone is insufficient.
      if (this.lastImportedHash !== null) {
        if (hashStructuredCanvas(next) === this.lastImportedHash) return;
        this.lastImportedHash = null;
      }
      if (this.shadow && typeof this.doc.applyCanvasOperations === "function") {
        const operations: CanvasOperation[] = diffCanvas(this.shadow, next);
        if (operations.length) {
          dbg("canvas local operation batch", this.doc.path, operations.length);
          this.doc.applyCanvasOperations(operations, CANVAS_LOCAL_ORIGIN);
        }
      } else {
        this.doc.reconcileFromCanvasData(data, CANVAS_LOCAL_ORIGIN);
      }
      this.shadow = next;
    } catch (e) {
      console.error(`[Realtime] failed to capture canvas update for ${this.doc.path}`, e);
    }
  }
}

function findOpenCanvases(plugin: RealtimePlugin, path: string): CanvasDiscovery {
  const supported: BoundCanvasView[] = [];
  const seenViews = new Set<unknown>();
  const seenCanvases = new Set<InternalCanvas>();
  let unsupported = 0;
  const inspect = (leaf: any) => {
    const view = leaf?.view;
    if (seenViews.has(view) || view?.getViewType?.() !== "canvas" || view?.file?.path !== path) {
      return;
    }
    seenViews.add(view);
    const canvas = view.canvas;
    if (isInternalCanvas(canvas)) {
      if (!seenCanvases.has(canvas)) {
        seenCanvases.add(canvas);
        supported.push({ canvas, host: view.containerEl, view });
      }
      return;
    }
    unsupported++;
    if (!loggedUnsupported) {
      loggedUnsupported = true;
      console.warn(
        "[Realtime] Obsidian canvas private API shape is unsupported; using disk write-through fallback.",
      );
    }
  };

  const workspace = plugin.app.workspace as any;
  workspace?.iterateAllLeaves?.(inspect);
  for (const leaf of workspace?.getLeavesOfType?.("canvas") ?? []) inspect(leaf);
  return { supported, unsupported };
}

function isInternalCanvas(value: unknown): value is InternalCanvas {
  const canvas = value as Partial<InternalCanvas> | null;
  // importData is required: without it applyRemote() can't push CRDT changes
  // into the view, and shouldDeferToLiveBinding() (which gates disk
  // write-through) must not suppress disk writes when remote apply is
  // impossible. Requiring it here means isActive() implies "can both capture
  // local edits AND apply remote updates" — the only configuration worth
  // patching for. If importData is missing we fall back to disk write-through
  // entirely, matching the logged warning.
  return (
    !!canvas &&
    typeof canvas.getData === "function" &&
    typeof canvas.requestSave === "function" &&
    typeof canvas.importData === "function"
  );
}

function selectedIds(canvas: InternalCanvas): string[] {
  const selection = canvas.selection;
  if (!(selection instanceof Set) && !(selection instanceof Map)) return [];
  const values = selection instanceof Map ? selection.keys() : selection.values();
  const ids: string[] = [];
  for (const value of values) {
    if (typeof value === "string") ids.push(value);
    else if (
      value &&
      typeof value === "object" &&
      typeof (value as { id?: unknown }).id === "string"
    )
      ids.push((value as { id: string }).id);
  }
  return [...new Set(ids)].sort();
}

/**
 * Selected node ids (sorted, edges excluded) with their geometry when it is a
 * numeric box. Reads Obsidian's live node map so presence publishing does not
 * serialize the whole Canvas per frame; falls back to one getData() snapshot
 * when the live map cannot answer for some selected id (older/private shapes).
 */
function selectedNodeGeometry(
  canvas: InternalCanvas,
): Array<{ id: string; geometry: CanvasPresenceNode | null }> {
  const ids = selectedIds(canvas);
  if (!ids.length) return [];
  const live: Array<{ id: string; geometry: CanvasPresenceNode }> = [];
  for (const id of ids) {
    const geometry = liveCanvasNodeGeometry(canvas, id);
    if (geometry === undefined) {
      const data = normalizeCanvas(canvas.getData());
      return ids.flatMap<{ id: string; geometry: CanvasPresenceNode | null }>((selectedId) => {
        if (!(selectedId in data.nodes)) return [];
        const node = data.nodes[selectedId];
        if (!node || typeof node !== "object" || Array.isArray(node))
          return [{ id: selectedId, geometry: null }];
        const { x, y, width, height } = node;
        if (
          typeof x !== "number" ||
          typeof y !== "number" ||
          typeof width !== "number" ||
          typeof height !== "number"
        )
          return [{ id: selectedId, geometry: null }];
        return [{ id: selectedId, geometry: { id: selectedId, x, y, width, height } }];
      });
    }
    if (geometry) live.push({ id, geometry });
  }
  return live;
}

const loggedAdapterFallbacks = new Set<string>();

function logAdapterFallback(reason: string, error?: unknown): void {
  const shape = error instanceof Error ? `${reason}:${error.name}:${error.message}` : reason;
  if (loggedAdapterFallbacks.has(shape)) return;
  loggedAdapterFallbacks.add(shape);
  if (error)
    console.warn("[Realtime] Canvas incremental API failed; using full import fallback.", error);
  else
    console.warn(
      "[Realtime] Canvas incremental API shape is unsupported; using full import fallback.",
    );
}

/**
 * Ordering calls are not stable across Obsidian versions. Adds append and
 * deletes retain relative order, so only discard order operations when those
 * ordinary mutations imply the exact requested order.
 */
function incrementalOperations(
  previous: StructuredCanvas,
  next: StructuredCanvas,
): CanvasOperation[] | null {
  const operations = diffCanvas(previous, next);
  const safeNodeOrder = impliedOrder(previous.nodeOrder, next.nodeOrder);
  const safeEdgeOrder = impliedOrder(previous.edgeOrder, next.edgeOrder);
  if (!safeNodeOrder || !safeEdgeOrder) return null;
  return operations.filter(
    (operation) => operation.type !== "node-order" && operation.type !== "edge-order",
  );
}

function impliedOrder(previous: readonly string[], next: readonly string[]): boolean {
  const nextIds = new Set(next);
  const previousIds = new Set(previous);
  const implied = previous.filter((id) => nextIds.has(id));
  for (const id of next) if (!previousIds.has(id)) implied.push(id);
  return implied.length === next.length && implied.every((id, index) => id === next[index]);
}

export function canvasAttachmentPaths(canvas: StructuredCanvas): string[] {
  const paths = new Set<string>();
  for (const value of Object.values(canvas.nodes)) {
    if (
      value &&
      typeof value === "object" &&
      !Array.isArray(value) &&
      value.type === "file" &&
      typeof value.file === "string" &&
      value.file
    ) {
      paths.add(value.file);
    }
  }
  return [...paths].sort();
}

/**
 * Normalize canvas data through the parser so field reordering or default
 * additions by Obsidian's view don't defeat the equality check. Both
 * {@link applyRemote} and {@link captureLocal} hash through
 * {@link hashStructuredCanvas} so the comparison is stable regardless of which
 * side produced the snapshot.
 */
function hashCanvasData(data: unknown): string {
  try {
    return hashStructuredCanvas(normalizeCanvas(data));
  } catch {
    return "";
  }
}

/** Equality fingerprint of an already normalized canvas snapshot. */
function hashStructuredCanvas(canvas: StructuredCanvas): string {
  return JSON.stringify({
    n: Object.keys(canvas.nodes).sort(),
    e: Object.keys(canvas.edges).sort(),
    // Include field-level content so a real local edit (e.g. a node move)
    // is NOT mistaken for the bounce-back even when the id sets match.
    nodes: canvas.nodes,
    edges: canvas.edges,
  });
}
