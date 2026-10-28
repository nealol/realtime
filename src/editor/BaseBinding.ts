import type RealtimePlugin from "../main";
import type { BaseDocument } from "../BaseDocument";
import { parseBase } from "../structured/base";
import { jsonEqual, type JsonValue } from "../structured/reconcile";

/**
 * Origin tag stamped on the Yjs transaction when we fold a local base edit
 * (captured via the patched `requestSave`) into the CRDT. The document's root
 * observer uses it to avoid re-pushing our own edit back into the live view.
 */
export const BASE_LOCAL_ORIGIN = Symbol("realtime-base-request-save");
let loggedUnsupported = false;

/**
 * The Bases file view is a {@link https://docs.obsidian.md/Reference/TypeScript+API/TextFileView TextFileView}
 * subclass, so it exposes the same load/save shape every text-backed view does:
 *
 *  - `getViewData()` serializes the live view state back to `.base` YAML;
 *  - `setViewData(data, clear)` loads YAML into the view and re-renders in place;
 *  - `requestSave()` is the debounced choke point through which every local edit
 *    is persisted.
 *
 * We mirror {@link CanvasBinding} exactly, just against this standard API rather
 * than the canvas-specific private one.
 */
interface InternalBaseView {
  file?: { path?: string } | null;
  getViewType?: () => string;
  getViewData: () => string;
  setViewData: (data: string, clear: boolean) => void;
  requestSave: () => void;
}

export class BaseBinding {
  private plugin: RealtimePlugin;
  private doc: BaseDocument;
  private view: InternalBaseView | null = null;
  private originalRequestSave: InternalBaseView["requestSave"] | null = null;
  /** Our patch function, so unpatch can tell whether we're still the top patch. */
  private patchedRequestSave: InternalBaseView["requestSave"] | null = null;
  private applyingRemote = false;
  /**
   * Before startup: the view has been seen displaying this file's disk
   * content, so what it shows since is this file plus local edits, never
   * leftovers of a view Obsidian reused from another file.
   */
  private startupAnchored = false;
  /** Before startup: what the view showed when bound, matched once the disk is known. */
  private boundSnapshot: JsonValue | null = null;

  constructor(plugin: RealtimePlugin, doc: BaseDocument) {
    this.plugin = plugin;
    this.doc = doc;
  }

  /** True only while we're patched onto a live, recognized Bases view. */
  isActive(): boolean {
    return this.view !== null;
  }

  tryBind(): void {
    const view = this.findOpenBase();
    // Same live view we are already patched onto — nothing to do.
    if (view && view === this.view) return;
    // The leaf was closed (view == null) or reopened as a fresh instance:
    // drop the stale patch before binding the new one, so live editing
    // survives close/reopen instead of silently dying.
    if (this.view && view !== this.view) this.unpatch();
    if (!view) return;
    this.view = view;
    // Capture the original in the closure (not via the mutable field): a stale
    // copy of this patch can linger on a view instance that Obsidian reuses
    // for another file, and it must keep delegating to the save function it
    // wrapped even after this binding rebinds elsewhere.
    const original = view.requestSave.bind(view);
    this.originalRequestSave = original;
    this.patchedRequestSave = () => {
      const result = original();
      if (!this.applyingRemote) this.captureLocal();
      return result;
    };
    view.requestSave = this.patchedRequestSave;
    if (!this.doc.isReady()) {
      this.boundSnapshot = this.readView();
      this.checkStartupAnchor();
    }
    this.applyRemote();
  }

  /**
   * True when the bound view no longer displays this document's file.
   * Obsidian reuses the same TextFileView when a leaf switches to another
   * `.base` file, so the binding must re-verify the path at every
   * capture/apply. Acting through a reused view would read the *other*
   * file's YAML into this CRDT, or write this CRDT's YAML into the other
   * file on disk.
   */
  private isStaleView(): boolean {
    return this.view?.file?.path !== this.doc.path;
  }

  /** Unpatch if the bound view is gone or now shows another file. */
  unbindIfStale(): void {
    if (!this.view) return;
    const found = this.findOpenBase();
    if (!found || found !== this.view || this.isStaleView()) this.unpatch();
  }

  /**
   * Push the CRDT's current value into the live view and persist it. Before
   * startup the CRDT holds only stored (possibly empty) state, so nothing is
   * pushed; the document calls this once its startup reconcile completes.
   */
  applyRemote(): void {
    if (!this.view) return;
    // The leaf may have switched this reused view to a different file since we
    // bound; pushing here would overwrite that file with this doc's content.
    if (this.isStaleView()) {
      this.unpatch();
      return;
    }
    if (!this.doc.isReady()) return;
    this.boundSnapshot = null;
    const next = this.doc.baseData();
    // Avoid a pointless re-render (and the scroll/selection churn it brings)
    // when the view already shows this content.
    if (sameBase(this.view.getViewData(), next)) return;
    this.applyingRemote = true;
    try {
      this.view.setViewData(next, false);
      // Converge disk too: while the view is open the StructuredDocument
      // write-through is suppressed in favor of this binding.
      this.originalRequestSave?.();
    } catch (e) {
      console.error(`[Realtime] failed to apply base update for ${this.doc.path}`, e);
    } finally {
      this.applyingRemote = false;
    }
  }

  /**
   * Before startup, recognize that the view displays this file: it shows (or
   * showed when bound) exactly the disk content the startup reconcile merges.
   */
  checkStartupAnchor(): void {
    if (this.startupAnchored || !this.view || this.doc.isReady() || this.isStaleView()) return;
    const current = this.readView();
    if (
      (current !== null && this.doc.matchesStartupDisk(current)) ||
      (this.boundSnapshot !== null && this.doc.matchesStartupDisk(this.boundSnapshot))
    ) {
      this.startupAnchored = true;
      this.boundSnapshot = null;
    }
  }

  /** Before startup: the view's current content, if it displays this file. */
  startupLiveValue(): JsonValue | null {
    if (!this.view || this.isStaleView()) return null;
    this.checkStartupAnchor();
    return this.startupAnchored ? this.readView() : null;
  }

  destroy(): void {
    this.unpatch();
  }

  private readView(): JsonValue | null {
    try {
      const data = this.view?.getViewData();
      return typeof data === "string" ? parseBase(data) : null;
    } catch {
      return null;
    }
  }

  /** Restore the view's original `requestSave` and forget the binding. */
  private unpatch(): void {
    // Only restore when we're still the top patch. If another binding patched
    // over us (the view instance is shared across files in a leaf), leave the
    // chain intact — its closure delegates to the save it captured, and our
    // capture path is disabled by nulling `view` below.
    if (this.view && this.originalRequestSave) {
      if (this.view.requestSave === this.patchedRequestSave) {
        this.view.requestSave = this.originalRequestSave;
      }
    }
    this.view = null;
    this.originalRequestSave = null;
    this.patchedRequestSave = null;
    this.startupAnchored = false;
    this.boundSnapshot = null;
  }

  private captureLocal(): void {
    try {
      // The reused view may now show a different file: its YAML belongs to
      // that file, not this document. Folding it in would overwrite this
      // base everywhere with the other base's content.
      if (this.view && this.isStaleView()) {
        this.unpatch();
        return;
      }
      // Before startup reconcile the document holds only its stored state, and
      // the YAML may be stale data of a reused view. Fold nothing: the startup
      // merge reads this view (startupLiveValue) once it has been seen showing
      // this file, and saved edits reach it through the disk.
      if (!this.doc.isReady()) {
        this.checkStartupAnchor();
        return;
      }
      const data = this.view?.getViewData();
      if (typeof data === "string") this.doc.reconcileFromBaseText(data, BASE_LOCAL_ORIGIN);
    } catch (e) {
      console.error(`[Realtime] failed to capture base update for ${this.doc.path}`, e);
    }
  }

  private findOpenBase(): InternalBaseView | null {
    let found: InternalBaseView | null = null;
    const inspect = (leaf: any) => {
      if (found) return;
      const view = leaf?.view;
      if (view?.file?.path !== this.doc.path) return;
      if (view?.getViewType?.() !== "bases") return;
      if (isInternalBaseView(view)) found = view;
      else if (!loggedUnsupported) {
        loggedUnsupported = true;
        console.warn(
          "[Realtime] Obsidian Bases view API shape is unsupported; using disk write-through fallback.",
        );
      }
    };

    const workspace = this.plugin.app.workspace as any;
    workspace?.iterateAllLeaves?.(inspect);
    for (const leaf of workspace?.getLeavesOfType?.("bases") ?? []) inspect(leaf);
    return found;
  }
}

function isInternalBaseView(value: unknown): value is InternalBaseView {
  const view = value as Partial<InternalBaseView> | null;
  return (
    !!view &&
    typeof view.getViewData === "function" &&
    typeof view.setViewData === "function" &&
    typeof view.requestSave === "function"
  );
}

/** Whether two `.base` texts hold the same value, ignoring YAML formatting. */
function sameBase(left: string, right: string): boolean {
  if (left === right) return true;
  try {
    return jsonEqual(parseBase(left), parseBase(right));
  } catch {
    return false;
  }
}
