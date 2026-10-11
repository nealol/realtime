import type { CanvasDocument } from "../CanvasDocument";
import { shouldPushEditorToShared } from "./LiveEdit";
import { discoverCanvasTextEditor } from "./CanvasTextEditorAdapter";
import { mountCanvasRemoteTextCursors } from "./CanvasRemoteTextCursors";
import { YTextEditorBinding } from "./YTextEditorBinding";

export class CanvasTextCardBinding {
  private timer: ReturnType<typeof setInterval> | null = null;
  private binding: YTextEditorBinding | null = null;
  private source: object | null = null;
  private nodeId: string | null = null;
  private cursorCleanup: (() => void) | null = null;
  private lifecycleCleanup: (() => void) | null = null;
  private activeEditorDetected = false;

  constructor(
    private readonly doc: CanvasDocument,
    private readonly getCanvas: () => unknown,
    private readonly onActiveNode: (nodeId: string | null) => void,
    private readonly host?: HTMLElement,
  ) {}

  start(): void {
    if (this.timer) return;
    if (this.host) {
      const refresh = () => this.refresh();
      this.host.addEventListener("focusin", refresh, true);
      this.host.addEventListener("keydown", refresh, true);
      this.host.addEventListener("beforeinput", refresh, true);
      const observer = new MutationObserver(refresh);
      observer.observe(this.host, { childList: true, subtree: true });
      this.lifecycleCleanup = () => {
        this.host?.removeEventListener("focusin", refresh, true);
        this.host?.removeEventListener("keydown", refresh, true);
        this.host?.removeEventListener("beforeinput", refresh, true);
        observer.disconnect();
      };
    }
    this.refresh();
    this.timer = setInterval(() => this.refresh(), 200);
  }

  refresh(): void {
    const active = discoverCanvasTextEditor(this.getCanvas());
    this.activeEditorDetected = active !== null;
    if (!active) {
      this.detach();
      return;
    }
    if (active.source === this.source && active.nodeId === this.nodeId) return;
    this.detach();
    const ytext = this.doc.canvasNodeText(active.nodeId);
    if (!ytext) return;
    this.source = active.source;
    this.nodeId = active.nodeId;
    this.onActiveNode(active.nodeId);
    this.binding = new YTextEditorBinding(active.editor, ytext, {
      whenReady: () => this.doc.whenReady(),
      isReady: () => this.doc.isReady(),
      hasSyncedOnce: () => this.doc.hasSyncedOnce,
      mayPushToShared: (sharedEmpty) =>
        shouldPushEditorToShared({
          sharedEmpty,
          isCreator: this.doc.isCreator,
          hasSyncedOnce: this.doc.hasSyncedOnce,
          providerOnline: this.doc.isProviderOnline,
        }),
    });
    if (active.view) {
      this.cursorCleanup = mountCanvasRemoteTextCursors(
        active.view,
        ytext,
        this.doc.awareness,
        active.nodeId,
      );
    }
  }

  /** A live editor without a Y.Text binding must not be overwritten by a snapshot import. */
  hasUnboundActiveEditor(): boolean {
    this.refresh();
    return this.activeEditorDetected && this.binding === null;
  }

  private detach(): void {
    this.binding?.destroy();
    this.binding = null;
    this.cursorCleanup?.();
    this.cursorCleanup = null;
    this.source = null;
    if (this.nodeId !== null) this.onActiveNode(null);
    this.nodeId = null;
  }

  destroy(): void {
    clearInterval(this.timer ?? undefined);
    this.timer = null;
    this.lifecycleCleanup?.();
    this.lifecycleCleanup = null;
    this.detach();
    this.activeEditorDetected = false;
  }
}
