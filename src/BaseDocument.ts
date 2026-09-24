import type RealtimePlugin from "./main";
import type { DocumentBootstrapOptions } from "./SyncedDoc";
import { StructuredDocument } from "./StructuredDocument";
import { BaseBinding, BASE_LOCAL_ORIGIN } from "./editor/BaseBinding";
import { parseBase, serializeBase } from "./structured/base";
import type { JsonValue } from "./structured/reconcile";

export class BaseDocument extends StructuredDocument {
  private binding: BaseBinding;

  constructor(
    plugin: RealtimePlugin,
    path: string,
    guid: string,
    serverDocId: string,
    isCreator: boolean,
    opts: DocumentBootstrapOptions = {},
  ) {
    super(plugin, path, guid, serverDocId, isCreator, opts);
    this.binding = new BaseBinding(plugin, this);
    this.binding.tryBind();
  }

  /** Fold a `.base` YAML snapshot captured from the live view into the CRDT. */
  reconcileFromBaseText(text: string, origin: unknown): void {
    this.applyValue(this.parse(text), origin);
  }

  /** Serialize the CRDT's current value to `.base` YAML for the live view. */
  baseData(): string {
    return this.serialize(this.value);
  }

  tryBindLiveBase(): void {
    this.binding.tryBind();
  }

  /** Drop the live binding if its view closed or now shows another file. */
  unbindStaleBase(): void {
    this.binding.unbindIfStale();
  }

  // Defer to the live binding only while it's actually patched onto an open
  // Bases view; otherwise the StructuredDocument disk write-through stays active
  // (so bases still sync when the API shape is unrecognized or the file is closed).
  protected shouldDeferToLiveBinding(): boolean {
    return this.binding.isActive();
  }

  protected parse(text: string): JsonValue {
    return parseBase(text);
  }

  protected serialize(value: JsonValue): string {
    return serializeBase(value);
  }

  protected onRootChanged(origin?: unknown): void {
    super.onRootChanged(origin);
    // Don't bounce our own just-captured edit back into the live view; remote
    // edits (and disk folds) carry a different origin and do get applied.
    if (origin === BASE_LOCAL_ORIGIN) return;
    this.binding.applyRemote();
  }

  protected destroySubclass(): void {
    this.binding.destroy();
    super.destroySubclass();
  }
}
