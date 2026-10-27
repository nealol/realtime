import { StateField } from "@codemirror/state";

// Minimal stand-in for the `obsidian` module so that src/Document.ts,
// src/VaultSync.ts and src/sync/clientToken.ts can run under Node/vitest.
// Only the symbols those files actually import are provided.

export class TAbstractFile {
  path: string;
  name: string;
  constructor(path: string) {
    this.path = path;
    this.name = path.slice(path.lastIndexOf("/") + 1);
  }
}

export class TFile extends TAbstractFile {
  extension: string;
  basename: string;
  constructor(path: string) {
    super(path);
    const dot = this.name.lastIndexOf(".");
    this.extension = dot >= 0 ? this.name.slice(dot + 1) : "";
    this.basename = dot >= 0 ? this.name.slice(0, dot) : this.name;
  }
}

/** Placeholder for Obsidian's App; tests pass a fake plugin's `app` in practice. */
export class App {}

export class PluginSettingTab {
  app: unknown;
  containerEl: HTMLElement;
  constructor(app: unknown, _plugin: unknown) {
    this.app = app;
    this.containerEl = document.createElement("div");
  }
}

export class ExtraButtonComponent {}

export class ToggleComponent {}

/** Mutable so tests can exercise Obsidian's mobile-only lifecycle paths. */
export const Platform = {
  isMobile: false,
};

/** Obsidian's per-editor owner field (MarkdownView, embed, …); provide it with `.init`. */
export const editorInfoField = StateField.define<unknown>({
  create: () => null,
  update: (value) => value,
});

/** Minimal Modal stand-in so modules that subclass it can be imported under Node. */
export class Modal {
  app: unknown;
  contentEl: { empty: () => void } = { empty: () => {} };
  constructor(app: unknown) {
    this.app = app;
  }
  open(): void {}
  close(): void {}
  onOpen(): void {}
  onClose(): void {}
}

/** Captures every Notice message so tests can assert on user-facing surfacing. */
export const notices: string[] = [];

export class Notice {
  constructor(message: string, _timeout?: number) {
    notices.push(message);
  }
  hide(): void {}
}

/** Mirrors Obsidian's path normalisation closely enough for our use. */
export function normalizePath(path: string): string {
  return path
    .replace(/\\/g, "/")
    .replace(/\/{2,}/g, "/")
    .replace(/(^\/|\/$)/g, "")
    .trim();
}

/** Obsidian's CORS-bypassing fetch — backed here by Node's global fetch. */
export async function requestUrl(opts: {
  url: string;
  method?: string;
  contentType?: string;
  headers?: Record<string, string>;
  body?: string | ArrayBuffer;
  throw?: boolean;
}): Promise<{ status: number; text: string; json: unknown; arrayBuffer: ArrayBuffer }> {
  const headers: Record<string, string> = { ...(opts.headers ?? {}) };
  if (opts.contentType) headers["Content-Type"] = opts.contentType;
  const res = await fetch(opts.url, {
    method: opts.method ?? "GET",
    headers,
    body: opts.body as BodyInit | undefined,
  });
  // Read the body once as bytes, then derive text/json from it (Obsidian's
  // requestUrl exposes all three on the response).
  const arrayBuffer = await res.arrayBuffer();
  const text = new TextDecoder().decode(arrayBuffer);
  let json: unknown = undefined;
  try {
    json = text ? JSON.parse(text) : undefined;
  } catch {
    /* not json */
  }
  return { status: res.status, text, json, arrayBuffer };
}
