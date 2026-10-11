import { TFile, normalizePath } from "obsidian";
import type RealtimePlugin from "./main";
import { ensureParentFolder } from "./vaultHelpers";

type ConflictSource = "local" | "remote";
const CONFLICT_COPY_RE = / \(conflicted copy .+\)$/;

/** True for sibling recovery copies, which must remain device-local. */
export function isConflictCopy(path: string): boolean {
  const normalized = normalizePath(path);
  const dot = normalized.lastIndexOf(".");
  const base = dot > normalized.lastIndexOf("/") ? normalized.slice(0, dot) : normalized;
  return CONFLICT_COPY_RE.test(base);
}

function sourceLabel(plugin: RealtimePlugin, source: ConflictSource): string {
  if (source === "remote") return "Remote";
  const label = plugin.settings.clientName
    .replace(/[^\p{L}\p{N} _-]+/gu, " ")
    .replace(/\s+/g, " ")
    .trim();
  return label || "This device";
}

function timestamp(now: Date): string {
  return now.toISOString().replace(/[-:]/g, "").replace("T", " ").slice(0, 15);
}

function splitPath(path: string): { stem: string; extension: string } {
  const slash = path.lastIndexOf("/");
  const dot = path.lastIndexOf(".");
  if (dot <= slash) return { stem: path, extension: "" };
  return { stem: path.slice(0, dot), extension: path.slice(dot) };
}

function candidatePath(
  plugin: RealtimePlugin,
  path: string,
  source: ConflictSource,
  now: Date,
  copy: number,
): string {
  const { stem, extension } = splitPath(normalizePath(path));
  const suffix = `conflicted copy ${sourceLabel(plugin, source)} ${timestamp(now)}`;
  return `${stem} (${suffix}${copy > 1 ? ` ${copy}` : ""})${extension}`;
}

export function conflictCopyPath(
  plugin: RealtimePlugin,
  path: string,
  source: ConflictSource,
  now = new Date(),
): string {
  let candidate = candidatePath(plugin, path, source, now, 1);
  let copy = 2;
  while (plugin.app.vault.getAbstractFileByPath(candidate)) {
    candidate = candidatePath(plugin, path, source, now, copy);
    copy++;
  }
  return candidate;
}

/** Preserve files under Obsidian's hidden config directory through the adapter. */
export async function preserveAdapterConflict(
  plugin: RealtimePlugin,
  path: string,
  content: ArrayBuffer,
  source: ConflictSource,
): Promise<string> {
  const now = new Date();
  let copy = 1;
  let destination = candidatePath(plugin, path, source, now, copy);
  while (await plugin.app.vault.adapter.exists(destination)) {
    destination = candidatePath(plugin, path, source, now, ++copy);
  }
  await plugin.app.vault.adapter.writeBinary(destination, content);
  return destination;
}

export async function preserveTextConflict(
  plugin: RealtimePlugin,
  path: string,
  content: string,
  source: ConflictSource,
): Promise<string> {
  const destination = conflictCopyPath(plugin, path, source);
  await ensureParentFolder(plugin.app, destination);
  await plugin.app.vault.create(destination, content);
  return destination;
}

/**
 * Keep `content` in a single recovery copy of `path` rather than one copy
 * per change. Rewrites `current` while it exists. Otherwise it writes nothing
 * when some copy already holds exactly this content (one an earlier session
 * wrote), and creates a copy only when none does. Returns the path written,
 * or null when an existing copy already held the content.
 */
export async function updateTextConflict(
  plugin: RealtimePlugin,
  path: string,
  content: string,
  source: ConflictSource,
  current: string | null,
): Promise<string | null> {
  const vault = plugin.app.vault;
  const existing = current === null ? null : vault.getAbstractFileByPath(current);
  if (current !== null && existing instanceof TFile) {
    await vault.modify(existing, content);
    return current;
  }
  const { stem, extension } = splitPath(normalizePath(path));
  for (const file of vault.getFiles()) {
    if (!file.path.startsWith(`${stem} (conflicted copy `)) continue;
    if (!file.path.endsWith(`)${extension}`)) continue;
    if ((await vault.read(file)) === content) return null;
  }
  return preserveTextConflict(plugin, path, content, source);
}

export async function preserveBinaryConflict(
  plugin: RealtimePlugin,
  path: string,
  content: ArrayBuffer,
  source: ConflictSource,
): Promise<string> {
  const destination = conflictCopyPath(plugin, path, source);
  await ensureParentFolder(plugin.app, destination);
  await plugin.app.vault.createBinary(destination, content);
  return destination;
}
