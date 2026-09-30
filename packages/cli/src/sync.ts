/**
 * Daemon-free sync between a local folder and a vault: a three-way diff of
 * {local files, the `.rtmd` snapshot of the last sync, remote listings}.
 *
 * Notes and structured docs have no content hash in remote listings, so
 * `status` only reports their remote add/delete; `pull` and `push` fetch
 * content lazily to detect real remote edits (and skip identical writes).
 */

import * as crypto from "node:crypto";
import * as fs from "node:fs";
import * as path from "node:path";
import { ConflictError, type VaultHandle } from "@realtime-md/sdk";
import {
  CliError,
  writeRtmd,
  type FileKind,
  type RtmdConfig,
  type SyncFileState,
  type Workspace,
} from "./config";
import { forgetIgnoredAttachments, shouldSyncFile } from "./attachmentFilter";
import { isExcluded, kindForPath } from "./kinds";

export function hashBytes(bytes: Uint8Array): string {
  return crypto.createHash("sha256").update(bytes).digest("hex");
}

export function hashText(text: string): string {
  return hashBytes(Buffer.from(text, "utf8"));
}

/**
 * Canvas/base files are JSON documents; the server stores a value, not text.
 * Hash a normalized (parse + re-stringify) form so formatting differences
 * between the local file and the server round-trip don't read as edits.
 */
export function normalizeStructured(text: string): string {
  try {
    return JSON.stringify(JSON.parse(text));
  } catch (e) {
    // Return the original text so the hash differs from the server's normalized
    // form, which surfaces the corrupted file as a real diff rather than
    // silently matching. The upload path will report a clear error.
    return text;
  }
}

export function hashLocalContent(kind: FileKind, bytes: Uint8Array): string {
  if (kind === "canvas" || kind === "base") {
    return hashText(normalizeStructured(Buffer.from(bytes).toString("utf8")));
  }
  return hashBytes(bytes);
}

// ---------- local scan ----------

export interface LocalEntry {
  kind: FileKind;
  size: number;
  mtimeMs: number;
  hash: string;
}

/** Walk the folder; reuse snapshot hashes when size+mtime are unchanged. */
export function scanLocal(
  dir: string,
  snapshot: Record<string, SyncFileState>,
  config?: RtmdConfig,
): Map<string, LocalEntry> {
  const out = new Map<string, LocalEntry>();
  const walk = (sub: string) => {
    for (const entry of fs.readdirSync(path.join(dir, sub), { withFileTypes: true })) {
      const rel = sub === "" ? entry.name : `${sub}/${entry.name}`;
      if (isExcluded(rel)) continue;
      if (entry.isSymbolicLink()) continue;
      if (entry.isDirectory()) {
        walk(rel);
        continue;
      }
      if (!entry.isFile()) continue;
      const kind = kindForPath(rel);
      if (config && !shouldSyncFile(config, rel, kind)) continue;
      const abs = path.join(dir, rel);
      const stat = fs.statSync(abs);
      const snap = snapshot[rel];
      const hash =
        snap && snap.size === stat.size && snap.mtimeMs === stat.mtimeMs
          ? snap.hash
          : hashLocalContent(kind, fs.readFileSync(abs));
      out.set(rel, { kind, size: stat.size, mtimeMs: stat.mtimeMs, hash });
    }
  };
  walk("");
  return out;
}

// ---------- remote listing & content ----------

export interface RemoteEntry {
  kind: FileKind;
  /** Server content hash — attachments only. */
  hash?: string;
  guid?: string;
}

export async function listRemote(
  vault: VaultHandle,
  config?: RtmdConfig,
): Promise<Map<string, RemoteEntry>> {
  const [notes, attachments, canvases, bases] = await Promise.all([
    vault.notes.list(),
    config?.attachmentSync?.enabled === false ? Promise.resolve([]) : vault.attachments.list(),
    vault.canvases.list(),
    vault.bases.list(),
  ]);
  const out = new Map<string, RemoteEntry>();
  for (const n of notes) out.set(n.path, { kind: "note", guid: n.guid });
  for (const a of attachments) {
    if (!config || shouldSyncFile(config, a.path, "attachment")) {
      out.set(a.path, { kind: "attachment", hash: a.hash });
    }
  }
  for (const c of canvases) out.set(c.path, { kind: "canvas", guid: c.guid });
  for (const b of bases) out.set(b.path, { kind: "base", guid: b.guid });
  return out;
}

interface RemoteContent {
  /** Hash comparable with local/snapshot hashes for this kind. */
  hash: string;
  /** Bytes to write locally on pull. */
  bytes: Uint8Array;
  guid?: string;
  /** Server version token for a Canvas/Base (`If-Match` on replace). */
  valueHash?: string;
}

/**
 * What a replace must still find on the server: the remote state `push`
 * compared against. Without it, a remote edit landing between that check and
 * the write would be silently reverted.
 */
interface RemotePrecondition {
  contentHash?: string;
  valueHash?: string;
  /** The remote had nothing at the path; creating must not replace a newcomer. */
  absent?: boolean;
}

async function fetchRemote(
  vault: VaultHandle,
  kind: FileKind,
  relPath: string,
): Promise<RemoteContent> {
  switch (kind) {
    case "note": {
      const note = await vault.notes.read(relPath);
      return {
        hash: hashText(note.content),
        bytes: Buffer.from(note.content, "utf8"),
        guid: note.guid,
      };
    }
    case "canvas":
    case "base": {
      const res =
        kind === "canvas" ? await vault.canvases.read(relPath) : await vault.bases.read(relPath);
      return {
        hash: hashText(JSON.stringify(res.value)),
        bytes: Buffer.from(`${JSON.stringify(res.value, null, "\t")}\n`, "utf8"),
        guid: res.guid,
        valueHash: res.valueHash,
      };
    }
    case "attachment": {
      const bytes = await vault.attachments.read(relPath);
      return { hash: hashBytes(bytes), bytes };
    }
  }
}

// ---------- status (pure classification, unit-testable) ----------

export type ChangeSide = "added" | "modified" | "deleted";

export interface StatusEntry {
  path: string;
  kind: FileKind;
  local?: ChangeSide;
  /** Remote `modified` is only detectable for attachments (listing hash). */
  remote?: ChangeSide;
}

export function classifyStatus(
  local: Map<string, { kind: FileKind; hash: string }>,
  snapshot: Record<string, SyncFileState>,
  remote: Map<string, RemoteEntry>,
): StatusEntry[] {
  const paths = new Set<string>([...local.keys(), ...Object.keys(snapshot), ...remote.keys()]);
  const out: StatusEntry[] = [];
  for (const p of [...paths].sort()) {
    const l = local.get(p);
    const s = snapshot[p];
    const r = remote.get(p);
    const entry: StatusEntry = { path: p, kind: (l?.kind ?? s?.kind ?? r?.kind)! };
    if (l && !s) entry.local = "added";
    else if (!l && s) entry.local = "deleted";
    else if (l && s && l.hash !== s.hash) entry.local = "modified";
    if (r && !s) entry.remote = "added";
    else if (!r && s) entry.remote = "deleted";
    else if (r && s && r.hash !== undefined && r.hash !== s.hash) entry.remote = "modified";
    if (entry.local || entry.remote) out.push(entry);
  }
  return out;
}

// ---------- pull / push ----------

export interface SyncReport {
  applied: { action: string; path: string }[];
  conflicts: { path: string; reason: string }[];
}

function ensureSync(config: RtmdConfig): NonNullable<RtmdConfig["sync"]> {
  config.sync ??= { lastSyncedAt: new Date(0).toISOString(), files: {} };
  return config.sync;
}

function recordLocal(
  dir: string,
  relPath: string,
  kind: FileKind,
  hash: string,
  files: Record<string, SyncFileState>,
  guid?: string,
): void {
  const stat = fs.statSync(path.join(dir, relPath));
  files[relPath] = {
    kind,
    hash,
    size: stat.size,
    mtimeMs: stat.mtimeMs,
    ...(guid ? { guid } : {}),
  };
}

/** Apply remote state to the local folder. Conflicts skip unless `theirs`. */
export async function pull(
  ws: Workspace,
  vault: VaultHandle,
  opts: { theirs?: boolean } = {},
): Promise<SyncReport> {
  const sync = ensureSync(ws.config);
  const snapshot = sync.files;
  forgetIgnoredAttachments(ws.config, snapshot);
  const local = scanLocal(ws.dir, snapshot, ws.config);
  const remote = await listRemote(vault, ws.config);
  const report: SyncReport = { applied: [], conflicts: [] };

  for (const [relPath, r] of remote) {
    if (isExcluded(relPath)) continue;
    const snap = snapshot[relPath];
    const l = local.get(relPath);
    // Attachment fast path: unchanged on the server, nothing to fetch.
    if (snap && r.hash !== undefined && r.hash === snap.hash) continue;
    const content = await fetchRemote(vault, r.kind, relPath);
    const remoteChanged = !snap || content.hash !== snap.hash;
    if (!remoteChanged) continue;
    const localChanged = snap ? !l || l.hash !== snap.hash : !!l;
    if (l && l.hash === content.hash) {
      // Same content on both sides; just adopt it into the snapshot.
      recordLocal(ws.dir, relPath, r.kind, content.hash, snapshot, content.guid);
      continue;
    }
    if (localChanged && !opts.theirs) {
      report.conflicts.push({
        path: relPath,
        reason: "modified locally and remotely (use --theirs to overwrite)",
      });
      continue;
    }
    const abs = path.join(ws.dir, relPath);
    try {
      if (fs.lstatSync(abs).isSymbolicLink()) {
        report.conflicts.push({
          path: relPath,
          reason: "local path is a symlink; refusing to overwrite",
        });
        continue;
      }
    } catch {
      // Path does not exist yet.
    }
    fs.mkdirSync(path.dirname(abs), { recursive: true });
    fs.writeFileSync(abs, content.bytes);
    recordLocal(ws.dir, relPath, r.kind, content.hash, snapshot, content.guid);
    report.applied.push({ action: snap ? "update" : "create", path: relPath });
  }

  // Deleted remotely: remove locally unless the local copy has new edits.
  for (const relPath of Object.keys(snapshot)) {
    if (remote.has(relPath)) continue;
    const snap = snapshot[relPath];
    const l = local.get(relPath);
    if (l && l.hash !== snap.hash && !opts.theirs) {
      report.conflicts.push({
        path: relPath,
        reason: "deleted remotely but modified locally (use --theirs to delete)",
      });
      continue;
    }
    if (l) fs.rmSync(path.join(ws.dir, relPath), { force: true });
    delete snapshot[relPath];
    report.applied.push({ action: "delete", path: relPath });
  }

  sync.lastSyncedAt = new Date().toISOString();
  writeRtmd(ws.dir, ws.config);
  return report;
}

async function uploadLocal(
  vault: VaultHandle,
  kind: FileKind,
  relPath: string,
  bytes: Uint8Array,
  exists: boolean,
  precondition: RemotePrecondition = {},
): Promise<string | undefined> {
  switch (kind) {
    case "note": {
      const text = Buffer.from(bytes).toString("utf8");
      const note = exists
        ? await vault.notes.replace(relPath, text, {
            expectedContentHash: precondition.contentHash,
          })
        : await vault.notes.create(relPath, text);
      return note.guid;
    }
    case "canvas":
    case "base": {
      let value: unknown;
      try {
        value = JSON.parse(Buffer.from(bytes).toString("utf8"));
      } catch (e) {
        throw new CliError(
          `${relPath}: invalid JSON (${e instanceof Error ? e.message : String(e)})`,
        );
      }
      const resource = kind === "canvas" ? vault.canvases : vault.bases;
      const res = exists
        ? await resource.replace(relPath, value, { ifMatch: precondition.valueHash })
        : await resource.create(relPath, value);
      return res.guid;
    }
    case "attachment":
      await vault.attachments.upload(relPath, bytes, {
        ifMatch: precondition.contentHash,
        ifNoneMatch: precondition.absent ? "*" : undefined,
      });
      return undefined;
  }
}

async function deleteRemote(vault: VaultHandle, kind: FileKind, relPath: string): Promise<void> {
  if (kind === "note") await vault.notes.delete(relPath);
  else if (kind === "canvas") await vault.canvases.delete(relPath);
  else if (kind === "base") await vault.bases.delete(relPath);
  else await vault.attachments.delete(relPath);
}

/** Apply local state to the vault. Conflicts skip unless `force`. */
export async function push(
  ws: Workspace,
  vault: VaultHandle,
  opts: { force?: boolean } = {},
): Promise<SyncReport> {
  const sync = ensureSync(ws.config);
  const snapshot = sync.files;
  forgetIgnoredAttachments(ws.config, snapshot);
  const local = scanLocal(ws.dir, snapshot, ws.config);
  const remote = await listRemote(vault, ws.config);
  const report: SyncReport = { applied: [], conflicts: [] };

  for (const [relPath, l] of local) {
    const snap = snapshot[relPath];
    const r = remote.get(relPath);
    const localChanged = !snap || l.hash !== snap.hash;
    if (!localChanged) continue;
    let precondition: RemotePrecondition = {};
    if (r && !opts.force) {
      // Detect a concurrent remote edit before overwriting it.
      const fetched = r.hash === undefined ? await fetchRemote(vault, r.kind, relPath) : null;
      const remoteHash = r.hash ?? fetched!.hash;
      if (remoteHash === l.hash) {
        recordLocal(ws.dir, relPath, l.kind, l.hash, snapshot, r.guid);
        continue;
      }
      if (!snap || remoteHash !== snap.hash) {
        report.conflicts.push({
          path: relPath,
          reason: "modified remotely too (use --force to overwrite)",
        });
        continue;
      }
      // ...and have the server refuse the write if one lands after this check.
      precondition = { contentHash: remoteHash, valueHash: fetched?.valueHash };
    }
    if (!r && snap && !opts.force) {
      report.conflicts.push({
        path: relPath,
        reason: "deleted remotely but modified locally (use --force to re-create)",
      });
      continue;
    }
    if (!r && !opts.force) precondition = { absent: true };
    const bytes = fs.readFileSync(path.join(ws.dir, relPath));
    let guid: string | undefined;
    try {
      guid = await uploadLocal(vault, l.kind, relPath, bytes, !!r, precondition);
    } catch (error) {
      if (!(error instanceof ConflictError)) throw error;
      const hint = opts.force ? "pull, then push again" : "use --force to overwrite";
      report.conflicts.push({
        path: relPath,
        reason: `${r ? "modified" : "created"} remotely during push (${hint})`,
      });
      continue;
    }
    recordLocal(ws.dir, relPath, l.kind, l.hash, snapshot, guid);
    report.applied.push({ action: r ? "update" : "create", path: relPath });
  }

  // Deleted locally: delete remotely unless the remote copy changed meanwhile.
  for (const relPath of Object.keys(snapshot)) {
    if (local.has(relPath)) continue;
    const snap = snapshot[relPath];
    const r = remote.get(relPath);
    if (r) {
      if (!opts.force) {
        const remoteHash = r.hash ?? (await fetchRemote(vault, r.kind, relPath)).hash;
        if (remoteHash !== snap.hash) {
          report.conflicts.push({
            path: relPath,
            reason: "deleted locally but modified remotely (use --force to delete)",
          });
          continue;
        }
      }
      await deleteRemote(vault, snap.kind, relPath);
      report.applied.push({ action: "delete", path: relPath });
    }
    delete snapshot[relPath];
  }

  sync.lastSyncedAt = new Date().toISOString();
  writeRtmd(ws.dir, ws.config);
  return report;
}
