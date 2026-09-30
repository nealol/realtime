import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { ConflictError, type UploadAttachmentOptions, type VaultHandle } from "@realtime-md/sdk";
import { afterEach, describe, expect, it } from "vitest";
import {
  classifyStatus,
  hashBytes,
  hashText,
  normalizeStructured,
  pull,
  push,
  type RemoteEntry,
} from "../../src/sync";
import type { FileKind, RtmdConfig, SyncFileState, Workspace } from "../../src/config";

const snapEntry = (kind: FileKind, hash: string): SyncFileState => ({
  kind,
  hash,
  size: 1,
  mtimeMs: 1,
});

describe("classifyStatus", () => {
  it("reports nothing when all three sides agree", () => {
    const h = hashText("hello");
    const entries = classifyStatus(
      new Map([["a.md", { kind: "note", hash: h }]]),
      { "a.md": snapEntry("note", h) },
      new Map<string, RemoteEntry>([["a.md", { kind: "note" }]]),
    );
    expect(entries).toEqual([]);
  });

  it("classifies local add/modify/delete against the snapshot", () => {
    const h = hashText("old");
    const entries = classifyStatus(
      new Map([
        ["new.md", { kind: "note" as const, hash: hashText("x") }],
        ["mod.md", { kind: "note" as const, hash: hashText("changed") }],
      ]),
      { "mod.md": snapEntry("note", h), "gone.md": snapEntry("note", h) },
      new Map<string, RemoteEntry>([
        ["mod.md", { kind: "note" }],
        ["gone.md", { kind: "note" }],
      ]),
    );
    expect(entries).toEqual([
      { path: "gone.md", kind: "note", local: "deleted" },
      { path: "mod.md", kind: "note", local: "modified" },
      { path: "new.md", kind: "note", local: "added" },
    ]);
  });

  it("classifies remote add/delete and attachment hash modification", () => {
    const h = hashText("bytes");
    const entries = classifyStatus(
      new Map([["pic.png", { kind: "attachment" as const, hash: h }]]),
      { "pic.png": snapEntry("attachment", h), "old.md": snapEntry("note", h) },
      new Map<string, RemoteEntry>([
        ["pic.png", { kind: "attachment", hash: "different" }],
        ["fresh.md", { kind: "note" }],
      ]),
    );
    expect(entries).toEqual([
      { path: "fresh.md", kind: "note", remote: "added" },
      { path: "old.md", kind: "note", local: "deleted", remote: "deleted" },
      { path: "pic.png", kind: "attachment", remote: "modified" },
    ]);
  });

  it("flags both sides for a conflict candidate", () => {
    const entries = classifyStatus(
      new Map([["a.png", { kind: "attachment" as const, hash: "local" }]]),
      { "a.png": snapEntry("attachment", "synced") },
      new Map<string, RemoteEntry>([["a.png", { kind: "attachment", hash: "remote" }]]),
    );
    expect(entries).toEqual([
      { path: "a.png", kind: "attachment", local: "modified", remote: "modified" },
    ]);
  });
});

describe("normalizeStructured", () => {
  it("ignores formatting differences", () => {
    expect(normalizeStructured('{\n\t"a": 1\n}\n')).toBe(normalizeStructured('{"a":1}'));
  });

  it("passes through invalid JSON unchanged", () => {
    expect(normalizeStructured("not json")).toBe("not json");
  });
});

const tempDirs: string[] = [];

afterEach(() => {
  for (const dir of tempDirs.splice(0)) fs.rmSync(dir, { recursive: true, force: true });
});

function tempWorkspace(config: RtmdConfig): Workspace {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "rtmd-sync-"));
  tempDirs.push(dir);
  return { dir, config };
}

function fakeVault(options: {
  attachmentList?: () => Promise<{ path: string; hash: string; size: number }[]>;
  read?: (path: string) => Promise<Uint8Array>;
  upload?: (path: string, bytes: Uint8Array, options?: UploadAttachmentOptions) => Promise<unknown>;
}): VaultHandle {
  return {
    notes: { list: async () => [] },
    attachments: {
      list: options.attachmentList ?? (async () => []),
      read:
        options.read ??
        (async () => {
          throw new Error("unexpected attachment read");
        }),
      upload:
        options.upload ??
        (async (path: string, bytes: Uint8Array) => ({
          path,
          hash: hashBytes(bytes),
          size: bytes.byteLength,
        })),
    },
    canvases: { list: async () => [] },
    bases: { list: async () => [] },
  } as unknown as VaultHandle;
}

describe("attachment filtering during push", () => {
  it("uploads only attachments matching the persistent include filter", async () => {
    const ws = tempWorkspace({
      version: 1,
      baseUrl: "https://example.com",
      vaultId: "v1",
      attachmentSync: { enabled: true, includeGlobs: ["data/**"] },
    });
    fs.mkdirSync(path.join(ws.dir, "data"));
    fs.writeFileSync(path.join(ws.dir, "data/config.json"), "{}");
    fs.writeFileSync(path.join(ws.dir, "LICENSE"), "license");
    const uploaded: string[] = [];
    const vault = fakeVault({
      upload: async (attachmentPath, bytes) => {
        uploaded.push(attachmentPath);
        return { path: attachmentPath, hash: hashBytes(bytes), size: bytes.byteLength };
      },
    });

    const report = await push(ws, vault);

    expect(uploaded).toEqual(["data/config.json"]);
    expect(report.applied).toEqual([{ action: "create", path: "data/config.json" }]);
    expect(Object.keys(ws.config.sync?.files ?? {})).toEqual(["data/config.json"]);
  });

  it("does not list, upload, delete, or snapshot attachments when disabled", async () => {
    const bytes = Buffer.from("image");
    const ws = tempWorkspace({
      version: 1,
      baseUrl: "https://example.com",
      vaultId: "v1",
      attachmentSync: { enabled: false, includeGlobs: ["**/*.png"] },
      sync: {
        lastSyncedAt: new Date(0).toISOString(),
        files: {
          "image.png": {
            kind: "attachment",
            hash: hashBytes(bytes),
            size: bytes.byteLength,
            mtimeMs: 1,
          },
        },
      },
    });
    fs.writeFileSync(path.join(ws.dir, "image.png"), bytes);
    const vault = fakeVault({
      attachmentList: async () => {
        throw new Error("attachments must not be listed");
      },
      upload: async () => {
        throw new Error("attachments must not be uploaded");
      },
    });

    const report = await push(ws, vault);

    expect(report).toEqual({ applied: [], conflicts: [] });
    expect(fs.existsSync(path.join(ws.dir, "image.png"))).toBe(true);
    expect(ws.config.sync?.files).toEqual({});
  });

  it("does not download remote attachments outside the include filter", async () => {
    const ws = tempWorkspace({
      version: 1,
      baseUrl: "https://example.com",
      vaultId: "v1",
      attachmentSync: { enabled: true, includeGlobs: ["assets/**"] },
    });
    const vault = fakeVault({
      attachmentList: async () => [{ path: "LICENSE", hash: hashText("license"), size: 7 }],
    });

    const report = await pull(ws, vault);

    expect(report).toEqual({ applied: [], conflicts: [] });
    expect(fs.existsSync(path.join(ws.dir, "LICENSE"))).toBe(false);
    expect(ws.config.sync?.files).toEqual({});
  });
});

describe("attachment preconditions during push", () => {
  const png = (tail: string) => Buffer.from(`\x89PNG\r\n\x1a\n${tail}`, "latin1");

  /** A workspace whose last sync saw `synced` at img/pic.png, now edited to `local`. */
  function editedWorkspace(synced: Buffer | null, local: Buffer): Workspace {
    const ws = tempWorkspace({
      version: 1,
      baseUrl: "https://example.com",
      vaultId: "v1",
      attachmentSync: { enabled: true, includeGlobs: ["img/**"] },
      sync: {
        lastSyncedAt: new Date(0).toISOString(),
        files: synced
          ? {
              "img/pic.png": {
                kind: "attachment",
                hash: hashBytes(synced),
                size: synced.byteLength,
                mtimeMs: 1,
              },
            }
          : {},
      },
    });
    fs.mkdirSync(path.join(ws.dir, "img"));
    fs.writeFileSync(path.join(ws.dir, "img/pic.png"), local);
    return ws;
  }

  function remoteHolding(bytes: Buffer | null) {
    return async () =>
      bytes ? [{ path: "img/pic.png", hash: hashBytes(bytes), size: bytes.byteLength }] : [];
  }

  it("replaces only the remote version it compared against", async () => {
    const synced = png("synced");
    const local = png("local edit");
    const ws = editedWorkspace(synced, local);
    const sent: (UploadAttachmentOptions | undefined)[] = [];
    const vault = fakeVault({
      attachmentList: remoteHolding(synced),
      upload: async (attachmentPath, bytes, options) => {
        sent.push(options);
        return { path: attachmentPath, hash: hashBytes(bytes), size: bytes.byteLength };
      },
    });

    const report = await push(ws, vault);

    expect(sent).toEqual([{ ifMatch: hashBytes(synced), ifNoneMatch: undefined }]);
    expect(report).toEqual({ applied: [{ action: "update", path: "img/pic.png" }], conflicts: [] });
    expect(ws.config.sync?.files["img/pic.png"]?.hash).toBe(hashBytes(local));
  });

  it("reports a conflict instead of overwriting an attachment replaced during push", async () => {
    const synced = png("synced");
    const ws = editedWorkspace(synced, png("local edit"));
    const vault = fakeVault({
      attachmentList: remoteHolding(synced),
      upload: async () => {
        throw new ConflictError("stale");
      },
    });

    const report = await push(ws, vault);

    expect(report).toEqual({
      applied: [],
      conflicts: [
        {
          path: "img/pic.png",
          reason: "modified remotely during push (use --force to overwrite)",
        },
      ],
    });
    expect(ws.config.sync?.files["img/pic.png"]?.hash).toBe(hashBytes(synced));
  });

  it("creates a new attachment only while the path is still empty", async () => {
    const ws = editedWorkspace(null, png("new"));
    const sent: (UploadAttachmentOptions | undefined)[] = [];
    const vault = fakeVault({
      attachmentList: remoteHolding(null),
      upload: async (_path, _bytes, options) => {
        sent.push(options);
        throw new ConflictError("exists");
      },
    });

    const report = await push(ws, vault);

    expect(sent).toEqual([{ ifMatch: undefined, ifNoneMatch: "*" }]);
    expect(report).toEqual({
      applied: [],
      conflicts: [
        {
          path: "img/pic.png",
          reason: "created remotely during push (use --force to overwrite)",
        },
      ],
    });
    expect(ws.config.sync?.files["img/pic.png"]).toBeUndefined();
  });

  it("overwrites without preconditions under --force", async () => {
    const synced = png("synced");
    const ws = editedWorkspace(synced, png("local edit"));
    const sent: (UploadAttachmentOptions | undefined)[] = [];
    const vault = fakeVault({
      attachmentList: remoteHolding(png("changed remotely")),
      upload: async (attachmentPath, bytes, options) => {
        sent.push(options);
        return { path: attachmentPath, hash: hashBytes(bytes), size: bytes.byteLength };
      },
    });

    const report = await push(ws, vault, { force: true });

    expect(sent).toEqual([{ ifMatch: undefined, ifNoneMatch: undefined }]);
    expect(report.applied).toEqual([{ action: "update", path: "img/pic.png" }]);
  });
});
