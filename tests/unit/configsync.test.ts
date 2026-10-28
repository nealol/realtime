import { describe, expect, it, vi } from "vitest";
import * as Y from "yjs";
import {
  ConfigSync,
  decideConfigReconcile,
  mergeJsonSettings,
  mergeJsonSettingsThreeWay,
  type ConfigMeta,
} from "../../src/ConfigSync";
import { LocalSyncState } from "../../src/localSyncState";
import { sha256Hex } from "../../src/hash";
import { makeFakePlugin } from "../support/fakePlugin";
import { freshGuid } from "../support/util";
import { waitFor } from "../support/util";
import { HttpError } from "../../src/httpError";
import type RealtimePlugin from "../../src/main";

const local: ConfigMeta = { hash: "local", size: 1, mtime: 200 };
const remote: ConfigMeta = { hash: "remote", size: 1, mtime: 100 };

describe("ConfigSync reconcile decisions", () => {
  it("does not spin when the adapter persistently fails to read local config", async () => {
    vi.useFakeTimers();
    const { plugin } = makeFakePlugin("https://sync.example.com", {
      sessionToken: "token",
      activeVaultId: "vault",
    });
    const indexDoc = new Y.Doc();
    const readBinary = vi.fn(async () => {
      throw new Error("adapter unavailable");
    });
    (plugin.app.vault as any).adapter = {
      exists: vi.fn(async () => true),
      readBinary,
      stat: vi.fn(async () => null),
      list: vi.fn(async () => ({
        files: [".obsidian/appearance.json"],
        folders: [],
      })),
    };
    (plugin.app.vault as any).configDir = ".obsidian";
    const sync = new ConfigSync(plugin as any, indexDoc);
    try {
      (sync as any).started = true;
      (sync as any).enabledCategories = new Set(["appearance"]);
      await (sync as any).reconcile(".obsidian/appearance.json");
      expect(readBinary).toHaveBeenCalledTimes(1);
      await Promise.resolve();
      await Promise.resolve();
      expect(readBinary).toHaveBeenCalledTimes(1);

      await vi.advanceTimersByTimeAsync(15_000);
      expect(readBinary.mock.calls.length).toBeLessThanOrEqual(2);
      sync.setPaused(true);
      const callsAtPause = readBinary.mock.calls.length;
      await vi.advanceTimersByTimeAsync(30_000);
      expect(readBinary).toHaveBeenCalledTimes(callsAtPause);
    } finally {
      sync.destroy();
      indexDoc.destroy();
      vi.useRealTimers();
    }
  });

  it("resumes an interrupted first pull without publishing a remote delete", async () => {
    const { plugin } = makeFakePlugin("https://sync.example.com", {
      sessionToken: "token",
      activeVaultId: "vault",
    });
    const indexDoc = new Y.Doc();
    const remote = { hash: "remote-hash", size: 1, mtime: 1 };
    indexDoc.getMap<ConfigMeta>("configFiles").set(".obsidian/appearance.json", remote);
    const sync = new ConfigSync(plugin as any, indexDoc);
    (sync as any).lastSyncedHash.set(".obsidian/appearance.json", remote.hash);
    let release!: () => void;
    let passes = 0;
    vi.spyOn(sync, "reconcileAll").mockImplementation(async () => {
      passes += 1;
      if (passes === 1) await new Promise<void>((resolve) => (release = resolve));
      else {
        const action = decideConfigReconcile(null, remote, remote.hash, {
          initialPull: (sync as any).initialPull,
        });
        if (action === "deleteRemote") {
          indexDoc.getMap("configFiles").delete(".obsidian/appearance.json");
        }
      }
    });
    try {
      sync.start(new Set(["appearance"] as any));
      await vi.waitFor(() => expect(release).toBeTypeOf("function"));
      sync.setPaused(true);
      release();
      await Promise.resolve();
      sync.setPaused(false);
      await vi.waitFor(() => expect(passes).toBe(2));
      expect(indexDoc.getMap("configFiles").has(".obsidian/appearance.json")).toBe(true);
    } finally {
      sync.destroy();
      indexDoc.destroy();
    }
  });

  it("defers config reconciliation while paused and scans on resume", async () => {
    const { plugin } = makeFakePlugin("https://sync.example.com", {
      sessionToken: "token",
      activeVaultId: "vault",
    });
    const indexDoc = new Y.Doc();
    (plugin.app.vault as any).adapter = {
      readBinary: vi.fn(async () => new Uint8Array([1]).buffer),
      exists: vi.fn(async () => true),
      stat: vi.fn(async () => ({ mtime: 1, ctime: 1, size: 1, type: "file" })),
    };
    const sync = new ConfigSync(plugin as any, indexDoc);
    const reconcile = vi.spyOn(sync, "reconcileAll").mockResolvedValue();
    sync.setPaused(true);
    sync.start(new Set(["appearance"] as any));
    await Promise.resolve();
    expect(reconcile).not.toHaveBeenCalled();
    sync.setPaused(false);
    await Promise.resolve();
    expect(reconcile).toHaveBeenCalledTimes(1);
    sync.destroy();
    indexDoc.destroy();
  });

  it("re-reads a config file only when its size or mtime changes", async () => {
    const { plugin } = makeFakePlugin("https://sync.example.com", {
      sessionToken: "token",
      activeVaultId: "vault",
    });
    const indexDoc = new Y.Doc();
    let stat = { mtime: 1, ctime: 1, size: 1, type: "file" };
    const readBinary = vi.fn(async () => new Uint8Array([1]).buffer);
    (plugin.app.vault as any).adapter = {
      readBinary,
      exists: vi.fn(async () => true),
      stat: vi.fn(async () => stat),
    };
    const sync = new ConfigSync(plugin as any, indexDoc);
    const info = (path: string) => (sync as any).localInfo(path);
    try {
      const first = await info(".obsidian/app.json");
      expect(await info(".obsidian/app.json")).toEqual(first);
      expect(readBinary).toHaveBeenCalledTimes(1);
      stat = { ...stat, mtime: 2 };
      await info(".obsidian/app.json");
      expect(readBinary).toHaveBeenCalledTimes(2);
    } finally {
      sync.destroy();
      indexDoc.destroy();
    }
  });

  it("does not publish an upload that was paused in flight", async () => {
    const { plugin } = makeFakePlugin("https://sync.example.com", {
      sessionToken: "token",
      activeVaultId: "vault",
    });
    const indexDoc = new Y.Doc();
    const sync = new ConfigSync(plugin as any, indexDoc);
    let release!: (exists: boolean) => void;
    plugin.auth.blobExists = vi.fn(() => new Promise<boolean>((resolve) => (release = resolve)));
    const upload = (sync as any).uploadBytes(
      ".obsidian/appearance.json",
      new Uint8Array([1]).buffer,
      null,
      null,
    );
    await waitFor(() => typeof release === "function", { label: "blob check started" });
    sync.setPaused(true);
    release(true);
    await upload;
    expect(indexDoc.getMap("configFiles").has(".obsidian/appearance.json")).toBe(false);
    sync.destroy();
    indexDoc.destroy();
  });

  it("does not apply a download that was paused in flight", async () => {
    const { plugin } = makeFakePlugin("https://sync.example.com", {
      sessionToken: "token",
      activeVaultId: "vault",
    });
    const indexDoc = new Y.Doc();
    const sync = new ConfigSync(plugin as any, indexDoc);
    let release!: (bytes: ArrayBuffer) => void;
    plugin.auth.getBlob = vi.fn(() => new Promise<ArrayBuffer>((resolve) => (release = resolve)));
    const writeBinary = vi.fn(async () => undefined);
    (plugin.app.vault as any).adapter = {
      exists: vi.fn(async () => false),
      stat: vi.fn(async () => null),
      readBinary: vi.fn(),
      writeBinary,
      mkdir: vi.fn(async () => undefined),
    };
    const download = (sync as any).download(
      ".obsidian/appearance.json",
      { hash: "remote", size: 1, mtime: 1 },
      null,
    );
    await waitFor(() => typeof release === "function", { label: "config download started" });
    sync.setPaused(true);
    release(new Uint8Array([1]).buffer);
    await download;
    expect(writeBinary).not.toHaveBeenCalled();
    sync.destroy();
    indexDoc.destroy();
  });

  it("retains a durable config baseline after the remote map deletion has persisted", async () => {
    const { plugin } = makeFakePlugin("https://sync.example.com", {
      sessionToken: "token",
      activeVaultId: "vault",
    });
    const localState = new LocalSyncState(`config-delete:${freshGuid()}`);
    await localState.whenSynced;
    localState.markSynced(".obsidian/appearance.json", "config", "old-hash", "old-hash");
    const indexDoc = new Y.Doc();
    const sync = new ConfigSync(plugin as any, indexDoc, localState);
    try {
      sync.seedBaseline();
      expect((sync as any).lastSyncedHash.get(".obsidian/appearance.json")).toBe("old-hash");
    } finally {
      sync.destroy();
      indexDoc.destroy();
      localState.destroy();
    }
  });

  it("migrates a legacy config marker into the persisted index baseline", async () => {
    const { plugin } = makeFakePlugin("https://sync.example.com", {
      sessionToken: "token",
      activeVaultId: "vault",
    });
    const localState = new LocalSyncState(`config-legacy:${freshGuid()}`);
    await localState.whenSynced;
    localState.mark(".obsidian/appearance.json", "config");
    const indexDoc = new Y.Doc();
    indexDoc
      .getMap<ConfigMeta>("configFiles")
      .set(".obsidian/appearance.json", { hash: "remote-hash", size: 1, mtime: 1 });
    const sync = new ConfigSync(plugin as any, indexDoc, localState);
    try {
      sync.seedBaseline();
      expect((sync as any).lastSyncedHash.get(".obsidian/appearance.json")).toBe("remote-hash");
    } finally {
      sync.destroy();
      indexDoc.destroy();
      localState.destroy();
    }
  });

  it("downloads remote config on a fresh device when both sides exist", () => {
    expect(decideConfigReconcile(local, remote, null)).toBe("download");
  });

  it("downloads a remote file this device has never pulled (no baseline)", () => {
    expect(decideConfigReconcile(null, remote, null)).toBe("download");
  });

  it("propagates a local delete once the initial pull is done", () => {
    expect(decideConfigReconcile(null, remote, remote.hash)).toBe("deleteRemote");
  });

  it("never publishes a delete during the initial pull (seeded baselines lie)", () => {
    expect(decideConfigReconcile(null, remote, remote.hash, { initialPull: true })).toBe(
      "download",
    );
  });

  it("downloads instead of deleting when remote moved past our delete baseline", () => {
    expect(decideConfigReconcile(null, remote, "older")).toBe("download");
  });

  it("deletes locally when the remote entry vanished and local matches baseline", () => {
    expect(decideConfigReconcile(local, null, local.hash)).toBe("deleteLocal");
  });

  it("keeps (re-uploads) local edits when remote deleted a file we changed", () => {
    expect(decideConfigReconcile(local, null, "older")).toBe("upload");
  });

  it("resolves clean one-sided edits from the baseline", () => {
    expect(decideConfigReconcile(local, remote, remote.hash)).toBe("upload");
    expect(decideConfigReconcile(local, remote, local.hash)).toBe("download");
  });

  it("merges true conflicts on JSON settings files", () => {
    expect(decideConfigReconcile(local, remote, "older", { canMerge: true })).toBe("merge");
  });

  it("tells a remote built on our publish from one last-writer-wins kept over it", () => {
    // Built on the version this device published: a clean update.
    expect(decideConfigReconcile(local, { ...remote, prev: local.hash }, local.hash)).toBe(
      "download",
    );
    // Published concurrently with ours from an older version: both sides changed.
    expect(
      decideConfigReconcile(local, { ...remote, prev: "older" }, local.hash, {
        canMerge: true,
        ownBase: true,
      }),
    ).toBe("merge");
    // Still exactly the version our publish replaced: that publish was lost.
    expect(
      decideConfigReconcile(local, { ...remote, prev: "oldest" }, local.hash, {
        replacedHash: remote.hash,
      }),
    ).toBe("upload");
    // Older publishers record no parent: keep the previous behavior.
    expect(decideConfigReconcile(local, remote, local.hash)).toBe("download");
  });

  it("downloads a remote that moved several versions past an untouched baseline", () => {
    // Another device published our baseline, so nothing of ours was overwritten.
    expect(
      decideConfigReconcile(local, { ...remote, prev: "middle" }, local.hash, {
        canMerge: true,
        ownBase: false,
      }),
    ).toBe("download");
    // Our own publish, then two remote versions built on it: its lineage says so.
    expect(
      decideConfigReconcile(
        local,
        { ...remote, prev: "middle", lineage: ["middle", local.hash.slice(0, 16)] },
        local.hash,
        { canMerge: true, ownBase: true },
      ),
    ).toBe("download");
  });

  it("falls back to newest-wins on non-mergeable conflicts", () => {
    expect(decideConfigReconcile(local, remote, "older")).toBe("upload");
    const olderLocal = { ...local, mtime: 50 };
    expect(decideConfigReconcile(olderLocal, remote, "older")).toBe("download");
  });
});

describe("mergeJsonSettings", () => {
  it("applies local keys on top of remote keys", () => {
    const merged = mergeJsonSettings(
      JSON.stringify({ theme: "obsidian", fontSize: 14 }),
      JSON.stringify({ fontSize: 18, spellcheck: true }),
    );
    expect(merged).not.toBeNull();
    expect(JSON.parse(merged as string)).toEqual({
      theme: "obsidian",
      fontSize: 18,
      spellcheck: true,
    });
  });

  it("returns null for non-object JSON so callers fall back to newest-wins", () => {
    expect(mergeJsonSettings("[1,2]", "{}")).toBeNull();
    expect(mergeJsonSettings("{}", '"str"')).toBeNull();
    expect(mergeJsonSettings("not json", "{}")).toBeNull();
  });
});

describe("three-way JSON settings merge", () => {
  it("merges disjoint key changes without flagging a conflict", () => {
    const base = JSON.stringify({ a: 1, b: 1, nested: { x: 1, y: 1 } });
    const remote = JSON.stringify({ a: 2, b: 1, nested: { x: 1, y: 2 } });
    const local = JSON.stringify({ a: 1, b: 3, nested: { x: 3, y: 1 } });
    const merged = mergeJsonSettingsThreeWay(base, remote, local);
    expect(merged?.conflicted).toBe(false);
    expect(JSON.parse(merged!.text)).toEqual({ a: 2, b: 3, nested: { x: 3, y: 2 } });
  });

  it("flags the same key changed differently on both sides", () => {
    const merged = mergeJsonSettingsThreeWay('{"a":1}', '{"a":2}', '{"a":3}');
    expect(merged?.conflicted).toBe(true);
  });

  it("returns null for non-object JSON", () => {
    expect(mergeJsonSettingsThreeWay("[]", "{}", "{}")).toBeNull();
  });

  it("publishes a clean plugin data merge without a recovery copy", async () => {
    const { plugin } = makeFakePlugin("https://sync.example.com", {
      sessionToken: "token",
      activeVaultId: "vault",
    });
    const path = ".obsidian/plugins/tasks/data.json";
    const encode = (value: unknown) => new TextEncoder().encode(JSON.stringify(value)).buffer;
    const baseBytes = encode({ lastOpened: 1, theme: "dark", recent: ["a"] });
    const remoteBytes = encode({ lastOpened: 2, theme: "dark", recent: ["a"] });
    const localBytes = encode({ lastOpened: 1, theme: "dark", recent: ["a", "b"] });
    const baseHash = await sha256Hex(baseBytes);
    const remoteHash = await sha256Hex(remoteBytes);
    const localHash = await sha256Hex(localBytes);
    const disk = new Map<string, ArrayBuffer>([[path, localBytes]]);
    const blobs = new Map<string, ArrayBuffer>([
      [baseHash, baseBytes],
      [remoteHash, remoteBytes],
    ]);
    const indexDoc = new Y.Doc();
    const configFiles = indexDoc.getMap<ConfigMeta>("configFiles");
    (plugin.app.vault as any).adapter = {
      exists: vi.fn(async (file: string) => disk.has(file)),
      stat: vi.fn(async () => ({ mtime: 1 })),
      readBinary: vi.fn(async (file: string) => disk.get(file)!),
      writeBinary: vi.fn(async (file: string, bytes: ArrayBuffer) => void disk.set(file, bytes)),
      mkdir: vi.fn(async () => undefined),
    };
    // Mirror the server: a path-scoped request only serves the hash that path
    // currently maps to; a hash-only request serves any stored blob.
    plugin.auth.getBlob = vi.fn(async (_vault: string, blobPath: string | null, hash: string) => {
      if (blobPath !== null && configFiles.get(blobPath)?.hash !== hash) {
        throw new HttpError("blob download failed: not found", 404);
      }
      const bytes = blobs.get(hash);
      if (!bytes) throw new Error("missing blob");
      return bytes;
    });
    plugin.auth.blobExists = vi.fn(async () => false);
    plugin.auth.putBlob = vi.fn(async (_vault: string, _path: string, hash: string, bytes) => {
      blobs.set(hash, bytes);
    });
    configFiles.set(path, { hash: remoteHash, size: remoteBytes.byteLength, mtime: 1 });
    const sync = new ConfigSync(plugin as any, indexDoc);
    try {
      (sync as any).lastSyncedHash.set(path, baseHash);
      await (sync as any).mergeConflict(
        path,
        { hash: localHash, size: localBytes.byteLength, mtime: 1 },
        configFiles.get(path),
      );
      const merged = JSON.parse(new TextDecoder().decode(disk.get(path)!));
      expect(merged).toEqual({ lastOpened: 2, theme: "dark", recent: ["a", "b"] });
      expect([...disk.keys()].filter((file) => /conflicted copy/.test(file))).toEqual([]);
      expect(configFiles.get(path)?.hash).toBe(await sha256Hex(disk.get(path)!));
    } finally {
      sync.destroy();
      indexDoc.destroy();
    }
  });
});

describe("ConfigSync with versions missed while away", () => {
  const path = ".obsidian/app.json";
  const json = (value: Record<string, number>) => {
    const bytes = new TextEncoder().encode(JSON.stringify(value, null, 2));
    return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength);
  };

  /** One device's config sync over an in-memory adapter and blob store. */
  async function device(
    onDisk: ArrayBuffer | null,
    blobs: Map<string, ArrayBuffer>,
    sharedIndex?: Y.Doc,
  ) {
    const { plugin } = makeFakePlugin("https://sync.example.com", {
      sessionToken: "token",
      activeVaultId: "vault",
    });
    const disk = new Map<string, ArrayBuffer>(onDisk ? [[path, onDisk]] : []);
    Object.assign(plugin.app.vault, {
      configDir: ".obsidian",
      adapter: {
        exists: async (p: string) => p === ".obsidian" || disk.has(p),
        stat: async (p: string) => {
          const stored = disk.get(p);
          return stored ? { mtime: 1, ctime: 1, size: stored.byteLength, type: "file" } : null;
        },
        readBinary: async (p: string) => disk.get(p),
        writeBinary: async (p: string, data: ArrayBuffer) => {
          disk.set(p, data);
        },
        mkdir: async () => {},
        remove: async (p: string) => {
          disk.delete(p);
        },
        list: async () => ({ files: [...disk.keys()], folders: [] }),
      },
    });
    Object.assign(plugin.auth, {
      getBlob: async (_vault: string, _path: string | null, hash: string) => {
        const blob = blobs.get(hash);
        if (!blob) throw new HttpError("blob not found", 404);
        return blob;
      },
      blobExists: async (_vault: string, _path: string, hash: string) => blobs.has(hash),
      putBlob: async (_vault: string, _path: string, hash: string, data: ArrayBuffer) => {
        blobs.set(hash, data);
      },
    });
    const localState = new LocalSyncState(`config-missed:${freshGuid()}`);
    await localState.whenSynced;
    const indexDoc = sharedIndex ?? new Y.Doc();
    // The fake implements the slice of the plugin ConfigSync uses.
    const realtime = plugin as unknown as RealtimePlugin;
    const sync = new ConfigSync(realtime, indexDoc, localState);
    return {
      disk,
      auth: plugin.auth,
      localState,
      configFiles: indexDoc.getMap<ConfigMeta>("configFiles"),
      sync,
      destroy: () => {
        sync.destroy();
        if (!sharedIndex) indexDoc.destroy();
        localState.destroy();
      },
    };
  }

  it("restores a missing synced settings file after a transient download failure", async () => {
    const v1 = json({ a: 1 });
    const h1 = await sha256Hex(v1);
    const blobs = new Map([[h1, v1]]);
    const shared = new Y.Doc();
    shared.getMap<ConfigMeta>("configFiles").set(path, {
      hash: h1,
      size: v1.byteLength,
      mtime: 1,
      prev: null,
    });
    const healthy = await device(v1, blobs, shared);
    const missing = await device(null, blobs, shared);
    try {
      for (const peer of [healthy, missing]) {
        peer.localState.markSynced(path, "config", h1, h1);
        peer.sync.seedBaseline();
      }
      const getBlob = missing.auth.getBlob;
      let failures = 0;
      missing.auth.getBlob = async (...args: Parameters<typeof getBlob>) => {
        if (failures === 0) {
          failures += 1;
          throw new Error("network unavailable");
        }
        return getBlob(...args);
      };
      healthy.sync.start(new Set(["mainSettings"]));
      missing.sync.start(new Set(["mainSettings"]));
      await waitFor(() => failures === 1, { label: "first download failed" });
      // The 2s retry runs after the startup pass finished; it must download,
      // not read the still-missing file as a local delete.
      await waitFor(() => missing.disk.has(path) || !missing.configFiles.has(path), {
        label: "retry reconciled",
      });
      expect(failures).toBe(1);
      expect(missing.configFiles.get(path)?.hash).toBe(h1);
      expect(new TextDecoder().decode(missing.disk.get(path))).toBe(new TextDecoder().decode(v1));
      expect(new TextDecoder().decode(healthy.disk.get(path))).toBe(new TextDecoder().decode(v1));
    } finally {
      healthy.destroy();
      missing.destroy();
      shared.destroy();
    }
  });

  it("propagates a real delete after an independently restored pending download", async () => {
    const v1 = json({ a: 1 });
    const h1 = await sha256Hex(v1);
    const missing = await device(null, new Map([[h1, v1]]));
    try {
      missing.configFiles.set(path, { hash: h1, size: v1.byteLength, mtime: 1 });
      missing.localState.markSynced(path, "config", h1, h1);
      missing.sync.seedBaseline();
      missing.auth.getBlob = async () => {
        throw new Error("network unavailable");
      };
      // Exercise startup directly without installing poll/focus timers.
      const startup = missing.sync as unknown as {
        started: boolean;
        enabledCategories: Set<"mainSettings">;
        runInitialPull: () => Promise<void>;
      };
      startup.started = true;
      startup.enabledCategories = new Set(["mainSettings"]);
      await startup.runInitialPull();

      // Another writer materializes the exact remote bytes before our retry.
      missing.disk.set(path, v1);
      await startup.runInitialPull();
      expect(missing.configFiles.get(path)?.hash).toBe(h1);

      missing.disk.delete(path);
      await missing.sync.reconcileAll();
      expect(missing.configFiles.has(path)).toBe(false);
    } finally {
      missing.destroy();
    }
  });

  it("keeps one retry per path and never deletes while a download keeps failing", async () => {
    const remoteMeta = { hash: "remote-hash", size: 1, mtime: 1 };
    const missing = await device(null, new Map());
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    try {
      missing.configFiles.set(path, remoteMeta);
      missing.localState.markSynced(path, "config", remoteMeta.hash, remoteMeta.hash);
      missing.sync.seedBaseline();
      let attempts = 0;
      missing.auth.getBlob = async () => {
        attempts += 1;
        throw new Error("network unavailable");
      };
      // Drive full passes directly, without start()'s poll/focus timers.
      const lifecycle = missing.sync as unknown as {
        started: boolean;
        enabledCategories: Set<"mainSettings">;
        runInitialPull: () => Promise<void>;
      };
      lifecycle.started = true;
      lifecycle.enabledCategories = new Set(["mainSettings"]);
      for (let i = 0; i < 3; i++) await lifecycle.runInitialPull();
      expect(attempts).toBe(3);
      await vi.advanceTimersByTimeAsync(2_000);
      expect(attempts).toBe(4);
      await vi.advanceTimersByTimeAsync(2_000);
      expect(attempts).toBe(5);
      expect(missing.configFiles.get(path)).toEqual(remoteMeta);
    } finally {
      missing.destroy();
      vi.useRealTimers();
    }
  });

  it("downloads a settings file changed twice elsewhere instead of reverting the first change", async () => {
    const [v1, v2, v3] = [json({ a: 1, b: 1 }), json({ a: 2, b: 1 }), json({ a: 2, b: 2 })];
    const [h1, h2, h3] = await Promise.all([v1, v2, v3].map(sha256Hex));
    const blobs = new Map([
      [h1, v1],
      [h2, v2],
      [h3, v3],
    ]);
    const away = await device(v1, blobs);
    try {
      away.localState.markSynced(path, "config", h1, h1);
      away.configFiles.set(path, { hash: h1, size: v1.byteLength, mtime: 1, prev: null });
      away.sync.seedBaseline();
      // Another device changed `a`, then `b`; only the latest entry arrives.
      away.configFiles.set(path, { hash: h3, size: v3.byteLength, mtime: 3, prev: h2 });
      away.sync.start(new Set(["mainSettings"]));
      await waitFor(() => away.disk.get(path) === v3 || away.configFiles.get(path)?.hash !== h3, {
        label: "reconciled",
      });
      expect(away.configFiles.get(path)?.hash).toBe(h3);
      expect(JSON.parse(new TextDecoder().decode(away.disk.get(path)))).toEqual({ a: 2, b: 2 });
    } finally {
      away.destroy();
    }
  });

  it("merges a publish that lost a race against the version both were built on", async () => {
    const [v1, ours, theirs] = [json({ a: 1, b: 1 }), json({ a: 1, b: 9 }), json({ a: 7, b: 1 })];
    const [h1, hOurs, hTheirs] = await Promise.all([v1, ours, theirs].map(sha256Hex));
    const blobs = new Map([
      [h1, v1],
      [hOurs, ours],
      [hTheirs, theirs],
    ]);
    const racer = await device(ours, blobs);
    try {
      // This device published `ours` over v1 and saw it in its own index...
      racer.localState.markPublished(path, "config", hOurs, h1);
      racer.localState.markSynced(path, "config", hOurs, hOurs);
      racer.configFiles.set(path, { hash: hOurs, size: ours.byteLength, mtime: 2, prev: h1 });
      racer.sync.seedBaseline();
      // ...then last-writer-wins kept another device's concurrent publish.
      racer.configFiles.set(path, { hash: hTheirs, size: theirs.byteLength, mtime: 2, prev: h1 });
      racer.sync.start(new Set(["mainSettings"]));
      await waitFor(() => racer.configFiles.get(path)?.hash !== hTheirs, { label: "merged" });
      expect(JSON.parse(new TextDecoder().decode(racer.disk.get(path)))).toEqual({ a: 7, b: 9 });
    } finally {
      racer.destroy();
    }
  });
});

describe("ConfigSync warm reconcile", () => {
  it("skips disabled config folders and rewrites no unchanged local state", async () => {
    const { plugin } = makeFakePlugin("https://sync.example.com", {
      sessionToken: "token",
      activeVaultId: "vault",
    });
    const bytes = (text: string) => {
      const encoded = new TextEncoder().encode(text);
      return encoded.buffer.slice(encoded.byteOffset, encoded.byteOffset + encoded.byteLength);
    };
    const synced = [
      ".obsidian/app.json",
      ".obsidian/snippets/wide.css",
      ".obsidian/themes/Minimal/theme.css",
    ];
    const disk = new Map<string, ArrayBuffer>(
      [...synced, ".obsidian/plugins/dataview/data.json", ".obsidian/cache/state.json"].map(
        (path) => [path, bytes(path)],
      ),
    );
    const listed: string[] = [];
    Object.assign(plugin.app.vault, {
      configDir: ".obsidian",
      adapter: {
        exists: async (p: string) => disk.has(p),
        stat: async (p: string) => {
          const stored = disk.get(p);
          return stored ? { mtime: 1, ctime: 1, size: stored.byteLength, type: "file" } : null;
        },
        readBinary: async (p: string) => disk.get(p),
        list: async (folder: string) => {
          listed.push(folder);
          const files: string[] = [];
          const folders = new Set<string>();
          for (const path of disk.keys()) {
            if (!path.startsWith(`${folder}/`)) continue;
            const rest = path.slice(folder.length + 1).split("/");
            if (rest.length === 1) files.push(path);
            else folders.add(`${folder}/${rest[0]}`);
          }
          return { files, folders: [...folders] };
        },
      },
    });
    const indexDoc = new Y.Doc();
    const configFiles = indexDoc.getMap<ConfigMeta>("configFiles");
    // The remote profile already holds exactly these files, including one in
    // the disabled plugin category that must stay untouched.
    for (const path of [...synced, ".obsidian/plugins/dataview/data.json"]) {
      const data = disk.get(path)!;
      configFiles.set(path, { hash: await sha256Hex(data), size: data.byteLength, mtime: 1 });
    }
    const remoteBefore = configFiles.toJSON();
    const localState = new LocalSyncState(`config-warm:${freshGuid()}`);
    await localState.whenSynced;
    const sync = new ConfigSync(plugin as unknown as RealtimePlugin, indexDoc, localState);
    // The private persisted doc: its update stream is what an unchanged reconcile costs.
    const internals = localState as unknown as { doc: Y.Doc };
    try {
      sync.seedBaseline();
      const lifecycle = sync as unknown as {
        started: boolean;
        enabledCategories: Set<string>;
        runInitialPull: () => Promise<void>;
      };
      lifecycle.started = true;
      lifecycle.enabledCategories = new Set(["mainSettings", "themesAndSnippets"]);
      await lifecycle.runInitialPull();
      // First pass adopts matching local files as synced.
      for (const path of synced) {
        expect(localState.get(path), path).toMatchObject({ kind: "config", candidate: false });
      }
      expect(localState.has(".obsidian/plugins/dataview/data.json")).toBe(false);

      let updates = 0;
      internals.doc.on("update", () => (updates += 1));
      listed.length = 0;
      expect(await sync.reconcileAll()).toBe(true);
      expect(await sync.reconcileAll()).toBe(true);
      expect(updates).toBe(0);
      expect(new Set(listed)).toEqual(
        new Set([
          ".obsidian",
          ".obsidian/snippets",
          ".obsidian/themes",
          ".obsidian/themes/Minimal",
        ]),
      );
      expect(configFiles.toJSON()).toEqual(remoteBefore);

      // Enabling the plugin category walks its folders again.
      listed.length = 0;
      lifecycle.enabledCategories.add("installedCommunityPlugins");
      expect(await sync.reconcileAll()).toBe(true);
      expect(listed).toEqual(
        expect.arrayContaining([".obsidian/plugins", ".obsidian/plugins/dataview"]),
      );
      expect(listed).not.toContain(".obsidian/cache");
    } finally {
      sync.destroy();
      indexDoc.destroy();
      localState.destroy();
    }
  });
});
