import { describe, it, expect, beforeAll, afterAll, afterEach, vi } from "vitest";
import * as Y from "yjs";
import { RealtimeProvider } from "../../src/sync/RealtimeProvider";
import { BinarySync, type BinaryMeta } from "../../src/BinarySync";
import { getClientToken } from "../../src/sync/clientToken";
import { makeFakePlugin } from "../support/fakePlugin";
import { notices } from "../support/obsidian-mock";
import { startAuthHarness, type AuthHarness } from "../support/authServer";
import { freshGuid, waitFor } from "../support/util";
import { LocalSyncState } from "../../src/localSyncState";
import { sha256Hex } from "../../src/hash";

const binaryModal = vi.hoisted(() => ({
  choice: "local" as "local" | "remote",
  delayMs: 0,
  calls: [] as Array<{ path: string; remoteDeleted: boolean }>,
}));

vi.mock("../../src/BinaryConflictModal", () => ({
  openBinaryConflictModal: async (
    _plugin: unknown,
    info: { path: string; remoteDeleted: boolean },
  ) => {
    binaryModal.calls.push(info);
    if (binaryModal.delayMs > 0) {
      await new Promise((resolve) => setTimeout(resolve, binaryModal.delayMs));
    }
    return binaryModal.choice;
  },
}));

let harness: AuthHarness;
let token: string;
let vaultId: string;

beforeAll(async () => {
  harness = await startAuthHarness();
  token = await harness.loginUser("alice");
  const vault = await harness.createVault(token, "attachments");
  vaultId = vault.id;
}, 180_000);

afterAll(async () => {
  await harness?.stop();
});

afterEach(() => {
  notices.length = 0;
  binaryModal.choice = "local";
  binaryModal.delayMs = 0;
  binaryModal.calls.length = 0;
});

const bytes = (arr: number[]) => new Uint8Array(arr).buffer;
const asArray = (buf: ArrayBuffer | undefined) => (buf ? [...new Uint8Array(buf)] : null);

/** A device: fake vault + an index document/provider + BinarySync. */
function makeDevice(name: string) {
  const { plugin, vault } = makeFakePlugin(harness.authUrl, {
    sessionToken: token,
    activeVaultId: vaultId,
    clientName: name,
  });
  const indexDoc = new Y.Doc();
  const provider = new RealtimeProvider(vaultId, indexDoc, () =>
    getClientToken(plugin as any, vaultId),
  );
  // Record the upload-status transitions the plugin would render.
  const uploadStates: string[] = [];
  plugin.setUploadStatus = (v: string) => uploadStates.push(v);
  // BinarySync needs isTextSyncBusy() (tests can flip it) and recordTrash().
  const trashed: unknown[] = [];
  const vaultSyncStub = {
    busy: false,
    isTextSyncBusy() {
      return this.busy;
    },
    recordTrash(entry: unknown) {
      trashed.push(entry);
    },
  };
  const bs = new BinarySync(plugin as any, vaultSyncStub as any, indexDoc);
  const binaries = indexDoc.getMap<BinaryMeta>("binaries");
  return { plugin, vault, indexDoc, provider, bs, binaries, uploadStates, vaultSyncStub };
}

const synced = (p: RealtimeProvider) =>
  waitFor(() => p.status === "connected", { label: "provider connected" });

describe("BinarySync", () => {
  it("uses the durable hash baseline when a deleted remote map entry is already absent", async () => {
    const { plugin, vault } = makeFakePlugin(harness.authUrl, {
      sessionToken: token,
      activeVaultId: vaultId,
    });
    const data = bytes([5, 4, 3, 2, 1]);
    vault.binaries.set("persisted-delete.bin", data);
    const hash = await sha256Hex(data);
    const localState = new LocalSyncState(`binary-delete:${freshGuid()}`);
    await localState.whenSynced;
    localState.markSynced("persisted-delete.bin", "binary", hash, hash);
    const indexDoc = new Y.Doc();
    const bs = new BinarySync(
      plugin as any,
      {
        isTextSyncBusy: () => false,
        recordTrash: () => {},
      } as any,
      indexDoc,
      localState,
    );
    try {
      bs.seedBaseline();
      await bs.reconcileAll(["persisted-delete.bin"]);
      expect(vault.binaries.has("persisted-delete.bin")).toBe(false);
    } finally {
      bs.destroy();
      indexDoc.destroy();
      localState.destroy();
    }
  });

  it("restores a prioritized attachment missing locally before the startup pass", async () => {
    const { plugin, vault } = makeFakePlugin(harness.authUrl, {
      sessionToken: token,
      activeVaultId: vaultId,
    });
    const data = bytes([9, 9, 9]);
    const hash = await sha256Hex(data);
    const localState = new LocalSyncState(`binary-priority:${freshGuid()}`);
    await localState.whenSynced;
    // Synced on an earlier run; the file is now missing locally (e.g. evicted).
    localState.markSynced("pic.png", "binary", hash, hash);
    const indexDoc = new Y.Doc();
    const binaries = indexDoc.getMap<BinaryMeta>("binaries");
    binaries.set("pic.png", { hash, size: 3 });
    plugin.auth.getBlob = async () => data;
    const bs = new BinarySync(
      plugin as any,
      { isTextSyncBusy: () => false, recordTrash: () => {} } as any,
      indexDoc,
      localState,
    );
    try {
      bs.seedBaseline();
      // An open note or canvas asks for the image before reconcileAll runs.
      bs.prioritizePaths(["pic.png"]);
      await waitFor(() => vault.binaries.has("pic.png") || !binaries.has("pic.png"), {
        label: "prioritized reconcile settled",
      });
      expect(binaries.get("pic.png")?.hash).toBe(hash);
      expect(asArray(vault.binaries.get("pic.png"))).toEqual([9, 9, 9]);
    } finally {
      bs.destroy();
      indexDoc.destroy();
      localState.destroy();
    }
  });

  it("downloads startup attachments in parallel within count and byte limits", async () => {
    const { plugin, vault } = makeFakePlugin(harness.authUrl, {
      sessionToken: token,
      activeVaultId: vaultId,
    });
    const indexDoc = new Y.Doc();
    const binaries = indexDoc.getMap<BinaryMeta>("binaries");
    const blobs = new Map<string, ArrayBuffer>();
    const addRemote = async (path: string, size: number) => {
      const data = new TextEncoder().encode(path).buffer as ArrayBuffer;
      const hash = await sha256Hex(data);
      blobs.set(hash, data);
      binaries.set(path, { hash, size });
    };
    for (let i = 0; i < 20; i++) await addRemote(`small/${i}.png`, 1024);
    for (let i = 0; i < 3; i++) await addRemote(`large/${i}.mov`, 20 * 1024 * 1024);

    let inFlight = 0;
    let largeInFlight = 0;
    let peak = 0;
    let largePeak = 0;
    plugin.auth.getBlob = async (_vault: string, path: string, hash: string) => {
      const large = path.startsWith("large/");
      inFlight++;
      if (large) largeInFlight++;
      peak = Math.max(peak, inFlight);
      largePeak = Math.max(largePeak, largeInFlight);
      await new Promise((resolve) => setTimeout(resolve, 20));
      inFlight--;
      if (large) largeInFlight--;
      return blobs.get(hash)!;
    };
    const bs = new BinarySync(
      plugin as any,
      { isTextSyncBusy: () => false, recordTrash: () => {} } as any,
      indexDoc,
    );
    try {
      bs.seedBaseline();
      await bs.reconcileAll([]);
      expect(vault.binaries.size).toBe(23);
      // Downloads overlap, up to the pool size.
      expect(peak).toBeGreaterThan(1);
      expect(peak).toBeLessThanOrEqual(6);
      // Two 20 MB files exceed the in-flight byte budget, so they run alone.
      expect(largePeak).toBe(1);
    } finally {
      bs.destroy();
      indexDoc.destroy();
    }
  });

  it("uploads a local binary and a peer downloads identical bytes", async () => {
    const A = makeDevice("A");
    const B = makeDevice("B");
    try {
      await synced(A.provider);
      await synced(B.provider);

      // B is online with its observer armed but has no local files.
      B.bs.seedBaseline();
      await B.bs.reconcileAll([]);

      // A creates a binary file and reconciles it (uploads + publishes hash).
      A.vault.binaries.set("img.png", bytes([1, 2, 3, 4, 5]));
      A.bs.seedBaseline();
      await A.bs.reconcileAll(["img.png"]);

      // The hash mapping propagates and B pulls the bytes from the blob store.
      await waitFor(() => B.vault.binaries.has("img.png"), {
        label: "B downloaded the blob",
      });
      expect(asArray(B.vault.binaries.get("img.png"))).toEqual([1, 2, 3, 4, 5]);

      // The published map entry carries the size.
      await waitFor(() => B.binaries.get("img.png")?.size === 5, { label: "size published" });
    } finally {
      A.bs.destroy();
      A.provider.destroy();
      A.indexDoc.destroy();
      B.bs.destroy();
      B.provider.destroy();
      B.indexDoc.destroy();
    }
  });

  it("leaves device-excluded binaries in the shared index without transferring bytes", async () => {
    const A = makeDevice("filter-source");
    const { plugin, vault } = makeFakePlugin(harness.authUrl, {
      sessionToken: token,
      activeVaultId: vaultId,
      clientName: "filter-target",
    });
    const indexDoc = new Y.Doc();
    const provider = new RealtimeProvider(vaultId, indexDoc, () =>
      getClientToken(plugin as any, vaultId),
    );
    const bs = new BinarySync(
      plugin as any,
      {
        isTextSyncBusy: () => false,
        recordTrash: () => {},
      } as any,
      indexDoc,
      undefined,
      () => false,
    );
    const binaries = indexDoc.getMap<BinaryMeta>("binaries");
    try {
      await synced(A.provider);
      await synced(provider);
      await bs.reconcileAll([]);

      A.vault.binaries.set("excluded.png", bytes([1, 2, 3]));
      A.bs.seedBaseline();
      await A.bs.reconcileAll(["excluded.png"]);
      await waitFor(() => binaries.has("excluded.png"), {
        label: "excluded binary metadata received",
      });
      await new Promise((resolve) => setTimeout(resolve, 100));

      expect(vault.binaries.has("excluded.png")).toBe(false);
      expect(binaries.has("excluded.png")).toBe(true);
    } finally {
      A.bs.destroy();
      A.provider.destroy();
      A.indexDoc.destroy();
      bs.destroy();
      provider.destroy();
      indexDoc.destroy();
    }
  });

  it("signals uploading then idle around an upload", async () => {
    const A = makeDevice("A");
    try {
      await synced(A.provider);
      A.vault.binaries.set("small.bin", bytes([4, 5, 6, 7, 8, 9]));
      A.bs.seedBaseline();
      await A.bs.reconcileAll(["small.bin"]);

      await waitFor(() => A.uploadStates.at(-1) === "idle", { label: "upload finished" });
      expect(A.uploadStates).toContain("uploading");
      expect(A.uploadStates.at(-1)).toBe("idle");
      await waitFor(() => A.binaries.has("small.bin"), { label: "published" });
    } finally {
      A.bs.destroy();
      A.provider.destroy();
      A.indexDoc.destroy();
    }
  });

  it("holds queued uploads while mobile sync is paused and resumes them", async () => {
    const A = makeDevice("mobile-paused");
    try {
      await synced(A.provider);
      A.provider.disconnect();
      vi.useFakeTimers();
      A.bs.setPaused(true);
      const blobExists = vi.spyOn(A.plugin.auth, "blobExists");
      A.vault.binaries.set("queued.bin", bytes([8, 6, 7, 5, 3, 0, 9]));
      A.bs.onLocalChanged("queued.bin");

      const reconcile = (A.bs as any).chains.get("queued.bin");
      expect(reconcile).toBeDefined();
      await reconcile;
      expect((A.bs as any).deferredReconciles.has("queued.bin")).toBe(true);
      await vi.advanceTimersByTimeAsync(60_000);
      expect((A.bs as any).deferredReconciles.has("queued.bin")).toBe(true);
      expect(blobExists).not.toHaveBeenCalled();
      expect(A.binaries.has("queued.bin")).toBe(false);
      expect(A.uploadStates).not.toContain("uploading");

      vi.useRealTimers();
      A.bs.setPaused(false);
      await waitFor(() => A.binaries.has("queued.bin"), {
        label: "paused upload resumed",
      });
      expect(A.uploadStates.at(-1)).toBe("idle");
    } finally {
      vi.useRealTimers();
      A.bs.destroy();
      A.provider.destroy();
      A.indexDoc.destroy();
    }
  });

  it("requeues an upload paused during the blob existence check", async () => {
    const A = makeDevice("pause-during-upload");
    let release!: (exists: boolean) => void;
    let checks = 0;
    let checkedResolve!: () => void;
    const checked = new Promise<void>((resolve) => {
      checkedResolve = resolve;
    });
    A.plugin.auth.blobExists = vi.fn(async () => {
      checks += 1;
      checkedResolve();
      if (checks > 1) return true;
      return new Promise<boolean>((resolve) => {
        release = resolve;
      });
    });
    try {
      await synced(A.provider);
      A.vault.binaries.set("paused-flight.bin", bytes([9, 8, 7]));
      A.bs.onLocalChanged("paused-flight.bin");
      await checked;
      A.bs.setPaused(true);
      release(true);
      await new Promise((resolve) => setTimeout(resolve, 20));
      expect(A.binaries.has("paused-flight.bin")).toBe(false);

      A.bs.setPaused(false);
      await waitFor(() => A.binaries.has("paused-flight.bin"), {
        label: "in-flight paused upload resumed",
      });
    } finally {
      A.bs.destroy();
      A.provider.destroy();
      A.indexDoc.destroy();
    }
  });

  it("preserves startup pull semantics across a mobile pause", async () => {
    const A = makeDevice("pause-source");
    const B = makeDevice("pause-target");
    try {
      await synced(A.provider);
      await synced(B.provider);
      A.vault.binaries.set("startup.bin", bytes([1, 3, 3, 7]));
      A.bs.seedBaseline();
      await A.bs.reconcileAll(["startup.bin"]);
      await waitFor(() => B.binaries.has("startup.bin"), {
        label: "paused target received binary metadata",
      });

      B.bs.seedBaseline();
      B.bs.setPaused(true);
      await B.bs.reconcileAll([]);
      expect(B.vault.binaries.has("startup.bin")).toBe(false);
      expect(B.binaries.has("startup.bin")).toBe(true);

      B.bs.setPaused(false);
      await waitFor(() => B.vault.binaries.has("startup.bin"), {
        label: "startup binary pulled after resume",
      });
      expect(asArray(B.vault.binaries.get("startup.bin"))).toEqual([1, 3, 3, 7]);
      expect(B.binaries.has("startup.bin")).toBe(true);
    } finally {
      A.bs.destroy();
      A.provider.destroy();
      A.indexDoc.destroy();
      B.bs.destroy();
      B.provider.destroy();
      B.indexDoc.destroy();
    }
  });

  it("restores a missing local binary from a persisted remote baseline", async () => {
    const A = makeDevice("A");
    const B = makeDevice("B");
    try {
      await synced(A.provider);
      await synced(B.provider);

      A.vault.binaries.set("image.png", bytes([7, 6, 5, 4]));
      A.bs.seedBaseline();
      await A.bs.reconcileAll(["image.png"]);

      await waitFor(() => B.binaries.has("image.png"), { label: "B index has image" });

      // Simulate a migrated IndexedDB baseline with no local vault file yet. This
      // must pull the blob, not interpret absence as a local delete.
      B.bs.seedBaseline();
      await B.bs.reconcileAll([]);

      await waitFor(() => B.vault.binaries.has("image.png"), { label: "B restored image" });
      expect(asArray(B.vault.binaries.get("image.png"))).toEqual([7, 6, 5, 4]);
      expect(B.binaries.has("image.png")).toBe(true);
    } finally {
      A.bs.destroy();
      A.provider.destroy();
      A.indexDoc.destroy();
      B.bs.destroy();
      B.provider.destroy();
      B.indexDoc.destroy();
    }
  });

  it("retries a failed remote disk write without advancing the baseline", async () => {
    const A = makeDevice("A");
    const B = makeDevice("B");
    try {
      await synced(A.provider);
      await synced(B.provider);

      A.vault.binaries.set("retry.png", bytes([8, 6, 7, 5, 3, 0, 9]));
      A.bs.seedBaseline();
      await A.bs.reconcileAll(["retry.png"]);
      await waitFor(() => B.binaries.has("retry.png"), { label: "B received retry metadata" });

      const originalCreate = B.plugin.app.vault.createBinary.bind(B.plugin.app.vault);
      let failedRetryWrite = false;
      const create = vi
        .spyOn(B.plugin.app.vault, "createBinary")
        .mockImplementation(async (path, data) => {
          if (path === "retry.png" && !failedRetryWrite) {
            failedRetryWrite = true;
            throw new Error("simulated disk failure");
          }
          return originalCreate(path, data);
        });
      B.bs.seedBaseline();
      await B.bs.reconcileAll([]);
      expect(B.vault.binaries.has("retry.png")).toBe(false);
      expect(B.binaries.has("retry.png")).toBe(true);

      await waitFor(() => B.vault.binaries.has("retry.png"), {
        timeout: 10_000,
        label: "failed write retried",
      });
      expect(create.mock.calls.filter(([path]) => path === "retry.png")).toHaveLength(2);
      expect(asArray(B.vault.binaries.get("retry.png"))).toEqual([8, 6, 7, 5, 3, 0, 9]);
    } finally {
      A.bs.destroy();
      A.provider.destroy();
      A.indexDoc.destroy();
      B.bs.destroy();
      B.provider.destroy();
      B.indexDoc.destroy();
    }
  });

  it("reports a large deferred file as pending while text sync is busy", async () => {
    const A = makeDevice("A");
    try {
      await synced(A.provider);
      // Pretend notes are actively syncing so large uploads are held back.
      A.vaultSyncStub.busy = true;

      // A >5MB file is queued but must not transfer yet.
      const big = new Uint8Array(6 * 1024 * 1024);
      big[0] = 1;
      A.vault.binaries.set("huge.bin", big.buffer);
      A.bs.seedBaseline();
      await A.bs.reconcileAll(["huge.bin"]);

      await waitFor(() => A.uploadStates.at(-1) === "pending", { label: "deferred -> pending" });
      expect(A.uploadStates).not.toContain("uploading");
      expect(A.binaries.has("huge.bin")).toBe(false); // not published while deferred

      // Once notes go quiet, it uploads and settles to idle.
      A.vaultSyncStub.busy = false;
      await waitFor(() => A.uploadStates.at(-1) === "idle", { timeout: 15_000, label: "drains" });
      expect(A.uploadStates).toContain("uploading");
      expect(A.binaries.has("huge.bin")).toBe(true);
    } finally {
      A.bs.destroy();
      A.provider.destroy();
      A.indexDoc.destroy();
    }
  });

  it("prioritizes a Canvas-referenced large file ahead of the text-busy deferral", async () => {
    const A = makeDevice("A");
    try {
      await synced(A.provider);
      A.vaultSyncStub.busy = true;
      const big = new Uint8Array(6 * 1024 * 1024);
      big[0] = 7;
      A.vault.binaries.set("canvas-image.png", big.buffer);
      A.bs.seedBaseline();
      A.bs.prioritizePaths(["canvas-image.png", "canvas-image.png"]);
      await A.bs.reconcileAll(["canvas-image.png"]);
      await waitFor(() => A.binaries.has("canvas-image.png"), {
        timeout: 15_000,
        label: "urgent Canvas attachment published",
      });
      expect(A.uploadStates).toContain("uploading");
    } finally {
      A.bs.destroy();
      A.indexDoc.destroy();
    }
  });

  it("does not rehash an agreed Canvas attachment on repeated priority requests", async () => {
    const A = makeDevice("A");
    try {
      await synced(A.provider);
      A.vault.binaries.set("repeated.png", bytes([1, 3, 5, 7]));
      A.bs.seedBaseline();
      const readBinary = vi.spyOn(A.plugin.app.vault, "readBinary");
      A.bs.prioritizePaths(["repeated.png"]);
      A.bs.prioritizePaths(["repeated.png"]);
      A.bs.prioritizePaths(["repeated.png"]);
      await waitFor(() => A.binaries.has("repeated.png"), { label: "priority upload published" });
      await waitFor(() => A.uploadStates.at(-1) === "idle", { label: "priority upload settled" });
      expect(readBinary).toHaveBeenCalledTimes(2);
    } finally {
      A.bs.destroy();
      A.provider.destroy();
      A.indexDoc.destroy();
    }
  });

  it("applies a clean remote delete to a peer's local attachment", async () => {
    const A = makeDevice("A");
    const B = makeDevice("B");
    try {
      await synced(A.provider);
      await synced(B.provider);
      B.bs.seedBaseline();
      await B.bs.reconcileAll([]);

      A.vault.binaries.set("doc.pdf", bytes([9, 8, 7]));
      A.bs.seedBaseline();
      await A.bs.reconcileAll(["doc.pdf"]);
      await waitFor(() => B.vault.binaries.has("doc.pdf"), { label: "B has the file" });

      // A deletes the file locally → the index entry is removed. B has no
      // unsynced local changes, so the remote delete is authoritative and B
      // removes its local copy too.
      A.vault.binaries.delete("doc.pdf");
      A.bs.onLocalDeleted("doc.pdf");

      await waitFor(() => !B.binaries.has("doc.pdf"), { label: "index entry gone" });
      await waitFor(() => !B.vault.binaries.has("doc.pdf"), { label: "B applied the delete" });
    } finally {
      A.bs.destroy();
      A.provider.destroy();
      A.indexDoc.destroy();
      B.bs.destroy();
      B.provider.destroy();
      B.indexDoc.destroy();
    }
  });

  it("keeps a peer attachment when remote delete races local edits (conflict)", async () => {
    binaryModal.delayMs = 1_000;
    const A = makeDevice("A");
    const B = makeDevice("B");
    try {
      await synced(A.provider);
      await synced(B.provider);
      B.bs.seedBaseline();
      await B.bs.reconcileAll([]);

      A.vault.binaries.set("doc.pdf", bytes([9, 8, 7]));
      A.bs.seedBaseline();
      await A.bs.reconcileAll(["doc.pdf"]);
      await waitFor(() => B.vault.binaries.has("doc.pdf"), { label: "B has the file" });

      // B edits the file locally (now diverged from the synced baseline)...
      B.vault.binaries.set("doc.pdf", bytes([1, 2, 3, 4]));

      // ...while A deletes it remotely. The delete is no longer clean, so B
      // keeps its local copy and surfaces a conflict for the user to resolve.
      A.vault.binaries.delete("doc.pdf");
      A.bs.onLocalDeleted("doc.pdf");

      await waitFor(() => !B.binaries.has("doc.pdf"), { label: "index entry gone" });
      await new Promise((resolve) => setTimeout(resolve, 200));
      expect(B.vault.binaries.has("doc.pdf")).toBe(true);
    } finally {
      A.bs.destroy();
      A.provider.destroy();
      A.indexDoc.destroy();
      B.bs.destroy();
      B.provider.destroy();
      B.indexDoc.destroy();
    }
  });

  it("re-downloads when a peer replaces the file (clean remote update)", async () => {
    const A = makeDevice("A");
    const B = makeDevice("B");
    try {
      await synced(A.provider);
      await synced(B.provider);
      B.bs.seedBaseline();
      await B.bs.reconcileAll([]);

      A.vault.binaries.set("photo.jpg", bytes([1, 1, 1]));
      A.bs.seedBaseline();
      await A.bs.reconcileAll(["photo.jpg"]);
      await waitFor(() => asArray(B.vault.binaries.get("photo.jpg"))?.[0] === 1, {
        label: "B has v1",
      });

      // A replaces the content; B (unchanged locally) should pull the new bytes.
      A.vault.binaries.set("photo.jpg", bytes([2, 2, 2, 2]));
      A.bs.onLocalChanged("photo.jpg");

      await waitFor(() => asArray(B.vault.binaries.get("photo.jpg"))?.length === 4, {
        label: "B has v2",
      });
      expect(asArray(B.vault.binaries.get("photo.jpg"))).toEqual([2, 2, 2, 2]);
    } finally {
      A.bs.destroy();
      A.provider.destroy();
      A.indexDoc.destroy();
      B.bs.destroy();
      B.provider.destroy();
      B.indexDoc.destroy();
    }
  });

  it("re-prompts and preserves the latest remote binary when it changes behind the modal", async () => {
    const A = makeDevice("A");
    const B = makeDevice("B");
    try {
      await synced(A.provider);
      await synced(B.provider);
      B.bs.seedBaseline();
      await B.bs.reconcileAll([]);

      A.vault.binaries.set("race.bin", bytes([1]));
      A.bs.seedBaseline();
      await A.bs.reconcileAll(["race.bin"]);
      await waitFor(() => asArray(B.vault.binaries.get("race.bin"))?.[0] === 1, {
        label: "binary conflict baseline downloaded",
      });

      B.vault.binaries.set("race.bin", bytes([9, 9, 9]));
      binaryModal.delayMs = 300;
      A.vault.binaries.set("race.bin", bytes([2, 2]));
      A.bs.onLocalChanged("race.bin");
      await waitFor(() => binaryModal.calls.length === 1, {
        label: "first binary conflict prompt opened",
      });

      A.vault.binaries.set("race.bin", bytes([3, 3]));
      A.bs.onLocalChanged("race.bin");
      await waitFor(() => binaryModal.calls.length === 2, {
        timeout: 10_000,
        label: "latest binary conflict prompted again",
      });

      const localHash = await sha256Hex(bytes([9, 9, 9]));
      await waitFor(() => B.binaries.get("race.bin")?.hash === localHash, {
        timeout: 10_000,
        label: "local binary choice published after revalidation",
      });
      const copies = [...B.vault.binaries.keys()].filter((path) =>
        /\(conflicted copy Remote /.test(path),
      );
      expect(copies).toHaveLength(1);
      expect(asArray(B.vault.binaries.get(copies[0]))).toEqual([3, 3]);
    } finally {
      A.bs.destroy();
      A.provider.destroy();
      A.indexDoc.destroy();
      B.bs.destroy();
      B.provider.destroy();
      B.indexDoc.destroy();
    }
  });

  it("keeps an upload that fails while offline and completes it after reconnecting", async () => {
    const { plugin, vault } = makeFakePlugin(harness.authUrl, {
      sessionToken: token,
      activeVaultId: vaultId,
    });
    const data = bytes([4, 2, 4, 2]);
    vault.binaries.set("offline.bin", data);
    const indexDoc = new Y.Doc();
    const bs = new BinarySync(
      plugin as any,
      { isTextSyncBusy: () => false, recordTrash: () => {} } as any,
      indexDoc,
    );
    let offline = true;
    let attempts = 0;
    const uploaded: string[] = [];
    plugin.auth.blobExists = async () => {
      attempts++;
      if (offline) throw new Error("net::ERR_INTERNET_DISCONNECTED");
      return false;
    };
    plugin.auth.putBlob = async (_vault: string, _path: string, hash: string) => {
      uploaded.push(hash);
    };
    try {
      await bs.reconcileAll([]);
      bs.onLocalChanged("offline.bin");
      await waitFor(() => attempts === 1, { label: "first upload attempt" });
      // Well past the old five-attempt limit.
      for (let expected = 2; expected <= 8; expected++) {
        bs.retryNow();
        await waitFor(() => attempts === expected, { label: `upload attempt ${expected}` });
      }
      expect(notices.some((n) => n.includes("failed to upload"))).toBe(false);

      offline = false;
      bs.retryNow();
      const hash = await sha256Hex(data);
      await waitFor(
        () => indexDoc.getMap<BinaryMeta>("binaries").get("offline.bin")?.hash === hash,
        {
          label: "offline upload published after reconnect",
        },
      );
      expect(uploaded).toEqual([hash]);
    } finally {
      bs.destroy();
      indexDoc.destroy();
    }
  });

  it("never writes downloaded bytes that do not match the blob hash", async () => {
    const { plugin, vault } = makeFakePlugin(harness.authUrl, {
      sessionToken: token,
      activeVaultId: vaultId,
    });
    const good = bytes([1, 2, 3, 4, 5]);
    const hash = await sha256Hex(good);
    const indexDoc = new Y.Doc();
    indexDoc.getMap<BinaryMeta>("binaries").set("torn.bin", { hash, size: 5 });
    const bs = new BinarySync(
      plugin as any,
      { isTextSyncBusy: () => false, recordTrash: () => {} } as any,
      indexDoc,
    );
    let downloads = 0;
    plugin.auth.getBlob = async () => {
      downloads++;
      return bytes([1, 2]); // a torn copy of the blob
    };
    try {
      await bs.reconcileAll([]);
      await waitFor(() => downloads >= 1, { label: "download attempted" });
      await new Promise((r) => setTimeout(r, 300));
      expect(vault.binaries.has("torn.bin")).toBe(false);

      plugin.auth.getBlob = async () => {
        downloads++;
        return good;
      };
      bs.retryNow();
      await waitFor(() => asArray(vault.binaries.get("torn.bin")) !== null, {
        label: "verified bytes written",
      });
      expect(asArray(vault.binaries.get("torn.bin"))).toEqual([1, 2, 3, 4, 5]);
    } finally {
      bs.destroy();
      indexDoc.destroy();
    }
  });
});
