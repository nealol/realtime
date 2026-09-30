import { createHash } from "node:crypto";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { ConflictError, RealtimeClient, type VaultHandle } from "../../src/index";
import { startAuthHarness, type AuthHarness } from "../support/harness";

let harness: AuthHarness;
let client: RealtimeClient;
let vault: VaultHandle;

beforeAll(async () => {
  harness = await startAuthHarness();
  client = new RealtimeClient({
    baseUrl: harness.authUrl,
    token: await harness.loginUser("alice"),
  });
  vault = client.vault((await client.vaults.create("Files Vault")).id);
});

afterAll(async () => {
  await harness?.stop();
});

const PNG_BYTES = new Uint8Array([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 1, 2, 3, 4]);

describe("attachments", () => {
  it("uploads, lists, reads, moves, and deletes", async () => {
    const uploaded = await vault.attachments.upload("img/pic 1.png", PNG_BYTES);
    expect(uploaded.path).toBe("img/pic 1.png");
    expect(uploaded.size).toBe(PNG_BYTES.length);
    expect(uploaded.hash).toMatch(/^[0-9a-f]{64}$/);

    expect(await vault.attachments.exists("img/pic 1.png")).toBe(true);
    expect(await vault.attachments.exists("img/nope.png")).toBe(false);

    const bytes = await vault.attachments.read("img/pic 1.png");
    expect(Array.from(bytes)).toEqual(Array.from(PNG_BYTES));

    const list = await vault.attachments.list();
    expect(list.map((a) => a.path)).toContain("img/pic 1.png");

    const moved = await vault.attachments.move("img/pic 1.png", "archive/pic.png");
    expect(moved.path).toBe("archive/pic.png");

    await vault.attachments.delete("archive/pic.png");
    expect(await vault.attachments.exists("archive/pic.png")).toBe(false);
  });

  it("refuses uploads whose precondition no longer holds", async () => {
    const version = (tail: number) => new Uint8Array([...PNG_BYTES, tail]);
    const first = await vault.attachments.upload("img/guarded.png", version(1), {
      ifNoneMatch: "*",
    });
    await expect(
      vault.attachments.upload("img/guarded.png", version(2), { ifNoneMatch: "*" }),
    ).rejects.toThrow(ConflictError);

    const second = await vault.attachments.upload("img/guarded.png", version(2), {
      ifMatch: first.hash,
    });
    await expect(
      vault.attachments.upload("img/guarded.png", version(3), { ifMatch: first.hash }),
    ).rejects.toThrow(ConflictError);

    const listed = (await vault.attachments.list()).find((a) => a.path === "img/guarded.png");
    expect(listed?.hash).toBe(second.hash);
  });

  it("mints public upload links", async () => {
    const link = await vault.attachments.createUploadLink({ landingDir: "inbox" });
    expect(link.uploadUrl).toContain("/upload");
    expect(link.landingDir).toBe("inbox");
    expect(link.expiresAt).toBeGreaterThan(Date.now());
  });
});

describe("blobs", () => {
  it("stores and retrieves content-addressed blobs", async () => {
    const bytes = new TextEncoder().encode("blob-payload");
    const hash = createHash("sha256").update(bytes).digest("hex");

    expect(await vault.blobs.exists(hash)).toBe(false);
    await vault.blobs.put(hash, bytes);
    expect(await vault.blobs.exists(hash)).toBe(true);
    expect(new TextDecoder().decode(await vault.blobs.get(hash))).toBe("blob-payload");
  });

  it("rejects uploads whose bytes do not match the hash", async () => {
    const wrongHash = "0".repeat(64);
    await expect(vault.blobs.put(wrongHash, new TextEncoder().encode("x"))).rejects.toThrow();
  });
});

describe("storage", () => {
  it("reports usage and garbage-collects orphaned blobs", async () => {
    const bytes = new TextEncoder().encode("orphan-blob");
    const hash = createHash("sha256").update(bytes).digest("hex");
    await vault.blobs.put(hash, bytes);

    const usage = await vault.storage.usage();
    expect(usage.blobsCurrentBytes + usage.blobsPreviousBytes).toBeGreaterThan(0);

    // A fresh upload may be a publish still in flight: the default grace keeps it.
    await vault.storage.gcBlobs();
    expect(await vault.blobs.exists(hash)).toBe(true);

    const gc = await vault.storage.gcBlobs({ minAgeSeconds: 0 });
    expect(gc.removed).toBeGreaterThanOrEqual(1);
    expect(await vault.blobs.exists(hash)).toBe(false);
  });
});

describe("backup config", () => {
  it("reads the unconfigured state and round-trips a config", async () => {
    const initial = await vault.backup.get();
    expect(initial.configured).toBe(false);

    const put = await vault.backup.put({
      remoteUrl: "https://example.com/repo.git",
      authMethod: "https",
      httpsToken: "secret",
      enabled: false,
    });
    expect(put.configured).toBe(true);
    expect(put.hasHttpsToken).toBe(true);
    expect(put.remoteUrl).toBe("https://example.com/repo.git");

    await vault.backup.delete();
    expect((await vault.backup.get()).configured).toBe(false);
  });
});
