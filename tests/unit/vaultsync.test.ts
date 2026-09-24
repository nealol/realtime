import { describe, it, expect, beforeAll, afterAll, beforeEach, vi } from "vitest";
import { mkdtempSync, rmSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";
import * as Y from "yjs";
import { Platform, TFile } from "obsidian";
import { RealtimeProvider } from "../../src/sync/RealtimeProvider";
import { VaultSync, shouldSyncCanvasBinaryPath } from "../../src/VaultSync";
import { getClientToken } from "../../src/sync/clientToken";
import { sha256Hex, sha256Text } from "../../src/hash";
import { startAuthHarness, type AuthHarness } from "../support/authServer";
import { makeFakePlugin, type FakePlugin } from "../support/fakePlugin";
import { waitFor } from "../support/util";
import { CompatibilityError } from "../../src/caps";
import { CanvasDocument } from "../../src/CanvasDocument";
import { parseCanvas, reconcileCanvas, serializeCanvas } from "../../src/structured/canvas";
import { toValue } from "../../src/structured/reconcile";
import { Peer } from "../support/peer";

const bootstrapModal = vi.hoisted(() => ({
  choice: "local" as "local" | "remote",
  delayMs: 0,
  calls: [] as Array<{ path: string; localContent: string; remoteContent: string }>,
}));

const binaryModal = vi.hoisted(() => ({
  choice: "local" as "local" | "remote",
  calls: [] as Array<{ path: string; remoteDeleted: boolean }>,
}));

vi.mock("../../src/TextConflictModal", () => ({
  openTextConflictModal: async (
    _plugin: unknown,
    info: { path: string; localContent: string; remoteContent: string },
  ) => {
    bootstrapModal.calls.push(info);
    if (bootstrapModal.delayMs > 0) {
      await new Promise((resolve) => setTimeout(resolve, bootstrapModal.delayMs));
    }
    return bootstrapModal.choice;
  },
}));

vi.mock("../../src/BinaryConflictModal", () => ({
  openBinaryConflictModal: async (
    _plugin: unknown,
    info: { path: string; remoteDeleted: boolean },
  ) => {
    binaryModal.calls.push(info);
    return binaryModal.choice;
  },
}));

let harness: AuthHarness;
let aliceToken: string;
let storageDir: string;

type NoteResponse = {
  path: string;
  guid: string;
  content: string;
};

async function createNote(vaultId: string, path: string, content: string): Promise<NoteResponse> {
  const response = await fetch(`${harness.authUrl}/api/vaults/${vaultId}/notes`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${aliceToken}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ path, content }),
  });
  expect(response.status).toBe(200);
  return (await response.json()) as NoteResponse;
}

async function listNotes(vaultId: string): Promise<NoteResponse[]> {
  const response = await fetch(`${harness.authUrl}/api/vaults/${vaultId}/notes`, {
    headers: { Authorization: `Bearer ${aliceToken}` },
  });
  expect(response.status).toBe(200);
  return (await response.json()) as NoteResponse[];
}

async function readNoteStatus(vaultId: string, path: string): Promise<number> {
  const response = await fetch(
    `${harness.authUrl}/api/vaults/${vaultId}/notes/${path
      .split("/")
      .map(encodeURIComponent)
      .join("/")}`,
    { headers: { Authorization: `Bearer ${aliceToken}` } },
  );
  return response.status;
}

async function readNote(vaultId: string, path: string): Promise<NoteResponse> {
  const response = await fetch(
    `${harness.authUrl}/api/vaults/${vaultId}/notes/${path
      .split("/")
      .map(encodeURIComponent)
      .join("/")}`,
    { headers: { Authorization: `Bearer ${aliceToken}` } },
  );
  expect(response.status).toBe(200);
  return (await response.json()) as NoteResponse;
}

async function replaceNote(vaultId: string, path: string, content: string): Promise<NoteResponse> {
  const response = await fetch(
    `${harness.authUrl}/api/vaults/${vaultId}/notes/${path
      .split("/")
      .map(encodeURIComponent)
      .join("/")}`,
    {
      method: "PUT",
      headers: {
        Authorization: `Bearer ${aliceToken}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ content }),
    },
  );
  expect(response.status).toBe(200);
  return (await response.json()) as NoteResponse;
}

async function deleteNote(vaultId: string, path: string): Promise<void> {
  const response = await fetch(
    `${harness.authUrl}/api/vaults/${vaultId}/notes/${path
      .split("/")
      .map(encodeURIComponent)
      .join("/")}`,
    {
      method: "DELETE",
      headers: { Authorization: `Bearer ${aliceToken}` },
    },
  );
  expect(response.status).toBe(200);
}

beforeAll(async () => {
  storageDir = mkdtempSync(join(tmpdir(), "realtime-vaultsync-"));
  harness = await startAuthHarness({ crdtStorageDir: storageDir });
  aliceToken = await harness.loginUser("alice");
}, 180_000);

afterAll(async () => {
  await harness?.stop();
  if (storageDir) rmSync(storageDir, { recursive: true, force: true });
});

beforeEach(() => {
  (Platform as any).isMobile = false;
  bootstrapModal.choice = "local";
  bootstrapModal.delayMs = 0;
  bootstrapModal.calls.length = 0;
  binaryModal.choice = "local";
  binaryModal.calls.length = 0;
});

/** A bare peer onto the shared vault index (the path -> guid `files` map). */
function makeIndexPeer(plugin: FakePlugin, vaultId: string) {
  const doc = new Y.Doc();
  const provider = new RealtimeProvider(vaultId, doc, () => getClientToken(plugin as any, vaultId));
  return { doc, files: doc.getMap<string>("files"), provider };
}

describe("VaultSync index", () => {
  it("preserves both versions when a fresh local path collides with a remote note", async () => {
    const vault = await harness.createVault(aliceToken, "same-path-bootstrap");
    const remote = await createNote(vault.id, "shared.md", "remote existing content");
    const local = makeFakePlugin(harness.authUrl, {
      sessionToken: aliceToken,
      activeVaultId: vault.id,
      clientName: "Local Device",
    });
    local.vault.files.set(remote.path, "unrelated local content");

    const sync = new VaultSync(local.plugin as any);
    (local.plugin as any).vaultSync = sync;
    try {
      await waitFor(() => bootstrapModal.calls.length === 1, {
        timeout: 20_000,
        label: "same-path collision surfaced",
      });
      await waitFor(
        async () => (await readNote(vault.id, remote.path)).content === "unrelated local content",
        {
          timeout: 20_000,
          label: "selected local version became canonical",
        },
      );
      const copies = [...local.vault.files.keys()].filter((path) =>
        /\(conflicted copy Remote /.test(path),
      );
      expect(copies).toHaveLength(1);
      expect(local.vault.files.get(copies[0])).toBe("remote existing content");
      expect(bootstrapModal.calls[0]).toEqual({
        path: remote.path,
        localContent: "unrelated local content",
        remoteContent: "remote existing content",
      });
    } finally {
      sync.destroy();
    }
  });

  it("adopts the remote note when a plugin created the same note locally from a template", async () => {
    const vault = await harness.createVault(aliceToken, "template-remote-superset");
    const remote = await createNote(
      vault.id,
      "Daily/2026-09-24.md",
      "# 2026-09-24\n\n## Tasks\n- written on the other device\n",
    );
    const local = makeFakePlugin(harness.authUrl, {
      sessionToken: aliceToken,
      activeVaultId: vault.id,
    });
    // The daily-notes plugin created today's note before sync caught up.
    local.vault.files.set(remote.path, "# 2026-09-24\n\n## Tasks\n");

    const sync = new VaultSync(local.plugin as any);
    (local.plugin as any).vaultSync = sync;
    try {
      await waitFor(() => local.vault.files.get(remote.path) === remote.content, {
        timeout: 20_000,
        label: "remote note adopted",
      });
      expect(bootstrapModal.calls).toEqual([]);
      expect((await readNote(vault.id, remote.path)).content).toBe(remote.content);
      expect([...local.vault.files.keys()].filter((path) => /conflicted copy/.test(path))).toEqual(
        [],
      );
    } finally {
      sync.destroy();
    }
  });

  it("publishes the local note when it extends a template the remote created", async () => {
    const vault = await harness.createVault(aliceToken, "template-local-superset");
    const remote = await createNote(vault.id, "Daily/2026-09-24.md", "# 2026-09-24\n\n## Tasks\n");
    const local = makeFakePlugin(harness.authUrl, {
      sessionToken: aliceToken,
      activeVaultId: vault.id,
    });
    const filled = "# 2026-09-24\n\n## Tasks\n- written offline\n";
    local.vault.files.set(remote.path, filled);

    const sync = new VaultSync(local.plugin as any);
    (local.plugin as any).vaultSync = sync;
    try {
      await waitFor(async () => (await readNote(vault.id, remote.path)).content === filled, {
        timeout: 20_000,
        label: "local superset published",
      });
      expect(bootstrapModal.calls).toEqual([]);
      expect(local.vault.files.get(remote.path)).toBe(filled);
    } finally {
      sync.destroy();
    }
  });

  it("adopts a remote canvas over an empty canvas a plugin created locally", async () => {
    const vault = await harness.createVault(aliceToken, "template-remote-canvas");
    const path = "Boards/Today.canvas";
    const guid = crypto.randomUUID();
    const seed = makeFakePlugin(harness.authUrl, {
      sessionToken: aliceToken,
      activeVaultId: vault.id,
    });
    const remoteNode = {
      id: "n1",
      type: "text",
      text: "remote",
      x: 0,
      y: 0,
      width: 100,
      height: 60,
    };
    const canvasPeer = new Peer(seed.plugin, `${vault.id}__${guid}`);
    const index = makeIndexPeer(seed.plugin as any, vault.id);
    await canvasPeer.whenSynced();
    canvasPeer.doc.transact(() =>
      reconcileCanvas(
        canvasPeer.doc.getMap("root"),
        parseCanvas(JSON.stringify({ nodes: [remoteNode], edges: [] })),
        canvasPeer.doc.clientID,
      ),
    );
    await canvasPeer.whenChangesSynced();
    await waitFor(() => index.provider.status === "connected", { label: "index peer connected" });
    index.doc.getMap("structured").set(path, { guid, kind: "canvas" });
    await waitFor(() => !index.provider.hasLocalChanges, { label: "index entry acknowledged" });

    const local = makeFakePlugin(harness.authUrl, {
      sessionToken: aliceToken,
      activeVaultId: vault.id,
    });
    local.vault.files.set(path, JSON.stringify({ nodes: [], edges: [] }));
    const sync = new VaultSync(local.plugin as any);
    (local.plugin as any).vaultSync = sync;
    try {
      await waitFor(() => JSON.parse(local.vault.files.get(path) ?? "{}").nodes?.length === 1, {
        timeout: 20_000,
        label: "remote canvas adopted",
      });
      await new Promise((resolve) => setTimeout(resolve, 500));
      expect(JSON.parse(serializeCanvas(toValue(canvasPeer.doc.getMap("root")))).nodes).toEqual([
        remoteNode,
      ]);
      expect([...local.vault.files.keys()].filter((file) => /conflicted copy/.test(file))).toEqual(
        [],
      );
    } finally {
      sync.destroy();
      canvasPeer.destroy();
      index.provider.destroy();
      index.doc.destroy();
    }
  });

  it("recovers a bootstrap decision after restart during the conflict window", async () => {
    const vault = await harness.createVault(aliceToken, "bootstrap-crash-window");
    const remote = await createNote(vault.id, "collision.md", "remote before restart");
    const local = makeFakePlugin(harness.authUrl, {
      sessionToken: aliceToken,
      activeVaultId: vault.id,
      clientName: "Restarting Device",
    });
    local.vault.files.set(remote.path, "offline local before restart");
    bootstrapModal.delayMs = 1_000;

    const first = new VaultSync(local.plugin as any);
    (local.plugin as any).vaultSync = first;
    await waitFor(() => bootstrapModal.calls.length === 1, {
      timeout: 20_000,
      label: "first conflict decision pending",
    });
    await new Promise((resolve) => setTimeout(resolve, 100));
    first.destroy();

    bootstrapModal.calls.length = 0;
    bootstrapModal.delayMs = 0;
    const restarted = new VaultSync(local.plugin as any);
    (local.plugin as any).vaultSync = restarted;
    try {
      await waitFor(() => bootstrapModal.calls.length === 1, {
        timeout: 20_000,
        label: "conflict resumed after restart",
      });
      await waitFor(
        async () =>
          (await readNote(vault.id, remote.path)).content === "offline local before restart",
        {
          timeout: 20_000,
          label: "restarted decision committed",
        },
      );
      const copies = [...local.vault.files.keys()].filter((path) =>
        /\(conflicted copy Remote /.test(path),
      );
      expect(copies.some((path) => local.vault.files.get(path) === "remote before restart")).toBe(
        true,
      );
    } finally {
      restarted.destroy();
    }
  });

  it("preserves an offline local edit when the remote path was deleted", async () => {
    const vault = await harness.createVault(aliceToken, "remote-delete-local-edit");
    const remote = await createNote(vault.id, "deleted.md", "last acknowledged content");
    const local = makeFakePlugin(harness.authUrl, {
      sessionToken: aliceToken,
      activeVaultId: vault.id,
      clientName: "Offline Device",
    });
    const first = new VaultSync(local.plugin as any);
    (local.plugin as any).vaultSync = first;
    await waitFor(() => local.vault.files.get(remote.path) === remote.content, {
      timeout: 20_000,
      label: "delete baseline materialized",
    });
    await waitFor(
      () =>
        (first as any).localSyncState.acknowledgedFingerprint(remote.path, remote.guid) !== null,
      { timeout: 20_000, label: "delete baseline acknowledged" },
    );
    await new Promise((resolve) => setTimeout(resolve, 100));
    first.destroy();

    local.vault.files.set(remote.path, "offline edit that must survive");
    await deleteNote(vault.id, remote.path);
    const restarted = new VaultSync(local.plugin as any);
    (local.plugin as any).vaultSync = restarted;
    try {
      await waitFor(() => !local.vault.files.has(remote.path), {
        timeout: 20_000,
        label: "remote deletion applied",
      });
      const copies = [...local.vault.files.keys()].filter((path) =>
        /\(conflicted copy Offline Device /.test(path),
      );
      expect(copies).toHaveLength(1);
      expect(local.vault.files.get(copies[0])).toBe("offline edit that must survive");
      expect(await readNoteStatus(vault.id, remote.path)).toBe(404);
    } finally {
      restarted.destroy();
    }
  });

  it("does not apply a stale remote delete after the path is reintroduced", async () => {
    const vault = await harness.createVault(aliceToken, "delete-readd-race");
    const remote = await createNote(vault.id, "raced.md", "stable content");
    const local = makeFakePlugin(harness.authUrl, {
      sessionToken: aliceToken,
      activeVaultId: vault.id,
    });
    const sync = new VaultSync(local.plugin as any);
    (local.plugin as any).vaultSync = sync;
    const peer = makeIndexPeer(local.plugin, vault.id);
    try {
      await waitFor(
        () =>
          local.vault.files.get(remote.path) === remote.content &&
          (sync as any).initialSynced &&
          peer.files.get(remote.path) === remote.guid,
        { timeout: 20_000, label: "delete race baseline ready" },
      );

      const read = local.vault.read.bind(local.vault);
      local.vault.read = vi.fn(async (file) => {
        if (file.path === remote.path) {
          await new Promise((resolve) => setTimeout(resolve, 250));
        }
        return read(file);
      });

      peer.files.delete(remote.path);
      await new Promise((resolve) => setTimeout(resolve, 50));
      peer.files.set(remote.path, remote.guid);
      await new Promise((resolve) => setTimeout(resolve, 500));

      expect(local.vault.files.get(remote.path)).toBe(remote.content);
      expect((sync as any).files.get(remote.path)).toBe(remote.guid);
    } finally {
      sync.destroy();
      peer.provider.destroy();
      peer.doc.destroy();
    }
  });

  it("finishes a remote delete after restart when the merged index persisted first", async () => {
    const vault = await harness.createVault(aliceToken, "delete-index-crash");
    const remote = await createNote(vault.id, "crash-delete.md", "acknowledged");
    const local = makeFakePlugin(harness.authUrl, {
      sessionToken: aliceToken,
      activeVaultId: vault.id,
    });
    const first = new VaultSync(local.plugin as any);
    (local.plugin as any).vaultSync = first;
    await waitFor(
      () =>
        local.vault.files.get(remote.path) === remote.content &&
        (first as any).localSyncState.acknowledgedFingerprint(remote.path, remote.guid) !== null,
      { timeout: 20_000, label: "crash-delete baseline acknowledged" },
    );

    const read = local.vault.read.bind(local.vault);
    let releaseRead!: () => void;
    const readBlocked = new Promise<void>((resolve) => {
      releaseRead = resolve;
    });
    local.vault.read = vi.fn(async (file) => {
      if (file.path === remote.path) await readBlocked;
      return read(file);
    });

    await deleteNote(vault.id, remote.path);
    await waitFor(() => !(first as any).files.has(remote.path), {
      timeout: 20_000,
      label: "deleted index merged before crash",
    });
    await new Promise((resolve) => setTimeout(resolve, 100));
    first.destroy();
    releaseRead();
    local.vault.read = read;

    const restarted = new VaultSync(local.plugin as any);
    (local.plugin as any).vaultSync = restarted;
    try {
      await waitFor(() => !local.vault.files.has(remote.path), {
        timeout: 20_000,
        label: "persisted remote deletion resumed",
      });
      expect([...local.vault.files.keys()].some((path) => /\(conflicted copy /.test(path))).toBe(
        false,
      );
      expect(await readNoteStatus(vault.id, remote.path)).toBe(404);
    } finally {
      restarted.destroy();
    }
  });

  it("keeps a remote-created note across client restart when it never existed locally", async () => {
    const vault = await harness.createVault(aliceToken, "remote-restart");
    const created = await createNote(vault.id, "Servant42/remote.md", "remote content");
    const first = makeFakePlugin(harness.authUrl, {
      sessionToken: aliceToken,
      activeVaultId: vault.id,
    });
    const firstSync = new VaultSync(first.plugin as any);
    (first.plugin as any).vaultSync = firstSync;

    firstSync.destroy();

    const second = makeFakePlugin(harness.authUrl, {
      sessionToken: aliceToken,
      activeVaultId: vault.id,
    });
    const secondSync = new VaultSync(second.plugin as any);
    (second.plugin as any).vaultSync = secondSync;
    try {
      await waitFor(() => second.vault.files.get(created.path) === created.content, {
        timeout: 20_000,
        label: "remote note materialized after restart",
      });
      expect((await listNotes(vault.id)).map((note) => note.guid)).toContain(created.guid);
      expect(await readNoteStatus(vault.id, created.path)).toBe(200);
    } finally {
      secondSync.destroy();
    }
  });

  it("does not conflict on restart when an automation edits a note it created remotely", async () => {
    const vault = await harness.createVault(aliceToken, "automation-remote-create");
    const local = makeFakePlugin(harness.authUrl, {
      sessionToken: aliceToken,
      activeVaultId: vault.id,
    });
    // Real vault reads hit the disk adapter and resolve after other microtasks.
    const read = local.vault.read.bind(local.vault);
    local.vault.read = async (file: TFile) => {
      await new Promise((resolve) => setTimeout(resolve, 500));
      return await read(file);
    };
    const first = new VaultSync(local.plugin as any);
    (local.plugin as any).vaultSync = first;
    await waitFor(() => (first as any).initialSynced, { timeout: 20_000, label: "bootstrapped" });

    // An automation creates the note while this client is running; the client
    // materializes it through vault.create, whose create event echoes back.
    const created = await createNote(vault.id, "Automation/log.md", "entry 1\n");
    await waitFor(() => local.vault.files.get(created.path) === created.content, {
      timeout: 20_000,
      label: "automation note materialized",
    });
    await waitFor(
      () => (first as any).localSyncState.get(created.path)?.fingerprint !== undefined,
      { timeout: 20_000, label: "materialized content acknowledged" },
    );
    // Let the echoed create handler finish its slow read before checking.
    await new Promise((resolve) => setTimeout(resolve, 1_000));
    expect((first as any).localSyncState.get(created.path)?.identity).toBe(created.guid);
    first.destroy();

    // The automation keeps writing while this client is closed.
    await replaceNote(vault.id, created.path, "entry 1\nentry 2\n");

    const restarted = new VaultSync(local.plugin as any);
    (local.plugin as any).vaultSync = restarted;
    try {
      await waitFor(() => local.vault.files.get(created.path) === "entry 1\nentry 2\n", {
        timeout: 20_000,
        label: "remote automation edit applied after restart",
      });
      expect(bootstrapModal.calls).toEqual([]);
      expect([...local.vault.files.keys()].filter((path) => /conflicted copy/.test(path))).toEqual(
        [],
      );
    } finally {
      restarted.destroy();
    }
  });

  it("does not publish a deletion when restart interrupts remote note materialization", async () => {
    const vault = await harness.createVault(aliceToken, "remote-create-interrupted");
    const created = await createNote(vault.id, "Servant42/interrupted.md", "remote content");
    const local = makeFakePlugin(harness.authUrl, {
      sessionToken: aliceToken,
      activeVaultId: vault.id,
    });
    const originalCreate = local.vault.create.bind(local.vault);
    let createStarted = false;
    let releaseCreate!: () => void;
    const createBlocked = new Promise<void>((resolve) => {
      releaseCreate = resolve;
    });
    local.vault.create = vi.fn(async () => {
      createStarted = true;
      await createBlocked;
      throw new Error("simulated shutdown during create");
    });

    const first = new VaultSync(local.plugin as any);
    (local.plugin as any).vaultSync = first;
    await waitFor(() => createStarted, {
      timeout: 20_000,
      label: "remote note disk create started",
    });
    expect((first as any).localSyncState.has(created.path)).toBe(false);
    first.destroy();
    releaseCreate();
    await new Promise((resolve) => setTimeout(resolve, 50));
    local.vault.create = originalCreate;

    const restarted = new VaultSync(local.plugin as any);
    (local.plugin as any).vaultSync = restarted;
    try {
      await waitFor(() => local.vault.files.get(created.path) === created.content, {
        timeout: 20_000,
        label: "interrupted remote note materialized after restart",
      });
      expect((await listNotes(vault.id)).map((note) => note.guid)).toContain(created.guid);
      expect(await readNoteStatus(vault.id, created.path)).toBe(200);
    } finally {
      restarted.destroy();
    }
  });

  it("restores an acknowledged note missing when the client starts", async () => {
    const vault = await harness.createVault(aliceToken, "offline-delete");
    const local = makeFakePlugin(harness.authUrl, {
      sessionToken: aliceToken,
      activeVaultId: vault.id,
    });
    const sync = new VaultSync(local.plugin as any);
    (local.plugin as any).vaultSync = sync;
    const created = await createNote(vault.id, "Servant42/lost.md", "acknowledged content");

    await waitFor(() => local.vault.files.get(created.path) === created.content, {
      timeout: 20_000,
      label: "remote note materialized",
    });
    sync.destroy();

    // The file disappears while the plugin is stopped. Absence alone is not
    // proof of an intentional delete: cloud storage may be unavailable or
    // evicted, so startup must restore the authoritative remote note.
    local.vault.files.delete(created.path);
    const restarted = new VaultSync(local.plugin as any);
    (local.plugin as any).vaultSync = restarted;
    try {
      await waitFor(() => local.vault.files.get(created.path) === created.content, {
        timeout: 20_000,
        label: "missing startup note restored",
      });
      expect((await listNotes(vault.id)).map((note) => note.guid)).toContain(created.guid);
      expect(await readNoteStatus(vault.id, created.path)).toBe(200);
    } finally {
      restarted.destroy();
    }
  });

  it("does not lose concurrent remote creates during list reconciliation", async () => {
    const vault = await harness.createVault(aliceToken, "concurrent-create");
    const paths = Array.from({ length: 40 }, (_, index) => `Servant42/concurrent-${index}.md`);

    const creations = Promise.all(
      paths.map((path, index) => createNote(vault.id, path, `content ${index}`)),
    );
    await Promise.all(Array.from({ length: 20 }, () => listNotes(vault.id)));
    const created = await creations;
    const listed = await listNotes(vault.id);
    const listedGuids = new Set(listed.map((note) => note.guid));

    expect(created.every((note) => listedGuids.has(note.guid))).toBe(true);
    expect(await Promise.all(paths.map((path) => readNoteStatus(vault.id, path)))).toEqual(
      Array(paths.length).fill(200),
    );
  });

  it("preserves unmaterialized notes through a high-fanout client restart", async () => {
    const vault = await harness.createVault(aliceToken, "high-fanout-restart");
    const paths = Array.from({ length: 180 }, (_, index) => `Fanout/remote-${index}.md`);
    const created = await Promise.all(
      paths.map((path, index) => createNote(vault.id, path, `remote ${index}`)),
    );
    const local = makeFakePlugin(harness.authUrl, {
      sessionToken: aliceToken,
      activeVaultId: vault.id,
    });

    const firstSync = new VaultSync(local.plugin as any);
    (local.plugin as any).vaultSync = firstSync;
    await waitFor(
      () => created.every((note) => (firstSync as any).files.get(note.path) === note.guid),
      { timeout: 20_000, label: "all remote index entries merged" },
    );
    firstSync.destroy();

    const restarted = new VaultSync(local.plugin as any);
    (local.plugin as any).vaultSync = restarted;
    try {
      let materialized = 0;
      try {
        await waitFor(
          () => {
            materialized = created.filter(
              (note) => local.vault.files.get(note.path) === note.content,
            ).length;
            return materialized === created.length;
          },
          { timeout: 60_000, label: "all remote notes materialized after restart" },
        );
      } catch (error) {
        const listed = await listNotes(vault.id);
        const remotelyReadable = (
          await Promise.all(paths.map((path) => readNote(vault.id, path)))
        ).filter((note, index) => note.content === created[index].content).length;
        const state = restarted as any;
        throw new Error(
          `${String(error)} (${materialized}/${created.length} materialized; ` +
            `${remotelyReadable}/${created.length} remotely readable; ${listed.length} listed; ` +
            `${state.documents.size} documents; ` +
            `${state.docQueue.size} queued; ${state.activeDocConnections} active)`,
        );
      }
      const listedGuids = new Set((await listNotes(vault.id)).map((note) => note.guid));
      expect(created.every((note) => listedGuids.has(note.guid))).toBe(true);
    } finally {
      restarted.destroy();
    }
  }, 90_000);

  it("respects binary settings for missing Canvas attachments", () => {
    expect(shouldSyncCanvasBinaryPath("image.png", false, "")).toBe(false);
    expect(shouldSyncCanvasBinaryPath("private/image.png", true, "private/**")).toBe(false);
    expect(shouldSyncCanvasBinaryPath("note.md", true, "")).toBe(false);
    expect(shouldSyncCanvasBinaryPath("image.png", true, "")).toBe(true);
  });

  it("keeps config files out of binary sync and removes legacy binary entries", async () => {
    const vault = await harness.createVault(aliceToken, "config-binary-cutover");
    const local = makeFakePlugin(harness.authUrl, {
      sessionToken: aliceToken,
      activeVaultId: vault.id,
    });
    const sync = new VaultSync(local.plugin as any);
    try {
      const state = sync as any;
      const configPath = ".obsidian/workspace.json";
      const existingConfigPath = ".obsidian/app.json";
      expect(state.classify(new TFile(configPath))).toBe("ignore");
      expect(state.classify(new TFile("attachment.bin"))).toBe("binary");

      state.indexDoc.getMap("binaries").set(configPath, {
        hash: "a".repeat(64),
        size: 2,
      });
      state.indexDoc.getMap("binaries").set(existingConfigPath, {
        hash: "b".repeat(64),
        size: 3,
      });
      state.indexDoc.getMap("configFiles").set(existingConfigPath, {
        hash: "c".repeat(64),
        size: 4,
        mtime: 42,
      });
      await state.reconcileBinariesAndMigrateStructured();

      expect(state.indexDoc.getMap("binaries").has(configPath)).toBe(false);
      expect(state.indexDoc.getMap("binaries").has(existingConfigPath)).toBe(false);
      expect(state.indexDoc.getMap("configFiles").get(configPath)).toEqual({
        hash: "a".repeat(64),
        size: 2,
        mtime: 0,
      });
      expect(state.indexDoc.getMap("configFiles").get(existingConfigPath)).toEqual({
        hash: "c".repeat(64),
        size: 4,
        mtime: 42,
      });
      expect(local.vault.binaries.has(configPath)).toBe(false);
    } finally {
      sync.destroy();
    }
  });

  it("preserves both binary paths when shutdown interrupts rename publication", async () => {
    const vault = await harness.createVault(aliceToken, "binary-rename-restart");
    const local = makeFakePlugin(harness.authUrl, {
      sessionToken: aliceToken,
      activeVaultId: vault.id,
    });
    const bytes = new Uint8Array([4, 8, 15, 16, 23, 42]).buffer;
    local.vault.binaries.set("old.bin", bytes);
    let sync: VaultSync | null = new VaultSync(local.plugin as any);
    (local.plugin as any).vaultSync = sync;
    await waitFor(
      () => {
        const binarySync = (sync as any).binarySync;
        const state = (sync as any).localSyncState.get("old.bin");
        return binarySync.hasPath("old.bin") && state?.candidate === false;
      },
      { timeout: 20_000, label: "binary rename baseline acknowledged" },
    );

    local.vault.binaries.delete("old.bin");
    local.vault.binaries.set("new.bin", bytes);
    (local.vault as any).emit("rename", local.vault.getAbstractFileByPath("new.bin"), "old.bin");
    sync.destroy();
    sync = null;

    sync = new VaultSync(local.plugin as any);
    (local.plugin as any).vaultSync = sync;
    try {
      await waitFor(
        () => {
          const binarySync = (sync as any).binarySync;
          return binarySync.hasPath("new.bin") && binarySync.hasPath("old.bin");
        },
        { timeout: 20_000, label: "interrupted binary rename recovered safely" },
      );
      expect([...new Uint8Array(local.vault.binaries.get("new.bin")!)]).toEqual([
        4, 8, 15, 16, 23, 42,
      ]);
      expect([...new Uint8Array(local.vault.binaries.get("old.bin")!)]).toEqual([
        4, 8, 15, 16, 23, 42,
      ]);
    } finally {
      sync.destroy();
    }
  });

  it("propagates create / delete / rename through namespaced docs", async () => {
    const vault = await harness.createVault(aliceToken, "notes");
    const vaultId = vault.id;
    const { plugin, vault: localVault } = makeFakePlugin(harness.authUrl, {
      sessionToken: aliceToken,
      activeVaultId: vaultId,
    });
    const peer = makeIndexPeer(plugin, vaultId);

    localVault.files.set("a.md", "alpha"); // one pre-existing file
    const sync = new VaultSync(plugin as any);

    try {
      // Initial sync registers the existing file and propagates to the peer.
      await waitFor(() => peer.files.has("a.md"), { timeout: 20_000, label: "a.md indexed" });

      // The file doc is reachable under the namespaced id `${vaultId}__${guid}`.
      const guid = peer.files.get("a.md")!;
      const fileDoc = new Y.Doc();
      const fileProvider = new RealtimeProvider(`${vaultId}__${guid}`, fileDoc, () =>
        getClientToken(plugin as any, `${vaultId}__${guid}`),
      );
      try {
        await waitFor(() => fileDoc.getText("contents").toString() === "alpha", {
          timeout: 20_000,
          label: "namespaced file doc has content",
        });
      } finally {
        fileProvider.destroy();
        fileDoc.destroy();
      }

      // Local create propagates.
      await localVault.create("b.md", "bravo");
      await waitFor(() => peer.files.has("b.md"), { label: "b.md indexed" });

      // A conflict-copy file must NOT be indexed / synced.
      await localVault.create("a (conflicted copy Brave Otter 2026-06-02 120000).md", "x");
      await new Promise((r) => setTimeout(r, 400));
      const indexedConflict = [...peer.files.keys()].some((p) => /conflicted copy/.test(p));
      expect(indexedConflict).toBe(false);

      // Delete removes the entry.
      const bFile = localVault.getAbstractFileByPath("b.md")!;
      await localVault.delete(bFile);
      await waitFor(() => !peer.files.has("b.md"), { label: "b.md unindexed" });

      // Rename moves the entry.
      localVault.rename("a.md", "c.md");
      await waitFor(() => peer.files.has("c.md") && !peer.files.has("a.md"), {
        label: "a.md -> c.md",
      });
    } finally {
      sync.destroy();
      peer.provider.destroy();
      peer.doc.destroy();
    }
  });

  it("publishes tracked renames as one atomic index transaction", async () => {
    const vault = await harness.createVault(aliceToken, "atomic-rename");
    const vaultId = vault.id;
    const { plugin, vault: localVault } = makeFakePlugin(harness.authUrl, {
      sessionToken: aliceToken,
      activeVaultId: vaultId,
    });
    localVault.files.set("a.md", "alpha");
    const sync = new VaultSync(plugin as any);
    try {
      const files = (sync as any).files as Y.Map<string>;
      await waitFor(() => files.has("a.md"), { timeout: 20_000, label: "a.md indexed" });

      const events: Array<Array<{ path: string; action: string }>> = [];
      files.observe((event) => {
        events.push(
          [...event.changes.keys.entries()].map(([path, change]) => ({
            path,
            action: change.action,
          })),
        );
      });

      localVault.rename("a.md", "c.md");
      await waitFor(() => files.has("c.md") && !files.has("a.md"), { label: "a.md -> c.md" });

      expect(
        events.some(
          (event) => event.length === 1 && event[0].path === "a.md" && event[0].action === "delete",
        ),
      ).toBe(false);
      expect(
        events.some(
          (event) =>
            event.some((change) => change.path === "a.md" && change.action === "delete") &&
            event.some((change) => change.path === "c.md" && change.action === "add"),
        ),
      ).toBe(true);
    } finally {
      sync.destroy();
    }
  });

  it("restores missing paths and preserves offline rename targets", async () => {
    const vault = await harness.createVault(aliceToken, "offline-path-changes");
    const vaultId = vault.id;
    const { plugin, vault: localVault } = makeFakePlugin(harness.authUrl, {
      sessionToken: aliceToken,
      activeVaultId: vaultId,
    });
    localVault.files.set("delete.md", "delete me");
    localVault.files.set("rename.md", "rename me");
    const peer = makeIndexPeer(plugin, vaultId);
    let sync: VaultSync | null = new VaultSync(plugin as any);
    (plugin as any).vaultSync = sync;
    try {
      await waitFor(() => peer.files.has("delete.md") && peer.files.has("rename.md"), {
        timeout: 20_000,
        label: "initial paths indexed",
      });
      // Let the device-local materialization manifest persist alongside the
      // index before simulating Obsidian being fully stopped.
      await waitFor(
        () => {
          const state = (sync as any).localSyncState;
          return (
            state.acknowledgedFingerprint("delete.md", peer.files.get("delete.md")) !== null &&
            state.acknowledgedFingerprint("rename.md", peer.files.get("rename.md")) !== null
          );
        },
        { timeout: 20_000, label: "offline path baselines acknowledged" },
      );
      await new Promise((resolve) => setTimeout(resolve, 100));
      sync.destroy();
      sync = null;

      await localVault.delete(localVault.getAbstractFileByPath("delete.md")!);
      localVault.rename("rename.md", "renamed.md");
      expect(peer.files.has("delete.md")).toBe(true);
      expect(peer.files.has("rename.md")).toBe(true);

      sync = new VaultSync(plugin as any);
      (plugin as any).vaultSync = sync;
      await waitFor(
        () =>
          peer.files.has("delete.md") &&
          peer.files.has("rename.md") &&
          peer.files.has("renamed.md") &&
          localVault.files.get("delete.md") === "delete me" &&
          localVault.files.get("rename.md") === "rename me",
        { timeout: 20_000, label: "offline path changes recovered safely" },
      );
    } finally {
      sync?.destroy();
      peer.provider.destroy();
      peer.doc.destroy();
    }
  });

  it("cleans up old cross-type index entries when a pending rename aborts", async () => {
    const vault = await harness.createVault(aliceToken, "cross-type-rename-abort");
    const vaultId = vault.id;
    const { plugin, vault: localVault } = makeFakePlugin(harness.authUrl, {
      sessionToken: aliceToken,
      activeVaultId: vaultId,
    });
    localVault.files.set("Board.canvas", JSON.stringify({ nodes: [], edges: [] }));
    const sync = new VaultSync(plugin as any);
    let releaseRead!: () => void;
    const blockedRead = new Promise<void>((resolve) => {
      releaseRead = resolve;
    });
    try {
      const structured = (sync as any).structured as Y.Map<{ guid: string; kind: string }>;
      const files = (sync as any).files as Y.Map<string>;
      await waitFor(() => structured.has("Board.canvas"), {
        timeout: 20_000,
        label: "Board.canvas structured index",
      });

      const originalRead = localVault.read.bind(localVault);
      localVault.read = async (file) => {
        if (file.path === "Board.md") {
          await blockedRead;
        }
        return originalRead(file);
      };

      localVault.rename("Board.canvas", "Board.md");
      await waitFor(() => !structured.has("Board.canvas"), {
        label: "old structured entry cleaned before pending text publish",
      });

      const renamed = localVault.getAbstractFileByPath("Board.md");
      if (renamed) await localVault.delete(renamed);
      releaseRead();
      await new Promise((r) => setTimeout(r, 50));

      expect(structured.has("Board.canvas")).toBe(false);
      expect(files.has("Board.md")).toBe(false);
    } finally {
      releaseRead();
      sync.destroy();
    }
  });

  it("refuses a token to a non-member of the vault", async () => {
    const vault = await harness.createVault(aliceToken, "private");
    const bobToken = await harness.loginUser("bob");
    const { plugin } = makeFakePlugin(harness.authUrl, {
      sessionToken: bobToken,
      activeVaultId: vault.id,
    });

    await expect(getClientToken(plugin as any, vault.id)).rejects.toThrow();
  });

  it("unregisters vault event handlers when destroyed", async () => {
    const vault = await harness.createVault(aliceToken, "cleanup");
    const { plugin, vault: localVault } = makeFakePlugin(harness.authUrl, {
      sessionToken: aliceToken,
      activeVaultId: vault.id,
    });

    const sync = new VaultSync(plugin as any);
    expect(localVault.handlerCount("create")).toBe(1);
    expect(localVault.handlerCount("delete")).toBe(1);
    expect(localVault.handlerCount("rename")).toBe(1);
    expect(localVault.handlerCount("modify")).toBe(1);

    sync.destroy();
    expect(localVault.handlerCount("create")).toBe(0);
    expect(localVault.handlerCount("delete")).toBe(0);
    expect(localVault.handlerCount("rename")).toBe(0);
    expect(localVault.handlerCount("modify")).toBe(0);
  });

  it("rescans files created while the initial sync pass is running", async () => {
    const vault = await harness.createVault(aliceToken, "initial-event-race");
    const { plugin, vault: localVault } = makeFakePlugin(harness.authUrl, {
      sessionToken: aliceToken,
      activeVaultId: vault.id,
    });
    localVault.files.set("slow.md", "slow");
    const sync = new VaultSync(plugin as any);
    (plugin as any).vaultSync = sync;
    try {
      await waitFor(() => (sync as any).initialSyncRunning, { label: "initial sync running" });
      await localVault.create("during-bootstrap.md", "created during bootstrap");
      await waitFor(() => (sync as any).files.has("during-bootstrap.md"), {
        timeout: 20_000,
        label: "bootstrap race file indexed",
      });
    } finally {
      sync.destroy();
    }
  });

  it("preserves a bootstrap create when a newer modify exists for the same file", async () => {
    const sync = Object.create(VaultSync.prototype) as any;
    const file = { path: "created-and-modified.md" };
    const order: string[] = [];
    Object.assign(sync, {
      destroyed: false,
      initialSynced: false,
      bootstrapVaultEvents: [
        { type: "create", file, version: 1 },
        { type: "modify", file, version: 2 },
      ],
      pathVersions: new Map([["created-and-modified.md", 2]]),
      handleLocalCreate: vi.fn(async () => order.push("create")),
      onLocalModify: vi.fn(() => order.push("modify")),
      handleLocalRename: vi.fn(),
      onLocalDelete: vi.fn(),
    });

    await sync.replayBootstrapVaultEvents();

    expect(order).toEqual(["create", "modify"]);
    expect(sync.bootstrapVaultEvents).toHaveLength(0);
  });

  it("retries when the final bootstrap rescan fails", async () => {
    const vault = await harness.createVault(aliceToken, "initial-rescan-retry");
    const { plugin } = makeFakePlugin(harness.authUrl, {
      sessionToken: aliceToken,
      activeVaultId: vault.id,
    });
    const sync = new VaultSync(plugin as any);
    (plugin as any).vaultSync = sync;
    const original = (sync as any).runInitialSyncPass.bind(sync);
    let calls = 0;
    (sync as any).runInitialSyncPass = vi.fn(async () => {
      calls += 1;
      if (calls === 2) throw new Error("final rescan failed");
      return original();
    });
    try {
      await waitFor(() => calls >= 3 && (sync as any).initialSynced, {
        timeout: 20_000,
        label: "bootstrap retried after final rescan failure",
      });
      expect((sync as any).backgroundSyncStarted).toBe(true);
    } finally {
      sync.destroy();
    }
  });

  it("does not create unowned startup priority state when an editor activates a document", () => {
    const sync = Object.create(VaultSync.prototype) as any;
    const doc = { connect: vi.fn() };
    Object.assign(sync, {
      files: new Map([["active.md", "active-guid"]]),
      documents: new Map(),
      prioritizedPaths: new Set(),
      prioritizedGuids: new Set(),
      mobileSuspended: false,
      markMobileDocumentUsed: vi.fn(),
      ensureDocument: vi.fn(() => doc),
    });

    expect(sync.ensureDocumentForPath("active.md")).toBe(doc);
    expect(doc.connect).toHaveBeenCalledOnce();
    expect(sync.prioritizedPaths).toEqual(new Set());
    expect(sync.prioritizedGuids).toEqual(new Set());
  });

  it("releases mobile channels in the background and catches up after resume", async () => {
    (Platform as any).isMobile = true;
    const vault = await harness.createVault(aliceToken, "mobile-lifecycle");
    await createNote(vault.id, "active.md", "active v1");
    await createNote(vault.id, "recent.md", "recent v1");
    const { plugin, vault: localVault } = makeFakePlugin(harness.authUrl, {
      sessionToken: aliceToken,
      activeVaultId: vault.id,
      recentPaths: ["recent.md"],
    });
    (plugin.app.workspace as any).getActiveFile = () =>
      localVault.getAbstractFileByPath("active.md");

    const sync = new VaultSync(plugin as any);
    (plugin as any).vaultSync = sync;
    try {
      await waitFor(
        () =>
          localVault.files.get("active.md") === "active v1" &&
          localVault.files.get("recent.md") === "recent v1" &&
          sync.allDocuments().length === 2 &&
          sync.allDocuments().every((doc) => doc.provider.status === "connected"),
        { timeout: 20_000, label: "mobile documents initially connected" },
      );

      sync.suspendForBackground();
      expect((sync as any).indexProvider.status).toBe("offline");
      expect(sync.allDocuments().every((doc) => doc.provider.status === "offline")).toBe(true);

      const activeFile = localVault.getAbstractFileByPath("active.md");
      expect(activeFile).not.toBeNull();
      await localVault.modify(activeFile!, "active local edit while hidden");
      await waitFor(
        () => sync.getDocumentForPath("active.md")?.content === "active local edit while hidden",
        { label: "hidden local edit persisted into the CRDT" },
      );
      await replaceNote(vault.id, "recent.md", "recent v2 while hidden");
      sync.reconnectAll();
      await new Promise((resolve) => setTimeout(resolve, 100));
      expect((sync as any).indexProvider.status).toBe("offline");
      expect(localVault.files.get("recent.md")).toBe("recent v1");

      await sync.resumeFromBackground();
      await waitFor(() => localVault.files.get("recent.md") === "recent v2 while hidden", {
        timeout: 20_000,
        label: "mobile document caught up after resume",
      });
      await waitFor(
        async () =>
          (await readNote(vault.id, "active.md")).content === "active local edit while hidden",
        {
          timeout: 20_000,
          label: "hidden local edit uploaded after resume",
        },
      );
      await waitFor(() => sync.allDocuments().every((doc) => doc.provider.status === "connected"), {
        timeout: 20_000,
        label: "mobile document channels restored",
      });
    } finally {
      sync.destroy();
      (Platform as any).isMobile = false;
    }
  });

  it("bounds mobile residency and rehydrates invalidated or locally edited documents", async () => {
    (Platform as any).isMobile = true;
    const vault = await harness.createVault(aliceToken, "mobile-working-set");
    const paths = Array.from({ length: 8 }, (_, index) => `note-${index}.md`);
    for (const path of paths) await createNote(vault.id, path, `${path} v1`);

    const { plugin, vault: localVault } = makeFakePlugin(harness.authUrl, {
      sessionToken: aliceToken,
      activeVaultId: vault.id,
    });
    plugin.settings.mobileMaxResidentDocs = 4;
    plugin.settings.mobileRecentResidentDocs = 1;
    plugin.settings.recentPaths = [paths[0]];
    const sync = new VaultSync(plugin as any);
    (plugin as any).vaultSync = sync;

    try {
      await waitFor(
        () =>
          paths.every((path) => localVault.files.get(path) === `${path} v1`) &&
          (sync as any).docQueue.size === 0 &&
          (sync as any).activeDocConnections === 0,
        { timeout: 30_000, label: "mobile working-set bootstrap" },
      );
      await waitFor(() => sync.allDocuments().length <= 4, {
        timeout: 10_000,
        label: "mobile resident cap",
      });
      expect(sync.getDocumentForPath(paths[0])).toBeDefined();

      const remotelyChanged = paths.find((path) => !sync.getDocumentForPath(path));
      expect(remotelyChanged).toBeDefined();
      await replaceNote(vault.id, remotelyChanged!, "remote invalidation v2");
      await waitFor(() => localVault.files.get(remotelyChanged!) === "remote invalidation v2", {
        timeout: 20_000,
        label: "invalidated document rehydrated",
      });

      await waitFor(() => sync.allDocuments().length <= 4, {
        timeout: 10_000,
        label: "resident cap restored after invalidation",
      });
      const locallyChanged = paths.find(
        (path) => path !== remotelyChanged && !sync.getDocumentForPath(path),
      );
      expect(locallyChanged).toBeDefined();
      const localFile = localVault.getAbstractFileByPath(locallyChanged!);
      expect(localFile).not.toBeNull();
      await localVault.modify(localFile!, "local hibernated edit v2");
      await waitFor(
        async () =>
          (await readNote(vault.id, locallyChanged!)).content === "local hibernated edit v2",
        {
          timeout: 20_000,
          label: "hibernated local edit uploaded",
        },
      );
      await waitFor(() => sync.allDocuments().length <= 4, {
        timeout: 10_000,
        label: "resident cap restored after local edit",
      });
    } finally {
      sync.destroy();
      (Platform as any).isMobile = false;
    }
  });

  it("keeps every mobile document resident when the server lacks invalidations", async () => {
    (Platform as any).isMobile = true;
    const vault = await harness.createVault(aliceToken, "mobile-without-invalidations");
    const paths = Array.from({ length: 5 }, (_, index) => `legacy-${index}.md`);
    for (const path of paths) await createNote(vault.id, path, `${path} content`);

    const { plugin, vault: localVault } = makeFakePlugin(harness.authUrl, {
      sessionToken: aliceToken,
      activeVaultId: vault.id,
    });
    plugin.settings.mobileMaxResidentDocs = 2;
    (plugin.auth as any).supportsCapability = () => false;
    const sync = new VaultSync(plugin as any);
    (plugin as any).vaultSync = sync;

    try {
      await waitFor(
        () =>
          paths.every((path) => localVault.files.get(path) === `${path} content`) &&
          (sync as any).docQueue.size === 0 &&
          (sync as any).activeDocConnections === 0,
        { timeout: 30_000, label: "legacy mobile bootstrap" },
      );
      await new Promise((resolve) => setTimeout(resolve, 1_200));
      expect(sync.allDocuments()).toHaveLength(paths.length);
    } finally {
      sync.destroy();
      (Platform as any).isMobile = false;
    }
  });

  it("preserves hibernated local edits when the remote changes while mobile is hidden", async () => {
    (Platform as any).isMobile = true;
    const vault = await harness.createVault(aliceToken, "mobile-hibernated-conflict");
    const paths = Array.from({ length: 6 }, (_, index) => `conflict-${index}.md`);
    for (const path of paths) await createNote(vault.id, path, "shared baseline");
    const { plugin, vault: localVault } = makeFakePlugin(harness.authUrl, {
      sessionToken: aliceToken,
      activeVaultId: vault.id,
      mobileMaxResidentDocs: 4,
      mobileRecentResidentDocs: 0,
    });
    const sync = new VaultSync(plugin as any);
    (plugin as any).vaultSync = sync;

    try {
      await waitFor(
        () =>
          paths.every((path) => localVault.files.get(path) === "shared baseline") &&
          (sync as any).docQueue.size === 0,
        { timeout: 30_000, label: "hibernated conflict bootstrap" },
      );
      await waitFor(() => sync.allDocuments().length <= 4, {
        timeout: 10_000,
        label: "hibernated conflict resident cap",
      });
      const path = paths.find((candidate) => !sync.getDocumentForPath(candidate));
      expect(path).toBeDefined();

      sync.suspendForBackground();
      const file = localVault.getAbstractFileByPath(path!);
      expect(file).not.toBeNull();
      await localVault.modify(file!, "local while hidden");
      await replaceNote(vault.id, path!, "remote while hidden");
      await sync.resumeFromBackground();

      await waitFor(() => bootstrapModal.calls.some((call) => call.path === path), {
        timeout: 20_000,
        label: "hibernated conflict prompt",
      });
      await waitFor(
        async () => (await readNote(vault.id, path!)).content === "local while hidden",
        {
          timeout: 20_000,
          label: "hibernated local conflict choice uploaded",
        },
      );
      await waitFor(() => sync.allDocuments().length <= 4, {
        timeout: 10_000,
        label: "hibernated conflict cap restored",
      });
    } finally {
      sync.destroy();
      (Platform as any).isMobile = false;
    }
  });

  it("waits for the mobile index handshake before resolving hidden binary divergence", async () => {
    (Platform as any).isMobile = true;
    const vault = await harness.createVault(aliceToken, "mobile-binary-divergence");
    const local = makeFakePlugin(harness.authUrl, {
      sessionToken: aliceToken,
      activeVaultId: vault.id,
    });
    const remote = makeFakePlugin(harness.authUrl, {
      sessionToken: aliceToken,
      activeVaultId: vault.id,
      clientName: "Remote Device",
    });
    const initialBytes = new Uint8Array([1, 1, 1]).buffer;
    const localBytes = new Uint8Array([2, 2, 2]).buffer;
    const remoteBytes = new Uint8Array([3, 3, 3]).buffer;
    local.vault.binaries.set("shared.bin", initialBytes);

    const sync = new VaultSync(local.plugin as any);
    (local.plugin as any).vaultSync = sync;
    const peer = makeIndexPeer(remote.plugin, vault.id);
    const peerBinaries = peer.doc.getMap<{ hash: string; size: number }>("binaries");
    const connectGate = Promise.withResolvers<void>();
    try {
      const initialHash = await sha256Hex(initialBytes);
      await waitFor(() => peerBinaries.get("shared.bin")?.hash === initialHash, {
        timeout: 20_000,
        label: "initial mobile binary baseline",
      });

      sync.suspendForBackground();
      const localFile = local.vault.getAbstractFileByPath("shared.bin");
      expect(localFile).not.toBeNull();
      await local.vault.modifyBinary(localFile!, localBytes);
      await waitFor(() => (sync as any).binarySync.deferredReconciles.has("shared.bin"), {
        label: "hidden local binary edit deferred",
      });

      const remoteHash = await sha256Hex(remoteBytes);
      await remote.plugin.auth.putBlob(vault.id, "shared.bin", remoteHash, remoteBytes);
      peerBinaries.set("shared.bin", { hash: remoteHash, size: remoteBytes.byteLength });
      await waitFor(
        () => !peer.provider.hasLocalChanges && peerBinaries.get("shared.bin")?.hash === remoteHash,
        { label: "concurrent remote binary update persisted" },
      );

      const indexProvider = (sync as any).indexProvider as RealtimeProvider;
      const connect = indexProvider.connect.bind(indexProvider);
      indexProvider.connect = async () => {
        await connectGate.promise;
        await connect();
      };

      const reconnectQueue = vi.spyOn(sync as any, "queueMobileReconnects");
      const resumed = sync.resumeFromBackground();
      // Models the plugin's ten-second reconnect interval firing while the
      // index is still blocked in its foreground handshake.
      sync.reconnectAll();
      await new Promise((resolve) => setTimeout(resolve, 100));
      expect((sync as any).binarySync.paused).toBe(true);
      expect(binaryModal.calls).toHaveLength(0);
      expect(reconnectQueue).not.toHaveBeenCalled();

      connectGate.resolve();
      await resumed;
      expect(reconnectQueue).toHaveBeenCalledTimes(1);
      await waitFor(() => binaryModal.calls.length === 1, {
        timeout: 20_000,
        label: "mobile binary divergence surfaced after index sync",
      });
      expect(binaryModal.calls[0]).toEqual({
        path: "shared.bin",
        remoteDeleted: false,
      });

      const localHash = await sha256Hex(localBytes);
      await waitFor(() => peerBinaries.get("shared.bin")?.hash === localHash, {
        timeout: 20_000,
        label: "chosen local binary published after conflict handling",
      });
      expect(
        [...local.vault.binaries.keys()].some((path) => path.includes("conflicted copy")),
      ).toBe(true);
    } finally {
      connectGate.resolve();
      sync.destroy();
      peer.provider.destroy();
      peer.doc.destroy();
      (Platform as any).isMobile = false;
    }
  });

  it("reconnects resident and priority mobile documents without recreating an evicted index entry", async () => {
    (Platform as any).isMobile = true;
    const sync = Object.create(VaultSync.prototype) as any;
    const index = new Y.Doc();
    const files = index.getMap<string>("files");
    files.set("a.md", "a");
    files.set("b.md", "b");
    files.set("active.md", "active");
    const connected: string[] = [];
    const completions = new Map<string, { promise: Promise<void>; resolve: () => void }>();
    const documents = new Map(
      [...files.entries()]
        .filter(([path]) => path !== "b.md")
        .map(([path, guid]) => {
          let resolve!: () => void;
          const promise = new Promise<void>((done) => {
            resolve = done;
          });
          const completion = { promise, resolve };
          completions.set(path, completion);
          return [
            path,
            {
              guid,
              provider: { status: "offline" },
              isReady: () => true,
              whenNextServerSync: () => completion.promise,
              whenReady: () => Promise.resolve(),
              connect: () => connected.push(path),
            },
          ];
        }),
    );

    Object.assign(sync, {
      destroyed: false,
      plugin: { auth: { supportsCapability: () => false } },
      mobileSuspended: false,
      files,
      structured: index.getMap("structured"),
      documents,
      structuredDocuments: new Map(),
      docQueue: new Map(),
      activeDocConnections: 0,
      docConnectionGeneration: 0,
      prioritizedPaths: new Set(),
      prioritizedGuids: new Set(),
      highPriorityDrained: true,
      startupPriorityPaths: () => new Set(["active.md"]),
      ensureDocument: (path: string) => documents.get(path),
    });

    try {
      sync.queueMobileReconnects();
      expect(connected).toEqual(["active.md"]);
      expect(sync.activeDocConnections).toBe(1);
      expect(sync.docQueue.size).toBe(1);

      completions.get("active.md")!.resolve();
      await Promise.resolve();
      await Promise.resolve();
      expect(connected).toEqual(["active.md", "a.md"]);
      expect(sync.activeDocConnections).toBe(1);

      completions.get("a.md")!.resolve();
      await Promise.resolve();
      await Promise.resolve();
      expect(connected).toEqual(["active.md", "a.md"]);
      expect(sync.activeDocConnections).toBe(0);
      expect(sync.docQueue.size).toBe(0);
      expect(documents.has("b.md")).toBe(false);
    } finally {
      index.destroy();
      (Platform as any).isMobile = false;
    }
  });

  it("never hibernates an unacknowledged mobile document while enforcing the cap", async () => {
    (Platform as any).isMobile = true;
    const sync = Object.create(VaultSync.prototype) as any;
    const destroyed: string[] = [];
    const documents = new Map<string, any>();
    for (const path of ["open.md", "pending.md", "old-1.md", "old-2.md", "new-1.md", "new-2.md"]) {
      documents.set(path, {
        guid: path,
        prepareForHibernation: vi.fn(async () => path !== "pending.md"),
        destroy: vi.fn(() => destroyed.push(path)),
      });
    }
    Object.assign(sync, {
      destroyed: false,
      plugin: {
        auth: { supportsCapability: () => true },
        settings: { mobileMaxResidentDocs: 4 },
      },
      documents,
      structuredDocuments: new Map(),
      mobileLastUsedAt: new Map([
        ["pending.md", 0],
        ["old-1.md", 1],
        ["old-2.md", 2],
        ["new-1.md", 3],
        ["new-2.md", 4],
      ]),
      mobileTrimTimer: null,
      mobileTrimRunning: false,
      startupPriorityPaths: () => new Set(["open.md"]),
      documentMatchesIndex: () => true,
    });

    try {
      await sync.trimMobileWorkingSet();
      expect(documents.has("open.md")).toBe(true);
      expect(documents.has("pending.md")).toBe(true);
      expect(destroyed).toEqual(["old-1.md", "old-2.md"]);
      expect(documents.size).toBe(4);
    } finally {
      if (sync.mobileTrimTimer !== null) window.clearTimeout(sync.mobileTrimTimer);
      (Platform as any).isMobile = false;
    }
  });

  it("keeps mobile work paused when the app is hidden again during the index handshake", async () => {
    (Platform as any).isMobile = true;
    const sync = Object.create(VaultSync.prototype) as any;
    const connected = Promise.withResolvers<void>();
    const indexProvider = {
      status: "offline",
      connect: vi.fn(async () => {
        await connected.promise;
        indexProvider.status = "connected";
      }),
    };
    const setPaused = vi.fn();
    Object.assign(sync, {
      destroyed: false,
      mobileSuspended: true,
      docConnectionGeneration: 4,
      indexProvider,
      plugin: {
        auth: { serverInfoChecked: vi.fn(async () => {}) },
        settings: { authServerUrl: "https://sync.example.com" },
      },
      scheduleMobileWorkingSetTrim: vi.fn(),
      binarySync: { setPaused },
      configSync: { setPaused },
      queueMobileReconnects: vi.fn(),
    });

    try {
      const resumed = sync.resumeFromBackground();
      expect(sync.mobileSuspended).toBe(false);
      sync.mobileSuspended = true;
      sync.docConnectionGeneration++;
      connected.resolve();
      await resumed;

      expect(setPaused).not.toHaveBeenCalled();
      expect(sync.queueMobileReconnects).not.toHaveBeenCalled();
    } finally {
      connected.resolve();
      (Platform as any).isMobile = false;
    }
  });

  it("unpauses deferred mobile work when a later reconnect succeeds", async () => {
    (Platform as any).isMobile = true;
    const sync = Object.create(VaultSync.prototype) as any;
    const setBinaryPaused = vi.fn();
    const setConfigPaused = vi.fn();
    const queueMobileReconnects = vi.fn();
    const indexProvider = { status: "offline" };
    Object.assign(sync, {
      destroyed: false,
      mobileSuspended: true,
      mobileCatchUpPending: false,
      mobileResumeInProgress: false,
      docConnectionGeneration: 1,
      indexProvider,
      plugin: { setStatus: vi.fn() },
      runInitialSync: vi.fn(async () => undefined),
      binarySync: { setPaused: setBinaryPaused },
      configSync: { setPaused: setConfigPaused },
      queueMobileReconnects,
      connectIndexAfterCompatibilityCheck: vi.fn(async () => undefined),
    });

    try {
      await sync.resumeFromBackground();
      expect(sync.mobileCatchUpPending).toBe(true);
      expect(setBinaryPaused).not.toHaveBeenCalled();
      expect(setConfigPaused).not.toHaveBeenCalled();

      indexProvider.status = "connected";
      sync.handleIndexStatus("connected");

      expect(setBinaryPaused).toHaveBeenCalledWith(false);
      expect(setConfigPaused).toHaveBeenCalledWith(false);
      expect(queueMobileReconnects).toHaveBeenCalledOnce();
      expect(sync.mobileCatchUpPending).toBe(false);
    } finally {
      (Platform as any).isMobile = false;
    }
  });

  it("queues retained documents after a failed foreground reconnect later succeeds", async () => {
    (Platform as any).isMobile = true;
    const sync = Object.create(VaultSync.prototype) as any;
    const queueMobileReconnects = vi.fn();
    const indexProvider = { status: "offline" };
    Object.assign(sync, {
      destroyed: false,
      mobileSuspended: true,
      mobileCatchUpPending: false,
      mobileResumeInProgress: false,
      docConnectionGeneration: 3,
      indexProvider,
      plugin: { setStatus: vi.fn() },
      runInitialSync: vi.fn(async () => undefined),
      binarySync: { setPaused: vi.fn() },
      configSync: { setPaused: vi.fn() },
      queueMobileReconnects,
      connectIndexAfterCompatibilityCheck: vi.fn(async () => undefined),
    });

    try {
      await sync.resumeFromBackground();
      expect(sync.mobileCatchUpPending).toBe(true);
      expect(queueMobileReconnects).not.toHaveBeenCalled();

      indexProvider.status = "connected";
      sync.handleIndexStatus("connected");

      expect(queueMobileReconnects).toHaveBeenCalledOnce();
      expect(sync.mobileCatchUpPending).toBe(false);
    } finally {
      (Platform as any).isMobile = false;
    }
  });

  it("checks server capabilities before opening the index and hard-blocks incompatibility", async () => {
    const sync = Object.create(VaultSync.prototype) as any;
    const order: string[] = [];
    const incompatible = new CompatibilityError(
      "server-incompatible",
      "unsupported required capability",
    );
    Object.assign(sync, {
      destroyed: false,
      mobileSuspended: false,
      plugin: {
        auth: {
          serverInfoChecked: vi.fn(async () => {
            order.push("caps");
            throw incompatible;
          }),
        },
        settings: { authServerUrl: "https://sync.example.com" },
        setStatus: vi.fn(),
      },
      indexProvider: {
        connect: vi.fn(async () => order.push("connect")),
        disconnect: vi.fn(),
      },
      scheduleMobileWorkingSetTrim: vi.fn(),
    });

    await sync.connectIndexAfterCompatibilityCheck();

    expect(order).toEqual(["caps"]);
    expect(sync.indexProvider.connect).not.toHaveBeenCalled();
    expect(sync.indexProvider.disconnect).toHaveBeenCalledOnce();
    expect(sync.plugin.setStatus).toHaveBeenCalledWith("offline");
  });

  it("keeps the index disconnected after transient verification failure and connects on retry", async () => {
    const sync = Object.create(VaultSync.prototype) as any;
    const serverInfoChecked = vi
      .fn()
      .mockRejectedValueOnce(new Error("offline"))
      .mockResolvedValueOnce({});
    Object.assign(sync, {
      destroyed: false,
      mobileSuspended: false,
      plugin: {
        auth: { serverInfoChecked },
        settings: { authServerUrl: "https://sync.example.com" },
        setStatus: vi.fn(),
      },
      indexProvider: {
        connect: vi.fn(async () => undefined),
        disconnect: vi.fn(),
      },
      scheduleMobileWorkingSetTrim: vi.fn(),
    });

    await sync.connectIndexAfterCompatibilityCheck();
    expect(sync.indexProvider.connect).not.toHaveBeenCalled();
    expect(sync.indexProvider.disconnect).toHaveBeenCalledOnce();

    await sync.connectIndexAfterCompatibilityCheck();
    expect(serverInfoChecked).toHaveBeenCalledTimes(2);
    expect(sync.indexProvider.connect).toHaveBeenCalledOnce();
  });

  it("replaces an existing queued document when the same path gets a new guid", () => {
    const sync = Object.create(VaultSync.prototype) as any;
    const oldDoc = { guid: "old-guid", destroy: vi.fn() };
    sync.destroyed = false;
    sync.documents = new Map([["same.md", oldDoc]]);
    sync.structuredDocuments = new Map();
    sync.docQueue = new Map([["same.md", { path: "same.md", guid: "queued-old", kind: "text" }]]);
    sync.prioritizedPaths = new Set();

    sync.enqueueDoc({ path: "same.md", guid: "new-guid", kind: "text" });

    expect(oldDoc.destroy).toHaveBeenCalledTimes(1);
    expect(sync.documents.has("same.md")).toBe(false);
    expect(sync.docQueue.get("same.md")).toEqual({
      path: "same.md",
      guid: "new-guid",
      kind: "text",
    });
  });

  it("replays the latest vault event captured during both bootstrap passes", async () => {
    const sync = Object.create(VaultSync.prototype) as any;
    const created = { path: "created.md" };
    const renamed = { path: "renamed.md" };
    const order: string[] = [];
    Object.assign(sync, {
      destroyed: false,
      initialSynced: false,
      bootstrapVaultEvents: [{ type: "delete", file: renamed, version: 1 }],
      pathVersions: new Map([
        ["created.md", 1],
        ["renamed.md", 1],
      ]),
      handleLocalCreate: vi.fn(async () => order.push("create")),
      onLocalModify: vi.fn(() => order.push("modify")),
      handleLocalRename: vi.fn(async () => order.push("rename")),
      onLocalDelete: vi.fn(() => order.push("delete")),
    });

    await sync.replayBootstrapVaultEvents();

    expect(order).toEqual(["delete"]);
    expect(sync.bootstrapVaultEvents).toHaveLength(0);
  });

  it("reruns a coalesced remote delete after the active reconciliation", async () => {
    const sync = Object.create(VaultSync.prototype) as any;
    const index = new Y.Doc();
    const first = Promise.withResolvers<void>();
    const calls: Array<[string, string, string | null]> = [];
    Object.assign(sync, {
      destroyed: false,
      files: index.getMap("files"),
      structured: index.getMap("structured"),
      remoteDeleteInFlight: new Set(),
      remoteDeletePending: new Map(),
      reconcileRemoteDelete: vi.fn(async (path: string, kind: string, identity: string | null) => {
        calls.push([path, kind, identity]);
        if (calls.length === 1) await first.promise;
      }),
    });

    const active = sync.handleRemoteDelete("same.md", "text", "old");
    await Promise.resolve();
    await sync.handleRemoteDelete("same.md", "text", "final");
    first.resolve();
    await active;

    expect(calls).toEqual([
      ["same.md", "text", "old"],
      ["same.md", "text", "final"],
    ]);
  });

  it("preserves resident text instead of stale disk content on remote delete", async () => {
    const local = makeFakePlugin("http://unused.invalid", {});
    const path = "resident.md";
    local.vault.files.set(path, "acknowledged disk");
    const index = new Y.Doc();
    const sync = Object.create(VaultSync.prototype) as any;
    const acknowledged = await sha256Text("acknowledged disk");
    Object.assign(sync, {
      destroyed: false,
      plugin: local.plugin,
      files: index.getMap("files"),
      structured: index.getMap("structured"),
      documents: new Map([[path, { content: "unsaved resident edit" }]]),
      structuredDocuments: new Map(),
      remoteDeletePreserved: new Set(),
      remoteDeletesApplying: new Set(),
      localSyncState: {
        acknowledgedFingerprint: vi.fn(() => acknowledged),
        remove: vi.fn(),
      },
      removeDocument: vi.fn(),
      removeStructuredDocument: vi.fn(),
    });

    await sync.reconcileRemoteDelete(path, "text", "guid");

    expect([...local.vault.files.values()]).toContain("unsaved resident edit");
    expect(local.vault.files.has(path)).toBe(false);
    index.destroy();
  });

  it("preserves non-empty resident canvas content on remote delete", async () => {
    const local = makeFakePlugin("http://unused.invalid", {});
    const path = "resident.canvas";
    const acknowledgedDisk = '{\n  "nodes": [],\n  "edges": []\n}\n';
    const residentCanvas =
      '{\n  "nodes": [\n    {\n      "id": "node-1",\n      "type": "text",\n      "text": "unsaved"\n    }\n  ],\n  "edges": [\n    {\n      "id": "edge-1",\n      "fromNode": "node-1",\n      "toNode": "node-1"\n    }\n  ]\n}\n';
    local.vault.files.set(path, acknowledgedDisk);
    const index = new Y.Doc();
    const sync = Object.create(VaultSync.prototype) as any;
    const acknowledged = await sha256Text(acknowledgedDisk);
    Object.assign(sync, {
      destroyed: false,
      plugin: local.plugin,
      files: index.getMap("files"),
      structured: index.getMap("structured"),
      documents: new Map(),
      structuredDocuments: new Map([[path, { canvasText: () => residentCanvas }]]),
      remoteDeletePreserved: new Set(),
      remoteDeletesApplying: new Set(),
      localSyncState: {
        acknowledgedFingerprint: vi.fn(() => acknowledged),
        remove: vi.fn(),
      },
      removeDocument: vi.fn(),
      removeStructuredDocument: vi.fn(),
    });
    Object.setPrototypeOf(sync.structuredDocuments.get(path), CanvasDocument.prototype);

    await sync.reconcileRemoteDelete(path, "canvas", "guid");

    const preserved = [...local.vault.files.values()].find((content) => content === residentCanvas);
    expect(preserved).toBe(residentCanvas);
    expect(JSON.parse(preserved!)).toMatchObject({
      nodes: [{ id: "node-1", text: "unsaved" }],
      edges: [{ id: "edge-1", fromNode: "node-1", toNode: "node-1" }],
    });
    expect(local.vault.files.has(path)).toBe(false);
    index.destroy();
  });

  it("ignores malformed structured index entries when resolving paths", () => {
    const sync = Object.create(VaultSync.prototype) as any;
    const index = new Y.Doc();
    sync.files = index.getMap("files");
    sync.structured = index.getMap("structured");
    sync.structured.set("Bad.canvas", { kind: "canvas" });
    sync.structured.set("Good.canvas", { guid: "good-guid", kind: "canvas" });

    expect(sync.pathForGuid("good-guid")).toBe("Good.canvas");
    expect(sync.pathForGuid("")).toBe(null);

    index.destroy();
  });
});
