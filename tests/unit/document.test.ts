import { describe, it, expect, beforeAll, afterAll, afterEach, vi } from "vitest";
import * as Y from "yjs";
import { Document } from "../../src/Document";
import { makeFakePlugin, type FakePlugin, type FakeVault } from "../support/fakePlugin";
import { notices } from "../support/obsidian-mock";
import { Peer } from "../support/peer";
import { startAuthHarness, type AuthHarness } from "../support/authServer";
import { waitFor, freshGuid } from "../support/util";
import { setDocumentEpoch } from "../../src/documentEpoch";
import { LocalSyncState } from "../../src/localSyncState";
import { storedContentFingerprint } from "../../src/SyncedDoc";
import { sha256Text } from "../../src/hash";

/**
 * The slice of VaultSync a Document reports disk/sync state to, backed by a
 * real LocalSyncState so state survives a simulated restart.
 */
function attachLocalSyncState(plugin: FakePlugin): LocalSyncState {
  const state = new LocalSyncState(`document-test:${freshGuid()}`);
  (plugin as any).vaultSync = {
    noteMaterialized: (path: string, kind: any, identity?: string) =>
      identity ? state.commit(path, kind, identity) : state.mark(path, kind),
    noteContentAcknowledged: (
      path: string,
      kind: any,
      identity: string,
      fingerprint: string,
      reconciled?: boolean,
    ) => state.markSynced(path, kind, identity, fingerprint, reconciled),
    noteDiskContent: (path: string, kind: any, identity: string, fingerprint: string) =>
      state.markDisk(path, kind, identity, fingerprint),
    diskFingerprint: async (path: string, identity: string) => {
      await state.whenSynced;
      return state.diskFingerprint(path, identity);
    },
    noteTextActivity: () => {},
    beginOwnWrite: () => () => {},
    isPendingLocalRemoval: () => false,
    acknowledgedFingerprint: (path: string, identity: string) =>
      state.acknowledgedFingerprint(path, identity),
  };
  return state;
}

const modalMock = vi.hoisted(() => ({
  choice: "local" as "local" | "remote",
  delayMs: 0,
  calls: [] as Array<{ path: string; localContent: string; remoteContent: string }>,
}));

vi.mock("../../src/TextConflictModal", () => ({
  openTextConflictModal: async (
    _plugin: unknown,
    info: { path: string; localContent: string; remoteContent: string },
  ) => {
    modalMock.calls.push(info);
    if (modalMock.delayMs > 0) {
      await new Promise((resolve) => setTimeout(resolve, modalMock.delayMs));
    }
    return modalMock.choice;
  },
}));

let harness: AuthHarness;
let token: string;
let vaultId: string;
let memberPlugin: FakePlugin;

beforeAll(async () => {
  harness = await startAuthHarness();
  token = await harness.loginUser("alice");
  const vault = await harness.createVault(token, "docs");
  vaultId = vault.id;
  memberPlugin = makeFakePlugin(harness.authUrl, {
    sessionToken: token,
    activeVaultId: vaultId,
  }).plugin;
}, 180_000);

afterAll(async () => {
  await harness?.stop();
});

afterEach(() => {
  notices.length = 0;
  modalMock.choice = "local";
  modalMock.delayMs = 0;
  modalMock.calls.length = 0;
});

/** The vault-namespaced doc id for a bare guid. */
const docId = (guid: string) => `${vaultId}__${guid}`;

/** Build a Document (client "A") over a fresh fake vault, optionally preloaded. */
function makeDoc(
  guid: string,
  opts: { file?: { path: string; content: string }; clientName?: string } = {},
) {
  const { plugin, vault } = makeFakePlugin(harness.authUrl, {
    sessionToken: token,
    activeVaultId: vaultId,
    clientName: opts.clientName,
  });
  if (opts.file) vault.files.set(opts.file.path, opts.file.content);
  const doc = new Document(plugin as any, opts.file?.path ?? "note.md", guid, docId(guid), true);
  return { doc, vault, plugin };
}

const conflictFiles = (vault: FakeVault) =>
  [...vault.files.keys()].filter((p) => /\(conflicted copy /.test(p));

describe("Document sync", () => {
  it("propagates local and remote edits (clean, no conflict)", async () => {
    const guid = freshGuid();
    const { doc, vault } = makeDoc(guid, { file: { path: "note.md", content: "seed" } });
    const peer = new Peer(memberPlugin, docId(guid));
    try {
      await doc.whenReady();
      await peer.whenSynced();

      // A's seeded content reaches B.
      await waitFor(() => peer.getText() === "seed", { label: "B sees seed" });

      // Remote edit from B lands on A's disk (no editor bound).
      peer.setText("seed + remote");
      await waitFor(() => vault.files.get("note.md") === "seed + remote", {
        label: "A disk has remote edit",
      });

      // Local disk edit on A propagates to B.
      vault.files.set("note.md", "local update");
      await doc.onDiskChanged();
      await waitFor(() => peer.getText() === "local update", { label: "B sees local edit" });

      expect(conflictFiles(vault)).toHaveLength(0);
      expect(notices).toHaveLength(0);
    } finally {
      doc.destroy();
      peer.destroy();
    }
  });

  it("does not write stale startup Y.Text to disk before the disk baseline is captured", async () => {
    const guid = freshGuid();
    const { doc, vault } = makeDoc(guid, { file: { path: "note.md", content: "newer disk" } });
    try {
      await doc.whenReady();
      (doc as any).startupBaselineCaptured = false;
      doc.ydoc.transact(() => {
        doc.ytext.delete(0, doc.ytext.length);
        doc.ytext.insert(0, "stale indexeddb");
      });

      await new Promise((r) => setTimeout(r, 250));

      expect(vault.files.get("note.md")).toBe("newer disk");
    } finally {
      doc.destroy();
    }
  });

  it("pure remote update writes to disk without a conflict copy", async () => {
    const guid = freshGuid();
    // Seed the server from B first.
    const peer = new Peer(memberPlugin, docId(guid));
    await peer.whenSynced();
    peer.setText("authored elsewhere");
    await peer.whenChangesSynced();

    // A starts with no local file at all.
    const { plugin, vault } = makeFakePlugin(harness.authUrl, {
      sessionToken: token,
      activeVaultId: vaultId,
    });
    const doc = new Document(plugin as any, "note.md", guid, docId(guid), false);
    try {
      await doc.whenReady();
      await waitFor(() => vault.files.get("note.md") === "authored elsewhere", {
        label: "A disk seeded from remote",
      });
      expect(conflictFiles(vault)).toHaveLength(0);
      expect(notices).toHaveLength(0);
    } finally {
      doc.destroy();
      peer.destroy();
    }
  });

  it("does not acknowledge a remote note before its disk create completes", async () => {
    const guid = freshGuid();
    const peer = new Peer(memberPlugin, docId(guid));
    await peer.whenSynced();
    peer.setText("authored elsewhere");
    await peer.whenChangesSynced();

    const { plugin, vault } = makeFakePlugin(harness.authUrl, {
      sessionToken: token,
      activeVaultId: vaultId,
    });
    const noteMaterialized = vi.fn();
    (plugin as any).vaultSync = {
      noteMaterialized,
      noteContentAcknowledged: vi.fn(),
      noteTextActivity: vi.fn(),
      beginOwnWrite: () => () => {},
      isPendingLocalRemoval: () => false,
    };
    const originalCreate = vault.create.bind(vault);
    let releaseCreate!: () => void;
    const createBlocked = new Promise<void>((resolve) => {
      releaseCreate = resolve;
    });
    vault.create = vi.fn(async (path, content) => {
      await createBlocked;
      return originalCreate(path, content);
    });

    const doc = new Document(plugin as any, "remote.md", guid, docId(guid), false);
    let ready = false;
    void doc.whenReady().then(() => {
      ready = true;
    });
    try {
      await waitFor(() => vi.mocked(vault.create).mock.calls.length > 0, {
        label: "remote disk create started",
      });
      expect(noteMaterialized).not.toHaveBeenCalled();
      expect(ready).toBe(false);

      releaseCreate();
      await doc.whenReady();
      expect(vault.files.get("remote.md")).toBe("authored elsewhere");
      expect(noteMaterialized).toHaveBeenCalledWith("remote.md", "text", guid);
    } finally {
      releaseCreate();
      doc.destroy();
      peer.destroy();
    }
  });

  it("does not write remote updates to disk while the note is open", async () => {
    const guid = freshGuid();
    const { plugin, vault } = makeFakePlugin(harness.authUrl, {
      sessionToken: token,
      activeVaultId: vaultId,
    });
    vault.files.set("note.md", "local buffer");
    (plugin.app.workspace as any).getLeavesOfType = () => [
      { view: { file: { path: "note.md" }, getMode: () => "source" } },
    ];

    const doc = new Document(plugin as any, "note.md", guid, docId(guid), true);
    const peer = new Peer(memberPlugin, docId(guid));
    try {
      await doc.whenReady();
      await peer.whenSynced();
      await waitFor(() => peer.getText() === "local buffer", { label: "seed synced" });

      peer.setText("remote update");
      await waitFor(() => doc.content === "remote update", { label: "remote reached doc" });
      await new Promise((r) => setTimeout(r, 250));

      expect(vault.files.get("note.md")).toBe("local buffer");
    } finally {
      doc.destroy();
      peer.destroy();
    }
  });

  it("preserves the latest in-memory text when read-only recovery overlaps a delayed save", async () => {
    const guid = freshGuid();
    const { doc, vault } = makeDoc(guid, {
      file: { path: "note.md", content: "stale disk snapshot" },
    });
    try {
      await doc.whenReady();
      (doc.provider as any).clientToken = {
        docId: docId(guid),
        url: harness.authUrl,
        authorization: "read-only",
      };

      const create = vault.create.bind(vault);
      const firstSave = Promise.withResolvers<void>();
      let releaseFirstSave!: () => void;
      const firstSaveRelease = new Promise<void>((resolve) => {
        releaseFirstSave = resolve;
      });
      let conflictSaves = 0;
      vault.create = async (path, content) => {
        if (/\(conflicted copy /.test(path)) {
          conflictSaves += 1;
          if (conflictSaves === 1) {
            firstSave.resolve();
            await firstSaveRelease;
          }
        }
        return create(path, content);
      };

      doc.ytext.insert(doc.ytext.length, " first edit");
      await firstSave.promise;
      doc.ytext.insert(doc.ytext.length, " second edit");
      doc.ytext.insert(doc.ytext.length, " third edit");
      releaseFirstSave();

      await waitFor(() => conflictFiles(vault).length === 2, {
        label: "queued read-only recovery saved",
      });
      const copies = conflictFiles(vault).map((path) => vault.files.get(path));
      expect(copies).toContain("stale disk snapshot first edit second edit third edit");
      expect(copies).not.toContain("stale disk snapshot");
      expect(vault.files.get("note.md")).toBe("stale disk snapshot");
    } finally {
      doc.destroy();
    }
  });

  it("writes remote updates to disk while the note is open in preview mode", async () => {
    const guid = freshGuid();
    const { plugin, vault } = makeFakePlugin(harness.authUrl, {
      sessionToken: token,
      activeVaultId: vaultId,
    });
    vault.files.set("note.md", "local preview");
    (plugin.app.workspace as any).getLeavesOfType = () => [
      { view: { file: { path: "note.md" }, getMode: () => "preview" } },
    ];

    const doc = new Document(plugin as any, "note.md", guid, docId(guid), true);
    const peer = new Peer(memberPlugin, docId(guid));
    try {
      await doc.whenReady();
      await peer.whenSynced();
      await waitFor(() => peer.getText() === "local preview", { label: "preview seed synced" });

      peer.setText("remote preview update");
      await waitFor(() => vault.files.get("note.md") === "remote preview update", {
        label: "preview disk receives remote update",
      });

      expect(doc.content).toBe("remote preview update");
    } finally {
      doc.destroy();
      peer.destroy();
    }
  });

  it("does not ingest disk modify events while a live editor binding owns the note", async () => {
    const guid = freshGuid();
    const { plugin, vault } = makeFakePlugin(harness.authUrl, {
      sessionToken: token,
      activeVaultId: vaultId,
    });
    vault.files.set("note.md", "shared buffer");
    (plugin.app.workspace as any).getLeavesOfType = () => [
      { view: { file: { path: "note.md" }, getMode: () => "source" } },
    ];

    const doc = new Document(plugin as any, "note.md", guid, docId(guid), true);
    const peer = new Peer(memberPlugin, docId(guid));
    try {
      await doc.whenReady();
      await peer.whenSynced();
      await waitFor(() => peer.getText() === "shared buffer", { label: "seed synced" });

      doc.bindEditor();
      vault.files.set("note.md", "disk save snapshot");
      await doc.onDiskChanged();
      await new Promise((r) => setTimeout(r, 250));

      expect(doc.content).toBe("shared buffer");
      expect(peer.getText()).toBe("shared buffer");
    } finally {
      doc.unbindEditor();
      doc.destroy();
      peer.destroy();
    }
  });

  it("ingests disk saves for an open note when no editor binding is attached", async () => {
    const guid = freshGuid();
    const { plugin, vault } = makeFakePlugin(harness.authUrl, {
      sessionToken: token,
      activeVaultId: vaultId,
    });
    vault.files.set("note.md", "before");
    (plugin.app.workspace as any).getLeavesOfType = () => [
      { view: { file: { path: "note.md" }, getMode: () => "source" } },
    ];

    const doc = new Document(plugin as any, "note.md", guid, docId(guid), true);
    const peer = new Peer(memberPlugin, docId(guid));
    try {
      await doc.whenReady();
      await peer.whenSynced();
      await waitFor(() => peer.getText() === "before", { label: "seed synced" });

      vault.files.set("note.md", "saved by unbound open editor");
      await doc.onDiskChanged();

      await waitFor(() => peer.getText() === "saved by unbound open editor", {
        label: "unbound open save reached peer",
      });
    } finally {
      doc.destroy();
      peer.destroy();
    }
  });

  it("does not write a scheduled disk update if the note opens during the debounce", async () => {
    // The race behind the "modified externally / changes merged in" bug: a remote
    // edit arrives while the note is closed and schedules a disk write; the user
    // opens the note within the 100ms debounce window. The write must re-check at
    // fire time and skip — otherwise vault.modify hits the open file and Obsidian
    // 3-way-merges it, duplicating characters.
    const guid = freshGuid();
    const { plugin, vault } = makeFakePlugin(harness.authUrl, {
      sessionToken: token,
      activeVaultId: vaultId,
    });
    vault.files.set("note.md", "base");
    // Note starts closed: the default workspace mock reports no open leaves.

    const doc = new Document(plugin as any, "note.md", guid, docId(guid), true);
    const peer = new Peer(memberPlugin, docId(guid));
    try {
      await doc.whenReady();
      await peer.whenSynced();
      await waitFor(() => peer.getText() === "base", { label: "seed synced" });

      // Remote edit lands -> onYTextChanged schedules a 100ms disk write.
      peer.setText("base + remote");
      await waitFor(() => doc.content === "base + remote", { label: "remote reached doc" });
      // Open the note before the debounce timer fires.
      (plugin.app.workspace as any).getLeavesOfType = () => [
        { view: { file: { path: "note.md" }, getMode: () => "source" } },
      ];
      await new Promise((r) => setTimeout(r, 250));

      expect(vault.files.get("note.md")).toBe("base");
    } finally {
      doc.destroy();
      peer.destroy();
    }
  });

  it("does not fold a stale write echo over a newer remote update", async () => {
    const guid = freshGuid();
    const { plugin, vault } = makeFakePlugin(harness.authUrl, {
      sessionToken: token,
      activeVaultId: vaultId,
    });
    vault.files.set("note.md", "base");
    const doc = new Document(plugin as any, "note.md", guid, docId(guid), true);
    const peer = new Peer(memberPlugin, docId(guid));
    try {
      await doc.whenReady();
      await peer.whenSynced();
      await waitFor(() => peer.getText() === "base", { label: "seed synced" });

      const writePromise = (doc as any).writeToDisk();
      await doc.onDiskChanged(); // disk already holds the text being written
      peer.setText("base + remote");
      await writePromise;

      await waitFor(() => doc.content === "base + remote", {
        label: "newer remote update applied",
      });
      expect(doc.content).toBe("base + remote");
      expect(peer.getText()).toBe("base + remote");
    } finally {
      doc.destroy();
      peer.destroy();
    }
  });

  it("folds a real disk edit that arrives while a write echo is in flight", async () => {
    const guid = freshGuid();
    const { doc, vault } = makeDoc(guid, { file: { path: "note.md", content: "base" } });
    try {
      await doc.whenReady();

      (doc as any).writingTextToDisk = "base";
      vault.files.set("note.md", "external edit");
      await doc.onDiskChanged();

      expect(doc.content).toBe("external edit");
    } finally {
      doc.destroy();
    }
  });

  it("retries a failed remote disk write without folding back a stale echo", async () => {
    const guid = freshGuid();
    const { doc, vault } = makeDoc(guid, {
      file: { path: "note.md", content: "before retry" },
    });
    const peer = new Peer(memberPlugin, docId(guid));
    try {
      await doc.whenReady();
      await peer.whenSynced();
      await waitFor(() => peer.getText() === "before retry", { label: "retry seed synced" });

      const originalModify = vault.modify.bind(vault);
      let attempts = 0;
      vault.modify = async (file, text) => {
        attempts++;
        if (attempts === 1) throw new Error("transient write failure");
        await originalModify(file, text);
      };

      peer.setText("after retry");
      await waitFor(() => doc.content === "after retry", { label: "retry remote reached doc" });
      await waitFor(() => vault.files.get("note.md") === "after retry", {
        timeout: 10_000,
        label: "retry converged disk",
      });

      expect(attempts).toBe(2);
      expect(doc.content).toBe("after retry");
      expect(peer.getText()).toBe("after retry");
    } finally {
      doc.destroy();
      peer.destroy();
    }
  });

  it("ignores its own delayed write echo after shared text has moved on", async () => {
    const guid = freshGuid();
    const { doc, vault } = makeDoc(guid, { file: { path: "note.md", content: "base" } });
    try {
      await doc.whenReady();

      (doc as any).writingTextToDisk = "old echo";
      (doc as any).applyText("newer shared");
      vault.files.set("note.md", "old echo");
      await doc.onDiskChanged();

      expect(doc.content).toBe("newer shared");
    } finally {
      doc.destroy();
    }
  });

  it("aborts a stale disk write when shared text changes during the pre-modify read", async () => {
    const guid = freshGuid();
    const { doc, vault } = makeDoc(guid, { file: { path: "note.md", content: "disk old" } });
    try {
      await doc.whenReady();
      (doc as any).applyText("stale write");
      if ((doc as any).writeTimer !== null) {
        window.clearTimeout((doc as any).writeTimer);
        (doc as any).writeTimer = null;
      }

      let readStarted = false;
      let releaseRead!: () => void;
      const originalRead = vault.read.bind(vault);
      vault.read = async (file) => {
        readStarted = true;
        await new Promise<void>((resolve) => {
          releaseRead = resolve;
        });
        return originalRead(file);
      };

      const writes: string[] = [];
      const originalModify = vault.modify.bind(vault);
      vault.modify = async (file, text) => {
        writes.push(text);
        await originalModify(file, text);
      };

      const writePromise = (doc as any).writeToDisk();
      await waitFor(() => readStarted, { label: "write reached pre-modify read" });
      (doc as any).applyText("newer shared");
      if ((doc as any).writeTimer !== null) {
        window.clearTimeout((doc as any).writeTimer);
        (doc as any).writeTimer = null;
      }
      releaseRead();
      await writePromise;

      expect(writes).not.toContain("stale write");
      expect(vault.files.get("note.md")).toBe("disk old");
      expect(doc.content).toBe("newer shared");
    } finally {
      doc.destroy();
    }
  });

  it("re-checks open editor state after the pre-modify disk read", async () => {
    const guid = freshGuid();
    const { plugin, vault } = makeFakePlugin(harness.authUrl, {
      sessionToken: token,
      activeVaultId: vaultId,
    });
    vault.files.set("note.md", "old");
    const doc = new Document(plugin as any, "note.md", guid, docId(guid), true);
    try {
      await doc.whenReady();
      (doc as any).applyText("new");
      if ((doc as any).writeTimer !== null) {
        window.clearTimeout((doc as any).writeTimer);
        (doc as any).writeTimer = null;
      }
      let reads = 0;
      let modified = false;
      const originalRead = vault.read.bind(vault);
      vault.read = async (file) => {
        reads++;
        const text = await originalRead(file);
        if (reads >= 1) {
          (plugin.app.workspace as any).getLeavesOfType = () => [
            { view: { file: { path: "note.md" }, getMode: () => "source" } },
          ];
        }
        return text;
      };
      const originalModify = vault.modify.bind(vault);
      vault.modify = async (file, text) => {
        modified = true;
        await originalModify(file, text);
      };

      await (doc as any).writeToDisk();

      expect(modified).toBe(false);
      expect(vault.files.get("note.md")).toBe("old");
    } finally {
      doc.destroy();
    }
  });

  it("does not flush to disk when an editor unbinds while the note stays open", async () => {
    // Obsidian tears down and immediately recreates the editor's view plugins on
    // mode switches; the transient unbind must not vault.modify an open file
    // (that surfaces as an external change Obsidian merges, duplicating text).
    const guid = freshGuid();
    const { plugin, vault } = makeFakePlugin(harness.authUrl, {
      sessionToken: token,
      activeVaultId: vaultId,
    });
    vault.files.set("note.md", "disk lags editor");
    (plugin.app.workspace as any).getLeavesOfType = () => [
      { view: { file: { path: "note.md" }, getMode: () => "source" } },
    ];

    const doc = new Document(plugin as any, "note.md", guid, docId(guid), true);
    const peer = new Peer(memberPlugin, docId(guid));
    try {
      await doc.whenReady();
      await peer.whenSynced();
      await waitFor(() => peer.getText() === "disk lags editor", { label: "seed synced" });

      // Editor holds newer content than disk (Obsidian hasn't autosaved yet).
      doc.bindEditor();
      peer.setText("disk lags editor +typed");
      await waitFor(() => doc.content === "disk lags editor +typed", { label: "edit reached doc" });

      doc.unbindEditor();
      await new Promise((r) => setTimeout(r, 250));

      // File still open => no write, so Obsidian never sees an external change.
      expect(vault.files.get("note.md")).toBe("disk lags editor");
    } finally {
      doc.destroy();
      peer.destroy();
    }
  });

  it("startup conflict: prompts and accepts local as the canonical version", async () => {
    const guid = freshGuid();
    const peer = new Peer(memberPlugin, docId(guid));

    // Phase 1 — establish a shared baseline "base" and persist it to A's IndexedDB.
    const a1 = makeDoc(guid, {
      file: { path: "note.md", content: "base" },
      clientName: "Brave Otter",
    });
    await a1.doc.whenReady();
    await peer.whenSynced();
    await waitFor(() => peer.getText() === "base", { label: "baseline synced" });
    await new Promise((r) => setTimeout(r, 250)); // let IndexedDB persist the baseline
    a1.doc.destroy();

    // Phase 2 — while A is gone, both sides diverge from the baseline.
    peer.setText("base REMOTE side");
    await peer.whenChangesSynced();
    a1.vault.files.set("note.md", "base LOCAL side"); // external offline edit on A's disk

    // Phase 3 — A restarts on the same guid + vault (baseline reloads from IndexedDB).
    const { plugin } = makeFakePlugin(harness.authUrl, {
      sessionToken: token,
      activeVaultId: vaultId,
      clientName: "Brave Otter",
    });
    (plugin.app.vault as FakeVault).files = a1.vault.files; // same on-disk state
    modalMock.delayMs = 500;
    const a2 = new Document(plugin as any, "note.md", guid, docId(guid), false);
    try {
      const ready = a2.whenReady();
      await waitFor(() => modalMock.calls.length === 1, { label: "conflict prompt opened" });
      await new Promise((resolve) => setTimeout(resolve, 250));
      expect((plugin.app.vault as FakeVault).files.get("note.md")).toBe("base LOCAL side");
      await ready;
      await waitFor(() => peer.getText() === a2.content && a2.content.length > 0, {
        timeout: 15_000,
        label: "A2 and B converge",
      });

      // Both sides agree on the explicitly selected local version.
      expect(a2.content).toBe(peer.getText());
      expect(a2.content).toBe("base LOCAL side");

      expect(modalMock.calls).toEqual([
        {
          path: "note.md",
          localContent: "base LOCAL side",
          remoteContent: "base REMOTE side",
        },
      ]);
      const conflicts = conflictFiles(plugin.app.vault as FakeVault);
      expect(conflicts).toHaveLength(1);
      expect((plugin.app.vault as FakeVault).files.get(conflicts[0])).toBe("base REMOTE side");
      expect(notices.some((n) => /kept your local version/i.test(n))).toBe(true);

      // The canonical text was written back to the live file.
      expect((plugin.app.vault as FakeVault).files.get("note.md")).toBe(a2.content);
    } finally {
      a2.destroy();
      peer.destroy();
    }
  });

  it("startup reconciliation auto-merges disjoint local and remote edits", async () => {
    const guid = freshGuid();
    const peer = new Peer(memberPlugin, docId(guid));
    const baseline = "title\nfirst\nsecond\n";
    const a1 = makeDoc(guid, { file: { path: "note.md", content: baseline } });
    await a1.doc.whenReady();
    await peer.whenSynced();
    await waitFor(() => peer.getText() === baseline, { label: "merge baseline synced" });
    await new Promise((resolve) => setTimeout(resolve, 250));
    a1.doc.destroy();

    peer.setText("title\nfirst\nremote second\n");
    await peer.whenChangesSynced();
    a1.vault.files.set("note.md", "local title\nfirst\nsecond\n");

    const { plugin } = makeFakePlugin(harness.authUrl, {
      sessionToken: token,
      activeVaultId: vaultId,
    });
    (plugin.app.vault as FakeVault).files = a1.vault.files;
    const a2 = new Document(plugin as any, "note.md", guid, docId(guid), false);
    try {
      await a2.whenReady();
      await waitFor(() => peer.getText() === "local title\nfirst\nremote second\n", {
        timeout: 15_000,
        label: "three-way merge converged",
      });
      expect(a2.content).toBe("local title\nfirst\nremote second\n");
      expect(modalMock.calls).toHaveLength(0);
      expect(conflictFiles(plugin.app.vault as FakeVault)).toHaveLength(0);
    } finally {
      a2.destroy();
      peer.destroy();
    }
  });

  it("restarts conflict resolution when the remote text changes behind the modal", async () => {
    const guid = freshGuid();
    const peer = new Peer(memberPlugin, docId(guid));
    const a1 = makeDoc(guid, { file: { path: "note.md", content: "base" } });
    await a1.doc.whenReady();
    await peer.whenSynced();
    await waitFor(() => peer.getText() === "base", { label: "live conflict baseline" });
    await new Promise((resolve) => setTimeout(resolve, 250));
    a1.doc.destroy();

    peer.setText("base REMOTE first");
    await peer.whenChangesSynced();
    a1.vault.files.set("note.md", "base LOCAL");
    const { plugin } = makeFakePlugin(harness.authUrl, {
      sessionToken: token,
      activeVaultId: vaultId,
    });
    (plugin.app.vault as FakeVault).files = a1.vault.files;
    modalMock.delayMs = 300;
    const restarted = new Document(plugin as any, "note.md", guid, docId(guid), false);
    try {
      const ready = restarted.whenReady();
      await waitFor(() => modalMock.calls.length === 1, {
        label: "first conflict prompt opened",
      });
      peer.setText("base REMOTE latest");
      await peer.whenChangesSynced();

      await waitFor(() => modalMock.calls.length === 2, {
        timeout: 10_000,
        label: "updated remote conflict prompted again",
      });
      await ready;
      await waitFor(() => peer.getText() === "base LOCAL", {
        label: "local choice applied after remote revalidation",
      });

      expect(modalMock.calls[1]?.remoteContent).toBe("base REMOTE latest");
      const copies = conflictFiles(plugin.app.vault as FakeVault);
      expect(copies).toHaveLength(1);
      expect((plugin.app.vault as FakeVault).files.get(copies[0])).toBe("base REMOTE latest");
    } finally {
      restarted.destroy();
      peer.destroy();
    }
  });

  it("startup conflict: accepts remote and preserves the local version", async () => {
    modalMock.choice = "remote";
    const guid = freshGuid();
    const peer = new Peer(memberPlugin, docId(guid));

    const a1 = makeDoc(guid, { file: { path: "note.md", content: "base" } });
    await a1.doc.whenReady();
    await peer.whenSynced();
    await waitFor(() => peer.getText() === "base", { label: "baseline synced" });
    await new Promise((r) => setTimeout(r, 250));
    a1.doc.destroy();

    peer.setText("base REMOTE side");
    await peer.whenChangesSynced();
    a1.vault.files.set("note.md", "base LOCAL side");

    const { plugin } = makeFakePlugin(harness.authUrl, {
      sessionToken: token,
      activeVaultId: vaultId,
      clientName: "Brave Otter",
    });
    (plugin.app.vault as FakeVault).files = a1.vault.files;
    const a2 = new Document(plugin as any, "note.md", guid, docId(guid), false);
    try {
      await a2.whenReady();
      await waitFor(() => peer.getText() === "base REMOTE side", {
        timeout: 15_000,
        label: "remote remains canonical",
      });

      expect(a2.content).toBe("base REMOTE side");
      expect(modalMock.calls).toHaveLength(1);
      const conflicts = conflictFiles(plugin.app.vault as FakeVault);
      expect(conflicts).toHaveLength(1);
      expect((plugin.app.vault as FakeVault).files.get(conflicts[0])).toBe("base LOCAL side");
      expect((plugin.app.vault as FakeVault).files.get("note.md")).toBe("base REMOTE side");
      expect(notices.some((n) => /kept the remote version/i.test(n))).toBe(true);
    } finally {
      a2.destroy();
      peer.destroy();
    }
  });

  it("startup reconcile fast-forwards a same-device partial remote prefix without prompting", async () => {
    const guid = freshGuid();
    const peer = new Peer(memberPlugin, docId(guid));
    await peer.whenSynced();
    peer.setText("draft");
    await peer.whenChangesSynced();

    const { plugin, vault } = makeFakePlugin(harness.authUrl, {
      sessionToken: token,
      activeVaultId: vaultId,
    });
    vault.files.set("note.md", "draft completed locally");
    const doc = new Document(plugin as any, "note.md", guid, docId(guid), false);
    try {
      await doc.whenReady();
      await waitFor(() => peer.getText() === "draft completed locally", {
        label: "local completion published",
      });

      expect(modalMock.calls).toHaveLength(0);
      expect(doc.content).toBe("draft completed locally");
      expect(vault.files.get("note.md")).toBe("draft completed locally");
    } finally {
      doc.destroy();
      peer.destroy();
    }
  });

  it("does not fast-forward stale prefix-extending disk text after an epoch rollover", async () => {
    modalMock.choice = "remote";
    const guid = freshGuid();
    const serverDocId = docId(guid);
    const peer = new Peer(memberPlugin, serverDocId);
    await peer.whenSynced();
    peer.setText("draft");
    await peer.whenChangesSynced();

    const { plugin, vault } = makeFakePlugin(harness.authUrl, {
      sessionToken: token,
      activeVaultId: vaultId,
    });
    vault.files.set("note.md", "draft completed before remote deletion");
    // Model a server that already rolled the document over to epoch 1.
    const docToken = plugin.auth.docToken.bind(plugin.auth);
    plugin.auth.docToken = async (...args: Parameters<typeof docToken>) => ({
      ...(await docToken(...args)),
      epoch: 1,
    });
    setDocumentEpoch(plugin as any, serverDocId, 1);
    const doc = new Document(plugin as any, "note.md", guid, serverDocId, false);
    try {
      await doc.whenReady();

      expect(modalMock.calls).toEqual([
        {
          path: "note.md",
          localContent: "draft completed before remote deletion",
          remoteContent: "draft",
        },
      ]);
      expect(peer.getText()).toBe("draft");
      expect(doc.content).toBe("draft");
      expect(vault.files.get("note.md")).toBe("draft");
      const conflicts = conflictFiles(vault);
      expect(conflicts).toHaveLength(1);
      expect(vault.files.get(conflicts[0])).toBe("draft completed before remote deletion");
    } finally {
      doc.destroy();
      peer.destroy();
    }
  });

  it("forces recovery when a durable path identity says the local file is unrelated", async () => {
    const guid = freshGuid();
    const peer = new Peer(memberPlugin, docId(guid));
    await peer.whenSynced();
    peer.setText("remote identity content");
    await peer.whenChangesSynced();

    const first = makeDoc(guid);
    try {
      await first.doc.whenReady();
      await waitFor(() => first.vault.files.get("note.md") === "remote identity content", {
        label: "remote identity materialized",
      });
      await new Promise((resolve) => setTimeout(resolve, 250));
    } finally {
      first.doc.destroy();
    }

    first.vault.files.set("note.md", "unrelated local content");
    const { plugin } = makeFakePlugin(harness.authUrl, {
      sessionToken: token,
      activeVaultId: vaultId,
    });
    (plugin.app.vault as FakeVault).files = first.vault.files;
    const restarted = new Document(plugin as any, "note.md", guid, docId(guid), false, {
      forceBootstrapConflict: true,
    });
    try {
      await restarted.whenReady();
      expect(modalMock.calls).toEqual([
        {
          path: "note.md",
          localContent: "unrelated local content",
          remoteContent: "remote identity content",
        },
      ]);
      expect(restarted.content).toBe("unrelated local content");
      const conflicts = conflictFiles(plugin.app.vault as FakeVault);
      expect(conflicts).toHaveLength(1);
      expect((plugin.app.vault as FakeVault).files.get(conflicts[0])).toBe(
        "remote identity content",
      );
    } finally {
      restarted.destroy();
      peer.destroy();
    }
  });

  it("does not overwrite a local note when the startup disk read fails transiently", async () => {
    const guid = freshGuid();
    const { plugin, vault } = makeFakePlugin(harness.authUrl, {
      sessionToken: token,
      activeVaultId: vaultId,
    });
    vault.files.set("note.md", "irreplaceable local text");
    const read = vault.read.bind(vault);
    let failures = 1;
    vault.read = async (file) => {
      if (failures > 0) {
        failures--;
        throw new Error("EBUSY: file is being synced by the OS");
      }
      return await read(file);
    };
    const peer = new Peer(memberPlugin, docId(guid));
    const doc = new Document(plugin as any, "note.md", guid, docId(guid), true);
    try {
      await doc.whenReady();
      await peer.whenSynced();
      await waitFor(() => peer.getText() === "irreplaceable local text", {
        timeout: 10_000,
        label: "local text seeded after the read recovered",
      });
      expect(vault.files.get("note.md")).toBe("irreplaceable local text");
      expect(doc.content).toBe("irreplaceable local text");
    } finally {
      doc.destroy();
      peer.destroy();
    }
  });

  it("creator startup: treats an empty first remote as unseeded, not a conflict", async () => {
    const guid = freshGuid();
    const peer = new Peer(memberPlugin, docId(guid));

    const a1 = makeDoc(guid, { file: { path: "note.md", content: "base" } });
    await a1.doc.whenReady();
    await peer.whenSynced();
    await waitFor(() => peer.getText() === "base", { label: "baseline synced" });
    await new Promise((r) => setTimeout(r, 250));
    a1.doc.destroy();

    peer.setText("");
    await peer.whenChangesSynced();
    a1.vault.files.set("note.md", "new local note body");

    const { plugin } = makeFakePlugin(harness.authUrl, {
      sessionToken: token,
      activeVaultId: vaultId,
      clientName: "Brave Otter",
    });
    (plugin.app.vault as FakeVault).files = a1.vault.files;
    const a2 = new Document(plugin as any, "note.md", guid, docId(guid), true);
    try {
      await a2.whenReady();
      await waitFor(() => peer.getText() === "new local note body", {
        timeout: 15_000,
        label: "creator local content reseeded empty remote",
      });

      expect(a2.content).toBe("new local note body");
      expect(modalMock.calls).toHaveLength(0);
      expect(conflictFiles(plugin.app.vault as FakeVault)).toHaveLength(0);
    } finally {
      a2.destroy();
      peer.destroy();
    }
  });

  it("non-creator startup: still prompts when the remote intentionally became empty", async () => {
    const guid = freshGuid();
    const peer = new Peer(memberPlugin, docId(guid));

    const a1 = makeDoc(guid, { file: { path: "note.md", content: "base" } });
    await a1.doc.whenReady();
    await peer.whenSynced();
    await waitFor(() => peer.getText() === "base", { label: "baseline synced" });
    await new Promise((r) => setTimeout(r, 250));
    a1.doc.destroy();

    peer.setText("");
    await peer.whenChangesSynced();
    a1.vault.files.set("note.md", "base LOCAL side");

    const { plugin } = makeFakePlugin(harness.authUrl, {
      sessionToken: token,
      activeVaultId: vaultId,
      clientName: "Brave Otter",
    });
    (plugin.app.vault as FakeVault).files = a1.vault.files;
    const a2 = new Document(plugin as any, "note.md", guid, docId(guid), false);
    try {
      await a2.whenReady();
      await waitFor(() => peer.getText() === "base LOCAL side", {
        timeout: 15_000,
        label: "local conflict choice synced over empty remote",
      });

      expect(modalMock.calls).toEqual([
        {
          path: "note.md",
          localContent: "base LOCAL side",
          remoteContent: "",
        },
      ]);
      expect(a2.content).toBe("base LOCAL side");
    } finally {
      a2.destroy();
      peer.destroy();
    }
  });

  it("restart durability: offline edits persist across a restart via IndexedDB", async () => {
    const guid = freshGuid();
    const a1 = makeDoc(guid, { file: { path: "note.md", content: "" } });
    await a1.doc.whenReady();
    await waitFor(() => a1.doc.provider.status === "connected", { label: "A1 connected" });

    // Go offline, then make a local edit that only IndexedDB captures.
    a1.doc.provider.disconnect();
    a1.vault.files.set("note.md", "written while offline");
    await a1.doc.onDiskChanged();
    await new Promise((r) => setTimeout(r, 250)); // persist to IndexedDB
    a1.doc.destroy();

    // Restart on the same guid; the offline edit must survive (no server needed).
    const { plugin } = makeFakePlugin(harness.authUrl, {
      sessionToken: token,
      activeVaultId: vaultId,
    });
    (plugin.app.vault as FakeVault).files = a1.vault.files;
    let releasePersistence!: () => void;
    const persistenceGate = new Promise<void>((resolve) => {
      releasePersistence = resolve;
    });
    class GatedDocument extends Document {
      protected override async afterPersistenceSynced(): Promise<void> {
        await persistenceGate;
        await super.afterPersistenceSynced();
      }
    }
    // VaultSync constructs queued documents disconnected, then may request a
    // connection immediately. The request must wait for IndexedDB replay.
    const a2 = new GatedDocument(plugin as any, "note.md", guid, docId(guid), false, {
      autoConnect: false,
    });
    const connectSpy = vi.spyOn(a2.provider, "connect");
    a2.connect();
    try {
      await new Promise((resolve) => setTimeout(resolve, 0));
      expect(connectSpy).not.toHaveBeenCalled();
      releasePersistence();
      await a2.whenReady();
      expect(connectSpy).toHaveBeenCalledTimes(1);
      expect(a2.content).toBe("written while offline");
    } finally {
      releasePersistence();
      a2.destroy();
    }
  });

  it("reconnect: ensureConnected revives a disconnected provider", async () => {
    const guid = freshGuid();
    const { doc, vault } = makeDoc(guid, { file: { path: "note.md", content: "hi" } });
    const peer = new Peer(memberPlugin, docId(guid));
    try {
      await doc.whenReady();
      await peer.whenSynced();
      await waitFor(() => doc.provider.status === "connected");
      await waitFor(() => peer.getText() === "hi", { label: "peer received initial text" });

      doc.provider.disconnect();
      await waitFor(() => doc.provider.status === "offline", { label: "went offline" });

      doc.ensureConnected();
      await waitFor(() => doc.provider.status === "connected", { label: "reconnected" });

      // Sync resumes after reconnect. The provider self-heals transient
      // reconnect races (stale-socket close events, token refetch) through
      // retry loops with backoff sleeps, so on a loaded CI box this can
      // legitimately take well over the default budget. Asserting the doc
      // and the disk separately pinpoints the failing layer if it recurs.
      peer.setText("after reconnect");
      await waitFor(() => doc.content === "after reconnect", {
        label: "update reached reconnected doc",
        timeout: 60_000,
      });
      await waitFor(() => vault.files.get("note.md") === "after reconnect", {
        label: "sync resumed",
        timeout: 60_000,
      });
    } finally {
      doc.destroy();
      peer.destroy();
    }
  });

  it("keeps a remote edit that reached IndexedDB but not disk before a restart", async () => {
    const guid = freshGuid();
    const { plugin, vault } = makeFakePlugin(harness.authUrl, {
      sessionToken: token,
      activeVaultId: vaultId,
    });
    const state = attachLocalSyncState(plugin);
    vault.files.set("note.md", "base\n");
    const first = new Document(plugin as any, "note.md", guid, docId(guid), true);
    const peer = new Peer(memberPlugin, docId(guid));
    try {
      await first.whenReady();
      await peer.whenSynced();
      await waitFor(() => peer.getText() === "base\n", { label: "peer has base" });
      const baseFingerprint = await storedContentFingerprint(first.epoch, "base\n");
      await waitFor(() => state.diskFingerprint("note.md", guid) === baseFingerprint, {
        label: "disk state recorded",
      });

      // Another device edits the (locally closed) note; this device quits
      // inside the disk-write debounce, so only IndexedDB has the edit.
      peer.setText("base\nremote paragraph\n");
      await waitFor(() => first.content === "base\nremote paragraph\n", {
        label: "remote edit reached the Y.Doc",
      });
      await new Promise((r) => setTimeout(r, 30));
      first.destroy();
      expect(vault.files.get("note.md")).toBe("base\n");

      const second = new Document(plugin as any, "note.md", guid, docId(guid), false);
      try {
        await second.whenReady();
        await waitFor(() => vault.files.get("note.md") === "base\nremote paragraph\n", {
          label: "stale disk caught up with the document",
        });
        expect(second.content).toBe("base\nremote paragraph\n");
        expect(peer.getText()).toBe("base\nremote paragraph\n");
      } finally {
        second.destroy();
      }
    } finally {
      first.destroy();
      peer.destroy();
      state.destroy();
    }
  });

  it("still folds a disk edit made while the plugin was not running", async () => {
    const guid = freshGuid();
    const { plugin, vault } = makeFakePlugin(harness.authUrl, {
      sessionToken: token,
      activeVaultId: vaultId,
    });
    const state = attachLocalSyncState(plugin);
    vault.files.set("note.md", "base\n");
    const first = new Document(plugin as any, "note.md", guid, docId(guid), true);
    const peer = new Peer(memberPlugin, docId(guid));
    try {
      await first.whenReady();
      await peer.whenSynced();
      await waitFor(() => peer.getText() === "base\n", { label: "peer has base" });
      const baseFingerprint = await storedContentFingerprint(first.epoch, "base\n");
      await waitFor(() => state.diskFingerprint("note.md", guid) === baseFingerprint, {
        label: "disk state recorded",
      });
      first.destroy();

      vault.files.set("note.md", "base\nedited while closed\n");
      const second = new Document(plugin as any, "note.md", guid, docId(guid), false);
      try {
        await second.whenReady();
        await waitFor(() => peer.getText() === "base\nedited while closed\n", {
          label: "offline disk edit published",
        });
      } finally {
        second.destroy();
      }
    } finally {
      first.destroy();
      peer.destroy();
      state.destroy();
    }
  });

  it("does not trust disk state a retired epoch's store recorded", async () => {
    const guid = freshGuid();
    const { plugin, vault } = makeFakePlugin(harness.authUrl, {
      sessionToken: token,
      activeVaultId: vaultId,
    });
    const state = attachLocalSyncState(plugin);
    vault.files.set("note.md", "base\n");
    const retired = new Document(plugin as any, "note.md", guid, docId(guid), true);
    const replacements: Document[] = [];
    const startReplacement = async (): Promise<Document> => {
      const doc = new Document(plugin as any, "note.md", guid, docId(guid), false, {
        autoConnect: false,
      });
      replacements.push(doc);
      await waitFor(() => (doc as any).persistenceReady === true, {
        label: "replacement store loaded",
      });
      return doc;
    };
    try {
      await retired.whenReady();
      // An offline edit is folded into the retired store (and recorded as
      // held there) but never uploaded before the epoch moves on.
      retired.disconnect();
      vault.files.set("note.md", "base\noffline edit\n");
      await retired.onDiskChanged();
      const recorded = await storedContentFingerprint(retired.epoch, "base\noffline edit\n");
      await waitFor(() => state.diskFingerprint("note.md", guid) === recorded, {
        label: "offline edit recorded as stored",
      });
      retired.destroy();
      setDocumentEpoch(plugin as any, docId(guid), 1);

      // The new epoch's store starts empty: the file is a local edit.
      const fresh = await startReplacement();
      expect((fresh as any).localChangedAtStartup).toBe(true);

      // Still one after the replacement's content reached the new store, if
      // the app stopped before the first reconcile folded the edit in.
      const replacement = new Y.Doc();
      replacement.getText("contents").insert(0, "base\n");
      Y.applyUpdate(fresh.ydoc, Y.encodeStateAsUpdate(replacement), fresh.provider);
      await (fresh as any).flushPersistence();
      fresh.destroy();
      const restarted = await startReplacement();
      expect(restarted.content).toBe("base\n");
      expect((restarted as any).localChangedAtStartup).toBe(true);
    } finally {
      retired.destroy();
      for (const doc of replacements) doc.destroy();
      state.destroy();
    }
  });

  it("takes the remote when an empty store meets a file holding acknowledged content", async () => {
    const guid = freshGuid();
    const { plugin, vault } = makeFakePlugin(harness.authUrl, {
      sessionToken: token,
      activeVaultId: vaultId,
    });
    const state = attachLocalSyncState(plugin);
    vault.files.set("note.md", "line one\nline two\n");
    const first = new Document(plugin as any, "note.md", guid, docId(guid), true);
    const peer = new Peer(memberPlugin, docId(guid));
    let second: Document | null = null;
    try {
      await first.whenReady();
      const acknowledged = await sha256Text("line one\nline two\n");
      await waitFor(() => state.acknowledgedFingerprint("note.md", guid) === acknowledged, {
        label: "content acknowledged",
      });
      first.destroy();
      // The store is gone (a new epoch, or evicted storage) and another
      // device changed an existing line meanwhile.
      await new Promise<void>((resolve, reject) => {
        const request = indexedDB.deleteDatabase(docId(guid));
        request.onsuccess = () => resolve();
        request.onerror = () => reject(request.error);
      });
      await peer.whenSynced();
      peer.setText("line ONE\nline two\n");
      await peer.whenChangesSynced();

      second = new Document(plugin as any, "note.md", guid, docId(guid), false);
      await second.whenReady();
      await waitFor(() => vault.files.get("note.md") === "line ONE\nline two\n", {
        label: "remote written to disk",
      });
      expect(modalMock.calls).toHaveLength(0);
      expect(second.content).toBe("line ONE\nline two\n");
    } finally {
      first.destroy();
      second?.destroy();
      peer.destroy();
      state.destroy();
    }
  });

  it("merges against the retired epoch's content when the new store starts empty", async () => {
    const guid = freshGuid();
    const { plugin, vault } = makeFakePlugin(harness.authUrl, {
      sessionToken: token,
      activeVaultId: vaultId,
    });
    const state = attachLocalSyncState(plugin);
    vault.files.set("note.md", "line one\nline two\n");
    const first = new Document(plugin as any, "note.md", guid, docId(guid), true);
    const peer = new Peer(memberPlugin, docId(guid));
    let second: Document | null = null;
    try {
      await first.whenReady();
      await waitFor(() => !first.provider.hasLocalChanges, { label: "first acknowledged" });
      const retiredBaseline = first.retiredBaseline();
      expect(retiredBaseline).toBe("line one\nline two\n");
      first.destroy();
      await new Promise<void>((resolve, reject) => {
        const request = indexedDB.deleteDatabase(docId(guid));
        request.onsuccess = () => resolve();
        request.onerror = () => reject(request.error);
      });
      // Remote and local each changed a different line.
      await peer.whenSynced();
      peer.setText("line ONE\nline two\n");
      await peer.whenChangesSynced();
      vault.files.set("note.md", "line one\nline two, edited here\n");

      second = new Document(plugin as any, "note.md", guid, docId(guid), false, {
        retiredBaseline,
      });
      await second.whenReady();
      const merged = "line ONE\nline two, edited here\n";
      await waitFor(() => peer.getText() === merged, { label: "merge reached the peer" });
      expect(second.content).toBe(merged);
      expect(vault.files.get("note.md")).toBe(merged);
      expect(modalMock.calls).toHaveLength(0);
    } finally {
      first.destroy();
      second?.destroy();
      peer.destroy();
      state.destroy();
    }
  });

  it("does not trust recorded disk state once the document's store is gone", async () => {
    const guid = freshGuid();
    const { plugin, vault } = makeFakePlugin(harness.authUrl, {
      sessionToken: token,
      activeVaultId: vaultId,
    });
    const state = attachLocalSyncState(plugin);
    vault.files.set("note.md", "base\n");
    const first = new Document(plugin as any, "note.md", guid, docId(guid), true);
    let second: Document | null = null;
    try {
      await first.whenReady();
      first.disconnect();
      vault.files.set("note.md", "base\noffline edit\n");
      await first.onDiskChanged();
      const recorded = await storedContentFingerprint(first.epoch, "base\noffline edit\n");
      await waitFor(() => state.diskFingerprint("note.md", guid) === recorded, {
        label: "offline edit recorded as stored",
      });
      first.destroy();
      await new Promise<void>((resolve, reject) => {
        const request = indexedDB.deleteDatabase(docId(guid));
        request.onsuccess = () => resolve();
        request.onerror = () => reject(request.error);
      });

      second = new Document(plugin as any, "note.md", guid, docId(guid), false, {
        autoConnect: false,
      });
      await waitFor(() => (second as any).persistenceReady === true, { label: "store loaded" });
      expect(second.content).toBe("");
      expect((second as any).localChangedAtStartup).toBe(true);
    } finally {
      first.destroy();
      second?.destroy();
      state.destroy();
    }
  });

  it("does not fold an older write's echo back while slow writes overlap remote edits", async () => {
    const guid = freshGuid();
    const { plugin, vault } = makeFakePlugin(harness.authUrl, {
      sessionToken: token,
      activeVaultId: vaultId,
    });
    vault.files.set("note.md", "v0");
    const doc = new Document(plugin as any, "note.md", guid, docId(guid), true);
    const peer = new Peer(memberPlugin, docId(guid));
    try {
      await doc.whenReady();
      await peer.whenSynced();
      await waitFor(() => peer.getText() === "v0", { label: "peer has v0" });

      // Slow storage: each modify lands 300ms after it starts. VaultSync
      // forwards every vault modify event to onDiskChanged.
      const original = vault.modify.bind(vault);
      vault.modify = async (file, text) => {
        await new Promise((r) => setTimeout(r, 300));
        await original(file, text);
      };
      let typed = false;
      vault.on("modify", () => {
        if (vault.files.get("note.md") === "v1" && !typed) {
          typed = true;
          setTimeout(() => peer.text.insert(peer.text.length, " then v3"), 60);
        }
        void doc.onDiskChanged();
      });

      peer.setText("v1");
      await waitFor(() => doc.content === "v1", { label: "v1 arrived" });
      await new Promise((r) => setTimeout(r, 150));
      peer.setText("v1 then v2");

      await waitFor(() => typed && peer.getText().endsWith(" then v3"), {
        label: "peer kept typing",
      });
      await waitFor(() => vault.files.get("note.md") === doc.content, {
        label: "disk settled",
        timeout: 10_000,
      });
      expect(doc.content).toBe("v1 then v2 then v3");
      expect(peer.getText()).toBe("v1 then v2 then v3");
    } finally {
      doc.destroy();
      peer.destroy();
    }
  });

  it("merges a disk edit with remote changes that had not reached disk yet", async () => {
    const guid = freshGuid();
    const { doc, vault } = makeDoc(guid, {
      file: { path: "note.md", content: "line one\nline two\n" },
    });
    const peer = new Peer(memberPlugin, docId(guid));
    try {
      await doc.whenReady();
      await peer.whenSynced();
      await waitFor(() => peer.getText() === "line one\nline two\n", { label: "seed synced" });

      // Hold the remote change's disk write so the file lags the document.
      let release!: () => void;
      const held = new Promise<void>((resolve) => (release = resolve));
      const original = vault.modify.bind(vault);
      vault.modify = async (file, text) => {
        await held;
        await original(file, text);
      };
      peer.setText("LINE ONE\nline two\n");
      await waitFor(() => doc.content === "LINE ONE\nline two\n", { label: "remote arrived" });

      vault.files.set("note.md", "line one\nline two, edited on disk\n");
      await doc.onDiskChanged();
      release();

      await waitFor(() => peer.getText() === "LINE ONE\nline two, edited on disk\n", {
        label: "both edits kept",
      });
      await waitFor(() => vault.files.get("note.md") === "LINE ONE\nline two, edited on disk\n", {
        label: "disk has the merge",
      });
      expect(conflictFiles(vault)).toHaveLength(0);
    } finally {
      doc.destroy();
      peer.destroy();
    }
  });
});
