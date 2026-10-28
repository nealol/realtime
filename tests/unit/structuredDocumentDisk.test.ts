import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import * as Y from "yjs";
import { BaseDocument } from "../../src/BaseDocument";
import { RealtimeProvider, SYNC_EVENT_LOCAL_CHANGES } from "../../src/sync/RealtimeProvider";
import { getClientToken } from "../../src/sync/clientToken";
import { LocalSyncState } from "../../src/localSyncState";
import { parseBase, serializeBase } from "../../src/structured/base";
import { reconcileInto, toValue, type JsonValue } from "../../src/structured/reconcile";
import { storedContentFingerprint } from "../../src/SyncedDoc";
import { sha256Text } from "../../src/hash";
import type * as Hash from "../../src/hash";
import { makeFakePlugin, type FakePlugin } from "../support/fakePlugin";
import { startAuthHarness, type AuthHarness } from "../support/authServer";
import { freshGuid, waitFor } from "../support/util";

let harness: AuthHarness;
let token: string;
let vaultId: string;
let peerPlugin: FakePlugin;

beforeAll(async () => {
  harness = await startAuthHarness();
  token = await harness.loginUser("structured-disk");
  vaultId = (await harness.createVault(token, "structured-disk")).id;
  peerPlugin = makeFakePlugin(harness.authUrl, {
    sessionToken: token,
    activeVaultId: vaultId,
  }).plugin;
}, 180_000);

afterAll(async () => {
  await harness?.stop();
});

/** Holds content fingerprints in flight until released. */
const hashGate = vi.hoisted(() => {
  const gate = {
    hashed: [] as string[],
    held: null as Promise<void> | null,
    release: () => {},
    hold(): void {
      const { promise, resolve } = Promise.withResolvers<void>();
      gate.held = promise;
      gate.release = () => {
        gate.held = null;
        resolve();
      };
    },
  };
  return gate;
});

vi.mock("../../src/hash", async (importOriginal) => {
  const actual = await importOriginal<typeof Hash>();
  return {
    ...actual,
    sha256Text: async (text: string) => {
      hashGate.hashed.push(text);
      if (hashGate.held) await hashGate.held;
      return actual.sha256Text(text);
    },
  };
});

afterEach(() => {
  hashGate.release();
  hashGate.hashed.length = 0;
});

const docId = (guid: string) => `${vaultId}__${guid}`;

/** Another device editing the Base's structured root directly. */
class StructuredPeer {
  readonly doc = new Y.Doc();
  readonly provider: RealtimeProvider;

  constructor(serverDocId: string) {
    this.provider = new RealtimeProvider(serverDocId, this.doc, () =>
      getClientToken(peerPlugin as any, serverDocId),
    );
  }

  get value(): JsonValue {
    return toValue(this.doc.getMap("root"));
  }

  set(value: JsonValue): void {
    this.doc.transact(() => reconcileInto(this.doc.getMap("root"), value));
  }

  destroy(): void {
    this.provider.destroy();
    this.doc.destroy();
  }
}

function attachLocalSyncState(plugin: FakePlugin): LocalSyncState {
  const state = new LocalSyncState(`structured-test:${freshGuid()}`);
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

/** Record the fingerprints the document reports as acknowledged. */
function recordAcknowledgements(plugin: FakePlugin): string[] {
  const recorded: string[] = [];
  const vaultSync = (plugin as any).vaultSync as {
    noteContentAcknowledged: (...args: [string, string, string, string, boolean?]) => void;
  };
  const note = vaultSync.noteContentAcknowledged;
  vaultSync.noteContentAcknowledged = (...args) => {
    recorded.push(args[3]);
    note(...args);
  };
  return recorded;
}

describe("StructuredDocument disk sync", () => {
  it("keeps a remote change that reached IndexedDB but not disk before a restart", async () => {
    const guid = freshGuid();
    const { plugin, vault } = makeFakePlugin(harness.authUrl, {
      sessionToken: token,
      activeVaultId: vaultId,
    });
    const state = attachLocalSyncState(plugin);
    const base = { formulas: { a: "1" } };
    vault.files.set("views.base", serializeBase(base));
    const first = new BaseDocument(plugin as any, "views.base", guid, docId(guid), true);
    const peer = new StructuredPeer(docId(guid));
    try {
      await first.whenReady();
      await waitFor(() => JSON.stringify(peer.value) === JSON.stringify(base), {
        label: "peer has the base",
      });
      const fingerprint = await storedContentFingerprint(first.epoch, serializeBase(base));
      await waitFor(() => state.diskFingerprint("views.base", guid) === fingerprint, {
        label: "disk state recorded",
      });

      const remote = { formulas: { a: "1", b: "2" } };
      peer.set(remote);
      await waitFor(() => JSON.stringify(first.value) === JSON.stringify(remote), {
        label: "remote change reached the document",
      });
      await new Promise((r) => setTimeout(r, 30));
      first.destroy();
      expect(parseBase(vault.files.get("views.base")!)).toEqual(base);

      const second = new BaseDocument(plugin as any, "views.base", guid, docId(guid), false);
      try {
        await second.whenReady();
        await waitFor(
          () =>
            JSON.stringify(parseBase(vault.files.get("views.base")!)) === JSON.stringify(remote),
          {
            label: "stale disk caught up",
          },
        );
        expect(second.value).toEqual(remote);
        expect(peer.value).toEqual(remote);
      } finally {
        second.destroy();
      }
    } finally {
      first.destroy();
      peer.destroy();
      state.destroy();
    }
  });

  it("preserves one conflict copy and keeps remote edits that land while writing it", async () => {
    const guid = freshGuid();
    const { plugin, vault } = makeFakePlugin(harness.authUrl, {
      sessionToken: token,
      activeVaultId: vaultId,
    });
    const state = attachLocalSyncState(plugin);
    vault.files.set("conflict.base", serializeBase({ formulas: { a: "1" } }));
    const first = new BaseDocument(plugin as any, "conflict.base", guid, docId(guid), true);
    const peer = new StructuredPeer(docId(guid));
    let second: BaseDocument | null = null;
    try {
      await first.whenReady();
      await waitFor(() => JSON.stringify(peer.value) === '{"formulas":{"a":"1"}}', {
        label: "peer has the base",
      });
      first.destroy();

      // Offline, both sides change the same formula.
      peer.set({ formulas: { a: "remote" } });
      vault.files.set("conflict.base", serializeBase({ formulas: { a: "local" } }));

      // Another remote edit arrives while the recovery copy is being written.
      let copies = 0;
      const create = vault.create.bind(vault);
      vault.create = async (path: string, text: string) => {
        if (/conflicted copy/.test(path)) {
          copies += 1;
          if (copies === 1) {
            peer.set({ formulas: { a: "remote", b: "added" } });
            await waitFor(() => JSON.stringify(second?.value).includes("added"), {
              label: "concurrent remote edit arrived",
            });
          }
        }
        return create(path, text);
      };

      second = new BaseDocument(plugin as any, "conflict.base", guid, docId(guid), false);
      await second.whenReady();
      const expected = { formulas: { a: "local", b: "added" } };
      await waitFor(() => JSON.stringify(peer.value) === JSON.stringify(expected), {
        label: "peer sees the merged result",
      });
      expect(second.value).toEqual(expected);
      await new Promise((r) => setTimeout(r, 2_500));
      expect(copies).toBe(1);
    } finally {
      second?.destroy();
      first.destroy();
      peer.destroy();
      state.destroy();
    }
  });

  it("does not fold an older write's echo back while slow writes overlap remote edits", async () => {
    const guid = freshGuid();
    const { plugin, vault } = makeFakePlugin(harness.authUrl, {
      sessionToken: token,
      activeVaultId: vaultId,
    });
    vault.files.set("views.base", serializeBase({ step: "v0" }));
    const doc = new BaseDocument(plugin as any, "views.base", guid, docId(guid), true);
    const peer = new StructuredPeer(docId(guid));
    try {
      await doc.whenReady();
      await waitFor(() => JSON.stringify(peer.value) === JSON.stringify({ step: "v0" }), {
        label: "peer has v0",
      });

      // Slow storage: each modify lands 300ms after it starts. VaultSync
      // forwards every vault modify event to onDiskChanged.
      const original = vault.modify.bind(vault);
      vault.modify = async (file, text) => {
        await new Promise((r) => setTimeout(r, 300));
        await original(file, text);
      };
      vault.on("modify", () => void doc.onDiskChanged());

      peer.set({ step: "v1" });
      await waitFor(() => JSON.stringify(doc.value) === JSON.stringify({ step: "v1" }), {
        label: "v1 arrived",
      });
      await new Promise((r) => setTimeout(r, 150)); // the v1 write is in flight
      const seenByPeer: string[] = [];
      peer.doc.getMap("root").observeDeep(() => seenByPeer.push(JSON.stringify(peer.value)));
      peer.set({ step: "v2" });

      await new Promise((r) => setTimeout(r, 2_000));
      expect(seenByPeer).not.toContain(JSON.stringify({ step: "v1" }));
      expect(doc.value).toEqual({ step: "v2" });
      expect(peer.value).toEqual({ step: "v2" });
      expect(parseBase(vault.files.get("views.base")!)).toEqual({ step: "v2" });
    } finally {
      doc.destroy();
      peer.destroy();
    }
  });

  it("merges remote edits that arrive while a disk edit is being folded", async () => {
    const guid = freshGuid();
    const { plugin, vault } = makeFakePlugin(harness.authUrl, {
      sessionToken: token,
      activeVaultId: vaultId,
    });
    vault.files.set("views.base", serializeBase({ formulas: { a: "1" } }));
    const doc = new BaseDocument(plugin as any, "views.base", guid, docId(guid), true);
    const collaborator = new Y.Doc();
    try {
      await doc.whenReady();
      Y.applyUpdate(collaborator, Y.encodeStateAsUpdate(doc.ydoc));
      vault.files.set("views.base", serializeBase({ formulas: { a: "1", local: "2" } }));
      const read = vault.read.bind(vault);
      let reads = 0;
      vault.read = async (file) => {
        const text = await read(file);
        reads += 1;
        if (reads === 2) {
          // A collaborator's edit lands while the fold reads the file again.
          const before = Y.encodeStateVector(doc.ydoc);
          collaborator.transact(() =>
            reconcileInto(collaborator.getMap("root"), { formulas: { a: "1", remote: "3" } }),
          );
          Y.applyUpdate(doc.ydoc, Y.encodeStateAsUpdate(collaborator, before), doc.provider);
        }
        return text;
      };

      await doc.onDiskChanged();

      expect(doc.value).toEqual({ formulas: { a: "1", local: "2", remote: "3" } });
    } finally {
      doc.destroy();
      collaborator.destroy();
    }
  });
});

describe("StructuredDocument acknowledged content", () => {
  /**
   * A ready Base over `value`. `remoteSet` applies a collaborator's change
   * and `acknowledge` reports that the server holds every local change, both
   * without a server round trip.
   */
  async function startAcknowledging(value: JsonValue) {
    const guid = freshGuid();
    const { plugin, vault } = makeFakePlugin(harness.authUrl, {
      sessionToken: token,
      activeVaultId: vaultId,
    });
    const state = attachLocalSyncState(plugin);
    const recorded = recordAcknowledgements(plugin);
    vault.files.set("views.base", serializeBase(value));
    const doc = new BaseDocument(plugin as any, "views.base", guid, docId(guid), true);
    const remote = new Y.Doc();
    const acknowledged = () => state.acknowledgedFingerprint("views.base", guid);
    const cleanup = () => {
      doc.destroy();
      remote.destroy();
      state.destroy();
    };
    try {
      await doc.whenReady();
      const initial = await sha256Text(serializeBase(doc.value));
      await waitFor(() => acknowledged() === initial, { label: "startup content acknowledged" });
    } catch (error) {
      cleanup();
      throw error;
    }
    Y.applyUpdate(remote, Y.encodeStateAsUpdate(doc.ydoc));
    const remoteSet = (next: JsonValue) => {
      const before = Y.encodeStateVector(doc.ydoc);
      remote.transact(() => reconcileInto(remote.getMap("root"), next));
      Y.applyUpdate(doc.ydoc, Y.encodeStateAsUpdate(remote, before), doc.provider);
    };
    const acknowledge = () => (doc.provider as any).emit(SYNC_EVENT_LOCAL_CHANGES, false);
    return { doc, recorded, acknowledged, remoteSet, acknowledge, cleanup };
  }

  it("keeps a fingerprint across transactions that change no content", async () => {
    const { doc, acknowledged, remoteSet, acknowledge, cleanup } = await startAcknowledging({
      formulas: { a: "1" },
    });
    try {
      hashGate.hold();
      remoteSet({ formulas: { a: "1", b: "2" } });
      const content = serializeBase(doc.value);
      acknowledge();
      await waitFor(() => hashGate.hashed.includes(content), { label: "content hashing" });
      doc.ydoc.transact(() => {});
      doc.ydoc.transact(() => reconcileInto(doc.root, doc.value));
      hashGate.release();

      const fingerprint = await sha256Text(content);
      await waitFor(() => acknowledged() === fingerprint, { label: "content acknowledged" });
      expect(serializeBase(doc.value)).toBe(content);
    } finally {
      cleanup();
    }
  });

  it("never records a value that changed while it was fingerprinted", async () => {
    const { doc, recorded, acknowledged, remoteSet, acknowledge, cleanup } =
      await startAcknowledging({ formulas: { a: "1" } });
    try {
      hashGate.hold();
      remoteSet({ formulas: { a: "1", b: "2" } });
      const stale = serializeBase(doc.value);
      acknowledge();
      await waitFor(() => hashGate.hashed.includes(stale), { label: "stale value hashing" });
      remoteSet({ formulas: { a: "1", c: "3" } });
      const newer = serializeBase(doc.value);
      hashGate.release();

      // No further acknowledgement arrives: the document retries on its own.
      const latest = await sha256Text(newer);
      await waitFor(() => acknowledged() === latest, { label: "newer value acknowledged" });
      expect(recorded).not.toContain(await sha256Text(stale));
    } finally {
      cleanup();
    }
  });
});
