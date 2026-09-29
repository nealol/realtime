import { afterAll, beforeAll, describe, expect, it } from "vitest";
import * as Y from "yjs";
import { BaseDocument } from "../../src/BaseDocument";
import { RealtimeProvider } from "../../src/sync/RealtimeProvider";
import { getClientToken } from "../../src/sync/clientToken";
import { LocalSyncState } from "../../src/localSyncState";
import { parseBase, serializeBase } from "../../src/structured/base";
import { reconcileInto, toValue, type JsonValue } from "../../src/structured/reconcile";
import { storedContentFingerprint } from "../../src/SyncedDoc";
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
  };
  return state;
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
});
