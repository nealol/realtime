import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import * as Y from "yjs";
import { RealtimeProvider, type SyncSocket } from "../../src/sync/RealtimeProvider";
import { startAuthHarness, type AuthHarness } from "../support/authServer";
import { makeFakePlugin, type FakePlugin } from "../support/fakePlugin";
import { freshGuid, waitFor } from "../support/util";
import {
  resetDocumentEpochStateForTests,
  setDocumentEpoch,
  setEpochProposalHandler,
} from "../../src/documentEpoch";
import { getClientToken, resetTokenRetryStateForTests } from "../../src/sync/clientToken";
import { createMuxSocket, resetMuxForTests } from "../../src/sync/mux";

let harness: AuthHarness;
let token: string;
let vaultId: string;
let plugin: FakePlugin;

class EpochClient {
  doc!: Y.Doc;
  provider!: RealtimeProvider;

  constructor(readonly documentId: string) {
    this.connect();
  }

  get text(): Y.Text {
    return this.doc.getText("contents");
  }

  rebuild(): void {
    this.provider.destroy();
    this.doc.destroy();
    this.connect();
  }

  destroy(): void {
    this.provider.destroy();
    this.doc.destroy();
  }

  private connect(): void {
    this.doc = new Y.Doc();
    this.provider = new RealtimeProvider(
      this.documentId,
      this.doc,
      () => getClientToken(plugin as any, this.documentId),
      { socketFactory: createMuxSocket },
    );
  }
}

beforeAll(async () => {
  harness = await startAuthHarness({
    env: {
      // Two initial SyncStep2 responses plus these clients' two edits.
      // Keep enough headroom that reconnecting both fresh documents does not
      // immediately trigger a second rollover.
      CRDT_EPOCH_MAX_UPDATES: "4",
      CRDT_EPOCH_MAX_STATE_BYTES: "536870912",
      CRDT_EPOCH_MAX_DELETE_SET_BYTES: "536870912",
      // Peers that never acknowledge hold a proposal open for this long.
      CRDT_EPOCH_ACK_TIMEOUT_MS: "3000",
    },
  });
  token = await harness.loginUser("epoch-client");
  const vault = await harness.createVault(token, "epoch-rollover");
  vaultId = vault.id;
  ({ plugin } = makeFakePlugin(harness.authUrl, {
    sessionToken: token,
    activeVaultId: vaultId,
  }));
}, 180_000);

afterAll(async () => {
  await harness?.stop();
});

afterEach(() => {
  setEpochProposalHandler(null);
  resetDocumentEpochStateForTests();
  resetMuxForTests();
});

async function tokenEpoch(documentId: string): Promise<number> {
  return (await plugin.auth.docToken(vaultId, documentId)).epoch ?? 0;
}

/** The epoch the server has activated (not merely proposed). */
async function activeEpoch(documentId: string): Promise<number> {
  const token = await plugin.auth.docToken(vaultId, documentId);
  const epoch = token.epoch ?? 0;
  return token.epochPending ? epoch - 1 : epoch;
}

/**
 * A direct socket that drops epoch proposals, like a peer that never
 * acknowledges them (a remote-cursor stream session, an older client).
 */
function nonAckingSocket(url: string): SyncSocket {
  const ws = new WebSocket(url);
  ws.binaryType = "arraybuffer";
  const socket = {
    binaryType: "arraybuffer",
    get readyState() {
      return ws.readyState;
    },
    onopen: null as ((event: unknown) => void) | null,
    onmessage: null as ((event: { data: unknown }) => void) | null,
    onclose: null as ((event: unknown) => void) | null,
    onerror: null as ((event: unknown) => void) | null,
    send: (data: Uint8Array | ArrayBuffer) => ws.send(data),
    close: () => ws.close(),
  };
  ws.onopen = (event) => socket.onopen?.(event);
  ws.onmessage = (event) => {
    if (new Uint8Array(event.data as ArrayBuffer)[0] === 103) return;
    socket.onmessage?.({ data: event.data });
  };
  ws.onclose = (event) => socket.onclose?.(event);
  ws.onerror = (event) => socket.onerror?.(event);
  return socket as unknown as SyncSocket;
}

describe("document epoch rollover (tier-2)", () => {
  it("replaces both real mux clients, preserves content, and accepts later writes", async () => {
    resetTokenRetryStateForTests(20);
    const documentId = `${vaultId}__${freshGuid()}`;
    const clients: EpochClient[] = [];
    let restartQueued = false;

    setEpochProposalHandler((proposedDocumentId, epoch) => {
      if (proposedDocumentId !== documentId) return;
      if (!setDocumentEpoch(plugin as any, documentId, epoch) || restartQueued) return;
      restartQueued = true;
      window.setTimeout(() => {
        for (const client of clients) client.rebuild();
        restartQueued = false;
      }, 0);
    });

    const first = new EpochClient(documentId);
    const second = new EpochClient(documentId);
    clients.push(first, second);
    const originalDocs = clients.map((client) => client.doc);

    try {
      await waitFor(() => clients.every((client) => client.provider.status === "connected"), {
        label: "epoch clients connected",
      });

      first.text.insert(0, "alpha");
      await waitFor(() => second.text.toString() === "alpha", { label: "first update converged" });
      second.text.insert(second.text.length, " beta");

      await waitFor(async () => (await tokenEpoch(documentId)) === 1, {
        label: "server activated epoch one",
        timeout: 20_000,
      });
      await waitFor(
        () =>
          clients.every(
            (client, index) =>
              client.doc !== originalDocs[index] &&
              client.provider.status === "connected" &&
              client.text.toString() === "alpha beta",
          ),
        { label: "fresh clients synchronized replacement epoch", timeout: 20_000 },
      );

      first.text.insert(first.text.length, " after");
      await waitFor(() => second.text.toString() === "alpha beta after", {
        label: "post-rollover update converged",
      });
      expect(first.text.toString()).toBe("alpha beta after");
    } finally {
      for (const client of clients) client.destroy();
    }
  }, 60_000);

  it("does not duplicate content when a peer holds the proposal open", async () => {
    resetTokenRetryStateForTests(20);
    const documentId = `${vaultId}__${freshGuid()}`;
    const client = new EpochClient(documentId);
    const original = client.doc;
    let rebuilt = false;
    const accept = (proposedDocumentId: string, epoch: number) => {
      if (proposedDocumentId !== documentId) return;
      if (!setDocumentEpoch(plugin as any, documentId, epoch) || rebuilt) return;
      rebuilt = true;
      window.setTimeout(() => client.rebuild(), 0);
    };
    setEpochProposalHandler(accept);
    (plugin as any).acceptDocumentEpoch = accept;

    const quietDoc = new Y.Doc();
    const quiet = new RealtimeProvider(
      documentId,
      quietDoc,
      () => plugin.auth.docToken(vaultId, documentId),
      { socketFactory: nonAckingSocket },
    );
    try {
      await waitFor(() => client.provider.status === "connected" && quiet.status === "connected", {
        label: "both peers connected",
      });
      original.getText("contents").insert(0, "alpha");
      await waitFor(() => quietDoc.getText("contents").toString() === "alpha", {
        label: "quiet peer caught up",
      });
      quietDoc.getText("contents").insert(5, " beta");
      await waitFor(() => rebuilt, { label: "client accepted the proposal", timeout: 20_000 });

      await waitFor(async () => (await activeEpoch(documentId)) === 1, {
        label: "server activated epoch one after the ack timeout",
        timeout: 20_000,
        interval: 250,
      });
      await waitFor(
        () =>
          client.doc !== original &&
          client.provider.status === "connected" &&
          client.text.toString().length > 0,
        { label: "client joined the replacement epoch", timeout: 20_000 },
      );
      await new Promise((r) => setTimeout(r, 500));

      const readerDoc = new Y.Doc();
      const reader = new RealtimeProvider(documentId, readerDoc, () =>
        plugin.auth.docToken(vaultId, documentId, undefined, "read-only"),
      );
      try {
        await waitFor(() => reader.status === "connected", { label: "reader connected" });
        expect(client.text.toString()).toBe("alpha beta");
        expect(readerDoc.getText("contents").toString()).toBe("alpha beta");
      } finally {
        reader.destroy();
        readerDoc.destroy();
      }
    } finally {
      client.destroy();
      quiet.destroy();
      quietDoc.destroy();
    }
  }, 90_000);
});
