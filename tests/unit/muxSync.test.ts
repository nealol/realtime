// Tier-2 end-to-end coverage for sub-cap mux sync: the real Rust
// server's native CRDT store and `/dmux` route, driving the actual
// `MuxWebSocket` transport through `RealtimeProvider`.
//
// Like the other tier-2 suites this spawns servers and is NOT run in cloud CI
// (the plugin project is disabled there); run it locally via `bun run test:unit`.
//
// The two scenarios here are the ones unit tests with a fake socket can't reach:
//   1. A small document set shares one real socket and syncs both directions.
//   2. When that shared socket drops, every provider reconnects and resyncs.
//
// Write attribution through the demux (git/search/plugin-db) is exercised by the
// wdio Obsidian e2e (it has the git/cr-sqlite infra); the demux reuses the same
// content-write attribution path used by direct `/d` connections.

import { describe, it, expect, beforeAll, afterAll, afterEach } from "vitest";
import * as Y from "yjs";
import { YSweetProvider as LegacyYSweetProvider } from "@y-sweet/client";
import { RealtimeProvider, type SyncSocket } from "../../src/sync/RealtimeProvider";
import { makeFakePlugin, type FakePlugin } from "../support/fakePlugin";
import { Peer } from "../support/peer";
import { startAuthHarness, type AuthHarness } from "../support/authServer";
import { waitFor, freshGuid } from "../support/util";
import {
  getClientToken,
  resetTokenRetryStateForTests,
  type ClientToken,
} from "../../src/sync/clientToken";
import { createMuxSocket, resetMuxForTests, setMuxWebSocketCtor } from "../../src/sync/mux";

let harness: AuthHarness;
let token: string;
let vaultId: string;
let plugin: FakePlugin;

// A real WebSocket subclass that records every `/dmux` socket the mux opens, so
// the test can assert all providers share exactly one (and a fresh one appears
// after a drop). Peers use the plain global socket and are not counted.
const RealWebSocket = globalThis.WebSocket as unknown as { new (url: string): WebSocket };
let opened: string[] = [];
class CountingWebSocket extends (RealWebSocket as any) {
  static instances: any[] = [];
  constructor(url: string) {
    super(url);
    opened.push(url);
    CountingWebSocket.instances.push(this);
  }
}

beforeAll(async () => {
  harness = await startAuthHarness();
  token = await harness.loginUser("alice");
  const vault = await harness.createVault(token, "mux");
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
  resetMuxForTests();
});

/** A provider for `serverDocId` routed through the mux. */
function muxProvider(serverDocId: string): {
  doc: Y.Doc;
  text: Y.Text;
  provider: RealtimeProvider;
  destroy: () => void;
} {
  const doc = new Y.Doc();
  const text = doc.getText("contents");
  const provider = new RealtimeProvider(
    serverDocId,
    doc,
    () => getClientToken(plugin as any, serverDocId),
    { socketFactory: createMuxSocket },
  );
  return {
    doc,
    text,
    provider,
    destroy: () => {
      provider.destroy();
      doc.destroy();
    },
  };
}

const fileDocId = (guid: string) => `${vaultId}__${guid}`;

async function getReadOnlyClientToken(serverDocId: string): Promise<ClientToken> {
  const response = await fetch(`${harness.authUrl}/api/doc-token`, {
    method: "POST",
    headers: {
      authorization: `Bearer ${token}`,
      "content-type": "application/json",
    },
    body: JSON.stringify({
      vaultId,
      docId: serverDocId,
      authorization: "read-only",
    }),
  });
  if (!response.ok) {
    throw new Error(`read-only token request failed: ${response.status}`);
  }
  return (await response.json()) as ClientToken;
}

describe("sub-cap mux sync (tier-2)", () => {
  it("carries multiple docs over one real socket and syncs both directions", async () => {
    opened = [];
    CountingWebSocket.instances = [];
    setMuxWebSocketCtor(CountingWebSocket as unknown as { new (url: string): WebSocket });
    resetTokenRetryStateForTests(50);

    const idA = fileDocId(freshGuid());
    const idB = fileDocId(freshGuid());
    const a = muxProvider(idA);
    const b = muxProvider(idB);
    const peerA = new Peer(plugin, idA);
    const peerB = new Peer(plugin, idB);
    try {
      await waitFor(() => a.provider.status === "connected" && b.provider.status === "connected", {
        label: "both mux providers connected",
      });
      await peerA.whenSynced();
      await peerB.whenSynced();

      // An ordinary sub-cap document set shares one client socket to /dmux.
      expect(opened).toHaveLength(1);
      expect(opened[0]).toMatch(/\/dmux\?chunked=1$/);

      // mux -> server -> peer, on each independent channel.
      a.text.insert(0, "hello A");
      b.text.insert(0, "hello B");
      await waitFor(() => peerA.getText() === "hello A", { label: "docA synced mux->peer" });
      await waitFor(() => peerB.getText() === "hello B", { label: "docB synced mux->peer" });

      // peer -> server -> mux, and channels must not cross-talk.
      peerB.setText("peer wrote B");
      await waitFor(() => b.text.toString() === "peer wrote B", {
        label: "docB synced peer->mux",
      });
      expect(a.text.toString()).toBe("hello A");

      // Still exactly one real socket after all the traffic.
      expect(opened).toHaveLength(1);
    } finally {
      a.destroy();
      b.destroy();
      peerA.destroy();
      peerB.destroy();
    }
  });

  it("reconnects and resyncs after the shared socket drops", async () => {
    opened = [];
    CountingWebSocket.instances = [];
    setMuxWebSocketCtor(CountingWebSocket as unknown as { new (url: string): WebSocket });
    resetTokenRetryStateForTests(50);

    const id = fileDocId(freshGuid());
    const a = muxProvider(id);
    const peer = new Peer(plugin, id);
    try {
      await waitFor(() => a.provider.status === "connected", { label: "mux provider connected" });
      await peer.whenSynced();

      a.text.insert(0, "before drop");
      await waitFor(() => peer.getText() === "before drop", { label: "initial sync" });
      expect(opened).toHaveLength(1);

      // Hard-kill the one real socket. The mux fans the drop out to the channel,
      // the provider reconnects, and the mux opens a fresh /dmux socket.
      const sock = CountingWebSocket.instances.at(-1);
      if (typeof sock.terminate === "function") sock.terminate();
      else sock.close();

      await waitFor(() => opened.length >= 2, { label: "reconnect opens a new socket" });
      await waitFor(() => a.provider.status === "connected", { label: "provider reconnected" });

      // An edit after the drop must still round-trip (resync succeeded).
      a.text.insert(a.text.length, " + after drop");
      await waitFor(() => peer.getText() === "before drop + after drop", {
        label: "resync after reconnect",
      });
    } finally {
      a.destroy();
      peer.destroy();
    }
  });
});

describe("native read-only sync (tier-2)", () => {
  it("completes the real provider handshake and keeps receiving updates", async () => {
    opened = [];
    CountingWebSocket.instances = [];
    const id = fileDocId(freshGuid());
    const writer = new Peer(plugin, id);
    await writer.whenSynced();
    writer.setText("first server value");
    await writer.whenChangesSynced();

    const doc = new Y.Doc();
    const text = doc.getText("contents");
    const provider = new RealtimeProvider(id, doc, () => getReadOnlyClientToken(id), {
      socketFactory: (url) => new CountingWebSocket(url) as unknown as SyncSocket,
    });
    try {
      await waitFor(
        () => provider.status === "connected" && text.toString() === "first server value",
        { label: "read-only provider completed handshake" },
      );

      // The provider sends SyncStep2 during every handshake. A read-only server
      // must ignore it rather than close and reconnect the socket.
      const { promise, resolve } = Promise.withResolvers<void>();
      window.setTimeout(resolve, 750);
      await promise;
      expect(opened).toHaveLength(1);

      writer.setText("second server value");
      await writer.whenChangesSynced();
      await waitFor(() => text.toString() === "second server value", {
        label: "read-only provider received a later update",
      });
      expect(provider.status).toBe("connected");
      expect(opened).toHaveLength(1);
    } finally {
      provider.destroy();
      doc.destroy();
      writer.destroy();
    }
  });
});

describe("legacy provider compatibility (tier-2)", () => {
  it("syncs edits both ways with a native provider on the same document", async () => {
    const id = fileDocId(freshGuid());
    const native = muxProvider(id);
    const legacyDoc = new Y.Doc();
    const legacyText = legacyDoc.getText("contents");
    const legacy = new LegacyYSweetProvider(
      () => getClientToken(plugin as any, id) as any,
      id,
      legacyDoc,
      { connect: true, showDebuggerLink: false },
    );
    try {
      await waitFor(() => native.provider.status === "connected" && legacy.status === "connected", {
        label: "native and legacy providers connected",
      });

      native.text.insert(0, "native");
      await waitFor(() => legacyText.toString() === "native", {
        label: "native edit reached legacy provider",
      });

      legacyText.insert(legacyText.length, " + legacy");
      await waitFor(() => native.text.toString() === "native + legacy", {
        label: "legacy edit reached native provider",
      });
    } finally {
      native.destroy();
      legacy.destroy();
      legacyDoc.destroy();
    }
  });
});
