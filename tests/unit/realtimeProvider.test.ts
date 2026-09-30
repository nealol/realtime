import { afterEach, describe, expect, it, vi } from "vitest";
import * as Y from "yjs";
import * as encoding from "lib0/encoding";
import * as decoding from "lib0/decoding";
import * as syncProtocol from "y-protocols/sync";
import * as awarenessProtocol from "y-protocols/awareness";
import {
  RealtimeProvider,
  SYNC_EVENT_DOCUMENT_INVALIDATED,
  SYNC_EVENT_LOCAL_CHANGES,
  type SyncSocket,
} from "../../src/sync/RealtimeProvider";
import { resetDocumentEpochStateForTests, setEpochProposalHandler } from "../../src/documentEpoch";
import { DocumentEpochPendingError, type ClientToken } from "../../src/sync/clientToken";
import { HttpError } from "../../src/httpError";

const TOKEN: ClientToken = {
  url: "ws://sync.test/d/vault__doc/ws",
  baseUrl: "http://sync.test/d/vault__doc",
  docId: "vault__doc",
  token: "secret",
  authorization: "full",
};

class FakeSocket implements SyncSocket {
  binaryType = "arraybuffer";
  readyState = 0;
  onopen: ((event: unknown) => void) | null = null;
  onmessage: ((event: { data: unknown }) => void) | null = null;
  onclose: ((event: unknown) => void) | null = null;
  onerror: ((event: unknown) => void) | null = null;
  readonly sent: Uint8Array[] = [];
  closeCount = 0;

  constructor(readonly url: string) {}

  send(data: Uint8Array | ArrayBuffer): void {
    const bytes = data instanceof Uint8Array ? data : new Uint8Array(data);
    this.sent.push(bytes.slice());
  }

  close(): void {
    this.closeCount += 1;
    this.readyState = 3;
  }

  open(): void {
    this.readyState = 1;
    this.onopen?.({});
  }

  deliver(message: Uint8Array): void {
    this.onmessage?.({ data: message });
  }

  drop(): void {
    this.readyState = 3;
    this.onclose?.({ code: 1006, reason: "test drop" });
  }
}

interface ProviderFixture {
  doc: Y.Doc;
  provider: RealtimeProvider;
  sockets: FakeSocket[];
  destroy(): void;
}

function fixture(
  tokenSource: () => Promise<ClientToken> = () => Promise.resolve(TOKEN),
): ProviderFixture {
  const doc = new Y.Doc();
  const sockets: FakeSocket[] = [];
  const provider = new RealtimeProvider("vault__doc", doc, tokenSource, {
    connect: false,
    socketFactory: (url) => {
      const socket = new FakeSocket(url);
      sockets.push(socket);
      return socket;
    },
  });
  return {
    doc,
    provider,
    sockets,
    destroy: () => {
      provider.destroy();
      doc.destroy();
    },
  };
}

function syncMessage(write: (encoder: encoding.Encoder) => void): Uint8Array {
  const encoder = encoding.createEncoder();
  encoding.writeVarUint(encoder, 0);
  write(encoder);
  return encoding.toUint8Array(encoder);
}

function serverStep1(doc: Y.Doc): Uint8Array {
  return syncMessage((encoder) => syncProtocol.writeSyncStep1(encoder, doc));
}

function serverStep2(doc: Y.Doc): Uint8Array {
  return syncMessage((encoder) => syncProtocol.writeSyncStep2(encoder, doc));
}

function syncAcknowledgement(version: number): Uint8Array {
  const versionEncoder = encoding.createEncoder();
  encoding.writeVarUint(versionEncoder, version);
  const encoder = encoding.createEncoder();
  encoding.writeVarUint(encoder, 102);
  encoding.writeVarUint8Array(encoder, encoding.toUint8Array(versionEncoder));
  return encoding.toUint8Array(encoder);
}

function documentInvalidation(documentId: string): Uint8Array {
  const encoder = encoding.createEncoder();
  encoding.writeVarUint(encoder, 105);
  encoding.writeVarUint8Array(encoder, new TextEncoder().encode(JSON.stringify({ documentId })));
  return encoding.toUint8Array(encoder);
}

function messageType(message: Uint8Array): number {
  return decoding.readVarUint(decoding.createDecoder(message));
}

async function openAndHandshake(f: ProviderFixture): Promise<FakeSocket> {
  const connecting = f.provider.connect();
  await vi.waitFor(() => expect(f.sockets).toHaveLength(1));
  const socket = f.sockets[0];
  socket.open();
  const server = new Y.Doc();
  socket.deliver(serverStep1(server));
  socket.deliver(serverStep2(server));
  await connecting;
  server.destroy();
  return socket;
}

afterEach(() => {
  setEpochProposalHandler(null);
  resetDocumentEpochStateForTests();
});

describe("RealtimeProvider", () => {
  it("waits for the server sync step before requesting a durable acknowledgement", async () => {
    const f = fixture();
    try {
      const connecting = f.provider.connect();
      await vi.waitFor(() => expect(f.sockets).toHaveLength(1));
      const socket = f.sockets[0];
      expect(socket.url).toBe("ws://sync.test/d/vault__doc/ws/vault__doc?token=secret");
      socket.open();

      expect(socket.sent.map(messageType)).not.toContain(102);
      const server = new Y.Doc();
      socket.deliver(serverStep1(server));
      expect(socket.sent.map(messageType)).toContain(102);
      socket.deliver(serverStep2(server));
      await connecting;
      expect(f.provider.status).toBe("connected");

      socket.deliver(syncAcknowledgement(0));
      expect(f.provider.hasLocalChanges).toBe(false);
      const localChanges: boolean[] = [];
      f.provider.on(SYNC_EVENT_LOCAL_CHANGES, (pending) => localChanges.push(pending));

      f.doc.getMap("values").set("local", "change");
      expect(f.provider.hasLocalChanges).toBe(true);
      expect(localChanges).toEqual([true]);
      const sentTypes = socket.sent.map(messageType);
      expect(sentTypes.slice(-2)).toEqual([0, 102]);

      socket.deliver(syncAcknowledgement(0));
      expect(f.provider.hasLocalChanges).toBe(true);
      socket.deliver(syncAcknowledgement(1));
      expect(f.provider.hasLocalChanges).toBe(false);
      expect(localChanges).toEqual([true, false]);
      server.destroy();
    } finally {
      f.destroy();
    }
  });

  it("does not send or acknowledge local edits made through a read-only token", async () => {
    const recoveries: Uint8Array[] = [];
    const doc = new Y.Doc();
    const sockets: FakeSocket[] = [];
    const provider = new RealtimeProvider(
      "vault__doc",
      doc,
      () => Promise.resolve({ ...TOKEN, authorization: "read-only" }),
      {
        connect: false,
        onReadOnlyUpdate: (update) => recoveries.push(update),
        socketFactory: (url) => {
          const socket = new FakeSocket(url);
          sockets.push(socket);
          return socket;
        },
      },
    );
    const f: ProviderFixture = {
      doc,
      provider,
      sockets,
      destroy: () => {
        provider.destroy();
        doc.destroy();
      },
    };
    try {
      const socket = await openAndHandshake(f);
      socket.deliver(syncAcknowledgement(0));
      const before = socket.sent.length;
      const localChanges: boolean[] = [];
      f.provider.on(SYNC_EVENT_LOCAL_CHANGES, (pending) => localChanges.push(pending));

      f.doc.getMap("values").set("local", "read-only edit");

      expect(socket.sent).toHaveLength(before);
      expect(f.provider.hasLocalChanges).toBe(false);
      expect(localChanges).toEqual([]);
      expect(recoveries).toHaveLength(1);
    } finally {
      f.destroy();
    }
  });

  it("coalesces connect calls and cannot reconnect after destroy", async () => {
    const token = Promise.withResolvers<ClientToken>();
    const f = fixture(() => token.promise);
    const first = f.provider.connect();
    const second = f.provider.connect();
    expect(second).toBe(first);

    f.provider.disconnect();
    token.resolve(TOKEN);
    await first;
    expect(f.sockets).toHaveLength(0);
    expect(f.provider.status).toBe("offline");

    const reconnecting = f.provider.connect();
    await vi.waitFor(() => expect(f.sockets).toHaveLength(1));
    const socket = f.sockets[0];
    socket.open();
    f.provider.destroy();
    await reconnecting;
    expect(socket.closeCount).toBe(1);
    expect(f.provider.status).toBe("offline");

    socket.drop();
    await Promise.resolve();
    expect(f.sockets).toHaveLength(1);
    f.doc.destroy();
  });

  it("persists and acknowledges a direct-socket epoch proposal", async () => {
    const f = fixture();
    let proposal: { documentId: string; epoch: number } | null = null;
    setEpochProposalHandler((documentId, epoch) => {
      proposal = { documentId, epoch };
    });
    try {
      const socket = await openAndHandshake(f);
      const body = new TextEncoder().encode(JSON.stringify({ epoch: 7 }));
      const encoder = encoding.createEncoder();
      encoding.writeVarUint(encoder, 103);
      encoding.writeVarUint8Array(encoder, body);
      socket.deliver(encoding.toUint8Array(encoder));

      expect(proposal).toEqual({ documentId: "vault__doc", epoch: 7 });
      const acknowledgement = socket.sent.at(-1)!;
      const decoder = decoding.createDecoder(acknowledgement);
      expect(decoding.readVarUint(decoder)).toBe(104);
      expect(JSON.parse(new TextDecoder().decode(decoding.readVarUint8Array(decoder)))).toEqual({
        epoch: 7,
      });
    } finally {
      f.destroy();
    }
  });

  it("surfaces advisory child-document invalidations", async () => {
    const f = fixture();
    const invalidated: string[] = [];
    f.provider.on(SYNC_EVENT_DOCUMENT_INVALIDATED, (documentId) => invalidated.push(documentId));
    try {
      const socket = await openAndHandshake(f);
      socket.deliver(documentInvalidation("vault__remote-guid"));
      expect(invalidated).toEqual(["vault__remote-guid"]);
      expect(f.provider.status).toBe("connected");
    } finally {
      f.destroy();
    }
  });

  it("publishes only this client's presence, never a remote client's timeout", async () => {
    const f = fixture();
    try {
      const socket = await openAndHandshake(f);
      const remote = new awarenessProtocol.Awareness(new Y.Doc());
      remote.setLocalState({ user: { name: "remote" } });
      const encoder = encoding.createEncoder();
      encoding.writeVarUint(encoder, 1);
      encoding.writeVarUint8Array(
        encoder,
        awarenessProtocol.encodeAwarenessUpdate(remote, [remote.clientID]),
      );
      socket.deliver(encoding.toUint8Array(encoder));
      expect(f.provider.awareness.getStates().has(remote.clientID)).toBe(true);

      const before = socket.sent.length;
      awarenessProtocol.removeAwarenessStates(f.provider.awareness, [remote.clientID], "timeout");
      expect(socket.sent.slice(before).map(messageType)).not.toContain(1);

      f.provider.awareness.setLocalStateField("user", { name: "me" });
      const published = socket.sent.slice(before).filter((message) => messageType(message) === 1);
      expect(published).toHaveLength(1);
      expect(awarenessClients(published[0])).toEqual([f.doc.clientID]);
      remote.destroy();
    } finally {
      f.destroy();
    }
  });

  it("gives up on a socket that never opens and tries again", async () => {
    vi.useFakeTimers();
    const f = fixture();
    try {
      void f.provider.connect();
      await vi.advanceTimersByTimeAsync(0);
      expect(f.sockets).toHaveLength(1);
      await vi.advanceTimersByTimeAsync(59_000);
      expect(f.sockets[0].closeCount).toBe(0);
      await vi.advanceTimersByTimeAsync(2_000);
      expect(f.sockets[0].closeCount).toBe(1);
      expect(f.provider.lastConnectionError).toBe("connect timeout");
      await vi.advanceTimersByTimeAsync(1_000);
      expect(f.sockets).toHaveLength(2);
    } finally {
      f.destroy();
      vi.useRealTimers();
    }
  });

  it("keeps content a read-only server lacks pending after sync-status replies", async () => {
    let authorization: "full" | "read-only" = "read-only";
    const f = fixture(() => Promise.resolve({ ...TOKEN, authorization }));
    try {
      // Persisted before the grant turned out to be read-only.
      f.doc.getText("contents").insert(0, "offline edit");
      const socket = await openAndHandshake(f);
      socket.deliver(syncAcknowledgement(1));
      expect(f.provider.hasLocalChanges).toBe(true);

      // Once a full-access connection's handshake carries the content and the
      // server acknowledges it, it is saved.
      authorization = "full";
      f.provider.clientToken = null;
      socket.drop();
      await vi.waitFor(() => expect(f.sockets).toHaveLength(2));
      const next = f.sockets[1];
      next.open();
      const server = new Y.Doc();
      next.deliver(serverStep1(server));
      next.deliver(serverStep2(server));
      await vi.waitFor(() => expect(f.provider.status).toBe("connected"));
      next.deliver(syncAcknowledgement(1));
      expect(f.provider.hasLocalChanges).toBe(false);
      server.destroy();
    } finally {
      f.destroy();
    }
  });

  it("backs off token retries per document", async () => {
    vi.useFakeTimers();
    const tokenSource = vi.fn(() => Promise.reject(new HttpError("forbidden", 403)));
    const f = fixture(tokenSource);
    try {
      void f.provider.connect();
      await vi.advanceTimersByTimeAsync(0);
      expect(tokenSource).toHaveBeenCalledTimes(1);
      await vi.advanceTimersByTimeAsync(3_000);
      expect(tokenSource).toHaveBeenCalledTimes(2);
      await vi.advanceTimersByTimeAsync(3_000);
      expect(tokenSource).toHaveBeenCalledTimes(2);
      await vi.advanceTimersByTimeAsync(3_000);
      expect(tokenSource).toHaveBeenCalledTimes(3);
      await vi.advanceTimersByTimeAsync(11_000);
      expect(tokenSource).toHaveBeenCalledTimes(3);
      await vi.advanceTimersByTimeAsync(1_000);
      expect(tokenSource).toHaveBeenCalledTimes(4);
    } finally {
      f.destroy();
      vi.useRealTimers();
    }
  });

  it("probes an idle document rarely and a document with pending changes quickly", async () => {
    vi.useFakeTimers();
    const f = fixture();
    const probes = (socket: FakeSocket) =>
      socket.sent.filter((message) => messageType(message) === 102).length;
    try {
      void f.provider.connect();
      await vi.advanceTimersByTimeAsync(0);
      const socket = f.sockets[0];
      socket.open();
      const server = new Y.Doc();
      socket.deliver(serverStep1(server));
      socket.deliver(serverStep2(server));
      server.destroy();
      await vi.advanceTimersByTimeAsync(0);
      socket.deliver(syncAcknowledgement(0));
      const idle = probes(socket);

      await vi.advanceTimersByTimeAsync(29_000);
      expect(probes(socket)).toBe(idle);
      await vi.advanceTimersByTimeAsync(1_000);
      expect(probes(socket)).toBe(idle + 1);
      socket.deliver(syncAcknowledgement(0));

      // An edit the server has not acknowledged is probed every 2s.
      f.doc.getText("contents").insert(0, "pending");
      const afterEdit = probes(socket);
      // The server answers with an older version: the edit is still pending.
      socket.deliver(syncAcknowledgement(0));
      expect(f.provider.hasLocalChanges).toBe(true);
      await vi.advanceTimersByTimeAsync(2_000);
      expect(probes(socket)).toBe(afterEdit + 1);
    } finally {
      f.destroy();
      vi.useRealTimers();
    }
  });

  it("mints a new token as soon as the server refuses the current one", async () => {
    const tokenSource = vi.fn(() => Promise.resolve({ ...TOKEN }));
    const f = fixture(tokenSource);
    try {
      void f.provider.connect();
      await vi.waitFor(() => expect(f.sockets).toHaveLength(1));
      f.sockets[0].onerror?.({ reason: "token-rejected" });
      await vi.waitFor(() => expect(f.sockets).toHaveLength(2));
      expect(tokenSource).toHaveBeenCalledTimes(2);
      expect(f.provider.lastConnectionError).toBe("token rejected");
    } finally {
      f.destroy();
    }
  });

  it("refreshes an expired token before reconnecting", async () => {
    let expiresAt = Date.now() + 3_600_000;
    const tokenSource = vi.fn(() => Promise.resolve({ ...TOKEN, expiresAt }));
    const f = fixture(tokenSource);
    try {
      const socket = await openAndHandshake(f);
      expect(tokenSource).toHaveBeenCalledTimes(1);
      // An hour later the connection drops; the cached token has expired.
      f.provider.clientToken = { ...f.provider.clientToken!, expiresAt: Date.now() - 1 };
      expiresAt = Date.now() + 3_600_000;
      socket.drop();
      await vi.waitFor(() => expect(f.sockets).toHaveLength(2));
      expect(tokenSource).toHaveBeenCalledTimes(2);
    } finally {
      f.destroy();
    }
  });

  it("polls quickly while the server activates an epoch this client accepted", async () => {
    vi.useFakeTimers();
    const tokenSource = vi.fn(() =>
      Promise.reject(new DocumentEpochPendingError("vault__doc", 2, 1)),
    );
    const f = fixture(tokenSource);
    try {
      void f.provider.connect();
      await vi.advanceTimersByTimeAsync(0);
      await vi.advanceTimersByTimeAsync(2_000 * 5);
      expect(tokenSource).toHaveBeenCalledTimes(6);
      expect(f.sockets).toHaveLength(0);
    } finally {
      f.destroy();
      vi.useRealTimers();
    }
  });
});

function awarenessClients(message: Uint8Array): number[] {
  const decoder = decoding.createDecoder(message);
  decoding.readVarUint(decoder);
  const update = decoding.createDecoder(decoding.readVarUint8Array(decoder));
  const count = decoding.readVarUint(update);
  const clients: number[] = [];
  for (let i = 0; i < count; i++) {
    clients.push(decoding.readVarUint(update));
    decoding.readVarUint(update);
    decoding.readVarString(update);
  }
  return clients;
}
