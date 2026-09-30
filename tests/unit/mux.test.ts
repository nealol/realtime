import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import * as encoding from "lib0/encoding";
import * as decoding from "lib0/decoding";
import {
  MuxWebSocket,
  decodeFrame,
  encodeData,
  encodeOpen,
  encodeClose,
  resetMuxForTests,
  setMuxWebSocketCtor,
} from "../../src/sync/mux";
import { setEpochProposalHandler } from "../../src/documentEpoch";

/**
 * A controllable stand-in for the real WebSocket the mux opens to `/dmux`.
 * Captures everything the mux sends and lets a test simulate the server side.
 */
class FakeServerSocket {
  static instances: FakeServerSocket[] = [];
  url: string;
  binaryType = "blob";
  readyState = 0; // CONNECTING
  onopen: ((ev: unknown) => void) | null = null;
  onmessage: ((ev: { data: ArrayBuffer }) => void) | null = null;
  onclose: ((ev: unknown) => void) | null = null;
  onerror: ((ev: unknown) => void) | null = null;
  sent: Uint8Array[] = [];

  constructor(url: string) {
    this.url = url;
    FakeServerSocket.instances.push(this);
  }

  send(data: ArrayBuffer | Uint8Array): void {
    this.sent.push(data instanceof Uint8Array ? data : new Uint8Array(data));
  }
  close(): void {
    this.readyState = 3;
  }

  /** Test helper: complete the connection handshake. */
  open(): void {
    this.readyState = 1;
    this.onopen?.({});
  }
  /** Test helper: deliver a binary frame to the mux client. */
  deliver(frame: Uint8Array): void {
    this.onmessage?.({ data: frame.buffer.slice(0) as ArrayBuffer });
  }
  /** Test helper: the parsed frames the mux has sent so far. */
  frames() {
    return this.sent.map((f) => decodeFrame(f));
  }
}

const DOC_URL = "wss://sync.example.com/d/vault__abc/ws/vault__abc?token=t-abc";
const DOC2_URL = "wss://sync.example.com/d/vault__def/ws/vault__def?token=t-def";

beforeEach(() => {
  resetMuxForTests();
  setEpochProposalHandler(null);
  FakeServerSocket.instances = [];
  setMuxWebSocketCtor(FakeServerSocket as unknown as { new (url: string): WebSocket });
});

afterEach(() => {
  setEpochProposalHandler(null);
  vi.useRealTimers();
  vi.restoreAllMocks();
});

describe("mux framing", () => {
  it("round-trips OPEN/DATA/CLOSE frames", () => {
    expect(decodeFrame(encodeOpen(3, "/d/x/ws/x?token=q"))).toEqual({
      type: "open",
      channelId: 3,
      pathAndQuery: "/d/x/ws/x?token=q",
    });

    const payload = new Uint8Array([0, 1, 2, 255, 9]);
    const data = decodeFrame(encodeData(7, payload));
    expect(data?.type).toBe("data");
    if (data?.type === "data") {
      expect(data.channelId).toBe(7);
      expect(Array.from(data.payload)).toEqual([0, 1, 2, 255, 9]);
    }

    expect(decodeFrame(encodeClose(5))).toEqual({ type: "close", channelId: 5 });
  });

  it("returns null on malformed input", () => {
    expect(decodeFrame(new Uint8Array([]))).toBeNull();
  });
});

describe("MuxWebSocket", () => {
  it("shares one real socket across channels and sends one OPEN each", () => {
    const a = new MuxWebSocket(DOC_URL);
    const b = new MuxWebSocket(DOC2_URL);

    // Both channels resolve to the same origin -> one real socket to /dmux.
    expect(FakeServerSocket.instances).toHaveLength(1);
    const server = FakeServerSocket.instances[0];
    expect(server.url).toBe("wss://sync.example.com/dmux");

    server.open();
    const opens = server.frames().filter((f) => f?.type === "open");
    expect(opens).toHaveLength(2);
    expect(opens.map((f) => (f?.type === "open" ? f.pathAndQuery : ""))).toEqual([
      "/d/vault__abc/ws/vault__abc?token=t-abc",
      "/d/vault__def/ws/vault__def?token=t-def",
    ]);

    expect(a.readyState).toBe(MuxWebSocket.CONNECTING);
    expect(b.readyState).toBe(MuxWebSocket.CONNECTING);
  });

  it("shards channels before reaching the server's per-connection ceiling", () => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
    vi.spyOn(Math, "random").mockReturnValue(0);

    const channels = Array.from(
      { length: 1_025 },
      (_, index) =>
        new MuxWebSocket(
          `wss://sync.example.com/d/vault__${index}/ws/vault__${index}?token=t-${index}`,
        ),
    );

    expect(FakeServerSocket.instances).toHaveLength(3);
    for (const server of FakeServerSocket.instances) server.open();

    const cursors = FakeServerSocket.instances.map(() => 0);
    const opensByServer = FakeServerSocket.instances.map(() => 0);
    let opened = 0;
    for (let tick = 0; tick < 600 && opened < channels.length; tick++) {
      FakeServerSocket.instances.forEach((server, serverIndex) => {
        const frames = server.frames();
        while (cursors[serverIndex] < frames.length) {
          const frame = frames[cursors[serverIndex]++];
          if (frame?.type === "open") {
            opensByServer[serverIndex]++;
            opened++;
            server.deliver(simpleFrame(2 /* OPEN_OK */, frame.channelId));
          } else if (frame?.type === "ping") {
            server.deliver(simpleFrame(7 /* PONG */, 0));
          }
        }
      });
      vi.advanceTimersByTime(100);
    }

    expect(opened).toBe(channels.length);
    expect(opensByServer).toEqual([512, 512, 1]);
    expect(
      channels.reduce<Record<number, number>>((counts, channel) => {
        counts[channel.readyState] = (counts[channel.readyState] ?? 0) + 1;
        return counts;
      }, {}),
    ).toEqual({ [MuxWebSocket.OPEN]: channels.length });

    resetMuxForTests();
    expect(FakeServerSocket.instances.every((server) => server.readyState === 3)).toBe(true);
  });

  it("fires onopen on OPEN_OK and routes DATA to the right channel", () => {
    const a = new MuxWebSocket(DOC_URL);
    const b = new MuxWebSocket(DOC2_URL);
    const server = FakeServerSocket.instances[0];
    server.open();

    const aChannel = openChannelId(server, "/d/vault__abc/ws/vault__abc?token=t-abc");
    const bChannel = openChannelId(server, "/d/vault__def/ws/vault__def?token=t-def");

    const aOpen = vi.fn();
    const bMsgs: number[][] = [];
    a.onopen = aOpen;
    b.onmessage = (ev) =>
      bMsgs.push(Array.from(new Uint8Array((ev as { data: ArrayBuffer }).data)));

    server.deliver(simpleFrame(2 /* OPEN_OK */, aChannel));
    expect(aOpen).toHaveBeenCalledOnce();
    expect(a.readyState).toBe(MuxWebSocket.OPEN);

    // DATA for channel b must not leak to a.
    server.deliver(encodeData(bChannel, new Uint8Array([42, 43])));
    expect(bMsgs).toEqual([[42, 43]]);
  });

  it("send() writes a DATA frame only once open", () => {
    const a = new MuxWebSocket(DOC_URL);
    const server = FakeServerSocket.instances[0];
    server.open();
    const aChannel = openChannelId(server, "/d/vault__abc/ws/vault__abc?token=t-abc");

    a.send(new Uint8Array([1, 2, 3])); // dropped: channel not OPEN yet
    expect(server.frames().some((f) => f?.type === "data")).toBe(false);

    server.deliver(simpleFrame(2, aChannel));
    a.send(new Uint8Array([1, 2, 3]));
    const data = server.frames().find((f) => f?.type === "data");
    expect(data?.type === "data" && Array.from(data.payload)).toEqual([1, 2, 3]);
  });

  it("persists epoch proposals through the handler before acknowledging them", () => {
    let persistedEpoch = 0;
    setEpochProposalHandler((documentId, epoch) => {
      expect(documentId).toBe("vault__abc");
      persistedEpoch = epoch;
    });
    const socket = new MuxWebSocket(DOC_URL);
    const onmessage = vi.fn();
    socket.onmessage = onmessage;
    const server = FakeServerSocket.instances[0];
    server.open();
    const channel = openChannelId(server, "/d/vault__abc/ws/vault__abc?token=t-abc");
    server.deliver(simpleFrame(2, channel));

    const proposal = encoding.createEncoder();
    encoding.writeVarUint(proposal, 103);
    encoding.writeVarUint8Array(proposal, new TextEncoder().encode(JSON.stringify({ epoch: 7 })));
    server.deliver(encodeData(channel, encoding.toUint8Array(proposal)));

    expect(persistedEpoch).toBe(7);
    expect(onmessage).not.toHaveBeenCalled();
    const acknowledgement = server
      .frames()
      .filter((frame) => frame?.type === "data")
      .at(-1);
    expect(acknowledgement?.type).toBe("data");
    if (acknowledgement?.type !== "data") throw new Error("missing epoch acknowledgement");
    const decoder = decoding.createDecoder(acknowledgement.payload);
    expect(decoding.readVarUint(decoder)).toBe(104);
    expect(JSON.parse(new TextDecoder().decode(decoding.readVarUint8Array(decoder)))).toEqual({
      epoch: 7,
    });
  });

  it("fans a real-socket drop out to every channel as error+close", () => {
    const a = new MuxWebSocket(DOC_URL);
    const b = new MuxWebSocket(DOC2_URL);
    const server = FakeServerSocket.instances[0];
    server.open();

    const aClose = vi.fn();
    const aErr = vi.fn();
    const bClose = vi.fn();
    a.onclose = aClose;
    a.onerror = aErr;
    b.onclose = bClose;

    server.onclose?.({});
    expect(aErr).toHaveBeenCalledOnce();
    expect(aClose).toHaveBeenCalledOnce();
    expect(bClose).toHaveBeenCalledOnce();
    expect(a.readyState).toBe(MuxWebSocket.CLOSED);
    expect(b.readyState).toBe(MuxWebSocket.CLOSED);
  });

  it("delivers a server CLOSE frame to just that channel", () => {
    const a = new MuxWebSocket(DOC_URL);
    const server = FakeServerSocket.instances[0];
    server.open();
    const aChannel = openChannelId(server, "/d/vault__abc/ws/vault__abc?token=t-abc");

    const aClose = vi.fn();
    a.onclose = aClose;
    server.deliver(simpleFrame(5 /* CLOSE */, aChannel));
    expect(aClose).toHaveBeenCalledOnce();
    expect(a.readyState).toBe(MuxWebSocket.CLOSED);
  });

  it("backs off replacement channels after OPEN_ERR", () => {
    vi.useFakeTimers();
    const first = new MuxWebSocket(DOC_URL);
    const server = FakeServerSocket.instances[0];
    server.open();
    const firstChannel = openChannelId(server, "/d/vault__abc/ws/vault__abc?token=t-abc");
    const firstClose = vi.fn();
    first.onclose = firstClose;

    server.deliver(simpleFrame(3 /* OPEN_ERR */, firstChannel));
    expect(firstClose).toHaveBeenCalledOnce();

    new MuxWebSocket(DOC_URL);
    expect(server.frames().filter((frame) => frame?.type === "open")).toHaveLength(1);
    vi.advanceTimersByTime(999);
    expect(server.frames().filter((frame) => frame?.type === "open")).toHaveLength(1);
    vi.advanceTimersByTime(1);
    expect(server.frames().filter((frame) => frame?.type === "open")).toHaveLength(2);

    const secondChannel = server
      .frames()
      .filter((frame) => frame?.type === "open")
      .at(-1)?.channelId;
    expect(secondChannel).toBeDefined();
    server.deliver(simpleFrame(3, secondChannel!));

    new MuxWebSocket(DOC_URL);
    vi.advanceTimersByTime(1_999);
    expect(server.frames().filter((frame) => frame?.type === "open")).toHaveLength(2);
    vi.advanceTimersByTime(1);
    expect(server.frames().filter((frame) => frame?.type === "open")).toHaveLength(3);

    const thirdChannel = server
      .frames()
      .filter((frame) => frame?.type === "open")
      .at(-1)?.channelId;
    expect(thirdChannel).toBeDefined();
    server.deliver(simpleFrame(2 /* OPEN_OK */, thirdChannel!));

    new MuxWebSocket(DOC2_URL);
    expect(server.frames().filter((frame) => frame?.type === "open")).toHaveLength(4);
  });

  it("reports a refused token without slowing other opens on the shard", () => {
    vi.useFakeTimers();
    const live = new MuxWebSocket(DOC2_URL);
    const refused = new MuxWebSocket(DOC_URL);
    const server = FakeServerSocket.instances[0];
    server.open();
    server.deliver(
      simpleFrame(
        2 /* OPEN_OK */,
        openChannelId(server, "/d/vault__def/ws/vault__def?token=t-def"),
      ),
    );
    expect(live.readyState).toBe(MuxWebSocket.OPEN);
    const channel = openChannelId(server, "/d/vault__abc/ws/vault__abc?token=t-abc");
    const onerror = vi.fn();
    refused.onerror = onerror;

    const openErr = encoding.createEncoder();
    encoding.writeVarUint(openErr, 3 /* OPEN_ERR */);
    encoding.writeVarUint(openErr, channel);
    encoding.writeVarUint(openErr, 2 /* token refused */);
    server.deliver(encoding.toUint8Array(openErr));
    expect(onerror).toHaveBeenCalledWith({ reason: "token-rejected" });

    // The replacement channel opens immediately: no admission backoff.
    new MuxWebSocket(DOC_URL);
    expect(server.frames().filter((frame) => frame?.type === "open")).toHaveLength(3);
  });

  it("decodes OPEN_ERR with and without a reason", () => {
    expect(decodeFrame(simpleFrame(3, 4))).toEqual({ type: "open_err", channelId: 4, reason: 0 });
    const withReason = encoding.createEncoder();
    encoding.writeVarUint(withReason, 3);
    encoding.writeVarUint(withReason, 4);
    encoding.writeVarUint(withReason, 1);
    expect(decodeFrame(encoding.toUint8Array(withReason))).toEqual({
      type: "open_err",
      channelId: 4,
      reason: 1,
    });
  });

  it("closes the idle real socket once an OPEN backoff has passed", () => {
    vi.useFakeTimers();
    const socket = new MuxWebSocket(DOC_URL);
    const server = FakeServerSocket.instances[0];
    server.open();
    const channel = openChannelId(server, "/d/vault__abc/ws/vault__abc?token=t-abc");
    server.deliver(simpleFrame(3 /* OPEN_ERR */, channel));
    expect(socket.readyState).toBe(MuxWebSocket.CLOSED);

    // Kept through the backoff, so a reconnecting channel honours it...
    vi.advanceTimersByTime(500);
    expect(server.readyState).toBe(1);
    // ...then released instead of heartbeating an empty connection forever.
    vi.advanceTimersByTime(600);
    expect(server.readyState).toBe(3);
  });

  it("flushes queued retries when an older OPEN succeeds after cooldown", () => {
    vi.useFakeTimers();
    const first = new MuxWebSocket(DOC_URL);
    new MuxWebSocket(DOC2_URL);
    const server = FakeServerSocket.instances[0];
    server.open();
    const firstChannel = openChannelId(server, "/d/vault__abc/ws/vault__abc?token=t-abc");
    const secondChannel = openChannelId(server, "/d/vault__def/ws/vault__def?token=t-def");

    server.deliver(simpleFrame(3 /* OPEN_ERR */, secondChannel));
    new MuxWebSocket(DOC2_URL);
    expect(server.frames().filter((frame) => frame?.type === "open")).toHaveLength(2);

    vi.setSystemTime(Date.now() + 1_000);
    server.deliver(simpleFrame(2 /* OPEN_OK */, firstChannel));
    expect(first.readyState).toBe(MuxWebSocket.OPEN);
    expect(server.frames().filter((frame) => frame?.type === "open")).toHaveLength(3);
  });

  it("paces a high-fanout queue below the server admission limit", () => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
    vi.spyOn(Math, "random").mockReturnValue(0);
    for (let index = 0; index < 180; index++) {
      new MuxWebSocket(
        `wss://sync.example.com/d/vault__${index}/ws/vault__${index}?token=t-${index}`,
      );
    }
    const server = FakeServerSocket.instances[0];
    server.open();

    let processed = 0;
    let accepted = 0;
    let rejected = 0;
    let windowStarted = Date.now();
    let opensInWindow = 0;

    for (let tick = 0; tick < 250 && processed < 180; tick++) {
      const opens = server.frames().filter((frame) => frame?.type === "open");
      while (processed < opens.length) {
        const frame = opens[processed++];
        if (!frame || frame.type !== "open") throw new Error("expected OPEN frame");
        if (Date.now() - windowStarted >= 10_000) {
          windowStarted = Date.now();
          opensInWindow = 0;
        }
        if (opensInWindow >= 128) {
          rejected++;
          server.deliver(simpleFrame(3 /* OPEN_ERR */, frame.channelId));
        } else {
          opensInWindow++;
          accepted++;
          server.deliver(simpleFrame(2 /* OPEN_OK */, frame.channelId));
        }
      }
      vi.advanceTimersByTime(100);
    }

    expect(processed).toBe(180);
    expect(accepted).toBe(180);
    expect(rejected).toBe(0);
    expect(Date.now()).toBeLessThan(20_000);
  });

  it("close() sends a CLOSE frame and stops sending", () => {
    const a = new MuxWebSocket(DOC_URL);
    const server = FakeServerSocket.instances[0];
    server.open();
    const aChannel = openChannelId(server, "/d/vault__abc/ws/vault__abc?token=t-abc");
    server.deliver(simpleFrame(2, aChannel));

    a.close();
    const close = server.frames().find((f) => f?.type === "close");
    expect(close).toEqual({ type: "close", channelId: aChannel });

    a.send(new Uint8Array([9]));
    expect(server.frames().some((f) => f?.type === "data")).toBe(false);

    // Closing the final channel releases the idle connection registry entry.
    // A later provider gets a fresh transport rather than retaining the dead
    // socket object forever.
    new MuxWebSocket(DOC_URL);
    expect(FakeServerSocket.instances).toHaveLength(2);
  });
});

/** Find the channel id the mux assigned to a given OPEN path. */
function openChannelId(server: FakeServerSocket, pathAndQuery: string): number {
  for (const frame of server.frames()) {
    if (frame?.type === "open" && frame.pathAndQuery === pathAndQuery) return frame.channelId;
  }
  throw new Error(`no OPEN frame for ${pathAndQuery}`);
}

/** Build a `[type][channel]` control frame the way the server would. */
function simpleFrame(type: number, channel: number): Uint8Array {
  const encoder = encoding.createEncoder();
  encoding.writeVarUint(encoder, type);
  encoding.writeVarUint(encoder, channel);
  return encoding.toUint8Array(encoder);
}
