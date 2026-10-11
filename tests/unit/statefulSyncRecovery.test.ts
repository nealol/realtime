import { describe, expect, it, vi } from "vitest";
import fc from "fast-check";
import * as Y from "yjs";
import * as decoding from "lib0/decoding";
import * as encoding from "lib0/encoding";
import * as syncProtocol from "y-protocols/sync";
import {
  RealtimeProvider,
  type SyncSocket,
  type SyncSocketFactory,
} from "../../src/sync/RealtimeProvider";
import {
  getDocumentEpoch,
  handleEpochProposal,
  resetDocumentEpochStateForTests,
  setDocumentEpoch,
  setEpochProposalHandler,
} from "../../src/documentEpoch";
import {
  preserveAdapterConflict,
  preserveBinaryConflict,
  preserveTextConflict,
} from "../../src/conflictRecovery";
import { makeFakePlugin } from "../support/fakePlugin";
import type { ClientToken } from "../../src/sync/clientToken";
import type RealtimePlugin from "../../src/main";

const MESSAGE_SYNC = 0;
const MESSAGE_SYNC_STATUS = 102;
const MESSAGE_EPOCH_PROPOSAL = 103;
const MESSAGE_EPOCH_ACKNOWLEDGEMENT = 104;
const SOCKET_OPEN = 1;

class ModelSocket implements SyncSocket {
  binaryType = "arraybuffer";
  readyState = SOCKET_OPEN;
  onopen: ((event: unknown) => void) | null = null;
  onmessage: ((event: { data: unknown }) => void) | null = null;
  onclose: ((event: unknown) => void) | null = null;
  onerror: ((event: unknown) => void) | null = null;
  readonly sent: Uint8Array[] = [];

  constructor(readonly generation: number) {
    queueMicrotask(() => this.onopen?.({}));
  }

  send(data: Uint8Array | ArrayBuffer): void {
    this.sent.push(data instanceof Uint8Array ? data.slice() : new Uint8Array(data).slice());
  }

  close(): void {
    if (this.readyState !== SOCKET_OPEN) return;
    this.readyState = 3;
  }

  fail(code = 1006): void {
    if (this.readyState !== SOCKET_OPEN) return;
    this.readyState = 3;
    this.onclose?.({ code, reason: "model disconnect" });
  }

  receive(message: Uint8Array): void {
    this.onmessage?.({ data: message });
  }
}

function syncStep2(): Uint8Array {
  const encoder = encoding.createEncoder();
  encoding.writeVarUint(encoder, MESSAGE_SYNC);
  syncProtocol.writeSyncStep2(encoder, new Y.Doc());
  return encoding.toUint8Array(encoder);
}

/** The server's half of a handshake: its state vector, then its state. */
function handshake(socket: ModelSocket | undefined): void {
  const encoder = encoding.createEncoder();
  encoding.writeVarUint(encoder, MESSAGE_SYNC);
  syncProtocol.writeSyncStep1(encoder, new Y.Doc());
  socket?.receive(encoding.toUint8Array(encoder));
  socket?.receive(syncStep2());
}

/** The server echoes a sync-status probe once everything sent before it is durable. */
function answerSyncStatus(socket: ModelSocket): void {
  const probe = socket.sent.findLast((message) => messageKind(message) === MESSAGE_SYNC_STATUS);
  if (probe) socket.receive(probe);
}

function epochProposal(epoch: number): Uint8Array {
  const encoder = encoding.createEncoder();
  encoding.writeVarUint(encoder, MESSAGE_EPOCH_PROPOSAL);
  encoding.writeVarUint8Array(encoder, new TextEncoder().encode(JSON.stringify({ epoch })));
  return encoding.toUint8Array(encoder);
}

function messageKind(message: Uint8Array): number {
  return message[0] ?? -1;
}

function acknowledgedEpoch(message: Uint8Array): number | null {
  const decoder = decoding.createDecoder(message);
  if (decoding.readVarUint(decoder) !== MESSAGE_EPOCH_ACKNOWLEDGEMENT) return null;
  const payload = decoding.readVarUint8Array(decoder);
  const parsed = JSON.parse(new TextDecoder().decode(payload)) as { epoch?: unknown };
  return typeof parsed.epoch === "number" ? parsed.epoch : null;
}

async function flush(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
}

type ProviderCommand =
  | { type: "connect" }
  | { type: "disconnect" }
  | { type: "expireToken" }
  | { type: "proposeEpoch"; epoch: number };

const providerCommand = fc.oneof(
  fc.constant<ProviderCommand>({ type: "connect" }),
  fc.constant<ProviderCommand>({ type: "disconnect" }),
  fc.constant<ProviderCommand>({ type: "expireToken" }),
  fc.record({ type: fc.constant("proposeEpoch" as const), epoch: fc.integer({ min: 0, max: 12 }) }),
);

describe("stateful sync boundary properties", () => {
  it("models epoch acknowledgement and reconnect-after-token-expiry", async () => {
    vi.useFakeTimers();
    await fc.assert(
      fc.asyncProperty(
        fc.array(providerCommand, { minLength: 1, maxLength: 40 }),
        async (commands) => {
          resetDocumentEpochStateForTests();
          const { plugin } = makeFakePlugin("https://sync.example.com", {
            sessionToken: "session",
            activeVaultId: "vault",
          });
          let tokenGeneration = 0;
          let tokenCalls = 0;
          let connected = true;
          const sockets: ModelSocket[] = [];
          const socketFactory: SyncSocketFactory = () => {
            const socket = new ModelSocket(tokenGeneration);
            sockets.push(socket);
            return socket;
          };
          const tokenSource = async (): Promise<ClientToken> => {
            tokenCalls += 1;
            return {
              docId: "vault__doc",
              url: "ws://sync.test/dmux",
              token: `token-${tokenGeneration}`,
              epoch: getDocumentEpoch(plugin as unknown as RealtimePlugin, "vault__doc"),
            };
          };
          setEpochProposalHandler((documentId, epoch) => {
            setDocumentEpoch(plugin as unknown as RealtimePlugin, documentId, epoch);
          });
          const doc = new Y.Doc();
          const provider = new RealtimeProvider("vault__doc", doc, tokenSource, {
            connect: false,
            socketFactory,
          });

          try {
            for (const command of commands) {
              if (command.type === "connect") {
                connected = true;
                void provider.connect();
                await flush();
                handshake(sockets.at(-1));
                await flush();
              } else if (command.type === "disconnect") {
                connected = false;
                provider.disconnect();
              } else if (command.type === "expireToken") {
                tokenGeneration += 1;
                const before = tokenCalls;
                const activeSocket = sockets.at(-1);
                if (connected && activeSocket?.readyState === SOCKET_OPEN) {
                  activeSocket.fail();
                  await vi.advanceTimersByTimeAsync(20_000);
                  await flush();
                  const retrySocket = sockets.at(-1);
                  if (retrySocket && retrySocket !== activeSocket) {
                    handshake(retrySocket);
                    await flush();
                  }
                  if (tokenCalls > before) expect(retrySocket).not.toBe(activeSocket);
                }
              } else {
                const previous = getDocumentEpoch(
                  plugin as unknown as RealtimePlugin,
                  "vault__doc",
                );
                const proposed = Math.max(previous, command.epoch);
                const socket = sockets.at(-1);
                if (socket?.readyState === SOCKET_OPEN) {
                  const sentBefore = socket.sent.length;
                  socket.receive(epochProposal(proposed));
                  await flush();
                  // The acknowledgement waits until the server confirms this
                  // client's changes, which it does by answering a probe.
                  answerSyncStatus(socket);
                  await flush();
                  expect(getDocumentEpoch(plugin as unknown as RealtimePlugin, "vault__doc")).toBe(
                    proposed,
                  );
                  expect(socket.sent.slice(sentBefore).map(acknowledgedEpoch)).toContain(proposed);
                } else {
                  handleEpochProposal("vault__doc", proposed);
                  expect(getDocumentEpoch(plugin as unknown as RealtimePlugin, "vault__doc")).toBe(
                    proposed,
                  );
                }
              }
            }
            expect(
              getDocumentEpoch(plugin as unknown as RealtimePlugin, "vault__doc"),
            ).toBeGreaterThanOrEqual(0);
          } finally {
            provider.destroy();
            doc.destroy();
            setEpochProposalHandler(null);
            resetDocumentEpochStateForTests();
          }
        },
      ),
      { numRuns: 35 },
    );
    vi.useRealTimers();
  }, 120_000);

  it("models torn conflict-record recovery without overwrite or byte loss", async () => {
    const pathArbitrary = fc
      .tuple(
        fc.array(fc.stringMatching(/^[a-z0-9_-]{1,8}$/), { minLength: 1, maxLength: 3 }),
        fc.constantFrom("md", "png", "json"),
      )
      .map(([parts, extension]) => `${parts.join("/")}.${extension}`);
    await fc.assert(
      fc.asyncProperty(
        pathArbitrary,
        fc.array(fc.uint8Array({ minLength: 0, maxLength: 128 }), { minLength: 1, maxLength: 15 }),
        async (path, records) => {
          const { plugin, vault } = makeFakePlugin("https://sync.example.com", {
            sessionToken: "session",
            activeVaultId: "vault",
          });
          const adapter = new Map<string, ArrayBuffer>();
          const vaultWithAdapter = plugin.app.vault as unknown as typeof plugin.app.vault & {
            adapter: {
              exists(path: string): Promise<boolean>;
              writeBinary(path: string, content: ArrayBuffer): Promise<void>;
            };
          };
          vaultWithAdapter.adapter = {
            exists: async (candidate: string) => adapter.has(candidate),
            writeBinary: async (candidate: string, content: ArrayBuffer) => {
              adapter.set(candidate, content.slice(0));
            },
          };
          const destinations = [new Set<string>(), new Set<string>(), new Set<string>()];
          for (const [index, record] of records.entries()) {
            const kind = index % 3;
            const bytes = record.slice().buffer;
            const destination =
              kind === 0
                ? await preserveTextConflict(
                    plugin as unknown as RealtimePlugin,
                    path,
                    Buffer.from(record).toString("hex"),
                    "remote",
                  )
                : kind === 1
                  ? await preserveBinaryConflict(
                      plugin as unknown as RealtimePlugin,
                      path,
                      bytes,
                      "local",
                    )
                  : await preserveAdapterConflict(
                      plugin as unknown as RealtimePlugin,
                      path,
                      bytes,
                      "local",
                    );
            expect(destinations[kind].has(destination)).toBe(false);
            destinations[kind].add(destination);
            const recovered =
              kind === 0
                ? Buffer.from(vault.files.get(destination) ?? "", "hex")
                : kind === 1
                  ? new Uint8Array(vault.binaries.get(destination) ?? new ArrayBuffer(0))
                  : new Uint8Array(adapter.get(destination) ?? new ArrayBuffer(0));
            expect([...recovered]).toEqual([...record]);
          }
        },
      ),
      { numRuns: 150 },
    );
  });
});
