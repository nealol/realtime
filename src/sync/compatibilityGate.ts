import type RealtimePlugin from "../main";
import { CompatibilityError } from "../caps";

/** How long a server that advertised incompatible caps blocks connections before a recheck. */
const INCOMPATIBLE_RECHECK_MS = 60_000;
/** How long an unreachable server-info endpoint blocks connections before a recheck. */
const UNREACHABLE_RECHECK_MS = 3_000;

/**
 * Prerequisite every sync connection must pass before it mints a token or
 * opens a socket. See {@link RealtimeProviderOptions.connectionGate}.
 */
export interface ConnectionGate {
  /** Resolve once the server may be contacted; reject while it may not. */
  ensure(): Promise<void>;
  /** Whether a connection attempt may proceed right now without verifying again. */
  isOpen(): boolean;
  /**
   * Whether the server is known to be incompatible: connections already open
   * must stop exchanging updates with it at once.
   */
  blocksTraffic(): boolean;
  /** An established connection dropped: the server may have changed and must be verified again. */
  invalidate(): void;
}

/** Thrown by {@link ServerCompatibilityGate.ensure} while connections must stay closed. */
export class ConnectionGateClosedError extends Error {
  readonly reason: unknown;
  readonly incompatible: boolean;

  constructor(reason: unknown) {
    const detail = reason instanceof Error ? reason.message : String(reason);
    super(`server compatibility not verified: ${detail}`);
    this.name = "ConnectionGateClosedError";
    this.reason = reason;
    this.incompatible = reason instanceof CompatibilityError;
  }
}

/**
 * The server's capability contract, verified once for every connection that
 * shares it. Checks are single-flight, and a failed check is remembered for a
 * while, so a vault with thousands of documents (each with its own reconnect
 * loop) costs one server-info request per verification, not one per document.
 *
 * A verification lasts until an established connection drops: the server may
 * have been upgraded meanwhile, so the next connection verifies it again.
 */
export class ServerCompatibilityGate implements ConnectionGate {
  /** Bumped by every invalidation; a check verifies the generation it started in. */
  private generation = 0;
  private verifiedGeneration = -1;
  private inFlight: Promise<void> | null = null;
  private failure: { error: unknown; retryAt: number } | null = null;
  private serverKey: string;

  constructor(
    private readonly serverUrl: () => string,
    private readonly check: () => Promise<unknown>,
    private readonly knownIncompatible: () => boolean = () => false,
  ) {
    this.serverKey = serverUrl();
  }

  isOpen(): boolean {
    this.followServer();
    return this.verifiedGeneration === this.generation && !this.knownIncompatible();
  }

  blocksTraffic(): boolean {
    this.followServer();
    return this.knownIncompatible() || this.failure?.error instanceof CompatibilityError;
  }

  /** Keeps a remembered failure: dropped connections must not shorten its recheck delay. */
  invalidate(): void {
    this.generation += 1;
  }

  async ensure(): Promise<void> {
    for (;;) {
      if (this.isOpen()) return;
      if (this.inFlight) {
        await this.inFlight;
        continue;
      }
      const failure = this.failure;
      if (failure && Date.now() < failure.retryAt)
        throw new ConnectionGateClosedError(failure.error);
      await this.runCheck();
    }
  }

  /**
   * Verify the server now, ignoring a remembered result (joins a check
   * already in flight). Rejects with the check's own error.
   */
  async verify(): Promise<void> {
    this.followServer();
    if (!this.inFlight) {
      this.failure = null;
      this.verifiedGeneration = -1;
    }
    await (this.inFlight ?? this.runCheck());
    const failure = this.failure;
    if (failure) throw failure.error;
  }

  private runCheck(): Promise<void> {
    const generation = this.generation;
    const serverKey = this.serverKey;
    const task = (async () => {
      try {
        await this.check();
        if (serverKey !== this.serverKey) return;
        // Another check of this server found it incompatible since; trust that.
        if (this.knownIncompatible()) {
          throw new CompatibilityError("server-incompatible", "server is known to be incompatible");
        }
        this.failure = null;
        this.verifiedGeneration = generation;
      } catch (error) {
        if (serverKey !== this.serverKey) return;
        const wait =
          error instanceof CompatibilityError ? INCOMPATIBLE_RECHECK_MS : UNREACHABLE_RECHECK_MS;
        this.failure = { error, retryAt: Date.now() + wait };
        this.verifiedGeneration = -1;
      }
    })().finally(() => {
      if (this.inFlight === task) this.inFlight = null;
    });
    this.inFlight = task;
    return task;
  }

  /** A different server shares nothing with what the previous one proved. */
  private followServer(): void {
    const key = this.serverUrl();
    if (key === this.serverKey) return;
    this.serverKey = key;
    this.invalidate();
    this.failure = null;
    this.inFlight = null;
  }
}

const gates = new WeakMap<RealtimePlugin, ServerCompatibilityGate>();

/** The compatibility gate shared by every sync connection of `plugin`. */
export function compatibilityGateFor(plugin: RealtimePlugin): ServerCompatibilityGate {
  let gate = gates.get(plugin);
  if (!gate) {
    gate = new ServerCompatibilityGate(
      () => plugin.settings.authServerUrl,
      () => plugin.auth.serverInfoChecked(plugin.settings.authServerUrl),
      () => !!plugin.lastCompatibilityError,
    );
    gates.set(plugin, gate);
  }
  return gate;
}
