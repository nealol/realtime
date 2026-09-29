// vitest setup (jsdom environment): provide the browser-ish globals that
// the native provider, legacy compatibility test, and y-indexeddb expect under Node.
import "fake-indexeddb/auto";
import WS from "ws";
import { beforeEach } from "vitest";
import { resetTokenRetryStateForTests } from "../../src/sync/clientToken";

// Document-token minting in src/sync/clientToken.ts keeps a module-global serial
// queue + 30s backoff armed by any failed token request. In tests every
// Document/Peer in a file shares that module, so a single transient fetch
// failure (server briefly busy) would stall every later connection for 30s —
// past the test timeout (the observed document.test.ts flake). Clear the state
// before each test and shrink the backoff so an in-test hiccup recovers fast.
beforeEach(() => {
  resetTokenRetryStateForTests(500);
});

// Force the `ws` implementation as the global WebSocket. On Node 22+ a global
// (undici) WebSocket already exists, but it clashes with jsdom's separate `Event`
// class ("event argument must be an instance of Event"); `ws` is a clean Node
// implementation both providers drive without that mismatch.
(globalThis as any).WebSocket = WS as unknown as typeof WebSocket;

// Node 25+ defines its own global `localStorage` accessor, which returns
// undefined unless the process runs with --localstorage-file. The accessor
// shadows jsdom's Storage, so restore the jsdom window's one for tests.
if (typeof localStorage === "undefined") {
  const storage = (globalThis as any).jsdom?.window?.localStorage;
  if (storage) {
    Object.defineProperty(globalThis, "localStorage", {
      value: storage,
      configurable: true,
      writable: true,
    });
  }
}

// Opt-in sync diagnostics for debugging test failures:
//   REALTIME_DEBUG=1 npx vitest run tests/unit/document.test.ts
if (process.env.REALTIME_DEBUG) {
  const { setDiagnosticLoggingEnabled } = await import("../../src/debug");
  setDiagnosticLoggingEnabled(true);
}
