import * as http from "http";
import type { AddressInfo } from "net";
import { afterEach, beforeEach, describe, expect, it, vi, type MockInstance } from "vitest";
import type { App, PluginManifest } from "obsidian";

// Obsidian's Plugin/view classes are the external boundary: a minimal Plugin
// that persists data in memory and runs registered cleanups on unload.
const lifecycle = vi.hoisted(() => ({
  persisted: new WeakMap<object, unknown>(),
  protocolHandlers: new Map<string, (params: Record<string, string>) => void>(),
}));

vi.mock("obsidian", async (importOriginal) => {
  const actual = await importOriginal<Record<string, unknown>>();
  class Plugin {
    app: object;
    manifest: unknown;
    private readonly cleanups: Array<() => void> = [];
    constructor(app: object, manifest: unknown) {
      this.app = app;
      this.manifest = manifest;
    }
    onunload(): void {}
    unload(): void {
      this.onunload();
      for (const cleanup of this.cleanups.splice(0)) cleanup();
    }
    async loadData(): Promise<unknown> {
      return structuredClone(lifecycle.persisted.get(this.app));
    }
    async saveData(data: unknown): Promise<void> {
      lifecycle.persisted.set(this.app, structuredClone(data));
    }
    addSettingTab(): void {}
    addStatusBarItem(): HTMLElement {
      return document.createElement("div");
    }
    registerEditorExtension(): void {}
    registerObsidianProtocolHandler(
      action: string,
      handler: (params: Record<string, string>) => void,
    ): void {
      lifecycle.protocolHandlers.set(action, handler);
    }
    addCommand(): void {}
    registerView(): void {}
    addRibbonIcon(): void {}
    registerInterval(id: number): number {
      this.cleanups.push(() => window.clearInterval(id));
      return id;
    }
    registerDomEvent(target: EventTarget, type: string, listener: EventListener): void {
      target.addEventListener(type, listener);
      this.cleanups.push(() => target.removeEventListener(type, listener));
    }
    registerEvent(): void {}
  }
  return {
    ...actual,
    Plugin,
    ItemView: class {},
    Menu: class {},
    MenuItem: class {},
    WorkspaceLeaf: class {},
    setIcon: () => {},
  };
});

// The cr-sqlite WASM engine is never opened by these lifecycle paths.
vi.mock("../../src/pluginDb/obsidianDeps", () => ({ buildEngineDeps: vi.fn() }));

import RealtimePlugin from "../../src/main";
import { REQUIRED_CAPS } from "../../src/caps";
import { FakeVault } from "../support/fakePlugin";
import { waitFor } from "../support/util";

const CAPS = Object.fromEntries(
  Object.entries(REQUIRED_CAPS).map(([name, accepted]) => [name, accepted[0]]),
);

const MANIFEST: PluginManifest = {
  id: "realtime",
  name: "Realtime",
  version: "0.0.0-test",
  minAppVersion: "1.0.0",
  author: "test",
  description: "lifecycle test",
};

interface FakeServer {
  url: string;
  vaults: Array<{ id: string; name: string; role: "admin" }>;
  /** `METHOD /path` of every request received. */
  requests: string[];
  /** Delay `/api/server-info` responses until the returned release is called. */
  holdServerInfo(): () => void;
  close(): Promise<void>;
}

/** A Realtime server that knows `tokens` (token → userId) and serves `vaults`. */
async function startServer(serverId: string, tokens: Map<string, string>): Promise<FakeServer> {
  let hold: Promise<void> | null = null;
  const httpServer = http.createServer(async (req, res) => {
    let body = "";
    req.setEncoding("utf8");
    for await (const chunk of req) body += chunk;
    const send = (status: number, json: unknown) => {
      res.writeHead(status, { "Content-Type": "application/json" });
      res.end(JSON.stringify(json));
    };
    const path = new URL(req.url ?? "/", "http://server").pathname;
    // Sync connects by minting document tokens; record which vault it syncs.
    const parsed: unknown = path === "/api/doc-token" ? JSON.parse(body) : null;
    const vaultId =
      parsed && typeof parsed === "object" && "vaultId" in parsed ? String(parsed.vaultId) : "";
    fake.requests.push(`${req.method} ${path}${vaultId ? ` vault=${vaultId}` : ""}`);
    if (path === "/api/server-info") {
      if (hold) await hold;
      return send(200, { serverId, caps: CAPS, requiredCaps: [] });
    }
    const token = (req.headers.authorization ?? "").replace("Bearer ", "");
    const userId = tokens.get(token);
    if (!userId) return send(401, { error: "unauthorized" });
    if (path === "/api/me") {
      return send(200, {
        userId,
        email: `${userId}@example.test`,
        displayName: userId.toUpperCase(),
      });
    }
    if (path === "/api/logout") {
      tokens.delete(token);
      return send(200, {});
    }
    if (path === "/api/vaults") return send(200, fake.vaults);
    return send(404, { error: "not found" });
  });
  await new Promise<void>((resolve) => httpServer.listen(0, "127.0.0.1", resolve));
  const { port } = httpServer.address() as AddressInfo;
  const fake: FakeServer = {
    url: `http://127.0.0.1:${port}`,
    vaults: [],
    requests: [],
    holdServerInfo() {
      let release = () => {};
      hold = new Promise<void>((resolve) => {
        release = resolve;
      });
      return () => {
        hold = null;
        release();
      };
    },
    close: () =>
      new Promise<void>((resolve) => {
        httpServer.closeAllConnections();
        httpServer.close(() => resolve());
      }),
  };
  return fake;
}

let oldServer: FakeServer;
let newServer: FakeServer;
const live = new Set<RealtimePlugin>();
let opened: MockInstance<typeof window.open>;

beforeEach(async () => {
  window.localStorage.clear();
  oldServer = await startServer("server-old", new Map([["old-token", "user-a"]]));
  newServer = await startServer("server-new", new Map([["new-token", "user-b"]]));
  opened = vi.spyOn(window, "open").mockImplementation(() => null);
});

afterEach(async () => {
  for (const plugin of live) plugin.unload();
  live.clear();
  opened.mockRestore();
  await Promise.all([oldServer.close(), newServer.close()]);
});

/**
 * Load the plugin signed in to the old server and bound to `vault-old`. The
 * workspace layout never reports ready, so each test drives sync starts
 * through awaitable lifecycle calls.
 */
async function loadPlugin() {
  const vault = new FakeVault();
  const secrets = new Map<string, string>([["realtime-session-token", "old-token"]]);
  const app = {
    vault,
    workspace: {
      on: () => ({}),
      onLayoutReady: () => {},
      getActiveFile: () => null,
      getLeavesOfType: () => [],
      iterateAllLeaves: () => {},
    },
    secretStorage: {
      getSecret: (key: string) => secrets.get(key) ?? null,
      setSecret: (key: string, value: string) => secrets.set(key, value),
    },
  };
  lifecycle.persisted.set(app, {
    authServerUrl: oldServer.url,
    authServerId: "server-old",
    userId: "user-a",
    activeVaultId: "vault-old",
    enabled: true,
  });
  // RealtimePlugin only touches this in-memory slice of Obsidian's App.
  const plugin = new RealtimePlugin(app as unknown as App, MANIFEST);
  live.add(plugin);
  await plugin.onload();
  const persisted = () => lifecycle.persisted.get(app) as Record<string, unknown>;
  return { plugin, vault, secrets, persisted };
}

function unload(plugin: RealtimePlugin): void {
  live.delete(plugin);
  plugin.unload();
}

const syncedVault = (server: FakeServer, vaultId: string) =>
  server.requests.includes(`POST /api/doc-token vault=${vaultId}`);

/** Begin a start that stays pending on the server's capability check. */
async function pendingStart(plugin: RealtimePlugin) {
  const release = oldServer.holdServerInfo();
  const start = plugin.reloadSync();
  await waitFor(() => oldServer.requests.includes("GET /api/server-info"), {
    label: "start awaiting server-info",
  });
  return { start, release };
}

describe("sync lifecycle cancellation", () => {
  it("starts sync for a signed-in vault and restarts it on reload", async () => {
    const { plugin, vault } = await loadPlugin();

    await plugin.reloadSync();

    const first = plugin.vaultSync;
    expect(first).not.toBeNull();
    const listeners = vault.handlerCount("modify");
    expect(listeners).toBeGreaterThan(0);
    await waitFor(() => syncedVault(oldServer, "vault-old"), { label: "vault-old connecting" });

    await plugin.reloadSync();

    expect(plugin.vaultSync).not.toBeNull();
    expect(plugin.vaultSync).not.toBe(first);
    expect(vault.handlerCount("modify")).toBe(listeners);
  });

  it("a start pending on the server check never syncs after unload", async () => {
    const { plugin, vault } = await loadPlugin();
    const { start, release } = await pendingStart(plugin);

    unload(plugin);
    release();
    await start;

    expect(plugin.vaultSync).toBeNull();
    expect(vault.handlerCount("modify")).toBe(0);
    expect(syncedVault(oldServer, "vault-old")).toBe(false);
  });

  it("a start pending on the server check never syncs after logout", async () => {
    const { plugin, vault, persisted } = await loadPlugin();
    const { start, release } = await pendingStart(plugin);

    await plugin.logout();
    release();
    await start;

    expect(plugin.vaultSync).toBeNull();
    expect(vault.handlerCount("modify")).toBe(0);
    expect(syncedVault(oldServer, "vault-old")).toBe(false);
    expect(plugin.auth.isLoggedIn).toBe(false);
    expect(persisted()).toMatchObject({ activeVaultId: "", userId: "" });
  });

  it("a start pending on the server check yields to a reload that disabled sync", async () => {
    const { plugin, vault } = await loadPlugin();
    const { start, release } = await pendingStart(plugin);

    plugin.settings.enabled = false;
    const reload = plugin.reloadSync();
    release();
    await Promise.all([start, reload]);

    expect(plugin.vaultSync).toBeNull();
    expect(vault.handlerCount("modify")).toBe(0);
    expect(syncedVault(oldServer, "vault-old")).toBe(false);
  });
});

describe("server migration", () => {
  /** Run a migration and complete its SSO through the deep-link handler. */
  async function migrate(plugin: RealtimePlugin, signal = new AbortController().signal) {
    const calls = opened.mock.calls.length;
    const migration = plugin.migrateToServer(newServer.url, signal);
    // Observe the outcome without leaving an unhandled rejection meanwhile.
    migration.catch(() => {});
    await waitFor(() => opened.mock.calls.length > calls, { label: "SSO opened" });
    lifecycle.protocolHandlers.get("realtime-auth")?.({ token: "new-token" });
    return migration;
  }

  async function running() {
    const loaded = await loadPlugin();
    await loaded.plugin.reloadSync();
    await waitFor(() => syncedVault(oldServer, "vault-old"), { label: "vault-old connecting" });
    const before = { settings: { ...loaded.plugin.settings }, data: loaded.persisted() };
    const sync = loaded.plugin.vaultSync;
    const tokens = new Map(loaded.secrets);
    const expectUnchanged = () => {
      expect(loaded.plugin.settings).toEqual(before.settings);
      expect(loaded.persisted()).toEqual(before.data);
      expect(loaded.secrets).toEqual(tokens);
      expect(loaded.plugin.vaultSync).toBe(sync);
      expect(loaded.plugin.auth.isLoggedIn).toBe(true);
    };
    return { ...loaded, sync, expectUnchanged };
  }

  it("leaves the active server, account, vault, and sync untouched without a matching vault", async () => {
    const { plugin, expectUnchanged } = await running();
    newServer.vaults = [{ id: "vault-other", name: "someone else's vault", role: "admin" }];

    await expect(migrate(plugin)).rejects.toBeInstanceOf(Error);

    expectUnchanged();
    expect(syncedVault(newServer, "vault-old")).toBe(false);
  });

  it("leaves everything untouched when cancelled, even if the deep link arrives later", async () => {
    const { plugin, expectUnchanged } = await running();
    newServer.vaults = [{ id: "vault-new", name: "fake-vault", role: "admin" }];
    const cancel = new AbortController();

    const migration = plugin.migrateToServer(newServer.url, cancel.signal);
    migration.catch(() => {});
    await waitFor(() => opened.mock.calls.length > 0, { label: "SSO opened" });
    cancel.abort();
    await expect(migration).rejects.toBeInstanceOf(Error);
    await expect(plugin.auth.handleProtocol({ token: "new-token" })).resolves.toBeNull();

    expectUnchanged();
    expect(syncedVault(newServer, "vault-new")).toBe(false);
  });

  it("commits the matching vault with the new server and restarts sync only against it", async () => {
    const { plugin, vault, sync, persisted } = await running();
    const listeners = vault.handlerCount("modify");
    newServer.vaults = [{ id: "vault-new", name: "fake-vault", role: "admin" }];

    await migrate(plugin);

    const committed = {
      authServerUrl: newServer.url,
      authServerId: "server-new",
      userId: "user-b",
      activeVaultId: "vault-new",
    };
    expect(plugin.settings).toMatchObject(committed);
    expect(persisted()).toMatchObject(committed);
    expect(plugin.vaultSync).not.toBeNull();
    expect(plugin.vaultSync).not.toBe(sync);
    expect(vault.handlerCount("modify")).toBe(listeners);
    await waitFor(() => syncedVault(newServer, "vault-new"), { label: "vault-new connecting" });
    expect(syncedVault(newServer, "vault-old")).toBe(false);
  });
});
