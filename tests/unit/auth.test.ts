import { afterEach, beforeEach, describe, expect, it, vi, type MockInstance } from "vitest";
import type RealtimePlugin from "../../src/main";

interface RequestOptions {
  url: string;
  headers?: Record<string, string>;
}

interface MockResponse {
  status: number;
  json: unknown;
  text: string;
  arrayBuffer: ArrayBuffer;
}

const http = vi.hoisted(() => ({
  handler: vi.fn<(opts: RequestOptions) => Promise<MockResponse>>(),
}));

vi.mock("obsidian", () => ({
  requestUrl: (opts: RequestOptions) => http.handler(opts),
}));

import { AuthClient, AuthError } from "../../src/auth";
import { CompatibilityError } from "../../src/caps";

const LEGACY_TOKEN_KEY = "realtime-session-token";
const KNOWN_SESSIONS_KEY = "realtime-known-sessions";
const SERVER_URL = "https://sync.example.test";
const CAPS = {
  restApi: "3",
  oauth: "1",
  pluginDbSync: "crsqlite-1",
  attachmentShim: "https://realtime.md/attachment-shim/v1",
  documentEpoch: "1",
  documentInvalidation: "1",
};

const me = (userId: string) => ({
  userId,
  email: `${userId}@example.test`,
  displayName: userId.toUpperCase(),
});

const response = (status: number, json: unknown = {}) => ({
  status,
  json,
  text: JSON.stringify(json),
  arrayBuffer: new ArrayBuffer(0),
});

function makeAuth(opts: { serverId?: string; userId?: string; token?: string } = {}) {
  const secrets = new Map<string, string>();
  const settings = {
    authServerUrl: SERVER_URL,
    authServerId: opts.serverId ?? "server-old",
    userId: opts.userId ?? "user-a",
    userDisplayName: opts.userId ? opts.userId.toUpperCase() : "",
    userEmail: opts.userId ? `${opts.userId}@example.test` : "",
    gitEmail: "",
    userPictureUrl: "",
    userAvatarUrlOverride: "",
    userAvatarUrl: "",
    activeVaultId: "vault-a",
    clientName: "Test Client",
    clientNameCustomized: false,
  };
  const plugin = {
    settings,
    app: {
      secretStorage: {
        getSecret: (key: string) => secrets.get(key) ?? null,
        setSecret: (key: string, value: string) => secrets.set(key, value),
      },
    },
    lastCompatibilityError: null as RealtimePlugin["lastCompatibilityError"],
    saveSettings: vi.fn(async () => {}),
    updateLocalAwareness: vi.fn(),
  };
  const auth = new AuthClient(plugin as unknown as RealtimePlugin);
  if (opts.token) secrets.set(tokenKey(auth), opts.token);
  return { auth, plugin, settings, secrets };
}

function serverInfo(serverId: string) {
  return response(200, { serverId, caps: CAPS, requiredCaps: [] });
}

function bearer(opts: RequestOptions): string {
  return opts.headers?.Authorization?.replace("Bearer ", "") ?? "";
}

function tokenKey(auth: AuthClient, userId?: string): string {
  const testAccess = auth as unknown as { tokenKey(userId?: string): string };
  return testAccess.tokenKey(userId);
}

beforeEach(() => {
  window.localStorage.clear();
  http.handler.mockReset();
});

describe("AuthClient session reliability", () => {
  it("recognizes and migrates a legacy token when authServerId is already set", async () => {
    const { auth, settings, secrets } = makeAuth({ serverId: "server-same", userId: "user-a" });
    secrets.set(LEGACY_TOKEN_KEY, "legacy-token");
    http.handler.mockImplementation(async (opts) => {
      if (opts.url.endsWith("/api/server-info")) return serverInfo("server-same");
      if (opts.url.endsWith("/api/me") && bearer(opts) === "legacy-token") {
        return response(200, me("user-a"));
      }
      throw new Error(`unexpected request ${opts.url}`);
    });

    expect(auth.isLoggedIn).toBe(true);
    await auth.ensureServerId();

    const currentKey = tokenKey(auth, "user-a");
    expect(settings.authServerId).toBe("server-same");
    expect(secrets.get(currentKey)).toBe("legacy-token");
    expect(secrets.get(LEGACY_TOKEN_KEY)).toBe("");
    expect(auth.isLoggedIn).toBe(true);
  });

  it("moves active and saved-account tokens when the server id changes", async () => {
    const { auth, settings, secrets } = makeAuth({
      serverId: "server-old",
      userId: "user-a",
      token: "token-a",
    });
    const oldActiveKey = tokenKey(auth, "user-a");
    const oldSavedKey = tokenKey(auth, "user-b");
    secrets.set(oldSavedKey, "token-b");
    window.localStorage.setItem(
      KNOWN_SESSIONS_KEY,
      JSON.stringify([
        { ...me("user-a"), serverUrl: SERVER_URL, serverId: "server-old", tokenKey: oldActiveKey },
        { ...me("user-b"), serverUrl: SERVER_URL, serverId: "server-old", tokenKey: oldSavedKey },
      ]),
    );
    http.handler.mockImplementation(async (opts) => {
      if (opts.url.endsWith("/api/server-info")) return serverInfo("server-new");
      if (opts.url.endsWith("/api/me")) {
        if (bearer(opts) === "token-a") return response(200, me("user-a"));
        if (bearer(opts) === "token-b") return response(200, me("user-b"));
      }
      throw new Error(`unexpected request ${opts.url}`);
    });

    await auth.ensureServerId();

    const newActiveKey = tokenKey(auth, "user-a");
    const newSavedKey = tokenKey(auth, "user-b");
    expect(settings.authServerId).toBe("server-new");
    expect(secrets.get(newActiveKey)).toBe("token-a");
    expect(secrets.get(newSavedKey)).toBe("token-b");
    expect(secrets.get(oldActiveKey)).toBe("");
    expect(secrets.get(oldSavedKey)).toBe("");
    const sessions = JSON.parse(window.localStorage.getItem(KNOWN_SESSIONS_KEY) ?? "[]");
    expect(sessions).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          userId: "user-a",
          serverId: "server-new",
          tokenKey: newActiveKey,
        }),
        expect.objectContaining({
          userId: "user-b",
          serverId: "server-new",
          tokenKey: newSavedKey,
        }),
      ]),
    );
  });

  it("clears a rejected active token after a server id change", async () => {
    const { auth, settings, secrets } = makeAuth({
      serverId: "server-old",
      userId: "user-a",
      token: "revoked-token",
    });
    const oldKey = tokenKey(auth, "user-a");
    http.handler.mockImplementation(async (opts) => {
      if (opts.url.endsWith("/api/server-info")) return serverInfo("server-new");
      if (opts.url.endsWith("/api/me")) return response(401);
      throw new Error(`unexpected request ${opts.url}`);
    });

    await expect(auth.ensureServerId()).resolves.toBe("server-new");

    expect(settings.authServerId).toBe("server-new");
    expect(settings.userId).toBe("");
    expect(secrets.get(oldKey)).toBe("");
    expect(auth.isLoggedIn).toBe(false);
  });

  it("forgets a stale saved session without clearing another active login", async () => {
    const { auth, settings, secrets } = makeAuth({
      serverId: "server-same",
      userId: "user-a",
      token: "active-token",
    });
    const activeKey = tokenKey(auth, "user-a");
    const staleKey = tokenKey(auth, "user-b");
    secrets.set(staleKey, "stale-token");
    window.localStorage.setItem(
      KNOWN_SESSIONS_KEY,
      JSON.stringify([
        { ...me("user-a"), serverUrl: SERVER_URL, serverId: "server-same", tokenKey: activeKey },
        { ...me("user-b"), serverUrl: SERVER_URL, serverId: "server-same", tokenKey: staleKey },
      ]),
    );
    http.handler.mockImplementation(async (opts) => {
      if (opts.url.endsWith("/api/server-info")) return serverInfo("server-same");
      if (opts.url.endsWith("/api/me") && bearer(opts) === "active-token") {
        return response(200, me("user-a"));
      }
      if (opts.url.endsWith("/api/me") && bearer(opts) === "stale-token") {
        return response(401);
      }
      throw new Error(`unexpected request ${opts.url}`);
    });

    const valid = await auth.validSessionsForServer(SERVER_URL);

    expect(valid.map((session) => session.userId)).toEqual(["user-a"]);
    expect(settings.userId).toBe("user-a");
    expect(secrets.get(activeKey)).toBe("active-token");
    expect(auth.isLoggedIn).toBe(true);
    const sessions = JSON.parse(window.localStorage.getItem(KNOWN_SESSIONS_KEY) ?? "[]") as Array<{
      tokenKey: string;
    }>;
    expect(sessions.some((session) => session.tokenKey === staleKey)).toBe(false);
  });

  it("clears the current identity and throws the exact AuthError on current-token 401", async () => {
    const { auth, settings, secrets } = makeAuth({
      serverId: "server-same",
      userId: "user-a",
      token: "active-token",
    });
    const activeKey = tokenKey(auth, "user-a");
    http.handler.mockResolvedValue(response(401));

    await expect(auth.me()).rejects.toEqual(
      new AuthError("Session expired. Please sign in again."),
    );
    expect(settings.userId).toBe("");
    expect(secrets.get(activeKey)).toBe("");
  });

  it.each(["network", "server", "capability"] as const)(
    "preserves the active token on %s failures",
    async (failure) => {
      const { auth, settings, secrets } = makeAuth({
        serverId: "server-old",
        userId: "user-a",
        token: "active-token",
      });
      const activeKey = tokenKey(auth, "user-a");
      http.handler.mockImplementation(async (opts) => {
        if (opts.url.endsWith("/api/server-info")) {
          if (failure === "network") throw new Error("offline");
          if (failure === "capability") {
            return response(200, { serverId: "server-new", caps: { ...CAPS, restApi: "999" } });
          }
          return serverInfo("server-new");
        }
        if (opts.url.endsWith("/api/me")) return response(500, { error: "temporary" });
        throw new Error(`unexpected request ${opts.url}`);
      });

      if (failure === "capability") {
        await expect(auth.ensureServerId()).rejects.toBeInstanceOf(CompatibilityError);
      } else {
        await expect(auth.ensureServerId()).resolves.toBe("server-old");
      }

      expect(settings.authServerId).toBe("server-old");
      expect(settings.userId).toBe("user-a");
      expect(secrets.get(activeKey)).toBe("active-token");
      expect(auth.isLoggedIn).toBe(true);
    },
  );
});

describe("AuthClient transactional sign-in", () => {
  const NEW_URL = "https://new.example.test";
  const BROKEN_URL = "https://broken.example.test";

  /** Old server knows only `active-token`; the candidate knows only `new-token`. */
  function routeServers(opts: RequestOptions): MockResponse {
    const url = new URL(opts.url);
    if (url.pathname === "/api/server-info") {
      if (url.origin === SERVER_URL) return serverInfo("server-old");
      if (url.origin === NEW_URL) return serverInfo("server-new");
      if (url.origin === BROKEN_URL) {
        return response(200, { serverId: "server-broken", caps: { ...CAPS, restApi: "999" } });
      }
    }
    if (url.pathname === "/api/me") {
      if (url.origin === SERVER_URL && bearer(opts) === "active-token") {
        return response(200, me("user-a"));
      }
      if (url.origin === NEW_URL && bearer(opts) === "new-token") {
        return response(200, me("user-b"));
      }
      return response(401);
    }
    throw new Error(`unexpected request ${opts.url}`);
  }

  function signedIn() {
    const harness = makeAuth({ serverId: "server-old", userId: "user-a", token: "active-token" });
    http.handler.mockImplementation(async (opts) => routeServers(opts));
    const activeKey = tokenKey(harness.auth, "user-a");
    const before = { ...harness.settings };
    const expectActiveUnchanged = () => {
      expect(harness.settings).toEqual(before);
      expect(harness.secrets.get(activeKey)).toBe("active-token");
      expect(harness.auth.isLoggedIn).toBe(true);
    };
    return { ...harness, activeKey, expectActiveUnchanged };
  }

  let opened: MockInstance<typeof window.open>;
  beforeEach(() => {
    opened = vi.spyOn(window, "open").mockImplementation(() => null);
  });
  afterEach(() => {
    opened.mockRestore();
  });

  /** Resolves once an SSO attempt opened the browser and awaits its token. */
  async function ssoOpened(times = 1): Promise<void> {
    await vi.waitFor(() => expect(opened.mock.calls.length).toBeGreaterThanOrEqual(times));
  }

  it("validates a candidate server without touching the active session until committed", async () => {
    const { auth, settings, secrets, activeKey, expectActiveUnchanged } = signedIn();

    const pending = auth.authenticateAt(NEW_URL);
    await ssoOpened();
    // The deep link hands the token to the waiting flow instead of signing in.
    await expect(auth.handleProtocol({ token: "new-token" })).resolves.toBeNull();
    const session = await pending;

    expect(session).toMatchObject({
      serverUrl: NEW_URL,
      serverId: "server-new",
      token: "new-token",
    });
    expectActiveUnchanged();

    await auth.commitSession(session, "vault-new");

    expect(settings).toMatchObject({
      authServerUrl: NEW_URL,
      authServerId: "server-new",
      userId: "user-b",
      userDisplayName: "USER-B",
      activeVaultId: "vault-new",
    });
    expect(secrets.get(tokenKey(auth, "user-b"))).toBe("new-token");
    expect(auth.isLoggedIn).toBe(true);
    // The previous server's session stays saved for switching back.
    expect(secrets.get(activeKey)).toBe("active-token");
  });

  it("refuses to commit a session whose sign-in was cancelled afterwards", async () => {
    const { auth, expectActiveUnchanged } = signedIn();
    const cancel = new AbortController();

    const pending = auth.authenticateAt(NEW_URL, cancel.signal);
    await ssoOpened();
    await auth.handleProtocol({ token: "new-token" });
    const session = await pending;
    cancel.abort();

    await expect(auth.commitSession(session, "vault-new")).rejects.toBeInstanceOf(Error);
    expectActiveUnchanged();
  });

  it("refuses to commit a session superseded by a newer sign-in", async () => {
    const { auth, expectActiveUnchanged } = signedIn();

    const first = auth.authenticateAt(NEW_URL);
    await ssoOpened();
    await auth.handleProtocol({ token: "new-token" });
    const stale = await first;
    const second = auth.authenticateAt(NEW_URL);
    await ssoOpened(2);

    await expect(auth.commitSession(stale, "vault-new")).rejects.toBeInstanceOf(Error);
    auth.destroy();
    await expect(second).rejects.toBeInstanceOf(Error);
    expectActiveUnchanged();
  });

  it("cancelling before the token arrives keeps a late deep link from replacing the session", async () => {
    const { auth, expectActiveUnchanged } = signedIn();
    const cancel = new AbortController();

    const pending = auth.authenticateAt(NEW_URL, cancel.signal);
    await ssoOpened();
    cancel.abort();
    await expect(pending).rejects.toBeInstanceOf(Error);

    // A callback from the cancelled flow is ignored, not treated as a new login.
    await expect(auth.handleProtocol({ token: "new-token" })).resolves.toBeNull();
    expect(auth.submitPastedCode("new-token")).toBe(false);
    expectActiveUnchanged();
  });

  it("ignores a cancelled flow's late token even when it is valid on the active server", async () => {
    const { auth, expectActiveUnchanged } = signedIn();
    http.handler.mockImplementation(async (opts) => {
      if (opts.url === `${SERVER_URL}/api/me` && bearer(opts) === "late-token") {
        return response(200, me("user-c"));
      }
      return routeServers(opts);
    });
    const cancel = new AbortController();
    const pending = auth.authenticateAt(SERVER_URL, cancel.signal);
    await ssoOpened();
    cancel.abort();
    await expect(pending).rejects.toBeInstanceOf(Error);

    await expect(auth.handleProtocol({ token: "late-token" })).resolves.toBeNull();
    expectActiveUnchanged();
  });

  it("does not let an older capability preflight supersede a newer sign-in", async () => {
    const { auth, settings } = signedIn();
    const preflight = Promise.withResolvers<MockResponse>();
    let checks = 0;
    http.handler.mockImplementation(async (opts) => {
      if (opts.url === `${NEW_URL}/api/server-info` && checks++ === 0) return preflight.promise;
      return routeServers(opts);
    });
    const older = auth.authenticateAt(NEW_URL);
    const rejectedOlder = expect(older).rejects.toBeInstanceOf(Error);
    const newer = auth.authenticateAt(NEW_URL);
    await ssoOpened();
    preflight.resolve(serverInfo("server-new"));
    await rejectedOlder;
    await auth.handleProtocol({ token: "new-token" });
    await auth.commitSession(await newer, "vault-new");

    expect(settings.authServerUrl).toBe(NEW_URL);
    expect(settings.userId).toBe("user-b");
    expect(settings.activeVaultId).toBe("vault-new");
  });

  it("does not commit an orphaned callback whose validation outlives unload", async () => {
    const { auth, expectActiveUnchanged } = signedIn();
    const preflight = Promise.withResolvers<MockResponse>();
    http.handler.mockImplementation(async (opts) => {
      if (opts.url === `${SERVER_URL}/api/server-info`) return preflight.promise;
      return routeServers(opts);
    });
    const pending = auth.handleProtocol({ token: "active-token" });
    const rejected = expect(pending).rejects.toBeInstanceOf(Error);
    auth.destroy();
    preflight.resolve(serverInfo("server-old"));
    await rejected;

    expectActiveUnchanged();
  });

  it("cancels a staged sign-in while token validation is still pending", async () => {
    const { auth, expectActiveUnchanged } = signedIn();
    const validation = Promise.withResolvers<MockResponse>();
    let validating = false;
    http.handler.mockImplementation(async (opts) => {
      if (opts.url === `${NEW_URL}/api/me`) {
        validating = true;
        return validation.promise;
      }
      return routeServers(opts);
    });
    const cancel = new AbortController();
    const pending = auth.authenticateAt(NEW_URL, cancel.signal);
    const rejected = expect(pending).rejects.toBeInstanceOf(Error);
    await ssoOpened();
    const callback = auth.handleProtocol({ token: "new-token" });
    const rejectedCallback = expect(callback).rejects.toBeInstanceOf(Error);
    await vi.waitFor(() => expect(validating).toBe(true));
    cancel.abort();
    await rejected;
    expectActiveUnchanged();
    validation.resolve(response(200, me("user-b")));
    await rejectedCallback;
    expectActiveUnchanged();
  });

  it("checking candidate servers leaves the active compatibility state alone", async () => {
    const { auth, plugin } = signedIn();
    const activeFailure = { reason: "server-incompatible" as const, detail: "active is broken" };
    plugin.lastCompatibilityError = activeFailure;

    await auth.validSessionsForServer(NEW_URL);
    expect(plugin.lastCompatibilityError).toEqual(activeFailure);

    plugin.lastCompatibilityError = null;
    await expect(auth.authenticateAt(BROKEN_URL)).rejects.toBeInstanceOf(CompatibilityError);
    expect(plugin.lastCompatibilityError).toBeNull();
    expect(opened).not.toHaveBeenCalled();

    await expect(auth.serverInfoChecked(BROKEN_URL)).rejects.toBeInstanceOf(CompatibilityError);
    expect(plugin.lastCompatibilityError).toBeNull();
  });
});
