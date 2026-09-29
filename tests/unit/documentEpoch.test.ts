import { beforeEach, describe, expect, it, vi } from "vitest";
import {
  epochPersistenceName,
  getDocumentEpoch,
  resetDocumentEpochStateForTests,
  setDocumentEpoch,
} from "../../src/documentEpoch";
import {
  DocumentEpochChangedError,
  DocumentEpochPendingError,
  getClientToken,
  resetTokenRetryStateForTests,
} from "../../src/sync/clientToken";
import { HttpError } from "../../src/httpError";

function tokenFor(docId: string) {
  return {
    url: `wss://sync.example.com/d/${docId}/ws`,
    baseUrl: `https://sync.example.com/d/${docId}`,
    docId,
    token: "token",
    epoch: 0,
  };
}

function plugin(server = "server-a") {
  return {
    settings: {
      authServerId: server,
      authServerUrl: "https://sync.example.com",
      activeVaultId: "vault",
    },
    auth: { docToken: vi.fn() },
    acceptDocumentEpoch: vi.fn(),
  } as any;
}

beforeEach(() => {
  localStorage.clear();
  resetDocumentEpochStateForTests();
  resetTokenRetryStateForTests(0);
});

describe("document epochs", () => {
  it("scopes durable epochs and IndexedDB names by server and document", () => {
    const first = plugin("server-a");
    const second = plugin("server-b");
    expect(getDocumentEpoch(first, "vault__note")).toBe(0);
    expect(setDocumentEpoch(first, "vault__note", 4)).toBe(true);
    expect(setDocumentEpoch(first, "vault__note", 4)).toBe(false);
    expect(getDocumentEpoch(first, "vault__note")).toBe(4);
    expect(getDocumentEpoch(first, "vault__other")).toBe(0);
    expect(getDocumentEpoch(second, "vault__note")).toBe(0);
    expect(epochPersistenceName(first, "vault__note", "persisted")).toBe("persisted:epoch:4");
  });

  it("keeps the released persistence namespace for epoch zero", () => {
    expect(epochPersistenceName(plugin(), "vault__note", "persisted")).toBe("persisted");
  });

  it("rejects invalid epoch values before they can be acknowledged", () => {
    const instance = plugin();
    expect(() => setDocumentEpoch(instance, "vault__note", -1)).toThrow("invalid document epoch");
    expect(() => setDocumentEpoch(instance, "vault__note", 1.5)).toThrow("invalid document epoch");
    setDocumentEpoch(instance, "vault__note", 2);
    expect(() => setDocumentEpoch(instance, "vault__note", 1)).toThrow("cannot move backward");
  });

  it("refuses a token for a newer epoch and asks the plugin to rebuild first", async () => {
    const instance = plugin();
    instance.auth.docToken.mockResolvedValue({
      url: "wss://sync.example.com/d/vault__note/ws",
      baseUrl: "https://sync.example.com/d/vault__note",
      docId: "vault__note",
      token: "token",
      epoch: 3,
    });

    await expect(getClientToken(instance, "vault__note")).rejects.toEqual(
      expect.objectContaining<DocumentEpochChangedError>({
        name: "DocumentEpochChangedError",
        documentId: "vault__note",
        epoch: 3,
      }),
    );
    expect(instance.acceptDocumentEpoch).toHaveBeenCalledWith("vault__note", 3);
  });

  it("returns a token when its epoch matches durable local state", async () => {
    const instance = plugin();
    setDocumentEpoch(instance, "vault__note", 2);
    const token = {
      url: "wss://sync.example.com/d/vault__note/ws",
      baseUrl: "https://sync.example.com/d/vault__note",
      docId: "vault__note",
      token: "token",
      epoch: 2,
    };
    instance.auth.docToken.mockResolvedValue(token);
    await expect(getClientToken(instance, "vault__note")).resolves.toEqual(token);
    expect(instance.acceptDocumentEpoch).not.toHaveBeenCalled();
  });

  it("waits for a pending epoch instead of reading the retiring one", async () => {
    const instance = plugin();
    setDocumentEpoch(instance, "vault__note", 3);
    instance.auth.docToken.mockResolvedValue({
      url: "wss://sync.example.com/d/vault__note/ws",
      baseUrl: "https://sync.example.com/d/vault__note",
      docId: "vault__note",
      token: "old-epoch-token",
      epoch: 2,
    });

    await expect(getClientToken(instance, "vault__note")).rejects.toEqual(
      expect.objectContaining<DocumentEpochPendingError>({
        name: "DocumentEpochPendingError",
        documentId: "vault__note",
        localEpoch: 3,
        serverEpoch: 2,
      }),
    );
    // A read-only grant to the retiring epoch would load its items into the
    // fresh local document, which later uploads them into the replacement.
    expect(instance.auth.docToken).toHaveBeenCalledTimes(1);
    expect(instance.auth.docToken).toHaveBeenCalledWith("vault", "vault__note", undefined);
    expect(getDocumentEpoch(instance, "vault__note")).toBe(3);
  });

  it("waits while the server activates the epoch a token names", async () => {
    const instance = plugin();
    setDocumentEpoch(instance, "vault__note", 3);
    instance.auth.docToken.mockResolvedValue({
      ...tokenFor("vault__note"),
      epoch: 3,
      epochPending: true,
    });
    await expect(getClientToken(instance, "vault__note", undefined, 3)).rejects.toBeInstanceOf(
      DocumentEpochPendingError,
    );
    instance.auth.docToken.mockResolvedValue({ ...tokenFor("vault__note"), epoch: 3 });
    await expect(getClientToken(instance, "vault__note", undefined, 3)).resolves.toEqual(
      expect.objectContaining({ epoch: 3 }),
    );
  });

  it("refuses tokens to a document instance built for an older epoch", async () => {
    const instance = plugin();
    setDocumentEpoch(instance, "vault__note", 4);
    instance.auth.docToken.mockResolvedValue({
      url: "wss://sync.example.com/d/vault__note/ws",
      baseUrl: "https://sync.example.com/d/vault__note",
      docId: "vault__note",
      token: "token",
      epoch: 4,
    });

    await expect(getClientToken(instance, "vault__note", undefined, 3)).rejects.toEqual(
      expect.objectContaining<DocumentEpochChangedError>({
        name: "DocumentEpochChangedError",
        documentId: "vault__note",
        epoch: 4,
      }),
    );
    expect(instance.auth.docToken).not.toHaveBeenCalled();
    await expect(getClientToken(instance, "vault__note", undefined, 4)).resolves.toEqual(
      expect.objectContaining({ epoch: 4 }),
    );
  });

  it("does not globally back off unrelated documents while an epoch is pending", async () => {
    resetTokenRetryStateForTests(30_000);
    const instance = plugin();
    setDocumentEpoch(instance, "vault__pending", 3);
    instance.auth.docToken.mockImplementation(async (_vault: string, docId: string) => ({
      url: `wss://sync.example.com/d/${docId}/ws`,
      baseUrl: `https://sync.example.com/d/${docId}`,
      docId,
      token: "token",
      epoch: docId === "vault__pending" ? 2 : 0,
    }));

    await expect(getClientToken(instance, "vault__pending")).rejects.toBeInstanceOf(
      DocumentEpochPendingError,
    );
    await expect(getClientToken(instance, "vault__other")).resolves.toEqual(
      expect.objectContaining({ docId: "vault__other" }),
    );
  });

  it("does not delay other documents after one document's request is rejected", async () => {
    resetTokenRetryStateForTests(30_000);
    const instance = plugin();
    instance.auth.docToken.mockImplementation(async (_vault: string, docId: string) => {
      if (docId === "vault__broken") throw new HttpError("internal error", 500);
      if (docId === "vault__forbidden") throw new HttpError("forbidden", 403);
      return tokenFor(docId);
    });

    await expect(getClientToken(instance, "vault__broken")).rejects.toThrow("internal error");
    await expect(getClientToken(instance, "vault__forbidden")).rejects.toThrow("forbidden");
    const started = Date.now();
    await expect(getClientToken(instance, "vault__healthy")).resolves.toEqual(
      expect.objectContaining({ docId: "vault__healthy" }),
    );
    expect(Date.now() - started).toBeLessThan(1_000);
  });

  it("backs off every document when the server is unreachable or failing", async () => {
    vi.useFakeTimers();
    try {
      resetTokenRetryStateForTests(30_000);
      const instance = plugin();
      let failure: Error | null = new Error("net::ERR_INTERNET_DISCONNECTED");
      instance.auth.docToken.mockImplementation(async (_vault: string, docId: string) => {
        if (failure) throw failure;
        return tokenFor(docId);
      });

      await expect(getClientToken(instance, "vault__a")).rejects.toThrow("DISCONNECTED");
      failure = null;
      const delayed = getClientToken(instance, "vault__b");
      await vi.advanceTimersByTimeAsync(29_000);
      expect(instance.auth.docToken).toHaveBeenCalledTimes(1);
      await vi.advanceTimersByTimeAsync(1_000);
      await expect(delayed).resolves.toEqual(expect.objectContaining({ docId: "vault__b" }));

      // Repeated server errors across documents also mean the server is down.
      failure = new HttpError("internal error", 500);
      for (const docId of ["vault__c", "vault__d", "vault__e"]) {
        await expect(getClientToken(instance, docId)).rejects.toThrow("internal error");
      }
      failure = null;
      const afterServerErrors = getClientToken(instance, "vault__f");
      await vi.advanceTimersByTimeAsync(29_000);
      expect(instance.auth.docToken).toHaveBeenCalledTimes(5);
      await vi.advanceTimersByTimeAsync(1_000);
      await expect(afterServerErrors).resolves.toEqual(
        expect.objectContaining({ docId: "vault__f" }),
      );
    } finally {
      vi.useRealTimers();
    }
  });
});
