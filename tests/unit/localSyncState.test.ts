import { describe, expect, it } from "vitest";
import type * as Y from "yjs";
import { LocalSyncState } from "../../src/localSyncState";
import { freshGuid } from "../support/util";

async function state() {
  const result = new LocalSyncState(`local-state-test:${freshGuid()}`);
  await result.whenSynced;
  return result;
}

describe("LocalSyncState", () => {
  it("keeps a candidate unresolved until identity and content are acknowledged", async () => {
    const local = await state();
    try {
      local.beginCandidate("note.md", "text", null, "local-hash");
      local.markSynced("note.md", "text", "guid-1", "different-hash");
      expect(local.get("note.md")).toMatchObject({
        identity: "guid-1",
        fingerprint: "different-hash",
        candidate: true,
        candidateFingerprint: "local-hash",
      });

      local.markSynced("note.md", "text", "guid-1", "local-hash");
      expect(local.get("note.md")).toEqual({
        kind: "text",
        identity: "guid-1",
        fingerprint: "local-hash",
        candidate: false,
      });
    } finally {
      local.destroy();
    }
  });

  it("writes nothing when markSynced repeats the recorded state", async () => {
    const local = await state();
    // The private persisted doc: its update stream is what an unchanged write costs.
    const internals = local as unknown as { doc: Y.Doc };
    let updates = 0;
    internals.doc.on("update", () => (updates += 1));
    const synced = (path: string, identity: string, fingerprint: string) => {
      const before = updates;
      local.markSynced(path, "config", identity, fingerprint);
      return updates - before;
    };
    try {
      // Legacy presence marker: upgraded once, then stable.
      local.mark("legacy.json", "config");
      expect(synced("legacy.json", "h1", "h1")).toBe(1);
      expect(synced("legacy.json", "h1", "h1")).toBe(0);

      // Own publish: the first sync drops `replacedIdentity` and keeps
      // `publishedFrom`; repeating it changes nothing.
      local.markPublished("own.json", "config", "h2", "h1");
      expect(synced("own.json", "h2", "h2")).toBe(1);
      expect(synced("own.json", "h2", "h2")).toBe(0);
      expect(local.get("own.json")).toEqual({
        kind: "config",
        identity: "h2",
        fingerprint: "h2",
        candidate: false,
        publishedFrom: "h1",
      });

      // A carried disk fingerprint is part of the unchanged state.
      local.markDisk("own.json", "config", "h2", "disk");
      expect(synced("own.json", "h2", "h2")).toBe(0);
      expect(local.diskFingerprint("own.json", "h2")).toBe("disk");

      // An unresolved candidate stays one without rewriting.
      local.beginCandidate("pending.json", "config", null, "local");
      expect(synced("pending.json", "h3", "h3")).toBe(1);
      expect(synced("pending.json", "h3", "h3")).toBe(0);
      expect(local.get("pending.json")).toMatchObject({
        candidate: true,
        candidateFingerprint: "local",
      });

      // A new identity still rewrites and drops identity-scoped fields.
      expect(synced("own.json", "h4", "h4")).toBe(1);
      expect(local.get("own.json")).toEqual({
        kind: "config",
        identity: "h4",
        fingerprint: "h4",
        candidate: false,
      });
    } finally {
      local.destroy();
    }
  });

  it("migrates legacy presence markers without inventing a content baseline", async () => {
    const local = await state();
    try {
      (local as any).paths.set("note.md", "text");
      local.migrateLegacyIdentity("note.md", "text", "guid-2");
      expect(local.get("note.md")).toEqual({
        kind: "text",
        identity: "guid-2",
        candidate: false,
      });
    } finally {
      local.destroy();
    }
  });

  it("detects a same-path identity replacement", async () => {
    const local = await state();
    try {
      local.markSynced("board.canvas", "canvas", "old-guid", "hash");
      expect(local.hasIdentityConflict("board.canvas", "old-guid")).toBe(false);
      expect(local.hasIdentityConflict("board.canvas", "new-guid")).toBe(true);
    } finally {
      local.destroy();
    }
  });

  it("moves the full acknowledged baseline with a local rename", async () => {
    const local = await state();
    try {
      local.markSynced("before.md", "text", "guid", "fingerprint");
      local.move("before.md", "after.md", "text");
      expect(local.get("before.md")).toBeNull();
      expect(local.get("after.md")).toEqual({
        kind: "text",
        identity: "guid",
        fingerprint: "fingerprint",
        candidate: false,
      });
    } finally {
      local.destroy();
    }
  });

  it("keeps an untracked rename pending until its new identity is reconciled", async () => {
    const local = await state();
    try {
      local.move("pending.md", "renamed.md", "text");
      expect(local.get("renamed.md")).toEqual({
        kind: "text",
        identity: null,
        candidate: true,
      });
    } finally {
      local.destroy();
    }
  });

  it("tracks thousands of independent bootstrap candidates without cross-path state", async () => {
    const local = await state();
    try {
      for (let index = 0; index < 2_500; index++) {
        local.beginCandidate(`folder/note-${index}.md`, "text", null, `local-${index}`);
      }
      for (let index = 0; index < 2_500; index += 2) {
        local.markSynced(`folder/note-${index}.md`, "text", `guid-${index}`, `local-${index}`);
      }

      expect(local.get("folder/note-0.md")?.candidate).toBe(false);
      expect(local.get("folder/note-1.md")).toMatchObject({
        identity: null,
        candidate: true,
        candidateFingerprint: "local-1",
      });
      expect(local.get("folder/note-2499.md")?.candidateFingerprint).toBe("local-2499");
    } finally {
      local.destroy();
    }
  });

  it("forgets recorded identities once the vault index epoch goes back", async () => {
    const scope = `local-state-test:${freshGuid()}`;
    const first = new LocalSyncState(scope);
    await first.whenSynced;
    first.adoptIndexEpoch(2);
    first.markSynced("note.md", "text", "guid", "fingerprint");
    // A newer epoch keeps what this device agreed with the server.
    expect(first.adoptIndexEpoch(3)).toBe(false);
    expect(first.get("note.md")?.identity).toBe("guid");
    first.destroy();

    // A restored backup's older index is another timeline. The recorded epoch
    // survives a restart, so the reset happens whenever sync next starts.
    const restarted = new LocalSyncState(scope);
    try {
      await restarted.whenSynced;
      expect(restarted.adoptIndexEpoch(1)).toBe(true);
      expect(restarted.has("note.md")).toBe(false);
      expect(restarted.adoptIndexEpoch(1)).toBe(false);
    } finally {
      restarted.destroy();
    }
  });
});
