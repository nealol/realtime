// E2e tests for the plugin-facing remote cursor API (src/cursors/api.ts)
// against the real Rust auth and sync server: plugins make local calls (no
// WebSocket or token plumbing) and get the same audit log + robot Git
// attribution as MCP/streaming edits.

import { describe, it, beforeAll, afterAll, expect, vi } from "vitest";
import { startAuthHarness, type AuthHarness } from "../support/authServer";
import { makeFakePlugin, type FakePlugin } from "../support/fakePlugin";
import { RealtimeCursorsAPI } from "../../src/cursors/api";
import type RealtimePlugin from "../../src/main";

let harness: AuthHarness;
let aliceToken: string;

beforeAll(async () => {
  harness = await startAuthHarness();
  aliceToken = await harness.loginUser("alice");
});

afterAll(async () => {
  await harness.stop();
});

async function setup(
  vaultName: string,
): Promise<{ plugin: FakePlugin; cursors: RealtimeCursorsAPI; vaultId: string }> {
  const vault = await harness.createVault(aliceToken, vaultName);
  const { plugin } = makeFakePlugin(harness.authUrl, {
    sessionToken: aliceToken,
    activeVaultId: vault.id,
  });
  const cursors = new RealtimeCursorsAPI(plugin as unknown as RealtimePlugin);
  return { plugin, cursors, vaultId: vault.id };
}

describe("remote cursors plugin API", () => {
  it("performs audited, cursor-attributed note edits via plain local calls", async () => {
    const { plugin, cursors, vaultId } = await setup("bots");

    const cursor = await cursors.acquire({ pluginId: "report-bot", name: "Report Bot" });
    expect(cursor.token).toBeTruthy();
    expect(cursor.vaultId).toBe(vaultId);
    expect(cursor.streamUrl).toContain(`/api/vaults/${vaultId}/stream`);

    // Acquire is cached per session until near expiry.
    const again = await cursors.acquire({ pluginId: "report-bot" });
    expect(again.token).toBe(cursor.token);

    // Note edits round-trip without the plugin touching tokens or URLs.
    const created = await cursor.notes.create("bots/report.md", "# Report\n");
    expect(created.guid).toBeTruthy();
    await cursor.notes.patch("bots/report.md", { old: "# Report", new: "# Daily Report" });
    await cursor.notes.append("bots/report.md", "- item 1");
    const read = await cursor.notes.read("bots/report.md");
    expect(read.content).toBe("# Daily Report\n- item 1");
    expect((await cursor.notes.list()).map((n) => n.path)).toContain("bots/report.md");

    // Settings sees the cursor as plugin-managed.
    const listed = await plugin.auth.listCursors(vaultId);
    const row = listed.find((c) => c.id === cursor.cursorId);
    expect(row?.pluginId).toBe("report-bot");
    expect(row?.name).toBe("Report Bot");

    // Only cursor actors are audited, so these entries prove the edits ran
    // as the robot, not as the signed-in user. Newest first; append is a
    // read+replace under the hood.
    const page = await plugin.auth.listCursorAudit(vaultId, cursor.cursorId);
    expect(page.entries.map((e) => e.operation)).toEqual([
      "note_replace",
      "note_patch",
      "note_create",
    ]);
    const patchEntry = page.entries[1];
    expect(patchEntry.path).toBe("bots/report.md");
    expect(patchEntry.beforeContent).toBe("# Report\n");
    expect(patchEntry.afterContent).toBe("# Daily Report\n");

    // An admin can undo an entry; the note reverts.
    await plugin.auth.undoCursorAudit(vaultId, cursor.cursorId, page.entries[0].id);
    expect((await cursor.notes.read("bots/report.md")).content).toBe("# Daily Report\n");

    // Moves and deletes are audited too.
    await cursor.notes.move("bots/report.md", "bots/archive.md");
    await cursor.notes.delete("bots/archive.md");
    const after = await plugin.auth.listCursorAudit(vaultId, cursor.cursorId);
    expect(after.entries.map((e) => e.operation).slice(0, 2)).toEqual(["note_delete", "note_move"]);
    expect(after.entries[1].toPath).toBe("bots/archive.md");
  });

  it("re-acquires transparently when the token stops working", async () => {
    const { cursors } = await setup("retry");

    const cursor = await cursors.acquire({ pluginId: "retry-bot" });
    await cursor.notes.create("a.md", "x");

    // Simulate expiry/revocation: the cached token no longer authenticates.
    const corrupted = "definitely-not-a-token";
    cursor.token = corrupted;

    // The call still succeeds: 401 → invalidate → fresh acquire → retry.
    expect((await cursor.notes.read("a.md")).content).toBe("x");
    const fresh = await cursors.acquire({ pluginId: "retry-bot" });
    expect(fresh.token).not.toBe(corrupted);
    expect(fresh.cursorId).toBe(cursor.cursorId);
  });

  it("append keeps an edit that lands between its read and its write", async () => {
    const { cursors } = await setup("append-race");
    const writer = await cursors.acquire({ pluginId: "append-bot" });
    const other = await cursors.acquire({ pluginId: "other-bot" });
    await writer.notes.create("log.md", "# Log\n");

    const realFetch = globalThis.fetch;
    let raced = false;
    const spy = vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
      if (!raced && init?.method === "PUT") {
        raced = true;
        await other.notes.patch("log.md", { old: "# Log", new: "# Team log" });
      }
      return realFetch(input, init);
    });
    try {
      await writer.notes.append("log.md", "- entry");
    } finally {
      spy.mockRestore();
    }

    expect(raced).toBe(true);
    expect((await writer.notes.read("log.md")).content).toBe("# Team log\n- entry");
  });

  it("two plugins get distinct cursors in the same vault", async () => {
    const { plugin, cursors, vaultId } = await setup("multi");

    const a = await cursors.acquire({ pluginId: "bot-a" });
    const b = await cursors.acquire({ pluginId: "bot-b" });
    expect(a.cursorId).not.toBe(b.cursorId);

    await a.notes.create("from-a.md", "a");
    await b.notes.create("from-b.md", "b");

    // Each cursor's audit log only contains its own operations.
    const auditA = await plugin.auth.listCursorAudit(vaultId, a.cursorId);
    const auditB = await plugin.auth.listCursorAudit(vaultId, b.cursorId);
    expect(auditA.entries.map((e) => e.path)).toEqual(["from-a.md"]);
    expect(auditB.entries.map((e) => e.path)).toEqual(["from-b.md"]);
  });
});
