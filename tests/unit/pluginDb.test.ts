import { describe, it, expect, vi } from "vitest";
import * as Y from "yjs";
import { makeEngine, makeRelay, newSnapStore } from "../support/crsqliteHarness";
import { _resetSqliteForTests, readAllChanges } from "../../src/pluginDb/crsqlite";
import { RealtimeSqlAPI } from "../../src/pluginDb/api";
import { MAX_BATCH_ROWS, SYNC_FORMAT } from "../../src/pluginDb/types";
import { CompatibilityError } from "../../src/caps";
import { waitFor } from "../support/util";

vi.mock("../../src/pluginDb/obsidianDeps", () => ({
  buildEngineDeps: vi.fn(),
}));

interface TaskRow {
  id: string;
  title: string;
  done: number | null;
}

async function titles(db: { query: <T>(s: string) => Promise<T[]> }): Promise<string[]> {
  const rows = await db.query<TaskRow>(`SELECT id, title, done FROM tasks ORDER BY id`);
  return rows.map((r) => r.title);
}

describe("SyncedPluginDatabase", () => {
  it("allows the internal debug executor to inspect protected SQLite tables", async () => {
    const db = makeEngine({ doc: new Y.Doc(), snap: newSnapStore() });
    try {
      await db.start();
      await db.whenLive();

      await expect(db.query("SELECT name FROM sqlite_master")).rejects.toThrow(
        "may not touch crsql_* or sqlite_* internals",
      );
      const rows = await db.debugExecute(
        "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name",
      );
      expect(rows.some((row) => row.name === "tasks")).toBe(true);
      await expect(db.debugExecute("SELECT count(*) AS count FROM crsql_changes")).resolves.toEqual(
        [{ count: 0 }],
      );
      await expect(
        db.debugExecute("UPDATE tasks SET title = 'debugged' WHERE id = 'missing'"),
      ).resolves.toEqual([]);
    } finally {
      await db.close();
    }
  });

  it("converges two peers through a shared batch log", async () => {
    const doc = new Y.Doc();
    const a = makeEngine({ doc, snap: newSnapStore() });
    const b = makeEngine({ doc, snap: newSnapStore() });
    try {
      await a.start();
      await b.start();
      await a.whenLive();
      await b.whenLive();

      await a.exec(`INSERT INTO tasks (id, title) VALUES (?, ?)`, ["a1", "from-a"]);
      await b.exec(`INSERT INTO tasks (id, title) VALUES (?, ?)`, ["b1", "from-b"]);

      await waitFor(async () => (await titles(a)).length === 2, { label: "a sees both" });
      await waitFor(async () => (await titles(b)).length === 2, { label: "b sees both" });

      expect(await titles(a)).toEqual(["from-a", "from-b"]);
      expect(await titles(b)).toEqual(["from-a", "from-b"]);
    } finally {
      await a.close();
      await b.close();
    }
  });

  it("queues offline writes and reconciles on reconnect", async () => {
    const relay = makeRelay();
    const a = makeEngine({ doc: relay.a, snap: newSnapStore() });
    const b = makeEngine({ doc: relay.b, snap: newSnapStore() });
    try {
      await a.start();
      await b.start();
      await a.whenLive();
      await b.whenLive();

      relay.pause();
      await a.exec(`INSERT INTO tasks (id, title) VALUES (?, ?)`, ["off-a", "offline-a"]);
      await b.exec(`INSERT INTO tasks (id, title) VALUES (?, ?)`, ["off-b", "offline-b"]);
      // Give the publish debounce time to append batches into each (isolated) doc.
      await new Promise((r) => setTimeout(r, 400));
      expect((await titles(a)).length).toBe(1);
      expect((await titles(b)).length).toBe(1);

      relay.resume();
      await waitFor(async () => (await titles(a)).length === 2, { label: "a reconciles" });
      await waitFor(async () => (await titles(b)).length === 2, { label: "b reconciles" });
    } finally {
      await a.close();
      await b.close();
    }
  });

  it("preserves clocks across snapshot save/restore without resurrecting deletes", async () => {
    const snap = newSnapStore();

    const doc1 = new Y.Doc();
    const a1 = makeEngine({ doc: doc1, snap });
    await a1.start();
    await a1.whenLive();
    await a1.exec(`INSERT INTO tasks (id, title) VALUES (?, ?)`, ["x", "keep"]);
    await a1.exec(`INSERT INTO tasks (id, title) VALUES (?, ?)`, ["y", "drop"]);
    await a1.close(); // persists snapshot

    // Reopen from snapshot: both rows survive.
    const doc2 = new Y.Doc();
    const a2 = makeEngine({ doc: doc2, snap });
    await a2.start();
    await a2.whenLive();
    expect(await titles(a2)).toEqual(["keep", "drop"]);

    // Delete one, snapshot again.
    await a2.exec(`DELETE FROM tasks WHERE id = ?`, ["y"]);
    await a2.close();

    // Reopen: the delete is preserved (no resurrection).
    const doc3 = new Y.Doc();
    const a3 = makeEngine({ doc: doc3, snap });
    try {
      await a3.start();
      await a3.whenLive();
      expect(await titles(a3)).toEqual(["keep"]);
    } finally {
      await a3.close();
    }
  });

  it("buffers batches from a newer schema and reports needs-migration", async () => {
    const doc = new Y.Doc();
    const newer = makeEngine({ doc, snap: newSnapStore(), schemaVersion: 2 });
    const older = makeEngine({ doc, snap: newSnapStore(), schemaVersion: 1 });
    try {
      await newer.start();
      await older.start();
      await newer.whenLive();
      await older.whenLive();

      await newer.exec(`INSERT INTO tasks (id, title) VALUES (?, ?)`, ["n1", "new-schema"]);

      await waitFor(() => older.state === "needs-migration", {
        label: "older enters needs-migration",
      });
      // The older peer must NOT apply ahead of its local schema.
      expect((await titles(older)).length).toBe(0);
    } finally {
      await newer.close();
      await older.close();
    }
  });

  it("drains buffered batches and returns to live after upgradeSchema", async () => {
    const doc = new Y.Doc();
    const newer = makeEngine({ doc, snap: newSnapStore(), schemaVersion: 2 });
    const older = makeEngine({ doc, snap: newSnapStore(), schemaVersion: 1 });
    try {
      await newer.start();
      await older.start();
      await newer.whenLive();
      await older.whenLive();

      await newer.exec(`INSERT INTO tasks (id, title) VALUES (?, ?)`, ["n1", "new-schema"]);
      await waitFor(() => older.state === "needs-migration", {
        label: "older enters needs-migration",
      });
      expect((await titles(older)).length).toBe(0);

      // The consumer "updates": migrate the running engine to v2. Buffered
      // batches drain and the engine goes back to live.
      await older.upgradeSchema(2, async (_tx, _from) => {
        /* schema unchanged between v1 and v2 in this test */
      });
      await waitFor(() => older.state === "live", { label: "older back to live" });
      expect(older.currentSchemaVersion).toBe(2);
      await waitFor(async () => (await titles(older)).length === 1, {
        label: "older applied the buffered batch",
      });
      expect(await titles(older)).toEqual(["new-schema"]);
    } finally {
      await newer.close();
      await older.close();
    }
  });

  it("falls back to the bootstrap endpoint when a needed range was compacted away", async () => {
    _resetSqliteForTests();
    const doc = new Y.Doc();
    // Producer makes data, then we simulate the server compacting it away.
    const producer = makeEngine({ doc, snap: newSnapStore() });
    await producer.start();
    await producer.whenLive();
    await producer.exec(`INSERT INTO tasks (id, title) VALUES (?, ?)`, ["c1", "compacted"]);
    await new Promise((r) => setTimeout(r, 400));

    // Capture the producer's full changeset to act as the server replica.
    const serverRows = await (
      await import("../../src/pluginDb/crsqlite")
    ).readAllChanges((producer as unknown as { db: any }).db);

    // A fresh consumer whose batch log is empty but whose meta says the range
    // was compacted: it must pull from bootstrap instead.
    const consumerDoc = new Y.Doc();
    const meta = consumerDoc.getMap("meta");
    const consumer = makeEngine({
      doc: consumerDoc,
      snap: newSnapStore(),
      bootstrap: async () => serverRows,
    });
    try {
      await consumer.start();
      await consumer.whenLive();
      await consumer.rebaseFromServer();
      await waitFor(async () => (await titles(consumer)).length === 1, {
        label: "consumer bootstrapped",
      });
      expect(await titles(consumer)).toEqual(["compacted"]);
    } finally {
      await producer.close();
      await consumer.close();
    }
  });

  it("stalls on a gapped batch instead of jumping the cursor, and recovers when the gap arrives", async () => {
    _resetSqliteForTests();
    const producerDoc = new Y.Doc();
    const producer = makeEngine({ doc: producerDoc, snap: newSnapStore() });
    const consumerDoc = new Y.Doc();
    const consumer = makeEngine({ doc: consumerDoc, snap: newSnapStore() });
    try {
      await producer.start();
      await producer.whenLive();
      await producer.exec(`INSERT INTO tasks (id, title) VALUES (?, ?)`, ["r1", "first"]);
      await new Promise((r) => setTimeout(r, 400));
      await producer.exec(`INSERT INTO tasks (id, title) VALUES (?, ?)`, ["r2", "second"]);
      await new Promise((r) => setTimeout(r, 400));

      const batches = producerDoc.getArray<any>("batches").toArray();
      expect(batches.length).toBeGreaterThanOrEqual(2);
      const first = batches[0];
      const later = batches[batches.length - 1];
      expect(later.fromDbVersion).toBeGreaterThan(0);

      // Deliver ONLY the later batch: applying it would jump the cursor past
      // the missing range and silently drop "first" forever.
      consumerDoc.getArray<any>("batches").push([later]);
      await consumer.start();
      await consumer.whenLive();
      await new Promise((r) => setTimeout(r, 300));
      expect(await titles(consumer)).toEqual([]);

      // The missing batch arrives out of order (appended at the end of the
      // log): the consumer must apply it and then the stalled later batch.
      consumerDoc.getArray<any>("batches").push([first]);
      await waitFor(async () => (await titles(consumer)).length === 2, {
        label: "consumer recovered once the gap arrived",
      });
      expect(await titles(consumer)).toEqual(["first", "second"]);
    } finally {
      await producer.close();
      await consumer.close();
    }
  });

  it("does not jump the cursor past a buffered newer-schema batch", async () => {
    _resetSqliteForTests();
    const doc = new Y.Doc();
    const newer = makeEngine({ doc, snap: newSnapStore(), schemaVersion: 2 });
    const older = makeEngine({ doc, snap: newSnapStore(), schemaVersion: 1 });
    try {
      await newer.start();
      await newer.whenLive();
      await newer.exec(`INSERT INTO tasks (id, title) VALUES (?, ?)`, ["b1", "buffered"]);
      await new Promise((r) => setTimeout(r, 400));
      await newer.exec(`INSERT INTO tasks (id, title) VALUES (?, ?)`, ["b2", "behind-buffer"]);
      await new Promise((r) => setTimeout(r, 400));

      // Simulate a later same-site batch authored at an older schema version
      // (e.g. republished by a downgraded client): the consumer must not let
      // it advance the cursor past the buffered batch's range.
      const arr = doc.getArray<any>("batches");
      const list = arr.toArray();
      expect(list.length).toBeGreaterThanOrEqual(2);
      arr.delete(list.length - 1, 1);
      arr.insert(list.length - 1, [{ ...list[list.length - 1], schemaVersion: 1 }]);

      await older.start();
      await waitFor(() => older.state === "needs-migration", {
        label: "older enters needs-migration",
      });
      await new Promise((r) => setTimeout(r, 300));
      expect(await titles(older)).toEqual([]);

      await older.upgradeSchema(2, async (_tx, _from) => {
        /* schema unchanged between v1 and v2 in this test */
      });
      await waitFor(() => older.state === "live", { label: "older back to live" });
      await waitFor(async () => (await titles(older)).length === 2, {
        label: "older applied the buffered batch and the one stalled behind it",
      });
      expect(await titles(older)).toEqual(["buffered", "behind-buffer"]);
    } finally {
      await newer.close();
      await older.close();
    }
  });

  it("rebases on startup when the log was compacted past the snapshot cursor", async () => {
    _resetSqliteForTests();
    const doc = new Y.Doc();
    const snap = newSnapStore();
    const producer = makeEngine({ doc, snap: newSnapStore() });
    const first = makeEngine({ doc, snap });
    await producer.start();
    await first.start();
    await producer.whenLive();
    await first.whenLive();

    await producer.exec(`INSERT INTO tasks (id, title) VALUES (?, ?)`, ["early", "early-row"]);
    await waitFor(async () => (await titles(first)).includes("early-row"), {
      label: "first consumer caught up",
    });
    await first.close(); // snapshot records the cursor at the early batch

    await producer.exec(`INSERT INTO tasks (id, title) VALUES (?, ?)`, ["late", "late-row"]);
    await new Promise((r) => setTimeout(r, 400));
    const serverRows = await readAllChanges((producer as any).db);

    // Simulate the server compacting the whole log while the consumer was
    // away: every batch dropped, only the high-water mark remains.
    const arr = doc.getArray<any>("batches");
    const marks: Record<string, number> = {};
    for (const b of arr.toArray()) {
      marks[b.siteId] = Math.max(marks[b.siteId] ?? 0, b.toDbVersion);
    }
    arr.delete(0, arr.length);
    doc.getMap("meta").set("compactedThrough", marks);

    const second = makeEngine({ doc, snap, bootstrap: async () => serverRows });
    try {
      await second.start();
      await second.whenLive();
      await waitFor(async () => (await titles(second)).length === 2, {
        label: "rebased from server on startup",
      });
      expect(await titles(second)).toEqual(["early-row", "late-row"]);
    } finally {
      await producer.close();
      await second.close();
    }
  });

  it("republishes rows whose batches were lost with the local doc store", async () => {
    _resetSqliteForTests();
    const snap = newSnapStore();
    const doc1 = new Y.Doc();
    const first = makeEngine({ doc: doc1, snap });
    await first.start();
    await first.whenLive();
    await first.exec(`INSERT INTO tasks (id, title) VALUES (?, ?)`, ["lost-doc", "republished"]);
    await first.close(); // snapshot records published=V; the batch only lives in doc1

    // Simulate loss of the Y.Doc store (e.g. IndexedDB eviction): the
    // snapshot survives but no batch ever reached the server. The restored
    // published watermark must not skip the re-publish.
    const doc2 = new Y.Doc();
    const second = makeEngine({ doc: doc2, snap, bootstrap: async () => [] });
    try {
      await second.start();
      await second.whenLive();
      expect(await titles(second)).toEqual(["republished"]);
      await waitFor(() => doc2.getArray("batches").length > 0, {
        label: "lost range republished after doc-store loss",
      });
    } finally {
      await second.close();
    }
  });

  it("flushes unpublished local edits before rebaseFromServer resets the DB", async () => {
    const doc = new Y.Doc();
    const db = makeEngine({ doc, snap: newSnapStore(), bootstrap: async () => [] });
    try {
      await db.start();
      await db.whenLive();

      await db.exec(`INSERT INTO tasks (id, title) VALUES (?, ?)`, ["local", "survives-rebase"]);
      await db.rebaseFromServer();

      await waitFor(async () => (await titles(db)).includes("survives-rebase"), {
        label: "unpublished local row survived rebase",
      });
    } finally {
      await db.close();
    }
  });

  it("publishes one committed db_version atomically when it exceeds the row target", async () => {
    const doc = new Y.Doc();
    const producer = makeEngine({ doc, snap: newSnapStore() });
    const consumer = makeEngine({ doc, snap: newSnapStore() });
    try {
      await producer.start();
      await producer.whenLive();
      await producer.transaction(async (tx) => {
        for (let index = 0; index < 600; index++) {
          await tx.exec(`INSERT INTO tasks (id, title) VALUES (?, ?)`, [
            `bulk-${index.toString().padStart(3, "0")}`,
            `row-${index}`,
          ]);
        }
      });

      const batches = doc.getArray<any>("batches");
      await waitFor(
        () => batches.toArray().some((batch) => batch.changes.length > MAX_BATCH_ROWS),
        { label: "oversized transaction published atomically" },
      );
      const oversized = batches.toArray().find((batch) => batch.changes.length > MAX_BATCH_ROWS);
      expect(
        new Set(oversized.changes.map((row: { db_version: number }) => row.db_version)).size,
      ).toBe(1);

      await consumer.start();
      await consumer.whenLive();
      await waitFor(async () => (await titles(consumer)).length === 600, {
        timeout: 20_000,
        label: "consumer received every row in oversized transaction",
      });
      expect(await titles(consumer)).toHaveLength(600);
    } finally {
      await producer.close();
      await consumer.close();
    }
  });

  it("rejects startup when the authoritative bootstrap fails", async () => {
    const snap = newSnapStore();
    const db = makeEngine({
      doc: new Y.Doc(),
      snap,
      bootstrap: async () => {
        throw new Error("bootstrap unavailable");
      },
    });
    try {
      await expect(db.start()).rejects.toThrow("bootstrap unavailable");
      expect(db.state).toBe("error");
    } finally {
      await db.close();
    }
    expect(snap.text).toBeNull();
  });

  it("keeps the live database intact when a rebase bootstrap fails", async () => {
    let failBootstrap = false;
    const db = makeEngine({
      doc: new Y.Doc(),
      snap: newSnapStore(),
      bootstrap: async () => {
        if (failBootstrap) throw new Error("rebase unavailable");
        return [];
      },
    });
    try {
      await db.start();
      await db.whenLive();
      await db.exec(`INSERT INTO tasks (id, title) VALUES (?, ?)`, ["local", "still-live"]);

      failBootstrap = true;
      await expect(db.rebaseFromServer()).rejects.toThrow("rebase unavailable");
      expect(db.state).toBe("live");
      expect(await titles(db)).toEqual(["still-live"]);
    } finally {
      await db.close();
    }
  });

  it("does not keep rebasing when a site's newest changes were overwritten before compaction", async () => {
    _resetSqliteForTests();
    const doc = new Y.Doc();
    const a = makeEngine({ doc, snap: newSnapStore() });
    const b = makeEngine({ doc, snap: newSnapStore() });
    let consumer: ReturnType<typeof makeEngine> | null = null;
    try {
      await a.start();
      await b.start();
      await a.whenLive();
      await b.whenLive();
      await a.exec(`INSERT INTO tasks (id, title) VALUES (?, ?)`, ["t", "from a"]);
      await new Promise((r) => setTimeout(r, 400));
      await a.exec(`UPDATE tasks SET title = ? WHERE id = ?`, ["a again", "t"]);
      await waitFor(async () => (await titles(b)).includes("a again"), { label: "b saw a" });
      await b.exec(`UPDATE tasks SET title = ? WHERE id = ?`, ["b wins", "t"]);
      await waitFor(async () => (await titles(a)).includes("b wins"), { label: "a saw b" });
      await new Promise((r) => setTimeout(r, 400));

      // The server replica, its cursor, and a log compacted through it: a's
      // last change survives only as b's overwrite.
      const replica = await readAllChanges((b as any).db);
      const arr = doc.getArray<any>("batches");
      const marks: Record<string, number> = {};
      for (const batch of arr.toArray()) {
        marks[batch.siteId] = Math.max(marks[batch.siteId] ?? 0, batch.toDbVersion);
      }
      arr.delete(0, arr.length);
      doc.getMap("meta").set("compactedThrough", marks);

      let bootstraps = 0;
      consumer = makeEngine({
        doc,
        snap: newSnapStore(),
        bootstrap: async () => {
          bootstraps++;
          return { changes: replica, cursor: marks };
        },
      });
      await consumer.start();
      await consumer.whenLive();
      expect(await titles(consumer)).toEqual(["b wins"]);
      // A later metadata change must not trigger another full rebase.
      doc.getMap("meta").set("compactedThrough", { ...marks });
      await new Promise((r) => setTimeout(r, 300));
      expect(bootstraps).toBe(1);
    } finally {
      await a.close();
      await b.close();
      await consumer?.close();
    }
  });

  it("does not rebase after a restart when its own earlier batches were partly compacted", async () => {
    _resetSqliteForTests();
    const doc = new Y.Doc();
    const snap = newSnapStore();
    const first = makeEngine({ doc, snap });
    await first.start();
    await first.whenLive();
    await first.exec(`INSERT INTO tasks (id, title) VALUES (?, ?)`, ["one", "first"]);
    await new Promise((r) => setTimeout(r, 400));
    await first.exec(`INSERT INTO tasks (id, title) VALUES (?, ?)`, ["two", "second"]);
    await new Promise((r) => setTimeout(r, 400));
    await first.close();

    // Other devices applied the first batch, so the server compacted it; the
    // second is still in the log.
    const arr = doc.getArray<any>("batches");
    expect(arr.length).toBe(2);
    const dropped = arr.get(0);
    arr.delete(0, 1);
    doc.getMap("meta").set("compactedThrough", { [dropped.siteId]: dropped.toDbVersion });

    let bootstraps = 0;
    const second = makeEngine({
      doc,
      snap,
      bootstrap: async () => {
        bootstraps++;
        return [];
      },
    });
    try {
      await second.start();
      await second.whenLive();
      await new Promise((r) => setTimeout(r, 300));
      expect(bootstraps).toBe(0);
      expect(await titles(second)).toEqual(["first", "second"]);
    } finally {
      await second.close();
    }
  });

  it("replays own-site batches that are newer than the restored snapshot", async () => {
    const doc = new Y.Doc();
    const snap = newSnapStore();
    const first = makeEngine({ doc, snap });
    const secondSnap = newSnapStore();
    let second: typeof first | null = null;
    try {
      await first.start();
      await first.whenLive();
      await first.exec(`INSERT INTO tasks (id, title) VALUES (?, ?)`, ["v1", "in-snapshot"]);
      await new Promise((r) => setTimeout(r, 400));
      await (first as any).persistSnapshot();
      const staleSnapshot = snap.text;
      expect(staleSnapshot).toBeTruthy();

      await first.exec(`INSERT INTO tasks (id, title) VALUES (?, ?)`, ["v2", "in-log-only"]);
      await new Promise((r) => setTimeout(r, 400));

      secondSnap.text = staleSnapshot;
      second = makeEngine({ doc, snap: secondSnap });
      await second.start();
      await second.whenLive();

      await waitFor(async () => (await titles(second!)).includes("in-log-only"), {
        label: "own newer batch replayed over stale snapshot",
      });
      expect(await titles(second)).toEqual(["in-snapshot", "in-log-only"]);
    } finally {
      await first.close();
      await second?.close();
    }
  });

  it("does not skip an own-site batch when it is newer than the local published watermark", async () => {
    const producerDoc = new Y.Doc();
    const producer = makeEngine({ doc: producerDoc, snap: newSnapStore() });
    const consumerDoc = new Y.Doc();
    const consumer = makeEngine({ doc: consumerDoc, snap: newSnapStore() });
    try {
      await producer.start();
      await producer.whenLive();
      await producer.exec(`INSERT INTO tasks (id, title) VALUES (?, ?)`, ["own", "own-newer"]);
      const changes = await readAllChanges((producer as any).db);
      expect(changes.length).toBeGreaterThan(0);

      await consumer.start();
      await consumer.whenLive();
      const site = (consumer as any).siteHex as string;
      consumerDoc.getArray("batches").push([
        {
          id: "forced-own-newer",
          siteId: site,
          fromDbVersion: 0,
          toDbVersion: changes[changes.length - 1].db_version,
          schemaVersion: 1,
          changes: changes.slice(0, MAX_BATCH_ROWS),
          createdAt: Date.now(),
          format: SYNC_FORMAT,
        },
      ]);

      await waitFor(async () => (await titles(consumer)).includes("own-newer"), {
        label: "own newer batch applied",
      });
    } finally {
      await producer.close();
      await consumer.close();
    }
  });

  it("does not stall on own-site batches with fromDbVersion > 0 (crash-recovery)", async () => {
    // Crash-recovery scenario: an own-site batch with fromDbVersion > 0
    // arrives (e.g. a second publish from before a restart). The applied
    // cursor doesn't track own-site progress, so the gap check would
    // false-fire and stall with a warning on every observer callback.
    const producerDoc = new Y.Doc();
    const producer = makeEngine({ doc: producerDoc, snap: newSnapStore() });
    const consumerDoc = new Y.Doc();
    const consumer = makeEngine({ doc: consumerDoc, snap: newSnapStore() });
    try {
      await producer.start();
      await producer.whenLive();
      await producer.exec(`INSERT INTO tasks (id, title) VALUES (?, ?)`, ["a", "row-a"]);
      await new Promise((r) => setTimeout(r, 400));
      await producer.exec(`INSERT INTO tasks (id, title) VALUES (?, ?)`, ["b", "row-b"]);
      await new Promise((r) => setTimeout(r, 400));

      await consumer.start();
      await consumer.whenLive();
      const site = (consumer as any).siteHex as string;
      const changes = await readAllChanges((producer as any).db);
      const last = changes[changes.length - 1].db_version;

      // Inject an own-site batch with fromDbVersion > 0 (simulating a
      // crash-recovery republish). Set published to the first batch's
      // watermark so the second batch is "past published" but contiguous.
      (consumer as any).published = { [site]: changes[0].db_version };
      consumerDoc.getArray("batches").push([
        {
          id: "own-gap-batch",
          siteId: site,
          fromDbVersion: changes[0].db_version,
          toDbVersion: last,
          schemaVersion: 1,
          changes: changes.slice(0, MAX_BATCH_ROWS),
          createdAt: Date.now(),
          format: SYNC_FORMAT,
        },
      ]);

      await waitFor(async () => (await titles(consumer)).includes("row-b"), {
        label: "own-site batch with fromDbVersion > 0 applied without stalling",
      });
      expect((consumer as any).published[site]).toBeGreaterThanOrEqual(last);
    } finally {
      await producer.close();
      await consumer.close();
    }
  });

  it("applies a server-authored batch whose integer fields arrive as BigInt", async () => {
    // The server publishes batches into the Y.Doc via lib0 `Any::BigInt` (see
    // server/src/ydoc.rs `json_to_any`), which the client Yjs binding decodes as
    // JS `bigint`. The engine must normalize those back to `number` so call
    // sites like `Math.max(applied, batch.toDbVersion)` do not throw.
    const producerDoc = new Y.Doc();
    const producer = makeEngine({ doc: producerDoc, snap: newSnapStore() });
    const consumerDoc = new Y.Doc();
    const consumer = makeEngine({ doc: consumerDoc, snap: newSnapStore() });
    try {
      await producer.start();
      await producer.whenLive();
      await producer.exec(`INSERT INTO tasks (id, title) VALUES (?, ?)`, ["s1", "server-bigint"]);
      const changes = await readAllChanges((producer as any).db);
      expect(changes.length).toBeGreaterThan(0);
      const producerSite = (producer as any).siteHex as string;

      await consumer.start();
      await consumer.whenLive();

      // Rebuild the batch with every integer field as BigInt, mirroring lib0's
      // Any::BigInt decoding of a server-published batch.
      const last = changes[changes.length - 1];
      const bigintChanges = changes.map((c) => ({
        ...c,
        col_version: BigInt(c.col_version),
        db_version: BigInt(c.db_version),
        cl: BigInt(c.cl),
        seq: BigInt(c.seq),
      }));
      consumerDoc.getArray("batches").push([
        {
          id: "server-bigint-batch",
          siteId: producerSite,
          fromDbVersion: BigInt(0),
          toDbVersion: BigInt(last.db_version),
          schemaVersion: BigInt(1),
          changes: bigintChanges,
          createdAt: BigInt(Date.now()),
          format: SYNC_FORMAT,
        },
      ]);

      await waitFor(async () => (await titles(consumer)).includes("server-bigint"), {
        label: "bigint batch applied without throwing",
      });
      expect(await titles(consumer)).toEqual(["server-bigint"]);
      // The crash site is `Math.max(applied, batch.toDbVersion)` which runs
      // AFTER applyChanges succeeds — so the row lands but the cursor never
      // advances and the batch is retried forever. Assert the cursor moved.
      await waitFor(() => ((consumer as any).cursor[producerSite] ?? 0) >= last.db_version, {
        label: "cursor advanced past the bigint batch",
      });
    } finally {
      await producer.close();
      await consumer.close();
    }
  });
});

describe("sqlIdentifiers / lint", () => {
  it("extracts identifiers, skipping strings and comments", async () => {
    const { sqlIdentifiers } = await import("../../src/pluginDb/SyncedPluginDatabase");
    expect(sqlIdentifiers("SELECT a, b FROM tasks WHERE t = 'sqlite_master'")).toEqual([
      "SELECT",
      "a",
      "b",
      "FROM",
      "tasks",
      "WHERE",
      "t",
    ]);
    expect(sqlIdentifiers("SELECT 1 -- crsql_changes\n+ 2")).toEqual(["SELECT"]);
    expect(sqlIdentifiers("SELECT /* sqlite_master */ x")).toEqual(["SELECT", "x"]);
    // Quoted identifiers are identifiers (with doubled-quote escapes).
    expect(sqlIdentifiers('SELECT * FROM "sqlite_master"')).toEqual([
      "SELECT",
      "FROM",
      "sqlite_master",
    ]);
    expect(sqlIdentifiers("SELECT * FROM `crsql_changes`")).toEqual([
      "SELECT",
      "FROM",
      "crsql_changes",
    ]);
    expect(sqlIdentifiers("SELECT * FROM [sqlite_master]")).toEqual([
      "SELECT",
      "FROM",
      "sqlite_master",
    ]);
    expect(sqlIdentifiers(`SELECT "we""ird" FROM t`)).toEqual(["SELECT", 'we"ird', "FROM", "t"]);
    // Escaped quote inside a string literal does not end it early.
    expect(sqlIdentifiers("SELECT * FROM t WHERE a = 'it''s crsql_x'")).toEqual([
      "SELECT",
      "FROM",
      "t",
      "WHERE",
      "a",
    ]);
  });
});

describe("RealtimeSqlAPI lifecycle", () => {
  const makePlugin = () => ({
    settings: {
      enabled: true,
      authServerUrl: "https://sync.example.test",
      authServerId: "server-a",
      userId: "user-a",
      activeVaultId: "vault-a",
    },
    auth: {
      isLoggedIn: true,
      ensureServerId: vi.fn(async () => "server-a"),
    },
    lastCompatibilityError: null as null | {
      reason: "server-incompatible";
      detail: string;
      serverVersion?: string;
    },
  });

  it("closes cached engines when the active vault scope changes", async () => {
    const plugin = makePlugin();
    const api = new RealtimeSqlAPI(plugin as any);
    const close = vi.fn(async () => {});
    (api as any).activeScope = (api as any).scope();
    (api as any).engines.set("cached", { close });

    plugin.settings.activeVaultId = "vault-b";
    await api.reconcileLifecycle();

    expect(close).toHaveBeenCalledOnce();
    expect((api as any).engines.size).toBe(0);
    expect((api as any).activeScope).toBeNull();
  });

  it("blocks SQL availability on a server capability mismatch", async () => {
    const plugin = makePlugin();
    plugin.lastCompatibilityError = {
      reason: "server-incompatible",
      detail: "server requires an unsupported pluginDbSync format",
    };
    const api = new RealtimeSqlAPI(plugin as any);

    await expect(api.whenAvailable()).rejects.toBeInstanceOf(CompatibilityError);
  });
});
