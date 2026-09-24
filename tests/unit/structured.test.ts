import { describe, expect, it } from "vitest";
import * as Y from "yjs";
import {
  jsonContains,
  mergeStructuredStartup,
  mergeStructuredStartupResult,
  reconcileInto,
  toValue,
} from "../../src/structured/reconcile";
import {
  parseCanvas,
  reconcileCanvas,
  serializeCanvas,
  type StructuredCanvas,
} from "../../src/structured/canvas";
import { parseBase, serializeBase } from "../../src/structured/base";

describe("structured reconciler", () => {
  it("preserves unknown keys and is idempotent", () => {
    const doc = new Y.Doc();
    const root = doc.getMap("root");
    reconcileInto(root, {
      known: true,
      future: { nested: "value" },
      items: [{ id: "a", label: "Alpha" }],
    });
    const before = Y.encodeStateAsUpdate(doc).length;
    reconcileInto(root, toValue(root));
    const after = Y.encodeStateAsUpdate(doc).length;

    expect(toValue(root)).toEqual({
      known: true,
      future: { nested: "value" },
      items: [{ id: "a", label: "Alpha" }],
    });
    expect(after).toBe(before);
  });

  it("stores configured text fields as Y.Text", () => {
    const doc = new Y.Doc();
    const root = doc.getMap("root");
    reconcileInto(root, { text: "hello" });
    expect(root.get("text")).toBeInstanceOf(Y.Text);
    reconcileInto(root, { text: "hello world" });
    expect(toValue(root)).toEqual({ text: "hello world" });
  });

  it("retains remote additions while folding an offline local edit", () => {
    expect(
      mergeStructuredStartup(
        { nodes: { a: { x: 0 } }, nodeOrder: ["a"] },
        { nodes: { a: { x: 50 } }, nodeOrder: ["a"] },
        {
          nodes: { a: { x: 0 }, b: { x: 10 } },
          nodeOrder: ["a", "b"],
        },
      ),
    ).toEqual({
      nodes: { a: { x: 50 }, b: { x: 10 } },
      nodeOrder: ["a", "b"],
    });
  });

  it("does not erase a remote edit with a concurrent local deletion", () => {
    expect(
      mergeStructuredStartup(
        { filters: { status: "open", owner: "alice" } },
        { filters: { owner: "alice" } },
        { filters: { status: "closed", owner: "alice" } },
      ),
    ).toEqual({ filters: { status: "closed", owner: "alice" } });
  });

  it("reports concurrent edits to the same scalar for recovery-copy handling", () => {
    expect(
      mergeStructuredStartupResult(
        { filters: { status: "open" } },
        { filters: { status: "local" } },
        { filters: { status: "remote" } },
      ),
    ).toEqual({
      value: { filters: { status: "local" } },
      conflicted: true,
    });
  });

  it("does not report disjoint structured edits as conflicts", () => {
    expect(
      mergeStructuredStartupResult(
        { filters: { status: "open" }, view: "table" },
        { filters: { status: "local" }, view: "table" },
        { filters: { status: "open" }, view: "cards" },
      ),
    ).toEqual({
      value: { filters: { status: "local" }, view: "cards" },
      conflicted: false,
    });
  });
});

describe("canvas serializer", () => {
  it("round-trips JSON Canvas arrays through id-keyed maps", () => {
    const parsed = parseCanvas(
      JSON.stringify({
        nodes: [{ id: "n1", type: "text", text: "hi", custom: 1 }],
        edges: [{ id: "e1", fromNode: "n1", toNode: "n2" }],
      }),
    );

    expect(parsed.nodeOrder).toEqual(["n1"]);
    expect(parsed.edgeOrder).toEqual(["e1"]);
    expect(JSON.parse(serializeCanvas(parsed))).toEqual({
      nodes: [{ id: "n1", type: "text", text: "hi", custom: 1 }],
      edges: [{ id: "e1", fromNode: "n1", toNode: "n2" }],
    });
  });
});

describe("canvas concurrent merge", () => {
  const apply = (doc: Y.Doc, data: unknown) => {
    doc.transact(() =>
      reconcileCanvas(doc.getMap("root"), parseCanvas(JSON.stringify(data)), doc.clientID),
    );
  };
  const canvasNodes = (root: Y.Map<any>) =>
    (JSON.parse(serializeCanvas(toValue(root))).nodes ?? []) as Array<Record<string, unknown>>;
  const canvasEdges = (root: Y.Map<any>) =>
    (JSON.parse(serializeCanvas(toValue(root))).edges ?? []) as Array<Record<string, unknown>>;

  it("merges a node move and a different node's text edit cleanly", () => {
    const seed = {
      nodes: [
        { id: "n1", type: "text", text: "first", x: 0, y: 0 },
        { id: "n2", type: "text", text: "second", x: 100, y: 0 },
      ],
      edges: [],
    };

    const doc1 = new Y.Doc();
    const doc2 = new Y.Doc();
    apply(doc1, seed);
    Y.applyUpdate(doc2, Y.encodeStateAsUpdate(doc1));

    // Client 1 moves n1; client 2 edits n2's text — concurrently, offline.
    apply(doc1, {
      nodes: [
        { id: "n1", type: "text", text: "first", x: 250, y: 80 },
        { id: "n2", type: "text", text: "second", x: 100, y: 0 },
      ],
      edges: [],
    });
    apply(doc2, {
      nodes: [
        { id: "n1", type: "text", text: "first", x: 0, y: 0 },
        { id: "n2", type: "text", text: "second EDITED", x: 100, y: 0 },
      ],
      edges: [],
    });

    // Exchange updates both directions.
    Y.applyUpdate(doc1, Y.encodeStateAsUpdate(doc2));
    Y.applyUpdate(doc2, Y.encodeStateAsUpdate(doc1));

    const result1 = serializeCanvas(toValue(doc1.getMap("root")));
    const result2 = serializeCanvas(toValue(doc2.getMap("root")));
    expect(result1).toBe(result2);

    const nodes = JSON.parse(result1).nodes as Array<Record<string, unknown>>;
    const n1 = nodes.find((n) => n.id === "n1")!;
    const n2 = nodes.find((n) => n.id === "n2")!;
    expect(n1.x).toBe(250);
    expect(n1.y).toBe(80);
    expect(n2.text).toBe("second EDITED");
  });

  it("tombstones deleted edges and ignores stale snapshots that still contain them", () => {
    const seed = {
      nodes: [
        { id: "n1", type: "text", text: "first", x: 0, y: 0 },
        { id: "n2", type: "text", text: "second", x: 100, y: 0 },
      ],
      edges: [{ id: "e1", fromNode: "n1", toNode: "n2" }],
    };

    const doc1 = new Y.Doc();
    const doc2 = new Y.Doc();
    apply(doc1, seed);
    Y.applyUpdate(doc2, Y.encodeStateAsUpdate(doc1));

    // Client 1 deletes the edge.
    apply(doc1, {
      nodes: seed.nodes,
      edges: [],
    });
    // Client 2, still holding the stale edge, captures a local edit (e.g. a
    // node move) whose full snapshot still includes e1.
    apply(doc2, {
      nodes: [
        { id: "n1", type: "text", text: "first", x: 50, y: 50 },
        { id: "n2", type: "text", text: "second", x: 100, y: 0 },
      ],
      edges: [{ id: "e1", fromNode: "n1", toNode: "n2" }],
    });

    // Sync: doc2 receives doc1's tombstone; doc1 receives doc2's stale edge.
    Y.applyUpdate(doc1, Y.encodeStateAsUpdate(doc2));
    Y.applyUpdate(doc2, Y.encodeStateAsUpdate(doc1));

    // The edge must NOT be resurrected on either side.
    expect(canvasEdges(doc1.getMap("root"))).toEqual([]);
    expect(canvasEdges(doc2.getMap("root"))).toEqual([]);
    // The node move from doc2 should still propagate.
    const n1 = canvasNodes(doc1.getMap("root")).find((n) => n.id === "n1")!;
    expect(n1.x).toBe(50);
    expect(n1.y).toBe(50);
  });

  it("tombstones deleted nodes and ignores stale snapshots that still contain them", () => {
    const seed = {
      nodes: [
        { id: "n1", type: "text", text: "first", x: 0, y: 0 },
        { id: "n2", type: "text", text: "second", x: 100, y: 0 },
      ],
      edges: [],
    };

    const doc1 = new Y.Doc();
    const doc2 = new Y.Doc();
    apply(doc1, seed);
    Y.applyUpdate(doc2, Y.encodeStateAsUpdate(doc1));

    // Client 1 deletes n2.
    apply(doc1, {
      nodes: [{ id: "n1", type: "text", text: "first", x: 0, y: 0 }],
      edges: [],
    });
    // Client 2, still holding n2, captures a stale snapshot that includes it.
    apply(doc2, {
      nodes: [
        { id: "n1", type: "text", text: "first", x: 0, y: 0 },
        { id: "n2", type: "text", text: "second EDITED", x: 100, y: 0 },
      ],
      edges: [],
    });

    Y.applyUpdate(doc1, Y.encodeStateAsUpdate(doc2));
    Y.applyUpdate(doc2, Y.encodeStateAsUpdate(doc1));

    const ids1 = canvasNodes(doc1.getMap("root")).map((n) => n.id);
    const ids2 = canvasNodes(doc2.getMap("root")).map((n) => n.id);
    expect(ids1).toEqual(["n1"]);
    expect(ids2).toEqual(["n1"]);
  });

  it("allows same-device undo: re-adding a deleted item on the device that deleted it clears the tombstone", () => {
    // Device A deletes n1, then undoes — the same id reappears in A's local
    // capture. Because the tombstone was stamped with A's clientID, A clears
    // it and re-inserts n1. The undo propagates to other devices.
    const doc1 = new Y.Doc();
    const doc2 = new Y.Doc();
    apply(doc1, {
      nodes: [{ id: "n1", type: "text", text: "first", x: 0, y: 0 }],
      edges: [],
    });
    Y.applyUpdate(doc2, Y.encodeStateAsUpdate(doc1));

    // A deletes n1.
    apply(doc1, { nodes: [], edges: [] });
    expect(canvasNodes(doc1.getMap("root"))).toEqual([]);

    // A undoes — re-adds n1 with the same id. Same clientID → tombstone cleared.
    apply(doc1, {
      nodes: [{ id: "n1", type: "text", text: "first again", x: 10, y: 10 }],
      edges: [],
    });
    const nodes = canvasNodes(doc1.getMap("root"));
    expect(nodes.map((n) => n.id)).toEqual(["n1"]);
    expect(nodes[0].text).toBe("first again");

    // Sync to doc2: the undo (clear + re-add) propagates.
    Y.applyUpdate(doc2, Y.encodeStateAsUpdate(doc1));
    expect(canvasNodes(doc2.getMap("root")).map((n) => n.id)).toEqual(["n1"]);
  });

  it("blocks cross-device re-add of a tombstoned id even after sync", () => {
    // A deletes n1; the tombstone (stamped with A's clientID) syncs to B.
    // B then captures a snapshot that still contains n1 (stale view). Because
    // B's clientID differs from the tombstone's stored clientID, B's re-add
    // is blocked.
    const doc1 = new Y.Doc();
    const doc2 = new Y.Doc();
    apply(doc1, {
      nodes: [{ id: "n1", type: "text", text: "first", x: 0, y: 0 }],
      edges: [],
    });
    Y.applyUpdate(doc2, Y.encodeStateAsUpdate(doc1));

    apply(doc1, { nodes: [], edges: [] });
    Y.applyUpdate(doc2, Y.encodeStateAsUpdate(doc1)); // tombstone arrives at B

    // B captures a stale snapshot containing n1.
    apply(doc2, {
      nodes: [{ id: "n1", type: "text", text: "stale", x: 0, y: 0 }],
      edges: [],
    });
    expect(canvasNodes(doc2.getMap("root"))).toEqual([]);
  });

  it("new nodes with fresh ids are never affected by tombstones", () => {
    const doc = new Y.Doc();
    apply(doc, {
      nodes: [{ id: "n1", type: "text", text: "first", x: 0, y: 0 }],
      edges: [],
    });
    apply(doc, { nodes: [], edges: [] });
    apply(doc, {
      nodes: [{ id: "n2", type: "text", text: "second", x: 0, y: 0 }],
      edges: [],
    });
    const nodes = canvasNodes(doc.getMap("root"));
    expect(nodes.map((n) => n.id)).toEqual(["n2"]);
  });

  it("does not tombstone concurrent remote nodes missing from a stale snapshot", () => {
    const doc1 = new Y.Doc();
    const doc2 = new Y.Doc();
    apply(doc1, {
      nodes: [{ id: "n1", type: "text", text: "first", x: 0, y: 0 }],
      edges: [],
    });
    Y.applyUpdate(doc2, Y.encodeStateAsUpdate(doc1));

    apply(doc1, {
      nodes: [
        { id: "n1", type: "text", text: "first", x: 0, y: 0 },
        { id: "n3", type: "text", text: "remote", x: 10, y: 10 },
      ],
      edges: [],
    });
    Y.applyUpdate(doc2, Y.encodeStateAsUpdate(doc1));

    apply(doc2, {
      nodes: [{ id: "n1", type: "text", text: "moved", x: 5, y: 5 }],
      edges: [],
    });

    expect(
      canvasNodes(doc2.getMap("root"))
        .map((n) => n.id)
        .sort(),
    ).toEqual(["n1", "n3"]);
    Y.applyUpdate(doc1, Y.encodeStateAsUpdate(doc2));
    expect(
      canvasNodes(doc1.getMap("root"))
        .map((n) => n.id)
        .sort(),
    ).toEqual(["n1", "n3"]);
  });
});

describe("base serializer", () => {
  it("round-trips YAML values", () => {
    const value = parseBase("views:\n  - type: table\n    filter: status == 'open'\n");
    expect(parseBase(serializeBase(value))).toEqual(value);
  });
});

describe("jsonContains", () => {
  it("accepts supersets of records and ordered arrays", () => {
    const empty = parseCanvas("");
    const filled = parseCanvas(
      JSON.stringify({ nodes: [{ id: "a", type: "text", text: "hi" }], edges: [] }),
    );
    expect(jsonContains(filled, empty)).toBe(true);
    expect(jsonContains(empty, filled)).toBe(false);
    expect(jsonContains({ views: [1, 2, 3], extra: true }, { views: [1, 3] })).toBe(true);
    expect(jsonContains({ views: [1, 2, 3] }, { views: [3, 1] })).toBe(false);
    expect(jsonContains({ a: { b: 1, c: 2 } }, { a: { b: 2 } })).toBe(false);
  });
});
