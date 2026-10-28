import * as Y from "yjs";
import { reconcileInto, reconcileValue, type JsonValue, type ReconcileOptions } from "./reconcile";

export interface StructuredCanvas extends Record<string, JsonValue> {
  nodes: Record<string, JsonValue>;
  edges: Record<string, JsonValue>;
  nodeOrder: string[];
  edgeOrder: string[];
}

export function parseCanvas(text: string): StructuredCanvas {
  const data = text.trim() ? JSON.parse(text) : {};
  const nodes: Record<string, JsonValue> = {};
  const edges: Record<string, JsonValue> = {};
  const nodeOrder: string[] = [];
  const edgeOrder: string[] = [];
  for (const node of Array.isArray(data.nodes) ? data.nodes : []) {
    if (!node || typeof node.id !== "string") continue;
    nodes[node.id] = node;
    nodeOrder.push(node.id);
  }
  for (const edge of Array.isArray(data.edges) ? data.edges : []) {
    if (!edge || typeof edge.id !== "string") continue;
    edges[edge.id] = edge;
    edgeOrder.push(edge.id);
  }
  return { nodes, edges, nodeOrder, edgeOrder };
}

export function serializeCanvas(value: JsonValue): string {
  const root = isObject(value) ? value : {};
  const nodes = isObject(root.nodes) ? root.nodes : {};
  const edges = isObject(root.edges) ? root.edges : {};
  const nodeOrder = Array.isArray(root.nodeOrder)
    ? root.nodeOrder.filter(isString)
    : Object.keys(nodes);
  const edgeOrder = Array.isArray(root.edgeOrder)
    ? root.edgeOrder.filter(isString)
    : Object.keys(edges);
  const orderedNodes = uniqueCanvasOrder(nodeOrder, nodes)
    .map((id) => nodes[id])
    .filter(isObject);
  const orderedEdges = uniqueCanvasOrder(edgeOrder, edges)
    .map((id) => edges[id])
    .filter(isObject);
  return `${JSON.stringify({ nodes: orderedNodes, edges: orderedEdges }, null, 2)}\n`;
}

function uniqueCanvasOrder(order: readonly string[], items: Record<string, JsonValue>): string[] {
  const seen = new Set<string>();
  const result: string[] = [];
  for (const id of [...order, ...Object.keys(items)]) {
    if (id in items && !seen.has(id)) {
      seen.add(id);
      result.push(id);
    }
  }
  return result;
}

/**
 * Canvas-specific reconcile that prevents stale full-snapshot resurrection
 * while still allowing same-device undo.
 *
 * The generic {@link reconcileInto} is last-writer-wins per key with no
 * tombstones: if Device A deletes edge `e1` and Device B later captures a
 * stale snapshot that still contains `e1`, the generic reconciler happily
 * `map.set("e1", …)` it back. This function adds `deletedNodeIds` /
 * `deletedEdgeIds` tombstone maps alongside `nodes` / `edges`.
 *
 * **Tombstone value = creator's `clientID`.** When a device deletes an item,
 * it stamps the tombstone with `doc.clientID`. Tombstones are CRDT state, so
 * they replicate to every device via normal Yjs sync, carrying the stored
 * clientID value. On re-add:
 * - If the tombstone's stored clientID matches this device's clientID, it's a
 *   **same-device undo** — clear the tombstone and re-insert the item.
 * - If the clientID differs, the snapshot is **stale** (another device deleted
 *   the item and this device's view hasn't caught up) — skip the re-insert.
 *
 * This preserves the undo UX ("delete edge, undo" syncs the edge back) while
 * blocking the stale-resurrection bug. The edge case of concurrent deletes of
 * the same id by two devices resolves to last-writer-wins on the tombstone
 * value, so at most one device can undo — acceptable for a rare race.
 *
 * **Deletions.** An item missing from `incoming` is deleted only when the
 * snapshot deliberately dropped it. With a `base` — the shared value the
 * snapshot was derived from (the content a startup or disk merge measured
 * local changes against) — that is exactly the items `base` holds: anything
 * else was added concurrently and never seen by the snapshot's author, so it
 * stays. Without a base the intent is unknown, and only items this client
 * created may be deleted; after a restart or a peer import every older item
 * is foreign, which is why callers that know their base must pass it.
 *
 * `nodeOrder` / `edgeOrder` are reconciled generically (arrays of ids); the
 * tombstone maps are managed here and left untouched by the generic path.
 */
export function reconcileCanvas(
  root: Y.Map<any>,
  incoming: StructuredCanvas,
  clientID: number,
  base?: StructuredCanvas,
): void {
  const nodes = ensureMap(root, "nodes");
  const edges = ensureMap(root, "edges");
  const deletedNodeIds = ensureMap(root, "deletedNodeIds");
  const deletedEdgeIds = ensureMap(root, "deletedEdgeIds");

  const options: ReconcileOptions = {};
  reconcileCanvasItems(nodes, incoming.nodes, deletedNodeIds, options, clientID, base?.nodes);
  reconcileCanvasItems(edges, incoming.edges, deletedEdgeIds, options, clientID, base?.edges);

  reconcileInto(ensureArray(root, "nodeOrder"), incoming.nodeOrder, options);
  reconcileInto(ensureArray(root, "edgeOrder"), incoming.edgeOrder, options);
}

function reconcileCanvasItems(
  itemsMap: Y.Map<any>,
  incoming: Record<string, JsonValue>,
  tombstones: Y.Map<number>,
  options: ReconcileOptions,
  clientID: number,
  known: Record<string, JsonValue> | undefined,
): void {
  for (const id of Array.from(itemsMap.keys())) {
    if (!(id in incoming)) {
      if (known) {
        // Not in the snapshot's base: a concurrent addition, not a deletion.
        if (!(id in known)) continue;
      } else {
        const originClient = (
          itemsMap as { _map?: Map<string, { id?: { client?: number } }> }
        )._map?.get(id)?.id?.client;
        // A stale snapshot must not tombstone items created by another client —
        // those concurrent remote nodes would be permanently un-resurrectable.
        if (originClient !== undefined && originClient !== clientID) continue;
      }
      // Stamp the tombstone with this device's clientID so a later same-device
      // re-add (undo) can clear it, while a cross-device stale snapshot cannot.
      tombstones.set(id, clientID);
      itemsMap.delete(id);
    }
  }
  for (const [id, incomingItem] of Object.entries(incoming)) {
    if (!isObject(incomingItem)) continue;
    const tombstonedBy = tombstones.get(id);
    if (tombstonedBy !== undefined) {
      // Same device that created the tombstone is re-adding — treat as undo.
      if (tombstonedBy === clientID) tombstones.delete(id);
      // Different device deleted this id; the incoming snapshot is stale.
      else continue;
    }
    let current = itemsMap.get(id);
    if (!(current instanceof Y.Map)) {
      current = new Y.Map();
      itemsMap.set(id, current);
    }
    reconcileItemFields(current, incomingItem, options);
  }
}

function reconcileItemFields(
  itemMap: Y.Map<any>,
  incoming: Record<string, JsonValue>,
  options: ReconcileOptions,
): void {
  for (const key of Array.from(itemMap.keys())) {
    if (!(key in incoming)) itemMap.delete(key);
  }
  for (const [key, next] of Object.entries(incoming)) {
    const current = itemMap.get(key);
    const reconciled = reconcileValue(current, next, key, options, (created) => {
      itemMap.set(key, created);
    });
    if (itemMap.get(key) !== reconciled) itemMap.set(key, reconciled);
  }
}

function ensureMap(root: Y.Map<any>, key: string): Y.Map<any> {
  let map = root.get(key);
  if (!(map instanceof Y.Map)) {
    map = new Y.Map();
    root.set(key, map);
  }
  return map;
}

function ensureArray(root: Y.Map<any>, key: string): Y.Array<any> {
  let array = root.get(key);
  if (!(array instanceof Y.Array)) {
    array = new Y.Array();
    root.set(key, array);
  }
  return array;
}

export function isStructuredCanvas(value: unknown): value is StructuredCanvas {
  if (!isObject(value)) return false;
  return (
    isObject(value.nodes) &&
    isObject(value.edges) &&
    Array.isArray(value.nodeOrder) &&
    Array.isArray(value.edgeOrder)
  );
}

function isObject(value: unknown): value is Record<string, JsonValue> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isString(value: unknown): value is string {
  return typeof value === "string";
}
