import * as Y from "yjs";

export type JsonValue =
  | null
  | boolean
  | number
  | string
  | JsonValue[]
  | { [key: string]: JsonValue };

export interface ReconcileOptions {
  textKeys?: Set<string>;
}

export interface StructuredMergeResult {
  value: JsonValue;
  conflicted: boolean;
}

/**
 * Apply only the local changes made from `baseline` while retaining unrelated
 * remote changes. Concurrent edits to the same scalar/array prefer local;
 * a local deletion does not erase a concurrently changed remote value.
 */
export function mergeStructuredStartup(
  baseline: JsonValue,
  local: JsonValue,
  remote: JsonValue,
): JsonValue {
  return mergeStructuredStartupResult(baseline, local, remote).value;
}

export function mergeStructuredStartupResult(
  baseline: JsonValue,
  local: JsonValue,
  remote: JsonValue,
): StructuredMergeResult {
  if (jsonEqual(local, baseline)) return { value: remote, conflicted: false };
  if (jsonEqual(remote, baseline) || jsonEqual(local, remote)) {
    return { value: local, conflicted: false };
  }
  if (!isRecord(local) || !isRecord(remote)) {
    return { value: local, conflicted: true };
  }

  const base = isRecord(baseline) ? baseline : {};
  const merged = Object.create(null) as Record<string, JsonValue>;
  let conflicted = false;
  const keys = new Set([...Object.keys(base), ...Object.keys(local), ...Object.keys(remote)]);
  for (const key of keys) {
    const baseHas = key in base;
    const localHas = key in local;
    const remoteHas = key in remote;

    if (!localHas) {
      if (!baseHas) {
        if (remoteHas) merged[key] = remote[key];
        continue;
      }
      if (!remoteHas) continue;
      // Local deleted the baseline key. Keep a concurrent remote edit, but
      // honor the deletion when remote still equals the baseline.
      if (!jsonEqual(remote[key], base[key])) {
        merged[key] = remote[key];
        conflicted = true;
      }
      continue;
    }
    if (!remoteHas) {
      if (!baseHas || !jsonEqual(local[key], base[key])) {
        merged[key] = local[key];
        if (baseHas) conflicted = true;
      }
      continue;
    }
    if (!baseHas) {
      if (isRecord(local[key]) && isRecord(remote[key])) {
        const child = mergeStructuredStartupResult({}, local[key], remote[key]);
        merged[key] = child.value;
        conflicted ||= child.conflicted;
      } else {
        merged[key] = local[key];
        conflicted ||= !jsonEqual(local[key], remote[key]);
      }
      continue;
    }
    const child = mergeStructuredStartupResult(base[key], local[key], remote[key]);
    merged[key] = child.value;
    conflicted ||= child.conflicted;
  }
  return { value: merged, conflicted };
}

const DEFAULT_TEXT_KEYS = new Set([
  "text",
  "label",
  "filter",
  "formula",
  "query",
  "name",
  "description",
]);

function shouldUseText(
  key: string | undefined,
  value: unknown,
  options?: ReconcileOptions,
): boolean {
  if (typeof value !== "string") return false;
  const keys = options?.textKeys ?? DEFAULT_TEXT_KEYS;
  return !!key && (keys.has(key) || value.length > 256);
}

export function reconcileInto(
  parent: Y.Map<any> | Y.Array<any>,
  value: JsonValue,
  options: ReconcileOptions = {},
): void {
  if (parent instanceof Y.Map) {
    reconcileMap(parent, isRecord(value) ? value : {}, options);
  } else if (parent instanceof Y.Array) {
    reconcileArray(parent, Array.isArray(value) ? value : [], options);
  }
}

function reconcileMap(
  map: Y.Map<any>,
  value: Record<string, JsonValue>,
  options: ReconcileOptions,
): void {
  for (const key of Array.from(map.keys())) {
    if (!(key in value)) map.delete(key);
  }
  for (const [key, next] of Object.entries(value)) {
    const current = map.get(key);
    const reconciled = reconcileValue(current, next, key, options, (created) =>
      map.set(key, created),
    );
    if (map.get(key) !== reconciled) map.set(key, reconciled);
  }
}

export function reconcileArray(
  array: Y.Array<any>,
  value: JsonValue[],
  options: ReconcileOptions,
): void {
  let i = 0;
  for (; i < value.length; i++) {
    const current = array.get(i);
    const next = reconcileValue(current, value[i], undefined, options, (created) => {
      if (i < array.length) array.delete(i, 1);
      array.insert(i, [created]);
    });
    if (array.get(i) !== next) {
      if (i < array.length) array.delete(i, 1);
      array.insert(i, [next]);
    }
  }
  if (array.length > value.length) array.delete(value.length, array.length - value.length);
}

export function reconcileValue(
  current: any,
  next: JsonValue,
  key: string | undefined,
  options: ReconcileOptions,
  attach: (created: any) => void,
): any {
  if (Array.isArray(next)) {
    const array = current instanceof Y.Array ? current : attachAndReturn(new Y.Array(), attach);
    reconcileArray(array, next, options);
    return array;
  }
  if (isRecord(next)) {
    const map = current instanceof Y.Map ? current : attachAndReturn(new Y.Map(), attach);
    reconcileMap(map, next, options);
    return map;
  }
  if (shouldUseText(key, next, options)) {
    const text = current instanceof Y.Text ? current : attachAndReturn(new Y.Text(), attach);
    applyStringToYText(text, typeof next === "string" ? next : "");
    return text;
  }
  return next;
}

function attachAndReturn<T>(value: T, attach: (created: T) => void): T {
  attach(value);
  return value;
}

export function toValue(value: any): JsonValue {
  if (value instanceof Y.Map) {
    const out: Record<string, JsonValue> = {};
    for (const [key, child] of value.entries()) out[key] = toValue(child);
    return out;
  }
  if (value instanceof Y.Array) return value.toArray().map((child) => toValue(child));
  if (value instanceof Y.Text) return value.toString();
  if (value === undefined) return null;
  return value as JsonValue;
}

function applyStringToYText(ytext: Y.Text, next: string): void {
  const old = ytext.toString();
  if (old === next) return;
  let prefix = 0;
  while (prefix < old.length && prefix < next.length && old[prefix] === next[prefix]) prefix++;
  let oldSuffix = old.length;
  let nextSuffix = next.length;
  while (oldSuffix > prefix && nextSuffix > prefix && old[oldSuffix - 1] === next[nextSuffix - 1]) {
    oldSuffix--;
    nextSuffix--;
  }
  const deleteLen = oldSuffix - prefix;
  if (deleteLen > 0) ytext.delete(prefix, deleteLen);
  const insert = next.slice(prefix, nextSuffix);
  if (insert) ytext.insert(prefix, insert);
}

/**
 * True when `outer` already holds everything in `inner`: every record key
 * with a contained value, and every array element in order. Used to settle
 * startups without a shared baseline where one side is a superset.
 */
export function jsonContains(outer: JsonValue, inner: JsonValue): boolean {
  if (jsonEqual(outer, inner)) return true;
  if (Array.isArray(outer) && Array.isArray(inner)) {
    let index = 0;
    for (const value of outer) {
      if (index === inner.length) break;
      if (jsonEqual(value, inner[index])) index++;
    }
    return index === inner.length;
  }
  if (!isRecord(outer) || !isRecord(inner)) return false;
  return Object.keys(inner).every((key) => key in outer && jsonContains(outer[key], inner[key]));
}

function isRecord(value: unknown): value is Record<string, JsonValue> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function jsonEqual(left: JsonValue, right: JsonValue): boolean {
  if (left === right) return true;
  if (Array.isArray(left) || Array.isArray(right)) {
    if (!Array.isArray(left) || !Array.isArray(right) || left.length !== right.length) {
      return false;
    }
    return left.every((value, index) => jsonEqual(value, right[index]));
  }
  if (!isRecord(left) || !isRecord(right)) return false;
  const leftKeys = Object.keys(left);
  const rightKeys = Object.keys(right);
  return (
    leftKeys.length === rightKeys.length &&
    leftKeys.every((key) => key in right && jsonEqual(left[key], right[key]))
  );
}
