/**
 * Version lineage for blob-backed index entries (`binaries`, `configFiles`).
 *
 * `prev` names only the version an entry replaced, so a device that missed
 * intermediate versions cannot tell them apart from a concurrent publish that
 * last-writer-wins kept over its own. Each entry therefore also carries the
 * hash prefixes of the versions it descends from, newest first.
 */

/** Ancestors one entry remembers. Older ones fall back to the `prev` rules. */
const LINEAGE_LENGTH = 8;
/** 64 bits of a sha256 hex digest: enough to tell a path's versions apart. */
const PREFIX_LENGTH = 16;

export interface VersionedEntry {
  hash: string;
  /** The version this one replaced (null for a new path); absent from older clients. */
  prev?: string | null;
  /** Hash prefixes of earlier versions, newest first; absent from older publishers. */
  lineage?: string[];
}

/** The lineage a new version replacing `replaced` publishes (empty for a new path). */
export function lineageAfter(replaced: VersionedEntry | null | undefined): string[] {
  if (!replaced?.hash) return [];
  const prior = Array.isArray(replaced.lineage)
    ? replaced.lineage.filter((entry): entry is string => typeof entry === "string")
    : [];
  return [replaced.hash.slice(0, PREFIX_LENGTH), ...prior].slice(0, LINEAGE_LENGTH);
}

/** Whether `entry` was built on `ancestor`, as far as its recorded lineage reaches. */
export function descendsFrom(entry: VersionedEntry, ancestor: string): boolean {
  if (entry.prev === ancestor) return true;
  return Array.isArray(entry.lineage) && entry.lineage.includes(ancestor.slice(0, PREFIX_LENGTH));
}
