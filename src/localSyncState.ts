import * as Y from "yjs";
import { IndexeddbPersistence } from "y-indexeddb";

export type MaterializedKind = "text" | "canvas" | "base" | "binary" | "config";

export interface LocalPathState {
  kind: MaterializedKind;
  /**
   * Server identity this device last reconciled with: document guid for text
   * and structured files, blob hash for binaries. `null` means the local file
   * predates any accepted remote identity; an omitted value is legacy state.
   */
  identity?: string | null;
  /** SHA-256 of content last durably acknowledged by the server. */
  fingerprint?: string;
  /** Candidate content exists locally but its index identity is not committed. */
  candidate?: boolean;
  /** SHA-256 of the local content whose bootstrap decision is still pending. */
  candidateFingerprint?: string;
  /**
   * Epoch-scoped fingerprint (`storedContentFingerprint` in SyncedDoc) of the
   * file content last known to be on disk while the document's (`identity`)
   * local store durably contained it. A disk file that still matches it was
   * not edited locally since, however far the document has moved on.
   */
  diskFingerprint?: string;
  /**
   * Blob identity (hash) that this device's last publish for the path
   * replaced. A remote entry still at exactly that version, not built on the
   * published one, means the publish never reached the shared index.
   */
  replacedIdentity?: string | null;
  /**
   * Set while `identity` is a version this device published: the version it
   * replaced (null for a new path). Agreeing with the shared index keeps it,
   * so a later remote version is known to race this publish only when it
   * does not descend from it. Absent when another device published `identity`.
   */
  publishedFrom?: string | null;
}

/** Keep `diskFingerprint` only while it describes the same identity. */
function carriedDiskFingerprint(
  current: LocalPathState | null,
  identity: string | null,
): { diskFingerprint?: string } {
  return current?.diskFingerprint && current.identity === identity
    ? { diskFingerprint: current.diskFingerprint }
    : {};
}

/** Keep `publishedFrom` only while the identity is still this device's publish. */
function carriedPublishedFrom(
  current: LocalPathState | null,
  identity: string | null,
): { publishedFrom?: string | null } {
  return current?.publishedFrom !== undefined && current.identity === identity
    ? { publishedFrom: current.publishedFrom }
    : {};
}

/**
 * Device-local record of paths and content identities successfully reconciled
 * on disk.
 *
 * Identities provide three-way merge baselines and protect local candidates.
 * A missing path at startup is deliberately not treated as a deletion: the
 * file may be temporarily unavailable or evicted by an external storage
 * provider. Only live vault delete events publish shared deletions.
 */
export class LocalSyncState {
  private readonly doc = new Y.Doc();
  private readonly paths = this.doc.getMap<MaterializedKind | LocalPathState>("materialized");
  private readonly meta = this.doc.getMap<number>("meta");
  private readonly persistence: IndexeddbPersistence;
  readonly whenSynced: Promise<void>;

  constructor(scope: string) {
    this.persistence = new IndexeddbPersistence(`realtime:local-state:${scope}`, this.doc);
    this.whenSynced = this.persistence.whenSynced.then(
      () => undefined,
      (error) => {
        console.error("[Realtime] local sync state failed to load", error);
      },
    );
  }

  /**
   * Record the vault index epoch this state agrees with. An index whose epoch
   * went down was restored from a server backup and holds an older timeline.
   * Every recorded identity is then forgotten, so local files rejoin as
   * candidates and a path missing from that index is not taken as a deletion.
   * Returns whether the state was reset.
   */
  adoptIndexEpoch(epoch: number): boolean {
    const recorded = this.meta.get("indexEpoch");
    const rewound = typeof recorded === "number" && recorded > epoch;
    this.doc.transact(() => {
      if (rewound) this.paths.clear();
      this.meta.set("indexEpoch", epoch);
    });
    return rewound;
  }

  has(path: string): boolean {
    return this.paths.has(path);
  }

  entries(): Array<[string, LocalPathState]> {
    return [...this.paths.entries()].map(([path, value]) => [
      path,
      typeof value === "string" ? { kind: value } : value,
    ]);
  }

  get(path: string): LocalPathState | null {
    const value = this.paths.get(path);
    if (!value) return null;
    return typeof value === "string" ? { kind: value } : value;
  }

  mark(path: string, kind: MaterializedKind): void {
    const current = this.get(path);
    if (current?.kind === kind) return;
    this.paths.set(path, current ? { ...current, kind } : { kind });
  }

  /** Record a local file that has not yet committed a shared index identity. */
  beginCandidate(
    path: string,
    kind: MaterializedKind,
    identity: string | null,
    candidateFingerprint?: string,
  ): void {
    const current = this.get(path);
    const pendingFingerprint = candidateFingerprint ?? current?.candidateFingerprint;
    if (
      current?.kind === kind &&
      current.identity === identity &&
      current.candidate === true &&
      current.candidateFingerprint === pendingFingerprint
    ) {
      return;
    }
    this.paths.set(path, {
      kind,
      identity,
      ...(current?.fingerprint ? { fingerprint: current.fingerprint } : {}),
      candidate: true,
      ...(pendingFingerprint ? { candidateFingerprint: pendingFingerprint } : {}),
      ...carriedDiskFingerprint(current, identity),
    });
  }

  /**
   * Upgrade pre-bootstrap string records using the index snapshot loaded from
   * IndexedDB before any remote merge. Never fills a genuinely absent record.
   */
  migrateLegacyIdentity(path: string, kind: MaterializedKind, identity: string): void {
    const current = this.get(path);
    if (!current || current.identity !== undefined) return;
    this.paths.set(path, { ...current, kind, identity, candidate: false });
  }

  /** Mark an index identity committed while retaining an acknowledged hash. */
  commit(path: string, kind: MaterializedKind, identity: string): void {
    const current = this.get(path);
    const fingerprint = current?.identity === identity ? current.fingerprint : undefined;
    this.paths.set(path, {
      kind,
      identity,
      ...(fingerprint ? { fingerprint } : {}),
      candidate: current?.candidate ?? false,
      ...(current?.candidateFingerprint
        ? { candidateFingerprint: current.candidateFingerprint }
        : {}),
      ...carriedDiskFingerprint(current, identity),
    });
  }

  /** Record content after the provider reports that no local updates remain. */
  markSynced(
    path: string,
    kind: MaterializedKind,
    identity: string,
    fingerprint: string,
    reconciled = false,
  ): void {
    const current = this.get(path);
    const candidate =
      current?.candidate === true && !reconciled && current.candidateFingerprint !== fingerprint;
    const next: LocalPathState = {
      kind,
      identity,
      fingerprint,
      candidate,
      ...(candidate && current?.candidateFingerprint
        ? { candidateFingerprint: current.candidateFingerprint }
        : {}),
      ...carriedDiskFingerprint(current, identity),
      ...carriedPublishedFrom(current, identity),
    };
    // Unchanged reconciles run on every poll; an identical write would still
    // emit a Yjs update and an IndexedDB write.
    if (
      current &&
      (Object.keys(current) as Array<keyof LocalPathState>).every(
        (key) => current[key] === next[key],
      ) &&
      (Object.keys(next) as Array<keyof LocalPathState>).every((key) => current[key] === next[key])
    )
      return;
    this.paths.set(path, next);
  }

  /**
   * Record content this device just published as `identity` in place of
   * `replacedIdentity` (null for a new path).
   */
  markPublished(
    path: string,
    kind: MaterializedKind,
    identity: string,
    replacedIdentity: string | null,
  ): void {
    const current = this.get(path);
    this.paths.set(path, {
      kind,
      identity,
      fingerprint: identity,
      candidate: false,
      replacedIdentity,
      publishedFrom: replacedIdentity,
      ...carriedDiskFingerprint(current, identity),
    });
  }

  /** Record disk content the document for `identity` durably contains. */
  markDisk(path: string, kind: MaterializedKind, identity: string, diskFingerprint: string): void {
    const current = this.get(path);
    if (!current || current.identity !== identity || current.diskFingerprint === diskFingerprint) {
      return;
    }
    this.paths.set(path, { ...current, kind, diskFingerprint });
  }

  diskFingerprint(path: string, identity: string): string | null {
    const state = this.get(path);
    return state?.identity === identity ? (state.diskFingerprint ?? null) : null;
  }

  candidateIdentity(path: string, kind: MaterializedKind): string | null {
    const state = this.get(path);
    if (state?.kind !== kind || !state.candidate || !state.identity) return null;
    return state.identity;
  }

  hasIdentityConflict(path: string, identity: string): boolean {
    const state = this.get(path);
    return state?.identity !== undefined && state.identity !== identity;
  }

  acknowledgedFingerprint(path: string, identity: string): string | null {
    const state = this.get(path);
    if (state?.identity !== identity || state.candidate) return null;
    return state.fingerprint ?? null;
  }

  remove(path: string): void {
    this.paths.delete(path);
  }

  move(from: string, to: string, kind: MaterializedKind): void {
    const current = this.get(from);
    this.paths.delete(from);
    this.paths.set(to, current ? { ...current, kind } : { kind, identity: null, candidate: true });
  }

  destroy(): void {
    this.persistence.destroy();
    this.doc.destroy();
  }
}
