import type { TAbstractFile } from "obsidian";

/** Where a file at its current path came from during the capture window. */
interface CapturedPath {
  /** Its path when the window opened; null for a file created during it. */
  origin: string | null;
  file: TAbstractFile;
  modified: boolean;
}

export type BootstrapReplayStep =
  /** The file that was at `path` when the window opened was deleted. */
  | { type: "delete"; path: string }
  /** The file that was at `from` now lives at `file.path`. */
  | { type: "rename"; from: string; file: TAbstractFile }
  /** A file created during the window. */
  | { type: "create"; file: TAbstractFile }
  /** A file edited in place. */
  | { type: "modify"; file: TAbstractFile };

/**
 * The net effect of the vault events captured before the initial sync.
 *
 * Replaying the raw events one by one lost user intent: an event was judged
 * stale by per-path versions that the replay's own handlers (and the plugin's
 * own re-materializing writes) kept bumping, so a delete could be skipped as
 * superseded, and a rename followed by an edit, or by a second rename, lost
 * track of the file's original path. Folding the events into one entry per
 * current path keeps chains and re-creations straight, and tells the startup
 * pass which paths it must not restore.
 */
export class BootstrapEventLog {
  /** Current path -> where its file came from. Untouched paths are absent. */
  private readonly current = new Map<string, CapturedPath>();
  /** Original path -> current path, for files moved away. */
  private readonly movedFrom = new Map<string, string>();
  /** Original paths whose file was deleted. */
  private readonly deleted = new Set<string>();

  get isEmpty(): boolean {
    return this.current.size === 0 && this.deleted.size === 0;
  }

  private originOf(path: string): string | null {
    const entry = this.current.get(path);
    return entry ? entry.origin : path;
  }

  create(path: string, file: TAbstractFile): void {
    // A file created where one was deleted replaces it in place.
    const replaces = this.deleted.delete(path);
    this.current.set(path, { origin: replaces ? path : null, file, modified: replaces });
  }

  modify(path: string, file: TAbstractFile): void {
    const entry = this.current.get(path);
    if (entry) {
      entry.modified = true;
      entry.file = file;
    } else {
      this.current.set(path, { origin: path, file, modified: true });
    }
  }

  delete(path: string): void {
    const origin = this.originOf(path);
    this.current.delete(path);
    // A file created during the window leaves nothing behind to replay.
    if (origin === null) return;
    this.movedFrom.delete(origin);
    this.deleted.add(origin);
  }

  rename(oldPath: string, newPath: string, file: TAbstractFile): void {
    const entry = this.current.get(oldPath);
    const origin = entry ? entry.origin : oldPath;
    this.current.delete(oldPath);
    this.current.set(newPath, { origin, file, modified: entry?.modified ?? false });
    if (origin === null) return;
    if (origin === newPath) this.movedFrom.delete(origin);
    else this.movedFrom.set(origin, newPath);
  }

  /**
   * The file at `path` when the window opened has since been deleted or
   * moved away, and nothing new occupies the path: restoring it from the
   * index now would undo what the user did.
   */
  removed(path: string): boolean {
    return !this.current.has(path) && (this.deleted.has(path) || this.movedFrom.has(path));
  }

  /** Where the file now at `path` was renamed from, if it moved there. */
  renamedFrom(path: string): string | null {
    const entry = this.current.get(path);
    return entry?.origin && entry.origin !== path ? entry.origin : null;
  }

  /**
   * Steps that reproduce the window's net effect: deletes first, then renames
   * ordered so each target is vacated before a file moves onto it, then new
   * files, then in-place edits.
   */
  plan(): BootstrapReplayStep[] {
    const steps: BootstrapReplayStep[] = [];
    for (const path of this.deleted) steps.push({ type: "delete", path });

    let pending = [...this.current]
      .filter(([path, entry]) => entry.origin !== null && entry.origin !== path)
      .map(([path, entry]) => ({ from: entry.origin as string, to: path, file: entry.file }));
    const inPlace: BootstrapReplayStep[] = [];
    while (pending.length > 0) {
      const vacating = new Set(pending.map((rename) => rename.from));
      const ready = pending.filter((rename) => !vacating.has(rename.to));
      if (ready.length === 0) {
        // Files swapped paths: no order frees a target first. Apply each
        // file's new content where it now is instead of moving identities.
        for (const rename of pending) inPlace.push({ type: "modify", file: rename.file });
        break;
      }
      for (const rename of ready) {
        steps.push({ type: "rename", from: rename.from, file: rename.file });
      }
      pending = pending.filter((rename) => !ready.includes(rename));
    }

    for (const [path, entry] of this.current) {
      if (entry.origin === null) {
        steps.push({ type: "create", file: entry.file });
      } else if (entry.origin === path && entry.modified) {
        steps.push({ type: "modify", file: entry.file });
      }
    }
    return [...steps, ...inPlace];
  }
}
