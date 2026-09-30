import * as Y from "yjs";
import { TFile, Notice, normalizePath } from "obsidian";
import type RealtimePlugin from "./main";
import { applyTextToYText } from "./diff";
import { dbg, snip } from "./debug";
import { ensureParentFolder, getFileByPath, isOpenInEditableMarkdown } from "./vaultHelpers";
import { openTextConflictModal } from "./TextConflictModal";
import { SyncedDoc, type DocumentBootstrapOptions } from "./SyncedDoc";
import { preserveTextConflict } from "./conflictRecovery";
import { mergeText, mergeWithoutBaseline } from "./textMerge";
import { sha256Text } from "./hash";
import { getDocumentEpoch } from "./documentEpoch";

/**
 * Origin tag used on Yjs transactions that originate from this Document writing
 * disk content into the shared text, so our own ytext observer can ignore them.
 */
const DISK_ORIGIN = Symbol("realtime-disk");
const DISK_WRITE_RETRY_MS = 2_000;

/**
 * A single collaboratively-edited Markdown file. Owns its own Y.Doc and
 * Realtime provider, and keeps the shared `contents` Y.Text in sync with the
 * file on disk when the file is not actively open in an editor.
 *
 * Offline durability: the Y.Doc is mirrored into IndexedDB via
 * {@link IndexeddbPersistence}. On startup we load that persisted state
 * (the "baseline" — the last state this device saw) *before* connecting, then
 * fold any local on-disk edits into the CRDT, then connect. This guarantees
 * offline edits become Yjs operations that merge with remote changes instead
 * of being clobbered.
 */
export class Document extends SyncedDoc {
  readonly ytext: Y.Text;

  /** Number of CodeMirror editors currently bound to this document. */
  private boundEditors = 0;
  /** Text of the disk write in progress, so its own modify event is not an edit. */
  private writingTextToDisk: string | null = null;
  /**
   * Content this device last wrote to, or read from, the file. A disk read
   * that still returns it (for example the late event of an older write)
   * is not an edit; anything else is someone else's change.
   */
  private diskKnownContent: string | null = null;
  /** Disk writes run one at a time; a request during one coalesces behind it. */
  private writeRunning: Promise<boolean> | null = null;
  private writeQueued: Promise<boolean> | null = null;
  /** Disk content captured at startup, before remote sync (for conflict checks). */
  private diskAtStartup: string | null = null;
  /** Locally persisted Y.Text content before the first remote sync. */
  private baselineAtStartup = "";
  /** The startup disk read failed and must succeed before reconciling. */
  private startupDiskReadPending = false;
  /** Whether the local disk diverged from the baseline at startup. */
  private localChangedAtStartup = false;
  /** Guards the one-time startup merge so reconnects don't re-run it. */
  private startupReconciled = false;
  /** True only after startup content is durably present on this device. */
  private startupReady = false;
  private startupReconciling = false;
  private forceBootstrapConflict: boolean;
  private readonly staleLocalFingerprint: string | null;
  private readonly retiredBaselineText: string | null;
  /** Suppress write-through while IndexedDB is replaying the startup baseline. */
  private startupBaselineCaptured = false;

  private ytextObserver: () => void;
  private writeTimer: number | null = null;

  constructor(
    plugin: RealtimePlugin,
    path: string,
    guid: string,
    serverDocId: string,
    isCreator: boolean,
    opts: DocumentBootstrapOptions = {},
  ) {
    super(plugin, path, guid, serverDocId, isCreator, opts);
    this.forceBootstrapConflict = opts.forceBootstrapConflict ?? false;
    this.staleLocalFingerprint = opts.staleLocalFingerprint ?? null;
    this.retiredBaselineText = opts.retiredBaseline ?? null;
    this.ytext = this.ydoc.getText("contents");

    // ytext changes (local edits from other peers, or our own editor) flow to
    // disk only while no editor is bound — otherwise Obsidian persists the file.
    this.ytextObserver = this.onYTextChanged.bind(this);
    this.ytext.observe(this.ytextObserver);
  }

  get content(): string {
    return this.ytext.toString();
  }

  protected serializeRecoveryContent(): string {
    return this.content;
  }

  bindEditor(): void {
    this.boundEditors++;
    dbg("bindEditor", this.path, "count", this.boundEditors);
  }

  unbindEditor(): void {
    this.boundEditors = Math.max(0, this.boundEditors - 1);
    dbg("unbindEditor", this.path, "count", this.boundEditors);
    if (this.boundEditors !== 0 || this.destroyed) return;
    // Obsidian destroys and immediately recreates the editor's view plugins on
    // mode switches (Live Preview ↔ Source), splits, and re-layout — all while
    // the file stays open — transiently dropping the count to zero. Defer a tick
    // so a rebind cancels the flush, and never write while the file is still
    // open anywhere: a vault.modify on an open file surfaces to Obsidian as an
    // external change, which it then 3-way merges into its editor buffer,
    // duplicating the just-typed text (and that merge gets re-sent to peers).
    window.setTimeout(() => {
      if (this.destroyed || this.boundEditors > 0 || this.isOpenInEditableMarkdown()) return;
      void this.writeToDisk();
    }, 0);
  }

  get hasBoundEditor(): boolean {
    return this.boundEditors > 0;
  }

  /**
   * An editor kept its own overlapping edits over `shared`: preserve that
   * shared version beside the note so the other side is not lost.
   */
  async preserveSharedConflict(shared: string): Promise<void> {
    if (this.destroyed) return;
    try {
      const preservedPath = await preserveTextConflict(this.plugin, this.path, shared, "remote");
      new Notice(
        `Realtime: "${this.path}" was edited here while other changes were arriving; ` +
          `kept your version and preserved the other as "${preservedPath}".`,
      );
    } catch (error) {
      console.error(`[Realtime] failed to preserve the shared version of ${this.path}`, error);
    }
  }

  /**
   * Startup sequence: load the persisted baseline, read local disk, then connect.
   * Local disk edits are deliberately not folded into Y.Text until the first
   * remote sync tells us whether the remote also changed from the baseline.
   */
  protected async afterPersistenceSynced(): Promise<void> {
    try {
      // A new epoch starts from an empty store; the content it replaced is
      // then the baseline local and remote changes are measured from.
      this.baselineAtStartup =
        !this.loadedStoredState && this.retiredBaselineText !== null
          ? this.retiredBaselineText
          : this.content;
      await this.captureStartupDisk();
    } catch (e) {
      // A failed read is not a missing file. Reconciling as if it were would
      // overwrite the local note with the (possibly empty) Y.Text; retry the
      // read before reconciling instead.
      console.warn(`[Realtime] startup read failed for ${this.path}; will retry`, e);
      this.startupDiskReadPending = true;
    } finally {
      // Even if the disk read fails, remote Y.Text updates must still be allowed
      // to materialize locally after the provider syncs.
      this.startupBaselineCaptured = true;
    }
  }

  private async captureStartupDisk(): Promise<void> {
    const disk = await this.readFromDisk();
    let stale = false;
    let unchangedSinceSync = false;
    if (disk !== null) {
      const fingerprint = await sha256Text(disk);
      stale = this.staleLocalFingerprint !== null && fingerprint === this.staleLocalFingerprint;
      // The file still holds content this document's store contains: the
      // disk merely lags the document (a pending write was interrupted by a
      // restart, or an editor save was), so it is not a local edit.
      unchangedSinceSync = await this.storeContainsDiskContent(disk);
    }
    if (this.destroyed) return;
    this.diskAtStartup = disk;
    this.diskKnownContent = disk;
    // An untouched copy of a note deleted remotely (and since re-created at
    // this path) is not a local change: let the new document replace it.
    if (stale) this.forceBootstrapConflict = false;
    this.localChangedAtStartup =
      disk !== null && !stale && !unchangedSinceSync && disk !== this.baselineAtStartup;
    this.startupDiskReadPending = false;
  }

  /**
   * Runs once, after the first successful server sync. Since local startup disk
   * edits have not yet been applied, the current Y.Text is the pre-merge remote
   * version. We compare baseline/local/remote and publish exactly one canonical
   * version.
   *  - pure remote update            -> write the merged text to disk;
   *  - local-only fast-forward       -> apply local disk to Y.Text;
   *  - both sides changed (conflict) -> prompt for local vs remote, then apply
   *                                     the chosen text as canonical.
   */
  protected async finishStartupReconcile(): Promise<void> {
    if (this.startupReady || this.startupReconciling || this.destroyed) return;
    this.startupReconciling = true;
    let completed = false;

    try {
      if (!this.startupReconciled) {
        // Throws while the file stays unreadable; the catch below retries.
        if (this.startupDiskReadPending) await this.captureStartupDisk();
        if (this.destroyed) return;
        const remote = this.content;
        const baseline = this.baselineAtStartup;
        const localDisk = this.diskAtStartup;
        const remoteChanged = remote !== baseline;
        const creatorHasLocalAgainstEmptyRemote =
          this.isCreator && localDisk !== null && localDisk.length > 0 && remote.length === 0;

        // Creator docs are the only side allowed to seed a brand-new server doc.
        // If their first remote is empty, treat it as unseeded instead of asking
        // the user to resolve a blank-vs-local conflict.
        const isConflict =
          localDisk !== null &&
          (this.localChangedAtStartup || this.forceBootstrapConflict) &&
          remote !== localDisk &&
          (remoteChanged || this.forceBootstrapConflict) &&
          !creatorHasLocalAgainstEmptyRemote;

        const sameDevicePrefixFastForward =
          isConflict &&
          !this.forceBootstrapConflict &&
          getDocumentEpoch(this.plugin, this.serverDocId) === 0 &&
          baseline.length === 0 &&
          localDisk !== null &&
          localDisk.length > remote.length &&
          localDisk.startsWith(remote);

        if (sameDevicePrefixFastForward) {
          this.applyText(localDisk);
        } else if (isConflict) {
          // Without a shared baseline (an unrelated local file, or a note both
          // devices created independently — typically by a plugin template),
          // a diff3 sees two overlapping inserts. Accept whichever side already
          // contains the other before asking the user.
          const allowLocalSuperset = getDocumentEpoch(this.plugin, this.serverDocId) === 0;
          let merge = this.forceBootstrapConflict
            ? mergeWithoutBaseline(localDisk, remote, allowLocalSuperset)
            : mergeText(baseline, localDisk, remote);
          if (merge.kind === "conflict" && !this.forceBootstrapConflict && baseline.length === 0) {
            merge = mergeWithoutBaseline(localDisk, remote, allowLocalSuperset);
          }
          if (merge.kind === "merged") {
            this.applyText(merge.content);
          } else {
            await this.resolveStartupConflict(localDisk, remote);
            if (this.destroyed) return;
          }
        } else if (
          this.localChangedAtStartup &&
          localDisk !== null &&
          (!remoteChanged || creatorHasLocalAgainstEmptyRemote)
        ) {
          this.applyText(localDisk);
        }
        this.startupReconciled = true;
      }

      // The editor (if open) receives the merged text via the ytext observer;
      // writing through vault.modify would make Obsidian merge an external change.
      let materialized = this.getFile() !== null;
      if (!this.hasBoundEditor && !this.isOpenInEditableMarkdown()) {
        materialized = await this.writeToDisk();
      } else if (materialized) {
        this.plugin.vaultSync?.noteMaterialized(this.path, "text", this.guid);
      }
      if (!materialized || this.destroyed) return;

      this.startupReady = true;
      if (!this.provider.hasLocalChanges) await this.recordAcknowledgedContent(true);
      completed = true;
    } catch (e) {
      console.error(`[Realtime] startup reconcile failed for ${this.path}`, e);
    } finally {
      this.startupReconciling = false;
      if (completed) {
        this.resolveWhenReady();
      } else if (!this.destroyed) {
        window.setTimeout(() => void this.finishStartupReconcile(), DISK_WRITE_RETRY_MS);
      }
    }
  }

  protected async afterChangesSynced(): Promise<void> {
    if (!this.startupReady || this.destroyed) return;
    await this.recordAcknowledgedContent(true);
  }

  private async recordAcknowledgedContent(reconciled = false): Promise<void> {
    const content = this.content;
    const fingerprint = await sha256Text(content);
    if (
      this.destroyed ||
      this.provider.hasLocalChanges ||
      this.content !== content ||
      !this.startupReady
    ) {
      return;
    }
    this.plugin.vaultSync?.noteContentAcknowledged(
      this.path,
      "text",
      this.guid,
      fingerprint,
      reconciled,
    );
  }

  private applyText(text: string): void {
    if (this.destroyed) return;
    this.ydoc.transact(() => {
      applyTextToYText(this.ytext, text);
    }, DISK_ORIGIN);
  }

  private async resolveStartupConflict(initialLocal: string, initialRemote: string): Promise<void> {
    let local = initialLocal;
    let remote = initialRemote;
    while (!this.destroyed) {
      const choice = await openTextConflictModal(this.plugin, {
        path: this.path,
        localContent: local,
        remoteContent: remote,
      });
      if (this.destroyed) return;

      const latestLocal = await this.readFromDisk();
      if (this.destroyed) return;
      const latestRemote = this.content;
      if (latestLocal === null) return;
      if (latestLocal !== local || latestRemote !== remote) {
        local = latestLocal;
        remote = latestRemote;
        continue;
      }

      const preservedPath = await preserveTextConflict(
        this.plugin,
        this.path,
        choice === "local" ? remote : local,
        choice === "local" ? "remote" : "local",
      );
      if (this.destroyed) return;

      const localAfterCopy = await this.readFromDisk();
      if (this.destroyed) return;
      if (localAfterCopy === null) return;
      if (localAfterCopy !== local || (choice === "local" && this.content !== remote)) {
        local = localAfterCopy;
        remote = this.content;
        continue;
      }

      if (choice === "local") this.applyText(local);
      const kept = choice === "local" ? "your local" : "the remote";
      new Notice(
        `Realtime: kept ${kept} version of "${this.path}"; preserved the other version as "${preservedPath}".`,
      );
      return;
    }
  }

  private onYTextChanged(): void {
    if (this.destroyed) return;
    if (!this.startupBaselineCaptured || !this.startupReady) return;
    // Note text-sync activity so the binary upload queue can defer large
    // transfers while notes are actively syncing.
    this.plugin.vaultSync?.noteTextActivity();
    // While a note is open, Obsidian owns its editor buffer and persistence;
    // writing through vault.modify would appear as an external file change.
    if (this.hasBoundEditor || this.isOpenInEditableMarkdown()) return;
    this.scheduleWriteToDisk();
  }

  private scheduleWriteToDisk(delayMs = 100): void {
    if (this.writeTimer !== null) {
      window.clearTimeout(this.writeTimer);
    }
    this.writeTimer = window.setTimeout(() => {
      this.writeTimer = null;
      // Re-check at fire time, not just when scheduled: the note may have been
      // opened during the debounce window. Writing through vault.modify onto a
      // now-open file makes Obsidian report an external change and 3-way-merge
      // it into the editor buffer, duplicating text (which is then re-sent).
      if (
        this.destroyed ||
        !this.startupBaselineCaptured ||
        !this.startupReady ||
        this.hasBoundEditor ||
        this.isOpenInEditableMarkdown()
      )
        return;
      void this.writeToDisk();
    }, delayMs);
  }

  /**
   * Called by VaultSync when the file changed on disk. Our own writes (and
   * late events for older ones) are recognised by content, not timing; any
   * other content is a real edit, merged with changes the document gained
   * since the file last matched it.
   */
  async onDiskChanged(): Promise<void> {
    if (this.destroyed) return;
    const disk = await this.readFromDisk();
    if (this.destroyed || disk === null) return;
    if (this.hasBoundEditor) {
      // Obsidian owns the file while an editor is bound, and LiveEdit keeps
      // that editor in step with the shared text: its saves are not edits.
      if (disk === this.content) this.noteDiskContent(disk);
      else this.diskKnownContent = disk;
      return;
    }
    if (disk === this.diskKnownContent || disk === this.writingTextToDisk) return;
    if (disk === this.content) {
      this.noteDiskContent(disk);
      return;
    }
    await this.foldDiskEdit(disk);
  }

  /**
   * Fold a disk edit into the shared text. The document may have moved on
   * since the file last matched it (typically remote edits whose disk write
   * was still pending), so merge against that last known disk content rather
   * than letting the file overwrite them.
   */
  private async foldDiskEdit(disk: string): Promise<void> {
    this.plugin.vaultSync?.noteTextActivity();
    const base = this.diskKnownContent;
    const shared = this.content;
    const merge = base !== null ? mergeText(base, disk, shared) : null;
    dbg(
      "onDiskChanged FOLD disk->ytext",
      this.path,
      "disk",
      snip(disk),
      "ytext",
      snip(shared),
      "merge",
      merge?.kind ?? "none",
    );
    if (merge?.kind === "conflict" && shared !== base) {
      const preservedPath = await preserveTextConflict(this.plugin, this.path, shared, "remote");
      if (this.destroyed) return;
      new Notice(
        `Realtime: "${this.path}" was edited on disk while newer changes were arriving; ` +
          `kept the disk version and preserved the other as "${preservedPath}".`,
      );
    }
    const latest = await this.readFromDisk();
    if (this.destroyed || latest !== disk) return;
    this.applyText(merge?.kind === "merged" ? merge.content : disk);
    this.noteDiskContent(disk);
  }

  /** The file is known to hold `text`, which this document now contains. */
  private noteDiskContent(text: string): void {
    this.diskKnownContent = text;
    // Read-only documents do not persist local folds, so a restart could not
    // rely on the document containing this content.
    if (this.provider.clientToken?.authorization === "read-only") return;
    void this.recordStoredDiskContent("text", text);
  }

  private getFile(): TFile | null {
    return getFileByPath(this.plugin.app, this.path);
  }

  private isOpenInEditableMarkdown(): boolean {
    return isOpenInEditableMarkdown(this.plugin.app, this.path);
  }

  protected canHibernateLocally(): boolean {
    return (
      !this.hasBoundEditor &&
      !this.isOpenInEditableMarkdown() &&
      this.writeRunning === null &&
      this.writeQueued === null &&
      this.writeTimer === null
    );
  }

  private async readFromDisk(): Promise<string | null> {
    const file = this.getFile();
    if (!file) return null;
    return await this.plugin.app.vault.read(file);
  }

  /**
   * Write the current shared text to disk. Writes never overlap: a request
   * made while one is running waits for it and then writes whatever the text
   * is by then, so an older write's content can never land after a newer one.
   */
  private writeToDisk(): Promise<boolean> {
    if (this.writeQueued) return this.writeQueued;
    const running = this.writeRunning;
    if (!running) return this.startDiskWrite();
    const queued = running.then(() => {
      this.writeQueued = null;
      return this.startDiskWrite();
    });
    this.writeQueued = queued;
    return queued;
  }

  private startDiskWrite(): Promise<boolean> {
    const running: Promise<boolean> = this.writeCurrentContent().finally(() => {
      if (this.writeRunning === running) this.writeRunning = null;
    });
    this.writeRunning = running;
    return running;
  }

  private async writeCurrentContent(): Promise<boolean> {
    if (this.destroyed) return false;
    const text = this.content;
    this.writingTextToDisk = text;
    try {
      const file = this.getFile();
      if (file) {
        // Never modify a file that is open in an editor — Obsidian owns its
        // buffer and persistence, and a vault.modify would surface as an
        // external change it 3-way-merges, duplicating text. This is the last
        // line of defence behind the callers' own open-state checks (which can
        // race an open that happens during an awaited read/schedule).
        if (this.hasBoundEditor || this.isOpenInEditableMarkdown()) {
          dbg(
            "writeToDisk SKIP (open/bound)",
            this.path,
            "bound",
            this.boundEditors,
            "open",
            this.isOpenInEditableMarkdown(),
          );
          this.plugin.vaultSync?.noteMaterialized(this.path, "text", this.guid);
          return true;
        }
        const current = await this.plugin.app.vault.read(file);
        // Re-check destroyed after the await: a doc replaced mid-write (rename,
        // guid change) must not clobber the file its successor now owns.
        if (this.destroyed) return false;
        if (current === text) {
          this.noteDiskContent(text);
          this.plugin.vaultSync?.noteMaterialized(this.path, "text", this.guid);
          if (this.startupReady && !this.provider.hasLocalChanges) {
            await this.recordAcknowledgedContent(true);
          }
          return true;
        }
        if (this.hasBoundEditor || this.isOpenInEditableMarkdown()) {
          dbg(
            "writeToDisk SKIP after read (open/bound)",
            this.path,
            "bound",
            this.boundEditors,
            "open",
            this.isOpenInEditableMarkdown(),
          );
          this.plugin.vaultSync?.noteMaterialized(this.path, "text", this.guid);
          return true;
        }
        // The shared text moved on during the read; the write it scheduled
        // will carry the newer text.
        if (this.content !== text) return false;
        if (this.diskKnownContent !== null && current !== this.diskKnownContent) {
          // Someone edited the file and its modify event has not been handled
          // yet: fold that edit in instead of overwriting it.
          await this.foldDiskEdit(current);
          return false;
        }
        dbg(
          "%cwriteToDisk MODIFY",
          "color:orange",
          this.path,
          snip(text),
          "bound",
          this.boundEditors,
          "open",
          this.isOpenInEditableMarkdown(),
        );
        const releaseWrite = this.plugin.vaultSync?.beginOwnWrite(file.path);
        try {
          await this.plugin.app.vault.modify(file, text);
        } finally {
          releaseWrite?.();
        }
      } else {
        // Remote-created file that does not exist locally yet.
        const path = normalizePath(this.path);
        // ...unless the user deleted or moved it away before the initial
        // sync finished: that is about to be published, not undone.
        if (this.plugin.vaultSync?.isPendingLocalRemoval(path)) return false;
        await ensureParentFolder(this.plugin.app, path);
        if (this.destroyed || this.content !== text) return false;
        const releaseWrite = this.plugin.vaultSync?.beginOwnWrite(path);
        try {
          await this.plugin.app.vault.create(path, text);
        } finally {
          releaseWrite?.();
        }
      }
      // A document replaced while the write was in flight must not record
      // state for the path its successor now owns.
      if (this.destroyed) return false;
      this.noteDiskContent(text);
      this.plugin.vaultSync?.noteMaterialized(this.path, "text", this.guid);
      if (this.startupReady && !this.provider.hasLocalChanges) {
        await this.recordAcknowledgedContent(true);
      }
      return true;
    } catch (e) {
      console.error(`[Realtime] writeToDisk failed for ${this.path}`, e);
      if (!this.destroyed) {
        // Whatever the failed write left on disk is ours, not a user edit.
        this.diskKnownContent = await this.readFromDisk().catch(() => this.diskKnownContent);
        this.scheduleWriteToDisk(DISK_WRITE_RETRY_MS);
      }
      return false;
    } finally {
      this.writingTextToDisk = null;
    }
  }

  protected destroySubclass(): void {
    if (this.writeTimer !== null) {
      window.clearTimeout(this.writeTimer);
      this.writeTimer = null;
    }
    this.ytext.unobserve(this.ytextObserver);
  }
}
