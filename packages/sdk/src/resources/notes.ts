import { ConflictError } from "../errors";
import type { Http } from "../http";
import { encodePath } from "../http";
import type {
  FrontmatterResponse,
  Note,
  NoteSummary,
  PatchFrontmatterBody,
  PatchNoteBody,
  PeriodicPeriod,
  PermalinkResponse,
  ReplaceNoteOptions,
} from "../types";

/** Reads `append` makes before giving up on a note that keeps changing. */
const APPEND_ATTEMPTS = 3;

/** Lowercase hex SHA-256 of a note's UTF-8 content, as `expectedContentHash` expects. */
export async function noteContentHash(content: string): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(content));
  return [...new Uint8Array(digest)].map((byte) => byte.toString(16).padStart(2, "0")).join("");
}

export class NotesResource {
  constructor(
    private http: Http,
    private vaultId: string,
  ) {}

  private note(path: string): string {
    return `/api/vaults/${this.vaultId}/notes/${encodePath(path)}`;
  }

  list(): Promise<NoteSummary[]> {
    return this.http.request("GET", `/api/vaults/${this.vaultId}/notes`);
  }

  read(path: string): Promise<Note> {
    return this.http.request("GET", this.note(path));
  }

  create(path: string, content = ""): Promise<Note> {
    return this.http.request("POST", `/api/vaults/${this.vaultId}/notes`, {
      body: { path, content },
    });
  }

  /**
   * Replace a note's full content. Pass `expectedContentHash` (see
   * {@link noteContentHash}) of the content this is based on to have the
   * server refuse the write if someone edited the note meanwhile.
   */
  replace(path: string, content: string, options: ReplaceNoteOptions = {}): Promise<Note> {
    const body: { content: string; expectedContentHash?: string } = { content };
    if (options.expectedContentHash) body.expectedContentHash = options.expectedContentHash;
    return this.http.request("PUT", this.note(path), { body });
  }

  patch(path: string, edit: PatchNoteBody): Promise<Note> {
    return this.http.request("PATCH", this.note(path), {
      body: { old: edit.old, new: edit.new, replaceAll: edit.replaceAll ?? false },
    });
  }

  /**
   * Convenience read-then-replace appending `text` on a fresh line. If the
   * note changes between the read and the write, the write is refused and
   * `append` re-reads rather than reverting that change, giving up with a
   * `ConflictError` after a few attempts on a note under heavy edits.
   */
  async append(path: string, text: string): Promise<Note> {
    for (let attempt = 1; ; attempt++) {
      const current = await this.read(path);
      const glue = current.content.length === 0 || current.content.endsWith("\n") ? "" : "\n";
      try {
        return await this.replace(path, `${current.content}${glue}${text}`, {
          expectedContentHash: await noteContentHash(current.content),
        });
      } catch (error) {
        const stale = error instanceof ConflictError && error.message === "stale";
        if (!stale || attempt >= APPEND_ATTEMPTS) throw error;
      }
    }
  }

  move(path: string, toPath: string): Promise<Note> {
    return this.http.request("POST", `/api/vaults/${this.vaultId}/note-moves/${encodePath(path)}`, {
      body: { toPath },
    });
  }

  async delete(path: string): Promise<void> {
    await this.http.request("DELETE", this.note(path));
  }

  permalink(path: string): Promise<PermalinkResponse> {
    return this.http.request(
      "POST",
      `/api/vaults/${this.vaultId}/note-permalinks/${encodePath(path)}`,
    );
  }
}

export class FrontmatterResource {
  constructor(
    private http: Http,
    private vaultId: string,
  ) {}

  parse(path: string): Promise<FrontmatterResponse> {
    return this.http.request(
      "GET",
      `/api/vaults/${this.vaultId}/note-frontmatter/${encodePath(path)}`,
    );
  }

  /** Set/unset frontmatter keys; returns the updated note. */
  patch(path: string, edit: PatchFrontmatterBody): Promise<Note> {
    return this.http.request(
      "PATCH",
      `/api/vaults/${this.vaultId}/note-frontmatter/${encodePath(path)}`,
      {
        body: { set: edit.set ?? {}, unset: edit.unset ?? [] },
      },
    );
  }
}

export class PeriodicNotesResource {
  constructor(
    private http: Http,
    private vaultId: string,
  ) {}

  /** Get or create the periodic note for `period` (today unless `date` given). */
  getOrCreate(
    period: PeriodicPeriod,
    opts: { date?: string; content?: string } = {},
  ): Promise<Note> {
    return this.http.request("POST", `/api/vaults/${this.vaultId}/periodic/${period}`, {
      body: { date: opts.date, content: opts.content ?? "" },
    });
  }

  append(period: PeriodicPeriod, text: string, opts: { date?: string } = {}): Promise<Note> {
    return this.http.request("POST", `/api/vaults/${this.vaultId}/periodic/${period}/append`, {
      body: { date: opts.date, text },
    });
  }
}
