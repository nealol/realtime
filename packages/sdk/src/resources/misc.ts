import type { Http } from "../http";
import { encodePath } from "../http";
import type {
  GcBlobsResult,
  GitBackupConfig,
  PluginDbChangeRow,
  PluginDbCursor,
  PluginDbEncodedVal,
  PluginDbExecuteResult,
  PluginDbQueryResult,
  PluginDbStatement,
  PutGitBackupBody,
  ReindexResponse,
  SearchHit,
  StorageUsage,
  TagCount,
} from "../types";

export class SearchResource {
  constructor(
    private http: Http,
    private vaultId: string,
  ) {}

  search(q: string, opts: { limit?: number } = {}): Promise<SearchHit[]> {
    return this.http.request("GET", `/api/vaults/${this.vaultId}/search`, {
      query: { q, limit: opts.limit },
    });
  }

  tags(): Promise<TagCount[]> {
    return this.http.request("GET", `/api/vaults/${this.vaultId}/tags`);
  }

  backlinks(path: string): Promise<SearchHit[]> {
    return this.http.request("GET", `/api/vaults/${this.vaultId}/backlinks/${encodePath(path)}`);
  }

  reindex(): Promise<ReindexResponse> {
    return this.http.request("POST", `/api/vaults/${this.vaultId}/reindex`);
  }
}

export class StorageResource {
  constructor(
    private http: Http,
    private vaultId: string,
  ) {}

  usage(): Promise<StorageUsage> {
    return this.http.request("GET", `/api/vaults/${this.vaultId}/storage`);
  }

  /**
   * Delete orphaned blobs at least `minBytes` large (default 0) that were
   * written at least `minAgeSeconds` ago (server default: one hour, so
   * uploads whose index entry is still being published are kept).
   */
  gcBlobs(opts: { minBytes?: number; minAgeSeconds?: number } = {}): Promise<GcBlobsResult> {
    return this.http.request("POST", `/api/vaults/${this.vaultId}/storage/gc-blobs`, {
      body: { minBytes: opts.minBytes, minAgeSeconds: opts.minAgeSeconds },
    });
  }
}

export class BackupResource {
  constructor(
    private http: Http,
    private vaultId: string,
  ) {}

  get(): Promise<GitBackupConfig> {
    return this.http.request("GET", `/api/vaults/${this.vaultId}/backup`);
  }

  put(config: PutGitBackupBody): Promise<GitBackupConfig> {
    return this.http.request("PUT", `/api/vaults/${this.vaultId}/backup`, { body: config });
  }

  async delete(): Promise<void> {
    await this.http.request("DELETE", `/api/vaults/${this.vaultId}/backup`);
  }

  /** Test the configured remote without pushing. */
  test(): Promise<unknown> {
    return this.http.request("POST", `/api/vaults/${this.vaultId}/backup/test`);
  }
}

/** Replication endpoints for a synced plugin database (cr-sqlite). */
export class PluginDbResource {
  constructor(
    private http: Http,
    private vaultId: string,
    private pluginId: string,
    private name: string,
  ) {}

  private get base(): string {
    return `/api/vaults/${this.vaultId}/plugin-dbs/${this.pluginId}/${this.name}`;
  }

  /** Bootstrap changes since a cursor (`{siteHex: dbVersion}`). */
  async changes(since?: PluginDbCursor): Promise<PluginDbChangeRow[]> {
    const res = await this.http.request<{ changes: PluginDbChangeRow[] }>(
      "GET",
      `${this.base}/changes`,
      {
        query: { since: since ? JSON.stringify(since) : undefined },
      },
    );
    return res.changes;
  }

  /** Run a read-only SELECT against the server replica. */
  query(
    sql: string,
    opts: { params?: PluginDbEncodedVal[]; limit?: number } = {},
  ): Promise<PluginDbQueryResult> {
    return this.http.request("POST", `${this.base}/query`, {
      body: { sql, params: opts.params ?? [], limit: opts.limit },
    });
  }

  /** Run write statements in one transaction; changes replicate to all devices. */
  execute(statements: PluginDbStatement[]): Promise<PluginDbExecuteResult> {
    return this.http.request("POST", `${this.base}/execute`, { body: { statements } });
  }

  /** Mark a write: replicate pending doc changes and produce a git commit. */
  async touch(): Promise<void> {
    await this.http.request("POST", `${this.base}/touch`);
  }

  /** Purge the database server-side (irreversible). */
  async delete(): Promise<void> {
    await this.http.request("DELETE", this.base);
  }
}
