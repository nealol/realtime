import type { Http } from "../http";
import { encodePath } from "../http";
import type {
  AttachmentSummary,
  CreateUploadLinkResponse,
  DeleteBlobResult,
  UploadAttachmentOptions,
  UploadAttachmentResponse,
} from "../types";
import { NotFoundError } from "../errors";

export class AttachmentsResource {
  constructor(
    private http: Http,
    private vaultId: string,
  ) {}

  private attachment(path: string): string {
    return `/api/vaults/${this.vaultId}/attachments/${encodePath(path)}`;
  }

  list(): Promise<AttachmentSummary[]> {
    return this.http.request("GET", `/api/vaults/${this.vaultId}/attachments`);
  }

  /** Download the attachment bytes. */
  async read(path: string): Promise<Uint8Array> {
    const res = await this.http.raw("GET", this.attachment(path));
    return new Uint8Array(await res.arrayBuffer());
  }

  /** True when the attachment exists (HEAD). */
  async exists(path: string): Promise<boolean> {
    try {
      await this.http.raw("HEAD", this.attachment(path));
      return true;
    } catch (e) {
      if (e instanceof NotFoundError) return false;
      throw e;
    }
  }

  /**
   * Upload `bytes` to `path`. Pass `ifMatch` (the hash you last saw there) or
   * `ifNoneMatch: "*"` to have the server refuse, instead of overwrite, an
   * attachment someone else changed or created meanwhile.
   */
  async upload(
    path: string,
    bytes: Uint8Array | ArrayBuffer,
    options: UploadAttachmentOptions = {},
  ): Promise<UploadAttachmentResponse> {
    const headers: Record<string, string> = { "Content-Type": "application/octet-stream" };
    if (options.ifMatch) headers["If-Match"] = `"${options.ifMatch}"`;
    if (options.ifNoneMatch) headers["If-None-Match"] = options.ifNoneMatch;
    const res = await this.http.raw("PUT", this.attachment(path), {
      body: bytes instanceof Uint8Array ? new Uint8Array(bytes) : bytes,
      headers,
    });
    return (await res.json()) as UploadAttachmentResponse;
  }

  /** Server-side fetch of `sourceUrl` into the vault at `path`. */
  uploadFromUrl(sourceUrl: string, path: string): Promise<UploadAttachmentResponse> {
    return this.http.request("POST", `/api/vaults/${this.vaultId}/attachments/from-url`, {
      body: { sourceUrl, path },
    });
  }

  /** Mint a public, expiring upload link (for third parties without a token). */
  createUploadLink(
    opts: { landingDir?: string; expiresInSeconds?: number } = {},
  ): Promise<CreateUploadLinkResponse> {
    return this.http.request("POST", `/api/vaults/${this.vaultId}/attachments/upload-link`, {
      body: { landingDir: opts.landingDir, expiresInSeconds: opts.expiresInSeconds },
    });
  }

  move(
    path: string,
    toPath: string,
    opts: { updateEmbeds?: boolean } = {},
  ): Promise<AttachmentSummary> {
    return this.http.request(
      "POST",
      `/api/vaults/${this.vaultId}/attachment-moves/${encodePath(path)}`,
      {
        body: { toPath, updateEmbeds: opts.updateEmbeds ?? false },
      },
    );
  }

  async delete(path: string): Promise<void> {
    await this.http.request("DELETE", this.attachment(path));
  }
}

/** Content-addressed blob store (sha256 hex keys). */
export class BlobsResource {
  constructor(
    private http: Http,
    private vaultId: string,
  ) {}

  private blob(hash: string): string {
    return `/api/vaults/${this.vaultId}/blobs/${hash}`;
  }

  async get(hash: string): Promise<Uint8Array> {
    const res = await this.http.raw("GET", this.blob(hash));
    return new Uint8Array(await res.arrayBuffer());
  }

  async exists(hash: string): Promise<boolean> {
    try {
      await this.http.raw("HEAD", this.blob(hash));
      return true;
    } catch (e) {
      if (e instanceof NotFoundError) return false;
      throw e;
    }
  }

  /** Upload bytes; the server verifies they hash to `hash` (sha256 hex). */
  async put(hash: string, bytes: Uint8Array | ArrayBuffer): Promise<void> {
    await this.http.raw("PUT", this.blob(hash), {
      body: bytes instanceof Uint8Array ? new Uint8Array(bytes) : bytes,
      headers: { "Content-Type": "application/octet-stream" },
    });
  }

  /** Reclaim a single orphaned blob; refuses blobs still referenced. */
  delete(hash: string): Promise<DeleteBlobResult> {
    return this.http.request("DELETE", this.blob(hash));
  }
}
