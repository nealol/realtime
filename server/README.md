# Realtime server

A small [axum](https://github.com/tokio-rs/axum) service that owns SSO accounts,
vaults, sharing/invites, and the Yjs document service. It checks vault access,
mints short-lived document tokens, handles the Yjs sync protocol under `/d/*`,
and stores versioned CRDT data in `CRDT_STORE`.

```
                        ┌────────── this server ──────────┐
Obsidian plugin ─HTTPS─▶│ auth, REST, vaults, sharing      │
Obsidian plugin ─WSS───▶│ /dmux and /d/* native Yjs sync  │
                        │ snapshot + update logs on disk   │
                        └──────────────────────────────────┘
```

The plugin uses its own small Yjs provider in `src/sync/RealtimeProvider.ts`.
The document update encoding remains Yjs v1, but capability checks decide
whether a released plugin can connect to a given server. No y-sweet process or
shared y-sweet key runs on the server.

**Binary files** (images, PDFs, and other non-Markdown attachments) do not go
through Yjs because that would bloat the CRDT. Instead the plugin syncs only
a `path → sha256` mapping through the CRDT index and stores the bytes in a
**content-addressed blob store** served by this server. The raw sync blob store is
under `/api/vaults/{id}/blobs/{hash}` for the matching plugin (a `HEAD` with
`?size=` reports a stored copy of another length as missing, so the uploader
repairs it, and path-scoped downloads refuse a copy whose length differs from
the index), while consumer-facing
attachment APIs live under `/api/vaults/{id}/attachments/*`. Attachments enforce an
extension allowlist, an attachment-specific size cap, SSRF checks for from-URL
fetches, and signed single-use public upload links via `/upload`. Vault members can
also create revocable public download links through
`/api/vaults/{id}/attachment-shares`; returned `/a/{share_id}` links require no
authentication and resolve only while the shared path still points to the exact
content hash captured at creation.

**Remote Cursors / MCP** expose a vault-scoped MCP resource at `/mcp/i/{appId}`.
MCP clients can use OAuth 2.1 against this server, and direct REST automation can
use the generated cursor secret as a bearer token. Both paths attribute writes to
the owning user and cursor in the git audit log.

**Git backup** lets vault admins mirror the audit repo to a remote
(`/api/vaults/{id}/backup`, also in the plugin's settings UI). The server pushes
after every audit commit, authenticating with either a server-generated ed25519
deploy key (add the public key to the remote with write access) or an HTTPS
access token. Push failures are recorded per vault and retried on the next commit.

## Running with Docker

The image runs one process and exposes one port. Mount `/data` so CRDT
generations, blobs, the application database, and git repositories survive
container replacement.

Native CRDT storage is single-writer. Do not point multiple running replicas at
different `CRDT_STORE` directories or a shared filesystem. Use one active
writer until an authoritative object-store backend with conditional manifest
writes is configured; load-balancer fan-out across independent stores is
split-brain.

```sh
docker run -p 8081:8081 -v realtime-data:/data \
  -e PUBLIC_BASE_URL=https://sync.example.com \
  -e OIDC_MODE=oidc \
  -e OIDC_ISSUER=https://id.example.com \
  -e OIDC_CLIENT_ID=... -e OIDC_CLIENT_SECRET=... \
  ghcr.io/<owner>/realtime-server
```

Set `PUBLIC_BASE_URL` to the URL clients reach this server at; it is baked into
the minted client tokens (as `wss://…/d/{doc}/ws`). Put `https://sync.example.com`
in the Obsidian plugin's **Auth server URL**.

## Running without Docker

1. Start the server:

   ```sh
   export PUBLIC_BASE_URL=http://127.0.0.1:8081
   export CRDT_STORE=./crdt
   export OIDC_MODE=oidc
   export OIDC_ISSUER=https://id.example.com
   export OIDC_CLIENT_ID=...
   export OIDC_CLIENT_SECRET=...
   cargo run --release
   ```

2. (Optional) Build the read-only web viewer for public share links
   (`/view/{id}`), served from `WEB_DIST_PATH`:

   ```sh
   bun run build:web   # from the repo root; outputs packages/web/dist
   ```

   Without it the share API still works, but `/view/*` pages return an error.
   The Docker image bundles a prebuilt copy.

## Configuration (environment)

| Variable | Default | Meaning |
| --- | --- | --- |
| `DATABASE_URL` | `sqlite://realtime.db?mode=rwc` | SeaORM sqlite URL |
| `BACKGROUND_JOBS_ENABLED` | enabled | Set `0` to pause Apalis workers without deleting queued intents |
| `BACKGROUND_JOB_CONCURRENCY` | `4` | Maximum jobs running at once in this server process |
| `BACKGROUND_JOB_MAX_ATTEMPTS` | `25` | Attempts before an intent is marked as a terminal failure |
| `BACKGROUND_JOB_RETRY_MIN_MS` | `250` | Minimum retry delay |
| `BACKGROUND_JOB_RETRY_MAX_MS` | `30000` | Maximum jittered exponential retry delay |
| `BACKGROUND_JOB_SHUTDOWN_TIMEOUT_MS` | `30000` | Time allowed for workers to finish during graceful shutdown |
| `SERVER_SHUTDOWN_TIMEOUT_MS` | `30000` | Maximum time HTTP/WebSocket connections may delay process shutdown |
| `BIND_ADDR` | `127.0.0.1:8081` | listen address |
| `PUBLIC_BASE_URL` | `http://127.0.0.1:8081` | this server's public URL; OIDC redirect default **and** the URL baked into minted client tokens (clients connect to `/d/*` here) |
| `CRDT_STORE` | `./crdt` (`/data/crdt` in Docker) | Native Yjs storage directory; each document has a checksummed snapshot, an append-only update segment, and an atomic manifest |
| `CRDT_EPOCH_PERIOD_DAYS` | `365` | Maximum active document-epoch age before replacement |
| `CRDT_EPOCH_RECOVERY_DAYS` | `30` | Retention window for immutable retired epochs |
| `CRDT_EPOCH_MAX_UPDATES` | `100000` | Update-count threshold for early replacement |
| `CRDT_EPOCH_MAX_STATE_BYTES` | `33554432` | Encoded-state growth allowed above the epoch's logical baseline |
| `CRDT_EPOCH_MAX_DELETE_SET_BYTES` | `8388608` | Encoded delete-set growth allowed above the epoch's logical baseline |
| `CRDT_EPOCH_ACK_TIMEOUT_MS` | `30000` | How long a proposed epoch waits for connected peers to acknowledge it before activating anyway |
| `BLOB_DIR` | `./blobs` | filesystem directory for the content-addressed binary blob store (use a path on the persistent volume, e.g. `/data/blobs`) |
| `OIDC_MODE` | `oidc` | `oidc` for a real IdP, `mock` for the in-process test issuer |
| `OIDC_ISSUER` / `OIDC_CLIENT_ID` / `OIDC_CLIENT_SECRET` / `OIDC_REDIRECT_URL` | — | OIDC config (real mode) |
| `ALLOWED_LOGIN_REDIRECTS` | — | Comma-separated web origins allowed for login redirects; the public origin is always allowed |
| `CORS_ALLOWED_ORIGINS` | Obsidian app origin | Comma-separated allowed CORS origins |
| `GIT_DATA_DIR` | `./git` (`/data/git` in Docker) | Per-vault git audit/backup repository directory |
| `GIT_AUDIT_ENABLED` | enabled | Set `0` to disable git audit commits |
| `GIT_DEBOUNCE_MS` | `5000` | Idle debounce before writing a git audit commit |
| `GIT_BOT_NAME` / `GIT_BOT_EMAIL` | `Realtime` / `realtime@localhost` | Git committer identity and fallback author |
| `GIT_INLINE_ATTACHMENT_MAX_BYTES` | `5242880` (5 MB) | Attachments up to this size are committed verbatim into the git repo; larger ones become a text shim (`version https://realtime.md/attachment-shim/v1`) carrying the sha256 and an authenticated `/api/vaults/{id}/blobs/{hash}` download URL |
| `CURSOR_EMAIL_DOMAIN` | domain from `GIT_BOT_EMAIL`, else `localhost` | Synthetic email domain for cursor-attributed git authors |
| `DAILY_NOTE_PATH_TEMPLATE` | `Daily Notes/{{YYYY-MM-DD}}.md` | Daily periodic note template |
| `WEEKLY_NOTE_PATH_TEMPLATE` / `MONTHLY_NOTE_PATH_TEMPLATE` / `QUARTERLY_NOTE_PATH_TEMPLATE` / `YEARLY_NOTE_PATH_TEMPLATE` | — | Optional periodic note templates |
| `ATTACHMENT_FETCH_HOST_ALLOWLIST` | — | Comma-separated hostnames allowed for server-side attachment fetches from URL |
| `ATTACHMENT_ALLOWED_EXTENSIONS` | common images, `pdf`, `txt` | Comma-separated extensions allowed for signed and server-fetched uploads; `*` allows every extension and extensionless files |
| `ATTACHMENT_MAX_BYTES` | raw blob max | Per-attachment upload/fetch size cap |
| `ATTACHMENTS_PATH_MODE` | `relative` | `relative` or `subfolder`; `subfolder` requires attachment paths under `ATTACHMENTS_SUBFOLDER` |
| `ATTACHMENTS_SUBFOLDER` | — | Required for subfolder path mode and used as the default signed-upload landing directory |
| `UPLOAD_TOKEN` | generated persistent secret under `GIT_DATA_DIR/.upload-token` | HMAC key for signed single-use browser upload links. When unset, the server generates a 256-bit secret and reuses it across restarts. Production (`OIDC_MODE=oidc`) refuses to start if the value is the old documented default or shorter than 32 characters. |
| `WEB_DIST_PATH` | `/usr/local/share/realtime-web` in Docker, else `../packages/web/dist` | Vite build output of the read-only web viewer (`packages/web`), served at `/view/{share_id}` for public note shares |
| `CRSQLITE_EXT_PATH` | `/usr/local/lib/crsqlite/crsqlite.so` in Docker, else unset | Path to the cr-sqlite loadable extension that backs synced **plugin databases** (server-side replica + git dumps under `.sql/`). When unset/missing, plugin-database replication degrades gracefully: client-to-client sync over the Y log is unaffected, but the server keeps no replica and git skips the per-DB SQL dumps. Its major version must match the client WASM's sync-format major. |

Git reconciliation, backup pushes, search refreshes, and plugin-database
replication run through an Apalis queue stored in the main SQLite database.
Vault admins can inspect the queue at `GET /api/vaults/{id}/jobs`, retry a
terminal failure with `POST /api/vaults/{id}/jobs/retry`, or cancel pending
work with `POST /api/vaults/{id}/jobs/cancel`. The two action routes accept
`{"intentKey":"..."}`.

`docker-entrypoint.sh` creates `CRDT_STORE`, `BLOB_DIR`, and `GIT_DATA_DIR`,
then starts `realtime-server` directly.

## Production health and sync metrics

The server exposes three unauthenticated operator endpoints:

- `GET /health/live` reports that the process can serve HTTP.
- `GET /health/ready` verifies the application database, writable CRDT/blob
  directories, the Git directory when audit is enabled, and restore-journal
  completion. It returns HTTP 503 after shutdown begins or when any required
  check fails.
- `GET /metrics` emits OpenMetrics counters and gauges for physical sync
  connections, multiplexed document channels, rejected opens, backpressure
  resets, native CRDT connections, loaded documents, durable updates and
  bytes, persistence latency, compactions, failures, and lagged consumers.

Configure a load balancer to remove the instance when `/health/ready` stops
returning 200. SIGTERM/SIGINT marks readiness as draining before Axum waits for
active HTTP and WebSocket connections; persistent background workers drain
after the HTTP server finishes. HTTP/WebSocket draining is bounded by
`SERVER_SHUTDOWN_TIMEOUT_MS`; worker draining is bounded separately by
`BACKGROUND_JOB_SHUTDOWN_TIMEOUT_MS`.

Baseline latency and throughput on the same storage class used in production:

```sh
cargo bench --manifest-path server/Cargo.toml --bench sync_soak
```

The soak harness performs concurrent durable writes across many documents,
periodically drops and reloads the document store, verifies sampled documents
after every restart, compacts in parallel, and prints JSON percentiles and disk
usage. Its workload is controlled by `RT_SOAK_DOCUMENTS`, `RT_SOAK_ROUNDS`,
`RT_SOAK_RESTART_EVERY`, and `RT_SOAK_COMPACT`. By default, each sampled restart
also appends a partial record and proves that startup truncates the torn tail;
set `RT_SOAK_INJECT_TORN_TAIL=0` to skip that fault. CI or deployment
qualification can enforce measured budgets with `RT_SOAK_MAX_P99_ACK_MS`,
`RT_SOAK_MAX_RESTART_MS`, and `RT_SOAK_MIN_WRITES_PER_SECOND`.

Use deployment-specific latency budgets rather than copying results between
filesystems. Initial alerting invariants:

- readiness failures page immediately;
- `realtime_crdt_update_failures_total`,
  `realtime_crdt_compaction_failures_total`,
  `realtime_crdt_lagged_connections_total`, and
  `realtime_sync_document_channel_backpressure_resets_total` should not
  increase during steady operation;
- alert when durable-update latency remains above twice the qualified soak
  baseline, then inspect disk latency and compaction activity;
- alert on sustained connection/channel growth after client traffic falls, and
  on a mean channel fan-out approaching the hard 512 channels per mux socket;
- keep the persistent volume below the operator's normal disk-capacity alarm
  (80% is a conservative starting point) so append, compaction, and backup
  swaps retain headroom.

The existing `crdt_storage` benchmark remains the focused append/compaction
microbenchmark. The `sync_soak` harness is the release-level concurrency and
restart qualification.

## Application database migrations

The server applies the ordered migrations in `server/src/migration/`
automatically during startup, before it creates application state, starts
background jobs, or accepts requests. Schema changes and their
`seaql_migrations` ledger rows commit together in one SQLite transaction.

The first migration is an adoption baseline. On a fresh database it creates
the current application tables. On an unversioned database from an older
Realtime release it keeps existing tables and rows, then the following
migration adds the known legacy columns and background-job tables. Re-running
startup with no pending migrations is a no-op.

Migration history must be an exact prefix of the migrations compiled into the
server binary. A binary using this migration system refuses to start when the
database contains an unknown migration or has missing/reordered history.
Applied migration files are therefore immutable: add a new, ordered migration
for every later schema change; never edit, rename, or delete an applied one.

Application migrations are forward-only. Before upgrading, create and verify a
full-server backup. Rolling back across a schema migration means restoring the
matching backup, not running a down migration or editing
`seaql_migrations` manually.

## CRDT storage maintenance

An accepted update reaches the live document only after its checksummed log
record has reached disk. The server compacts long logs in the background and
treats the manifest swap as the compaction commit. An incomplete final record
is truncated during startup; checksum failures stop the document instead of
replacing it with an empty one. If the next record would cross the 512 MiB
replay limit, the server compacts synchronously before appending it, and the
append path rejects any segment that still cannot stay within that limit.

Long-lived documents also use logical epochs. The server measures encoded
state, encoded delete-set size, accepted updates, age, and active connections.
It proposes a new epoch after one year, after 100,000 accepted updates, or when
encoded state/delete-set growth above the epoch's clean logical baseline reaches
a configured bound. Connected clients persist the epoch and acknowledge it before the
server accepts any later write from that connection. The replacement contains
the same logical Y.Text, Y.Map, and Y.Array content with fresh CRDT identities.
Epochs are monotonic on the client: after acknowledging a proposal, a client
retries token acquisition until that epoch activates rather than reopening the
older namespace while another client is still acknowledging. While a proposal
is pending, newly minted document tokens name the pending epoch and carry
`epochPending: true`; connections to it are refused until it activates, so no
client joins the retiring epoch after the proposal (its items would otherwise
be re-uploaded into the replacement as duplicate content). Activation waits
for connected peers to acknowledge for at most `CRDT_EPOCH_ACK_TIMEOUT_MS`.
The old physical document remains immutable for
`CRDT_EPOCH_RECOVERY_DAYS`; tokens for a retired epoch receive HTTP 409 and
clients obtain a fresh token and Y.Doc. The epoch manifest is the activation
commit point, so an interrupted replacement leaves the old epoch authoritative
and resumes on restart.

Stop the server before running maintenance commands:

```sh
realtime-server crdt inspect /data/crdt
realtime-server crdt repair /data/crdt
realtime-server crdt import-ysweet /data/old-ysweet /data/crdt
realtime-server crdt migrate-ysweet-cutover \
  /data/old-ysweet /data/crdt 12345
```

`inspect` only reads files and exits unsuccessfully when any document is
corrupt. `repair` can truncate a bad update-log tail or rebuild a damaged
manifest from valid generation files; its JSON report says which documents
changed. The import
command reads y-sweet 0.9 filesystem entries (`{docId}/data.ysweet`) and skips
document IDs already present in the destination. Old atomic `{docId}.yjs`
snapshots from the first native store format migrate automatically on first
load and remain as `{docId}.yjs.v1-migrated`.

`migrate-ysweet-cutover` sends SIGTERM to the supplied y-sweet PID and waits for
the process to exit, which closes active WebSockets and finishes the store's
normal shutdown path before import starts. Remove y-sweet from the load
balancer before invoking it. Start the native server at the same public URL
afterward; clients obtain fresh document tokens and reconnect without changing
plugin configuration. Keep the old y-sweet store intact until the native store
passes `crdt inspect`. The destination must not contain documents from a prior
partial import; the importer skips existing document IDs.

Run the storage benchmark with `cargo bench --manifest-path server/Cargo.toml
--bench crdt_storage`. It reports append acknowledgement percentiles, disk
growth before and after compaction, cold-load time, and a 64-document parallel
write measurement.

## Full-server backup and restore

The Git remote configured for a vault protects its audit repository only. Use
the `backup` commands for disaster recovery. A full backup contains:

- the main SQLite database, including accounts, sessions, vault metadata,
  memberships, search state, blob metadata, and the durable background-job queue;
- native CRDT snapshots and update logs;
- content-addressed attachment bytes;
- every per-vault Git repository and its server-managed deploy key;
- server-side cr-sqlite plugin-database replicas.

Backup creation and restore are offline operations. Stop the server first. The
server, `backup create`, and `backup restore` take the same advisory lock beside
`DATABASE_URL`, so a second process fails instead of taking a mixed-time copy.
`backup verify` only reads an already-created backup and does not need the live
server configuration.

Create and verify a backup directory:

```sh
realtime-server backup create /var/backups/realtime-2026-08-10
realtime-server backup verify /var/backups/realtime-2026-08-10
```

The destination must not exist. Creation writes to a private sibling staging
directory, snapshots SQLite databases through SQLite's backup API, verifies
every store, fsyncs the result, and then renames it into place. The completed
directory contains `manifest.json` and `data/`. The versioned manifest records
the server ID, server and storage format versions, capability versions, exact
directory inventory, file sizes, Unix modes, and SHA-256 digests.

Restore only after `backup verify` succeeds:

```sh
realtime-server backup restore /var/backups/realtime-2026-08-10 --force
```

Restore copies each component to a sibling of its configured destination and
verifies the staged copy before touching live state. It then swaps the main
database, CRDT store, blob store, Git store, and plugin replicas under a durable
restore journal. SQLite WAL, shared-memory, and rollback-journal sidecars are
removed as part of the same transaction. A pre-commit interruption rolls back
to the old state; an interruption after the commit marker keeps the restored
state and finishes cleanup. The server refuses to start while the journal is
present. Re-run the same restore command to recover it.

Keep enough free space for the staged copy and the old state during the swap.
Do not place a backup inside any live state directory. A backup contains
plaintext note and attachment data, session and OAuth records, upload secrets,
and Git credentials; store it with the same access controls as the live server.
Copy or archive the completed directory only after verification.

For the Docker image, stop the serving container and run the binary against the
same data volume with a separate backup mount:

```sh
docker run --rm --entrypoint realtime-server \
  -v realtime-data:/data \
  -v /srv/realtime-backups:/backups \
  ghcr.io/<owner>/realtime-server \
  backup create /backups/realtime-2026-08-10
```

Use `backup verify` or `backup restore ... --force` in the final line for those
operations. Preserve the container's `DATABASE_URL`, `CRDT_STORE`, `BLOB_DIR`,
and `GIT_DATA_DIR` overrides if they differ from the image defaults.

## OAuth, MCP, and API docs

The server is also an OAuth 2.1 authorization server for MCP clients. Discovery is
available at `/.well-known/oauth-authorization-server`, and MCP protected resource
metadata is available at `/.well-known/oauth-protected-resource/mcp/i/{appId}`.
Tokens are opaque, hashed in SQLite, and scoped to the remote cursor's vault.

Swagger UI is served at `/docs`, with the OpenAPI JSON at `/openapi.json`. The spec
covers consumer-facing REST/auth/OAuth/upload/permalink routes and intentionally
excludes `/mcp`, `/d/*`, raw blob storage, and `/api/doc-token`.

## Compatibility & versioning

`GET /api/server-info` advertises the server's release `version` (semver,
operator-facing only) and a `caps` map of named capability versions per
compatibility surface (`restApi`, `oauth`, `pluginDbSync`, `attachmentShim`,
`documentEpoch`, `documentInvalidation`).
Clients gate on `caps`, not on `version`. Cap constants live in
`src/caps.rs`; bump a cap's value only on a wire-incompatible change to that
surface. See **[../docs/versioning.md](../docs/versioning.md)** for the full
rules, the client gating behavior, and rollout notes.

## Mock OIDC (tests/dev)

With `OIDC_MODE=mock`, `/auth/login` short-circuits the IdP round-trip and issues
a session directly (the user can be chosen with `?mock_sub=&mock_email=&mock_name=`).
This drives the full login → session → vault → doc-token flow with no external IdP
and backs the Tier-2 / Tier-3 test harnesses.

## Tests

```sh
cargo test
```

Covers the native CRDT engine and sync transport with temp snapshot directories,
plus login, sessions, vault APIs, invites, document-token scopes, git audit,
plugin databases, streaming cursors, sharing, and binary blob validation.
