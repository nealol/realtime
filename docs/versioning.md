# Compatibility & Versioning

Realtime has two independent release semvers — one for the Obsidian plugin
(`package.json` / `manifest.json`) and one for the server
(`server/Cargo.toml`). They release on different cadences: a single server
serves many client versions, and a single client talks to many server
versions. **Neither semver is used to gate compatibility directly.**

Compatibility is gated by **named capability versions** ("caps"): each
compatibility surface has its own opaque string version, bumped only when that
surface changes in a wire-incompatible way. The server advertises its caps on
`GET /api/server-info`; the Obsidian plugin intersects them against its own
accepted values and hard-blocks on mismatch.

```
GET /api/server-info
200 OK
{
  "serverId": "abc123",
  "version": "0.4.2",          // server release semver — operator-facing only
  "caps": {
    "restApi": "3",
    "oauth": "1",
    "pluginDbSync": "crsqlite-1",
    "attachmentShim": "https://realtime.md/attachment-shim/v1",
    "documentEpoch": "1",
    "documentInvalidation": "1"
  },
  "requiredCaps": ["documentEpoch"]
}
```

## Why named caps instead of a single `apiVersion`

A single integer would force every client to update on any change to any
surface. Named caps let you bump only the affected surface; clients that don't
use that surface don't need to update. The codebase already had three
independent compatibility tags before this system was introduced
(`SYNC_FORMAT`, `ATTACHMENT_SHIM_VERSION`, per-DB `schemaVersion`); caps
generalize that pattern.

## The six caps

| Cap | Current value | Bumps when |
|---|---|---|
| `restApi` | `"3"` | Any `/api/*` request/response body changes in a breaking way, or a change to the sync transport the plugin requires (e.g. the `/dmux` multiplexer) |
| `oauth` | `"1"` | OAuth 2.1 server metadata or token endpoint shape changes (plugin's own OAuth flow only — see "External MCP clients" below) |
| `pluginDbSync` | `"crsqlite-1"` | The cr-sqlite `crsql_changes` wire encoding changes |
| `attachmentShim` | `"https://realtime.md/attachment-shim/v1"` | The text shim format committed into git backups for oversized blobs changes |
| `documentEpoch` | `"1"` | The proposal/acknowledgement protocol, token epoch semantics, or logical replacement rules change |
| `documentInvalidation` | `"1"` | The advisory child-document invalidation message or its delivery semantics change |

### Where the constants live

- **Server**: `server/src/caps.rs` — `REST_API`, `OAUTH`, `PLUGIN_DB_SYNC`,
  `ATTACHMENT_SHIM`, `DOCUMENT_EPOCH`, `DOCUMENT_INVALIDATION`, plus `caps()`
  returning all six in stable order.
  `ATTACHMENT_SHIM` is the single source of truth, re-exported by
  `git::ATTACHMENT_SHIM_VERSION`.
- **Client (plugin)**: `src/caps.ts` — `REQUIRED_CAPS` maps each cap name to
  the list of accepted values. `pluginDbSync` is sourced from
  `SYNC_FORMAT` in `src/pluginDb/types.ts` so the client-side cr-sqlite
  format constant and the cap value cannot drift.
- **SDK / CLI**: `packages/sdk/src/types.ts` mirrors the optional
  `version` / `caps` / `requiredCaps` fields on `ServerInfoResponse` so
  consumers can self-gate. The SDK and CLI do **not** enforce caps
  themselves — only the Obsidian plugin does.

## Bump rules

1. **Bump a cap's value only on a wire-incompatible change to that surface.**
   Adding an optional request/response field does NOT bump. Removing or
   renaming a field, changing a type, or changing semantics DOES bump.
2. **Cap values are compiled-in constants, never env config** — they must
   reflect the actual code on both sides.
3. **New surfaces get a new cap name; old cap names never disappear.**
4. **To accept a range of server releases during a migration window**, list
   multiple accepted values on the client (e.g.
   `REQUIRED_CAPS.restApi: ["1", "2"]`). The server advertises one value per
   cap; a client supports a range of server releases iff its accepted list
   for every cap contains the server's advertised value.

## Client gating behavior

`checkServerCaps(caps, requiredCaps)` in `src/caps.ts` is a pure function
implementing these rules:

| Server response | Result |
|---|---|
| `caps` is `null` / `undefined` / not an object | **block** — `"server-incompatible"` |
| `caps` is an object but missing a mandatory cap (`restApi`, `oauth`, `pluginDbSync`, `attachmentShim`, `documentEpoch`) | **block** — `"server-incompatible"` |
| mandatory cap value not in `REQUIRED_CAPS[name]` | **block** — `"server-incompatible"` |
| `documentInvalidation` missing or unsupported | **allow sync**, but disable mobile document eviction |
| cap name in server's `requiredCaps` but unknown to client | **block** — `"client-too-old"` |
| unknown cap name NOT in `requiredCaps` | **ignored** (forward-compatible additive surfaces) |

The result shape is `{ ok: true } | { ok: false; reason: "server-incompatible" | "client-too-old"; detail: string }`.
There is intentionally **no `"server-too-old"` or `"server-too-new"`** — cap
values are opaque strings, so direction cannot be inferred from a value
mismatch. `"client-too-old"` is used only when the server advertises a cap
name the client doesn't know exists (via `requiredCaps`).

### Enforcement points in the plugin

`Auth.serverInfoChecked` (in `src/auth.ts`) runs `checkServerCaps` on every
`/api/server-info` fetch and throws `CompatibilityError` on failure. It updates
`plugin.lastCompatibilityError` only for the active server; checking a staged
migration destination does not change the current server's banner. Callers include:

- `ensureServerId` — startup / legacy token migration
- `setSession` — protocol-token validation before committing a session
- `validSessionsForServer` — settings UI session picker
- `useKnownSession` — saved-session restore (check before trusting the
  saved session's server)
- `authenticateAt` — **before** `window.open` launches the SSO browser flow,
  so an incompatible server never sends the user to the browser

`authenticateAt` returns a staged session; `commitSession` installs it only while
the sign-in attempt is still current. Migration also validates the matching vault
before committing. Cancellation, logout, or unload prevents a late protocol token
or completed validation from replacing the active session.

`maybeStartSync` (in `src/main.ts`) hard-blocks on `CompatibilityError`:
sets the sync status and returns before `me()` / `startSync()`. Network and
offline errors are still tolerated as before — only compatibility failures
hard-block.

`ServerCompatibilityGate` (in `src/sync/compatibilityGate.ts`) shares one
single-flight server-info check across the vault index, notes, Canvas, Bases,
and plugin databases. Every provider uses it before requesting document tokens
or opening a socket, including autonomous reconnects and cached-token attempts.
An established connection dropping invalidates the shared check. A known
incompatibility also stops traffic on existing sockets; edits remain local and
the next compatible handshake replays them.

The settings banner (`CompatibilityBanner` in `src/settings.tsx`) renders
only when `plugin.lastCompatibilityError` is set, showing `reason`, `detail`,
and `serverVersion` if known. **No banner is shown when compatible** — per
project policy, the plugin does not nudge about newer server versions unless
compatibility is actually broken.

## Future optional caps

The `requiredCaps` field exists so future optional caps can be added without
hard-blocking old clients. The rollout pattern:

1. Add the new cap to `caps.rs` and `caps()` so the server advertises it.
2. Do **not** add it to `requiredCaps`. Old clients that don't know the name
   ignore it (per the gating rules above) and keep working.
3. Once all deployed clients understand the name, add it to `requiredCaps`
   if you want to make it mandatory for new clients.

**Note**: `requiredCaps` only protects clients that already implement this
checker. Pre-v1 clients with no cap checker will proceed until they hit a
real API mismatch. This is inherent and acceptable — the mechanism is
forward-looking.

## External MCP clients (Cursor, etc.)

External MCP clients consume the OAuth 2.1 server metadata document at
`/.well-known/oauth-authorization-server`, not `/api/server-info`. The
`oauth` cap gates the **plugin's own** OAuth flow only. If external-client
gating becomes necessary, surface the cap value in the OAuth server metadata
document as a follow-up.

## Testing

- **Server**: `server/tests/integration.rs` —
  `server_info_returns_stable_id_without_auth` asserts `version`, all six
  `caps` values, and the required `documentEpoch` cap.
- **Client**: `tests/unit/caps.test.ts` — unit tests covering every
  branch of `checkServerCaps` (undefined/null/non-object/array caps, exact
  match, missing mandatory cap, unsupported value, non-string value, unknown
  cap ignored vs. in `requiredCaps`, non-string/non-array `requiredCaps`,
  and `CompatibilityError` shape).
- **Full local validation**: `bun run test:all` runs typecheck, all unit
  suites, SDK e2e, and Rust server tests.
- **Released-version lab**: `bun run test:compat` downloads SHA-256-pinned
  release assets into `.compat-cache`, executes the released client cap checker,
  executes both released and current server cap declarations, runs the rollout
  matrix, and replays the checked-in Yjs v1 and y-sweet 0.9.1 corpora.
  `bun run test:compat:released` also starts the pinned released server image
  and compares its live `/api/server-info` response with its tagged source.
