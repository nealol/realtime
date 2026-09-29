# Repository Guidelines

## Project Layout

- `src/` contains the Obsidian plugin client. The main entry point is `src/main.ts`; editor bindings live under `src/editor/`; plugin database sync lives under `src/pluginDb/`; history UI and binary/text conflict UI are split into focused modules.
- `server/` contains the Rust auth/API server, y-sweet proxy integration, blob storage, OAuth/MCP support, and integration tests.
- `packages/sdk/` contains the TypeScript SDK and its unit/e2e tests.
- `packages/cli/`, `packages/web/`, and `packages/plugin-api-types/` are workspace packages used by the CLI, read-only web viewer, and public plugin API types.
- `tests/` contains plugin unit/e2e support for the Obsidian plugin.
- Generated build artifacts such as `main.js`, `styles.css`, `crsqlite.wasm`, `server/target/`, and package `dist/` directories should not be hand-edited.

## Tooling

This repository is Bun-first for JavaScript/TypeScript tasks.

- Install dependencies with `bun install`.
- Run the plugin/watch build with `bun run dev`.
- Build production artifacts with `bun run build`.
- Run TypeScript checks with `bun run typecheck`.
- Format TypeScript with `bun run format` (`oxfmt`).
- Run the default JS test suite with `bun run test`.
- Run all configured unit suites with `bun run test:unit`.
- Run SDK tests with `bun run test:sdk`; run SDK e2e tests with `bun run test:sdk:e2e`.
- Run plugin e2e tests with `bun run test:e2e` (requires built artifacts from `bun run build`).
- Run Rust server tests with `bun run test:server` or `cargo test --manifest-path server/Cargo.toml`.
- Run the full local validation path with `bun run test:all` (includes typecheck, all unit suites, SDK e2e, and Rust server tests).

Prefer targeted test commands while iterating, then run the broadest relevant suite before handing work back.

## Coding Conventions

- Use TypeScript for plugin, SDK, CLI, and web code; use Rust for server code.
- Keep formatting consistent with `oxfmt` for TypeScript and `rustfmt`/Cargo conventions for Rust.
- Prefer existing abstractions around Yjs documents, vault sync, blob storage, auth, and SDK HTTP resources rather than adding parallel implementations.
- Preserve offline-first and conflict-resolution behavior when touching sync paths. Be especially careful around first-write seeding, binary divergence handling, Yjs awareness, cr-sqlite replication, and server-side audit/git behavior.
- Avoid broad refactors across plugin/server/package boundaries unless the change explicitly requires it.
- Do not hand-edit generated OpenAPI/build outputs unless the generation path is unavailable and the user requested it.

## Testing Notes

- Vitest is configured with named projects: `plugin`, `sdk-unit`, `cli-unit`, `web-unit`, and `sdk-e2e`.
- Plugin tests run in `jsdom` with the Obsidian mock from `tests/support/obsidian-mock.ts`.
- Some tests spawn y-sweet or the Rust server and intentionally disable file parallelism. Avoid changing those settings unless you are addressing test infrastructure directly.
- Rust server tests use mock OIDC and hermetic test state; prefer adding focused integration coverage in `server/tests/` for API/auth/storage changes.

## Operational Notes

- The plugin expects one public auth/sync URL; the server owns the native Yjs
  document lifecycle and multiplexes document channels under `/dmux`.
- Public REST/OAuth/API behavior is documented through the server OpenAPI setup and README files. Keep docs in sync when changing user-facing routes, configuration, SDK APIs, or plugin setup.
- Workspace packages are linked with Bun workspaces. Keep version and package boundary changes deliberate.

## Compatibility & Versioning

The server and client release on independent cadences; a single server serves many client versions and a single client talks to many server versions. Compatibility is gated by **named capability versions** ("caps"), not by either side's semver.

- `GET /api/server-info` returns `serverId`, `version` (server release semver, operator-facing only — **not** used for gating), `caps` (map of surface name → opaque string version), and `requiredCaps` (cap names the client must understand; currently `documentEpoch`).
- Cap constants live in `server/src/caps.rs` (server) and `src/caps.ts` (`REQUIRED_CAPS`, client). The cr-sqlite sync format constant is shared: `caps::PLUGIN_DB_SYNC` on the server, `SYNC_FORMAT` in `src/pluginDb/types.ts` on the client — they must stay in lockstep.
- Current caps: `restApi="3"`, `oauth="1"`,
  `pluginDbSync="crsqlite-1"`,
  `attachmentShim="https://realtime.md/attachment-shim/v1"`,
  `documentEpoch="1"`, `documentInvalidation="1"`.
- `documentEpoch` is required. It gates the proposal/acknowledgement rollover
  protocol and the `epoch` field on document tokens; clients must create a
  fresh local Y.Doc and persistence namespace when it changes. While a
  proposal is pending, tokens name the pending epoch and add the optional
  `epochPending: true` (no cap bump); clients must wait for activation and must
  never connect a fresh-epoch Y.Doc to the retiring epoch, or its items are
  later re-uploaded into the replacement as duplicate content.
- `documentInvalidation` is optional. It gates advisory child-document
  invalidation messages. Mobile document eviction must remain disabled when
  the cap is absent or unsupported; do not add it to server `requiredCaps`
  because older non-evicting clients remain safe.

### Bump rules

- Bump a cap's value **only** on a wire-incompatible change to that surface. Adding an optional request/response field does NOT bump. Removing/renaming a field, changing a type, or changing semantics DOES bump.
- Cap values are compiled-in constants, never env config — they must reflect the actual code.
- New surfaces get a new cap name; old cap names never disappear.

### Client gating (`src/caps.ts`)

`checkServerCaps(caps, requiredCaps)` enforces:
- `caps` is `null`/`undefined`/not an object → block `"server-incompatible"` (this plugin requires caps to prove `/dmux` support).
- `caps` is an object but lacks a mandatory cap → block `"server-incompatible"`.
- mandatory or server-required cap value not in `REQUIRED_CAPS[name]` → block
  `"server-incompatible"` (direction cannot be inferred from opaque strings;
  never reported as "too old/new").
- optional `documentInvalidation` missing or unsupported → allow sync but
  disable mobile document eviction.
- cap name in server's `requiredCaps` but unknown to client → block `"client-too-old"`.
- unknown cap name NOT in `requiredCaps` → ignored (forward-compatible additive surfaces).

`Auth.serverInfoChecked` runs the check on every `/api/server-info` fetch, sets `plugin.lastCompatibilityError` on failure, and throws `CompatibilityError`. `maybeStartSync` hard-blocks on `CompatibilityError` but still tolerates network/offline errors. The settings banner renders only when `lastCompatibilityError` is set — **no upgrade nudges when compatible**.

### Enforcement scope

- **Only the Obsidian plugin enforces caps.** The SDK and CLI mirror the optional `version`/`caps`/`requiredCaps` fields on `ServerInfoResponse` (`packages/sdk/src/types.ts`) so consumers can self-gate, but they do not block on mismatches themselves.
- **Future `requiredCaps` cannot protect pre-v1 clients** that lack the checker — they proceed until a real API mismatch. This is inherent and acceptable; the mechanism is forward-looking.
- **External MCP clients** (Cursor, etc.) consume the OAuth server metadata document, not `/api/server-info`. The `oauth` cap gates the plugin's own OAuth flow only. If external-client gating is needed, surface the cap value in the OAuth server metadata as a follow-up.
