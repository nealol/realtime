# @realtime-md/sdk

TypeScript SDK for the [Realtime](https://github.com/nealol/realtime) server: authenticate as a user or a remote cursor (robot), manage vaults, edit notes/canvases/bases/attachments, search, and stream tokens into notes live over WebSocket.

Isomorphic core (browser + Node ≥ 20, global `fetch`/`WebSocket`); interactive login helpers live under `@realtime-md/sdk/node`.

## Install

```sh
npm install @realtime-md/sdk
```

## As a user (session token)

```ts
import { RealtimeClient } from "@realtime-md/sdk";

const client = new RealtimeClient({ baseUrl: "https://realtime.example.com", token });
console.log(await client.me());

const vaultInfo = await client.vaults.create("My Vault"); // remote vault creation
const vault = client.vault(vaultInfo.id);

await vault.notes.create("Hello.md", "# Hello");
await vault.notes.patch("Hello.md", { old: "Hello", new: "World" });
await vault.frontmatter.patch("Hello.md", { set: { status: "done" } });
await vault.search.search("world");
await vault.attachments.upload("img/pic.png", bytes);
```

## Whole-file replaces

A plain `replace` or attachment `upload` overwrites whatever the server holds,
including changes made after you read the file. Pass what you read to have the
server refuse a stale write with `ConflictError` instead:

```ts
import { noteContentHash } from "@realtime-md/sdk";

const note = await vault.notes.read("Hello.md");
await vault.notes.replace("Hello.md", rewrite(note.content), {
  expectedContentHash: await noteContentHash(note.content),
});

const canvas = await vault.canvases.read("Planning.canvas");
await vault.canvases.replace("Planning.canvas", edit(canvas.value), {
  ifMatch: canvas.valueHash,
});

// Attachments: the hash from `list` (or an earlier upload) you replace…
await vault.attachments.upload("img/pic.png", bytes, { ifMatch: listed.hash });
// …or require that nothing is at the path yet.
await vault.attachments.upload("img/new.png", bytes, { ifNoneMatch: "*" });
```

`notes.append` does this itself and re-reads when another edit lands first.
Moves refuse to overwrite an existing target with `ConflictError`.

## Atomic Canvas edits

`canvases.applyOperations()` sends one field-level batch. Patches name the fields to set and remove, so an unchanged stale field never becomes an edit. Deletes create tombstones; use `node-restore` or `edge-restore` when restoring an ID on purpose.

```ts
await vault.canvases.applyOperations("Planning.canvas", {
  mutationId: crypto.randomUUID(),
  operations: [
    { type: "node-patch", id: "task-1", patch: { set: { x: 320 }, remove: [] } },
    { type: "edge-delete", id: "old-link" },
  ],
});
```

The server applies the array in one Yjs transaction and rejects the whole batch if any operation fails. A repeated `mutationId` doesn't apply the batch twice; its response reflects the document's current value. Existing node and edge helpers try operation batches first, then retry through their older REST routes when a server returns 404.

Get a token interactively (Node): `loginViaBrowser({ baseUrl })` from `@realtime-md/sdk/node` (requires the loopback origin in the server's `ALLOWED_LOGIN_REDIRECTS`), or paste one from `{baseUrl}/auth/login`.

## As a remote cursor (robot)

Cursor edits get robot Git attribution and an admin-undoable audit log.

```ts
import { CursorClient } from "@realtime-md/sdk";
import { loginCursorViaOAuth } from "@realtime-md/sdk/node";

// One-call OAuth 2.1 PKCE: registers a client, opens the browser,
// catches the loopback redirect, returns auto-refreshing tokens.
const { tokenProvider } = await loginCursorViaOAuth({ baseUrl, mcpUrl: cursor.mcpUrl });
const robot = new CursorClient({ baseUrl, vaultId, tokenProvider });

await robot.notes.append("Log.md", "- robot was here");

// Stream tokens into a note in near-realtime (named caret included):
const stream = await robot.stream("Draft.md", { mode: "after", text: "## Draft" });
for await (const chunk of llmOutput) await stream.write(chunk);
const { auditId, inserted } = await stream.end();
```

A cursor secret token (from cursor creation) works too: `new CursorClient({ baseUrl, vaultId, token })`.

## Errors

Non-2xx responses throw `ApiError` (`status`, `message`); 400 → `ValidationError`, 401 → `AuthError`, 404 → `NotFoundError`, and 409 → `ConflictError`. Token providers can refresh once on 401 before the error surfaces.

## Compatibility & versioning

`serverInfo()` returns `{ serverId, version?, caps?, requiredCaps? }`. The
optional `caps` map carries named capability versions per surface
(`restApi`, `oauth`, `pluginDbSync`, `attachmentShim`, `documentEpoch`,
`documentInvalidation`). The
SDK also mirrors `epoch` on document-token responses. Consumers must create a
fresh local Y.Doc when that value changes; the SDK does not do this or enforce
caps itself. Newer servers also return `expiresAt` (ms since the Unix epoch);
mint a fresh token instead of reconnecting with one past it. Only the Obsidian plugin performs that lifecycle automatically. See
**[../../docs/versioning.md](../../docs/versioning.md)** for the cap names,
bump rules, and gating behavior.

## Coverage

Vaults · invites · members · remote cursors + audit log (list/undo) · notes (CRUD, patch, move, permalinks, frontmatter, periodic) · attachments + content-addressed blobs · canvases (nodes/edges) · bases (views/filters/formulas/properties) · search/tags/backlinks · storage + blob GC · git backup config · plugin-db replication + server-side SQL (list/query/execute) · native Yjs document tokens · OAuth 2.1 (discovery, dynamic registration, PKCE, refresh) · streaming WebSocket API.

Everything the MCP server's tools can do is covered via the same underlying REST endpoints.
