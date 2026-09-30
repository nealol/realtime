# Plugin-managed remote cursors

Obsidian plugins can edit vault documents *as a robot* instead of as the
signed-in user. Edits made through a plugin-managed remote cursor get:

- **Git attribution** to the cursor (`Author: <name> <cursor+{appId}@…>` with
  `On-Behalf-Of` trailers naming the human who authorized it), and
- **an audit log entry** (Settings → Remote Cursors → Audit log) that admins
  can review and undo for ~3 days.

The cursor for a `(vault, pluginId)` pair is created on first acquire and shows
up in settings as "Managed by plugin: `<pluginId>`".

## Acquiring a cursor

```ts
const realtime = app.plugins.plugins["realtime"];
const cursor = await realtime.cursors.acquire({
  pluginId: this.manifest.id,   // required; same trust model as realtime.sql
  name: "My Bot",               // optional display name, first acquire only
});
```

The returned handle carries credentials for the remote surfaces — `token`,
`mcpUrl`, and `streamUrl` (see [streaming.md](./streaming.md)) — but for plain
document edits you don't need any of them:

## Local note edits (no WebSocket, no token plumbing)

The handle's `notes` methods call the server as the cursor and handle token
caching and renewal internally (expired tokens re-acquire transparently):

```ts
await cursor.notes.create("Reports/today.md", "# Report\n");
await cursor.notes.patch("Reports/today.md", { old: "# Report", new: "# Daily Report" });
await cursor.notes.append("Reports/today.md", "- generated item");
await cursor.notes.replace("Reports/today.md", "# Daily Report\nrewritten\n");
await cursor.notes.move("Reports/today.md", "Archive/today.md");
const note = await cursor.notes.read("Archive/today.md"); // { path, guid, content, permalink }
const all  = await cursor.notes.list();                   // [{ path, guid, permalink }]
await cursor.notes.delete("Archive/today.md");
```

Every mutation above is one audit entry and one cursor-attributed Git write.
The edit is applied on the server and syncs back to all clients (including the
local vault) through the normal realtime channel.

Notes:

- `append` is a convenience read-then-replace. The write carries the hash of
  the content it read, so an edit that lands in between is never reverted:
  the server refuses the write (`stale`) and `append` re-reads and retries,
  surfacing `stale` only if the note keeps changing. Prefer `patch` with a
  unique anchor under heavy contention.
- Errors surface the server's reason, e.g. `anchor_not_found` (patch anchor
  missing), `ambiguous` (multiple matches without `replaceAll`), `exists`
  (create/move target taken), `stale` (the note changed since it was read).
- For realtime token streaming with a live caret, use the WebSocket API at
  `cursor.streamUrl` with `cursor.token` ([streaming.md](./streaming.md)).
- Any vault member's client may acquire a plugin cursor; admins can delete it
  (and inspect/undo its operations) in settings.
