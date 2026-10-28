# Lifecycle & deletion

[← Back to index](./README.md)

## Open, refcount, close

`open()` for a given `(pluginId, name)` returns a handle backed by a **single
shared engine**. Opening the same pair again returns another handle to the same
engine and increments a refcount; each `close()` decrements it. The engine shuts
down — flushing a final snapshot and disconnecting — when the **last** handle
closes.

```ts
const a = await realtime.sql.open({ pluginId, name: "tasks", schemaVersion: 1, migrate });
const b = await realtime.sql.open({ pluginId, name: "tasks", schemaVersion: 1, migrate });
// a and b share one engine.
await a.close(); // engine stays alive (b still open)
await b.close(); // engine closes, final snapshot written
```

Always `close()` what you `open()`, e.g. in `onunload()`.

## Snapshot persistence

The database lives in WASM memory and is mirrored to a local snapshot at:

```
.obsidian/plugins/realtime/plugin-dbs/server-<server>/vault-<vault>/<pluginId>/<name>.snap
```

Snapshots are written on a short debounce after writes and on `close()`, using an
atomic temp-write + replace. The snapshot stores the full `crsql_changes` dump —
not plain user rows — so cr-sqlite's site ids, column clocks and tombstones
survive a restart. This is why reopening never resurrects a deleted row.

User-defined indexes, views, and triggers are stored alongside the table schema.
They are recreated after the historical CRR changes are restored, so restoring
a snapshot does not fire user triggers again. Older snapshots without these
definitions recover them by running `migrate(tx, 0)` on a temporary database and
copying the missing definitions, without discarding unpublished local changes.

You never manage snapshots directly; the engine handles them. If a snapshot is
corrupt, the engine discards it and bootstraps from the server (see below).

## Deletion: the trash-bin model

Deleting a database is a **soft delete**, mirroring how Realtime handles deleted
notes and attachments.

```ts
await realtime.sql.delete({ pluginId, name: "tasks" });
```

This:

1. Sets a tombstone (`meta.deletedAt`) on the per-DB doc. Any open handle closes,
   the local snapshot is removed, and consumers get `state: "error"` with reason
   `deleted`.
2. Records an entry in the **vault trash bin** (the same bin as deleted notes),
   shown as a "Database" item labeled `<pluginId>/<name>`.

The data is retained on the server. From the trash bin (command palette →
**"Realtime: Open trash"**) the user can:

- **Restore** — clears the tombstone and the next `open()` re-bootstraps. The UI
  rejects restore if a live database with the same id already exists.
- **Delete** (permanent) — **purges**: the server deletes its replica and the git
  dump (producing a clean "database deleted" commit) and trims the per-DB doc.
  This is irreversible.

You can also restore programmatically:

```ts
await realtime.sql.restore({ pluginId, name: "tasks" });
const db = await realtime.sql.open({ pluginId, name: "tasks", schemaVersion: 1, migrate });
```

### Reacting to deletion in your plugin

If another device deletes a database you have open, you'll observe it through the
state change:

```ts
db.onStateChange((state) => {
  if (state === "error") {
    // The database may have been deleted elsewhere. Close your handle and
    // update the UI; a future open() can restore it.
  }
});
```

## What purge destroys (server side)

| Action | Replica file | Git dump | Per-DB doc | Recoverable? |
| --- | --- | --- | --- | --- |
| Soft delete (`delete`) | kept | kept | tombstoned | yes — restore |
| Restore (`restore`) | kept | kept | tombstone cleared | n/a |
| Purge (trash → Delete) | **deleted** | **deleted** | trimmed | **no** |

## The divergence escape hatch

If a database is ever suspected to have diverged or corrupted, rebuild it from
the authoritative server replica:

```ts
await db.rebaseFromServer();
```

This discards the local database and snapshot, bootstraps fresh from the server,
and re-applies the log. It is also exposed as the command-palette action
**"Realtime: Rebase plugin databases from server"**, and is triggered
automatically when a snapshot restore fails or a gap is detected (a needed range
of the log was compacted away before this device caught up).
