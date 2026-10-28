# Canvas collaboration internals

Realtime keeps the standard JSON Canvas file shape on disk. Each Canvas gets its own Yjs document with ID-keyed `nodes` and `edges` maps, order arrays, and deletion tombstones. Text-like fields remain `Y.Text`; no cursor, selection, drag, resize, or viewport data enters persistent document state.

## Local edits

`CanvasBinding` keeps a normalized shadow of the state last shown in the bound Obsidian view. A save or interaction sample compares the current view with that shadow and sends only changed fields through `CanvasDocument.applyCanvasOperations()`. The operation batch runs in one Yjs transaction. A node move therefore changes `x` and `y` without replaying stale text or style fields.

Pointer interactions sample every 75 ms and publish once more on release, cancellation, blur, hiding, or teardown. Save requests during the gesture share that sampler instead of triggering a full capture per frame. A wrapped `onUnloadFile` flushes pending geometry before Obsidian saves and reuses the view; without that hook, saves retain immediate capture. Non-gesture edits remain immediate. Each capture normalizes once and computes a bounce-back fingerprint only when an imported snapshot needs comparison. Remote document events coalesce, and snapshot imports wait until the final local capture.

Before the first server sync, bindings do not fold view snapshots into the stored CRDT or import its possibly empty value. Once a view has been observed showing this file's startup disk content, its newer unsaved content participates in the startup merge alongside saved disk edits. A reused view that has not shown this file's content is not trusted.

Disk reconciliation carries the known merge base into the tombstone-aware reconciler. Removing a node or edge from that base deletes it even if another client, or this device before a restart, created it. Items added concurrently outside the base remain intact.

Server acknowledgements coalesce fingerprint recording into fixed 250 ms windows; startup recording remains immediate. A content revision guards the asynchronous hash, so recording does not need a second full-tree reconstruction and serialization. A changed revision is retried rather than publishing a stale fingerprint.

## Remote edits and private APIs

`CanvasViewAdapter` isolates Obsidian's private Canvas methods. It probes reads, item mutation, rendering, selection, interaction, viewport, and active text-editor shapes before calling them. A supported operation batch updates the live node or edge directly while retaining selection and viewport state.

Ordering changes, missing methods, and thrown private calls trigger one full `importData(..., true)` fallback. If the plugin can't attach a safe live binding at all, `StructuredDocument` writes the CRDT value to disk. Reused leaves recheck `file.path` before every read or write so an old binding can't overwrite the newly opened Canvas.

## Text cards and awareness

An active text card binds its CodeMirror document to the node's `Y.Text`. Relative cursor positions carry the active node ID; clients render a remote caret only when both the node ID and resolved `Y.Text` match. Detaching the editor clears the cursor and editing state.

Canvas awareness payload version 1 carries selections, drag or resize previews, the edited node, viewport coordinates, and a monotonic sequence. Parsers reject malformed or unknown versions. Unbind, blur, hidden-document, and disconnect paths clear local Canvas awareness and remove overlays.

## Server and automation

`POST /api/vaults/{id}/canvas-operations/{path}` accepts node, edge, and order operations. The server validates the complete batch against one decoded Yjs state, builds one field-level Yjs update, then writes it. A bad operation leaves the document unchanged. Deletes create tombstones; callers must use an explicit restore operation to reuse a tombstoned ID.

`mutationId` deduplicates retries. The SDK exposes the same operation union through `canvases.applyOperations()`, MCP exposes `apply_canvas_operations`, and the CLI accepts JSON through `rtmd canvas-apply`. Existing SDK node and edge helpers try the batch endpoint first, then use the older route only when a server returns 404.

## Attachments and diagnostics

File nodes pass through `VaultSync.canvasBinaryPaths()` before priority changes. Markdown, Canvas, Base, excluded, and disabled paths never enter binary sync. Missing indexed binaries move ahead of background transfers; repeated Canvas saves don't schedule repeated hashing.

Enable diagnostic logging in Realtime's advanced settings to inspect local operation counts, incremental applications, full-import fallbacks, deferred remote updates, and presence cleanup. Server logs include failed Canvas operation batches with their vault and path.
