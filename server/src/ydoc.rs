use anyhow::{anyhow, Result};
use sea_orm::sea_query::OnConflict;
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, Set};
use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue};
use std::collections::HashMap;
use yrs::types::ToJson;
use yrs::{
    Any, Array, ArrayPrelim, Doc, GetString, Map, MapPrelim, Out, ReadTxn, Text, TextPrelim,
    Transact, Update,
};

use crate::crdt::DocumentStore;
use crate::entities::vault_files;
use crate::error::{AppError, AppResult};
use crate::session::now_millis;
use crate::state::AppState;

pub async fn read_update(state: &AppState, doc_id: &str) -> AppResult<Vec<u8>> {
    read_update_with(&state.documents, doc_id).await
}

pub async fn read_update_with(documents: &DocumentStore, doc_id: &str) -> AppResult<Vec<u8>> {
    documents.read_update(doc_id).await.map_err(AppError::from)
}

pub async fn write_update(state: &AppState, doc_id: &str, update: Vec<u8>) -> AppResult<()> {
    state
        .documents
        .apply_update(doc_id, &update)
        .await
        .map_err(AppError::from)
}

pub(crate) async fn read_update_for_write(
    state: &AppState,
    doc_id: &str,
) -> AppResult<(u64, Vec<u8>)> {
    state
        .documents
        .read_update_with_epoch(doc_id)
        .await
        .map_err(AppError::from)
}

pub(crate) async fn write_update_at_epoch(
    state: &AppState,
    doc_id: &str,
    epoch: u64,
    update: Vec<u8>,
) -> AppResult<()> {
    state
        .documents
        .apply_update_at_epoch(doc_id, epoch, &update)
        .await
        .map_err(AppError::from)
}

/// Read, decide, and write one document atomically (see
/// [`DocumentStore::modify`]): `build` gets the current encoded state and
/// returns the update to apply (empty: none) plus a result. No other write
/// lands in between, so `build` may refuse a write based on what it sees.
pub(crate) async fn modify<T>(
    state: &AppState,
    doc_id: &str,
    build: impl FnOnce(&[u8]) -> AppResult<(Vec<u8>, T)>,
) -> AppResult<T> {
    state
        .documents
        .modify(doc_id, build)
        .await
        .map_err(AppError::from)?
}

pub async fn set_text(state: &AppState, doc_id: &str, new_content: &str) -> AppResult<()> {
    let (epoch, current) = read_update_for_write(state, doc_id).await?;
    let update = build_set_text_update(&current, "contents", new_content)?;
    if update.is_empty() {
        return Ok(());
    }
    write_update_at_epoch(state, doc_id, epoch, update).await
}

/// Edit a note's text atomically: `edit` computes the new text from the
/// current text, and no other write lands before the resulting update, so
/// `edit` may refuse (e.g. a stale precondition) and a concurrent edit is
/// never reverted by diffing against an older read. Returns the text before
/// and after.
pub async fn update_text<F>(state: &AppState, doc_id: &str, edit: F) -> AppResult<(String, String)>
where
    F: FnOnce(&str) -> AppResult<String>,
{
    modify(state, doc_id, |current| {
        let before =
            decode_text(current, "contents").map_err(|e| AppError::Internal(e.to_string()))?;
        let after = edit(&before)?;
        let update = build_set_text_update(current, "contents", &after)?;
        Ok((update, (before, after)))
    })
    .await
}

pub async fn set_structured(state: &AppState, doc_id: &str, value: &JsonValue) -> AppResult<()> {
    update_structured(state, doc_id, |_| Ok(value.clone()))
        .await
        .map(|_| ())
}

/// Replace a structured document's value atomically: `edit` sees the current
/// value and may refuse a write whose caller based it on an older read.
/// Returns the value before the edit.
pub async fn update_structured<F>(state: &AppState, doc_id: &str, edit: F) -> AppResult<JsonValue>
where
    F: FnOnce(&JsonValue) -> AppResult<JsonValue>,
{
    modify(state, doc_id, |current| {
        let before = decode_structured(current).map_err(|e| AppError::Internal(e.to_string()))?;
        let after = edit(&before)?;
        let update = build_structured_update(current, &after)?;
        Ok((update, before))
    })
    .await
}

/// Map `path` to `guid` in the index. The check and the write are atomic, so
/// a path another client took after the caller's own existence check (the
/// `vault_files` mirror can lag the index) is refused instead of silently
/// orphaning that client's document.
pub async fn index_set_file(
    state: &AppState,
    vault_id: &str,
    path: &str,
    guid: &str,
) -> AppResult<()> {
    modify(state, vault_id, |current| {
        let occupied = decode_files_map(current)
            .map_err(|e| AppError::Internal(e.to_string()))?
            .into_iter()
            .any(|(existing_path, existing_guid)| existing_path == path && existing_guid != guid);
        if occupied {
            return Err(AppError::Conflict("note already exists".into()));
        }
        let update = build_map_set_update(current, "files", path, guid.to_string())?;
        Ok((update, ()))
    })
    .await?;
    upsert_vault_file(state, vault_id, path, guid).await
}

pub async fn index_remove_file(state: &AppState, vault_id: &str, path: &str) -> AppResult<()> {
    let (epoch, current) = read_update_for_write(state, vault_id).await?;
    let guid = decode_files_map(&current)
        .map_err(|e| AppError::Internal(e.to_string()))?
        .into_iter()
        .find(|(p, _)| p == path)
        .map(|(_, guid)| guid);
    let update = match &guid {
        Some(guid) => {
            let mut entry = trash_entry_common(path, "text");
            entry.insert("guid".to_string(), Any::String(guid.as_str().into()));
            build_trash_and_remove_update(&current, "files", path, entry)?
        }
        None => build_map_remove_update(&current, "files", path)?,
    };
    if !update.is_empty() {
        write_update_at_epoch(state, vault_id, epoch, update).await?;
    }
    if let Some(guid) = guid {
        vault_files::Entity::delete_many()
            .filter(vault_files::Column::VaultId.eq(vault_id))
            .filter(vault_files::Column::Guid.eq(guid))
            .exec(&state.db)
            .await?;
    }
    Ok(())
}

pub async fn index_rename(state: &AppState, vault_id: &str, from: &str, to: &str) -> AppResult<()> {
    let guid = modify(state, vault_id, |current| {
        let files = decode_files_map(current).map_err(|e| AppError::Internal(e.to_string()))?;
        if files.iter().any(|(path, _)| path == to) {
            return Err(AppError::Conflict("exists".into()));
        }
        let guid = files
            .into_iter()
            .find(|(path, _)| path == from)
            .map(|(_, guid)| guid)
            .ok_or(AppError::NotFound)?;
        let update = build_map_rename_update(current, "files", from, to, guid.clone())?;
        Ok((update, guid))
    })
    .await?;
    upsert_vault_file(state, vault_id, to, &guid).await
}

/// A condition on the attachment a path holds, checked atomically with the
/// index write that replaces it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AttachmentPrecondition<'a> {
    /// Write whatever the path holds.
    None,
    /// The path must hold no attachment (`If-None-Match: *`).
    Absent,
    /// The path must still hold this content hash (`If-Match`).
    Hash(&'a str),
}

pub async fn index_set_binary(
    state: &AppState,
    vault_id: &str,
    path: &str,
    hash: &str,
    size: i64,
) -> AppResult<()> {
    index_set_binary_if(
        state,
        vault_id,
        path,
        hash,
        size,
        AttachmentPrecondition::None,
    )
    .await
}

/// Map `path` to a blob once `precondition` holds for the attachment it maps
/// to now. Refusals are `Conflict("stale")` when the path no longer holds the
/// expected hash and `Conflict("exists")` when it was meant to be empty.
pub async fn index_set_binary_if(
    state: &AppState,
    vault_id: &str,
    path: &str,
    hash: &str,
    size: i64,
    precondition: AttachmentPrecondition<'_>,
) -> AppResult<()> {
    modify(state, vault_id, |current| {
        let existing = decode_binaries_map(current)
            .map_err(|e| AppError::Internal(e.to_string()))?
            .into_iter()
            .find(|(p, _)| p == path)
            .and_then(|(_, meta)| binary_hash_size(&meta));
        let existing_hash = existing.as_ref().map(|(hash, _)| hash.as_str());
        match precondition {
            AttachmentPrecondition::None => {}
            AttachmentPrecondition::Absent => {
                if existing.is_some() {
                    return Err(AppError::Conflict("exists".into()));
                }
            }
            AttachmentPrecondition::Hash(expected) => {
                if !existing_hash.is_some_and(|hash| hash.eq_ignore_ascii_case(expected)) {
                    return Err(AppError::Conflict("stale".into()));
                }
            }
        }
        if existing
            .as_ref()
            .is_some_and(|(existing, existing_size)| existing == hash && *existing_size == size)
        {
            return Ok((Vec::new(), ()));
        }
        // Record the version this replaces, like clients do, so a client whose
        // concurrent publish loses the map to this one sees a conflict rather
        // than a clean update.
        let prev = existing_hash.map_or(Any::Null, |hash| Any::String(hash.into()));
        let metadata = HashMap::from([
            ("hash".to_string(), Any::String(hash.into())),
            ("size".to_string(), Any::BigInt(size)),
            ("prev".to_string(), prev),
        ]);
        let update = build_map_set_update(current, "binaries", path, metadata)?;
        Ok((update, ()))
    })
    .await
}

pub async fn index_remove_binary(state: &AppState, vault_id: &str, path: &str) -> AppResult<()> {
    let (epoch, current) = read_update_for_write(state, vault_id).await?;
    let meta = decode_binaries_map(&current)
        .map_err(|e| AppError::Internal(e.to_string()))?
        .into_iter()
        .find(|(p, _)| p == path)
        .map(|(_, meta)| meta);
    let update = match meta.as_ref().and_then(binary_hash_size) {
        Some((hash, size)) => {
            let mut entry = trash_entry_common(path, "binary");
            entry.insert("hash".to_string(), Any::String(hash.into()));
            entry.insert("size".to_string(), Any::BigInt(size));
            build_trash_and_remove_update(&current, "binaries", path, entry)?
        }
        None => build_map_remove_update(&current, "binaries", path)?,
    };
    if !update.is_empty() {
        write_update_at_epoch(state, vault_id, epoch, update).await?;
    }
    Ok(())
}

pub async fn index_rename_binary(
    state: &AppState,
    vault_id: &str,
    from: &str,
    to: &str,
) -> AppResult<()> {
    modify(state, vault_id, |current| {
        let binaries =
            decode_binaries_map(current).map_err(|e| AppError::Internal(e.to_string()))?;
        if binaries.iter().any(|(path, _)| path == to) {
            return Err(AppError::Conflict("exists".into()));
        }
        let meta = binaries
            .into_iter()
            .find(|(path, _)| path == from)
            .map(|(_, meta)| meta)
            .ok_or(AppError::NotFound)?;
        let update = build_map_rename_update(current, "binaries", from, to, meta)?;
        Ok((update, ()))
    })
    .await
}

pub async fn index_set_structured(
    state: &AppState,
    vault_id: &str,
    path: &str,
    guid: &str,
    kind: &str,
) -> AppResult<()> {
    modify(state, vault_id, |current| {
        let occupied = decode_structured_index(current)
            .map_err(|e| AppError::Internal(e.to_string()))?
            .into_iter()
            .any(|entry| entry.path == path && entry.guid != guid);
        if occupied {
            return Err(AppError::Conflict("structured file already exists".into()));
        }
        let metadata = HashMap::from([
            ("guid".to_string(), Any::String(guid.into())),
            ("kind".to_string(), Any::String(kind.into())),
        ]);
        let update = build_map_set_update(current, "structured", path, metadata)?;
        Ok((update, ()))
    })
    .await
}

pub async fn index_remove_structured(
    state: &AppState,
    vault_id: &str,
    path: &str,
) -> AppResult<()> {
    let (epoch, current) = read_update_for_write(state, vault_id).await?;
    let entry_meta = decode_structured_index(&current)
        .map_err(|e| AppError::Internal(e.to_string()))?
        .into_iter()
        .find(|entry| entry.path == path);
    let update = match entry_meta {
        Some(meta) => {
            let mut entry = trash_entry_common(path, &meta.kind);
            entry.insert("guid".to_string(), Any::String(meta.guid.as_str().into()));
            build_trash_and_remove_update(&current, "structured", path, entry)?
        }
        None => build_map_remove_update(&current, "structured", path)?,
    };
    if !update.is_empty() {
        write_update_at_epoch(state, vault_id, epoch, update).await?;
    }
    Ok(())
}

pub async fn index_rename_structured(
    state: &AppState,
    vault_id: &str,
    from: &str,
    to: &str,
) -> AppResult<()> {
    modify(state, vault_id, |current| {
        let entries =
            decode_structured_index(current).map_err(|e| AppError::Internal(e.to_string()))?;
        if entries.iter().any(|entry| entry.path == to) {
            return Err(AppError::Conflict("exists".into()));
        }
        let entry = entries
            .into_iter()
            .find(|entry| entry.path == from)
            .ok_or(AppError::NotFound)?;
        let metadata = HashMap::from([
            ("guid".to_string(), Any::String(entry.guid.into())),
            ("kind".to_string(), Any::String(entry.kind.into())),
        ]);
        let update = build_map_rename_update(current, "structured", from, to, metadata)?;
        Ok((update, ()))
    })
    .await
}

/// One index-doc mutation in a rollback batch.
#[derive(Clone, Debug)]
pub enum IndexOp {
    SetFile {
        path: String,
        guid: String,
    },
    RemoveFile {
        path: String,
    },
    SetBinary {
        path: String,
        hash: String,
        size: i64,
    },
    RemoveBinary {
        path: String,
    },
    SetStructured {
        path: String,
        guid: String,
        kind: String,
    },
    RemoveStructured {
        path: String,
    },
}

/// Build a single update applying every op in one transaction, including the
/// trash entries that the per-op helpers above would write (so batched deletes
/// stay recoverable). Removes that target a missing entry are no-ops.
pub fn build_index_batch_update(current_update: &[u8], ops: &[IndexOp]) -> AppResult<Vec<u8>> {
    let files = decode_files_map(current_update).map_err(|e| AppError::Internal(e.to_string()))?;
    let binaries =
        decode_binaries_map(current_update).map_err(|e| AppError::Internal(e.to_string()))?;
    let structured =
        decode_structured_index(current_update).map_err(|e| AppError::Internal(e.to_string()))?;

    let doc = doc_from_update(current_update)?;
    let before = doc.transact().state_vector();
    let files_map = doc.get_or_insert_map("files");
    let binaries_map = doc.get_or_insert_map("binaries");
    let structured_map = doc.get_or_insert_map("structured");
    let trash = doc.get_or_insert_map("trash");
    {
        let mut txn = doc.transact_mut();
        let add_trash = |txn: &mut yrs::TransactionMut, entry: HashMap<String, Any>| {
            trash.insert(txn, uuid::Uuid::new_v4().to_string(), entry);
        };
        for op in ops {
            match op {
                IndexOp::SetFile { path, guid } => {
                    files_map.insert(&mut txn, path.to_string(), guid.to_string());
                }
                IndexOp::RemoveFile { path } => {
                    if let Some((_, guid)) = files.iter().find(|(p, _)| p == path) {
                        let mut entry = trash_entry_common(path, "text");
                        entry.insert("guid".to_string(), Any::String(guid.as_str().into()));
                        add_trash(&mut txn, entry);
                    }
                    files_map.remove(&mut txn, path);
                }
                IndexOp::SetBinary { path, hash, size } => {
                    let metadata = HashMap::from([
                        ("hash".to_string(), Any::String(hash.as_str().into())),
                        ("size".to_string(), Any::BigInt(*size)),
                    ]);
                    binaries_map.insert(&mut txn, path.to_string(), metadata);
                }
                IndexOp::RemoveBinary { path } => {
                    if let Some((hash, size)) = binaries
                        .iter()
                        .find(|(p, _)| p == path)
                        .and_then(|(_, meta)| binary_hash_size(meta))
                    {
                        let mut entry = trash_entry_common(path, "binary");
                        entry.insert("hash".to_string(), Any::String(hash.into()));
                        entry.insert("size".to_string(), Any::BigInt(size));
                        add_trash(&mut txn, entry);
                    }
                    binaries_map.remove(&mut txn, path);
                }
                IndexOp::SetStructured { path, guid, kind } => {
                    let metadata = HashMap::from([
                        ("guid".to_string(), Any::String(guid.as_str().into())),
                        ("kind".to_string(), Any::String(kind.as_str().into())),
                    ]);
                    structured_map.insert(&mut txn, path.to_string(), metadata);
                }
                IndexOp::RemoveStructured { path } => {
                    if let Some(meta) = structured.iter().find(|e| &e.path == path) {
                        let mut entry = trash_entry_common(path, &meta.kind);
                        entry.insert("guid".to_string(), Any::String(meta.guid.as_str().into()));
                        add_trash(&mut txn, entry);
                    }
                    structured_map.remove(&mut txn, path);
                }
            }
        }
    }
    let update = doc.transact().encode_state_as_update_v1(&before);
    Ok(update)
}

/// Apply a batch of index ops in one read/write round trip, then mirror the
/// `vault_files` DB registry the same way the per-op helpers do.
pub async fn index_apply_batch(state: &AppState, vault_id: &str, ops: &[IndexOp]) -> AppResult<()> {
    if ops.is_empty() {
        return Ok(());
    }
    let (epoch, current) = read_update_for_write(state, vault_id).await?;
    let files = decode_files_map(&current).map_err(|e| AppError::Internal(e.to_string()))?;
    let update = build_index_batch_update(&current, ops)?;
    if !update.is_empty() {
        write_update_at_epoch(state, vault_id, epoch, update).await?;
    }
    for op in ops {
        match op {
            IndexOp::SetFile { path, guid } => {
                upsert_vault_file(state, vault_id, path, guid).await?;
            }
            IndexOp::RemoveFile { path } => {
                if let Some((_, guid)) = files.iter().find(|(p, _)| p == path) {
                    vault_files::Entity::delete_many()
                        .filter(vault_files::Column::VaultId.eq(vault_id))
                        .filter(vault_files::Column::Guid.eq(guid))
                        .exec(&state.db)
                        .await?;
                }
            }
            _ => {}
        }
    }
    Ok(())
}

pub fn build_structured_update(current_update: &[u8], value: &JsonValue) -> AppResult<Vec<u8>> {
    if decode_structured(current_update).map_err(|e| AppError::Internal(e.to_string()))? == *value {
        return Ok(Vec::new());
    }
    let doc = doc_from_update(current_update)?;
    let before = doc.transact().state_vector();
    let root = doc.get_or_insert_map("root");
    {
        let mut txn = doc.transact_mut();
        reconcile_map(&mut txn, &root, value.as_object());
    }
    let update = doc.transact().encode_state_as_update_v1(&before);
    Ok(update)
}

fn reconcile_map(
    txn: &mut yrs::TransactionMut,
    map: &yrs::MapRef,
    value: Option<&JsonMap<String, JsonValue>>,
) {
    let empty = JsonMap::new();
    let value = value.unwrap_or(&empty);
    let keys: Vec<String> = map.keys(txn).map(ToString::to_string).collect();
    for key in keys {
        if !value.contains_key(&key) {
            map.remove(txn, &key);
        }
    }
    for (key, next) in value {
        reconcile_map_value(txn, map, key, next);
    }
}

fn reconcile_array(txn: &mut yrs::TransactionMut, array: &yrs::ArrayRef, value: &[JsonValue]) {
    let mut i = 0;
    while i < value.len() {
        reconcile_array_value(txn, array, i as u32, &value[i]);
        i += 1;
    }
    let len = array.len(txn);
    if len > value.len() as u32 {
        array.remove_range(txn, value.len() as u32, len - value.len() as u32);
    }
}

fn reconcile_map_value(
    txn: &mut yrs::TransactionMut,
    map: &yrs::MapRef,
    key: &str,
    next: &JsonValue,
) {
    if let Some(values) = next.as_array() {
        match map.get(txn, key) {
            Some(Out::YArray(array)) => reconcile_array(txn, &array, values),
            _ => {
                map.insert(txn, key.to_string(), ArrayPrelim::default());
                if let Some(Out::YArray(array)) = map.get(txn, key) {
                    reconcile_array(txn, &array, values);
                }
            }
        }
    } else if let Some(object) = next.as_object() {
        match map.get(txn, key) {
            Some(Out::YMap(child)) => reconcile_map(txn, &child, Some(object)),
            _ => {
                map.insert(txn, key.to_string(), MapPrelim::default());
                if let Some(Out::YMap(child)) = map.get(txn, key) {
                    reconcile_map(txn, &child, Some(object));
                }
            }
        }
    } else if should_use_text(Some(key), next) {
        let text = next.as_str().unwrap_or_default();
        match map.get(txn, key) {
            Some(Out::YText(ytext)) => apply_string_to_ytext(txn, &ytext, text),
            _ => {
                map.insert(txn, key.to_string(), TextPrelim::new(""));
                if let Some(Out::YText(ytext)) = map.get(txn, key) {
                    apply_string_to_ytext(txn, &ytext, text);
                }
            }
        }
    } else {
        let next_any = json_to_any(next);
        if map.get(txn, key).is_some_and(|current| match current {
            Out::Any(current) => any_to_json(&current) == any_to_json(&next_any),
            _ => false,
        }) {
            return;
        }
        map.insert(txn, key.to_string(), next_any);
    }
}

fn reconcile_array_value(
    txn: &mut yrs::TransactionMut,
    array: &yrs::ArrayRef,
    index: u32,
    next: &JsonValue,
) {
    if let Some(values) = next.as_array() {
        match array.get(txn, index) {
            Some(Out::YArray(child)) => reconcile_array(txn, &child, values),
            _ => {
                if index < array.len(txn) {
                    array.remove_range(txn, index, 1);
                }
                array.insert(txn, index, ArrayPrelim::default());
            }
        }
        if let Some(Out::YArray(child)) = array.get(txn, index) {
            reconcile_array(txn, &child, values);
        }
    } else if let Some(object) = next.as_object() {
        match array.get(txn, index) {
            Some(Out::YMap(child)) => reconcile_map(txn, &child, Some(object)),
            _ => {
                if index < array.len(txn) {
                    array.remove_range(txn, index, 1);
                }
                array.insert(txn, index, MapPrelim::default());
            }
        }
        if let Some(Out::YMap(child)) = array.get(txn, index) {
            reconcile_map(txn, &child, Some(object));
        }
    } else if should_use_text(None, next) {
        let text = next.as_str().unwrap_or_default();
        match array.get(txn, index) {
            Some(Out::YText(ytext)) => apply_string_to_ytext(txn, &ytext, text),
            _ => {
                if index < array.len(txn) {
                    array.remove_range(txn, index, 1);
                }
                array.insert(txn, index, TextPrelim::new(""));
            }
        }
        if let Some(Out::YText(ytext)) = array.get(txn, index) {
            apply_string_to_ytext(txn, &ytext, text);
        }
    } else {
        let next_any = json_to_any(next);
        if array.get(txn, index).is_some_and(|current| match current {
            Out::Any(current) => any_to_json(&current) == any_to_json(&next_any),
            _ => false,
        }) {
            return;
        }
        if index < array.len(txn) {
            array.remove_range(txn, index, 1);
        }
        array.insert(txn, index, next_any);
    }
}

fn should_use_text(key: Option<&str>, value: &JsonValue) -> bool {
    let Some(value) = value.as_str() else {
        return false;
    };
    key.is_some_and(|key| {
        matches!(
            key,
            "text" | "label" | "filter" | "formula" | "query" | "name" | "description"
        )
    }) || value.len() > 256
}

/// Replace only the changed middle of `ytext`. Server documents use yrs'
/// default byte offsets, and `common_affixes` returns byte offsets on
/// character boundaries, so the splice never splits a character. Replacing
/// the whole text instead would move concurrent insertions to the start of
/// the document and undo concurrent deletions.
fn apply_string_to_ytext(txn: &mut yrs::TransactionMut, ytext: &yrs::TextRef, next: &str) {
    let old = ytext.get_string(txn);
    if old == next {
        return;
    }
    let (prefix, old_suffix, new_suffix) = common_affixes(&old, next);
    let delete_len = (old_suffix - prefix) as u32;
    if delete_len > 0 {
        ytext.remove_range(txn, prefix as u32, delete_len);
    }
    let insert = &next[prefix..new_suffix];
    if !insert.is_empty() {
        ytext.insert(txn, prefix as u32, insert);
    }
}

pub(crate) fn json_to_any(value: &JsonValue) -> Any {
    match value {
        JsonValue::Null => Any::Null,
        JsonValue::Bool(v) => Any::Bool(*v),
        JsonValue::Number(v) => v
            .as_i64()
            .map(Any::BigInt)
            .unwrap_or_else(|| Any::Number(v.as_f64().unwrap_or(0.0))),
        JsonValue::String(v) => Any::String(v.as_str().into()),
        JsonValue::Array(values) => Any::Array(values.iter().map(json_to_any).collect()),
        JsonValue::Object(values) => Any::Map(
            values
                .iter()
                .map(|(k, v)| (k.clone(), json_to_any(v)))
                .collect::<HashMap<_, _>>()
                .into(),
        ),
    }
}

fn build_set_text_update(
    current_update: &[u8],
    name: &str,
    new_content: &str,
) -> AppResult<Vec<u8>> {
    let doc = doc_from_update(current_update)?;
    let before = doc.transact().state_vector();
    let text = doc.get_or_insert_text(name);
    let old_content = {
        let txn = doc.transact();
        text.get_string(&txn)
    };
    if old_content == new_content {
        return Ok(Vec::new());
    }
    {
        let mut txn = doc.transact_mut();
        apply_string_to_ytext(&mut txn, &text, new_content);
    }
    let update = doc.transact().encode_state_as_update_v1(&before);
    Ok(update)
}

fn build_map_set_update<V>(
    current_update: &[u8],
    map_name: &str,
    key: &str,
    value: V,
) -> AppResult<Vec<u8>>
where
    V: Into<Any>,
{
    let doc = doc_from_update(current_update)?;
    let before = doc.transact().state_vector();
    let map = doc.get_or_insert_map(map_name);
    {
        let mut txn = doc.transact_mut();
        map.insert(&mut txn, key.to_string(), value);
    }
    let update = doc.transact().encode_state_as_update_v1(&before);
    Ok(update)
}

/// Base fields shared by every trash entry written into the index doc's `trash`
/// map: the original path, the document kind, and the deletion time.
fn trash_entry_common(path: &str, kind: &str) -> HashMap<String, Any> {
    HashMap::from([
        ("path".to_string(), Any::String(path.into())),
        ("kind".to_string(), Any::String(kind.into())),
        ("deletedAt".to_string(), Any::BigInt(now_millis())),
    ])
}

/// Extract `(hash, size)` from a `binaries` map value (`{ hash, size }`).
fn binary_hash_size(value: &Any) -> Option<(String, i64)> {
    let Any::Map(meta) = value else { return None };
    let Some(Any::String(hash)) = meta.get("hash") else {
        return None;
    };
    let size = match meta.get("size") {
        Some(Any::BigInt(v)) => *v,
        Some(Any::Number(v)) => *v as i64,
        _ => 0,
    };
    Some((hash.to_string(), size))
}

/// In a single update, record a `trash` entry (keyed by a fresh uuid) and remove
/// the live entry from `live_map`. Keeping deletes recoverable: a later restore
/// just re-adds the live entry from the retained guid/hash.
fn build_trash_and_remove_update(
    current_update: &[u8],
    live_map: &str,
    key: &str,
    entry: HashMap<String, Any>,
) -> AppResult<Vec<u8>> {
    let doc = doc_from_update(current_update)?;
    let before = doc.transact().state_vector();
    let trash = doc.get_or_insert_map("trash");
    let live = doc.get_or_insert_map(live_map);
    {
        let mut txn = doc.transact_mut();
        trash.insert(&mut txn, uuid::Uuid::new_v4().to_string(), entry);
        live.remove(&mut txn, key);
    }
    let update = doc.transact().encode_state_as_update_v1(&before);
    Ok(update)
}

fn build_map_remove_update(current_update: &[u8], map_name: &str, key: &str) -> AppResult<Vec<u8>> {
    let doc = doc_from_update(current_update)?;
    let before = doc.transact().state_vector();
    let map = doc.get_or_insert_map(map_name);
    {
        let mut txn = doc.transact_mut();
        map.remove(&mut txn, key);
    }
    let update = doc.transact().encode_state_as_update_v1(&before);
    Ok(update)
}

fn build_map_rename_update<V>(
    current_update: &[u8],
    map_name: &str,
    from: &str,
    to: &str,
    value: V,
) -> AppResult<Vec<u8>>
where
    V: Into<Any>,
{
    let doc = doc_from_update(current_update)?;
    let before = doc.transact().state_vector();
    let map = doc.get_or_insert_map(map_name);
    {
        let mut txn = doc.transact_mut();
        map.remove(&mut txn, from);
        map.insert(&mut txn, to.to_string(), value);
    }
    let update = doc.transact().encode_state_as_update_v1(&before);
    Ok(update)
}

fn doc_from_update(update: &[u8]) -> AppResult<Doc> {
    let doc = Doc::new();
    let update = crate::safe_yrs::decode_v1::<Update>(update)
        .map_err(|e| AppError::Internal(format!("decode update: {e:?}")))?;
    {
        let mut txn = doc.transact_mut();
        txn.apply_update(update);
    }
    Ok(doc)
}

fn common_affixes(old: &str, new: &str) -> (usize, usize, usize) {
    let mut prefix = 0;
    for ((old_idx, old_ch), (new_idx, new_ch)) in old.char_indices().zip(new.char_indices()) {
        if old_ch != new_ch {
            break;
        }
        prefix = old_idx + old_ch.len_utf8();
        debug_assert_eq!(prefix, new_idx + new_ch.len_utf8());
    }

    let mut old_suffix = old.len();
    let mut new_suffix = new.len();
    while old_suffix > prefix && new_suffix > prefix {
        let old_ch = old[..old_suffix].chars().next_back().unwrap();
        let new_ch = new[..new_suffix].chars().next_back().unwrap();
        if old_ch != new_ch {
            break;
        }
        old_suffix -= old_ch.len_utf8();
        new_suffix -= new_ch.len_utf8();
    }
    (prefix, old_suffix, new_suffix)
}

pub(crate) async fn upsert_vault_file(
    state: &AppState,
    vault_id: &str,
    path: &str,
    guid: &str,
) -> AppResult<()> {
    // Atomic upsert keyed on the unique (vault_id, guid) index. A find-then-
    // insert here races: rapid file events for the same guid (e.g. delete →
    // restore → restore-to-new-path) fire concurrent calls that all see "no
    // row" and then collide on INSERT with
    // `UNIQUE constraint failed: vault_files.vault_id, vault_files.guid`.
    vault_files::Entity::insert(vault_files::ActiveModel {
        id: Set(uuid::Uuid::new_v4().to_string()),
        vault_id: Set(vault_id.to_string()),
        guid: Set(guid.to_string()),
        path: Set(path.to_string()),
        updated_at: Set(now_millis()),
    })
    .on_conflict(
        OnConflict::columns([vault_files::Column::VaultId, vault_files::Column::Guid])
            .update_columns([vault_files::Column::Path, vault_files::Column::UpdatedAt])
            .to_owned(),
    )
    .exec(&state.db)
    .await?;
    Ok(())
}

/// Decode a named root `Y.Text` from a full-state update into a String.
pub fn decode_text(update: &[u8], name: &str) -> Result<String> {
    let doc = Doc::new();
    let update = crate::safe_yrs::decode_v1::<Update>(update)
        .map_err(|e| anyhow!("decode update: {e:?}"))?;
    {
        let mut txn = doc.transact_mut();
        txn.apply_update(update);
    }
    let text = doc.get_or_insert_text(name);
    let txn = doc.transact();
    Ok(text.get_string(&txn))
}

/// Decode the vault index doc's `files` map (path -> guid) from a full-state update.
pub fn decode_files_map(update: &[u8]) -> Result<Vec<(String, String)>> {
    decode_string_map(update, "files")
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StructuredIndexEntry {
    pub path: String,
    pub guid: String,
    pub kind: String,
}

/// Decode the vault index doc's `structured` map (path -> { guid, kind }).
pub fn decode_structured_index(update: &[u8]) -> Result<Vec<StructuredIndexEntry>> {
    let doc = Doc::new();
    let update =
        crate::safe_yrs::decode_v1::<Update>(update).map_err(|e| anyhow!("decode index: {e:?}"))?;
    {
        let mut txn = doc.transact_mut();
        txn.apply_update(update);
    }
    let map = doc.get_or_insert_map("structured");
    let txn = doc.transact();
    let mut out = Vec::new();
    if let Any::Map(entries) = map.to_json(&txn) {
        for (path, value) in entries.iter() {
            let Any::Map(meta) = value else { continue };
            let Some(Any::String(guid)) = meta.get("guid") else {
                continue;
            };
            let Some(Any::String(kind)) = meta.get("kind") else {
                continue;
            };
            out.push(StructuredIndexEntry {
                path: path.clone(),
                guid: guid.to_string(),
                kind: kind.to_string(),
            });
        }
    }
    Ok(out)
}

/// Decode a structured document rooted at `Y.Map("root")` into JSON.
pub fn decode_structured(update: &[u8]) -> Result<JsonValue> {
    let doc = Doc::new();
    let update = crate::safe_yrs::decode_v1::<Update>(update)
        .map_err(|e| anyhow!("decode update: {e:?}"))?;
    {
        let mut txn = doc.transact_mut();
        txn.apply_update(update);
    }
    let root = doc.get_or_insert_map("root");
    let txn = doc.transact();
    Ok(any_to_json(&root.to_json(&txn)))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinaryEntry {
    pub path: String,
    /// Lowercase hex sha256 of the attachment bytes (blob store key).
    pub hash: String,
    pub size: u64,
}

/// Decode the vault index doc's `binaries` map into typed entries, skipping
/// any entry whose metadata is malformed (missing/mistyped hash or size).
pub fn decode_binaries_entries(update: &[u8]) -> Result<Vec<BinaryEntry>> {
    decode_hash_entries(update, "binaries")
}

/// Decode the vault index doc's `configFiles` map (Obsidian config folder
/// files synced via the blob store) into typed entries, skipping any entry
/// whose metadata is malformed. Same shape as `binaries` entries.
pub fn decode_config_entries(update: &[u8]) -> Result<Vec<BinaryEntry>> {
    decode_hash_entries(update, "configFiles")
}

/// Decode a `path -> { hash, size, ... }` index map into typed entries.
fn decode_hash_entries(update: &[u8], map_name: &str) -> Result<Vec<BinaryEntry>> {
    let mut out = Vec::new();
    for (path, value) in decode_any_map(update, map_name)? {
        let Any::Map(meta) = value else { continue };
        let Some(Any::String(hash)) = meta.get("hash") else {
            continue;
        };
        let size = match meta.get("size") {
            Some(Any::Number(value)) if *value >= 0.0 => *value as u64,
            Some(Any::BigInt(value)) if *value >= 0 => *value as u64,
            _ => continue,
        };
        out.push(BinaryEntry {
            path,
            hash: hash.to_string(),
            size,
        });
    }
    Ok(out)
}

/// Decode the vault index doc's `binaries` map (path -> JSON metadata) from a full-state update.
pub fn decode_binaries_map(update: &[u8]) -> Result<Vec<(String, Any)>> {
    decode_any_map(update, "binaries")
}

/// Decode an index-doc map (path -> JSON metadata) from a full-state update.
fn decode_any_map(update: &[u8], map_name: &str) -> Result<Vec<(String, Any)>> {
    let doc = Doc::new();
    let update =
        crate::safe_yrs::decode_v1::<Update>(update).map_err(|e| anyhow!("decode index: {e:?}"))?;
    {
        let mut txn = doc.transact_mut();
        txn.apply_update(update);
    }
    let map = doc.get_or_insert_map(map_name);
    let txn = doc.transact();
    let mut out = Vec::new();
    if let Any::Map(entries) = map.to_json(&txn) {
        for (path, value) in entries.iter() {
            out.push((path.clone(), value.clone()));
        }
    }
    Ok(out)
}

fn decode_string_map(update: &[u8], name: &str) -> Result<Vec<(String, String)>> {
    let doc = Doc::new();
    let update =
        crate::safe_yrs::decode_v1::<Update>(update).map_err(|e| anyhow!("decode index: {e:?}"))?;
    {
        let mut txn = doc.transact_mut();
        txn.apply_update(update);
    }
    let map = doc.get_or_insert_map(name);
    let txn = doc.transact();
    let mut out = Vec::new();
    if let Any::Map(entries) = map.to_json(&txn) {
        for (path, value) in entries.iter() {
            if let Any::String(guid) = value {
                out.push((path.clone(), guid.to_string()));
            }
        }
    }
    Ok(out)
}

pub(crate) fn any_to_json(value: &Any) -> JsonValue {
    match value {
        Any::Null | Any::Undefined => JsonValue::Null,
        Any::Bool(v) => JsonValue::Bool(*v),
        Any::Number(v) => JsonNumber::from_f64(*v)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        Any::BigInt(v) => JsonValue::Number(JsonNumber::from(*v)),
        Any::String(v) => JsonValue::String(v.to_string()),
        Any::Buffer(v) => JsonValue::Array(
            v.iter()
                .map(|b| JsonValue::Number(JsonNumber::from(*b)))
                .collect(),
        ),
        Any::Array(values) => JsonValue::Array(values.iter().map(any_to_json).collect()),
        Any::Map(entries) => {
            let mut out = JsonMap::new();
            for (key, child) in entries.iter() {
                out.insert(key.clone(), any_to_json(child));
            }
            JsonValue::Object(out)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use yrs::{Map, Text};

    fn text_update(name: &str, value: &str) -> Vec<u8> {
        let doc = Doc::new();
        let text = doc.get_or_insert_text(name);
        {
            let mut txn = doc.transact_mut();
            text.insert(&mut txn, 0, value);
        }
        let update = doc
            .transact()
            .encode_state_as_update_v1(&yrs::StateVector::default());
        update
    }

    fn files_update(entries: &[(&str, &str)]) -> Vec<u8> {
        let doc = Doc::new();
        let map = doc.get_or_insert_map("files");
        {
            let mut txn = doc.transact_mut();
            for (path, guid) in entries {
                map.insert(&mut txn, path.to_string(), guid.to_string());
            }
        }
        let update = doc
            .transact()
            .encode_state_as_update_v1(&yrs::StateVector::default());
        update
    }

    fn structured_index_update(entries: &[(&str, &str, &str)]) -> Vec<u8> {
        let doc = Doc::new();
        let map = doc.get_or_insert_map("structured");
        {
            let mut txn = doc.transact_mut();
            for (path, guid, kind) in entries {
                let meta = HashMap::from([
                    ("guid".to_string(), Any::String((*guid).into())),
                    ("kind".to_string(), Any::String((*kind).into())),
                ]);
                map.insert(&mut txn, path.to_string(), meta);
            }
        }
        let update = doc
            .transact()
            .encode_state_as_update_v1(&yrs::StateVector::default());
        update
    }

    fn structured_doc_update() -> Vec<u8> {
        let doc = Doc::new();
        let root = doc.get_or_insert_map("root");
        {
            let mut txn = doc.transact_mut();
            root.insert(&mut txn, "title".to_string(), "Inbox".to_string());
            root.insert(&mut txn, "count".to_string(), 2.0);
        }
        let update = doc
            .transact()
            .encode_state_as_update_v1(&yrs::StateVector::default());
        update
    }

    fn apply_update(base: &[u8], update: &[u8]) -> Vec<u8> {
        let doc = doc_from_update(base).unwrap();
        let update = crate::safe_yrs::decode_v1::<Update>(update).unwrap();
        {
            let mut txn = doc.transact_mut();
            txn.apply_update(update);
        }
        let out = doc
            .transact()
            .encode_state_as_update_v1(&yrs::StateVector::default());
        out
    }

    #[test]
    fn decode_text_roundtrips() {
        let update = text_update("contents", "# Hello\nworld");
        assert_eq!(decode_text(&update, "contents").unwrap(), "# Hello\nworld");
    }

    #[test]
    fn decode_files_map_roundtrips() {
        let update = files_update(&[("a.md", "guid-a"), ("dir/b.md", "guid-b")]);
        let mut got = decode_files_map(&update).unwrap();
        got.sort();
        assert_eq!(
            got,
            vec![
                ("a.md".to_string(), "guid-a".to_string()),
                ("dir/b.md".to_string(), "guid-b".to_string()),
            ]
        );
    }

    #[test]
    fn trash_and_remove_drops_live_entry_and_records_trash() {
        let base = files_update(&[("a.md", "guid-a"), ("b.md", "guid-b")]);
        let mut entry = trash_entry_common("a.md", "text");
        entry.insert("guid".to_string(), Any::String("guid-a".into()));
        let update = build_trash_and_remove_update(&base, "files", "a.md", entry).unwrap();
        let merged = apply_update(&base, &update);

        // The live files map no longer contains the removed entry.
        let files = decode_files_map(&merged).unwrap();
        assert_eq!(files, vec![("b.md".to_string(), "guid-b".to_string())]);

        // Exactly one trash entry was recorded, referencing the removed file.
        let doc = doc_from_update(&merged).unwrap();
        let trash = doc.get_or_insert_map("trash");
        let txn = doc.transact();
        let Any::Map(entries) = trash.to_json(&txn) else {
            panic!("trash map missing");
        };
        assert_eq!(entries.len(), 1);
        let Some((_, Any::Map(value))) = entries.iter().next() else {
            panic!("trash entry not a map");
        };
        assert_eq!(value.get("path"), Some(&Any::String("a.md".into())));
        assert_eq!(value.get("kind"), Some(&Any::String("text".into())));
        assert_eq!(value.get("guid"), Some(&Any::String("guid-a".into())));
        assert!(matches!(value.get("deletedAt"), Some(Any::BigInt(_))));
    }

    #[test]
    fn binary_hash_size_extracts_fields() {
        let meta = Any::Map(
            HashMap::from([
                ("hash".to_string(), Any::String("abc".into())),
                ("size".to_string(), Any::BigInt(42)),
            ])
            .into(),
        );
        assert_eq!(binary_hash_size(&meta), Some(("abc".to_string(), 42)));
        assert_eq!(binary_hash_size(&Any::String("x".into())), None);
    }

    #[test]
    fn decode_structured_index_roundtrips() {
        let update = structured_index_update(&[("board.canvas", "guid-c", "canvas")]);
        assert_eq!(
            decode_structured_index(&update).unwrap(),
            vec![StructuredIndexEntry {
                path: "board.canvas".into(),
                guid: "guid-c".into(),
                kind: "canvas".into(),
            }]
        );
    }

    #[test]
    fn decode_structured_roundtrips_root_map() {
        let update = structured_doc_update();
        assert_eq!(
            decode_structured(&update).unwrap(),
            serde_json::json!({ "title": "Inbox", "count": 2.0 })
        );
    }

    #[test]
    fn build_set_text_update_replaces_middle_only() {
        let base = text_update("contents", "hello brave world");
        let diff = build_set_text_update(&base, "contents", "hello bold world").unwrap();
        let merged = apply_update(&base, &diff);
        assert_eq!(
            decode_text(&merged, "contents").unwrap(),
            "hello bold world"
        );
    }

    /// A client edit concurrent with a server text edit, both made on `base`.
    fn concurrent_client_edit(
        base: &[u8],
        edit: impl FnOnce(&yrs::TextRef, &mut yrs::TransactionMut),
    ) -> Vec<u8> {
        let doc = doc_from_update(base).unwrap();
        let before = doc.transact().state_vector();
        let text = doc.get_or_insert_text("contents");
        {
            let mut txn = doc.transact_mut();
            edit(&text, &mut txn);
        }
        let update = doc.transact().encode_state_as_update_v1(&before);
        update
    }

    #[test]
    fn set_text_on_non_ascii_note_keeps_concurrent_edits_in_place() {
        let original = "Café notes\nline two\nline three\n";
        let base = text_update("contents", original);
        let server =
            build_set_text_update(&base, "contents", "Café notes\nline two\nline THREE\n").unwrap();
        let client = concurrent_client_edit(&base, |text, txn| {
            let two = original.find("two").unwrap() as u32;
            text.remove_range(txn, two, 3);
            text.insert(txn, "Café notes".len() as u32, " (edited)");
        });
        let merged = apply_update(&apply_update(&base, &server), &client);
        assert_eq!(
            decode_text(&merged, "contents").unwrap(),
            "Café notes (edited)\nline \nline THREE\n"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn update_text_keeps_a_client_edit_that_races_it() {
        let root = std::env::temp_dir().join(format!("realtime-ydoc-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&root).await.unwrap();
        let mut config = crate::config::Config::test_default();
        config.database_url = format!("sqlite://{}?mode=rwc", root.join("state.db").display());
        config.crdt_store_dir = root.join("crdt").to_string_lossy().into_owned();
        config.blob_dir = root.join("blobs").to_string_lossy().into_owned();
        config.git_data_dir = root.join("git").to_string_lossy().into_owned();
        config.git_enabled = false;
        let state = crate::build_state(config).await.unwrap();
        let doc_id = "vault__note";
        state
            .documents
            .apply_update(doc_id, &text_update("contents", "Café\nbeta\ngamma\n"))
            .await
            .unwrap();
        let base = state.documents.read_update(doc_id).await.unwrap();
        let concurrent = concurrent_client_edit(&base, |text, txn| {
            text.insert(txn, "Café".len() as u32, " (typed meanwhile)");
        });

        let documents = state.documents.clone();
        let mut racing = None;
        let (before, after) = update_text(&state, doc_id, |content| {
            // A client edit arrives while the new text is being computed. It
            // cannot land before this write; it merges after it instead.
            racing = Some(tokio::spawn(async move {
                documents.apply_update(doc_id, &concurrent).await
            }));
            Ok(content.replace("gamma", "GAMMA"))
        })
        .await
        .unwrap();
        racing.unwrap().await.unwrap().unwrap();

        assert_eq!(before, "Café\nbeta\ngamma\n");
        assert_eq!(after, "Café\nbeta\nGAMMA\n");
        let current = state.documents.read_update(doc_id).await.unwrap();
        assert_eq!(
            decode_text(&current, "contents").unwrap(),
            "Café (typed meanwhile)\nbeta\nGAMMA\n"
        );
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[test]
    fn build_set_text_update_handles_unicode_boundaries() {
        let base = text_update("contents", "hi 🦀 world");
        let diff = build_set_text_update(&base, "contents", "hi 🦀 vault").unwrap();
        let merged = apply_update(&base, &diff);
        assert_eq!(decode_text(&merged, "contents").unwrap(), "hi 🦀 vault");
    }

    #[test]
    fn build_map_rename_update_moves_file_entry() {
        let base = files_update(&[("a.md", "guid-a")]);
        let diff =
            build_map_rename_update(&base, "files", "a.md", "b.md", "guid-a".to_string()).unwrap();
        let merged = apply_update(&base, &diff);
        assert_eq!(
            decode_files_map(&merged).unwrap(),
            vec![("b.md".into(), "guid-a".into())]
        );
    }

    #[test]
    fn build_index_batch_update_applies_all_ops_in_one_update() {
        let base = files_update(&[("old.md", "guid-old"), ("keep.md", "guid-keep")]);
        let ops = vec![
            IndexOp::SetFile {
                path: "new.md".into(),
                guid: "guid-new".into(),
            },
            IndexOp::RemoveFile {
                path: "old.md".into(),
            },
            IndexOp::SetBinary {
                path: "img/a.png".into(),
                hash: "a".repeat(64),
                size: 9,
            },
            IndexOp::SetStructured {
                path: "b.canvas".into(),
                guid: "guid-c".into(),
                kind: "canvas".into(),
            },
        ];
        let update = build_index_batch_update(&base, &ops).unwrap();
        let merged = apply_update(&base, &update);

        let mut files = decode_files_map(&merged).unwrap();
        files.sort();
        assert_eq!(
            files,
            vec![
                ("keep.md".to_string(), "guid-keep".to_string()),
                ("new.md".to_string(), "guid-new".to_string()),
            ]
        );
        let bins = decode_binaries_entries(&merged).unwrap();
        assert_eq!(bins.len(), 1);
        assert_eq!(bins[0].path, "img/a.png");
        let structured = decode_structured_index(&merged).unwrap();
        assert_eq!(structured.len(), 1);
        assert_eq!(structured[0].kind, "canvas");

        // The removed file landed in trash with its guid.
        let doc = doc_from_update(&merged).unwrap();
        let trash = doc.get_or_insert_map("trash");
        let txn = doc.transact();
        let Any::Map(entries) = trash.to_json(&txn) else {
            panic!("trash map missing");
        };
        assert_eq!(entries.len(), 1);
        let Some((_, Any::Map(value))) = entries.iter().next() else {
            panic!("trash entry not a map");
        };
        assert_eq!(value.get("guid"), Some(&Any::String("guid-old".into())));
    }

    #[test]
    fn build_structured_update_roundtrips_and_noops() {
        let base = text_update("unused", "");
        let value = serde_json::json!({ "nodes": { "a": { "id": "a", "text": "hello" } }, "nodeOrder": ["a"] });
        let diff = build_structured_update(&base, &value).unwrap();
        let merged = apply_update(&base, &diff);
        assert_eq!(decode_structured(&merged).unwrap(), value);
        assert!(build_structured_update(&merged, &value).unwrap().is_empty());
    }

    #[test]
    fn build_structured_update_promotes_text_keys_and_long_strings() {
        let base = text_update("unused", "");
        let long = "x".repeat(257);
        let value = serde_json::json!({ "label": "short", "plain": "short", "long": long });
        let merged = apply_update(&base, &build_structured_update(&base, &value).unwrap());
        let doc = doc_from_update(&merged).unwrap();
        let root = doc.get_or_insert_map("root");
        let txn = doc.transact();
        assert!(matches!(root.get(&txn, "label"), Some(Out::YText(_))));
        assert!(matches!(
            root.get(&txn, "plain"),
            Some(Out::Any(Any::String(_)))
        ));
        assert!(matches!(root.get(&txn, "long"), Some(Out::YText(_))));
    }

    #[test]
    fn build_structured_update_arrays_grow_shrink_and_delete_keys() {
        let base = text_update("unused", "");
        let first = serde_json::json!({ "items": [1, 2, 3], "gone": true });
        let second = serde_json::json!({ "items": [1, 4] });
        let merged = apply_update(&base, &build_structured_update(&base, &first).unwrap());
        let merged = apply_update(&merged, &build_structured_update(&merged, &second).unwrap());
        assert_eq!(decode_structured(&merged).unwrap(), second);
    }

    #[test]
    fn build_structured_update_handles_unicode_text_boundaries() {
        let base = text_update("unused", "");
        let first = serde_json::json!({ "text": "hi 🦀 world" });
        let second = serde_json::json!({ "text": "hi 🦀 vault" });
        let merged = apply_update(&base, &build_structured_update(&base, &first).unwrap());
        let merged = apply_update(&merged, &build_structured_update(&merged, &second).unwrap());
        assert_eq!(decode_structured(&merged).unwrap(), second);
    }
}
