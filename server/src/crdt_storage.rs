//! Crash-safe incremental persistence for native Yjs documents.
//!
//! Each document owns a directory containing a checksummed full-state snapshot,
//! an append-only checksummed update segment, and an atomically replaced
//! manifest. The manifest is the commit point for compaction: a crash before the
//! swap leaves the old generation authoritative; a crash after it leaves the new
//! generation authoritative.

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::ops::Bound;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;
use yrs::{Doc, ReadTxn, StateVector, Transact};
use yrs_kvstore::{DocOps, KVEntry, KVStore};

use crate::crdt::MAX_UPDATE_BYTES;

const FORMAT: &str = "realtime-crdt";
const FORMAT_VERSION: u32 = 3;
const LEGACY_FORMAT_VERSION: u32 = 2;
const SNAPSHOT_MAGIC: &[u8; 8] = b"RTCRDT02";
const LOG_MAGIC: &[u8; 8] = b"RTCRDL02";
const SNAPSHOT_HEADER_BYTES: usize = 8 + 8 + 32;
const RECORD_HEADER_BYTES: usize = 8 + 32;
const MANIFEST_FILE: &str = "manifest.json";
const MAX_LOG_BYTES: usize = 512 * 1024 * 1024;
pub(crate) const MAX_SNAPSHOT_BYTES: usize = 512 * 1024 * 1024;
pub(crate) const COMPACT_AFTER_RECORDS: u64 = 256;
pub(crate) const COMPACT_AFTER_BYTES: u64 = 4 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("document I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("corrupt persisted document {document_id}: {reason}")]
    Corrupt { document_id: String, reason: String },
    #[error(
        "document {document_id} update segment would exceed the {max_bytes}-byte replay limit"
    )]
    LogTooLarge { document_id: String, max_bytes: u64 },
    #[error("invalid legacy y-sweet store: {0}")]
    Legacy(String),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Manifest {
    format: String,
    version: u32,
    generation: u64,
    snapshot: String,
    updates: String,
    /// Updates already folded into this generation's snapshot.
    #[serde(default)]
    total_records: u64,
}

impl Manifest {
    fn new(generation: u64, total_records: u64) -> Self {
        Self {
            format: FORMAT.to_string(),
            version: FORMAT_VERSION,
            generation,
            snapshot: snapshot_name(generation),
            updates: log_name(generation),
            total_records,
        }
    }

    fn validate(&self, document_id: &str) -> Result<(), StorageError> {
        if self.format != FORMAT {
            return corrupt(document_id, format!("unknown format {:?}", self.format));
        }
        if self.version != FORMAT_VERSION && self.version != LEGACY_FORMAT_VERSION {
            return corrupt(
                document_id,
                format!(
                    "unsupported format version {}; expected {LEGACY_FORMAT_VERSION} or {FORMAT_VERSION}",
                    self.version
                ),
            );
        }
        if self.snapshot != snapshot_name(self.generation)
            || self.updates != log_name(self.generation)
        {
            return corrupt(
                document_id,
                "manifest file names do not match its generation",
            );
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct DocumentPersistence {
    directory: PathBuf,
    document_id: String,
    generation: u64,
    log_bytes: u64,
    records: u64,
    total_records: u64,
    recovered_tail: bool,
    append_failed: bool,
    max_log_bytes: u64,
}

impl DocumentPersistence {
    pub(crate) fn should_compact(&self) -> bool {
        self.records >= COMPACT_AFTER_RECORDS || self.log_bytes >= COMPACT_AFTER_BYTES
    }

    pub(crate) async fn append_update(&mut self, update: &[u8]) -> Result<(), StorageError> {
        if self.append_failed {
            // A prior append may have left part of a record behind. Cut the
            // segment back to its last durable length before appending again;
            // the document stays loaded (and writable) once that succeeds.
            if self.discard_partial_record().await.is_err() {
                return corrupt(
                    &self.document_id,
                    "a prior append failed and its partial record could not be removed",
                );
            }
            self.append_failed = false;
        }
        let record_len = self.record_len(update.len())?;
        if self
            .log_bytes
            .checked_add(record_len)
            .is_none_or(|projected| projected > self.max_log_bytes)
        {
            return Err(StorageError::LogTooLarge {
                document_id: self.document_id.clone(),
                max_bytes: self.max_log_bytes,
            });
        }
        let record_len = record_len as usize;
        let mut record = Vec::with_capacity(record_len);
        record.extend_from_slice(&(update.len() as u64).to_le_bytes());
        record.extend_from_slice(&checksum(update));
        record.extend_from_slice(update);

        let path = self.directory.join(log_name(self.generation));
        let result = async {
            let mut file = tokio::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .await?;
            file.write_all(&record).await?;
            file.sync_all().await
        }
        .await;
        if let Err(error) = result {
            // Remove whatever part of this unacknowledged record reached the
            // file so the next append cannot land behind a torn record. Only
            // if that fails does the segment refuse writes until it succeeds.
            if self.discard_partial_record().await.is_err() {
                self.append_failed = true;
            }
            return Err(StorageError::Io(error));
        }
        self.log_bytes = self.log_bytes.saturating_add(record.len() as u64);
        self.records = self.records.saturating_add(1);
        self.total_records = self.total_records.saturating_add(1);
        Ok(())
    }

    /// Truncate the update segment back to its last durable record boundary.
    async fn discard_partial_record(&self) -> Result<(), std::io::Error> {
        let path = self.directory.join(log_name(self.generation));
        let file = tokio::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .await?;
        file.set_len(self.log_bytes).await?;
        file.sync_all().await
    }

    pub(crate) fn requires_compaction(&self, update_bytes: usize) -> Result<bool, StorageError> {
        let record_len = self.record_len(update_bytes)?;
        Ok(self
            .log_bytes
            .checked_add(record_len)
            .is_none_or(|projected| projected > self.max_log_bytes))
    }

    fn record_len(&self, update_bytes: usize) -> Result<u64, StorageError> {
        RECORD_HEADER_BYTES
            .checked_add(update_bytes)
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or_else(|| corruption(&self.document_id, "update record length overflow"))
    }

    pub(crate) async fn compact(&mut self, snapshot: &[u8]) -> Result<(), StorageError> {
        if snapshot.len() > MAX_SNAPSHOT_BYTES {
            return corrupt(
                &self.document_id,
                format!("snapshot exceeds {MAX_SNAPSHOT_BYTES} bytes"),
            );
        }
        let next = self
            .generation
            .checked_add(1)
            .ok_or_else(|| corruption(&self.document_id, "generation overflow"))?;
        let snapshot_path = self.directory.join(snapshot_name(next));
        let log_path = self.directory.join(log_name(next));
        let manifest_path = self.directory.join(MANIFEST_FILE);

        atomic_write(&snapshot_path, &encode_snapshot(snapshot)).await?;
        atomic_write(&log_path, LOG_MAGIC).await?;
        let manifest = serde_json::to_vec_pretty(&Manifest::new(next, self.total_records))
            .map_err(|error| corruption(&self.document_id, error.to_string()))?;
        atomic_write(&manifest_path, &manifest).await?;

        self.generation = next;
        self.log_bytes = LOG_MAGIC.len() as u64;
        self.records = 0;
        self.recovered_tail = false;
        self.append_failed = false;
        remove_old_generations(&self.directory, next).await;
        Ok(())
    }

    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }

    pub(crate) fn log_bytes(&self) -> u64 {
        self.log_bytes
    }

    #[cfg(test)]
    pub(crate) fn records(&self) -> u64 {
        self.records
    }

    pub(crate) fn total_records(&self) -> u64 {
        self.total_records
    }

    #[cfg(test)]
    pub(crate) fn limit_to_one_fresh_record_for_test(&mut self, update_bytes: usize) -> u64 {
        self.max_log_bytes = LOG_MAGIC.len() as u64 + self.record_len(update_bytes).unwrap();
        self.max_log_bytes
    }
}

pub(crate) struct LoadedDocument {
    pub(crate) doc: Doc,
    pub(crate) persistence: DocumentPersistence,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum RecoveryMode {
    Inspect,
    RecoverInterruptedAppend,
    Repair,
}

struct ReplayResult {
    bytes: u64,
    records: u64,
    repaired: bool,
}

pub(crate) async fn load_or_create(
    root: &Path,
    document_id: &str,
) -> Result<LoadedDocument, StorageError> {
    let directory = document_directory(root, document_id);
    if tokio::fs::try_exists(directory.join(MANIFEST_FILE)).await? {
        return load_existing(
            &directory,
            document_id,
            RecoveryMode::RecoverInterruptedAppend,
        )
        .await;
    }

    let legacy = root.join(format!("{document_id}.yjs"));
    if tokio::fs::try_exists(&legacy).await? {
        let bytes = tokio::fs::read(&legacy).await?;
        if bytes.len() > MAX_SNAPSHOT_BYTES {
            return corrupt(
                document_id,
                format!("legacy snapshot exceeds {MAX_SNAPSHOT_BYTES} bytes"),
            );
        }
        let doc = decode_doc(document_id, &bytes)?;
        let loaded = create_document(root, document_id, doc).await?;
        let archived = root.join(format!("{document_id}.yjs.v1-migrated"));
        if !tokio::fs::try_exists(&archived).await? {
            tokio::fs::rename(&legacy, &archived).await?;
            sync_directory(root).await?;
        }
        return Ok(loaded);
    }

    if tokio::fs::try_exists(&directory).await? {
        // The manifest is the creation commit point. Anything in a directory
        // without one is debris from an interrupted first-time create.
        tokio::fs::remove_dir_all(&directory).await?;
        sync_directory(root).await?;
    }
    create_document(root, document_id, Doc::new()).await
}

/// Create a replacement epoch from a fully materialized logical document.
///
/// If the physical epoch was already written before a crash, load its committed
/// contents instead of overwriting it. The epoch manifest is switched only
/// after this function returns successfully.
pub(crate) async fn create_or_load_replacement(
    root: &Path,
    document_id: &str,
    doc: Doc,
) -> Result<LoadedDocument, StorageError> {
    let directory = document_directory(root, document_id);
    if tokio::fs::try_exists(directory.join(MANIFEST_FILE)).await? {
        return load_existing(
            &directory,
            document_id,
            RecoveryMode::RecoverInterruptedAppend,
        )
        .await;
    }
    if tokio::fs::try_exists(&directory).await? {
        // A replacement is not authoritative until both its own storage
        // manifest and the logical epoch manifest are committed. A directory
        // without the former can only be debris from an interrupted create.
        tokio::fs::remove_dir_all(&directory).await?;
        sync_directory(root).await?;
    }
    create_document(root, document_id, doc).await
}

async fn create_document(
    root: &Path,
    document_id: &str,
    doc: Doc,
) -> Result<LoadedDocument, StorageError> {
    let directory = document_directory(root, document_id);
    tokio::fs::create_dir_all(root).await?;
    let temporary = root.join(format!(".{document_id}.crdt.tmp-{}", nanoid::nanoid!(12)));
    tokio::fs::create_dir(&temporary).await?;
    sync_directory(root).await?;
    let result = async {
        let snapshot = doc
            .transact()
            .encode_state_as_update_v1(&StateVector::default());
        if snapshot.len() > MAX_SNAPSHOT_BYTES {
            return corrupt(
                document_id,
                format!("snapshot exceeds {MAX_SNAPSHOT_BYTES} bytes"),
            );
        }
        atomic_write(
            &temporary.join(snapshot_name(0)),
            &encode_snapshot(&snapshot),
        )
        .await?;
        atomic_write(&temporary.join(log_name(0)), LOG_MAGIC).await?;
        let manifest = serde_json::to_vec_pretty(&Manifest::new(0, 0))
            .map_err(|error| corruption(document_id, error.to_string()))?;
        atomic_write(&temporary.join(MANIFEST_FILE), &manifest).await?;
        tokio::fs::rename(&temporary, &directory).await?;
        sync_directory(root).await?;
        Ok::<(), StorageError>(())
    }
    .await;
    if result.is_err() {
        let _ = tokio::fs::remove_dir_all(&temporary).await;
    }
    result?;
    Ok(LoadedDocument {
        doc,
        persistence: DocumentPersistence {
            directory,
            document_id: document_id.to_string(),
            generation: 0,
            log_bytes: LOG_MAGIC.len() as u64,
            records: 0,
            total_records: 0,
            recovered_tail: false,
            append_failed: false,
            max_log_bytes: MAX_LOG_BYTES as u64,
        },
    })
}

async fn load_existing(
    directory: &Path,
    document_id: &str,
    mode: RecoveryMode,
) -> Result<LoadedDocument, StorageError> {
    let (manifest, mut recovered_manifest) = match read_manifest(directory, document_id).await {
        Ok(manifest) => (manifest, false),
        Err(error) if mode == RecoveryMode::Repair => (
            recover_manifest(directory, document_id)
                .await
                .map_err(|_| error)?,
            true,
        ),
        Err(error) => return Err(error),
    };
    let mut loaded = match load_generation(directory, document_id, &manifest, mode).await {
        Ok(loaded) => loaded,
        Err(error) if mode == RecoveryMode::Repair => {
            let recovered = recover_manifest(directory, document_id)
                .await
                .map_err(|_| error)?;
            recovered_manifest = true;
            load_generation(directory, document_id, &recovered, mode).await?
        }
        Err(error) => return Err(error),
    };
    loaded.persistence.recovered_tail |= recovered_manifest;
    if mode != RecoveryMode::Inspect {
        remove_old_generations(directory, loaded.persistence.generation()).await;
    }
    Ok(loaded)
}

async fn load_generation(
    directory: &Path,
    document_id: &str,
    manifest: &Manifest,
    mode: RecoveryMode,
) -> Result<LoadedDocument, StorageError> {
    manifest.validate(document_id)?;
    let bytes = tokio::fs::read(directory.join(&manifest.snapshot)).await?;
    let snapshot = decode_snapshot(document_id, &bytes)?;
    let doc = decode_doc(document_id, snapshot)?;
    let replay = replay_log(directory.join(&manifest.updates), document_id, &doc, mode).await?;
    Ok(LoadedDocument {
        doc,
        persistence: DocumentPersistence {
            directory: directory.to_path_buf(),
            document_id: document_id.to_string(),
            generation: manifest.generation,
            log_bytes: replay.bytes,
            records: replay.records,
            total_records: manifest.total_records.saturating_add(replay.records),
            recovered_tail: replay.repaired,
            append_failed: false,
            max_log_bytes: MAX_LOG_BYTES as u64,
        },
    })
}

async fn read_manifest(directory: &Path, document_id: &str) -> Result<Manifest, StorageError> {
    let bytes = tokio::fs::read(directory.join(MANIFEST_FILE)).await?;
    let manifest: Manifest = serde_json::from_slice(&bytes)
        .map_err(|error| corruption(document_id, format!("invalid manifest: {error}")))?;
    manifest.validate(document_id)?;
    Ok(manifest)
}

async fn recover_manifest(directory: &Path, document_id: &str) -> Result<Manifest, StorageError> {
    let mut generations = Vec::new();
    let mut entries = tokio::fs::read_dir(directory).await?;
    while let Some(entry) = entries.next_entry().await? {
        let Some(name) = entry.file_name().to_str().map(str::to_string) else {
            continue;
        };
        let Some(raw) = name
            .strip_prefix("snapshot-")
            .and_then(|name| name.strip_suffix(".bin"))
        else {
            continue;
        };
        if let Ok(generation) = raw.parse::<u64>() {
            generations.push(generation);
        }
    }
    generations.sort_unstable_by(|left, right| right.cmp(left));
    for generation in generations {
        let manifest = Manifest::new(generation, 0);
        if !tokio::fs::try_exists(directory.join(&manifest.updates)).await? {
            continue;
        }
        if load_generation(directory, document_id, &manifest, RecoveryMode::Repair)
            .await
            .is_ok()
        {
            let bytes = serde_json::to_vec_pretty(&manifest)
                .map_err(|error| corruption(document_id, error.to_string()))?;
            atomic_write(&directory.join(MANIFEST_FILE), &bytes).await?;
            return Ok(manifest);
        }
    }
    corrupt(document_id, "no recoverable storage generation")
}

async fn replay_log(
    path: PathBuf,
    document_id: &str,
    doc: &Doc,
    mode: RecoveryMode,
) -> Result<ReplayResult, StorageError> {
    let bytes = tokio::fs::read(&path).await?;
    if bytes.len() > MAX_LOG_BYTES {
        return corrupt(
            document_id,
            format!("update segment exceeds {MAX_LOG_BYTES} bytes"),
        );
    }
    if !bytes.starts_with(LOG_MAGIC) {
        return corrupt(document_id, "invalid update-segment header");
    }

    let mut offset = LOG_MAGIC.len();
    let mut durable_len = offset;
    let mut records = 0u64;
    let mut repaired = false;
    while offset < bytes.len() {
        let record_start = offset;
        if bytes.len() - offset < RECORD_HEADER_BYTES {
            if mode == RecoveryMode::Inspect {
                return corrupt(document_id, "truncated update-record header");
            }
            truncate_log(&path, record_start as u64).await?;
            repaired = true;
            break;
        }
        let length = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
        offset += 8;
        let expected: [u8; 32] = bytes[offset..offset + 32].try_into().unwrap();
        offset += 32;
        // A record that claims to run past the end of the segment (however
        // large its garbled length) is an append the crash interrupted.
        let end = usize::try_from(length)
            .ok()
            .and_then(|length| offset.checked_add(length))
            .filter(|end| *end <= bytes.len());
        let Some(end) = end else {
            if mode == RecoveryMode::Inspect {
                return corrupt(document_id, "truncated update-record payload");
            }
            truncate_log(&path, record_start as u64).await?;
            repaired = true;
            break;
        };
        if end - offset > MAX_UPDATE_BYTES {
            if mode == RecoveryMode::Repair {
                truncate_log(&path, record_start as u64).await?;
                repaired = true;
                break;
            }
            return corrupt(
                document_id,
                format!("update record exceeds {MAX_UPDATE_BYTES} bytes"),
            );
        }
        let update = &bytes[offset..end];
        if checksum(update) != expected {
            // A tail that is all zeros is space a crash allocated before the
            // append's data landed; no acknowledged record looks like that.
            // Any other mismatch may be damage to acknowledged data.
            let zero_tail = bytes[record_start..].iter().all(|byte| *byte == 0);
            if mode == RecoveryMode::Repair
                || (mode == RecoveryMode::RecoverInterruptedAppend && zero_tail)
            {
                truncate_log(&path, record_start as u64).await?;
                repaired = true;
                break;
            }
            return corrupt(
                document_id,
                format!("checksum mismatch at record {records}"),
            );
        }
        let decoded = match crate::safe_yrs::validate_update(update) {
            Ok(update) => update,
            Err(_error) if mode == RecoveryMode::Repair => {
                truncate_log(&path, record_start as u64).await?;
                repaired = true;
                break;
            }
            Err(error) => {
                return corrupt(
                    document_id,
                    format!("malformed update at record {records}: {error}"),
                )
            }
        };
        doc.transact_mut().apply_update(decoded);
        records += 1;
        offset = end;
        durable_len = end;
    }
    let bytes = if repaired { durable_len } else { bytes.len() };
    Ok(ReplayResult {
        bytes: bytes as u64,
        records,
        repaired,
    })
}

async fn truncate_log(path: &Path, length: u64) -> Result<(), StorageError> {
    let file = tokio::fs::OpenOptions::new().write(true).open(path).await?;
    file.set_len(length).await?;
    file.sync_all().await?;
    if let Some(parent) = path.parent() {
        sync_directory(parent).await?;
    }
    Ok(())
}

fn encode_snapshot(snapshot: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(SNAPSHOT_HEADER_BYTES + snapshot.len());
    bytes.extend_from_slice(SNAPSHOT_MAGIC);
    bytes.extend_from_slice(&(snapshot.len() as u64).to_le_bytes());
    bytes.extend_from_slice(&checksum(snapshot));
    bytes.extend_from_slice(snapshot);
    bytes
}

fn decode_snapshot<'a>(document_id: &str, bytes: &'a [u8]) -> Result<&'a [u8], StorageError> {
    if bytes.len() < SNAPSHOT_HEADER_BYTES || !bytes.starts_with(SNAPSHOT_MAGIC) {
        return corrupt(document_id, "invalid snapshot header");
    }
    let length = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
    let length = usize::try_from(length)
        .map_err(|_| corruption(document_id, "snapshot length does not fit this platform"))?;
    if length > MAX_SNAPSHOT_BYTES {
        return corrupt(
            document_id,
            format!("snapshot exceeds {MAX_SNAPSHOT_BYTES} bytes"),
        );
    }
    if bytes.len() != SNAPSHOT_HEADER_BYTES + length {
        return corrupt(document_id, "snapshot length mismatch");
    }
    let expected: [u8; 32] = bytes[16..48].try_into().unwrap();
    let snapshot = &bytes[SNAPSHOT_HEADER_BYTES..];
    if checksum(snapshot) != expected {
        return corrupt(document_id, "snapshot checksum mismatch");
    }
    Ok(snapshot)
}

fn decode_doc(document_id: &str, update: &[u8]) -> Result<Doc, StorageError> {
    let decoded = crate::safe_yrs::validate_update(update)
        .map_err(|error| corruption(document_id, format!("invalid Yjs snapshot: {error}")))?;
    let doc = Doc::new();
    doc.transact_mut().apply_update(decoded);
    Ok(doc)
}

#[cfg(any(test, feature = "fuzzing"))]
fn exercise_storage_bytes(bytes: &[u8]) {
    if let Ok(snapshot) = decode_snapshot("fuzz", bytes) {
        let _ = decode_doc("fuzz", snapshot);
    }
    if let Ok(update) = crate::safe_yrs::validate_update(bytes) {
        let doc = Doc::new();
        doc.transact_mut().apply_update(update);
        std::hint::black_box(doc.transact().state_vector());
    }
    if let Ok(manifest) = serde_json::from_slice::<Manifest>(bytes) {
        let _ = manifest.validate("fuzz");
    }
}

/// Parser entry point used by the out-of-process fuzz target.
#[cfg(feature = "fuzzing")]
pub fn fuzz_storage_bytes(bytes: &[u8]) {
    if bytes.len() <= MAX_SNAPSHOT_BYTES {
        exercise_storage_bytes(bytes);
    }
}

fn checksum(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

fn snapshot_name(generation: u64) -> String {
    format!("snapshot-{generation}.bin")
}

fn log_name(generation: u64) -> String {
    format!("updates-{generation}.log")
}

fn document_directory(root: &Path, document_id: &str) -> PathBuf {
    root.join(format!("{document_id}.crdt"))
}

async fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), StorageError> {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| std::io::Error::other("invalid storage path"))?;
    let temporary = path.with_file_name(format!(".{file_name}.tmp-{}", nanoid::nanoid!(12)));
    let result = async {
        let mut file = tokio::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)
            .await?;
        file.write_all(bytes).await?;
        file.sync_all().await?;
        drop(file);
        tokio::fs::rename(&temporary, path).await?;
        let parent = path
            .parent()
            .ok_or_else(|| std::io::Error::other("storage path has no parent"))?;
        sync_directory(parent).await?;
        Ok::<(), std::io::Error>(())
    }
    .await;
    if result.is_err() {
        let _ = tokio::fs::remove_file(&temporary).await;
    }
    result.map_err(StorageError::Io)
}

async fn sync_directory(path: &Path) -> Result<(), std::io::Error> {
    #[cfg(unix)]
    tokio::fs::File::open(path).await?.sync_all().await?;
    Ok(())
}

async fn remove_old_generations(directory: &Path, active: u64) {
    let Ok(mut entries) = tokio::fs::read_dir(directory).await else {
        return;
    };
    while let Ok(Some(entry)) = entries.next_entry().await {
        let Some(name) = entry.file_name().to_str().map(str::to_string) else {
            continue;
        };
        let generation = name
            .strip_prefix("snapshot-")
            .and_then(|name| name.strip_suffix(".bin"))
            .or_else(|| {
                name.strip_prefix("updates-")
                    .and_then(|name| name.strip_suffix(".log"))
            })
            .and_then(|generation| generation.parse::<u64>().ok());
        if generation.is_some_and(|generation| generation < active) {
            let _ = tokio::fs::remove_file(entry.path()).await;
        }
    }
}

fn corruption(document_id: &str, reason: impl Into<String>) -> StorageError {
    StorageError::Corrupt {
        document_id: document_id.to_string(),
        reason: reason.into(),
    }
}

fn corrupt<T>(document_id: &str, reason: impl Into<String>) -> Result<T, StorageError> {
    Err(corruption(document_id, reason))
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StoreInspection {
    pub format: &'static str,
    pub version: u32,
    pub root: String,
    pub documents: Vec<DocumentInspection>,
    pub healthy: usize,
    pub repaired: usize,
    pub corrupt: usize,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DocumentInspection {
    pub document_id: String,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub generation: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub update_log_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub update_records: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

pub async fn inspect_store(root: &Path, repair: bool) -> Result<StoreInspection, StorageError> {
    tokio::fs::create_dir_all(root).await?;
    crate::crdt_epoch::validate_store_manifests(root)
        .await
        .map_err(|error| StorageError::Corrupt {
            document_id: "epoch-manifest".to_string(),
            reason: error.to_string(),
        })?;
    let mut document_ids = Vec::new();
    let mut entries = tokio::fs::read_dir(root).await?;
    while let Some(entry) = entries.next_entry().await? {
        let metadata = entry.metadata().await?;
        if !metadata.is_dir() {
            continue;
        }
        let Some(name) = entry.file_name().to_str().map(str::to_string) else {
            continue;
        };
        if let Some(document_id) = name.strip_suffix(".crdt") {
            document_ids.push(document_id.to_string());
        }
    }
    document_ids.sort();

    let mode = if repair {
        RecoveryMode::Repair
    } else {
        RecoveryMode::Inspect
    };
    let mut documents = Vec::with_capacity(document_ids.len());
    let mut healthy = 0;
    let mut repaired_count = 0;
    let mut corrupt_count = 0;
    for document_id in document_ids {
        let directory = document_directory(root, &document_id);
        match load_existing(&directory, &document_id, mode).await {
            Ok(loaded) => {
                let status = if loaded.persistence.recovered_tail {
                    repaired_count += 1;
                    "repaired"
                } else {
                    healthy += 1;
                    "ok"
                };
                let snapshot_bytes = loaded
                    .doc
                    .transact()
                    .encode_state_as_update_v1(&StateVector::default())
                    .len() as u64;
                documents.push(DocumentInspection {
                    document_id,
                    status: status.to_string(),
                    generation: Some(loaded.persistence.generation()),
                    snapshot_bytes: Some(snapshot_bytes),
                    update_log_bytes: Some(loaded.persistence.log_bytes()),
                    update_records: Some(loaded.persistence.total_records()),
                    error: None,
                });
            }
            Err(error) => {
                corrupt_count += 1;
                documents.push(DocumentInspection {
                    document_id,
                    status: "corrupt".to_string(),
                    generation: None,
                    snapshot_bytes: None,
                    update_log_bytes: None,
                    update_records: None,
                    error: Some(error.to_string()),
                });
            }
        }
    }

    Ok(StoreInspection {
        format: FORMAT,
        version: FORMAT_VERSION,
        root: root.display().to_string(),
        documents,
        healthy,
        repaired: repaired_count,
        corrupt: corrupt_count,
    })
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ImportReport {
    pub source: String,
    pub destination: String,
    pub imported: Vec<String>,
    pub skipped: Vec<String>,
    pub errors: Vec<String>,
}

pub async fn import_ysweet_store(
    source: &Path,
    destination: &Path,
) -> Result<ImportReport, StorageError> {
    tokio::fs::create_dir_all(destination).await?;
    let mut report = ImportReport {
        source: source.display().to_string(),
        destination: destination.display().to_string(),
        imported: Vec::new(),
        skipped: Vec::new(),
        errors: Vec::new(),
    };
    let mut entries = tokio::fs::read_dir(source).await?;
    while let Some(entry) = entries.next_entry().await? {
        if !entry.metadata().await?.is_dir() {
            continue;
        }
        let Some(document_id) = entry.file_name().to_str().map(str::to_string) else {
            continue;
        };
        if document_id.is_empty()
            || document_id.len() > 255
            || !document_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            report
                .errors
                .push(format!("{document_id}: invalid document id"));
            continue;
        }
        let source_file = entry.path().join("data.ysweet");
        if !tokio::fs::try_exists(&source_file).await? {
            continue;
        }
        if tokio::fs::try_exists(document_directory(destination, &document_id)).await?
            || tokio::fs::try_exists(destination.join(format!("{document_id}.yjs"))).await?
        {
            report.skipped.push(document_id);
            continue;
        }
        match import_ysweet_document(&source_file, destination, &document_id).await {
            Ok(()) => report.imported.push(document_id),
            Err(error) => report.errors.push(format!("{document_id}: {error}")),
        }
    }
    report.imported.sort();
    report.skipped.sort();
    report.errors.sort();
    Ok(report)
}

async fn import_ysweet_document(
    source: &Path,
    destination: &Path,
    document_id: &str,
) -> Result<(), StorageError> {
    let bytes = tokio::fs::read(source).await?;
    let data: BTreeMap<Vec<u8>, Vec<u8>> =
        bincode::deserialize(&bytes).map_err(|error| StorageError::Legacy(error.to_string()))?;
    let store = LegacyKv { data };
    let doc = Doc::new();
    let loaded = {
        let mut txn = doc.transact_mut();
        store
            .load_doc("doc", &mut txn)
            .map_err(|error| StorageError::Legacy(error.to_string()))?
    };
    if !loaded && !store.data.is_empty() {
        return Err(StorageError::Legacy(
            "data.ysweet does not contain the expected doc".to_string(),
        ));
    }
    create_document(destination, document_id, doc).await?;
    Ok(())
}

struct LegacyEntry {
    key: Vec<u8>,
    value: Vec<u8>,
}

impl KVEntry for LegacyEntry {
    fn key(&self) -> &[u8] {
        &self.key
    }

    fn value(&self) -> &[u8] {
        &self.value
    }
}

struct LegacyCursor {
    entries: std::vec::IntoIter<LegacyEntry>,
}

impl Iterator for LegacyCursor {
    type Item = LegacyEntry;

    fn next(&mut self) -> Option<Self::Item> {
        self.entries.next()
    }
}

struct LegacyKv {
    data: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl<'a> KVStore<'a> for LegacyKv {
    type Error = Infallible;
    type Cursor = LegacyCursor;
    type Entry = LegacyEntry;
    type Return = Vec<u8>;

    fn get(&self, key: &[u8]) -> Result<Option<Self::Return>, Self::Error> {
        Ok(self.data.get(key).cloned())
    }

    fn upsert(&self, _key: &[u8], _value: &[u8]) -> Result<(), Self::Error> {
        unreachable!("legacy import is read-only")
    }

    fn remove(&self, _key: &[u8]) -> Result<(), Self::Error> {
        unreachable!("legacy import is read-only")
    }

    fn remove_range(&self, _from: &[u8], _to: &[u8]) -> Result<(), Self::Error> {
        unreachable!("legacy import is read-only")
    }

    fn iter_range(&self, from: &[u8], to: &[u8]) -> Result<Self::Cursor, Self::Error> {
        let entries = self
            .data
            .range((Bound::Included(from.to_vec()), Bound::Excluded(to.to_vec())))
            .map(|(key, value)| LegacyEntry {
                key: key.clone(),
                value: value.clone(),
            })
            .collect::<Vec<_>>()
            .into_iter();
        Ok(LegacyCursor { entries })
    }

    fn peek_back(&self, key: &[u8]) -> Result<Option<Self::Entry>, Self::Error> {
        Ok(self
            .data
            .range(..key.to_vec())
            .next_back()
            .map(|(key, value)| LegacyEntry {
                key: key.clone(),
                value: value.clone(),
            }))
    }
}

impl<'a> DocOps<'a> for LegacyKv {}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{engine::general_purpose::STANDARD, Engine as _};
    use proptest::prelude::*;
    use rand::{rngs::StdRng, RngCore, SeedableRng};
    use yrs::Map;

    fn temp_store() -> PathBuf {
        std::env::temp_dir().join(format!("realtime-storage-v2-{}", uuid::Uuid::new_v4()))
    }

    fn map_update(key: &str, value: &str) -> Vec<u8> {
        let doc = Doc::new();
        doc.get_or_insert_map("values")
            .insert(&mut doc.transact_mut(), key, value);
        let update = doc
            .transact()
            .encode_state_as_update_v1(&StateVector::default());
        update
    }

    fn map_value(doc: &Doc, key: &str) -> Option<String> {
        doc.get_or_insert_map("values")
            .get(&doc.transact(), key)
            .and_then(|value| value.cast::<String>().ok())
    }

    async fn apply(loaded: &mut LoadedDocument, update: &[u8]) {
        loaded.persistence.append_update(update).await.unwrap();
        loaded
            .doc
            .transact_mut()
            .apply_update(crate::safe_yrs::decode_v1::<yrs::Update>(update).unwrap());
    }

    fn snapshot(doc: &Doc) -> Vec<u8> {
        doc.transact()
            .encode_state_as_update_v1(&StateVector::default())
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(512))]

        #[test]
        fn fuzz_arbitrary_storage_inputs_do_not_panic(
            bytes in prop::collection::vec(any::<u8>(), 0..65_536),
        ) {
            exercise_storage_bytes(&bytes);
        }

        #[test]
        fn fuzz_every_single_bit_snapshot_mutation_is_rejected(selector in any::<usize>()) {
            let mut encoded = encode_snapshot(&map_update("mutation", "must-fail"));
            let byte = selector % encoded.len();
            let bit = (selector / encoded.len()) % 8;
            encoded[byte] ^= 1 << bit;
            prop_assert!(decode_snapshot("mutated", &encoded).is_err());
        }
    }

    #[tokio::test]
    async fn deterministic_corrupt_tail_fuzz_preserves_the_last_durable_update() {
        let mut random = StdRng::seed_from_u64(0x5eed_cafe_f00d_beef);
        for round in 0..64 {
            let root = temp_store();
            tokio::fs::create_dir_all(&root).await.unwrap();
            let mut loaded = load_or_create(&root, "doc").await.unwrap();
            let expected = format!("round-{round}");
            apply(&mut loaded, &map_update("durable", &expected)).await;
            drop(loaded);

            let log = root.join("doc.crdt/updates-0.log");
            let valid_length = tokio::fs::metadata(&log).await.unwrap().len();
            let mut tail = vec![0; (random.next_u32() as usize % 256) + 1];
            random.fill_bytes(&mut tail);
            let mut file = tokio::fs::OpenOptions::new()
                .append(true)
                .open(&log)
                .await
                .unwrap();
            file.write_all(&tail).await.unwrap();
            file.sync_all().await.unwrap();
            drop(file);

            let inspection = inspect_store(&root, true).await.unwrap();
            assert_eq!(inspection.repaired, 1, "round {round}");
            assert_eq!(
                tokio::fs::metadata(&log).await.unwrap().len(),
                valid_length,
                "round {round}"
            );
            let recovered = load_or_create(&root, "doc").await.unwrap();
            assert_eq!(
                map_value(&recovered.doc, "durable").as_deref(),
                Some(expected.as_str()),
                "round {round}"
            );
            let _ = tokio::fs::remove_dir_all(root).await;
        }
    }

    #[tokio::test]
    async fn interrupted_first_creation_without_manifest_is_recreated() {
        let root = temp_store();
        let directory = root.join("doc.crdt");
        tokio::fs::create_dir_all(&directory).await.unwrap();
        tokio::fs::write(
            directory.join("snapshot-0.bin"),
            encode_snapshot(&map_update("uncommitted", "discarded")),
        )
        .await
        .unwrap();

        let loaded = load_or_create(&root, "doc").await.unwrap();

        assert_eq!(map_value(&loaded.doc, "uncommitted"), None);
        assert!(tokio::fs::try_exists(directory.join("manifest.json"))
            .await
            .unwrap());
        drop(loaded);
        load_or_create(&root, "doc").await.unwrap();
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn corrupt_committed_first_generation_still_fails_closed() {
        let root = temp_store();
        tokio::fs::create_dir_all(&root).await.unwrap();
        drop(load_or_create(&root, "doc").await.unwrap());
        tokio::fs::write(root.join("doc.crdt/snapshot-0.bin"), b"corrupt")
            .await
            .unwrap();

        let error = match load_or_create(&root, "doc").await {
            Ok(_) => panic!("committed corruption must fail closed"),
            Err(error) => error,
        };

        assert!(matches!(error, StorageError::Corrupt { .. }));
        assert!(error.to_string().contains("invalid snapshot header"));
        assert!(tokio::fs::try_exists(root.join("doc.crdt/manifest.json"))
            .await
            .unwrap());
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn appends_updates_without_rewriting_the_snapshot() {
        let root = temp_store();
        tokio::fs::create_dir_all(&root).await.unwrap();
        let mut loaded = load_or_create(&root, "doc").await.unwrap();
        let snapshot_path = root.join("doc.crdt/snapshot-0.bin");
        let before = tokio::fs::read(&snapshot_path).await.unwrap();
        for index in 0..32 {
            apply(
                &mut loaded,
                &map_update(&format!("key-{index}"), &format!("value-{index}")),
            )
            .await;
        }
        assert_eq!(tokio::fs::read(&snapshot_path).await.unwrap(), before);
        assert_eq!(loaded.persistence.records(), 32);
        drop(loaded);

        let reloaded = load_or_create(&root, "doc").await.unwrap();
        for index in 0..32 {
            assert_eq!(
                map_value(&reloaded.doc, &format!("key-{index}")),
                Some(format!("value-{index}"))
            );
        }
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn existing_epoch_manifest_with_missing_current_store_is_corrupt() {
        let root = temp_store();
        tokio::fs::create_dir_all(&root).await.unwrap();
        crate::crdt_epoch::load_or_create_manifest(&root, "doc")
            .await
            .unwrap();

        let error = crate::crdt_epoch::validate_store_manifests(&root)
            .await
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("missing current physical document"));
        assert!(!root.join("doc.crdt").exists());
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn append_refuses_to_create_an_unreplayable_segment() {
        let root = temp_store();
        tokio::fs::create_dir_all(&root).await.unwrap();
        let mut loaded = load_or_create(&root, "doc").await.unwrap();
        let update = map_update("bounded", "value");
        let limit = loaded
            .persistence
            .limit_to_one_fresh_record_for_test(update.len());
        loaded.persistence.append_update(&update).await.unwrap();
        assert_eq!(loaded.persistence.log_bytes(), limit);

        let error = loaded.persistence.append_update(&update).await.unwrap_err();
        assert!(matches!(error, StorageError::LogTooLarge { .. }));
        assert_eq!(
            tokio::fs::metadata(root.join("doc.crdt/updates-0.log"))
                .await
                .unwrap()
                .len(),
            limit
        );
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn interrupted_append_is_truncated_to_the_last_checksummed_record() {
        let root = temp_store();
        tokio::fs::create_dir_all(&root).await.unwrap();
        let mut loaded = load_or_create(&root, "doc").await.unwrap();
        apply(&mut loaded, &map_update("kept", "yes")).await;
        drop(loaded);

        let log = root.join("doc.crdt/updates-0.log");
        let valid_len = tokio::fs::metadata(&log).await.unwrap().len();
        let mut file = tokio::fs::OpenOptions::new()
            .append(true)
            .open(&log)
            .await
            .unwrap();
        file.write_all(&[17; 19]).await.unwrap();
        file.sync_all().await.unwrap();
        drop(file);

        let reloaded = load_or_create(&root, "doc").await.unwrap();
        assert_eq!(map_value(&reloaded.doc, "kept").as_deref(), Some("yes"));
        assert!(reloaded.persistence.recovered_tail);
        assert_eq!(tokio::fs::metadata(&log).await.unwrap().len(), valid_len);
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn failed_append_discards_its_partial_record_before_the_next_write() {
        let root = temp_store();
        tokio::fs::create_dir_all(&root).await.unwrap();
        let mut loaded = load_or_create(&root, "doc").await.unwrap();
        apply(&mut loaded, &map_update("first", "kept")).await;

        // An append failed after part of its record reached the segment, and
        // truncating it at the time failed too, so the segment is latched.
        let log = root.join("doc.crdt/updates-0.log");
        let mut file = tokio::fs::OpenOptions::new()
            .append(true)
            .open(&log)
            .await
            .unwrap();
        file.write_all(&[42; 23]).await.unwrap();
        file.sync_all().await.unwrap();
        drop(file);
        loaded.persistence.append_failed = true;

        // The next write repairs the segment instead of refusing forever.
        loaded
            .persistence
            .append_update(&map_update("second", "kept"))
            .await
            .unwrap();
        drop(loaded);

        let reloaded = load_or_create(&root, "doc").await.unwrap();
        assert!(!reloaded.persistence.recovered_tail);
        assert_eq!(map_value(&reloaded.doc, "first").as_deref(), Some("kept"));
        assert_eq!(map_value(&reloaded.doc, "second").as_deref(), Some("kept"));
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn zero_filled_and_overlong_tails_are_interrupted_appends() {
        for tail in [vec![0u8; 96], {
            // A garbled header whose length runs far past the end of the file.
            let mut header = u64::MAX.to_le_bytes().to_vec();
            header.extend_from_slice(&[7; 32]);
            header.extend_from_slice(b"partial");
            header
        }] {
            let root = temp_store();
            tokio::fs::create_dir_all(&root).await.unwrap();
            let mut loaded = load_or_create(&root, "doc").await.unwrap();
            apply(&mut loaded, &map_update("kept", "yes")).await;
            drop(loaded);

            let log = root.join("doc.crdt/updates-0.log");
            let valid_len = tokio::fs::metadata(&log).await.unwrap().len();
            let mut file = tokio::fs::OpenOptions::new()
                .append(true)
                .open(&log)
                .await
                .unwrap();
            file.write_all(&tail).await.unwrap();
            file.sync_all().await.unwrap();
            drop(file);

            let reloaded = load_or_create(&root, "doc").await.unwrap();
            assert_eq!(map_value(&reloaded.doc, "kept").as_deref(), Some("yes"));
            assert!(reloaded.persistence.recovered_tail);
            assert_eq!(tokio::fs::metadata(&log).await.unwrap().len(), valid_len);
            let _ = tokio::fs::remove_dir_all(root).await;
        }
    }

    #[tokio::test]
    async fn checksum_corruption_fails_closed_and_repair_truncates_it() {
        let root = temp_store();
        tokio::fs::create_dir_all(&root).await.unwrap();
        let mut loaded = load_or_create(&root, "doc").await.unwrap();
        apply(&mut loaded, &map_update("corrupt", "discarded")).await;
        drop(loaded);

        let log = root.join("doc.crdt/updates-0.log");
        let mut bytes = tokio::fs::read(&log).await.unwrap();
        *bytes.last_mut().unwrap() ^= 0x80;
        tokio::fs::write(&log, bytes).await.unwrap();

        let error = match load_or_create(&root, "doc").await {
            Ok(_) => panic!("checksum corruption must fail closed"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("checksum mismatch"));
        let inspection = inspect_store(&root, false).await.unwrap();
        assert_eq!(inspection.corrupt, 1);

        let repaired = inspect_store(&root, true).await.unwrap();
        assert_eq!(repaired.repaired, 1);
        assert_eq!(tokio::fs::metadata(&log).await.unwrap().len(), 8);
        let reloaded = load_or_create(&root, "doc").await.unwrap();
        assert_eq!(map_value(&reloaded.doc, "corrupt"), None);
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn orphaned_compaction_files_are_ignored_until_manifest_swap() {
        let root = temp_store();
        tokio::fs::create_dir_all(&root).await.unwrap();
        let mut loaded = load_or_create(&root, "doc").await.unwrap();
        apply(&mut loaded, &map_update("committed", "yes")).await;
        drop(loaded);

        let orphan = decode_doc("doc", &map_update("orphan", "no")).unwrap();
        atomic_write(
            &root.join("doc.crdt/snapshot-1.bin"),
            &encode_snapshot(&snapshot(&orphan)),
        )
        .await
        .unwrap();
        atomic_write(&root.join("doc.crdt/updates-1.log"), LOG_MAGIC)
            .await
            .unwrap();
        atomic_write(&root.join("doc.crdt/.manifest.json.tmp-crash"), b"partial")
            .await
            .unwrap();

        let reloaded = load_or_create(&root, "doc").await.unwrap();
        assert_eq!(reloaded.persistence.generation(), 0);
        assert_eq!(
            map_value(&reloaded.doc, "committed").as_deref(),
            Some("yes")
        );
        assert_eq!(map_value(&reloaded.doc, "orphan"), None);
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn compaction_reclaims_obsolete_generations_and_preserves_state() {
        let root = temp_store();
        tokio::fs::create_dir_all(&root).await.unwrap();
        let mut loaded = load_or_create(&root, "doc").await.unwrap();
        apply(&mut loaded, &map_update("first", "one")).await;
        loaded
            .persistence
            .compact(&snapshot(&loaded.doc))
            .await
            .unwrap();
        apply(&mut loaded, &map_update("second", "two")).await;
        loaded
            .persistence
            .compact(&snapshot(&loaded.doc))
            .await
            .unwrap();
        assert!(!tokio::fs::try_exists(root.join("doc.crdt/snapshot-0.bin"))
            .await
            .unwrap());
        assert!(!tokio::fs::try_exists(root.join("doc.crdt/snapshot-1.bin"))
            .await
            .unwrap());
        assert!(tokio::fs::try_exists(root.join("doc.crdt/snapshot-2.bin"))
            .await
            .unwrap());
        drop(loaded);

        let reloaded = load_or_create(&root, "doc").await.unwrap();
        assert_eq!(map_value(&reloaded.doc, "first").as_deref(), Some("one"));
        assert_eq!(map_value(&reloaded.doc, "second").as_deref(), Some("two"));
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn repair_rebuilds_a_corrupt_manifest_from_valid_generation_files() {
        let root = temp_store();
        tokio::fs::create_dir_all(&root).await.unwrap();
        let mut loaded = load_or_create(&root, "doc").await.unwrap();
        apply(&mut loaded, &map_update("generation", "kept")).await;
        loaded
            .persistence
            .compact(&snapshot(&loaded.doc))
            .await
            .unwrap();
        drop(loaded);

        tokio::fs::write(root.join("doc.crdt/manifest.json"), b"corrupt")
            .await
            .unwrap();
        let failed = inspect_store(&root, false).await.unwrap();
        assert_eq!(failed.corrupt, 1);

        let repaired = inspect_store(&root, true).await.unwrap();
        assert_eq!(repaired.repaired, 1);
        assert_eq!(repaired.documents[0].generation, Some(1));
        let reloaded = load_or_create(&root, "doc").await.unwrap();
        assert_eq!(
            map_value(&reloaded.doc, "generation").as_deref(),
            Some("kept")
        );
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn migrates_atomic_v1_snapshots_once() {
        let root = temp_store();
        tokio::fs::create_dir_all(&root).await.unwrap();
        tokio::fs::write(root.join("doc.yjs"), map_update("legacy", "loaded"))
            .await
            .unwrap();

        let loaded = load_or_create(&root, "doc").await.unwrap();
        assert_eq!(map_value(&loaded.doc, "legacy").as_deref(), Some("loaded"));
        assert!(tokio::fs::try_exists(root.join("doc.crdt/manifest.json"))
            .await
            .unwrap());
        assert!(tokio::fs::try_exists(root.join("doc.yjs.v1-migrated"))
            .await
            .unwrap());
        assert!(!tokio::fs::try_exists(root.join("doc.yjs")).await.unwrap());
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn imports_a_ysweet_filesystem_store_without_overwriting_documents() {
        let corpus: serde_json::Value =
            serde_json::from_str(include_str!("../../compat/corpus/ysweet-0.9.1.json")).unwrap();
        assert_eq!(corpus["schemaVersion"], 1);
        assert_eq!(corpus["producer"]["ySweetVersion"], "0.9.1");
        let fixture = &corpus["document"];
        let source = temp_store();
        let destination = temp_store();
        let document_id = fixture["id"].as_str().unwrap();
        tokio::fs::create_dir_all(source.join(document_id))
            .await
            .unwrap();
        let encoded = STANDARD
            .decode(fixture["dataYsWeetBase64"].as_str().unwrap())
            .unwrap();
        tokio::fs::write(source.join(document_id).join("data.ysweet"), encoded)
            .await
            .unwrap();

        let first = import_ysweet_store(&source, &destination).await.unwrap();
        assert_eq!(first.imported, vec![document_id]);
        assert!(first.errors.is_empty());
        let loaded = load_or_create(&destination, document_id).await.unwrap();
        assert_eq!(
            map_value(&loaded.doc, fixture["expectedKey"].as_str().unwrap()).as_deref(),
            fixture["expectedValue"].as_str()
        );

        let second = import_ysweet_store(&source, &destination).await.unwrap();
        assert_eq!(second.skipped, vec![document_id]);
        let _ = tokio::fs::remove_dir_all(source).await;
        let _ = tokio::fs::remove_dir_all(destination).await;
    }
}
