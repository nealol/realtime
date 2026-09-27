//! Native Yjs-compatible document storage and synchronization.
//!
//! The server owns the complete document lifecycle: Yjs v1 updates are merged
//! with `yrs`, durably stored in checksummed incremental segments, and
//! synchronized over the same y-protocol wire format consumed by
//! `@y-sweet/client`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, RwLock, Weak};
use std::time::{Duration, Instant};

use axum::body::Bytes;
use axum::extract::ws::{Message as AxumMessage, WebSocket, WebSocketUpgrade};
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::{broadcast, mpsc, Mutex};
use url::Url;
use yrs::sync::{Awareness, Message, SyncMessage};
use yrs::updates::encoder::Encode;
use yrs::{Doc, ReadTxn, StateVector, Transact};

use crate::crdt_epoch::{self, DocumentEpochMetrics, EpochPolicy};
use crate::crdt_storage::{self, DocumentPersistence, StorageError};
use crate::error::{AppError, AppResult};
use crate::operations::SyncMetrics;
use crate::state::{AppState, Principal};

/// Largest accepted wire update and persisted document snapshot.
pub const MAX_UPDATE_BYTES: usize = 64 * 1024 * 1024;
/// Presence is ephemeral JSON and must never consume the document-update budget.
pub const MAX_AWARENESS_BYTES: usize = 64 * 1024;
const CONNECTION_CAPACITY: usize = 256;
const BROADCAST_CAPACITY: usize = 512;
pub(crate) const SYNC_STATUS_MESSAGE: u8 = 102;
const EPOCH_PROPOSAL_MESSAGE: u8 = 103;
const EPOCH_ACK_MESSAGE: u8 = 104;
const DOCUMENT_INVALIDATED_MESSAGE: u8 = 105;
const RETIRED_EPOCH_RETRY_MS: u64 = 60_000;
const EPOCH_ACK_TIMEOUT_MS: u64 = 30_000;

/// Access level encoded in client tokens and enforced on every content update.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Level {
    Full,
    ReadOnly,
}

impl Level {
    pub fn as_str(self) -> &'static str {
        match self {
            Level::Full => "full",
            Level::ReadOnly => "read-only",
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CrdtError {
    #[error("invalid document id")]
    InvalidDocumentId,
    #[error("document update exceeds {MAX_UPDATE_BYTES} bytes")]
    UpdateTooLarge,
    #[error("invalid Yjs update: {0}")]
    InvalidUpdate(String),
    #[error("corrupt persisted document {document_id}: {reason}")]
    CorruptDocument { document_id: String, reason: String },
    #[error("document I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("document storage: {0}")]
    Storage(String),
    #[error("sync protocol: {0}")]
    Protocol(String),
    #[error("document {document_id} is switching to epoch {epoch}; reconnect with a fresh token")]
    EpochTransition { document_id: String, epoch: u64 },
    #[error(
        "document {document_id} token targets retired epoch {requested}; current epoch is {current}"
    )]
    RetiredEpoch {
        document_id: String,
        requested: u64,
        current: u64,
    },
}

impl From<CrdtError> for AppError {
    fn from(error: CrdtError) -> Self {
        match error {
            CrdtError::InvalidDocumentId | CrdtError::InvalidUpdate(_) => {
                AppError::BadRequest(error.to_string())
            }
            CrdtError::UpdateTooLarge => AppError::PayloadTooLarge,
            CrdtError::EpochTransition { .. } | CrdtError::RetiredEpoch { .. } => {
                AppError::Conflict(error.to_string())
            }
            _ => AppError::Internal(error.to_string()),
        }
    }
}

impl From<StorageError> for CrdtError {
    fn from(error: StorageError) -> Self {
        match error {
            StorageError::Io(error) => CrdtError::Io(error),
            StorageError::Corrupt {
                document_id,
                reason,
            } => CrdtError::CorruptDocument {
                document_id,
                reason,
            },
            StorageError::Legacy(reason) => CrdtError::CorruptDocument {
                document_id: "legacy-import".to_string(),
                reason,
            },
            StorageError::LogTooLarge {
                document_id,
                max_bytes,
            } => CrdtError::Storage(format!(
                "{document_id} update segment exceeds its {max_bytes}-byte replay limit"
            )),
        }
    }
}

struct StoreInner {
    directory: PathBuf,
    policy: EpochPolicy,
    documents: Mutex<HashMap<String, LiveDocument>>,
    epochs: Mutex<HashMap<String, Arc<Mutex<EpochRuntime>>>>,
    load_locks: Mutex<HashMap<String, Weak<Mutex<()>>>>,
    retirement_deadlines: StdMutex<HashMap<String, u64>>,
    next_connection_id: AtomicU64,
    sync_metrics: SyncMetrics,
    #[cfg(test)]
    write_pauses: Mutex<HashMap<String, WritePause>>,
    #[cfg(test)]
    connection_write_pauses: Mutex<HashMap<String, WritePause>>,
    #[cfg(test)]
    connection_pauses: Mutex<HashMap<String, ConnectionPause>>,
}

struct LiveDocument {
    epoch: u64,
    document: Weak<PersistentDocument>,
}

struct EpochRuntime {
    manifest: crdt_epoch::EpochManifest,
    connections: HashMap<u64, EpochConnectionState>,
    activating: bool,
    write_gate: Arc<Mutex<()>>,
}

#[cfg(test)]
struct WritePause {
    reached: tokio::sync::oneshot::Sender<()>,
    resume: tokio::sync::oneshot::Receiver<()>,
}

#[cfg(test)]
struct ConnectionPause {
    reached: tokio::sync::oneshot::Sender<()>,
    resume: tokio::sync::oneshot::Receiver<()>,
}

#[derive(Clone, Copy)]
struct EpochConnectionState {
    epoch: u64,
    acknowledged: bool,
}

/// Lazily loaded, process-wide document store.
///
/// Loaded documents are held weakly by the store. Active sync connections and
/// callers keep them alive; an idle document can be dropped and safely loaded
/// again from its last acknowledged snapshot.
#[derive(Clone)]
pub struct DocumentStore(Arc<StoreInner>);

impl DocumentStore {
    pub async fn new(directory: impl Into<PathBuf>) -> Result<Self, CrdtError> {
        Self::new_with_policy(directory, EpochPolicy::default()).await
    }

    pub async fn new_with_policy(
        directory: impl Into<PathBuf>,
        policy: EpochPolicy,
    ) -> Result<Self, CrdtError> {
        let directory = directory.into();
        tokio::fs::create_dir_all(&directory).await?;
        Ok(Self(Arc::new(StoreInner {
            directory,
            policy,
            documents: Mutex::new(HashMap::new()),
            epochs: Mutex::new(HashMap::new()),
            load_locks: Mutex::new(HashMap::new()),
            retirement_deadlines: StdMutex::new(HashMap::new()),
            next_connection_id: AtomicU64::new(1),
            sync_metrics: SyncMetrics::default(),
            #[cfg(test)]
            write_pauses: Mutex::new(HashMap::new()),
            #[cfg(test)]
            connection_write_pauses: Mutex::new(HashMap::new()),
            #[cfg(test)]
            connection_pauses: Mutex::new(HashMap::new()),
        })))
    }

    pub fn directory(&self) -> &Path {
        &self.0.directory
    }

    pub fn sync_metrics(&self) -> SyncMetrics {
        self.0.sync_metrics.clone()
    }

    pub async fn residency_counts(&self) -> (usize, usize) {
        let live_documents = {
            let mut documents = self.0.documents.lock().await;
            documents.retain(|_, live| live.document.strong_count() > 0);
            documents.len()
        };
        let known_documents = self.0.epochs.lock().await.len();
        (live_documents, known_documents)
    }

    pub async fn ensure_document(&self, document_id: &str) -> Result<(), CrdtError> {
        self.get_or_load(document_id).await?;
        self.maybe_activate_epoch(document_id).await?;
        Ok(())
    }

    pub async fn read_update(&self, document_id: &str) -> Result<Vec<u8>, CrdtError> {
        let (_, document) = self.get_or_load(document_id).await?;
        document.read_update().await
    }

    /// Run a read-only closure against the live document. Cheaper than
    /// [`Self::read_update`] for point lookups: no full-state encode, and it
    /// does not wait on the mutation lock held by writers.
    pub async fn read_with<R>(
        &self,
        document_id: &str,
        read: impl FnOnce(&Doc) -> R,
    ) -> Result<R, CrdtError> {
        let (_, document) = self.get_or_load(document_id).await?;
        document.read_with(read)
    }

    pub(crate) async fn read_update_with_epoch(
        &self,
        document_id: &str,
    ) -> Result<(u64, Vec<u8>), CrdtError> {
        let write_gate = self.write_gate(document_id).await?;
        let _write_guard = write_gate.lock().await;
        let (epoch, document) = self.get_or_load(document_id).await?;
        document.read_update().await.map(|update| (epoch, update))
    }

    async fn read_update_for_epoch(
        &self,
        document_id: &str,
        expected_epoch: u64,
    ) -> Result<Vec<u8>, CrdtError> {
        let write_gate = self.write_gate(document_id).await?;
        let _write_guard = write_gate.lock().await;
        let (epoch, document) = self.get_or_load(document_id).await?;
        if expected_epoch != epoch {
            return Err(CrdtError::RetiredEpoch {
                document_id: document_id.to_string(),
                requested: expected_epoch,
                current: epoch,
            });
        }
        document.read_update().await
    }

    pub async fn apply_update(&self, document_id: &str, update: &[u8]) -> Result<(), CrdtError> {
        self.apply_update_for_epoch(document_id, None, update).await
    }

    pub(crate) async fn apply_update_at_epoch(
        &self,
        document_id: &str,
        expected_epoch: u64,
        update: &[u8],
    ) -> Result<(), CrdtError> {
        self.apply_update_for_epoch(document_id, Some(expected_epoch), update)
            .await
    }

    async fn apply_update_for_epoch(
        &self,
        document_id: &str,
        expected_epoch: Option<u64>,
        update: &[u8],
    ) -> Result<(), CrdtError> {
        let write_gate = self.write_gate(document_id).await?;
        let _write_guard = write_gate.lock().await;
        let (epoch, document) = self.get_or_load(document_id).await?;
        if let Some(expected_epoch) = expected_epoch {
            if expected_epoch != epoch {
                return Err(CrdtError::RetiredEpoch {
                    document_id: document_id.to_string(),
                    requested: expected_epoch,
                    current: epoch,
                });
            }
        }
        self.reject_server_write_during_transition(document_id)
            .await?;
        #[cfg(test)]
        let write_pause = self.0.write_pauses.lock().await.remove(document_id);
        #[cfg(test)]
        if let Some(pause) = write_pause {
            let _ = pause.reached.send(());
            let _ = pause.resume.await;
        }
        document.apply_update(update).await?;
        drop(_write_guard);
        if let Err(error) = self.after_write(document_id, epoch, &document).await {
            tracing::error!(
                %error,
                %document_id,
                epoch,
                "post-write CRDT maintenance failed"
            );
        }
        Ok(())
    }

    /// Force a compacted generation for offline maintenance and benchmarks.
    pub async fn compact_document(&self, document_id: &str) -> Result<(), CrdtError> {
        let (_, document) = self.get_or_load(document_id).await?;
        document.compact().await
    }

    /// Open an authenticated-in-process peer used by server features and
    /// integration tests. External callers must use the token-validated routes.
    pub async fn connect_internal(
        &self,
        document_id: &str,
        level: Level,
    ) -> Result<CrdtConnection, CrdtError> {
        self.connect(document_id, level, None, None).await
    }

    pub async fn document_bytes_for_vault(&self, vault_id: &str) -> Result<u64, CrdtError> {
        validate_document_id(vault_id)?;
        let prefix = format!("{vault_id}__");
        let manifests = crdt_epoch::manifests_for_vault(&self.0.directory, vault_id)
            .await
            .map_err(epoch_error)?;
        let mut physical_ids = std::collections::HashSet::new();
        for manifest in manifests {
            physical_ids.insert(manifest.current.physical_document_id);
            if let Some(pending) = manifest.pending {
                physical_ids.insert(pending.physical_document_id);
            }
            physical_ids.extend(
                manifest
                    .retired
                    .into_iter()
                    .map(|retired| retired.physical_document_id),
            );
        }
        let mut total = 0;
        let mut entries = tokio::fs::read_dir(&self.0.directory).await?;
        while let Some(entry) = entries.next_entry().await? {
            let Some(name) = entry.file_name().to_str().map(str::to_string) else {
                continue;
            };
            let document_id = name
                .strip_suffix(".crdt")
                .or_else(|| name.strip_suffix(".yjs"));
            let Some(document_id) = document_id else {
                continue;
            };
            if document_id != vault_id
                && !document_id.starts_with(&prefix)
                && !physical_ids.contains(document_id)
            {
                continue;
            }
            let metadata = entry.metadata().await?;
            if metadata.is_file() {
                total += metadata.len();
            } else if metadata.is_dir() {
                let mut files = tokio::fs::read_dir(entry.path()).await?;
                while let Some(file) = files.next_entry().await? {
                    let metadata = file.metadata().await?;
                    if metadata.is_file() {
                        total += metadata.len();
                    }
                }
            }
        }
        Ok(total)
    }

    pub async fn document_count_for_vault(&self, vault_id: &str) -> Result<u64, CrdtError> {
        validate_document_id(vault_id)?;
        let manifests = crdt_epoch::manifests_for_vault(&self.0.directory, vault_id)
            .await
            .map_err(epoch_error)?;
        Ok(manifests
            .iter()
            .filter(|manifest| {
                manifest.logical_document_id != vault_id
                    && crate::plugindb::parse_doc_id(&manifest.logical_document_id).is_none()
            })
            .count() as u64)
    }

    pub async fn document_exists(&self, document_id: &str) -> Result<bool, CrdtError> {
        validate_document_id(document_id)?;
        if crdt_epoch::manifest_exists(&self.0.directory, document_id)
            .await
            .map_err(epoch_error)?
        {
            return Ok(true);
        }
        let physical_manifest = self
            .0
            .directory
            .join(format!("{document_id}.crdt/manifest.json"));
        let legacy_snapshot = self.0.directory.join(format!("{document_id}.yjs"));
        Ok(tokio::fs::try_exists(physical_manifest).await?
            || tokio::fs::try_exists(legacy_snapshot).await?)
    }

    pub(crate) async fn connect(
        &self,
        document_id: &str,
        level: Level,
        attribution: Option<Arc<Attribution>>,
        requested_epoch: Option<u64>,
    ) -> Result<CrdtConnection, CrdtError> {
        let (epoch, document) = self.get_or_load(document_id).await?;
        if let Some(requested) = requested_epoch {
            if requested != epoch {
                return Err(CrdtError::RetiredEpoch {
                    document_id: document_id.to_string(),
                    requested,
                    current: epoch,
                });
            }
        }
        let connection_id = self.0.next_connection_id.fetch_add(1, Ordering::Relaxed);
        // Subscribe before publishing the connection in the epoch runtime. A
        // proposal broadcast after registration is then queued even if the
        // connection task has not started polling yet.
        let events = document.subscribe();
        let runtime = self.epoch_runtime(document_id).await?;
        let pending_epoch = {
            let mut runtime = runtime.lock().await;
            if runtime.activating {
                let pending_epoch = runtime
                    .manifest
                    .pending
                    .as_ref()
                    .map(|pending| pending.epoch)
                    .unwrap_or_else(|| runtime.manifest.current.epoch.saturating_add(1));
                return Err(CrdtError::EpochTransition {
                    document_id: document_id.to_string(),
                    epoch: pending_epoch,
                });
            }
            if runtime.manifest.current.epoch != epoch {
                return Err(CrdtError::RetiredEpoch {
                    document_id: document_id.to_string(),
                    requested: epoch,
                    current: runtime.manifest.current.epoch,
                });
            }
            runtime.connections.insert(
                connection_id,
                EpochConnectionState {
                    epoch,
                    acknowledged: false,
                },
            );
            runtime
                .manifest
                .pending
                .as_ref()
                .map(|pending| pending.epoch)
        };
        let (to_server, from_client) = mpsc::channel(CONNECTION_CAPACITY);
        let (to_client, from_server) = mpsc::channel(CONNECTION_CAPACITY);
        #[cfg(test)]
        let connection_pause = self.0.connection_pauses.lock().await.remove(document_id);
        #[cfg(test)]
        if let Some(pause) = connection_pause {
            let _ = pause.reached.send(());
            let _ = pause.resume.await;
        }
        self.0.sync_metrics.crdt_connection_opened();
        tokio::spawn(run_connection(
            self.clone(),
            document_id.to_string(),
            epoch,
            connection_id,
            document,
            level,
            from_client,
            to_client,
            attribution,
            pending_epoch,
            events,
        ));
        Ok(CrdtConnection {
            sender: to_server,
            receiver: from_server,
        })
    }

    async fn get_or_load(
        &self,
        document_id: &str,
    ) -> Result<(u64, Arc<PersistentDocument>), CrdtError> {
        validate_document_id(document_id)?;
        let load_lock = self.document_lock(document_id).await;
        let _load_guard = load_lock.lock().await;
        let runtime = self.epoch_runtime_initialized(document_id).await?;
        let (epoch, physical_document_id) = {
            let runtime = runtime.lock().await;
            (
                runtime.manifest.current.epoch,
                runtime.manifest.current.physical_document_id.clone(),
            )
        };

        if let Some(document) = self.live_document(document_id, epoch).await {
            return Ok((epoch, document));
        }

        let loaded = crdt_storage::load_or_create(&self.0.directory, &physical_document_id).await?;
        let document = Arc::new(PersistentDocument::new(
            loaded.doc,
            loaded.persistence,
            self.0.sync_metrics.clone(),
        ));

        self.0.documents.lock().await.insert(
            document_id.to_string(),
            LiveDocument {
                epoch,
                document: Arc::downgrade(&document),
            },
        );
        Ok((epoch, document))
    }

    async fn live_document(
        &self,
        document_id: &str,
        epoch: u64,
    ) -> Option<Arc<PersistentDocument>> {
        let mut documents = self.0.documents.lock().await;
        documents.retain(|_, live| live.document.strong_count() > 0);
        documents
            .get(document_id)
            .filter(|live| live.epoch == epoch)
            .and_then(|live| live.document.upgrade())
    }

    async fn document_lock(&self, document_id: &str) -> Arc<Mutex<()>> {
        let mut locks = self.0.load_locks.lock().await;
        locks.retain(|_, lock| lock.strong_count() > 0);
        match locks.get(document_id).and_then(Weak::upgrade) {
            Some(lock) => lock,
            None => {
                let lock = Arc::new(Mutex::new(()));
                locks.insert(document_id.to_string(), Arc::downgrade(&lock));
                lock
            }
        }
    }

    async fn epoch_runtime(
        &self,
        document_id: &str,
    ) -> Result<Arc<Mutex<EpochRuntime>>, CrdtError> {
        if let Some(runtime) = self.0.epochs.lock().await.get(document_id).cloned() {
            return Ok(runtime);
        }
        let document_lock = self.document_lock(document_id).await;
        let _initialization = document_lock.lock().await;
        self.epoch_runtime_initialized(document_id).await
    }

    async fn epoch_runtime_initialized(
        &self,
        document_id: &str,
    ) -> Result<Arc<Mutex<EpochRuntime>>, CrdtError> {
        if let Some(runtime) = self.0.epochs.lock().await.get(document_id).cloned() {
            return Ok(runtime);
        }
        let mut manifest = crdt_epoch::load_or_create_manifest(&self.0.directory, document_id)
            .await
            .map_err(epoch_error)?;
        crdt_epoch::remove_expired(&self.0.directory, &mut manifest, crdt_epoch::now_millis())
            .await
            .map_err(epoch_error)?;
        let runtime = Arc::new(Mutex::new(EpochRuntime {
            manifest,
            connections: HashMap::new(),
            activating: false,
            write_gate: Arc::new(Mutex::new(())),
        }));
        let retirement_deadline = runtime
            .lock()
            .await
            .manifest
            .retired
            .iter()
            .map(|retired| retired.delete_after_ms)
            .min();
        self.0
            .epochs
            .lock()
            .await
            .insert(document_id.to_string(), runtime.clone());
        if let Some(deadline) = retirement_deadline {
            self.schedule_retired_cleanup(document_id, deadline);
        }
        Ok(runtime)
    }

    pub async fn current_epoch(&self, document_id: &str) -> Result<u64, CrdtError> {
        validate_document_id(document_id)?;
        let runtime = self.epoch_runtime(document_id).await?;
        let epoch = runtime.lock().await.manifest.current.epoch;
        Ok(epoch)
    }

    pub async fn epoch_metrics(
        &self,
        document_id: &str,
    ) -> Result<DocumentEpochMetrics, CrdtError> {
        let (epoch, document) = self.get_or_load(document_id).await?;
        let (encoded_state_bytes, delete_set_bytes, update_count) =
            document.epoch_measurements().await?;
        let runtime = self.epoch_runtime(document_id).await?;
        let runtime = runtime.lock().await;
        if runtime.manifest.current.epoch != epoch {
            return Err(CrdtError::RetiredEpoch {
                document_id: document_id.to_string(),
                requested: epoch,
                current: runtime.manifest.current.epoch,
            });
        }
        let active_connections = runtime
            .connections
            .values()
            .filter(|connection| connection.epoch == epoch)
            .count() as u64;
        let now = crdt_epoch::now_millis();
        let started_at_ms = runtime.manifest.current.started_at_ms;
        let baseline_encoded_state_bytes = runtime.manifest.current.baseline_encoded_state_bytes;
        let baseline_delete_set_bytes = runtime.manifest.current.baseline_delete_set_bytes;
        Ok(DocumentEpochMetrics {
            epoch,
            encoded_state_bytes,
            delete_set_bytes,
            baseline_encoded_state_bytes,
            baseline_delete_set_bytes,
            update_count,
            active_connections,
            started_at_ms,
            age_ms: now.saturating_sub(started_at_ms),
        })
    }

    pub async fn begin_epoch_transition(&self, document_id: &str) -> Result<u64, CrdtError> {
        self.begin_epoch_transition_for(document_id, None)
            .await?
            .ok_or_else(|| CrdtError::Protocol("document epoch changed before proposal".into()))
    }

    async fn begin_epoch_transition_for(
        &self,
        document_id: &str,
        expected_epoch: Option<u64>,
    ) -> Result<Option<u64>, CrdtError> {
        let write_gate = self.write_gate(document_id).await?;
        let write_guard = write_gate.lock().await;
        let (current_epoch, document) = self.get_or_load(document_id).await?;
        if expected_epoch.is_some_and(|expected| expected != current_epoch) {
            return Ok(None);
        }
        // Reject deterministic schema/type incompatibilities before publishing
        // a proposal. Activation rebuilds again after all acknowledgements so
        // it still includes writes that were already in flight.
        document.logical_replacement().await?;
        let runtime = self.epoch_runtime(document_id).await?;
        let (pending_epoch, activate_now) = {
            let mut runtime = runtime.lock().await;
            if runtime.manifest.current.epoch != current_epoch {
                return Err(CrdtError::RetiredEpoch {
                    document_id: document_id.to_string(),
                    requested: current_epoch,
                    current: runtime.manifest.current.epoch,
                });
            }
            let pending_epoch = runtime
                .manifest
                .begin(crdt_epoch::now_millis())
                .map_err(epoch_error)?
                .epoch;
            for connection in runtime.connections.values_mut() {
                if connection.epoch == current_epoch {
                    connection.acknowledged = false;
                }
            }
            crdt_epoch::save_manifest(&self.0.directory, &runtime.manifest)
                .await
                .map_err(epoch_error)?;
            let activate_now = !runtime
                .connections
                .values()
                .any(|connection| connection.epoch == current_epoch);
            (pending_epoch, activate_now)
        };
        let _ = document
            .events
            .send(epoch_wire_message(EPOCH_PROPOSAL_MESSAGE, pending_epoch)?);
        drop(write_guard);
        if activate_now {
            self.maybe_activate_epoch(document_id).await?;
        }
        Ok(Some(pending_epoch))
    }

    async fn reject_server_write_during_transition(
        &self,
        document_id: &str,
    ) -> Result<(), CrdtError> {
        let runtime = self.epoch_runtime(document_id).await?;
        let runtime = runtime.lock().await;
        if let Some(pending) = &runtime.manifest.pending {
            return Err(CrdtError::EpochTransition {
                document_id: document_id.to_string(),
                epoch: pending.epoch,
            });
        }
        Ok(())
    }

    async fn connection_can_write(
        &self,
        document_id: &str,
        connection_id: u64,
        epoch: u64,
    ) -> Result<(), CrdtError> {
        let runtime = self.epoch_runtime(document_id).await?;
        let runtime = runtime.lock().await;
        if runtime.manifest.current.epoch != epoch {
            return Err(CrdtError::RetiredEpoch {
                document_id: document_id.to_string(),
                requested: epoch,
                current: runtime.manifest.current.epoch,
            });
        }
        if let Some(connection) = runtime.connections.get(&connection_id) {
            if connection.acknowledged {
                let pending_epoch = runtime
                    .manifest
                    .pending
                    .as_ref()
                    .map(|pending| pending.epoch)
                    .unwrap_or(epoch.saturating_add(1));
                return Err(CrdtError::EpochTransition {
                    document_id: document_id.to_string(),
                    epoch: pending_epoch,
                });
            }
        }
        Ok(())
    }

    async fn apply_connection_update(
        &self,
        document_id: &str,
        connection_id: u64,
        epoch: u64,
        document: &Arc<PersistentDocument>,
        update: &[u8],
    ) -> Result<(), CrdtError> {
        let write_gate = self.write_gate(document_id).await?;
        let _write_guard = write_gate.lock().await;
        self.connection_can_write(document_id, connection_id, epoch)
            .await?;
        #[cfg(test)]
        let write_pause = self
            .0
            .connection_write_pauses
            .lock()
            .await
            .remove(document_id);
        #[cfg(test)]
        if let Some(pause) = write_pause {
            let _ = pause.reached.send(());
            let _ = pause.resume.await;
        }
        document.apply_update(update).await
    }

    async fn acknowledge_epoch(
        &self,
        document_id: &str,
        connection_id: u64,
        epoch: u64,
    ) -> Result<(), CrdtError> {
        let runtime = self.epoch_runtime(document_id).await?;
        {
            let mut runtime = runtime.lock().await;
            let expected = runtime
                .manifest
                .pending
                .as_ref()
                .map(|pending| pending.epoch)
                .ok_or_else(|| CrdtError::Protocol("no epoch transition is pending".into()))?;
            if epoch != expected {
                return Err(CrdtError::Protocol(format!(
                    "acknowledged epoch {epoch}, expected {expected}"
                )));
            }
            let current_epoch = runtime.manifest.current.epoch;
            let connection = runtime
                .connections
                .get_mut(&connection_id)
                .ok_or_else(|| CrdtError::Protocol("unknown epoch connection".into()))?;
            if connection.epoch != current_epoch {
                return Err(CrdtError::RetiredEpoch {
                    document_id: document_id.to_string(),
                    requested: connection.epoch,
                    current: current_epoch,
                });
            }
            connection.acknowledged = true;
        }
        self.maybe_activate_epoch(document_id).await
    }

    async fn connection_closed(&self, document_id: &str, connection_id: u64) {
        let Ok(runtime) = self.epoch_runtime(document_id).await else {
            return;
        };
        let had_pending = {
            let mut runtime = runtime.lock().await;
            runtime.connections.remove(&connection_id);
            runtime.manifest.pending.is_some()
        };
        if had_pending {
            if let Err(error) = self.maybe_activate_epoch(document_id).await {
                tracing::error!(%error, %document_id, "failed to activate document epoch");
            }
        }
    }

    async fn after_write(
        &self,
        document_id: &str,
        epoch: u64,
        document: &Arc<PersistentDocument>,
    ) -> Result<(), CrdtError> {
        self.notify_parent_invalidated(document_id).await?;
        let runtime = self.epoch_runtime(document_id).await?;
        if runtime.lock().await.manifest.pending.is_some() {
            return Ok(());
        }
        let (encoded_state_bytes, delete_set_bytes, update_count) =
            document.epoch_measurements().await?;
        let (
            started_at_ms,
            baseline_encoded_state_bytes,
            baseline_delete_set_bytes,
            active_connections,
        ) = {
            let runtime = runtime.lock().await;
            if runtime.manifest.current.epoch != epoch {
                return Ok(());
            }
            (
                runtime.manifest.current.started_at_ms,
                runtime.manifest.current.baseline_encoded_state_bytes,
                runtime.manifest.current.baseline_delete_set_bytes,
                runtime
                    .connections
                    .values()
                    .filter(|connection| connection.epoch == epoch)
                    .count() as u64,
            )
        };
        let metrics = DocumentEpochMetrics {
            epoch,
            encoded_state_bytes,
            delete_set_bytes,
            baseline_encoded_state_bytes,
            baseline_delete_set_bytes,
            update_count,
            active_connections,
            started_at_ms,
            age_ms: crdt_epoch::now_millis().saturating_sub(started_at_ms),
        };
        if self.0.policy.should_rollover(&metrics) {
            self.begin_epoch_transition_for(document_id, Some(epoch))
                .await?;
        }
        Ok(())
    }

    /// Tell live vault-index clients that one child document has a new durable
    /// state. The notification is advisory and intentionally not persisted:
    /// reconnecting clients perform a bounded catch-up sweep.
    async fn notify_parent_invalidated(&self, document_id: &str) -> Result<(), CrdtError> {
        let Some((parent_id, child_id)) = document_id.split_once("__") else {
            return Ok(());
        };
        if parent_id.is_empty() || child_id.is_empty() {
            return Ok(());
        }
        let parent = self
            .0
            .documents
            .lock()
            .await
            .get(parent_id)
            .and_then(|entry| entry.document.upgrade());
        if let Some(parent) = parent {
            parent.publish(document_invalidation_message(document_id)?);
        }
        Ok(())
    }

    async fn maybe_activate_epoch(&self, document_id: &str) -> Result<(), CrdtError> {
        let write_gate = self.write_gate(document_id).await?;
        let _write_guard = write_gate.lock().await;
        let runtime = self.epoch_runtime(document_id).await?;
        let (current_epoch, pending_epoch, pending_physical) = {
            let mut runtime = runtime.lock().await;
            let Some(pending) = runtime.manifest.pending.clone() else {
                return Ok(());
            };
            let current_epoch = runtime.manifest.current.epoch;
            let waiting = runtime
                .connections
                .values()
                .any(|connection| connection.epoch == current_epoch && !connection.acknowledged);
            let timed_out = crdt_epoch::now_millis().saturating_sub(pending.proposed_at_ms)
                >= EPOCH_ACK_TIMEOUT_MS;
            if runtime.activating || (waiting && !timed_out) {
                return Ok(());
            }
            if waiting {
                tracing::warn!(
                    document_id,
                    pending_epoch = pending.epoch,
                    "activating document epoch after ack timeout"
                );
            }
            runtime.activating = true;
            (current_epoch, pending.epoch, pending.physical_document_id)
        };

        let result = async {
            let (_, current_document) = self.get_or_load(document_id).await?;
            let replacement = current_document.logical_replacement().await?;
            let loaded = crdt_storage::create_or_load_replacement(
                &self.0.directory,
                &pending_physical,
                replacement,
            )
            .await?;
            let (baseline_encoded_state_bytes, baseline_delete_set_bytes) =
                crdt_epoch::document_measurements(&loaded.doc);

            {
                let mut runtime = runtime.lock().await;
                let still_pending = runtime
                    .manifest
                    .pending
                    .as_ref()
                    .is_some_and(|pending| pending.epoch == pending_epoch);
                if runtime.manifest.current.epoch != current_epoch || !still_pending {
                    return Err(CrdtError::Protocol(
                        "document epoch changed during activation".into(),
                    ));
                }
                runtime
                    .manifest
                    .activate(
                        crdt_epoch::now_millis(),
                        self.0.policy.recovery_window_ms,
                        baseline_encoded_state_bytes,
                        baseline_delete_set_bytes,
                    )
                    .map_err(epoch_error)?;
                crdt_epoch::save_manifest(&self.0.directory, &runtime.manifest)
                    .await
                    .map_err(epoch_error)?;
                if let Err(error) = crdt_epoch::remove_expired(
                    &self.0.directory,
                    &mut runtime.manifest,
                    crdt_epoch::now_millis(),
                )
                .await
                {
                    // Activation is already committed. Keep the retired entry
                    // in the manifest and retry deletion on a later reload.
                    tracing::warn!(%error, %document_id, "failed to remove retired document epoch");
                }
                runtime
                    .connections
                    .retain(|_, connection| connection.epoch == pending_epoch);
                runtime.activating = false;
            }

            let document = Arc::new(PersistentDocument::new(
                loaded.doc,
                loaded.persistence,
                self.0.sync_metrics.clone(),
            ));
            self.0.documents.lock().await.insert(
                document_id.to_string(),
                LiveDocument {
                    epoch: pending_epoch,
                    document: Arc::downgrade(&document),
                },
            );
            Ok(())
        }
        .await;

        if result.is_err() {
            runtime.lock().await.activating = false;
        } else if let Some(deadline) = runtime
            .lock()
            .await
            .manifest
            .retired
            .iter()
            .map(|retired| retired.delete_after_ms)
            .min()
        {
            self.schedule_retired_cleanup(document_id, deadline);
        }
        result
    }

    fn schedule_retired_cleanup(&self, document_id: &str, deadline_ms: u64) {
        let Ok(mut deadlines) = self.0.retirement_deadlines.lock() else {
            tracing::warn!(%document_id, "retired epoch scheduler lock poisoned");
            return;
        };
        if deadlines
            .get(document_id)
            .is_some_and(|scheduled| *scheduled <= deadline_ms)
        {
            return;
        }
        deadlines.insert(document_id.to_string(), deadline_ms);
        drop(deadlines);

        let store = self.clone();
        let document_id = document_id.to_string();
        tokio::spawn(async move {
            let delay_ms = deadline_ms.saturating_sub(crdt_epoch::now_millis());
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            let should_run = {
                let Ok(mut deadlines) = store.0.retirement_deadlines.lock() else {
                    tracing::warn!(%document_id, "retired epoch scheduler lock poisoned");
                    return;
                };
                if deadlines.get(&document_id) == Some(&deadline_ms) {
                    deadlines.remove(&document_id);
                    true
                } else {
                    false
                }
            };
            if should_run {
                if let Err(error) = store.cleanup_retired_epochs(&document_id).await {
                    tracing::warn!(%error, %document_id, "failed to remove retired document epoch");
                    store.schedule_retired_cleanup(
                        &document_id,
                        crdt_epoch::now_millis().saturating_add(RETIRED_EPOCH_RETRY_MS),
                    );
                }
            }
        });
    }

    async fn cleanup_retired_epochs(&self, document_id: &str) -> Result<(), CrdtError> {
        let runtime = self.epoch_runtime(document_id).await?;
        let next_deadline = {
            let mut runtime = runtime.lock().await;
            crdt_epoch::remove_expired(
                &self.0.directory,
                &mut runtime.manifest,
                crdt_epoch::now_millis(),
            )
            .await
            .map_err(epoch_error)?;
            runtime
                .manifest
                .retired
                .iter()
                .map(|retired| retired.delete_after_ms)
                .min()
        };
        if let Some(deadline) = next_deadline {
            self.schedule_retired_cleanup(document_id, deadline);
        }
        Ok(())
    }

    async fn write_gate(&self, document_id: &str) -> Result<Arc<Mutex<()>>, CrdtError> {
        let runtime = self.epoch_runtime(document_id).await?;
        let write_gate = runtime.lock().await.write_gate.clone();
        Ok(write_gate)
    }

    #[cfg(test)]
    async fn pause_next_write(
        &self,
        document_id: &str,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
        let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
        self.0.write_pauses.lock().await.insert(
            document_id.to_string(),
            WritePause {
                reached: reached_tx,
                resume: resume_rx,
            },
        );
        (reached_rx, resume_tx)
    }

    #[cfg(test)]
    async fn pause_next_connection_write_after_check(
        &self,
        document_id: &str,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
        let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
        self.0.connection_write_pauses.lock().await.insert(
            document_id.to_string(),
            WritePause {
                reached: reached_tx,
                resume: resume_rx,
            },
        );
        (reached_rx, resume_tx)
    }

    #[cfg(test)]
    async fn pause_next_connection_after_registration(
        &self,
        document_id: &str,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
        let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
        self.0.connection_pauses.lock().await.insert(
            document_id.to_string(),
            ConnectionPause {
                reached: reached_tx,
                resume: resume_rx,
            },
        );
        (reached_rx, resume_tx)
    }
}

fn epoch_error(error: crdt_epoch::EpochError) -> CrdtError {
    CrdtError::Storage(error.to_string())
}

fn validate_document_id(document_id: &str) -> Result<(), CrdtError> {
    if !document_id.is_empty()
        && document_id.len() <= 255
        && document_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        Ok(())
    } else {
        Err(CrdtError::InvalidDocumentId)
    }
}

struct PersistentDocument {
    awareness: RwLock<Awareness>,
    mutation: Mutex<()>,
    persistence: Mutex<DocumentPersistence>,
    compaction_scheduled: AtomicBool,
    events: broadcast::Sender<Vec<u8>>,
    sync_metrics: SyncMetrics,
}

impl PersistentDocument {
    fn new(doc: Doc, persistence: DocumentPersistence, sync_metrics: SyncMetrics) -> Self {
        let (events, _) = broadcast::channel(BROADCAST_CAPACITY);
        Self {
            awareness: RwLock::new(Awareness::new(doc)),
            mutation: Mutex::new(()),
            persistence: Mutex::new(persistence),
            compaction_scheduled: AtomicBool::new(false),
            events,
            sync_metrics,
        }
    }

    fn read_with<R>(&self, read: impl FnOnce(&Doc) -> R) -> Result<R, CrdtError> {
        let awareness = self
            .awareness
            .read()
            .map_err(|_| CrdtError::Protocol("document lock poisoned".into()))?;
        Ok(read(awareness.doc()))
    }

    fn snapshot(&self) -> Result<Vec<u8>, CrdtError> {
        let awareness = self
            .awareness
            .read()
            .map_err(|_| CrdtError::Protocol("document lock poisoned".into()))?;
        let snapshot = awareness
            .doc()
            .transact()
            .encode_state_as_update_v1(&StateVector::default());
        Ok(snapshot)
    }

    async fn read_update(&self) -> Result<Vec<u8>, CrdtError> {
        let _mutation = self.mutation.lock().await;
        self.snapshot()
    }

    async fn epoch_measurements(&self) -> Result<(u64, u64, u64), CrdtError> {
        let _mutation = self.mutation.lock().await;
        let doc = {
            let awareness = self
                .awareness
                .read()
                .map_err(|_| CrdtError::Protocol("document lock poisoned".into()))?;
            awareness.doc().clone()
        };
        let (encoded_state_bytes, delete_set_bytes) =
            tokio::task::spawn_blocking(move || crdt_epoch::document_measurements(&doc))
                .await
                .map_err(|error| {
                    CrdtError::Protocol(format!("epoch measurements join: {error}"))
                })?;
        let update_count = self.persistence.lock().await.total_records();
        Ok((encoded_state_bytes, delete_set_bytes, update_count))
    }

    async fn logical_replacement(&self) -> Result<Doc, CrdtError> {
        let _mutation = self.mutation.lock().await;
        let awareness = self
            .awareness
            .read()
            .map_err(|_| CrdtError::Protocol("document lock poisoned".into()))?;
        crdt_epoch::replacement_doc(awareness.doc()).map_err(epoch_error)
    }

    /// Append-before-publish transaction.
    ///
    /// A checksummed update record is fsynced before the update reaches the live
    /// document or any peer. A failed append therefore cannot mutate
    /// acknowledged state. Compaction runs after publication, except when the
    /// next record would cross the replay limit; that boundary compacts
    /// synchronously before the append.
    async fn apply_update(self: &Arc<Self>, update: &[u8]) -> Result<(), CrdtError> {
        let started = Instant::now();
        let result = self.apply_update_inner(update).await;
        self.sync_metrics
            .observe_update(update.len(), started.elapsed(), result.is_ok());
        result
    }

    async fn apply_update_inner(self: &Arc<Self>, update: &[u8]) -> Result<(), CrdtError> {
        if update.len() > MAX_UPDATE_BYTES {
            return Err(CrdtError::UpdateTooLarge);
        }
        // Decode + preflight the update against a disposable empty document
        // before it reaches durable storage or the live awareness document.
        // validate_update returns the validated `Update` for the live apply.
        let decoded = crate::safe_yrs::validate_update(update)
            .map_err(|error| CrdtError::InvalidUpdate(error.to_string()))?;

        let _mutation = self.mutation.lock().await;
        let requires_compaction = self
            .persistence
            .lock()
            .await
            .requires_compaction(update.len())?;
        if requires_compaction {
            self.compact_locked().await?;
        }
        let should_compact = {
            let mut persistence = self.persistence.lock().await;
            persistence.append_update(update).await?;
            persistence.should_compact()
        };

        {
            let mut awareness = self
                .awareness
                .write()
                .map_err(|_| CrdtError::Protocol("document lock poisoned".into()))?;
            awareness.doc_mut().transact_mut().apply_update(decoded);
        }

        let message = Message::Sync(SyncMessage::Update(update.to_vec())).encode_v1();
        let _ = self.events.send(message);
        drop(_mutation);
        if should_compact
            && self
                .compaction_scheduled
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        {
            let document = self.clone();
            tokio::spawn(async move {
                document.compact_if_needed().await;
                document
                    .compaction_scheduled
                    .store(false, Ordering::Release);
            });
        }
        Ok(())
    }

    async fn compact_if_needed(&self) {
        let _mutation = self.mutation.lock().await;
        let should_compact = self.persistence.lock().await.should_compact();
        if !should_compact {
            return;
        }
        if let Err(error) = self.compact_locked().await {
            tracing::error!(%error, "failed to compact CRDT document");
        }
    }

    async fn compact(&self) -> Result<(), CrdtError> {
        let _mutation = self.mutation.lock().await;
        self.compact_locked().await
    }

    async fn compact_locked(&self) -> Result<(), CrdtError> {
        let started = Instant::now();
        let result = self.compact_locked_inner().await;
        self.sync_metrics
            .observe_compaction(started.elapsed(), result.is_ok());
        result
    }

    async fn compact_locked_inner(&self) -> Result<(), CrdtError> {
        let snapshot = match self.snapshot() {
            Ok(snapshot) => snapshot,
            Err(error) => {
                return Err(error);
            }
        };
        self.persistence.lock().await.compact(&snapshot).await?;
        Ok(())
    }

    fn subscribe(&self) -> broadcast::Receiver<Vec<u8>> {
        self.events.subscribe()
    }

    fn publish(&self, message: Vec<u8>) {
        let _ = self.events.send(message);
    }

    fn initial_messages(&self) -> Result<[Vec<u8>; 2], CrdtError> {
        let awareness = self
            .awareness
            .read()
            .map_err(|_| CrdtError::Protocol("document lock poisoned".into()))?;
        let state_vector = awareness.doc().transact().state_vector();
        let awareness_update = awareness
            .update()
            .map_err(|error| CrdtError::Protocol(error.to_string()))?;
        Ok([
            Message::Sync(SyncMessage::SyncStep1(state_vector)).encode_v1(),
            Message::Awareness(awareness_update).encode_v1(),
        ])
    }

    async fn sync_step2(&self, state_vector: &StateVector) -> Result<Vec<u8>, CrdtError> {
        // A new peer must not declare its initial sync complete from a snapshot
        // taken while another update is still in the persist-before-publish
        // phase. Waiting here makes the SyncStep2 a high-water mark over every
        // mutation already accepted by this document.
        let _mutation = self.mutation.lock().await;
        let awareness = self
            .awareness
            .read()
            .map_err(|_| CrdtError::Protocol("document lock poisoned".into()))?;
        let update = awareness
            .doc()
            .transact()
            .encode_state_as_update_v1(state_vector);
        Ok(Message::Sync(SyncMessage::SyncStep2(update)).encode_v1())
    }

    fn awareness_snapshot(&self) -> Result<Vec<u8>, CrdtError> {
        let awareness = self
            .awareness
            .read()
            .map_err(|_| CrdtError::Protocol("document lock poisoned".into()))?;
        let update = awareness
            .update()
            .map_err(|error| CrdtError::Protocol(error.to_string()))?;
        Ok(Message::Awareness(update).encode_v1())
    }

    fn apply_awareness(&self, update: yrs::sync::AwarenessUpdate) -> Result<(), CrdtError> {
        let encoded = Message::Awareness(update.clone()).encode_v1();
        let changed = self
            .awareness
            .write()
            .map_err(|_| CrdtError::Protocol("document lock poisoned".into()))?
            .apply_update_summary(update)
            .map_err(|error| CrdtError::Protocol(error.to_string()))?
            .is_some();
        if changed {
            let _ = self.events.send(encoded);
        }
        Ok(())
    }

    fn remove_awareness(&self, client_id: u64) -> Result<(), CrdtError> {
        let message = {
            let mut awareness = self
                .awareness
                .write()
                .map_err(|_| CrdtError::Protocol("document lock poisoned".into()))?;
            awareness.remove_state(client_id);
            let update = awareness
                .update_with_clients([client_id])
                .map_err(|error| CrdtError::Protocol(error.to_string()))?;
            Message::Awareness(update).encode_v1()
        };
        let _ = self.events.send(message);
        Ok(())
    }
}

#[cfg(test)]
fn decode_document(update: &[u8]) -> Result<Doc, CrdtError> {
    let decoded = crate::safe_yrs::validate_update(update)
        .map_err(|error| CrdtError::InvalidUpdate(error.to_string()))?;
    let doc = Doc::new();
    doc.transact_mut().apply_update(decoded);
    Ok(doc)
}

/// In-process endpoint used by the native WebSocket transports and server-side
/// streaming cursors.
pub struct CrdtConnection {
    sender: mpsc::Sender<Vec<u8>>,
    receiver: mpsc::Receiver<Vec<u8>>,
}

impl CrdtConnection {
    pub async fn send(&self, message: Vec<u8>) -> Result<(), CrdtError> {
        self.sender
            .send(message)
            .await
            .map_err(|_| CrdtError::Protocol("document connection closed".into()))
    }

    pub async fn recv(&mut self) -> Option<Vec<u8>> {
        self.receiver.recv().await
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_connection(
    store: DocumentStore,
    document_id: String,
    epoch: u64,
    connection_id: u64,
    document: Arc<PersistentDocument>,
    level: Level,
    mut incoming: mpsc::Receiver<Vec<u8>>,
    outgoing: mpsc::Sender<Vec<u8>>,
    attribution: Option<Arc<Attribution>>,
    pending_epoch: Option<u64>,
    mut events: broadcast::Receiver<Vec<u8>>,
) {
    let mut awareness_client = None;
    let result = async {
        for message in document.initial_messages()? {
            send_outgoing(&outgoing, message).await?;
        }
        if let Some(pending_epoch) = pending_epoch {
            send_outgoing(
                &outgoing,
                epoch_wire_message(EPOCH_PROPOSAL_MESSAGE, pending_epoch)?,
            )
            .await?;
        }

        loop {
            tokio::select! {
                incoming = incoming.recv() => {
                    let Some(bytes) = incoming else { break };
                    if bytes.len() > MAX_UPDATE_BYTES {
                        return Err(CrdtError::UpdateTooLarge);
                    }
                    let message = crate::safe_yrs::decode_message(&bytes)
                        .map_err(|error| CrdtError::Protocol(error.to_string()))?;
                    match message {
                        Message::Sync(SyncMessage::SyncStep1(state_vector)) => {
                            send_outgoing(
                                &outgoing,
                                document.sync_step2(&state_vector).await?,
                            )
                            .await?;
                        }
                        Message::Sync(SyncMessage::SyncStep2(update))
                        | Message::Sync(SyncMessage::Update(update)) => {
                            if level == Level::ReadOnly {
                                // `@y-sweet/client` answers the server's initial
                                // SyncStep1 with SyncStep2 even for read-only
                                // tokens. Ignore every client-authored update at
                                // this level, but keep the connection alive so
                                // it can continue receiving server updates.
                                tracing::debug!(
                                    "ignored client update on read-only CRDT connection"
                                );
                                continue;
                            }
                            store
                                .apply_connection_update(
                                    &document_id,
                                    connection_id,
                                    epoch,
                                    &document,
                                    &update,
                                )
                                .await?;
                            if let Some(attribution) = &attribution {
                                attribution.mark_content_write().await;
                            }
                            if let Err(error) =
                                store.after_write(&document_id, epoch, &document).await
                            {
                                tracing::error!(
                                    %error,
                                    %document_id,
                                    epoch,
                                    "post-write CRDT maintenance failed"
                                );
                            }
                        }
                        Message::Awareness(update) => {
                            if bytes.len() > MAX_AWARENESS_BYTES {
                                return Err(CrdtError::Protocol(format!(
                                    "awareness update exceeds {MAX_AWARENESS_BYTES} bytes"
                                )));
                            }
                            if update.clients.len() != 1 {
                                return Err(CrdtError::Protocol(
                                    "awareness update must contain exactly one client".into(),
                                ));
                            }
                            let client_id = *update.clients.keys().next().expect("one client");
                            match awareness_client {
                                Some(bound) if bound != client_id => {
                                    return Err(CrdtError::Protocol(
                                        "awareness client does not match this connection".into(),
                                    ));
                                }
                                None => awareness_client = Some(client_id),
                                _ => {}
                            }
                            document.apply_awareness(update)?;
                        }
                        Message::AwarenessQuery => {
                            send_outgoing(&outgoing, document.awareness_snapshot()?).await?;
                        }
                        Message::Auth(Some(reason)) => {
                            return Err(CrdtError::Protocol(format!(
                                "client denied authorization: {reason}"
                            )));
                        }
                        Message::Auth(None) => {}
                        Message::Custom(SYNC_STATUS_MESSAGE, payload) => {
                            send_outgoing(
                                &outgoing,
                                Message::Custom(SYNC_STATUS_MESSAGE, payload).encode_v1(),
                            )
                            .await?;
                        }
                        Message::Custom(EPOCH_ACK_MESSAGE, payload) => {
                            let acknowledged = decode_epoch_payload(&payload)?;
                            store
                                .acknowledge_epoch(
                                    &document_id,
                                    connection_id,
                                    acknowledged.epoch,
                                )
                                .await?;
                            break;
                        }
                        Message::Custom(tag, _) => {
                            return Err(CrdtError::Protocol(format!(
                                "unsupported message tag {tag}"
                            )));
                        }
                    }
                }
                event = events.recv() => {
                    match event {
                        Ok(message) => send_outgoing(&outgoing, message).await?,
                        Err(broadcast::error::RecvError::Lagged(_)) => {
                            store.0.sync_metrics.crdt_connection_lagged();
                            return Err(CrdtError::Protocol(
                                "connection lagged behind document updates".into(),
                            ));
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                }
            }
        }
        Ok::<(), CrdtError>(())
    }
    .await;

    if let Some(client_id) = awareness_client {
        let _ = document.remove_awareness(client_id);
    }
    store.connection_closed(&document_id, connection_id).await;
    store.0.sync_metrics.crdt_connection_closed();
    if let Err(error) = result {
        tracing::debug!("CRDT connection closed: {error}");
    }
}

async fn send_outgoing(
    outgoing: &mpsc::Sender<Vec<u8>>,
    message: Vec<u8>,
) -> Result<(), CrdtError> {
    outgoing
        .send(message)
        .await
        .map_err(|_| CrdtError::Protocol("client disconnected".into()))
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct EpochWirePayload {
    epoch: u64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DocumentInvalidatedWirePayload<'a> {
    document_id: &'a str,
}

fn epoch_wire_message(tag: u8, epoch: u64) -> Result<Vec<u8>, CrdtError> {
    let payload = serde_json::to_vec(&EpochWirePayload { epoch })
        .map_err(|error| CrdtError::Protocol(error.to_string()))?;
    Ok(Message::Custom(tag, payload).encode_v1())
}

fn document_invalidation_message(document_id: &str) -> Result<Vec<u8>, CrdtError> {
    let payload = serde_json::to_vec(&DocumentInvalidatedWirePayload { document_id })
        .map_err(|error| CrdtError::Protocol(error.to_string()))?;
    Ok(Message::Custom(DOCUMENT_INVALIDATED_MESSAGE, payload).encode_v1())
}

fn decode_epoch_payload(payload: &[u8]) -> Result<EpochWirePayload, CrdtError> {
    serde_json::from_slice(payload)
        .map_err(|error| CrdtError::Protocol(format!("invalid epoch acknowledgement: {error}")))
}

#[derive(Deserialize)]
pub struct TokenQuery {
    token: Option<String>,
}

/// Yjs WebSocket endpoint compatible with `@y-sweet/client` token URLs.
pub async fn websocket(
    State(state): State<AppState>,
    AxumPath((document_id, repeated_id)): AxumPath<(String, String)>,
    Query(query): Query<TokenQuery>,
    ws: WebSocketUpgrade,
) -> Response {
    if document_id != repeated_id {
        return AppError::BadRequest("document ids do not match".into()).into_response();
    }
    let Some(token) = query.token else {
        return AppError::Unauthorized.into_response();
    };
    let Some(grant) = state.sync_grant(&token, &document_id).await else {
        return AppError::Unauthorized.into_response();
    };
    let attribution = Arc::new(Attribution::new(
        state.clone(),
        &document_id,
        grant.principal,
    ));
    let connection = match state
        .documents
        .connect(
            &document_id,
            grant.level,
            Some(attribution),
            Some(grant.epoch),
        )
        .await
    {
        Ok(connection) => connection,
        Err(error) => return AppError::from(error).into_response(),
    };
    let sync_metrics = state.sync_metrics.clone();
    ws.on_upgrade(move |socket| bridge_websocket(socket, connection, sync_metrics))
}

async fn bridge_websocket(
    socket: WebSocket,
    mut connection: CrdtConnection,
    sync_metrics: SyncMetrics,
) {
    sync_metrics.physical_connection_opened();
    let (mut sink, mut stream) = socket.split();
    loop {
        tokio::select! {
            incoming = stream.next() => {
                match incoming {
                    Some(Ok(AxumMessage::Binary(bytes))) => {
                        if connection.send(bytes.to_vec()).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(AxumMessage::Close(_))) | None | Some(Err(_)) => break,
                    Some(Ok(_)) => {}
                }
            }
            outgoing = connection.recv() => {
                let Some(bytes) = outgoing else { break };
                if sink.send(AxumMessage::Binary(bytes.into())).await.is_err() {
                    break;
                }
            }
        }
    }
    sync_metrics.physical_connection_closed();
}

pub async fn get_update(
    State(state): State<AppState>,
    AxumPath(document_id): AxumPath<String>,
    headers: HeaderMap,
) -> Response {
    let Some(token) = bearer_from_headers(&headers) else {
        return AppError::Unauthorized.into_response();
    };
    let Some(grant) = state.sync_grant(token, &document_id).await else {
        return AppError::Unauthorized.into_response();
    };
    match state.documents.current_epoch(&document_id).await {
        Ok(epoch) if epoch == grant.epoch => {}
        Ok(epoch) => {
            return AppError::from(CrdtError::RetiredEpoch {
                document_id,
                requested: grant.epoch,
                current: epoch,
            })
            .into_response();
        }
        Err(error) => return AppError::from(error).into_response(),
    }
    match state
        .documents
        .read_update_for_epoch(&document_id, grant.epoch)
        .await
    {
        Ok(update) => (StatusCode::OK, update).into_response(),
        Err(error) => AppError::from(error).into_response(),
    }
}

pub async fn post_update(
    State(state): State<AppState>,
    AxumPath(document_id): AxumPath<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let Some(token) = bearer_from_headers(&headers) else {
        return AppError::Unauthorized.into_response();
    };
    let Some(grant) = state.sync_grant(token, &document_id).await else {
        return AppError::Unauthorized.into_response();
    };
    if grant.level != Level::Full {
        return AppError::Forbidden.into_response();
    }
    match state.documents.current_epoch(&document_id).await {
        Ok(epoch) if epoch == grant.epoch => {}
        Ok(epoch) => {
            return AppError::from(CrdtError::RetiredEpoch {
                document_id,
                requested: grant.epoch,
                current: epoch,
            })
            .into_response();
        }
        Err(error) => return AppError::from(error).into_response(),
    }
    match state
        .documents
        .apply_update_at_epoch(&document_id, grant.epoch, &body)
        .await
    {
        Ok(()) => {
            Attribution::new(state.clone(), &document_id, grant.principal)
                .mark_content_write()
                .await;
            StatusCode::OK.into_response()
        }
        Err(error) => AppError::from(error).into_response(),
    }
}

fn bearer_from_headers(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
}

pub async fn ensure_doc(state: &AppState, document_id: &str) -> AppResult<()> {
    state.documents.ensure_document(document_id).await?;
    Ok(())
}

/// Create the client-token shape expected by `@y-sweet/client`.
pub async fn mint_client_token(
    state: &AppState,
    document_id: &str,
    level: Level,
) -> AppResult<Value> {
    validate_document_id(document_id)?;
    let epoch = state.documents.current_epoch(document_id).await?;
    let (url, base_url) = client_urls(&state.config.public_base_url, document_id)
        .map_err(|error| AppError::Internal(error.to_string()))?;
    Ok(json!({
        "url": url,
        "baseUrl": base_url,
        "docId": document_id,
        "token": nanoid::nanoid!(48),
        "authorization": level.as_str(),
        "epoch": epoch,
    }))
}

fn client_urls(public_base_url: &str, document_id: &str) -> Result<(String, String), CrdtError> {
    let mut base = Url::parse(public_base_url)
        .map_err(|error| CrdtError::Protocol(format!("invalid PUBLIC_BASE_URL: {error}")))?;
    base.set_query(None);
    base.set_fragment(None);
    base.set_path(&format!("/d/{document_id}"));
    let base_url = base.to_string().trim_end_matches('/').to_string();

    let mut websocket = base;
    let scheme = match websocket.scheme() {
        "http" => "ws",
        "https" => "wss",
        scheme => {
            return Err(CrdtError::Protocol(format!(
                "unsupported PUBLIC_BASE_URL scheme {scheme}"
            )))
        }
    };
    websocket
        .set_scheme(scheme)
        .map_err(|_| CrdtError::Protocol("invalid WebSocket scheme".into()))?;
    websocket.set_path(&format!("/d/{document_id}/ws"));
    Ok((websocket.to_string(), base_url))
}

/// Everything needed to attribute a successfully persisted content write.
pub(crate) struct Attribution {
    vault_id: String,
    principal: Principal,
    state: AppState,
    plugin_db: Option<(String, String)>,
    search_target: Option<SearchTarget>,
}

enum SearchTarget {
    Vault,
    Note(String),
}

impl Attribution {
    pub(crate) fn new(state: AppState, document_id: &str, principal: Principal) -> Self {
        let vault_id = document_id
            .split("__")
            .next()
            .unwrap_or(document_id)
            .to_string();
        let plugin_db =
            crate::plugindb::parse_doc_id(document_id).map(|(_, plugin, name)| (plugin, name));
        let search_target = if plugin_db.is_some() {
            None
        } else if document_id == vault_id {
            Some(SearchTarget::Vault)
        } else {
            document_id
                .strip_prefix(vault_id.as_str())
                .and_then(|suffix| suffix.strip_prefix("__"))
                .map(|guid| SearchTarget::Note(guid.to_string()))
        };
        Self {
            vault_id,
            principal,
            state,
            plugin_db,
            search_target,
        }
    }

    async fn mark_content_write(&self) {
        self.state
            .git
            .mark_write(&self.vault_id, &self.principal)
            .await;
        match &self.search_target {
            Some(SearchTarget::Vault) => {
                self.state.search.mark_vault_write(&self.vault_id).await;
            }
            Some(SearchTarget::Note(guid)) => {
                self.state
                    .search
                    .mark_note_write(&self.vault_id, guid)
                    .await;
            }
            None => {}
        }
        if let Some((plugin, name)) = &self.plugin_db {
            self.state
                .plugindb
                .mark_write(&self.vault_id, plugin, name)
                .await;
        }
    }
}

/// Read one unsigned LEB128 var-uint used by the multiplex transport.
pub(crate) fn read_varint(buf: &[u8]) -> Option<(u64, &[u8])> {
    let mut result = 0u64;
    let mut shift = 0u32;
    for (index, byte) in buf.iter().copied().enumerate() {
        let value = (byte & 0x7f) as u64;
        if shift == 63 && value > 1 {
            return None;
        }
        result |= value << shift;
        if byte & 0x80 == 0 {
            return Some((result, &buf[index + 1..]));
        }
        if shift == 63 {
            return None;
        }
        shift += 7;
    }
    None
}

#[cfg(any(test, feature = "fuzzing"))]
fn exercise_protocol_bytes(bytes: &[u8]) {
    let Ok(message) = crate::safe_yrs::decode_message(bytes) else {
        return;
    };
    match message {
        Message::Sync(SyncMessage::SyncStep1(state_vector)) => {
            std::hint::black_box(state_vector);
        }
        Message::Sync(SyncMessage::SyncStep2(update))
        | Message::Sync(SyncMessage::Update(update)) => {
            if let Ok(update) = crate::safe_yrs::validate_update(&update) {
                let doc = Doc::new();
                doc.transact_mut().apply_update(update);
                std::hint::black_box(doc.transact().state_vector());
            }
        }
        Message::Awareness(update) => {
            let mut awareness = Awareness::new(Doc::new());
            let _ = awareness.apply_update_summary(update);
        }
        Message::Custom(EPOCH_ACK_MESSAGE | EPOCH_PROPOSAL_MESSAGE, payload) => {
            let _ = decode_epoch_payload(&payload);
        }
        message => {
            std::hint::black_box(message);
        }
    }
}

/// Parser entry point used by the out-of-process fuzz target.
#[cfg(feature = "fuzzing")]
pub fn fuzz_protocol_bytes(bytes: &[u8]) {
    if bytes.len() <= MAX_UPDATE_BYTES {
        exercise_protocol_bytes(bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{engine::general_purpose::STANDARD, Engine as _};
    use proptest::prelude::*;
    use tokio::time::{timeout, Duration};
    use yrs::sync::awareness::AwarenessUpdateEntry;
    use yrs::sync::AwarenessUpdate;
    use yrs::{GetString, Map, Text, Transact, Update};

    fn temp_store() -> PathBuf {
        std::env::temp_dir().join(format!("realtime-crdt-{}", uuid::Uuid::new_v4()))
    }

    #[tokio::test]
    async fn file_document_count_excludes_index_and_plugin_databases() {
        let directory = temp_store();
        let store = DocumentStore::new(&directory).await.unwrap();
        store.read_update("vault").await.unwrap();
        store.read_update("vault__note-guid").await.unwrap();
        store
            .read_update("vault__plugindb__tasks__main")
            .await
            .unwrap();

        assert_eq!(store.document_count_for_vault("vault").await.unwrap(), 1);
        let _ = tokio::fs::remove_dir_all(directory).await;
    }

    #[tokio::test]
    async fn document_exists_recognizes_legacy_snapshot_before_migration() {
        let directory = temp_store();
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let document_id = "vault__legacy-guid";
        tokio::fs::write(
            directory.join(format!("{document_id}.yjs")),
            map_update("legacy", "present"),
        )
        .await
        .unwrap();
        let store = DocumentStore::new(&directory).await.unwrap();

        assert!(store.document_exists(document_id).await.unwrap());
        assert!(directory.join(format!("{document_id}.yjs")).exists());
        assert!(!directory.join(format!("{document_id}.crdt")).exists());
        let _ = tokio::fs::remove_dir_all(directory).await;
    }

    fn map_update(key: &str, value: &str) -> Vec<u8> {
        let doc = Doc::new();
        let map = doc.get_or_insert_map("values");
        map.insert(&mut doc.transact_mut(), key, value);
        let update = doc
            .transact()
            .encode_state_as_update_v1(&StateVector::default());
        update
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(512))]

        #[test]
        fn fuzz_arbitrary_protocol_messages_do_not_panic(bytes in prop::collection::vec(any::<u8>(), 0..65_536)) {
            exercise_protocol_bytes(&bytes);
        }
    }

    fn assert_released_corpus_document(doc: &Doc) {
        let text = doc.get_or_insert_text("contents");
        assert_eq!(text.get_string(&doc.transact()), "hello α\nworld");
        let metadata = doc.get_or_insert_map("metadata");
        let txn = doc.transact();
        assert!(matches!(
            metadata.get(&txn, "kind"),
            Some(yrs::Out::Any(yrs::Any::String(value))) if value.as_ref() == "markdown"
        ));
        assert!(matches!(
            metadata.get(&txn, "revision"),
            Some(yrs::Out::Any(yrs::Any::Number(value))) if value == 7.0
        ));
    }

    #[test]
    fn released_yjs_v1_corpus_decodes_as_update_and_sync_messages() {
        let corpus: Value =
            serde_json::from_str(include_str!("../../compat/corpus/yjs-v1.json")).unwrap();
        assert_eq!(corpus["schemaVersion"], 1);
        assert_eq!(corpus["producer"]["pluginVersion"], "0.4.3-alpha.7");
        let fixture = &corpus["document"];

        let update = STANDARD
            .decode(fixture["updateV1Base64"].as_str().unwrap())
            .unwrap();
        let doc = Doc::new();
        doc.transact_mut()
            .apply_update(crate::safe_yrs::decode_v1::<Update>(&update).unwrap());
        assert_released_corpus_document(&doc);

        let sync_update = STANDARD
            .decode(fixture["syncUpdateMessageBase64"].as_str().unwrap())
            .unwrap();
        let Message::Sync(SyncMessage::Update(update)) =
            crate::safe_yrs::decode_v1::<Message>(&sync_update).unwrap()
        else {
            panic!("released message is not a sync update");
        };
        let doc = Doc::new();
        doc.transact_mut()
            .apply_update(crate::safe_yrs::decode_v1::<Update>(&update).unwrap());
        assert_released_corpus_document(&doc);

        let sync_step1 = STANDARD
            .decode(fixture["syncStep1MessageBase64"].as_str().unwrap())
            .unwrap();
        assert!(matches!(
            crate::safe_yrs::decode_v1::<Message>(&sync_step1).unwrap(),
            Message::Sync(SyncMessage::SyncStep1(_))
        ));
    }

    fn replace_text_update(current: &[u8], value: &str) -> Vec<u8> {
        let doc = Doc::new();
        doc.transact_mut()
            .apply_update(crate::safe_yrs::decode_v1::<Update>(current).unwrap());
        let before = doc.transact().state_vector();
        let text = doc.get_or_insert_text("contents");
        {
            let mut txn = doc.transact_mut();
            let len = text.len(&txn);
            if len > 0 {
                text.remove_range(&mut txn, 0, len);
            }
            text.insert(&mut txn, 0, value);
        }
        let update = doc.transact().encode_state_as_update_v1(&before);
        update
    }

    #[tokio::test]
    async fn concurrent_updates_are_merged_and_survive_reload() {
        let directory = temp_store();
        let store = DocumentStore::new(&directory).await.unwrap();
        let mut tasks = Vec::new();
        for index in 0..32 {
            let store = store.clone();
            tasks.push(tokio::spawn(async move {
                store
                    .apply_update(
                        "vault__document",
                        &map_update(&format!("k{index}"), &format!("v{index}")),
                    )
                    .await
                    .unwrap();
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }

        drop(store);
        let reloaded = DocumentStore::new(&directory).await.unwrap();
        let update = reloaded.read_update("vault__document").await.unwrap();
        let doc = decode_document(&update).unwrap();
        let map = doc.get_or_insert_map("values");
        let txn = doc.transact();
        for index in 0..32 {
            assert_eq!(
                map.get(&txn, &format!("k{index}"))
                    .and_then(|value| value.cast::<String>().ok()),
                Some(format!("v{index}"))
            );
        }
        let _ = tokio::fs::remove_dir_all(directory).await;
    }

    #[tokio::test]
    async fn corrupt_snapshot_fails_closed_without_overwrite() {
        let directory = temp_store();
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let path = directory.join("broken.yjs");
        let corrupt = b"not-a-yjs-update";
        tokio::fs::write(&path, corrupt).await.unwrap();

        let store = DocumentStore::new(&directory).await.unwrap();
        let error = store.read_update("broken").await.unwrap_err();
        assert!(matches!(error, CrdtError::CorruptDocument { .. }));
        assert_eq!(tokio::fs::read(&path).await.unwrap(), corrupt);
        let _ = tokio::fs::remove_dir_all(directory).await;
    }

    #[tokio::test]
    async fn sync_status_ack_is_sent_only_after_the_update_is_durable() {
        let directory = temp_store();
        let store = DocumentStore::new(&directory).await.unwrap();
        let mut connection = store
            .connect_internal("vault__document", Level::Full)
            .await
            .unwrap();
        connection.recv().await.unwrap();
        connection.recv().await.unwrap();

        let update = map_update("durable", "yes");
        connection
            .send(Message::Sync(SyncMessage::Update(update)).encode_v1())
            .await
            .unwrap();
        connection
            .send(Message::Custom(SYNC_STATUS_MESSAGE, vec![7]).encode_v1())
            .await
            .unwrap();

        loop {
            let bytes = timeout(Duration::from_secs(2), connection.recv())
                .await
                .unwrap()
                .unwrap();
            if matches!(
                crate::safe_yrs::decode_v1::<Message>(&bytes).unwrap(),
                Message::Custom(SYNC_STATUS_MESSAGE, payload) if payload == vec![7]
            ) {
                break;
            }
        }

        let independent = DocumentStore::new(&directory).await.unwrap();
        let persisted = independent.read_update("vault__document").await.unwrap();
        let doc = decode_document(&persisted).unwrap();
        let map = doc.get_or_insert_map("values");
        assert_eq!(
            map.get(&doc.transact(), "durable")
                .and_then(|value| value.cast::<String>().ok()),
            Some("yes".to_string())
        );
        let _ = tokio::fs::remove_dir_all(directory).await;
    }

    #[tokio::test]
    async fn durable_child_write_invalidates_the_live_vault_index() {
        let directory = temp_store();
        let store = DocumentStore::new(&directory).await.unwrap();
        let mut index = store.connect_internal("vault", Level::Full).await.unwrap();
        index.recv().await.unwrap();
        index.recv().await.unwrap();

        store
            .apply_update("vault__document", &map_update("remote", "change"))
            .await
            .unwrap();

        let payload = timeout(Duration::from_secs(2), async {
            loop {
                let bytes = index.recv().await.unwrap();
                if let Message::Custom(DOCUMENT_INVALIDATED_MESSAGE, payload) =
                    crate::safe_yrs::decode_v1::<Message>(&bytes).unwrap()
                {
                    break payload;
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&payload).unwrap(),
            json!({ "documentId": "vault__document" })
        );
        let _ = tokio::fs::remove_dir_all(directory).await;
    }

    #[tokio::test]
    async fn accepted_log_never_grows_beyond_the_replay_limit() {
        let directory = temp_store();
        let store = DocumentStore::new(&directory).await.unwrap();
        store
            .apply_update("vault__document", &map_update("first", "kept"))
            .await
            .unwrap();
        let (_, document) = store.get_or_load("vault__document").await.unwrap();
        let queued = (0..16)
            .map(|index| {
                (
                    format!("queued-{index:02}"),
                    map_update(&format!("queued-{index:02}"), "kept"),
                )
            })
            .collect::<Vec<_>>();
        let largest_update = queued.iter().map(|(_, update)| update.len()).max().unwrap();
        let replay_limit = document
            .persistence
            .lock()
            .await
            .limit_to_one_fresh_record_for_test(largest_update);

        let mut writes = Vec::new();
        for (_, update) in &queued {
            let store = store.clone();
            let update = update.clone();
            writes.push(tokio::spawn(async move {
                store.apply_update("vault__document", &update).await
            }));
        }
        for write in writes {
            write.await.unwrap().unwrap();
        }
        {
            let persistence = document.persistence.lock().await;
            assert!(persistence.generation() >= 1);
            assert!(persistence.log_bytes() <= replay_limit);
        }

        drop(document);
        drop(store);
        let reloaded = DocumentStore::new(&directory).await.unwrap();
        let update = reloaded.read_update("vault__document").await.unwrap();
        let doc = decode_document(&update).unwrap();
        let map = doc.get_or_insert_map("values");
        let txn = doc.transact();
        assert_eq!(
            map.get(&txn, "first")
                .and_then(|value| value.cast::<String>().ok())
                .as_deref(),
            Some("kept")
        );
        for (key, _) in queued {
            assert_eq!(
                map.get(&txn, &key)
                    .and_then(|value| value.cast::<String>().ok())
                    .as_deref(),
                Some("kept")
            );
        }
        let _ = tokio::fs::remove_dir_all(directory).await;
    }

    #[tokio::test]
    async fn read_only_connection_ignores_updates_without_closing() {
        let directory = temp_store();
        let store = DocumentStore::new(&directory).await.unwrap();
        let mut connection = store
            .connect_internal("vault__document", Level::ReadOnly)
            .await
            .unwrap();
        connection.recv().await.unwrap();
        connection.recv().await.unwrap();
        connection
            .send(Message::Sync(SyncMessage::Update(map_update("forbidden", "write"))).encode_v1())
            .await
            .unwrap();

        connection
            .send(Message::Custom(SYNC_STATUS_MESSAGE, vec![11]).encode_v1())
            .await
            .unwrap();
        loop {
            let bytes = timeout(Duration::from_secs(2), connection.recv())
                .await
                .unwrap()
                .expect("read-only connection must stay open");
            if matches!(
                crate::safe_yrs::decode_v1::<Message>(&bytes).unwrap(),
                Message::Custom(SYNC_STATUS_MESSAGE, payload) if payload == vec![11]
            ) {
                break;
            }
        }
        let update = store.read_update("vault__document").await.unwrap();
        let doc = decode_document(&update).unwrap();
        assert!(doc
            .get_or_insert_map("values")
            .get(&doc.transact(), "forbidden")
            .is_none());
        let _ = tokio::fs::remove_dir_all(directory).await;
    }

    #[tokio::test]
    async fn first_multi_client_awareness_update_is_rejected_without_publishing_ids() {
        let directory = temp_store();
        let store = DocumentStore::new(&directory).await.unwrap();
        let mut connection = store
            .connect_internal("vault__document", Level::Full)
            .await
            .unwrap();
        connection.recv().await.unwrap();
        connection.recv().await.unwrap();

        connection
            .send(
                Message::Awareness(AwarenessUpdate {
                    clients: HashMap::from([
                        (
                            1,
                            AwarenessUpdateEntry {
                                clock: 1,
                                json: r#"{"user":{"name":"one"}}"#.to_string(),
                            },
                        ),
                        (
                            2,
                            AwarenessUpdateEntry {
                                clock: 1,
                                json: r#"{"user":{"name":"two"}}"#.to_string(),
                            },
                        ),
                    ]),
                })
                .encode_v1(),
            )
            .await
            .unwrap();
        timeout(Duration::from_secs(2), async {
            while connection.recv().await.is_some() {}
        })
        .await
        .unwrap();
        let (_, document) = store.get_or_load("vault__document").await.unwrap();
        assert!(document.awareness.read().unwrap().clients().is_empty());
        let _ = tokio::fs::remove_dir_all(directory).await;
    }

    #[tokio::test]
    async fn later_multi_client_awareness_update_cannot_inject_or_retain_an_id() {
        let directory = temp_store();
        let store = DocumentStore::new(&directory).await.unwrap();
        let mut connection = store
            .connect_internal("vault__document", Level::Full)
            .await
            .unwrap();
        connection.recv().await.unwrap();
        connection.recv().await.unwrap();
        let entry = |name: &str| AwarenessUpdateEntry {
            clock: 1,
            json: format!(r#"{{"user":{{"name":"{name}"}}}}"#),
        };
        connection
            .send(
                Message::Awareness(AwarenessUpdate {
                    clients: HashMap::from([(1, entry("one"))]),
                })
                .encode_v1(),
            )
            .await
            .unwrap();
        connection
            .send(
                Message::Awareness(AwarenessUpdate {
                    clients: HashMap::from([(1, entry("one")), (2, entry("two"))]),
                })
                .encode_v1(),
            )
            .await
            .unwrap();
        timeout(Duration::from_secs(2), async {
            while connection.recv().await.is_some() {}
        })
        .await
        .unwrap();
        let (_, document) = store.get_or_load("vault__document").await.unwrap();
        assert!(document.awareness.read().unwrap().clients().is_empty());
        let _ = tokio::fs::remove_dir_all(directory).await;
    }

    #[tokio::test]
    async fn oversized_awareness_frame_is_rejected() {
        let directory = temp_store();
        let store = DocumentStore::new(&directory).await.unwrap();
        let mut connection = store
            .connect_internal("vault__document", Level::Full)
            .await
            .unwrap();
        connection.recv().await.unwrap();
        connection.recv().await.unwrap();
        let oversized = Message::Awareness(AwarenessUpdate {
            clients: HashMap::from([(
                1,
                AwarenessUpdateEntry {
                    clock: 1,
                    json: "x".repeat(MAX_AWARENESS_BYTES),
                },
            )]),
        })
        .encode_v1();
        assert!(oversized.len() > MAX_AWARENESS_BYTES);
        connection.send(oversized).await.unwrap();
        assert!(timeout(Duration::from_secs(2), connection.recv())
            .await
            .unwrap()
            .is_none());
        let (_, document) = store.get_or_load("vault__document").await.unwrap();
        assert!(document.awareness.read().unwrap().clients().is_empty());
        let _ = tokio::fs::remove_dir_all(directory).await;
    }

    #[tokio::test]
    async fn persistence_failure_does_not_mutate_live_state() {
        let directory = temp_store();
        let store = DocumentStore::new(&directory).await.unwrap();
        let (_, document) = store.get_or_load("document").await.unwrap();
        let log = directory.join("document.crdt").join("updates-0.log");
        tokio::fs::remove_file(&log).await.unwrap();
        tokio::fs::create_dir(&log).await.unwrap();
        let error = document
            .apply_update(&map_update("must-not", "appear"))
            .await
            .unwrap_err();
        assert!(matches!(error, CrdtError::Io(_)));
        let snapshot = document.read_update().await.unwrap();
        let doc = decode_document(&snapshot).unwrap();
        assert!(doc
            .get_or_insert_map("values")
            .get(&doc.transact(), "must-not")
            .is_none());
        let _ = tokio::fs::remove_dir_all(directory).await;
    }

    #[tokio::test]
    async fn invalid_document_ids_cannot_escape_the_store() {
        let directory = temp_store();
        let store = DocumentStore::new(&directory).await.unwrap();
        for id in ["", "../escape", "nested/name", "space name", "💥"] {
            assert!(matches!(
                store.ensure_document(id).await,
                Err(CrdtError::InvalidDocumentId)
            ));
        }
        let _ = tokio::fs::remove_dir_all(directory).await;
    }

    #[tokio::test]
    async fn epoch_activation_waits_for_a_checked_in_process_write() {
        let directory = temp_store();
        let store = DocumentStore::new(&directory).await.unwrap();
        let (write_reached, resume_write) = store.pause_next_write("vault__document").await;

        let writer = {
            let store = store.clone();
            tokio::spawn(async move {
                store
                    .apply_update("vault__document", &map_update("raced", "preserved"))
                    .await
            })
        };
        write_reached.await.unwrap();

        let rollover = {
            let store = store.clone();
            tokio::spawn(async move { store.begin_epoch_transition("vault__document").await })
        };
        tokio::task::yield_now().await;
        assert!(
            !rollover.is_finished(),
            "replacement must wait for the checked write's durable append"
        );

        resume_write.send(()).unwrap();
        writer.await.unwrap().unwrap();
        assert_eq!(rollover.await.unwrap().unwrap(), 1);

        let update = store.read_update("vault__document").await.unwrap();
        let doc = decode_document(&update).unwrap();
        assert_eq!(
            doc.get_or_insert_map("values")
                .get(&doc.transact(), "raced")
                .and_then(|value| value.cast::<String>().ok())
                .as_deref(),
            Some("preserved")
        );
        let _ = tokio::fs::remove_dir_all(directory).await;
    }

    #[tokio::test]
    async fn epoch_activation_cannot_snapshot_between_connection_check_and_apply() {
        let directory = temp_store();
        let store = DocumentStore::new(&directory).await.unwrap();
        let mut writer = store
            .connect_internal("vault__document", Level::Full)
            .await
            .unwrap();
        let mut acknowledger = store
            .connect_internal("vault__document", Level::Full)
            .await
            .unwrap();
        for connection in [&mut writer, &mut acknowledger] {
            connection.recv().await.unwrap();
            connection.recv().await.unwrap();
        }
        let (write_checked, resume_write) = store
            .pause_next_connection_write_after_check("vault__document")
            .await;
        writer
            .send(Message::Sync(SyncMessage::Update(map_update("raced", "preserved"))).encode_v1())
            .await
            .unwrap();
        write_checked.await.unwrap();

        let rollover = {
            let store = store.clone();
            tokio::spawn(async move { store.begin_epoch_transition("vault__document").await })
        };
        tokio::task::yield_now().await;
        assert!(
            !rollover.is_finished(),
            "epoch transition must wait for the gated connection write"
        );
        resume_write.send(()).unwrap();
        assert_eq!(rollover.await.unwrap().unwrap(), 1);
        for connection in [&mut writer, &mut acknowledger] {
            loop {
                let bytes = connection.recv().await.unwrap();
                if matches!(
                    crate::safe_yrs::decode_v1::<Message>(&bytes).unwrap(),
                    Message::Custom(EPOCH_PROPOSAL_MESSAGE, _)
                ) {
                    break;
                }
            }
        }
        acknowledger
            .send(epoch_wire_message(EPOCH_ACK_MESSAGE, 1).unwrap())
            .await
            .unwrap();
        writer
            .send(epoch_wire_message(EPOCH_ACK_MESSAGE, 1).unwrap())
            .await
            .unwrap();
        tokio::task::yield_now().await;
        assert_eq!(store.current_epoch("vault__document").await.unwrap(), 0);

        timeout(Duration::from_secs(2), async {
            while store.current_epoch("vault__document").await.unwrap() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let update = store.read_update("vault__document").await.unwrap();
        let doc = decode_document(&update).unwrap();
        assert_eq!(
            doc.get_or_insert_map("values")
                .get(&doc.transact(), "raced")
                .and_then(|value| value.cast::<String>().ok())
                .as_deref(),
            Some("preserved")
        );
        let _ = tokio::fs::remove_dir_all(directory).await;
    }

    #[tokio::test]
    async fn retired_epoch_is_deleted_when_its_recovery_window_expires() {
        let directory = temp_store();
        let store = DocumentStore::new_with_policy(
            &directory,
            EpochPolicy {
                recovery_window_ms: 1_000,
                ..EpochPolicy::default()
            },
        )
        .await
        .unwrap();
        store
            .apply_update("vault__document", &map_update("durable", "yes"))
            .await
            .unwrap();
        let retired_path = directory.join("vault__document.crdt");
        assert!(retired_path.exists());

        assert_eq!(
            store
                .begin_epoch_transition("vault__document")
                .await
                .unwrap(),
            1
        );
        assert!(
            retired_path.exists(),
            "recovery window must retain the old epoch"
        );
        timeout(Duration::from_secs(3), async {
            while retired_path.exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(store.current_epoch("vault__document").await.unwrap(), 1);
        let _ = tokio::fs::remove_dir_all(directory).await;
    }

    #[tokio::test]
    async fn epoch_proposal_survives_registration_before_connection_task_start() {
        let directory = temp_store();
        let store = DocumentStore::new(&directory).await.unwrap();
        let (registered, resume_connection) = store
            .pause_next_connection_after_registration("vault__document")
            .await;
        let connector = {
            let store = store.clone();
            tokio::spawn(
                async move { store.connect_internal("vault__document", Level::Full).await },
            )
        };
        registered.await.unwrap();

        assert_eq!(
            store
                .begin_epoch_transition("vault__document")
                .await
                .unwrap(),
            1
        );
        assert_eq!(store.current_epoch("vault__document").await.unwrap(), 0);

        resume_connection.send(()).unwrap();
        let mut connection = connector.await.unwrap().unwrap();
        let proposal = timeout(Duration::from_secs(1), async {
            loop {
                let bytes = connection.recv().await.unwrap();
                if let Message::Custom(EPOCH_PROPOSAL_MESSAGE, payload) =
                    crate::safe_yrs::decode_v1::<Message>(&bytes).unwrap()
                {
                    break decode_epoch_payload(&payload).unwrap();
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(proposal.epoch, 1);
        connection
            .send(epoch_wire_message(EPOCH_ACK_MESSAGE, 1).unwrap())
            .await
            .unwrap();
        timeout(Duration::from_secs(1), async {
            while store.current_epoch("vault__document").await.unwrap() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let _ = tokio::fs::remove_dir_all(directory).await;
    }

    #[tokio::test]
    async fn epoch_waits_for_ack_preserves_content_and_rejects_retired_connections() {
        let directory = temp_store();
        let store = DocumentStore::new(&directory).await.unwrap();
        store
            .apply_update("vault__document", &map_update("before", "kept"))
            .await
            .unwrap();
        let mut connection = store
            .connect_internal("vault__document", Level::Full)
            .await
            .unwrap();
        // Ensure run_connection subscribed before the proposal broadcast.
        connection.recv().await.unwrap();
        connection.recv().await.unwrap();

        assert_eq!(
            store
                .begin_epoch_transition("vault__document")
                .await
                .unwrap(),
            1
        );
        let proposal = timeout(Duration::from_secs(1), async {
            loop {
                let bytes = connection.recv().await.unwrap();
                if let Message::Custom(EPOCH_PROPOSAL_MESSAGE, payload) =
                    crate::safe_yrs::decode_v1::<Message>(&bytes).unwrap()
                {
                    break decode_epoch_payload(&payload).unwrap();
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(proposal.epoch, 1);
        assert_eq!(store.current_epoch("vault__document").await.unwrap(), 0);

        // Unacknowledged clients may finish writes already in flight.
        connection
            .send(Message::Sync(SyncMessage::Update(map_update("in-flight", "kept"))).encode_v1())
            .await
            .unwrap();
        connection
            .send(epoch_wire_message(EPOCH_ACK_MESSAGE, 1).unwrap())
            .await
            .unwrap();
        // A queued post-ack write must never enter either epoch.
        let _ = connection
            .send(Message::Sync(SyncMessage::Update(map_update("too-late", "lost"))).encode_v1())
            .await;

        let activated = timeout(Duration::from_secs(2), async {
            loop {
                if store.current_epoch("vault__document").await.unwrap() == 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        if activated.is_err() {
            let runtime = store.epoch_runtime("vault__document").await.unwrap();
            let runtime = runtime.lock().await;
            let state = format!(
                "current={}, pending={:?}, activating={}, connections={:?}",
                runtime.manifest.current.epoch,
                runtime
                    .manifest
                    .pending
                    .as_ref()
                    .map(|pending| pending.epoch),
                runtime.activating,
                runtime
                    .connections
                    .iter()
                    .map(|(id, connection)| (*id, connection.epoch, connection.acknowledged))
                    .collect::<Vec<_>>()
            );
            drop(runtime);
            let retry = store.maybe_activate_epoch("vault__document").await;
            panic!("epoch did not activate: {state}; retry={retry:?}",);
        }

        let update = store.read_update("vault__document").await.unwrap();
        let doc = decode_document(&update).unwrap();
        let map = doc.get_or_insert_map("values");
        let txn = doc.transact();
        assert_eq!(
            map.get(&txn, "before")
                .and_then(|value| value.cast::<String>().ok())
                .as_deref(),
            Some("kept")
        );
        assert_eq!(
            map.get(&txn, "in-flight")
                .and_then(|value| value.cast::<String>().ok())
                .as_deref(),
            Some("kept")
        );
        assert!(map.get(&txn, "too-late").is_none());
        drop(txn);

        assert!(matches!(
            store
                .connect("vault__document", Level::Full, None, Some(0))
                .await,
            Err(CrdtError::RetiredEpoch {
                requested: 0,
                current: 1,
                ..
            })
        ));
        let runtime = store.epoch_runtime("vault__document").await.unwrap();
        let runtime = runtime.lock().await;
        assert_eq!(runtime.manifest.retired.len(), 1);
        assert!(directory.join("vault__document.crdt").is_dir());
        drop(runtime);
        let _ = tokio::fs::remove_dir_all(directory).await;
    }

    #[tokio::test]
    async fn epoch_stays_current_until_the_second_client_acknowledges() {
        let directory = temp_store();
        let store = DocumentStore::new(&directory).await.unwrap();
        let mut first = store
            .connect_internal("vault__document", Level::Full)
            .await
            .unwrap();
        let mut second = store
            .connect_internal("vault__document", Level::Full)
            .await
            .unwrap();
        for connection in [&mut first, &mut second] {
            connection.recv().await.unwrap();
            connection.recv().await.unwrap();
        }

        assert_eq!(
            store
                .begin_epoch_transition("vault__document")
                .await
                .unwrap(),
            1
        );
        for connection in [&mut first, &mut second] {
            timeout(Duration::from_secs(1), async {
                loop {
                    let bytes = connection.recv().await.unwrap();
                    if let Message::Custom(EPOCH_PROPOSAL_MESSAGE, payload) =
                        crate::safe_yrs::decode_v1::<Message>(&bytes).unwrap()
                    {
                        assert_eq!(decode_epoch_payload(&payload).unwrap().epoch, 1);
                        break;
                    }
                }
            })
            .await
            .unwrap();
        }

        first
            .send(epoch_wire_message(EPOCH_ACK_MESSAGE, 1).unwrap())
            .await
            .unwrap();
        timeout(Duration::from_secs(1), async {
            loop {
                let runtime = store.epoch_runtime("vault__document").await.unwrap();
                let runtime = runtime.lock().await;
                if runtime.connections.len() == 1
                    && runtime
                        .connections
                        .values()
                        .all(|connection| !connection.acknowledged)
                {
                    break;
                }
                drop(runtime);
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            store.current_epoch("vault__document").await.unwrap(),
            0,
            "the token endpoint must keep returning the old epoch while one client is unacknowledged"
        );

        second
            .send(epoch_wire_message(EPOCH_ACK_MESSAGE, 1).unwrap())
            .await
            .unwrap();
        timeout(Duration::from_secs(2), async {
            loop {
                if store.current_epoch("vault__document").await.unwrap() == 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let _ = tokio::fs::remove_dir_all(directory).await;
    }

    #[tokio::test]
    async fn month_and_year_edit_simulation_bounds_active_epoch_history() {
        let directory = temp_store();
        let store = DocumentStore::new_with_policy(
            &directory,
            EpochPolicy {
                recovery_window_ms: u64::MAX / 2,
                max_age_ms: u64::MAX,
                max_update_count: 30,
                max_encoded_state_bytes: u64::MAX,
                max_delete_set_bytes: u64::MAX,
            },
        )
        .await
        .unwrap();
        let control_directory = temp_store();
        let control = DocumentStore::new_with_policy(
            &control_directory,
            EpochPolicy {
                recovery_window_ms: u64::MAX / 2,
                max_age_ms: u64::MAX,
                max_update_count: u64::MAX,
                max_encoded_state_bytes: u64::MAX,
                max_delete_set_bytes: u64::MAX,
            },
        )
        .await
        .unwrap();

        for day in 0..365 {
            for current_store in [&store, &control] {
                let current = current_store.read_update("vault__note").await.unwrap();
                let update = replace_text_update(&current, &format!("day-{day:03}"));
                current_store
                    .apply_update("vault__note", &update)
                    .await
                    .unwrap();
            }
            if day == 29 {
                assert_eq!(store.current_epoch("vault__note").await.unwrap(), 1);
            }
        }

        assert_eq!(store.current_epoch("vault__note").await.unwrap(), 12);
        let bounded = store.epoch_metrics("vault__note").await.unwrap();
        let unbounded = control.epoch_metrics("vault__note").await.unwrap();
        assert_eq!(bounded.update_count, 5);
        assert!(
            bounded.encoded_state_bytes * 4 < unbounded.encoded_state_bytes,
            "replacement did not materially bound history: bounded={}, unbounded={}",
            bounded.encoded_state_bytes,
            unbounded.encoded_state_bytes
        );
        let update = store.read_update("vault__note").await.unwrap();
        let doc = decode_document(&update).unwrap();
        let text = doc.get_or_insert_text("contents");
        assert_eq!(text.get_string(&doc.transact()), "day-364");

        let _ = tokio::fs::remove_dir_all(directory).await;
        let _ = tokio::fs::remove_dir_all(control_directory).await;
    }

    #[tokio::test]
    async fn interrupted_epoch_creation_keeps_old_state_and_resumes_after_restart() {
        let directory = temp_store();
        let store = DocumentStore::new_with_policy(
            &directory,
            EpochPolicy {
                recovery_window_ms: 0,
                ..EpochPolicy::default()
            },
        )
        .await
        .unwrap();
        store
            .apply_update("vault__document", &map_update("durable", "yes"))
            .await
            .unwrap();
        let runtime = store.epoch_runtime("vault__document").await.unwrap();
        let pending_physical = {
            let mut runtime = runtime.lock().await;
            let pending = runtime
                .manifest
                .begin(crdt_epoch::now_millis())
                .unwrap()
                .clone();
            crdt_epoch::save_manifest(&directory, &runtime.manifest)
                .await
                .unwrap();
            pending.physical_document_id
        };
        // A directory without a committed storage manifest models an
        // interruption before the replacement snapshot became durable.
        tokio::fs::create_dir(directory.join(format!("{pending_physical}.crdt")))
            .await
            .unwrap();
        assert_eq!(store.current_epoch("vault__document").await.unwrap(), 0);
        let old = store.read_update("vault__document").await.unwrap();
        let old_doc = decode_document(&old).unwrap();
        assert_eq!(
            old_doc
                .get_or_insert_map("values")
                .get(&old_doc.transact(), "durable")
                .and_then(|value| value.cast::<String>().ok())
                .as_deref(),
            Some("yes")
        );

        drop(store);
        let restarted = DocumentStore::new_with_policy(
            &directory,
            EpochPolicy {
                recovery_window_ms: 0,
                ..EpochPolicy::default()
            },
        )
        .await
        .unwrap();
        restarted.ensure_document("vault__document").await.unwrap();
        assert_eq!(restarted.current_epoch("vault__document").await.unwrap(), 1);
        assert!(
            !directory.join("vault__document.crdt").exists(),
            "expired retired epoch was not removed during restart recovery"
        );
        let restored = restarted.read_update("vault__document").await.unwrap();
        let restored_doc = decode_document(&restored).unwrap();
        assert_eq!(
            restored_doc
                .get_or_insert_map("values")
                .get(&restored_doc.transact(), "durable")
                .and_then(|value| value.cast::<String>().ok())
                .as_deref(),
            Some("yes")
        );
        let _ = tokio::fs::remove_dir_all(directory).await;
    }

    #[tokio::test]
    async fn operational_metrics_release_connections_and_record_failed_updates() {
        let directory = temp_store();
        let store = DocumentStore::new(&directory).await.unwrap();
        let connection = store
            .connect_internal("metrics__document", Level::Full)
            .await
            .unwrap();
        assert_eq!(store.sync_metrics().snapshot().crdt_connections_active, 1);
        drop(connection);
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if store.sync_metrics().snapshot().crdt_connections_active == 0 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        assert!(store
            .apply_update("metrics__document", &[0xff])
            .await
            .is_err());
        store.compact_document("metrics__document").await.unwrap();
        let metrics = store.sync_metrics().snapshot();
        assert_eq!(metrics.crdt_connections_total, 1);
        assert_eq!(metrics.crdt_updates_total, 1);
        assert_eq!(metrics.crdt_update_failures_total, 1);
        assert_eq!(metrics.crdt_update_bytes_total, 1);
        assert_eq!(metrics.crdt_compactions_total, 1);
        let _ = tokio::fs::remove_dir_all(directory).await;
    }

    #[test]
    fn client_token_urls_preserve_the_provider_contract() {
        let (url, base_url) = client_urls("https://sync.example.test", "vault__document").unwrap();
        assert_eq!(
            url,
            "wss://sync.example.test/d/vault__document/ws".to_string()
        );
        assert_eq!(
            base_url,
            "https://sync.example.test/d/vault__document".to_string()
        );
    }
}
