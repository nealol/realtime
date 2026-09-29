//! Durable document epochs for bounding long-lived Yjs history.
//!
//! An epoch manifest is the commit point. Each replacement is written as a
//! complete, independent CRDT document before the manifest switches to it.
//! The previous physical document remains immutable until its recovery window
//! expires.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;
use yrs::types::AsPrelim;
use yrs::updates::encoder::Encode;
use yrs::{
    Array, ArrayRef, Doc, GetString, In, Map, MapRef, Out, ReadTxn, Text, TextRef, Transact,
};

const FORMAT: &str = "realtime-crdt-epochs";
const FORMAT_VERSION: u32 = 1;

#[derive(Clone, Debug)]
pub struct EpochPolicy {
    pub recovery_window_ms: u64,
    pub max_age_ms: u64,
    pub max_update_count: u64,
    pub max_encoded_state_bytes: u64,
    pub max_delete_set_bytes: u64,
    /// How long a proposed epoch waits for connected peers to acknowledge it
    /// before activating anyway.
    pub ack_timeout_ms: u64,
}

impl Default for EpochPolicy {
    fn default() -> Self {
        Self {
            recovery_window_ms: 30 * 24 * 60 * 60 * 1_000,
            max_age_ms: 365 * 24 * 60 * 60 * 1_000,
            max_update_count: 100_000,
            max_encoded_state_bytes: 32 * 1024 * 1024,
            max_delete_set_bytes: 8 * 1024 * 1024,
            ack_timeout_ms: 30_000,
        }
    }
}

impl EpochPolicy {
    pub fn should_rollover(&self, metrics: &DocumentEpochMetrics) -> bool {
        metrics.update_count > 0
            && (metrics.age_ms >= self.max_age_ms
                || metrics.update_count >= self.max_update_count
                || metrics
                    .encoded_state_bytes
                    .saturating_sub(metrics.baseline_encoded_state_bytes)
                    >= self.max_encoded_state_bytes
                || metrics
                    .delete_set_bytes
                    .saturating_sub(metrics.baseline_delete_set_bytes)
                    >= self.max_delete_set_bytes)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DocumentEpochMetrics {
    pub epoch: u64,
    pub encoded_state_bytes: u64,
    pub delete_set_bytes: u64,
    pub baseline_encoded_state_bytes: u64,
    pub baseline_delete_set_bytes: u64,
    pub update_count: u64,
    pub active_connections: u64,
    pub started_at_ms: u64,
    pub age_ms: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EpochRecord {
    pub epoch: u64,
    pub physical_document_id: String,
    pub started_at_ms: u64,
    #[serde(default)]
    pub baseline_encoded_state_bytes: u64,
    #[serde(default)]
    pub baseline_delete_set_bytes: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PendingEpoch {
    pub epoch: u64,
    pub physical_document_id: String,
    pub proposed_at_ms: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RetiredEpoch {
    pub epoch: u64,
    pub physical_document_id: String,
    pub retired_at_ms: u64,
    pub delete_after_ms: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EpochManifest {
    format: String,
    version: u32,
    pub logical_document_id: String,
    pub current: EpochRecord,
    pub pending: Option<PendingEpoch>,
    pub retired: Vec<RetiredEpoch>,
}

impl EpochManifest {
    pub fn initial(logical_document_id: &str, now_ms: u64) -> Self {
        Self {
            format: FORMAT.to_string(),
            version: FORMAT_VERSION,
            logical_document_id: logical_document_id.to_string(),
            current: EpochRecord {
                epoch: 0,
                physical_document_id: logical_document_id.to_string(),
                started_at_ms: now_ms,
                baseline_encoded_state_bytes: 0,
                baseline_delete_set_bytes: 0,
            },
            pending: None,
            retired: Vec::new(),
        }
    }

    pub fn begin(&mut self, now_ms: u64) -> Result<&PendingEpoch, EpochError> {
        if self.pending.is_none() {
            let epoch = self
                .current
                .epoch
                .checked_add(1)
                .ok_or(EpochError::EpochOverflow)?;
            self.pending = Some(PendingEpoch {
                epoch,
                physical_document_id: physical_document_id(&self.logical_document_id, epoch),
                proposed_at_ms: now_ms,
            });
        }
        Ok(self.pending.as_ref().expect("pending epoch was just set"))
    }

    pub fn activate(
        &mut self,
        now_ms: u64,
        recovery_window_ms: u64,
        baseline_encoded_state_bytes: u64,
        baseline_delete_set_bytes: u64,
    ) -> Result<(), EpochError> {
        let pending = self.pending.take().ok_or(EpochError::NoPendingEpoch)?;
        let delete_after_ms = now_ms.saturating_add(recovery_window_ms);
        self.retired.push(RetiredEpoch {
            epoch: self.current.epoch,
            physical_document_id: self.current.physical_document_id.clone(),
            retired_at_ms: now_ms,
            delete_after_ms,
        });
        self.current = EpochRecord {
            epoch: pending.epoch,
            physical_document_id: pending.physical_document_id,
            started_at_ms: now_ms,
            baseline_encoded_state_bytes,
            baseline_delete_set_bytes,
        };
        Ok(())
    }

    pub fn validate(&self, expected_document_id: &str) -> Result<(), EpochError> {
        if self.format != FORMAT
            || self.version != FORMAT_VERSION
            || self.logical_document_id != expected_document_id
        {
            return Err(EpochError::InvalidManifest(format!(
                "manifest identity or format does not match {expected_document_id}"
            )));
        }
        if self.current.epoch == 0 && self.current.physical_document_id != self.logical_document_id
        {
            return Err(EpochError::InvalidManifest(
                "epoch zero must use the legacy physical document id".into(),
            ));
        }
        if self.current.epoch > 0
            && self.current.physical_document_id
                != physical_document_id(&self.logical_document_id, self.current.epoch)
        {
            return Err(EpochError::InvalidManifest(
                "current physical document id does not match its epoch".into(),
            ));
        }
        if let Some(pending) = &self.pending {
            if pending.epoch != self.current.epoch.saturating_add(1)
                || pending.physical_document_id
                    != physical_document_id(&self.logical_document_id, pending.epoch)
            {
                return Err(EpochError::InvalidManifest(
                    "pending epoch is not the current epoch successor".into(),
                ));
            }
        }
        let mut seen = BTreeMap::new();
        for retired in &self.retired {
            let expected_physical = if retired.epoch == 0 {
                self.logical_document_id.clone()
            } else {
                physical_document_id(&self.logical_document_id, retired.epoch)
            };
            if seen
                .insert(retired.epoch, &retired.physical_document_id)
                .is_some()
                || retired.epoch >= self.current.epoch
                || retired.physical_document_id != expected_physical
                || retired.delete_after_ms < retired.retired_at_ms
            {
                return Err(EpochError::InvalidManifest(
                    "retired epoch identity, order, or retention is invalid".into(),
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum EpochError {
    #[error("document epoch I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid document epoch manifest: {0}")]
    InvalidManifest(String),
    #[error("document epoch counter overflow")]
    EpochOverflow,
    #[error("document has no pending epoch")]
    NoPendingEpoch,
    #[error("document contains unsupported root type {kind:?} at {name:?}")]
    UnsupportedRoot { name: String, kind: String },
}

pub fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

pub async fn load_or_create_manifest(
    root: &Path,
    logical_document_id: &str,
) -> Result<EpochManifest, EpochError> {
    let path = manifest_path(root, logical_document_id);
    if tokio::fs::try_exists(&path).await? {
        let bytes = tokio::fs::read(&path).await?;
        let manifest: EpochManifest = serde_json::from_slice(&bytes)
            .map_err(|error| EpochError::InvalidManifest(error.to_string()))?;
        manifest.validate(logical_document_id)?;
        return Ok(manifest);
    }
    let manifest = EpochManifest::initial(logical_document_id, now_millis());
    save_manifest(root, &manifest).await?;
    Ok(manifest)
}

pub async fn manifest_exists(root: &Path, logical_document_id: &str) -> Result<bool, EpochError> {
    Ok(tokio::fs::try_exists(manifest_path(root, logical_document_id)).await?)
}

pub async fn manifests_for_vault(
    root: &Path,
    vault_id: &str,
) -> Result<Vec<EpochManifest>, EpochError> {
    let prefix = format!("{vault_id}__");
    let mut manifests = Vec::new();
    let mut entries = tokio::fs::read_dir(root).await?;
    while let Some(entry) = entries.next_entry().await? {
        let Some(name) = entry.file_name().to_str().map(str::to_string) else {
            continue;
        };
        if !name.starts_with("epochs-") || !name.ends_with(".json") {
            continue;
        }
        let bytes = tokio::fs::read(entry.path()).await?;
        let manifest: EpochManifest = serde_json::from_slice(&bytes)
            .map_err(|error| EpochError::InvalidManifest(error.to_string()))?;
        manifest.validate(&manifest.logical_document_id)?;
        if manifest.logical_document_id == vault_id
            || manifest.logical_document_id.starts_with(&prefix)
        {
            manifests.push(manifest);
        }
    }
    Ok(manifests)
}

pub async fn validate_store_manifests(root: &Path) -> Result<(), EpochError> {
    let mut entries = tokio::fs::read_dir(root).await?;
    while let Some(entry) = entries.next_entry().await? {
        let Some(name) = entry.file_name().to_str().map(str::to_string) else {
            continue;
        };
        if !name.starts_with("epochs-") || !name.ends_with(".json") {
            continue;
        }
        let bytes = tokio::fs::read(entry.path()).await?;
        let manifest: EpochManifest = serde_json::from_slice(&bytes)
            .map_err(|error| EpochError::InvalidManifest(format!("{name}: {error}")))?;
        manifest.validate(&manifest.logical_document_id)?;
        let expected_path = manifest_path(root, &manifest.logical_document_id);
        if expected_path != entry.path() {
            return Err(EpochError::InvalidManifest(format!(
                "{name} does not match logical document {}",
                manifest.logical_document_id
            )));
        }
        let current_directory =
            root.join(format!("{}.crdt", manifest.current.physical_document_id));
        if !tokio::fs::try_exists(current_directory.join("manifest.json")).await? {
            return Err(EpochError::InvalidManifest(format!(
                "{name} references missing current physical document {}",
                manifest.current.physical_document_id
            )));
        }
    }
    Ok(())
}

pub async fn save_manifest(root: &Path, manifest: &EpochManifest) -> Result<(), EpochError> {
    manifest.validate(&manifest.logical_document_id)?;
    let bytes = serde_json::to_vec_pretty(manifest)
        .map_err(|error| EpochError::InvalidManifest(error.to_string()))?;
    atomic_write(&manifest_path(root, &manifest.logical_document_id), &bytes).await
}

pub async fn remove_expired(
    root: &Path,
    manifest: &mut EpochManifest,
    now_ms: u64,
) -> Result<(), EpochError> {
    let retired_epochs = std::mem::take(&mut manifest.retired);
    let mut retained = Vec::with_capacity(retired_epochs.len());
    let mut retired_epochs = retired_epochs.into_iter();
    while let Some(retired) = retired_epochs.next() {
        if retired.delete_after_ms > now_ms {
            retained.push(retired);
            continue;
        }
        let path = root.join(format!("{}.crdt", retired.physical_document_id));
        match tokio::fs::remove_dir_all(&path).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                retained.push(retired);
                retained.extend(retired_epochs);
                manifest.retired = retained;
                return Err(error.into());
            }
        }
    }
    manifest.retired = retained;
    save_manifest(root, manifest).await
}

pub fn replacement_doc(source: &Doc) -> Result<Doc, EpochError> {
    enum RootValue {
        Text(String),
        Array(Vec<In>),
        Map(BTreeMap<String, In>),
    }

    let source_txn = source.transact();
    let mut roots = Vec::new();
    for (name, value) in source_txn.root_refs() {
        let copied = match value {
            Out::YText(text) => RootValue::Text(text.get_string(&source_txn)),
            Out::YArray(array) => RootValue::Array(
                array
                    .iter(&source_txn)
                    .map(|value| value.as_prelim(&source_txn))
                    .collect(),
            ),
            Out::YMap(map) => RootValue::Map(
                map.iter(&source_txn)
                    .map(|(key, value)| (key.to_string(), value.as_prelim(&source_txn)))
                    .collect(),
            ),
            Out::UndefinedRef(branch) if name == "contents" => {
                RootValue::Text(TextRef::from(branch).get_string(&source_txn))
            }
            Out::UndefinedRef(branch) if name == "batches" => {
                let array = ArrayRef::from(branch);
                RootValue::Array(
                    array
                        .iter(&source_txn)
                        .map(|value| value.as_prelim(&source_txn))
                        .collect(),
                )
            }
            Out::UndefinedRef(branch)
                if matches!(
                    name,
                    "files"
                        | "structured"
                        | "trash"
                        | "root"
                        | "meta"
                        | "cursors"
                        | "cursorsAt"
                        | "binaries"
                        | "configFiles"
                        | "values"
                ) =>
            {
                let map = MapRef::from(branch);
                RootValue::Map(
                    map.iter(&source_txn)
                        .map(|(key, value)| (key.to_string(), value.as_prelim(&source_txn)))
                        .collect(),
                )
            }
            unsupported => {
                return Err(EpochError::UnsupportedRoot {
                    name: name.to_string(),
                    kind: format!("{unsupported:?}"),
                });
            }
        };
        roots.push((name.to_string(), copied));
    }
    drop(source_txn);

    let replacement = Doc::new();
    for (name, value) in roots {
        match value {
            RootValue::Text(value) => {
                let text = replacement.get_or_insert_text(name);
                text.insert(&mut replacement.transact_mut(), 0, &value);
            }
            RootValue::Array(values) => {
                let array = replacement.get_or_insert_array(name);
                let mut txn = replacement.transact_mut();
                for value in values {
                    array.push_back(&mut txn, value);
                }
            }
            RootValue::Map(values) => {
                let map = replacement.get_or_insert_map(name);
                let mut txn = replacement.transact_mut();
                for (key, value) in values {
                    map.insert(&mut txn, key, value);
                }
            }
        }
    }
    Ok(replacement)
}

pub fn document_measurements(doc: &Doc) -> (u64, u64) {
    let txn = doc.transact();
    let state_bytes = txn
        .encode_state_as_update_v1(&yrs::StateVector::default())
        .len() as u64;
    let delete_set_bytes = txn.snapshot().delete_set.encode_v1().len() as u64;
    (state_bytes, delete_set_bytes)
}

fn physical_document_id(logical_document_id: &str, epoch: u64) -> String {
    let digest = Sha256::digest(logical_document_id.as_bytes());
    format!("epoch-{digest:x}-{epoch}")
}

fn manifest_path(root: &Path, logical_document_id: &str) -> PathBuf {
    let digest = Sha256::digest(logical_document_id.as_bytes());
    root.join(format!("epochs-{digest:x}.json"))
}

async fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), EpochError> {
    let parent = path
        .parent()
        .ok_or_else(|| EpochError::InvalidManifest(format!("{} has no parent", path.display())))?;
    tokio::fs::create_dir_all(parent).await?;
    let temp = parent.join(format!(
        ".{}.{}.tmp",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("epoch"),
        nanoid::nanoid!(12)
    ));
    let result = async {
        let mut file = tokio::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temp)
            .await?;
        file.write_all(bytes).await?;
        file.sync_all().await?;
        tokio::fs::rename(&temp, path).await?;
        let directory = tokio::fs::File::open(parent).await?;
        directory.sync_all().await
    }
    .await;
    if result.is_err() {
        let _ = tokio::fs::remove_file(&temp).await;
    }
    result.map_err(EpochError::Io)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use yrs::types::ToJson;
    use yrs::{Any, Array, Map, MapPrelim, Text, TextPrelim};

    #[test]
    fn logical_replacement_preserves_supported_root_values_without_history() {
        let source = Doc::new();
        let text = source.get_or_insert_text("contents");
        let array = source.get_or_insert_array("batches");
        let map = source.get_or_insert_map("files");
        {
            let mut txn = source.transact_mut();
            text.insert(&mut txn, 0, "before");
            text.remove_range(&mut txn, 0, 6);
            text.insert(&mut txn, 0, "after");
            array.push_back(&mut txn, Any::String("batch".into()));
            map.insert(&mut txn, "note.md", "guid");
        }

        let replacement = replacement_doc(&source).unwrap();
        let replacement_text = replacement.get_or_insert_text("contents");
        let replacement_array = replacement.get_or_insert_array("batches");
        let replacement_map = replacement.get_or_insert_map("files");
        let txn = replacement.transact();
        assert_eq!(replacement_text.get_string(&txn), "after");
        assert_eq!(
            replacement_array.to_json(&txn),
            Any::Array(vec![Any::String("batch".into())].into())
        );
        assert_eq!(
            replacement_map.get(&txn, "note.md"),
            Some(Out::Any(Any::String("guid".into())))
        );
        drop(txn);
        let (_, old_delete_set) = document_measurements(&source);
        let (_, replacement_delete_set) = document_measurements(&replacement);
        assert!(old_delete_set > replacement_delete_set);
    }

    #[test]
    fn logical_replacement_preserves_nested_collaborative_types() {
        let source = Doc::new();
        let root = source.get_or_insert_map("root");
        let nodes = root.insert(&mut source.transact_mut(), "nodes", MapPrelim::default());
        let node = nodes.insert(&mut source.transact_mut(), "n1", MapPrelim::default());
        node.insert(
            &mut source.transact_mut(),
            "text",
            TextPrelim::new("shared"),
        );

        let encoded = source
            .transact()
            .encode_state_as_update_v1(&yrs::StateVector::default());
        let decoded = Doc::new();
        decoded
            .transact_mut()
            .apply_update(crate::safe_yrs::decode_v1::<yrs::Update>(&encoded).unwrap());
        let replacement = replacement_doc(&decoded).unwrap();
        let root = replacement.get_or_insert_map("root");
        let txn = replacement.transact();
        let nodes = root.get(&txn, "nodes").unwrap().cast::<MapRef>().unwrap();
        let node = nodes.get(&txn, "n1").unwrap().cast::<MapRef>().unwrap();
        let text = node.get(&txn, "text").unwrap().cast::<TextRef>().unwrap();
        assert_eq!(text.get_string(&txn), "shared");
    }

    #[test]
    fn policy_requires_content_and_accepts_each_bound() {
        let policy = EpochPolicy {
            recovery_window_ms: 1,
            max_age_ms: 10,
            max_update_count: 20,
            max_encoded_state_bytes: 30,
            max_delete_set_bytes: 40,
            ack_timeout_ms: 50,
        };
        let base = DocumentEpochMetrics {
            epoch: 0,
            encoded_state_bytes: 1,
            delete_set_bytes: 1,
            baseline_encoded_state_bytes: 0,
            baseline_delete_set_bytes: 0,
            update_count: 1,
            active_connections: 0,
            started_at_ms: 0,
            age_ms: 1,
        };
        assert!(!policy.should_rollover(&base));
        for metrics in [
            DocumentEpochMetrics {
                age_ms: 10,
                ..base.clone()
            },
            DocumentEpochMetrics {
                update_count: 20,
                ..base.clone()
            },
            DocumentEpochMetrics {
                encoded_state_bytes: 30,
                ..base.clone()
            },
            DocumentEpochMetrics {
                delete_set_bytes: 40,
                ..base.clone()
            },
        ] {
            assert!(policy.should_rollover(&metrics));
        }
        assert!(!policy.should_rollover(&DocumentEpochMetrics {
            age_ms: u64::MAX,
            update_count: 0,
            ..base
        }));
        assert!(!policy.should_rollover(&DocumentEpochMetrics {
            epoch: 1,
            encoded_state_bytes: 35,
            delete_set_bytes: 45,
            baseline_encoded_state_bytes: 30,
            baseline_delete_set_bytes: 40,
            update_count: 1,
            active_connections: 0,
            started_at_ms: 0,
            age_ms: 0,
        }));
    }

    #[tokio::test]
    async fn manifest_round_trips_and_retires_after_window() {
        let root = std::env::temp_dir().join(format!("realtime-epoch-{}", nanoid::nanoid!()));
        tokio::fs::create_dir_all(&root).await.unwrap();
        let mut manifest = EpochManifest::initial("vault__doc", 10);
        let pending = manifest.begin(20).unwrap().clone();
        tokio::fs::create_dir(root.join(format!("{}.crdt", pending.physical_document_id)))
            .await
            .unwrap();
        manifest.activate(30, 40, 12, 3).unwrap();
        save_manifest(&root, &manifest).await.unwrap();

        let mut loaded = load_or_create_manifest(&root, "vault__doc").await.unwrap();
        assert_eq!(loaded.current.epoch, 1);
        assert_eq!(loaded.current.baseline_encoded_state_bytes, 12);
        assert_eq!(loaded.current.baseline_delete_set_bytes, 3);
        assert_eq!(loaded.retired[0].delete_after_ms, 70);
        remove_expired(&root, &mut loaded, 69).await.unwrap();
        assert_eq!(loaded.retired.len(), 1);
        remove_expired(&root, &mut loaded, 70).await.unwrap();
        assert!(loaded.retired.is_empty());
        tokio::fs::remove_dir_all(root).await.unwrap();
    }

    #[tokio::test]
    async fn store_validation_rejects_manifest_under_the_wrong_hash() {
        let root = std::env::temp_dir().join(format!("realtime-epoch-{}", nanoid::nanoid!()));
        tokio::fs::create_dir_all(&root).await.unwrap();
        let manifest = EpochManifest::initial("vault__doc", 10);
        let bytes = serde_json::to_vec(&manifest).unwrap();
        tokio::fs::write(root.join("epochs-wrong.json"), bytes)
            .await
            .unwrap();
        assert!(matches!(
            validate_store_manifests(&root).await,
            Err(EpochError::InvalidManifest(_))
        ));
        tokio::fs::remove_dir_all(root).await.unwrap();
    }

    #[tokio::test]
    async fn failed_retirement_delete_keeps_every_later_epoch_recoverable() {
        let root = std::env::temp_dir().join(format!("realtime-epoch-{}", nanoid::nanoid!()));
        tokio::fs::create_dir_all(&root).await.unwrap();
        let mut manifest = EpochManifest::initial("vault__doc", 0);
        manifest.begin(1).unwrap();
        manifest.activate(2, 0, 1, 1).unwrap();
        manifest.begin(3).unwrap();
        manifest.activate(4, 0, 1, 1).unwrap();

        // remove_dir_all rejects a file. The second retired epoch must remain
        // tracked rather than being dropped from the manifest on that error.
        tokio::fs::write(root.join("vault__doc.crdt"), b"not a directory")
            .await
            .unwrap();
        assert!(remove_expired(&root, &mut manifest, 5).await.is_err());
        assert_eq!(
            manifest
                .retired
                .iter()
                .map(|retired| retired.epoch)
                .collect::<Vec<_>>(),
            vec![0, 1]
        );
        tokio::fs::remove_dir_all(root).await.unwrap();
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        #[test]
        fn epoch_manifest_state_machine_preserves_successor_and_retirement_invariants(
            actions in prop::collection::vec(0u8..4, 1..100),
            recovery_window_ms in any::<u64>(),
        ) {
            let mut manifest = EpochManifest::initial("vault__doc", 0);
            let mut now_ms = 0u64;
            for action in actions {
                now_ms = now_ms.saturating_add(1);
                match action {
                    0 => {
                        let pending = manifest.begin(now_ms).unwrap();
                        prop_assert_eq!(pending.epoch, manifest.current.epoch.saturating_add(1));
                    }
                    1 if manifest.pending.is_some() => {
                        let previous = manifest.current.clone();
                        manifest
                            .activate(now_ms, recovery_window_ms, 1, 1)
                            .unwrap();
                        prop_assert_eq!(manifest.current.epoch, previous.epoch + 1);
                        let previous_is_retained = manifest.retired.iter().any(|retired| {
                            retired.epoch == previous.epoch
                                && retired.physical_document_id == previous.physical_document_id
                        });
                        prop_assert!(previous_is_retained);
                    }
                    2 => {
                        manifest.retired.retain(|retired| retired.delete_after_ms > now_ms);
                    }
                    _ => {}
                }
                prop_assert!(manifest.validate("vault__doc").is_ok());
                prop_assert!(manifest
                    .retired
                    .windows(2)
                    .all(|pair| pair[0].epoch < pair[1].epoch));
            }
        }
    }
}
