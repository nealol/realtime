//! Per-vault git audit log + backup.
//!
//! Every remote vault gets its own git repository under `config.git_data_dir`,
//! whose working tree mirrors the vault as plain-text markdown. Document writes
//! are coalesced with a debounce (default 5s) into a single commit, **attributed
//! to the authenticated principal** that made them.
//!
//! How attribution works: the server mints every per-user connection token and
//! terminates the native Yjs WebSocket data path. When a content write is seen
//! on a connection, the document engine calls
//! [`GitService::mark_write`] with the resolved [`Principal`]. Content itself is
//! pulled authoritatively from the native document store, so the client can never
//! forge file contents — only its own already-authenticated identity.
//!
//! The commit is a full-tree materialization from the *authoritative* CRDT state
//! (the vault index doc's `files` map + each file doc's `contents` text), so
//! creates / edits / deletes / renames all fall out of a single `git add -A`.
//!
//! Binary attachments (the index doc's `binaries` map) are included too: ones up
//! to `config.git_inline_attachment_max_bytes` are copied verbatim from the blob
//! store, larger ones become a text shim (see [`attachment_shim`]) carrying the
//! sha256 and an authenticated blob-download URL.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use sea_orm::{ActiveModelTrait, DatabaseConnection, EntityTrait, Set};
#[cfg(test)]
use serde_json::json;
use serde_json::Value as JsonValue;
use tokio::sync::Mutex;

use crate::config::Config;
use crate::crdt::DocumentStore;
use crate::entities::git_backups;
use crate::jobs::JobQueue;
use crate::plugindb::PluginDbService;
use crate::session::now_millis;
use crate::state::{Principal, PrincipalActor};
use crate::structured::canvas_to_file_json;
use crate::ydoc::{
    decode_binaries_entries, decode_config_entries, decode_files_map, decode_structured,
    decode_structured_index, decode_text,
};

/// A principal seen contributing to a vault during one debounce window.
type Contributor = Principal;

/// First line of the text shim committed in place of an oversized attachment.
/// Source of truth lives in `caps::ATTACHMENT_SHIM`; re-exported here so the
/// shim writer and the advertised cap value cannot drift.
pub(crate) use crate::caps::ATTACHMENT_SHIM as ATTACHMENT_SHIM_VERSION;

/// One entry of the working tree to materialize.
enum TreeContent {
    Text(String),
    /// Inline attachment: copy bytes from the blob store at `src`. `shim` is
    /// the fallback written when the blob file is missing on disk, so the
    /// commit still records the attachment instead of failing or pruning it.
    BlobCopy {
        src: PathBuf,
        hash: String,
        shim: String,
    },
}

/// Git-LFS-style pointer for attachments too large to inline in the repo.
/// The `url` line is the authenticated Realtime API download endpoint; it is
/// omitted when no public base URL is configured.
fn attachment_shim(public_base_url: &str, vault_id: &str, hash: &str, size: u64) -> String {
    let mut shim = format!(
        "version {ATTACHMENT_SHIM_VERSION}\noid sha256:{hash}\nsize {size}\nvault {vault_id}\n"
    );
    let base = public_base_url.trim_end_matches('/');
    if !base.is_empty() {
        shim.push_str(&format!("url {base}/api/vaults/{vault_id}/blobs/{hash}\n"));
    }
    shim
}

struct VaultState {
    /// Serializes actual commit work per vault, including out-of-band
    /// [`GitService::commit_now`] calls (rollback), so two materializations
    /// never interleave.
    commit_lock: Arc<Mutex<()>>,
}

impl VaultState {
    fn new() -> Self {
        VaultState {
            commit_lock: Arc::new(Mutex::new(())),
        }
    }
}

/// Overrides the generated commit subject and appends extra trailers; used by
/// rollback to stamp `Rollback to …` / `Rollback-Of:` on its commit.
#[derive(Clone, Debug, Default)]
pub struct CommitOverride {
    pub subject: String,
    pub trailers: Vec<(String, String)>,
}

struct Inner {
    config: Arc<Config>,
    db: DatabaseConnection,
    documents: DocumentStore,
    plugindb: PluginDbService,
    jobs: JobQueue,
    vaults: Mutex<HashMap<String, VaultState>>,
}

#[derive(Clone)]
pub struct GitService(Arc<Inner>);

impl GitService {
    pub fn new(
        config: Arc<Config>,
        db: DatabaseConnection,
        documents: DocumentStore,
        plugindb: PluginDbService,
        jobs: JobQueue,
    ) -> Self {
        GitService(Arc::new(Inner {
            config,
            db,
            documents,
            plugindb,
            jobs,
            vaults: Mutex::new(HashMap::new()),
        }))
    }

    fn debounce(&self) -> Duration {
        Duration::from_millis(self.0.config.git_debounce_ms)
    }

    /// Persist a content reconciliation attributed to `who`.
    pub async fn mark_write(&self, vault_id: &str, who: &Principal) {
        if !self.0.config.git_enabled {
            return;
        }
        if let Err(error) = self
            .0
            .jobs
            .enqueue_git(vault_id, who, self.debounce())
            .await
        {
            tracing::error!("queue git reconciliation for vault {vault_id} failed: {error:#}");
        }
    }

    pub(crate) async fn reconcile(
        &self,
        vault_id: &str,
        intent_key: &str,
        revision: i64,
    ) -> Result<()> {
        if !self.0.config.git_enabled {
            return Ok(());
        }
        let commit_lock = self.commit_lock(vault_id).await;
        let committed = {
            let _guard = commit_lock.lock().await;
            self.commit_once(vault_id, &[], None, Some((intent_key, revision)))
                .await?
        };
        if committed {
            self.0.jobs.enqueue_backup(vault_id).await?;
        }
        Ok(())
    }

    /// Commit the vault's current state immediately, bypassing the queue delay.
    /// Holds the per-vault commit lock, then returns the new HEAD hash if a
    /// commit was made. A queued reconciliation later observes a clean tree.
    /// Used by rollback so the "Rollback to …" commit's tree matches the
    /// rollback target instead of coalescing later edits.
    pub async fn commit_now(
        &self,
        vault_id: &str,
        who: &Principal,
        ov: CommitOverride,
    ) -> Result<Option<String>> {
        if !self.0.config.git_enabled {
            return Ok(None);
        }
        let commit_lock = self.commit_lock(vault_id).await;
        let committed = {
            let _guard = commit_lock.lock().await;
            self.commit_once(vault_id, std::slice::from_ref(who), Some(&ov), None)
                .await?
        };
        if !committed {
            return Ok(None);
        }
        if let Err(error) = self.0.jobs.enqueue_backup(vault_id).await {
            tracing::error!("queue backup push for vault {vault_id} failed: {error:#}");
        }
        let repo = self.repo_path(vault_id)?;
        let out = self.git_raw(&repo, &["rev-parse", "HEAD"]).await?;
        if !out.status.success() {
            return Ok(None);
        }
        Ok(Some(
            String::from_utf8_lossy(&out.stdout).trim().to_string(),
        ))
    }

    async fn commit_lock(&self, vault_id: &str) -> Arc<Mutex<()>> {
        self.0
            .vaults
            .lock()
            .await
            .entry(vault_id.to_string())
            .or_insert_with(VaultState::new)
            .commit_lock
            .clone()
    }

    /// The vault repo's directory, if the repo has been initialized.
    pub fn repo_dir(&self, vault_id: &str) -> Result<Option<PathBuf>> {
        let repo = self.repo_path(vault_id)?;
        Ok(repo.join(".git").is_dir().then_some(repo))
    }

    /// Run a git command in the vault's repo, returning the raw output.
    pub async fn git_output(&self, vault_id: &str, args: &[&str]) -> Result<std::process::Output> {
        let repo = self.repo_path(vault_id)?;
        self.git_raw(&repo, args).await
    }

    /// Materialize the vault's current CRDT state and commit the diff.
    /// Returns whether a commit was actually made.
    async fn commit_once(
        &self,
        vault_id: &str,
        contributors: &[Contributor],
        ov: Option<&CommitOverride>,
        intent: Option<(&str, i64)>,
    ) -> Result<bool> {
        self.0.plugindb.reconcile_vault(vault_id).await?;
        let repo = self.ensure_repo(vault_id).await?;

        // 1. Authoritative file set from the vault index doc's `files` map.
        let index_update = self.fetch_as_update(vault_id).await?;
        let files = decode_files_map(&index_update)?; // Vec<(path, guid)>
        let structured = decode_structured_index(&index_update)?;

        // 2. Reconstruct each markdown/structured file from its own doc.
        let mut tree: Vec<(PathBuf, TreeContent)> =
            Vec::with_capacity(files.len() + structured.len());
        for (path, guid) in &files {
            let rel = match safe_rel_path(path) {
                Ok(rel) => rel,
                Err(e) => {
                    tracing::warn!("git audit: skipping unsafe path {path:?}: {e}");
                    continue;
                }
            };
            let doc_id = format!("{vault_id}__{guid}");
            match self.fetch_as_update(&doc_id).await {
                Ok(update) => match decode_text(&update, "contents") {
                    Ok(content) => tree.push((rel, TreeContent::Text(content))),
                    Err(e) => tracing::warn!("git audit: decode {path} failed: {e}"),
                },
                Err(e) => tracing::warn!("git audit: fetch {doc_id} failed: {e}"),
            }
        }
        for entry in &structured {
            let rel = match safe_rel_path(&entry.path) {
                Ok(rel) => rel,
                Err(e) => {
                    tracing::warn!("git audit: skipping unsafe path {:?}: {e}", entry.path);
                    continue;
                }
            };
            let doc_id = format!("{vault_id}__{}", entry.guid);
            match self.fetch_as_update(&doc_id).await {
                Ok(update) => match decode_structured(&update)
                    .and_then(|value| serialize_structured_for_git(&entry.kind, value))
                {
                    Ok(content) => tree.push((rel, TreeContent::Text(content))),
                    Err(e) => tracing::warn!("git audit: decode {} failed: {e}", entry.path),
                },
                Err(e) => tracing::warn!("git audit: fetch {doc_id} failed: {e}"),
            }
        }

        // 2a. Binary attachments from the `binaries` map, plus synced config
        // folder files from the `configFiles` map (same blob-store shape).
        // Small ones are committed verbatim from the blob store; larger ones
        // as a text shim pointing at the authenticated blob download endpoint.
        let mut blob_entries = decode_binaries_entries(&index_update)?;
        blob_entries.extend(decode_config_entries(&index_update)?);
        for entry in blob_entries {
            let rel = match safe_rel_path(&entry.path) {
                Ok(rel) => rel,
                Err(e) => {
                    tracing::warn!("git audit: skipping unsafe path {:?}: {e}", entry.path);
                    continue;
                }
            };
            let src =
                match crate::blobs::blob_fs_path(&self.0.config.blob_dir, vault_id, &entry.hash) {
                    Ok(src) => src,
                    Err(e) => {
                        tracing::warn!("git audit: skipping attachment {:?}: {e}", entry.path);
                        continue;
                    }
                };
            let shim = attachment_shim(
                &self.0.config.public_base_url,
                vault_id,
                &entry.hash,
                entry.size,
            );
            if entry.size <= self.0.config.git_inline_attachment_max_bytes {
                tree.push((
                    rel,
                    TreeContent::BlobCopy {
                        src,
                        hash: entry.hash,
                        shim,
                    },
                ));
            } else {
                tree.push((rel, TreeContent::Text(shim)));
            }
        }

        // 2b. Deterministic SQL dumps of synced plugin databases (when the
        // cr-sqlite extension is configured). Purged DBs are excluded, so the
        // prune step in materialize_tree removes their dumps → "deleted" commits.
        for (rel, sql) in self.0.plugindb.dumps_for_vault(vault_id).await {
            tree.push((rel, TreeContent::Text(sql)));
        }

        // 3. Write the tree to disk (and prune anything no longer present).
        let repo_for_blocking = repo.clone();
        tokio::task::spawn_blocking(move || materialize_tree(&repo_for_blocking, &tree))
            .await
            .context("materialize task panicked")??;

        // 4. Claim attribution after materialization, immediately before the
        // commit boundary. Later writes remain queued for the next revision.
        let claimed;
        let (claim_revision, contributors) = if let Some((intent_key, revision)) = intent {
            claimed = self.0.jobs.load_contributors(intent_key, revision).await?;
            (Some(claimed.0), claimed.1.as_slice())
        } else {
            (None, contributors)
        };
        let committed = self.commit(&repo, vault_id, contributors, ov).await?;
        if let (Some((intent_key, _)), Some(revision)) = (intent, claim_revision) {
            self.0.jobs.clear_contributors(intent_key, revision).await?;
        }
        Ok(committed)
    }

    /// Lazily create the vault's repo with a bot identity. Idempotent.
    async fn ensure_repo(&self, vault_id: &str) -> Result<PathBuf> {
        let repo = self.repo_path(vault_id)?;
        if repo.join(".git").is_dir() {
            return Ok(repo);
        }
        tokio::fs::create_dir_all(&repo)
            .await
            .with_context(|| format!("create repo dir {}", repo.display()))?;
        self.git(&repo, &["init", "-q"]).await?;
        self.git(&repo, &["config", "core.autocrlf", "false"])
            .await?;
        self.git(&repo, &["config", "user.name", &self.0.config.git_bot_name])
            .await?;
        self.git(
            &repo,
            &["config", "user.email", &self.0.config.git_bot_email],
        )
        .await?;
        Ok(repo)
    }

    fn repo_path(&self, vault_id: &str) -> Result<PathBuf> {
        // vault_id is a server-issued UUID, but guard against separators regardless.
        if vault_id.is_empty()
            || vault_id.contains('/')
            || vault_id.contains('\\')
            || vault_id.contains("..")
        {
            bail!("invalid vault id");
        }
        Ok(PathBuf::from(&self.0.config.git_data_dir).join(vault_id))
    }

    /// Fetch a document's full state from the native document engine.
    async fn fetch_as_update(&self, doc_id: &str) -> Result<Vec<u8>> {
        crate::ydoc::read_update_with(&self.0.documents, doc_id)
            .await
            .map_err(|e| anyhow::anyhow!(e.to_string()))
    }

    /// Stage everything and commit if the working tree actually changed.
    /// Returns whether a commit was made.
    async fn commit(
        &self,
        repo: &Path,
        vault_id: &str,
        contributors: &[Contributor],
        ov: Option<&CommitOverride>,
    ) -> Result<bool> {
        self.git(repo, &["add", "-A"]).await?;

        // The staged diff drives both the no-op check and the commit subject.
        let diff = self
            .git_raw(
                repo,
                &["diff", "--cached", "--name-status", "--find-renames"],
            )
            .await?;
        if !diff.status.success() {
            bail!(
                "git diff --cached failed ({}): {}",
                diff.status,
                String::from_utf8_lossy(&diff.stderr).trim()
            );
        }
        let changes = parse_name_status(&String::from_utf8_lossy(&diff.stdout));
        if changes.is_empty() {
            return Ok(false); // nothing changed — idempotent no-op
        }
        let link_only = self.link_only_updates(repo, &changes).await;

        let (author, message) = build_commit_meta(
            vault_id,
            &changes,
            &link_only,
            contributors,
            &self.0.config,
            ov,
        );
        // Pin the committer to the Realtime bot via command-scoped (`-c`) config so
        // it never falls back to the server's *global* git identity — regardless of
        // what (if anything) is in global config or whether ensure_repo's local
        // config write ran. The author is the attributed principal (or the bot).
        let committer_name = format!("user.name={}", self.0.config.git_bot_name);
        let committer_email = format!("user.email={}", self.0.config.git_bot_email);
        self.git(
            repo,
            &[
                "-c",
                "commit.gpgsign=false",
                "-c",
                &committer_name,
                "-c",
                &committer_email,
                "commit",
                "--no-allow-empty",
                "--author",
                &author,
                "-m",
                &message,
            ],
        )
        .await?;

        Ok(true)
    }

    /// Of the staged modifications, find the ones that are purely link updates
    /// caused by this commit's renames (every changed line is the old line with
    /// references to a renamed file swapped for the new name). Only meaningful
    /// when the commit could read "Rename X to Y and update links" — i.e. when
    /// every non-rename change is a modification; otherwise returns empty.
    async fn link_only_updates(&self, repo: &Path, changes: &[StagedChange]) -> HashSet<String> {
        let renames: Vec<(String, String)> = changes
            .iter()
            .filter_map(|c| Some((c.path.clone(), c.renamed_to.clone()?)))
            .collect();
        if renames.is_empty() || !changes.iter().all(|c| matches!(c.status, 'R' | 'M')) {
            return HashSet::new();
        }
        let mut link_only = HashSet::new();
        for change in changes.iter().filter(|c| c.status == 'M') {
            let out = self
                .git_raw(repo, &["diff", "--cached", "--", &change.path])
                .await;
            if let Ok(out) = out {
                if out.status.success()
                    && is_link_only_update(&String::from_utf8_lossy(&out.stdout), &renames)
                {
                    link_only.insert(change.path.clone());
                }
            }
        }
        link_only
    }

    /// Push the vault repo to its configured backup remote (if any), recording
    /// the outcome on the `git_backups` row. The remote URL is passed on the
    /// command line each time, so neither it nor any credential ever lands in
    /// the repo's `.git/config`.
    async fn push(&self, vault_id: &str, repo: &Path) -> Result<()> {
        let Some(cfg) = git_backups::Entity::find_by_id(vault_id.to_string())
            .one(&self.0.db)
            .await?
        else {
            return Ok(());
        };
        if !cfg.enabled {
            return Ok(());
        }

        let refspec = format!("HEAD:refs/heads/{}", cfg.branch);
        let remote_url = cfg.remote_url.clone();
        let result = self
            .run_remote_git(vault_id, &cfg, &["push", &remote_url, &refspec], Some(repo))
            .await;

        let mut active: git_backups::ActiveModel = cfg.into();
        match &result {
            Ok(()) => {
                active.last_push_at = Set(Some(now_millis()));
                active.last_push_error = Set(None);
            }
            Err(e) => active.last_push_error = Set(Some(format!("{e:#}"))),
        }
        active.updated_at = Set(now_millis());
        active.update(&self.0.db).await?;
        result
    }

    pub(crate) async fn push_backup(&self, vault_id: &str) -> Result<()> {
        let Some(repo) = self.repo_dir(vault_id)? else {
            return Ok(());
        };
        self.push(vault_id, &repo).await
    }

    /// Validate a backup config by listing the remote's refs with the same
    /// credential wiring a real push would use.
    pub async fn test_remote(&self, vault_id: &str, cfg: &git_backups::Model) -> Result<()> {
        let remote_url = cfg.remote_url.clone();
        self.run_remote_git(vault_id, cfg, &["ls-remote", &remote_url, "HEAD"], None)
            .await
    }

    /// Run a credentialed git command against a backup remote. For SSH the
    /// private key is materialized (0600) next to the vault repo and wired via
    /// `GIT_SSH_COMMAND`; for HTTPS the token reaches git through an inline
    /// credential helper reading an env var, so it never appears in argv.
    async fn run_remote_git(
        &self,
        vault_id: &str,
        cfg: &git_backups::Model,
        args: &[&str],
        repo: Option<&Path>,
    ) -> Result<()> {
        let mut cmd = tokio::process::Command::new("git");
        if let Some(repo) = repo {
            cmd.arg("-C").arg(repo);
        }
        cmd.env("GIT_TERMINAL_PROMPT", "0");
        match cfg.auth_method.as_str() {
            "ssh" => {
                let key = cfg
                    .ssh_private_key
                    .as_deref()
                    .context("ssh backup has no private key")?;
                let key_path = self.ssh_key_path(vault_id)?;
                write_private_key(&key_path, key).await?;
                cmd.env(
                    "GIT_SSH_COMMAND",
                    format!(
                        "ssh -i {} -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new -o BatchMode=yes",
                        shell_quote(&key_path.to_string_lossy())
                    ),
                );
            }
            "https" => {
                let token = cfg
                    .https_token
                    .as_deref()
                    .context("https backup has no token")?;
                cmd.arg("-c").arg(
                    "credential.helper=!f() { echo \"username=x-token\"; echo \"password=$GIT_BACKUP_TOKEN\"; }; f",
                );
                cmd.env("GIT_BACKUP_TOKEN", token);
            }
            // "none": credential-less remotes (file:// paths in tests).
            "none" => {}
            other => bail!("unknown backup auth method {other:?}"),
        }
        cmd.args(args);

        let out = tokio::time::timeout(Duration::from_secs(60), cmd.output())
            .await
            .context("git remote operation timed out")?
            .with_context(|| format!("spawn git {args:?}"))?;
        if !out.status.success() {
            bail!(
                "git {} failed ({}): {}",
                args.first().copied().unwrap_or("?"),
                out.status,
                String::from_utf8_lossy(&out.stderr).trim()
            );
        }
        Ok(())
    }

    fn ssh_key_path(&self, vault_id: &str) -> Result<PathBuf> {
        let repo = self.repo_path(vault_id)?;
        Ok(repo.with_extension("ssh_key"))
    }

    /// Best-effort removal of the materialized SSH key (on backup deletion).
    pub async fn remove_backup_key_file(&self, vault_id: &str) {
        if let Ok(path) = self.ssh_key_path(vault_id) {
            let _ = tokio::fs::remove_file(path).await;
        }
    }

    /// Run `git -C <repo> <args>`, returning an error on non-zero exit.
    async fn git(&self, repo: &Path, args: &[&str]) -> Result<()> {
        let out = self.git_raw(repo, args).await?;
        if !out.status.success() {
            bail!(
                "git {:?} failed ({}): {}",
                args,
                out.status,
                String::from_utf8_lossy(&out.stderr).trim()
            );
        }
        Ok(())
    }

    /// Run `git -C <repo> <args>`, returning the raw output (caller checks status).
    async fn git_raw(&self, repo: &Path, args: &[&str]) -> Result<std::process::Output> {
        tokio::process::Command::new("git")
            .arg("-C")
            .arg(repo)
            .args(args)
            .env("GIT_TERMINAL_PROMPT", "0")
            .output()
            .await
            .with_context(|| format!("spawn git {args:?}"))
    }
}

/// Generate an ed25519 deploy keypair, returning (private OpenSSH, public OpenSSH).
pub fn generate_ssh_keypair() -> Result<(String, String)> {
    use ssh_key::{Algorithm, LineEnding, PrivateKey};
    let key = PrivateKey::random(&mut rand::rngs::OsRng, Algorithm::Ed25519)
        .context("generate ed25519 key")?;
    let private = key.to_openssh(LineEnding::LF)?.to_string();
    let public = key.public_key().to_openssh()?;
    Ok((private, format!("{public}\n")))
}

/// Write an SSH private key with owner-only permissions (ssh refuses 0644 keys).
async fn write_private_key(path: &Path, key: &str) -> Result<()> {
    tokio::fs::write(path, key)
        .await
        .with_context(|| format!("write ssh key {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .await
            .with_context(|| format!("chmod ssh key {}", path.display()))?;
    }
    Ok(())
}

/// Single-quote a string for embedding in `GIT_SSH_COMMAND` (parsed by sh).
fn shell_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', r"'\''"))
}

/// One staged change: git name-status letter (A/M/D/R) plus the path
/// (for renames, the old path, with the destination in `renamed_to`).
#[derive(Clone, Debug, PartialEq, Eq)]
struct StagedChange {
    status: char,
    path: String,
    renamed_to: Option<String>,
}

/// Parse `git diff --cached --name-status --find-renames` output:
/// `X\tpath` lines, or `R<score>\told\tnew` for renames.
fn parse_name_status(output: &str) -> Vec<StagedChange> {
    output
        .lines()
        .filter_map(|line| {
            let (status, rest) = line.split_once('\t')?;
            let status = status.chars().next()?;
            let (path, renamed_to) = if status == 'R' {
                let (old, new) = rest.split_once('\t')?;
                (old.to_string(), Some(new.to_string()))
            } else {
                (rest.to_string(), None)
            };
            Some(StagedChange {
                status,
                path,
                renamed_to,
            })
        })
        .collect()
}

/// Most files listed in a commit subject before falling back to a count.
const SUBJECT_MAX_FILES: usize = 3;

/// Git-repo directory holding plugin-database SQL dumps
/// (`.sql/{plugin_id}/{db_name}.sql`).
pub(crate) const SQL_DUMP_DIR: &str = ".sql";

/// Parse a `.sql/{plugin_id}/{db_name}.sql` dump path into (plugin_id, db_name).
pub(crate) fn sql_dump_parts(path: &str) -> Option<(&str, &str)> {
    let rest = path.strip_prefix(SQL_DUMP_DIR)?.strip_prefix('/')?;
    let (plugin, file) = rest.split_once('/')?;
    let name = file.strip_suffix(".sql")?;
    if plugin.is_empty() || name.is_empty() || name.contains('/') {
        return None;
    }
    Some((plugin, name))
}

/// How one staged change reads in a commit subject: plugin-database dumps are
/// phrased by plugin, renames as `old → new`, everything else by its vault path.
fn change_label(change: &StagedChange) -> String {
    if let Some(new) = &change.renamed_to {
        return format!("{} → {new}", change.path);
    }
    match sql_dump_parts(&change.path) {
        Some((plugin, name)) => format!("{plugin} plugin database ({name})"),
        None => change.path.to_string(),
    }
}

/// The strings a rename plausibly changes inside other notes' links: the full
/// path, the path without `.md`, the basename, and the basename stem —
/// longest-first so broader forms are rewritten before their substrings.
fn rename_link_variants(old: &str, new: &str) -> Vec<(String, String)> {
    let mut variants = vec![(old.to_string(), new.to_string())];
    if let (Some(o), Some(n)) = (old.strip_suffix(".md"), new.strip_suffix(".md")) {
        variants.push((o.to_string(), n.to_string()));
    }
    let (old_base, new_base) = (
        old.rsplit('/').next().unwrap_or(old),
        new.rsplit('/').next().unwrap_or(new),
    );
    if old_base != old {
        variants.push((old_base.to_string(), new_base.to_string()));
    }
    if let (Some(o), Some(n)) = (old_base.strip_suffix(".md"), new_base.strip_suffix(".md")) {
        variants.push((o.to_string(), n.to_string()));
    }
    variants.sort_by_key(|(o, _)| std::cmp::Reverse(o.len()));
    variants.dedup();
    variants
}

/// True when a staged file diff consists solely of lines whose only difference
/// is references to renamed files being updated: every removed line, with the
/// rename variants substituted, equals the matching added line. Conservative —
/// any insertion, deletion, or unrelated edit makes this false.
fn is_link_only_update(diff: &str, renames: &[(String, String)]) -> bool {
    let mut removed = Vec::new();
    let mut added = Vec::new();
    for line in diff.lines() {
        if line.starts_with("--- ") || line.starts_with("+++ ") {
            continue;
        }
        if let Some(rest) = line.strip_prefix('-') {
            removed.push(rest);
        } else if let Some(rest) = line.strip_prefix('+') {
            added.push(rest);
        }
    }
    if removed.is_empty() || removed.len() != added.len() {
        return false;
    }
    let mut variants: Vec<(String, String)> = renames
        .iter()
        .flat_map(|(old, new)| rename_link_variants(old, new))
        .collect();
    variants.sort_by_key(|(o, _)| std::cmp::Reverse(o.len()));
    removed.iter().zip(&added).all(|(before, after)| {
        let mut rewritten = before.to_string();
        for (old, new) in &variants {
            rewritten = rewritten.replace(old.as_str(), new);
        }
        rewritten == **after
    })
}

/// A rename whose basename is unchanged — the file just moved folders.
fn is_move(change: &StagedChange) -> bool {
    let Some(new) = &change.renamed_to else {
        return false;
    };
    let old_base = change.path.rsplit('/').next().unwrap_or(&change.path);
    let new_base = new.rsplit('/').next().unwrap_or(new);
    old_base == new_base
}

/// The subject verb a change reads under: renames split into "rename" (name
/// changed) and "move" (folder changed); unknown statuses (e.g. T) group with
/// "update". Returned in display order.
fn change_verb(change: &StagedChange) -> &'static str {
    match change.status {
        'R' if is_move(change) => "move",
        'R' => "rename",
        'A' => "add",
        'D' => "delete",
        _ => "update",
    }
}

const VERB_ORDER: [&str; 5] = ["rename", "move", "add", "update", "delete"];

/// Summarize staged changes as a commit subject. Rename/move-led commits —
/// renames plus, at most, modifications that are pure link updates for those
/// renames — read as "Rename Old.md → New.md[ and update links]" (or "Move …"
/// for folder-only changes). Otherwise each kind is listed under its own verb,
/// or just counted when more than [`SUBJECT_MAX_FILES`] entries changed.
fn commit_subject(changes: &[StagedChange], link_only: &HashSet<String>) -> String {
    if changes.is_empty() {
        return "Record contributors".to_string();
    }
    let (renames, others): (Vec<&StagedChange>, Vec<&StagedChange>) =
        changes.iter().partition(|c| c.status == 'R');
    if !renames.is_empty() && others.iter().all(|c| link_only.contains(&c.path)) {
        let mut subject = if renames.len() <= SUBJECT_MAX_FILES {
            let mut segments: Vec<String> = Vec::new();
            for verb in ["rename", "move"] {
                let pairs: Vec<String> = renames
                    .iter()
                    .filter(|c| change_verb(c) == verb)
                    .map(|c| format!("{} → {}", c.path, c.renamed_to.as_deref().unwrap_or("?")))
                    .collect();
                if !pairs.is_empty() {
                    segments.push(format!("{verb} {}", pairs.join(", ")));
                }
            }
            segments.join(", ")
        } else if renames.iter().all(|c| is_move(c)) {
            format!("move {} files", renames.len())
        } else {
            format!("rename {} files", renames.len())
        };
        if !others.is_empty() {
            subject.push_str(" and update links");
        }
        subject[..1].make_ascii_uppercase();
        return subject;
    }

    if changes.len() <= SUBJECT_MAX_FILES {
        // List each kind under its own verb: "Rename a.md → b.md, update c.md".
        let mut segments: Vec<String> = Vec::new();
        for verb in VERB_ORDER {
            let labels: Vec<String> = changes
                .iter()
                .filter(|c| change_verb(c) == verb)
                .map(change_label)
                .collect();
            if !labels.is_empty() {
                segments.push(format!("{verb} {}", labels.join(", ")));
            }
        }
        let mut subject = segments.join(", ");
        subject[..1].make_ascii_uppercase();
        return subject;
    }

    let verb = match changes.iter().map(|c| c.status).collect::<HashSet<_>>() {
        s if s.len() == 1 => match changes[0].status {
            'A' => "Add",
            'D' => "Delete",
            _ => "Update",
        },
        _ => "Update",
    };
    if changes.iter().all(|c| sql_dump_parts(&c.path).is_some()) {
        format!("{verb} {} plugin databases", changes.len())
    } else {
        format!("{verb} {} files", changes.len())
    }
}

/// Build the `--author` value and the commit message (with structured trailers).
fn build_commit_meta(
    vault_id: &str,
    changes: &[StagedChange],
    link_only: &HashSet<String>,
    contributors: &[Contributor],
    config: &Config,
    ov: Option<&CommitOverride>,
) -> (String, String) {
    let (author_name, author_email) = match contributors.first() {
        Some(p) => author_identity(p, config),
        None => (config.git_bot_name.clone(), config.git_bot_email.clone()),
    };
    let author = format!("{author_name} <{author_email}>");

    let subject = match ov {
        Some(ov) if !ov.subject.is_empty() => ov.subject.clone(),
        _ => commit_subject(changes, link_only),
    };
    let mut message = format!("{subject}\n\n");
    message.push_str(&format!("Vault-Id: {vault_id}\n"));
    if let Some(p) = contributors.first() {
        append_principal_trailers(&mut message, p);
    }
    for p in contributors.iter().skip(1) {
        let (name, email) = author_identity(p, config);
        message.push_str(&format!("Co-authored-by: {} <{}>\n", name, email));
        append_principal_trailers(&mut message, p);
    }
    if let Some(ov) = ov {
        for (key, value) in &ov.trailers {
            message.push_str(&format!("{key}: {value}\n"));
        }
    }
    (author, message)
}

fn author_identity(principal: &Principal, config: &Config) -> (String, String) {
    match &principal.actor {
        PrincipalActor::User => (
            principal.display_name.clone(),
            principal
                .git_email
                .clone()
                .unwrap_or_else(|| principal.email.clone()),
        ),
        PrincipalActor::Cursor {
            app_id,
            cursor_name,
            ..
        } => (
            cursor_name.clone(),
            format!("cursor+{app_id}@{}", config.cursor_email_domain),
        ),
    }
}

fn append_principal_trailers(message: &mut String, principal: &Principal) {
    match &principal.actor {
        PrincipalActor::User => {
            message.push_str(&format!("Principal-Id: {}\n", principal.user_id));
            message.push_str("Principal-Type: user\n");
        }
        PrincipalActor::Cursor {
            cursor_id,
            cursor_name,
            ..
        } => {
            message.push_str("Principal-Type: cursor\n");
            message.push_str(&format!("Cursor-Id: {cursor_id}\n"));
            message.push_str(&format!("Cursor-Name: {cursor_name}\n"));
            message.push_str(&format!(
                "On-Behalf-Of: {} <{}>\n",
                principal.display_name, principal.email
            ));
            message.push_str(&format!("Authorized-User-Id: {}\n", principal.user_id));
        }
    }
}

/// Validate a vault-relative path and turn it into a safe relative `PathBuf`.
/// Rejects absolute paths, `..`, `.`, empty components, and backslashes.
pub(crate) fn safe_rel_path(path: &str) -> Result<PathBuf> {
    if path.is_empty() || path.contains('\\') {
        bail!("unsafe path {path:?}");
    }
    let mut rel = PathBuf::new();
    for comp in path.split('/') {
        if comp.is_empty() || comp == "." || comp == ".." {
            bail!("unsafe path component in {path:?}");
        }
        rel.push(comp);
    }
    Ok(rel)
}

fn serialize_structured_for_git(kind: &str, value: JsonValue) -> Result<String> {
    match kind {
        "canvas" => Ok(format!(
            "{}\n",
            serde_json::to_string_pretty(&canvas_to_file_json(value))?
        )),
        "base" => Ok(serde_yaml::to_string(&value)?),
        other => bail!("unknown structured document kind {other}"),
    }
}

/// Write every file in `tree` and delete any working-tree file not present in it.
/// Runs on a blocking thread (synchronous fs walk).
fn materialize_tree(repo: &Path, tree: &[(PathBuf, TreeContent)]) -> Result<()> {
    let mut desired: HashSet<PathBuf> = HashSet::with_capacity(tree.len());
    for (rel, content) in tree {
        let full = repo.join(rel);
        if let Some(parent) = full.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("mkdir {}", parent.display()))?;
        }
        match content {
            TreeContent::Text(content) => {
                std::fs::write(&full, content)
                    .with_context(|| format!("write {}", full.display()))?;
            }
            TreeContent::BlobCopy { src, hash, shim } => {
                // Skip unchanged attachments so large files aren't rewritten on
                // every commit. The hash compare (not just size) also catches a
                // shim left behind by an earlier missing-blob fallback or a
                // threshold change: shim text never hashes to the blob's hash.
                if file_sha256(&full).as_deref() == Some(hash.as_str()) {
                    desired.insert(rel.clone());
                    continue;
                }
                if let Err(e) = std::fs::copy(src, &full) {
                    // Missing blob must not fail the whole commit; a shim keeps
                    // the path alive (pruning it would record a bogus delete).
                    tracing::warn!(
                        "git audit: blob {} for {} unavailable ({e}); writing shim",
                        src.display(),
                        rel.display()
                    );
                    std::fs::write(&full, shim)
                        .with_context(|| format!("write {}", full.display()))?;
                }
            }
        }
        desired.insert(rel.clone());
    }

    let mut present = Vec::new();
    collect_files(repo, repo, &mut present)?;
    for rel in present {
        if !desired.contains(&rel) {
            let _ = std::fs::remove_file(repo.join(&rel));
        }
    }
    Ok(())
}

/// Lowercase hex sha256 of a file's bytes, or `None` if it can't be read
/// (typically: it doesn't exist yet).
fn file_sha256(path: &Path) -> Option<String> {
    use sha2::{Digest, Sha256};
    let mut file = std::fs::File::open(path).ok()?;
    let mut hasher = Sha256::new();
    std::io::copy(&mut file, &mut hasher).ok()?;
    Some(
        hasher
            .finalize()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect(),
    )
}

/// Collect all file paths (relative to `root`) under `dir`, skipping the top-level `.git`.
fn collect_files(root: &Path, dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    for entry in std::fs::read_dir(dir).with_context(|| format!("read_dir {}", dir.display()))? {
        let entry = entry?;
        let path = entry.path();
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            if dir == root && entry.file_name() == ".git" {
                continue;
            }
            collect_files(root, &path, out)?;
        } else if let Ok(rel) = path.strip_prefix(root) {
            out.push(rel.to_path_buf());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use yrs::{Doc, Map, ReadTxn, Text, Transact};

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

    #[test]
    fn decode_text_roundtrips() {
        let update = text_update("contents", "# Hello\nworld");
        assert_eq!(decode_text(&update, "contents").unwrap(), "# Hello\nworld");
    }

    fn binaries_update(entries: &[(&str, yrs::Any)]) -> Vec<u8> {
        let doc = Doc::new();
        let map = doc.get_or_insert_map("binaries");
        {
            let mut txn = doc.transact_mut();
            for (path, meta) in entries {
                map.insert(&mut txn, path.to_string(), meta.clone());
            }
        }
        let update = doc
            .transact()
            .encode_state_as_update_v1(&yrs::StateVector::default());
        update
    }

    fn binary_meta_any(hash: &str, size: i64) -> yrs::Any {
        yrs::Any::from(std::collections::HashMap::from([
            ("hash".to_string(), yrs::Any::String(hash.into())),
            ("size".to_string(), yrs::Any::BigInt(size)),
        ]))
    }

    #[test]
    fn decode_binaries_entries_roundtrips_and_skips_malformed() {
        use crate::ydoc::BinaryEntry;
        let hash = "a".repeat(64);
        let update = binaries_update(&[
            ("img/a.png", binary_meta_any(&hash, 123)),
            ("not-a-map.png", yrs::Any::String("nope".into())),
            (
                "no-size.png",
                yrs::Any::from(std::collections::HashMap::from([(
                    "hash".to_string(),
                    yrs::Any::String(hash.as_str().into()),
                )])),
            ),
        ]);
        let got = decode_binaries_entries(&update).unwrap();
        assert_eq!(
            got,
            vec![BinaryEntry {
                path: "img/a.png".into(),
                hash: hash.clone(),
                size: 123,
            }]
        );
    }

    #[test]
    fn decode_config_entries_reads_config_files_map() {
        use crate::ydoc::BinaryEntry;
        let hash = "b".repeat(64);
        let doc = Doc::new();
        let map = doc.get_or_insert_map("configFiles");
        {
            let mut txn = doc.transact_mut();
            map.insert(
                &mut txn,
                ".obsidian/app.json".to_string(),
                binary_meta_any(&hash, 42),
            );
        }
        let update = doc
            .transact()
            .encode_state_as_update_v1(&yrs::StateVector::default());
        let got = decode_config_entries(&update).unwrap();
        assert_eq!(
            got,
            vec![BinaryEntry {
                path: ".obsidian/app.json".into(),
                hash,
                size: 42,
            }]
        );
        // Config paths survive the git path safety check (dotfolder ≠ dot component).
        assert!(safe_rel_path(".obsidian/app.json").is_ok());
    }

    #[test]
    fn attachment_shim_format() {
        let hash = "ab".repeat(32);
        // Trailing slash on the base URL must not produce a double slash.
        let shim = attachment_shim("http://x.test/", "v1", &hash, 42);
        assert_eq!(
            shim,
            format!(
                "version https://realtime.md/attachment-shim/v1\n\
                 oid sha256:{hash}\nsize 42\nvault v1\n\
                 url http://x.test/api/vaults/v1/blobs/{hash}\n"
            )
        );
    }

    #[test]
    fn attachment_shim_omits_url_without_base() {
        let hash = "ab".repeat(32);
        let shim = attachment_shim("", "v1", &hash, 42);
        assert!(!shim.contains("url "));
        assert!(shim.ends_with("vault v1\n"));
    }

    fn tmpdir(label: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "realtime-git-test-{label}-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn materialize_tree_inlines_blob_and_prunes() {
        let repo = tmpdir("repo");
        let blobs = tmpdir("blobs");
        let bytes: &[u8] = b"\x89PNG fake image bytes";
        let src = blobs.join("blob");
        std::fs::write(&src, bytes).unwrap();
        let hash = file_sha256(&src).unwrap();
        std::fs::write(repo.join("stale.md"), "gone").unwrap();

        let tree = vec![
            (PathBuf::from("a.md"), TreeContent::Text("hello".into())),
            (
                PathBuf::from("img/a.png"),
                TreeContent::BlobCopy {
                    src: src.clone(),
                    hash: hash.clone(),
                    shim: "SHIM".into(),
                },
            ),
        ];
        materialize_tree(&repo, &tree).unwrap();
        assert_eq!(std::fs::read(repo.join("img/a.png")).unwrap(), bytes);
        assert_eq!(std::fs::read_to_string(repo.join("a.md")).unwrap(), "hello");
        assert!(!repo.join("stale.md").exists());

        // Unchanged attachment: skipped, not re-copied — even if the source
        // blob has since vanished.
        std::fs::remove_file(&src).unwrap();
        materialize_tree(&repo, &tree).unwrap();
        assert_eq!(std::fs::read(repo.join("img/a.png")).unwrap(), bytes);

        // Attachment removed from the index: pruned like any other file.
        materialize_tree(&repo, &tree[..1]).unwrap();
        assert!(!repo.join("img/a.png").exists());
    }

    #[test]
    fn materialize_tree_writes_shim_when_blob_missing() {
        let repo = tmpdir("repo");
        let tree = vec![(
            PathBuf::from("big.pdf"),
            TreeContent::BlobCopy {
                src: PathBuf::from("/nonexistent/blob"),
                hash: "a".repeat(64),
                shim: "SHIM\n".into(),
            },
        )];
        materialize_tree(&repo, &tree).unwrap();
        assert_eq!(
            std::fs::read_to_string(repo.join("big.pdf")).unwrap(),
            "SHIM\n"
        );

        // Blob shows up later (re-upload): the shim is replaced by real bytes.
        let blobs = tmpdir("blobs");
        let bytes: &[u8] = b"real pdf bytes";
        let src = blobs.join("blob");
        std::fs::write(&src, bytes).unwrap();
        let tree = vec![(
            PathBuf::from("big.pdf"),
            TreeContent::BlobCopy {
                src,
                hash: file_sha256(&blobs.join("blob")).unwrap(),
                shim: "SHIM\n".into(),
            },
        )];
        materialize_tree(&repo, &tree).unwrap();
        assert_eq!(std::fs::read(repo.join("big.pdf")).unwrap(), bytes);
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

    fn changes(entries: &[(char, &str)]) -> Vec<StagedChange> {
        entries
            .iter()
            .map(|(status, path)| StagedChange {
                status: *status,
                path: path.to_string(),
                renamed_to: None,
            })
            .collect()
    }

    fn rename(old: &str, new: &str) -> StagedChange {
        StagedChange {
            status: 'R',
            path: old.to_string(),
            renamed_to: Some(new.to_string()),
        }
    }

    fn subject(changes: &[StagedChange]) -> String {
        commit_subject(changes, &HashSet::new())
    }

    #[test]
    fn parses_name_status_output() {
        let parsed = parse_name_status("M\tnote.md\nA\tdir/new.md\nD\told.md\nR100\ta.md\tb.md\n");
        let mut expected = changes(&[('M', "note.md"), ('A', "dir/new.md"), ('D', "old.md")]);
        expected.push(rename("a.md", "b.md"));
        assert_eq!(parsed, expected);
        assert!(parse_name_status("").is_empty());
    }

    #[test]
    fn commit_subject_phrases_renames() {
        assert_eq!(
            subject(&[rename("Old.md", "New.md")]),
            "Rename Old.md → New.md"
        );
        assert_eq!(
            subject(&[rename("a.md", "b.md"), rename("c.md", "dir/d.md")]),
            "Rename a.md → b.md, c.md → dir/d.md"
        );
        assert_eq!(
            subject(&[
                rename("a.md", "b.md"),
                rename("c.md", "d.md"),
                rename("e.md", "f.md"),
                rename("g.md", "h.md"),
            ]),
            "Rename 4 files"
        );
    }

    #[test]
    fn commit_subject_phrases_moves() {
        // Same basename, different folder → "move".
        assert_eq!(
            subject(&[rename("a/Note.md", "b/Note.md")]),
            "Move a/Note.md → b/Note.md"
        );
        // Renames and moves get their own segments.
        assert_eq!(
            subject(&[rename("Old.md", "New.md"), rename("a/Note.md", "b/Note.md")]),
            "Rename Old.md → New.md, move a/Note.md → b/Note.md"
        );
        // A move alongside a real edit lists per-kind verbs.
        let staged = vec![
            rename("a/Note.md", "b/Note.md"),
            changes(&[('M', "ref.md")]).remove(0),
        ];
        assert_eq!(
            commit_subject(&staged, &HashSet::new()),
            "Move a/Note.md → b/Note.md, update ref.md"
        );
        // ... but folds into the move when the edit is link-only.
        let link_only: HashSet<String> = ["ref.md".to_string()].into();
        assert_eq!(
            commit_subject(&staged, &link_only),
            "Move a/Note.md → b/Note.md and update links"
        );
        // All-move count form.
        assert_eq!(
            subject(&[
                rename("a/1.md", "b/1.md"),
                rename("a/2.md", "b/2.md"),
                rename("a/3.md", "b/3.md"),
                rename("a/4.md", "b/4.md"),
            ]),
            "Move 4 files"
        );
    }

    #[test]
    fn commit_subject_folds_link_only_updates_into_rename() {
        let staged = vec![
            rename("Old.md", "New.md"),
            changes(&[('M', "ref.md")]).remove(0),
        ];
        let link_only: HashSet<String> = ["ref.md".to_string()].into();
        assert_eq!(
            commit_subject(&staged, &link_only),
            "Rename Old.md → New.md and update links"
        );
        // A genuine edit alongside a rename lists each kind under its own verb.
        assert_eq!(
            commit_subject(&staged, &HashSet::new()),
            "Rename Old.md → New.md, update ref.md"
        );
    }

    #[test]
    fn link_only_diffs_detected() {
        let renames = vec![("dir/Old.md".to_string(), "dir/New.md".to_string())];
        let link_diff = "\
diff --git a/ref.md b/ref.md
--- a/ref.md
+++ b/ref.md
@@ -1,2 +1,2 @@
-See [[Old]] and [link](dir/Old.md).
+See [[New]] and [link](dir/New.md).
 unchanged";
        assert!(is_link_only_update(link_diff, &renames));

        let real_edit = "\
@@ -1 +1 @@
-See [[Old]] for context.
+See [[New]] for more context.";
        assert!(!is_link_only_update(real_edit, &renames));

        let addition = "\
@@ -1 +1,2 @@
 existing
+brand new line";
        assert!(!is_link_only_update(addition, &renames));
    }

    #[test]
    fn commit_subject_lists_few_files_with_matching_verb() {
        assert_eq!(subject(&[]), "Record contributors");
        assert_eq!(subject(&changes(&[('A', "a.md")])), "Add a.md");
        assert_eq!(subject(&changes(&[('D', "a.md")])), "Delete a.md");
        assert_eq!(
            subject(&changes(&[('M', "a.md"), ('M', "dir/b.md")])),
            "Update a.md, dir/b.md"
        );
        // Mixed change kinds each get their own verb.
        assert_eq!(
            subject(&changes(&[('A', "a.md"), ('D', "b.md")])),
            "Add a.md, delete b.md"
        );
    }

    #[test]
    fn commit_subject_phrases_plugin_database_dumps() {
        assert_eq!(
            subject(&changes(&[('M', ".sql/my-plugin/tasks.sql")])),
            "Update my-plugin plugin database (tasks)"
        );
        assert_eq!(
            subject(&changes(&[
                ('M', "a.md"),
                ('M', ".sql/my-plugin/tasks.sql")
            ])),
            "Update a.md, my-plugin plugin database (tasks)"
        );
        // All-dump commits beyond the listing limit count databases, not files.
        assert_eq!(
            subject(&changes(&[
                ('M', ".sql/p1/a.sql"),
                ('M', ".sql/p1/b.sql"),
                ('M', ".sql/p2/a.sql"),
                ('M', ".sql/p3/a.sql"),
            ])),
            "Update 4 plugin databases"
        );
        // A vault file that merely looks dump-like keeps its path.
        assert_eq!(
            subject(&changes(&[('M', "notes/.sql/x.sql")])),
            "Update notes/.sql/x.sql"
        );
    }

    #[test]
    fn commit_subject_counts_many_files() {
        assert_eq!(
            subject(&changes(&[
                ('M', "a.md"),
                ('M', "b.md"),
                ('A', "c.md"),
                ('M', "d.md"),
            ])),
            "Update 4 files"
        );
        assert_eq!(
            subject(&changes(&[
                ('A', "a.md"),
                ('A', "b.md"),
                ('A', "c.md"),
                ('A', "d.md"),
            ])),
            "Add 4 files"
        );
    }

    #[test]
    fn generated_keypair_is_valid_openssh() {
        let (private, public) = generate_ssh_keypair().unwrap();
        let parsed = ssh_key::PrivateKey::from_openssh(&private).unwrap();
        let pub_parsed = ssh_key::PublicKey::from_openssh(&public).unwrap();
        assert_eq!(parsed.public_key().key_data(), pub_parsed.key_data());
        assert!(public.starts_with("ssh-ed25519 "));
    }

    #[test]
    fn shell_quote_escapes_single_quotes() {
        assert_eq!(shell_quote("/a/b"), "'/a/b'");
        assert_eq!(shell_quote("a'b"), r"'a'\''b'");
    }

    #[test]
    fn safe_rel_path_rejects_traversal() {
        assert!(safe_rel_path("../etc/passwd").is_err());
        assert!(safe_rel_path("/abs").is_err());
        assert!(safe_rel_path("a/../b").is_err());
        assert!(safe_rel_path("a\\b").is_err());
        assert!(safe_rel_path("").is_err());
        assert_eq!(
            safe_rel_path("a/b.md").unwrap(),
            PathBuf::from("a").join("b.md")
        );
    }

    #[test]
    fn serializes_structured_canvas_for_git() {
        let value = json!({
            "nodes": {
                "n2": { "id": "n2", "type": "text", "text": "second" },
                "n1": { "id": "n1", "type": "text", "text": "first" }
            },
            "edges": {
                "e1": { "id": "e1", "fromNode": "n1", "toNode": "n2" }
            },
            "nodeOrder": ["n1", "n2"],
            "edgeOrder": ["e1"]
        });
        let serialized = serialize_structured_for_git("canvas", value).unwrap();
        assert_eq!(
            serde_json::from_str::<JsonValue>(&serialized).unwrap(),
            json!({
                "nodes": [
                    { "id": "n1", "type": "text", "text": "first" },
                    { "id": "n2", "type": "text", "text": "second" }
                ],
                "edges": [
                    { "id": "e1", "fromNode": "n1", "toNode": "n2" }
                ]
            })
        );
    }

    #[test]
    fn serializes_structured_base_for_git() {
        let serialized =
            serialize_structured_for_git("base", json!({ "views": [{ "type": "table" }] }))
                .unwrap();
        assert!(serialized.contains("views:"));
        assert!(serialized.contains("type: table"));
    }

    #[test]
    fn commit_meta_uses_primary_author_and_coauthors() {
        let config = test_config();
        let contributors = vec![
            Principal {
                user_id: "u1".into(),
                display_name: "Alice".into(),
                email: "a@x".into(),
                git_email: None,
                actor: PrincipalActor::User,
                expires_at_ms: 0,
            },
            Principal {
                user_id: "u2".into(),
                display_name: "Bob".into(),
                email: "b@x".into(),
                git_email: None,
                actor: PrincipalActor::User,
                expires_at_ms: 0,
            },
        ];
        let (author, message) = build_commit_meta(
            "v1",
            &changes(&[('M', "a.md")]),
            &HashSet::new(),
            &contributors,
            &config,
            None,
        );
        assert_eq!(author, "Alice <a@x>");
        assert!(message.contains("Principal-Id: u1"));
        assert!(message.contains("Co-authored-by: Bob <b@x>"));
        assert!(!message.contains("Co-authored-by: Alice"));
    }

    #[test]
    fn commit_meta_falls_back_to_bot() {
        let config = test_config();
        let (author, message) = build_commit_meta(
            "v1",
            &changes(&[('M', "a.md")]),
            &HashSet::new(),
            &[],
            &config,
            None,
        );
        assert_eq!(author, "Realtime <realtime@localhost>");
        assert!(!message.contains("Principal-Id"));
    }

    #[test]
    fn commit_meta_attributes_cursor_author_on_behalf_of_user() {
        let config = test_config();
        let contributors = vec![Principal {
            user_id: "u1".into(),
            display_name: "Alice".into(),
            email: "a@x".into(),
            git_email: None,
            actor: PrincipalActor::Cursor {
                cursor_id: "c1".into(),
                app_id: "app123".into(),
                cursor_name: "Claude".into(),
            },
            expires_at_ms: 0,
        }];

        let (author, message) = build_commit_meta(
            "v1",
            &changes(&[('M', "a.md")]),
            &HashSet::new(),
            &contributors,
            &config,
            None,
        );
        assert_eq!(author, "Claude <cursor+app123@localhost>");
        assert!(message.contains("Principal-Type: cursor"));
        assert!(message.contains("Cursor-Id: c1"));
        assert!(message.contains("Cursor-Name: Claude"));
        assert!(message.contains("On-Behalf-Of: Alice <a@x>"));
        assert!(message.contains("Authorized-User-Id: u1"));
    }

    #[test]
    fn commit_meta_override_replaces_subject_and_appends_trailer() {
        let config = test_config();
        let contributors = vec![Principal {
            user_id: "u1".into(),
            display_name: "Alice".into(),
            email: "a@x".into(),
            git_email: None,
            actor: PrincipalActor::User,
            expires_at_ms: 0,
        }];
        let ov = CommitOverride {
            subject: "Rollback to abc123 (2026-06-10 12:00)".into(),
            trailers: vec![("Rollback-Of".into(), "abc123".repeat(6))],
        };
        let (author, message) = build_commit_meta(
            "v1",
            &changes(&[('M', "a.md")]),
            &HashSet::new(),
            &contributors,
            &config,
            Some(&ov),
        );
        assert_eq!(author, "Alice <a@x>");
        assert!(message.starts_with("Rollback to abc123 (2026-06-10 12:00)\n\n"));
        assert!(message.contains(&format!("Rollback-Of: {}", "abc123".repeat(6))));
        assert!(message.contains("Principal-Id: u1"));
    }

    fn test_config() -> Config {
        Config {
            database_url: String::new(),
            background_jobs_enabled: true,
            background_job_concurrency: 4,
            background_job_max_attempts: 25,
            background_job_retry_min_ms: 250,
            background_job_retry_max_ms: 30_000,
            background_job_shutdown_timeout_ms: 30_000,
            server_shutdown_timeout_ms: 30_000,
            bind_addr: String::new(),
            public_base_url: String::new(),
            crdt_store_dir: std::env::temp_dir()
                .join(format!("realtime-git-test-{}", uuid::Uuid::new_v4()))
                .display()
                .to_string(),
            crdt_epoch_period_days: 365,
            crdt_epoch_recovery_days: 30,
            crdt_epoch_max_updates: 100_000,
            crdt_epoch_max_state_bytes: 32 * 1024 * 1024,
            crdt_epoch_max_delete_set_bytes: 8 * 1024 * 1024,
            crdt_epoch_ack_timeout_ms: 30_000,
            crdt_max_documents_per_vault: 100_000,
            blob_dir: String::new(),
            oidc_mode: crate::config::OidcMode::Mock,
            oidc_issuer: None,
            oidc_client_id: None,
            oidc_client_secret: None,
            oidc_redirect_url: None,
            allowed_login_redirects: vec![],
            cors_allowed_origins: vec![],
            git_data_dir: ".".into(),
            git_enabled: true,
            git_debounce_ms: 5000,
            git_bot_name: "Realtime".into(),
            git_bot_email: "realtime@localhost".into(),
            git_inline_attachment_max_bytes: 5 * 1024 * 1024,
            cursor_email_domain: "localhost".into(),
            daily_note_path_template: "Daily Notes/{{YYYY-MM-DD}}.md".into(),
            weekly_note_path_template: None,
            monthly_note_path_template: None,
            quarterly_note_path_template: None,
            yearly_note_path_template: None,
            attachment_fetch_host_allowlist: vec![],
            attachment_allowed_extensions: vec![
                "png".into(),
                "jpg".into(),
                "jpeg".into(),
                "gif".into(),
                "webp".into(),
                "svg".into(),
                "pdf".into(),
                "txt".into(),
            ],
            attachment_max_bytes: crate::blobs::MAX_BLOB_BYTES,
            attachments_path_mode: "relative".into(),
            attachments_subfolder: None,
            upload_token: "test-upload-token".into(),
            crsqlite_ext_path: None,
            web_dist_path: "../packages/web/dist".into(),
        }
    }
}
