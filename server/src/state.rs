use std::collections::HashMap;
use std::sync::Arc;

use sea_orm::DatabaseConnection;
use tokio::sync::Mutex;

use crate::session::now_millis;

use crate::config::Config;
use crate::crdt::{DocumentStore, Level};
use crate::git::GitService;
use crate::jobs::JobQueue;
use crate::operations::{RuntimeHealth, SyncMetrics};
use crate::plugindb::PluginDbService;
use crate::search::SearchService;

/// The actor that initiated a write attributed to a real authenticated user.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
pub enum PrincipalActor {
    User,
    Cursor {
        cursor_id: String,
        app_id: String,
        cursor_name: String,
    },
}

/// An authenticated principal bound to a document connection token at mint time,
/// used to attribute git audit commits. Cursor writes retain the real user in
/// `user_id` / `display_name` / `email` and store the cursor as `actor`.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
pub struct Principal {
    pub user_id: String,
    pub display_name: String,
    pub email: String,
    pub git_email: Option<String>,
    pub actor: PrincipalActor,
    /// Epoch millis after which this token->principal mapping is evicted.
    pub expires_at_ms: i64,
}

impl Principal {
    pub fn actor_key(&self) -> String {
        match &self.actor {
            PrincipalActor::User => format!("user:{}", self.user_id),
            PrincipalActor::Cursor { cursor_id, .. } => format!("cursor:{cursor_id}"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct SyncGrant {
    pub document_id: String,
    pub level: Level,
    pub epoch: u64,
    pub principal: Principal,
}

/// How often expired sync grants are swept. Lookups check expiry themselves,
/// so a sweep only bounds memory; doing it on every call made each token mint
/// and channel open O(live grants) under one lock.
const GRANT_PRUNE_INTERVAL_MS: i64 = 60_000;

/// Opaque, document-scoped sync tokens cached by this process.
#[derive(Default)]
pub struct SyncGrants {
    grants: HashMap<String, SyncGrant>,
    next_prune_at_ms: i64,
}

impl SyncGrants {
    fn prune_if_due(&mut self, now: i64) {
        if now < self.next_prune_at_ms {
            return;
        }
        self.grants
            .retain(|_, grant| grant.principal.expires_at_ms > now);
        self.next_prune_at_ms = now + GRANT_PRUNE_INTERVAL_MS;
    }
}

#[derive(Clone, Debug)]
pub struct PendingDocumentCreation {
    pub user_id: String,
    pub path: String,
    pub expires_at_ms: i64,
}

/// A synthetic identity used by the mock OIDC issuer (test mode).
#[derive(Clone)]
pub struct MockIdentity {
    pub issuer: String,
    pub subject: String,
    pub email: String,
    pub name: String,
    pub picture: Option<String>,
}

/// Pending OIDC Authorization-Code flow, keyed by `state`.
pub struct OidcFlow {
    pub pkce_verifier: String,
    pub nonce: String,
    pub redirect: String,
    pub created_at: i64,
    /// Present only in mock mode; lets the callback skip the IdP round-trip.
    pub mock: Option<MockIdentity>,
    pub oauth_flow_key: Option<String>,
}

impl OidcFlow {
    pub fn is_expired(&self) -> bool {
        now_millis() - self.created_at > 5 * 60 * 1000
    }
}

pub struct OAuthFlow {
    pub client_id: String,
    pub redirect_uri: String,
    pub code_challenge: String,
    pub scope: String,
    pub state: Option<String>,
    pub app_id: String,
    pub created_at: i64,
}

impl OAuthFlow {
    pub fn is_expired(&self) -> bool {
        now_millis() - self.created_at > 5 * 60 * 1000
    }
}

#[derive(Clone)]
pub struct AppState {
    pub db: DatabaseConnection,
    pub config: Arc<Config>,
    /// Stable, persisted id for this server, advertised via `/api/server-info`.
    pub server_id: String,
    /// Native persistent Yjs document engine.
    pub documents: DocumentStore,
    /// Persistent Apalis queue for derived server work.
    pub jobs: JobQueue,
    pub http: reqwest::Client,
    pub oidc: Arc<Mutex<HashMap<String, OidcFlow>>>,
    pub oauth_flows: Arc<Mutex<HashMap<String, OAuthFlow>>>,
    /// Per-vault git audit log + backup engine.
    pub git: GitService,
    /// Synced plugin-database replication engine.
    pub plugindb: PluginDbService,
    /// Persistent search reconciliation scheduler.
    pub search: SearchService,
    /// Opaque, document-scoped sync tokens cached by this process.
    pub sync_grants: Arc<Mutex<SyncGrants>>,
    /// Short-lived creation reservations for client-created file documents.
    /// A reservation is keyed by document id and bound to its creator/path.
    pub pending_document_creations: Arc<Mutex<HashMap<String, PendingDocumentCreation>>>,
    /// Serializes document-count quota admission and first persistent creation.
    pub document_creation_lock: Arc<Mutex<()>>,
    /// Process-local sync counters exported through `/metrics`.
    pub sync_metrics: SyncMetrics,
    /// Readiness state; set to draining before graceful HTTP shutdown.
    pub runtime_health: RuntimeHealth,
}

impl AppState {
    /// Ensure a logical document exists while atomically admitting new content
    /// documents against the per-vault quota. Vault roots and plugin databases
    /// are intentionally outside the content-document quota.
    pub async fn ensure_vault_document(
        &self,
        vault_id: &str,
        document_id: &str,
    ) -> crate::error::AppResult<()> {
        let _guard = self.document_creation_lock.lock().await;
        if self.documents.document_exists(document_id).await? {
            self.documents.ensure_document(document_id).await?;
            return Ok(());
        }
        let is_root = document_id == vault_id;
        let is_plugin_db = crate::plugindb::parse_doc_id(document_id).is_some();
        if !is_root && !is_plugin_db {
            let count = self.documents.document_count_for_vault(vault_id).await?;
            if count >= self.config.crdt_max_documents_per_vault {
                return Err(crate::error::AppError::BadRequest(
                    "vault document limit reached".into(),
                ));
            }
        }
        self.documents.ensure_document(document_id).await?;
        Ok(())
    }

    /// Remember the document, access level, and principal for a freshly minted
    /// connection token. Expired entries are swept periodically.
    pub async fn record_sync_grant(
        &self,
        token: String,
        document_id: String,
        level: Level,
        epoch: u64,
        principal: Principal,
    ) {
        let mut grants = self.sync_grants.lock().await;
        grants.prune_if_due(now_millis());
        grants.grants.insert(
            token,
            SyncGrant {
                document_id,
                level,
                epoch,
                principal,
            },
        );
    }

    /// Resolve an unexpired token only for the document it was minted for.
    pub async fn sync_grant(&self, token: &str, document_id: &str) -> Option<SyncGrant> {
        let mut grants = self.sync_grants.lock().await;
        let now = now_millis();
        grants.prune_if_due(now);
        grants
            .grants
            .get(token)
            .filter(|grant| grant.document_id == document_id && grant.principal.expires_at_ms > now)
            .cloned()
    }

    pub async fn reserve_document_creation(
        &self,
        document_id: String,
        user_id: String,
        path: String,
    ) -> bool {
        const MAX_PENDING_PER_USER: usize = 4_096;
        const CREATION_TTL_MS: i64 = 5 * 60 * 1_000;

        let now = now_millis();
        let mut reservations = self.pending_document_creations.lock().await;
        reservations.retain(|_, reservation| reservation.expires_at_ms > now);
        if let Some(existing) = reservations.get_mut(&document_id) {
            // Reconnects often omit the optional creation path after the first
            // token request. Keep the original binding; reject only an attempt
            // to rebind the same document id to a different non-empty path.
            if existing.user_id != user_id {
                return false;
            }
            if existing.path.is_empty() && !path.is_empty() {
                existing.path = path;
                existing.expires_at_ms = now + CREATION_TTL_MS;
                return true;
            }
            return path.is_empty() || existing.path == path;
        }
        if reservations
            .values()
            .filter(|reservation| reservation.user_id == user_id)
            .count()
            >= MAX_PENDING_PER_USER
        {
            return false;
        }
        reservations.insert(
            document_id,
            PendingDocumentCreation {
                user_id,
                path,
                expires_at_ms: now + CREATION_TTL_MS,
            },
        );
        true
    }
}
