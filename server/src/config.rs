use std::env;
use std::path::{Path, PathBuf};

use rand::RngCore;

use crate::{SERVER_BOT_EMAIL, SERVER_NAME};

/// Reject the documented default and other low-entropy HMAC keys in production.
const INSECURE_UPLOAD_TOKEN: &str = "dev-upload-token-change-me";
const MIN_PRODUCTION_UPLOAD_TOKEN_LEN: usize = 32;

/// Runtime configuration, read from the environment. See README for the full list.
#[derive(Clone, Debug)]
pub struct Config {
    pub database_url: String,
    /// Master switch for the persistent Apalis workers. Enqueueing remains
    /// enabled while workers are paused, so no desired work is discarded.
    pub background_jobs_enabled: bool,
    pub background_job_concurrency: usize,
    pub background_job_max_attempts: u32,
    pub background_job_retry_min_ms: u64,
    pub background_job_retry_max_ms: u64,
    pub background_job_shutdown_timeout_ms: u64,
    /// Maximum time HTTP/WebSocket connections may delay process shutdown.
    pub server_shutdown_timeout_ms: u64,
    pub bind_addr: String,
    /// Public base URL of *this* server (used to build the OIDC redirect default).
    pub public_base_url: String,
    /// Filesystem directory for native Yjs document generations.
    pub crdt_store_dir: String,
    /// Maximum age of an active CRDT epoch before logical-state replacement.
    pub crdt_epoch_period_days: u64,
    /// How long retired CRDT epochs remain available for offline recovery.
    pub crdt_epoch_recovery_days: u64,
    pub crdt_epoch_max_updates: u64,
    pub crdt_epoch_max_state_bytes: u64,
    pub crdt_epoch_max_delete_set_bytes: u64,
    /// Milliseconds a proposed document epoch waits for connected peers to
    /// acknowledge before activating anyway.
    pub crdt_epoch_ack_timeout_ms: u64,
    /// Maximum registered file documents in one vault. Index and plugin-db
    /// documents are separate bounded surfaces.
    pub crdt_max_documents_per_vault: u64,
    /// Filesystem directory for the content-addressed binary blob store.
    pub blob_dir: String,
    /// "oidc" for a real IdP, "mock" for the in-process test issuer.
    pub oidc_mode: OidcMode,
    pub oidc_issuer: Option<String>,
    pub oidc_client_id: Option<String>,
    pub oidc_client_secret: Option<String>,
    pub oidc_redirect_url: Option<String>,
    pub allowed_login_redirects: Vec<String>,
    pub cors_allowed_origins: Vec<String>,
    /// Directory holding the per-vault git audit/backup repositories.
    pub git_data_dir: String,
    /// Master switch for the git audit log (disable to make it a no-op).
    pub git_enabled: bool,
    /// Debounce window: edits are coalesced into one commit after this idle gap.
    pub git_debounce_ms: u64,
    /// Identity used as the committer (and as the author when none is known).
    pub git_bot_name: String,
    pub git_bot_email: String,
    /// Attachments up to this size are committed verbatim into git backups;
    /// larger ones are committed as a text shim pointing at the blob store.
    pub git_inline_attachment_max_bytes: u64,
    /// Domain used for synthetic cursor authors in git audit commits.
    pub cursor_email_domain: String,
    pub daily_note_path_template: String,
    pub weekly_note_path_template: Option<String>,
    pub monthly_note_path_template: Option<String>,
    pub quarterly_note_path_template: Option<String>,
    pub yearly_note_path_template: Option<String>,
    pub attachment_fetch_host_allowlist: Vec<String>,
    pub attachment_allowed_extensions: Vec<String>,
    pub attachment_max_bytes: u64,
    pub attachments_path_mode: String,
    pub attachments_subfolder: Option<String>,
    pub upload_token: String,
    /// Filesystem path to the cr-sqlite loadable extension (matching the client
    /// WASM's sync-format major). When unset/missing, plugin-database replication
    /// degrades gracefully: changes still flow client-to-client over the Y log,
    /// but the server keeps no replica and git skips the per-DB SQL dumps.
    pub crsqlite_ext_path: Option<String>,
    /// Vite build output of the read-only web viewer (packages/web/dist),
    /// served at /view/{share_id}.
    pub web_dist_path: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OidcMode {
    Oidc,
    Mock,
}

fn opt(name: &str) -> Option<String> {
    env::var(name).ok().filter(|s| !s.is_empty())
}

impl Config {
    pub fn from_env() -> Self {
        let oidc_mode = match opt("OIDC_MODE").as_deref() {
            Some("mock") => OidcMode::Mock,
            _ => OidcMode::Oidc,
        };
        if oidc_mode == OidcMode::Mock && opt("ALLOW_MOCK_OIDC").as_deref() != Some("1") {
            panic!("OIDC_MODE=mock requires ALLOW_MOCK_OIDC=1 and must not be used in production");
        }
        let git_data_dir = opt("GIT_DATA_DIR").unwrap_or_else(|| "./git".to_string());
        let upload_token_path = PathBuf::from(&git_data_dir).join(".upload-token");
        Config {
            database_url: opt("DATABASE_URL")
                .unwrap_or_else(|| "sqlite://realtime.db?mode=rwc".to_string()),
            background_jobs_enabled: opt("BACKGROUND_JOBS_ENABLED").as_deref() != Some("0"),
            background_job_concurrency: opt("BACKGROUND_JOB_CONCURRENCY")
                .and_then(|s| s.parse().ok())
                .unwrap_or(4),
            background_job_max_attempts: opt("BACKGROUND_JOB_MAX_ATTEMPTS")
                .and_then(|s| s.parse().ok())
                .unwrap_or(25),
            background_job_retry_min_ms: opt("BACKGROUND_JOB_RETRY_MIN_MS")
                .and_then(|s| s.parse().ok())
                .unwrap_or(250),
            background_job_retry_max_ms: opt("BACKGROUND_JOB_RETRY_MAX_MS")
                .and_then(|s| s.parse().ok())
                .unwrap_or(30_000),
            background_job_shutdown_timeout_ms: opt("BACKGROUND_JOB_SHUTDOWN_TIMEOUT_MS")
                .and_then(|s| s.parse().ok())
                .unwrap_or(30_000),
            server_shutdown_timeout_ms: opt("SERVER_SHUTDOWN_TIMEOUT_MS")
                .and_then(|s| s.parse().ok())
                .unwrap_or(30_000),
            bind_addr: opt("BIND_ADDR").unwrap_or_else(|| "127.0.0.1:8081".to_string()),
            public_base_url: opt("PUBLIC_BASE_URL")
                .unwrap_or_else(|| "http://127.0.0.1:8081".to_string()),
            crdt_store_dir: opt("CRDT_STORE").unwrap_or_else(|| "./crdt".to_string()),
            crdt_epoch_period_days: opt("CRDT_EPOCH_PERIOD_DAYS")
                .and_then(|s| s.parse().ok())
                .unwrap_or(365),
            crdt_epoch_recovery_days: opt("CRDT_EPOCH_RECOVERY_DAYS")
                .and_then(|s| s.parse().ok())
                .unwrap_or(30),
            crdt_epoch_max_updates: opt("CRDT_EPOCH_MAX_UPDATES")
                .and_then(|s| s.parse().ok())
                .unwrap_or(100_000),
            crdt_epoch_max_state_bytes: opt("CRDT_EPOCH_MAX_STATE_BYTES")
                .and_then(|s| s.parse().ok())
                .unwrap_or(32 * 1024 * 1024),
            crdt_epoch_max_delete_set_bytes: opt("CRDT_EPOCH_MAX_DELETE_SET_BYTES")
                .and_then(|s| s.parse().ok())
                .unwrap_or(8 * 1024 * 1024),
            crdt_epoch_ack_timeout_ms: opt("CRDT_EPOCH_ACK_TIMEOUT_MS")
                .and_then(|s| s.parse().ok())
                .unwrap_or(30_000),
            crdt_max_documents_per_vault: opt("CRDT_MAX_DOCUMENTS_PER_VAULT")
                .and_then(|s| s.parse().ok())
                .unwrap_or(100_000),
            blob_dir: opt("BLOB_DIR").unwrap_or_else(|| "./blobs".to_string()),
            oidc_mode,
            oidc_issuer: opt("OIDC_ISSUER"),
            oidc_client_id: opt("OIDC_CLIENT_ID"),
            oidc_client_secret: opt("OIDC_CLIENT_SECRET"),
            oidc_redirect_url: opt("OIDC_REDIRECT_URL"),
            allowed_login_redirects: list("ALLOWED_LOGIN_REDIRECTS"),
            cors_allowed_origins: list("CORS_ALLOWED_ORIGINS"),
            git_data_dir,
            git_enabled: opt("GIT_AUDIT_ENABLED").as_deref() != Some("0"),
            git_debounce_ms: opt("GIT_DEBOUNCE_MS")
                .and_then(|s| s.parse().ok())
                .unwrap_or(5000),
            git_bot_name: opt("GIT_BOT_NAME").unwrap_or_else(|| SERVER_NAME.to_string()),
            git_bot_email: opt("GIT_BOT_EMAIL").unwrap_or_else(|| SERVER_BOT_EMAIL.to_string()),
            git_inline_attachment_max_bytes: opt("GIT_INLINE_ATTACHMENT_MAX_BYTES")
                .and_then(|s| s.parse().ok())
                .unwrap_or(5 * 1024 * 1024),
            cursor_email_domain: opt("CURSOR_EMAIL_DOMAIN").unwrap_or_else(|| {
                opt("GIT_BOT_EMAIL")
                    .and_then(|email| email.split_once('@').map(|(_, domain)| domain.to_string()))
                    .unwrap_or_else(|| "localhost".to_string())
            }),
            daily_note_path_template: opt("DAILY_NOTE_PATH_TEMPLATE")
                .unwrap_or_else(|| "Daily Notes/{{YYYY-MM-DD}}.md".to_string()),
            weekly_note_path_template: opt("WEEKLY_NOTE_PATH_TEMPLATE"),
            monthly_note_path_template: opt("MONTHLY_NOTE_PATH_TEMPLATE"),
            quarterly_note_path_template: opt("QUARTERLY_NOTE_PATH_TEMPLATE"),
            yearly_note_path_template: opt("YEARLY_NOTE_PATH_TEMPLATE"),
            attachment_fetch_host_allowlist: list("ATTACHMENT_FETCH_HOST_ALLOWLIST"),
            attachment_allowed_extensions: {
                let configured = list("ATTACHMENT_ALLOWED_EXTENSIONS");
                let values = if configured.is_empty() {
                    vec![
                        "png".into(),
                        "jpg".into(),
                        "jpeg".into(),
                        "gif".into(),
                        "webp".into(),
                        "svg".into(),
                        "pdf".into(),
                        "txt".into(),
                    ]
                } else {
                    configured
                };
                values
                    .into_iter()
                    .map(|ext| ext.trim_start_matches('.').to_ascii_lowercase())
                    .collect::<Vec<_>>()
                    .into_iter()
                    .filter(|ext| !ext.is_empty())
                    .collect()
            },
            attachment_max_bytes: opt("ATTACHMENT_MAX_BYTES")
                .and_then(|s| s.parse().ok())
                .unwrap_or(crate::blobs::MAX_BLOB_BYTES),
            attachments_path_mode: opt("ATTACHMENTS_PATH_MODE")
                .unwrap_or_else(|| "relative".to_string()),
            attachments_subfolder: opt("ATTACHMENTS_SUBFOLDER"),
            upload_token: resolve_upload_token(opt("UPLOAD_TOKEN"), oidc_mode, &upload_token_path),
            crsqlite_ext_path: opt("CRSQLITE_EXT_PATH"),
            web_dist_path: opt("WEB_DIST_PATH")
                .unwrap_or_else(|| "../packages/web/dist".to_string()),
        }
    }

    /// Where the IdP should redirect back to after login.
    pub fn redirect_url(&self) -> String {
        self.oidc_redirect_url.clone().unwrap_or_else(|| {
            format!(
                "{}/auth/callback",
                self.public_base_url.trim_end_matches('/')
            )
        })
    }
}

#[cfg(test)]
impl Config {
    /// A minimal, self-contained Config for unit tests.
    pub(crate) fn test_default() -> Self {
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
                .join(format!("realtime-test-crdt-{}", nanoid::nanoid!()))
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
            oidc_mode: OidcMode::Mock,
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
            attachment_allowed_extensions: vec!["png".into(), "txt".into()],
            attachment_max_bytes: crate::blobs::MAX_BLOB_BYTES,
            attachments_path_mode: "relative".into(),
            attachments_subfolder: None,
            upload_token: "test-upload-token".into(),
            crsqlite_ext_path: None,
            web_dist_path: "../packages/web/dist".into(),
        }
    }
}

fn list(name: &str) -> Vec<String> {
    opt(name)
        .map(|s| {
            s.split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

fn resolve_upload_token(
    configured: Option<String>,
    oidc_mode: OidcMode,
    persist_path: &Path,
) -> String {
    if let Some(token) = configured {
        enforce_upload_token_entropy(&token, oidc_mode);
        return token;
    }
    if let Ok(existing) = std::fs::read_to_string(persist_path) {
        let existing = existing.trim().to_string();
        if !existing.is_empty() && existing != INSECURE_UPLOAD_TOKEN {
            enforce_upload_token_entropy(&existing, oidc_mode);
            return existing;
        }
    }
    let mut bytes = [0u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut bytes);
    let generated: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    if let Some(parent) = persist_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    if let Err(error) = std::fs::write(persist_path, &generated) {
        if oidc_mode == OidcMode::Oidc {
            panic!(
                "failed to persist generated UPLOAD_TOKEN to {}: {error}",
                persist_path.display()
            );
        }
    }
    generated
}

fn enforce_upload_token_entropy(token: &str, oidc_mode: OidcMode) {
    if oidc_mode != OidcMode::Oidc {
        return;
    }
    if token == INSECURE_UPLOAD_TOKEN || token.len() < MIN_PRODUCTION_UPLOAD_TOKEN_LEN {
        panic!(
            "UPLOAD_TOKEN must be at least {MIN_PRODUCTION_UPLOAD_TOKEN_LEN} characters \
             and must not use the documented default in production"
        );
    }
}

#[cfg(test)]
mod upload_token_tests {
    use super::*;

    #[test]
    fn production_rejects_insecure_default() {
        let err = std::panic::catch_unwind(|| {
            enforce_upload_token_entropy(INSECURE_UPLOAD_TOKEN, OidcMode::Oidc);
        });
        assert!(err.is_err());
    }

    #[test]
    fn production_rejects_short_secret() {
        let err = std::panic::catch_unwind(|| {
            enforce_upload_token_entropy("short-secret", OidcMode::Oidc);
        });
        assert!(err.is_err());
    }

    #[test]
    fn mock_mode_allows_short_secret() {
        enforce_upload_token_entropy("test-upload-token", OidcMode::Mock);
    }

    #[test]
    fn generates_and_reuses_persistent_secret() {
        let dir = std::env::temp_dir().join(format!("realtime-upload-token-{}", nanoid::nanoid!()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(".upload-token");
        let first = resolve_upload_token(None, OidcMode::Mock, &path);
        let second = resolve_upload_token(None, OidcMode::Mock, &path);
        assert_eq!(first, second);
        assert!(first.len() >= MIN_PRODUCTION_UPLOAD_TOKEN_LEN);
        assert_ne!(first, INSECURE_UPLOAD_TOKEN);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
