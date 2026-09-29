pub mod attachments;
pub mod audit;
pub mod blobs;
pub mod caps;
pub mod config;
pub mod crdt;

mod crdt_epoch;
pub mod crdt_storage;
pub mod db;
pub mod dmux;
pub mod entities;
pub mod error;
pub mod full_backup;
pub mod git;
pub mod history;
pub mod jobs;
pub mod mcp;
mod migration;
pub mod notes;
pub mod oauth;
pub mod oidc;
pub mod openapi;
pub mod operations;
pub mod permalink;
pub mod plugindb;
pub mod rollback;
pub mod routes;
mod safe_yrs;
pub mod search;
pub mod session;
pub mod shares;
pub mod state;
pub mod storage;
pub mod stream;
pub mod structured;
pub mod web;
pub mod words;
pub mod ydoc;

pub const SERVER_NAME: &str = "Realtime";
pub const SERVER_SLUG: &str = "realtime";
pub const SERVER_BOT_EMAIL: &str = "realtime@localhost";

use std::sync::Arc;

use axum::{
    extract::DefaultBodyLimit,
    http::HeaderValue,
    routing::{any, delete, get, post, put},
    Router,
};
use sea_orm::Database;
use std::time::Duration;
use tower_http::cors::{Any, CorsLayer};
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use crate::config::Config;
use crate::crdt::DocumentStore;
use crate::state::AppState;

/// Build the full application state from config, opening the DB and applying
/// pending schema migrations. Shared by the binary and integration tests.
pub async fn build_state(config: Config) -> anyhow::Result<AppState> {
    let db = Database::connect(&config.database_url).await?;
    db::migrate_schema(&db).await?;
    let server_id = db::ensure_server_id(&db).await?;

    let http = reqwest::Client::builder()
        // Following redirects on the token endpoint opens us to SSRF.
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(30))
        .build()?;

    let config = Arc::new(config);
    let days_ms = 24 * 60 * 60 * 1_000;
    let documents = DocumentStore::new_with_policy(
        &config.crdt_store_dir,
        crdt_epoch::EpochPolicy {
            recovery_window_ms: config.crdt_epoch_recovery_days.saturating_mul(days_ms),
            max_age_ms: config.crdt_epoch_period_days.saturating_mul(days_ms),
            max_update_count: config.crdt_epoch_max_updates,
            max_encoded_state_bytes: config.crdt_epoch_max_state_bytes,
            max_delete_set_bytes: config.crdt_epoch_max_delete_set_bytes,
            ack_timeout_ms: config.crdt_epoch_ack_timeout_ms,
        },
    )
    .await?;
    let sync_metrics = documents.sync_metrics();
    let jobs = jobs::JobQueue::new(&db, &config).await?;
    let plugindb =
        plugindb::PluginDbService::new(config.clone(), db.clone(), documents.clone(), jobs.clone());
    let git = git::GitService::new(
        config.clone(),
        db.clone(),
        documents.clone(),
        plugindb.clone(),
        jobs.clone(),
    );
    let search = search::SearchService::new(jobs.clone());

    let state = AppState {
        db,
        config,
        server_id,
        documents,
        jobs: jobs.clone(),
        http,
        oidc: Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
        oauth_flows: Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
        git,
        plugindb,
        search,
        sync_grants: Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
        pending_document_creations: Arc::new(tokio::sync::Mutex::new(
            std::collections::HashMap::new(),
        )),
        document_creation_lock: Arc::new(tokio::sync::Mutex::new(())),
        sync_metrics,
        runtime_health: operations::RuntimeHealth::default(),
    };
    jobs.start(state.clone()).await?;
    audit::spawn_retention_task(state.clone());
    Ok(state)
}

/// Assemble the axum router for the given state.
pub fn app(state: AppState) -> Router {
    let cors = if state.config.cors_allowed_origins.is_empty() {
        CorsLayer::new()
            .allow_origin(HeaderValue::from_static("obsidian://app"))
            .allow_methods(Any)
            .allow_headers(Any)
    } else {
        let origins: Vec<HeaderValue> = state
            .config
            .cors_allowed_origins
            .iter()
            .filter_map(|origin| origin.parse().ok())
            .collect();
        CorsLayer::new()
            .allow_origin(origins)
            .allow_methods(Any)
            .allow_headers(Any)
    };

    Router::new()
        .route("/health/live", get(operations::live))
        .route("/health/ready", get(operations::ready))
        .route("/metrics", get(operations::metrics))
        .merge(SwaggerUi::new("/docs").url("/openapi.json", openapi::ApiDoc::openapi()))
        .merge(mcp::router(state.clone()))
        .route("/auth/login", get(oidc::login))
        .route("/auth/callback", get(oidc::callback))
        .route(
            "/.well-known/oauth-protected-resource",
            get(oauth::protected_resource),
        )
        .route(
            "/.well-known/oauth-protected-resource/mcp/i/{app_id}",
            get(oauth::protected_resource_app),
        )
        .route(
            "/.well-known/oauth-authorization-server",
            get(oauth::authorization_server),
        )
        .route("/oauth/register", post(oauth::register_client))
        .route("/oauth/authorize", get(oauth::authorize))
        .route("/oauth/token", post(oauth::token))
        .route(
            "/upload",
            post(attachments::public_upload)
                .layer(DefaultBodyLimit::max(blobs::MAX_BLOB_BYTES as usize)),
        )
        .route(
            "/api/vaults/{id}/shares",
            get(shares::get_share)
                .post(shares::create_share)
                .delete(shares::delete_share),
        )
        .route(
            "/api/vaults/{id}/attachment-shares",
            get(shares::get_attachment_share)
                .post(shares::create_attachment_share)
                .delete(shares::delete_attachment_share),
        )
        .route("/a/{share_id}", get(shares::view_shared_attachment))
        // Public (unauthenticated) read-only share viewer API.
        .route("/api/view/{share_id}", get(shares::view_share))
        .route("/api/view/{share_id}/events", get(shares::view_events))
        .route("/api/view/{share_id}/resolve", get(shares::view_resolve))
        .route(
            "/api/view/{share_id}/attachments/{*path}",
            get(shares::view_attachment),
        )
        // The read-only web viewer SPA (packages/web build output).
        .route("/view/{share_id}", get(web::serve_index))
        .route("/view/assets/{*path}", get(web::serve_asset))
        .route("/n/{guid}", get(permalink::note_by_guid))
        .route("/p", get(permalink::note_by_path))
        .route("/api/server-info", get(routes::server_info))
        .route("/api/me", get(routes::me).patch(routes::update_me))
        .route("/api/logout", post(routes::logout))
        .route(
            "/api/vaults",
            get(routes::list_vaults).post(routes::create_vault),
        )
        .route("/api/vaults/{id}/invites", post(routes::create_invite))
        .route(
            "/api/vaults/{id}/cursors",
            get(routes::list_cursors).post(routes::create_cursor),
        )
        .route(
            "/api/vaults/{id}/cursors/plugin",
            post(routes::acquire_plugin_cursor),
        )
        .route(
            "/api/vaults/{id}/cursors/{cursor_id}",
            post(routes::rename_cursor).delete(routes::delete_cursor),
        )
        .route(
            "/api/vaults/{id}/cursors/{cursor_id}/token",
            post(routes::regenerate_cursor_token),
        )
        .route(
            "/api/vaults/{id}/cursors/{cursor_id}/audit",
            get(routes::list_cursor_audit),
        )
        .route(
            "/api/vaults/{id}/cursors/{cursor_id}/audit/{entry_id}/undo",
            post(routes::undo_cursor_audit),
        )
        .route(
            "/api/vaults/{id}/backup",
            get(routes::get_backup)
                .put(routes::put_backup)
                .delete(routes::delete_backup),
        )
        .route("/api/vaults/{id}/backup/test", post(routes::test_backup))
        // Git history browsing + vault rollback.
        .route(
            "/api/vaults/{id}/history/commits",
            get(history::list_commits),
        )
        .route(
            "/api/vaults/{id}/history/commits/{hash}",
            get(history::get_commit),
        )
        .route(
            "/api/vaults/{id}/history/commits/{hash}/tree",
            get(history::get_tree),
        )
        .route(
            "/api/vaults/{id}/history/commits/{hash}/file",
            get(history::get_file),
        )
        .route(
            "/api/vaults/{id}/history/commits/{hash}/blob",
            get(history::get_blob),
        )
        .route(
            "/api/vaults/{id}/history/commits/{hash}/rollback/preview",
            post(rollback::rollback_preview),
        )
        .route(
            "/api/vaults/{id}/history/commits/{hash}/rollback",
            post(rollback::rollback),
        )
        .route("/api/vaults/{id}/storage", get(storage::get_storage))
        .route("/api/vaults/{id}/storage/gc-blobs", post(storage::gc_blobs))
        .route("/api/vaults/{id}/jobs", get(routes::list_jobs))
        .route("/api/vaults/{id}/jobs/retry", post(routes::retry_job))
        .route("/api/vaults/{id}/jobs/cancel", post(routes::cancel_job))
        .route("/api/vaults/{id}/members", get(routes::list_members))
        .route(
            "/api/vaults/{id}/members/{user_id}/promote",
            post(routes::promote_member),
        )
        .route(
            "/api/vaults/{id}/members/{user_id}",
            delete(routes::remove_member),
        )
        .route("/api/vaults/{id}/stream", get(stream::stream_ws))
        .route("/api/vaults/{id}/files", post(routes::upsert_file))
        .route(
            "/api/vaults/{id}/notes",
            get(notes::list_notes).post(notes::create_note),
        )
        .route("/api/vaults/{id}/search", get(search::search_notes))
        .route(
            "/api/vaults/{id}/canvases",
            get(structured::list_canvases).post(structured::create_canvas),
        )
        .route(
            "/api/vaults/{id}/bases",
            get(structured::list_bases).post(structured::create_base),
        )
        .route("/api/vaults/{id}/tags", get(search::list_tags))
        .route("/api/vaults/{id}/reindex", post(search::reindex))
        .route(
            "/api/clearFtsAndReindex",
            post(search::clear_fts_and_reindex),
        )
        .route(
            "/api/vaults/{id}/backlinks/{*path}",
            get(search::list_backlinks),
        )
        .route(
            "/api/vaults/{id}/notes/{*path}",
            get(notes::read_note)
                .put(notes::replace_note)
                .patch(notes::patch_note)
                .delete(notes::delete_note),
        )
        .route(
            "/api/vaults/{id}/note-moves/{*path}",
            post(notes::move_note),
        )
        .route(
            "/api/vaults/{id}/note-permalinks/{*path}",
            post(notes::note_permalink),
        )
        .route(
            "/api/vaults/{id}/note-frontmatter/{*path}",
            get(notes::parse_frontmatter).patch(notes::patch_frontmatter),
        )
        .route(
            "/api/vaults/{id}/canvas/{*path}",
            get(structured::read_canvas)
                .put(structured::replace_canvas)
                .delete(structured::delete_canvas),
        )
        .route(
            "/api/vaults/{id}/canvas-nodes/{*path}",
            post(structured::add_canvas_node)
                .patch(structured::update_canvas_node)
                .delete(structured::delete_canvas_node),
        )
        .route(
            "/api/vaults/{id}/canvas-edges/{*path}",
            post(structured::add_canvas_edge)
                .patch(structured::update_canvas_edge)
                .delete(structured::delete_canvas_edge),
        )
        .route(
            "/api/vaults/{id}/canvas-operations/{*path}",
            post(structured::apply_canvas_operations),
        )
        .route(
            "/api/vaults/{id}/canvas-moves/{*path}",
            post(structured::move_canvas),
        )
        .route(
            "/api/vaults/{id}/base/{*path}",
            get(structured::read_base)
                .put(structured::replace_base)
                .delete(structured::delete_base),
        )
        .route(
            "/api/vaults/{id}/base-views/{*path}",
            get(structured::list_base_views)
                .post(structured::add_base_view)
                .patch(structured::update_base_view)
                .delete(structured::delete_base_view),
        )
        .route(
            "/api/vaults/{id}/base-filters/{*path}",
            put(structured::set_base_filters),
        )
        .route(
            "/api/vaults/{id}/base-view-filters/{*path}",
            put(structured::set_base_view_filters),
        )
        .route(
            "/api/vaults/{id}/base-formulas/{*path}",
            put(structured::set_base_formula).delete(structured::delete_base_formula),
        )
        .route(
            "/api/vaults/{id}/base-properties/{*path}",
            put(structured::set_base_property).delete(structured::delete_base_property),
        )
        .route(
            "/api/vaults/{id}/base-moves/{*path}",
            post(structured::move_base),
        )
        .route(
            "/api/vaults/{id}/periodic/{period}",
            post(notes::periodic_note_get_or_create),
        )
        .route(
            "/api/vaults/{id}/periodic/{period}/append",
            post(notes::periodic_note_append),
        )
        .route(
            "/api/vaults/{id}/attachments",
            get(attachments::list_attachments),
        )
        .route(
            "/api/vaults/{id}/attachments/from-url",
            post(attachments::upload_attachment_url),
        )
        .route(
            "/api/vaults/{id}/attachments/upload-link",
            post(attachments::create_upload_link),
        )
        .route(
            "/api/vaults/{id}/attachments/{*path}",
            get(attachments::read_attachment)
                .head(attachments::head_attachment)
                .put(attachments::upload_attachment)
                .delete(attachments::delete_attachment)
                .layer(DefaultBodyLimit::max(blobs::MAX_BLOB_BYTES as usize)),
        )
        .route(
            "/api/vaults/{id}/attachment-moves/{*path}",
            post(attachments::move_attachment),
        )
        // Content-addressed binary blob store. PUT opts out of the default body
        // cap so large attachments can stream through (it verifies the hash).
        .route(
            "/api/vaults/{id}/blobs/{hash}",
            get(blobs::get_blob)
                .head(blobs::head_blob)
                .put(blobs::put_blob)
                .delete(storage::delete_blob)
                .layer(DefaultBodyLimit::max(blobs::MAX_BLOB_BYTES as usize)),
        )
        .route("/api/invites/redeem", post(routes::redeem_invite))
        .route("/api/doc-token", post(routes::doc_token))
        // Synced plugin databases (cr-sqlite).
        .route(
            "/api/vaults/{id}/plugin-dbs/{plugin}/{name}/changes",
            get(plugindb::routes::get_changes),
        )
        .route(
            "/api/vaults/{id}/plugin-dbs/{plugin}/{name}/touch",
            post(plugindb::routes::touch),
        )
        .route(
            "/api/vaults/{id}/plugin-dbs/{plugin}/{name}",
            delete(plugindb::routes::delete_plugin_db),
        )
        .route(
            "/api/vaults/{id}/plugin-dbs",
            get(plugindb::routes::list_plugin_dbs),
        )
        .route(
            "/api/vaults/{id}/plugin-dbs/{plugin}/{name}/query",
            post(plugindb::routes::query_plugin_db),
        )
        .route(
            "/api/vaults/{id}/plugin-dbs/{plugin}/{name}/execute",
            post(plugindb::routes::execute_plugin_db),
        )
        // Native Yjs-compatible document transport and document-level HTTP API.
        .route("/d/{doc_id}/ws/{repeated_id}", get(crdt::websocket))
        .route("/d/{doc_id}/as-update", get(crdt::get_update))
        .route(
            "/d/{doc_id}/update",
            post(crdt::post_update).layer(DefaultBodyLimit::max(crdt::MAX_UPDATE_BYTES)),
        )
        // Bounded multiplexing: each client shard carries many native document
        // sessions over one WebSocket (see src/sync/mux.ts).
        .route("/dmux", any(dmux::dmux))
        .layer(cors)
        .with_state(state)
}
