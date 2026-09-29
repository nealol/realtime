//! Integration tests over the axum app with the mock OIDC issuer, temp SQLite
//! databases, and isolated native CRDT stores.

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use axum::Router;
use http_body_util::BodyExt;
use realtime_server::config::{Config, OidcMode};
use realtime_server::{app, build_state};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use tower::ServiceExt;

fn text_update(name: &str, value: &str) -> Vec<u8> {
    use yrs::{Text, Transact};
    let doc = yrs::Doc::new();
    let text = doc.get_or_insert_text(name);
    {
        let mut txn = doc.transact_mut();
        text.insert(&mut txn, 0, value);
    }
    use yrs::ReadTxn;
    let u = doc
        .transact()
        .encode_state_as_update_v1(&yrs::StateVector::default());
    u
}

fn files_update(entries: &[(&str, &str)]) -> Vec<u8> {
    use yrs::{Map, Transact};
    let doc = yrs::Doc::new();
    let map = doc.get_or_insert_map("files");
    {
        let mut txn = doc.transact_mut();
        for (path, guid) in entries {
            map.insert(&mut txn, path.to_string(), guid.to_string());
        }
    }
    use yrs::ReadTxn;
    let u = doc
        .transact()
        .encode_state_as_update_v1(&yrs::StateVector::default());
    u
}

async fn fake_attachment_source() -> String {
    use axum::routing::get;

    async fn image() -> ([(&'static str, &'static str); 1], Vec<u8>) {
        (
            [("content-type", "image/png")],
            b"\x89PNG\r\n\x1a\nremote image".to_vec(),
        )
    }

    let router = Router::new().route("/image.png", get(image));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    format!("http://{addr}")
}

/// Build a git-enabled `AppState` plus its repo dir and a synthetic vault id.
async fn git_state() -> (realtime_server::state::AppState, std::path::PathBuf, String) {
    git_state_ext(None).await
}

/// Like [`git_state`], optionally wiring the cr-sqlite loadable extension.
async fn git_state_ext(
    crsqlite_ext_path: Option<String>,
) -> (realtime_server::state::AppState, std::path::PathBuf, String) {
    git_state_ext_with_debounce(crsqlite_ext_path, 50).await
}

async fn git_state_ext_with_debounce(
    crsqlite_ext_path: Option<String>,
    git_debounce_ms: u64,
) -> (realtime_server::state::AppState, std::path::PathBuf, String) {
    let mut db_path = std::env::temp_dir();
    db_path.push(format!("realtime-test-{}.db", uuid::Uuid::new_v4()));
    let mut git_dir = std::env::temp_dir();
    git_dir.push(format!("realtime-git-{}", uuid::Uuid::new_v4()));

    let config = Config {
        database_url: format!("sqlite://{}?mode=rwc", db_path.display()),
        background_jobs_enabled: true,
        background_job_concurrency: 4,
        background_job_max_attempts: 25,
        background_job_retry_min_ms: 250,
        background_job_retry_max_ms: 30_000,
        background_job_shutdown_timeout_ms: 30_000,
        server_shutdown_timeout_ms: 30_000,
        bind_addr: "127.0.0.1:0".into(),
        public_base_url: "http://auth.test".into(),
        crdt_store_dir: std::env::temp_dir()
            .join(format!("realtime-crdt-{}", uuid::Uuid::new_v4()))
            .display()
            .to_string(),
        crdt_epoch_period_days: 365,
        crdt_epoch_recovery_days: 30,
        crdt_epoch_max_updates: 100_000,
        crdt_epoch_max_state_bytes: 32 * 1024 * 1024,
        crdt_epoch_max_delete_set_bytes: 8 * 1024 * 1024,
        crdt_max_documents_per_vault: 100_000,
        blob_dir: std::env::temp_dir().display().to_string(),
        oidc_mode: OidcMode::Mock,
        oidc_issuer: None,
        oidc_client_id: None,
        oidc_client_secret: None,
        oidc_redirect_url: None,
        allowed_login_redirects: vec![],
        cors_allowed_origins: vec![],
        git_data_dir: git_dir.display().to_string(),
        git_enabled: true,
        git_debounce_ms,
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
        attachment_max_bytes: realtime_server::blobs::MAX_BLOB_BYTES,
        attachments_path_mode: "relative".into(),
        attachments_subfolder: None,
        upload_token: "test-upload-token".into(),
        crsqlite_ext_path,
        web_dist_path: "../packages/web/dist".into(),
    };
    let state = build_state(config).await.unwrap();
    let vault_id = uuid::Uuid::new_v4().to_string();
    (state, git_dir, vault_id)
}

async fn seed_git_note(state: &realtime_server::state::AppState, vault_id: &str) {
    state
        .documents
        .apply_update(vault_id, &files_update(&[("note.md", "g1")]))
        .await
        .unwrap();
    state
        .documents
        .apply_update(
            &format!("{vault_id}__g1"),
            &text_update("contents", "# Note g1\n"),
        )
        .await
        .unwrap();
}

fn files_and_binaries_update(binaries: &[(String, String, i64)]) -> Vec<u8> {
    use yrs::{Any, Map, ReadTxn, Transact};
    let doc = yrs::Doc::new();
    let files = doc.get_or_insert_map("files");
    let binary_map = doc.get_or_insert_map("binaries");
    {
        let mut txn = doc.transact_mut();
        files.insert(&mut txn, "note.md", "g1");
        for (path, hash, size) in binaries {
            let metadata = HashMap::from([
                ("hash".to_string(), Any::String(hash.as_str().into())),
                ("size".to_string(), Any::BigInt(*size)),
            ]);
            binary_map.insert(&mut txn, path.clone(), Any::from(metadata));
        }
    }
    let update = doc
        .transact()
        .encode_state_as_update_v1(&yrs::StateVector::default());
    update
}

fn git_out(repo: &std::path::Path, args: &[&str]) -> (bool, String) {
    let out = std::process::Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(args)
        .output()
        .expect("run git");
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stdout).trim().to_string(),
    )
}

/// Poll the repo until a commit appears (or time out), returning the last log line.
async fn wait_for_commit(repo: &std::path::Path) -> String {
    for _ in 0..100 {
        let (ok, line) = git_out(repo, &["log", "-1", "--format=%an|%ae|%s"]);
        if ok && !line.is_empty() {
            return line;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("no commit appeared in repo {}", repo.display());
}

fn principal(id: &str, name: &str, email: &str) -> realtime_server::state::Principal {
    realtime_server::state::Principal {
        user_id: id.into(),
        display_name: name.into(),
        email: email.into(),
        git_email: None,
        actor: realtime_server::state::PrincipalActor::User,
        expires_at_ms: i64::MAX,
    }
}

// ---------- harness ----------

async fn test_app() -> Router {
    test_app_with_attachment_max(realtime_server::blobs::MAX_BLOB_BYTES).await
}

async fn test_app_with_state() -> (Router, realtime_server::state::AppState) {
    test_app_with_public_url("http://auth.test").await
}

async fn test_app_with_public_url(
    public_base_url: &str,
) -> (Router, realtime_server::state::AppState) {
    let config = test_config(public_base_url, realtime_server::blobs::MAX_BLOB_BYTES);
    let state = build_state(config).await.unwrap();
    (app(state.clone()), state)
}

async fn test_app_with_attachment_max(attachment_max_bytes: u64) -> Router {
    app_from_config(&test_config("http://auth.test", attachment_max_bytes)).await
}

fn test_config(public_base_url: &str, attachment_max_bytes: u64) -> Config {
    let mut path = std::env::temp_dir();
    path.push(format!("realtime-test-{}.db", uuid::Uuid::new_v4()));
    let database_url = format!("sqlite://{}?mode=rwc", path.display());

    let mut blob_dir = std::env::temp_dir();
    blob_dir.push(format!("realtime-blobs-{}", uuid::Uuid::new_v4()));

    let mut git_dir = std::env::temp_dir();
    git_dir.push(format!("realtime-git-{}", uuid::Uuid::new_v4()));

    Config {
        database_url,
        background_jobs_enabled: true,
        background_job_concurrency: 4,
        background_job_max_attempts: 25,
        background_job_retry_min_ms: 250,
        background_job_retry_max_ms: 30_000,
        background_job_shutdown_timeout_ms: 30_000,
        server_shutdown_timeout_ms: 30_000,
        bind_addr: "127.0.0.1:0".into(),
        public_base_url: public_base_url.to_string(),
        crdt_store_dir: std::env::temp_dir()
            .join(format!("realtime-crdt-{}", uuid::Uuid::new_v4()))
            .display()
            .to_string(),
        crdt_epoch_period_days: 365,
        crdt_epoch_recovery_days: 30,
        crdt_epoch_max_updates: 100_000,
        crdt_epoch_max_state_bytes: 32 * 1024 * 1024,
        crdt_epoch_max_delete_set_bytes: 8 * 1024 * 1024,
        crdt_max_documents_per_vault: 100_000,
        blob_dir: blob_dir.display().to_string(),
        oidc_mode: OidcMode::Mock,
        oidc_issuer: None,
        oidc_client_id: None,
        oidc_client_secret: None,
        oidc_redirect_url: None,
        allowed_login_redirects: vec!["http://app".into()],
        cors_allowed_origins: vec!["http://app".into()],
        git_data_dir: git_dir.display().to_string(),
        // Off by default; the git-specific test builds its own app with it enabled.
        git_enabled: false,
        git_debounce_ms: 50,
        git_bot_name: "Realtime".into(),
        git_bot_email: "realtime@localhost".into(),
        git_inline_attachment_max_bytes: 5 * 1024 * 1024,
        cursor_email_domain: "localhost".into(),
        daily_note_path_template: "Daily Notes/{{YYYY-MM-DD}}.md".into(),
        weekly_note_path_template: None,
        monthly_note_path_template: None,
        quarterly_note_path_template: None,
        yearly_note_path_template: None,
        attachment_fetch_host_allowlist: vec!["127.0.0.1".into()],
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
        attachment_max_bytes,
        attachments_path_mode: "relative".into(),
        attachments_subfolder: None,
        upload_token: "test-upload-token".into(),
        crsqlite_ext_path: None,
        web_dist_path: "../packages/web/dist".into(),
    }
}

async fn app_from_config(config: &Config) -> Router {
    app(build_state(config.clone()).await.unwrap())
}

#[tokio::test]
async fn operational_health_and_metrics_track_readiness_and_crdt_writes() {
    let config = test_config("http://auth.test", realtime_server::blobs::MAX_BLOB_BYTES);
    let state = build_state(config).await.unwrap();
    state
        .documents
        .apply_update("health__document", &text_update("contents", "observed"))
        .await
        .unwrap();
    let router = app(state.clone());

    let (live_status, live) = send(&router, "GET", "/health/live", None, None).await;
    assert_eq!(live_status, StatusCode::OK);
    assert_eq!(live["status"], "up");

    let (ready_status, ready) = send(&router, "GET", "/health/ready", None, None).await;
    assert_eq!(ready_status, StatusCode::OK);
    assert_eq!(ready["status"], "ready");
    assert_eq!(ready["checks"]["database"], true);
    assert_eq!(ready["checks"]["crdtStore"], true);
    assert_eq!(ready["checks"]["blobStore"], true);

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(header::CONTENT_TYPE)
            .unwrap()
            .to_str()
            .unwrap(),
        "application/openmetrics-text; version=1.0.0; charset=utf-8"
    );
    let body = String::from_utf8(
        response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    assert!(body.contains("realtime_crdt_updates_total 1\n"));
    assert!(body.contains("realtime_crdt_update_bytes_total "));
    assert!(body.ends_with("# EOF\n"));

    state.runtime_health.begin_draining();
    let (draining_status, draining) = send(&router, "GET", "/health/ready", None, None).await;
    assert_eq!(draining_status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(draining["status"], "not_ready");
    assert_eq!(draining["checks"]["draining"], true);
    state.jobs.shutdown().await;
}

async fn send(
    app: &Router,
    method: &str,
    uri: &str,
    bearer: Option<&str>,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let mut req = Request::builder().method(method).uri(uri);
    if let Some(b) = bearer {
        req = req.header(header::AUTHORIZATION, format!("Bearer {b}"));
    }
    let req = if let Some(b) = body {
        req.header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(serde_json::to_vec(&b).unwrap()))
            .unwrap()
    } else {
        req.body(Body::empty()).unwrap()
    };

    let res = app.clone().oneshot(req).await.unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    (status, value)
}

async fn multipart_upload(
    app: &Router,
    token: &str,
    path: &str,
    filename: &str,
    content_type: &str,
    bytes: &[u8],
) -> (StatusCode, Value) {
    let boundary = format!("----realtime-{}", uuid::Uuid::new_v4());
    let mut body = Vec::new();
    body.extend_from_slice(
        format!("--{boundary}\r\nContent-Disposition: form-data; name=\"path\"\r\n\r\n{path}\r\n")
            .as_bytes(),
    );
    body.extend_from_slice(format!("--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"{filename}\"\r\nContent-Type: {content_type}\r\n\r\n").as_bytes());
    body.extend_from_slice(bytes);
    body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/upload?token={token}"))
                .header(
                    header::CONTENT_TYPE,
                    format!("multipart/form-data; boundary={boundary}"),
                )
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    (status, value)
}

/// Drive the mock login flow and return a session token for the given subject.
async fn login(app: &Router, sub: &str) -> String {
    login_with_picture(app, sub, None).await
}

/// Drive the mock login flow with an optional OpenID `picture` URL.
async fn login_with_picture(app: &Router, sub: &str, picture: Option<&str>) -> String {
    let mut uri = format!("/auth/login?redirect=http://app/cb&mock_sub={sub}&mock_name={sub}");
    if let Some(pic) = picture {
        uri.push_str("&mock_picture=");
        uri.push_str(&url::form_urlencoded::byte_serialize(pic.as_bytes()).collect::<String>());
    }
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(uri)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::SEE_OTHER, "login should redirect");
    let loc = res
        .headers()
        .get(header::LOCATION)
        .unwrap()
        .to_str()
        .unwrap();
    let state = url::Url::parse(&format!("http://x{loc}"))
        .unwrap()
        .query_pairs()
        .find(|(k, _)| k == "state")
        .unwrap()
        .1
        .into_owned();

    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/auth/callback?state={state}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        StatusCode::SEE_OTHER,
        "callback should redirect"
    );
    let loc = res
        .headers()
        .get(header::LOCATION)
        .unwrap()
        .to_str()
        .unwrap();
    url::Url::parse(loc)
        .unwrap()
        .query_pairs()
        .find(|(k, _)| k == "token")
        .unwrap()
        .1
        .into_owned()
}

fn pkce_challenge(verifier: &str) -> String {
    use base64::Engine;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()))
}

async fn oauth_token(app: &Router, owner_sub: &str, verifier: &str) -> (String, String, String) {
    let session = login(app, owner_sub).await;
    let (status, vault) = send(
        app,
        "POST",
        "/api/vaults",
        Some(&session),
        Some(json!({"name": "OAuth Vault"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let vault_id = vault["id"].as_str().unwrap().to_string();

    let (status, cursor) = send(
        app,
        "POST",
        &format!("/api/vaults/{vault_id}/cursors"),
        Some(&session),
        Some(json!({"name": "MCP Client"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let resource = cursor["mcpUrl"].as_str().unwrap();

    let (status, client) = send(
        app,
        "POST",
        "/oauth/register",
        None,
        Some(json!({"redirect_uris": ["http://client/cb"]})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let client_id = client["client_id"].as_str().unwrap();

    let authorize_uri = format!(
        "/oauth/authorize?response_type=code&client_id={client_id}&redirect_uri=http%3A%2F%2Fclient%2Fcb&code_challenge={}&code_challenge_method=S256&resource={}&state=s1&mock_sub={owner_sub}&mock_name={owner_sub}",
        pkce_challenge(verifier),
        url::form_urlencoded::byte_serialize(resource.as_bytes()).collect::<String>()
    );
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(authorize_uri)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::SEE_OTHER);
    let loc = res
        .headers()
        .get(header::LOCATION)
        .unwrap()
        .to_str()
        .unwrap();
    let oidc_state = url::Url::parse(&format!("http://x{loc}"))
        .unwrap()
        .query_pairs()
        .find(|(k, _)| k == "state")
        .unwrap()
        .1
        .into_owned();
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/auth/callback?state={oidc_state}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::SEE_OTHER);
    let loc = res
        .headers()
        .get(header::LOCATION)
        .unwrap()
        .to_str()
        .unwrap();
    let code = url::Url::parse(loc)
        .unwrap()
        .query_pairs()
        .find(|(k, _)| k == "code")
        .unwrap()
        .1
        .into_owned();

    let body = format!(
        "grant_type=authorization_code&code={code}&redirect_uri=http%3A%2F%2Fclient%2Fcb&client_id={client_id}&code_verifier={verifier}"
    );
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/oauth/token")
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let tokens: Value = serde_json::from_slice(&bytes).unwrap();
    (
        tokens["access_token"].as_str().unwrap().to_string(),
        tokens["refresh_token"].as_str().unwrap().to_string(),
        vault_id,
    )
}

// ---------- tests ----------

#[tokio::test]
async fn internal_ydoc_reads_and_writes_do_not_use_the_public_origin() {
    let config = test_config(
        "http://unused-public-origin.invalid",
        realtime_server::blobs::MAX_BLOB_BYTES,
    );
    let state = build_state(config).await.unwrap();
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        realtime_server::ydoc::write_update(
            &state,
            "vault__note",
            text_update("contents", "MCP create body"),
        ),
    )
    .await
    .expect("native internal write must not use the public origin")
    .unwrap();
    let update = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        realtime_server::ydoc::read_update(&state, "vault__note"),
    )
    .await
    .expect("native internal read must not use the public origin")
    .unwrap();
    assert_eq!(
        realtime_server::ydoc::decode_text(&update, "contents").unwrap(),
        "MCP create body"
    );
}

#[tokio::test]
async fn login_creates_session_and_me_works() {
    let app = test_app().await;

    let token = login_with_picture(&app, "alice", Some("https://example.com/alice.png")).await;
    let (status, me) = send(&app, "GET", "/api/me", Some(&token), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(me["displayName"], "alice");
    assert_eq!(me["gitEmail"], Value::Null);
    assert_eq!(me["pictureUrl"], "https://example.com/alice.png");
    assert_eq!(me["avatarUrl"], "https://example.com/alice.png");
    assert_eq!(me["avatarUrlOverride"], Value::Null);

    let (status, me) = send(
        &app,
        "PATCH",
        "/api/me",
        Some(&token),
        Some(json!({ "gitEmail": "alice+git@example.com" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(me["gitEmail"], "alice+git@example.com");

    let (status, me) = send(&app, "GET", "/api/me", Some(&token), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(me["gitEmail"], "alice+git@example.com");

    // git_email is self-settable and flows unescaped into the git author line
    // and Co-authored-by trailers, so injection attempts must be rejected.
    let cases = [
        (
            "a@b.com>\nSigned-off-by: attacker <a@x>\n",
            "trailer injection via '>'",
        ),
        ("a@b.com\r\nX: y", "crlf trailer injection"),
        ("alice<@example.com", "angle bracket in local"),
        ("alice @example.com", "whitespace in local part"),
        ("alice\n@example.com", "newline in local"),
        ("not-an-email", "missing '@'"),
        ("alice@", "empty domain"),
        ("@example.com", "empty local"),
        ("alice@example", "domain without dot"),
        ("a@b@example.com", "multiple '@'"),
    ];
    for (email, label) in cases {
        let (status, me) = send(
            &app,
            "PATCH",
            "/api/me",
            Some(&token),
            Some(json!({ "gitEmail": email })),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "case {label:?}: {email:?}");
        assert!(
            me["gitEmail"].is_null(),
            "case {label:?}: gitEmail should be absent in error body"
        );
    }

    // The last accepted value is still in place (no partial write).
    let (status, me) = send(&app, "GET", "/api/me", Some(&token), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(me["gitEmail"], "alice+git@example.com");

    // Empty string clears the stored value.
    let (status, me) = send(
        &app,
        "PATCH",
        "/api/me",
        Some(&token),
        Some(json!({ "gitEmail": "" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(me["gitEmail"], Value::Null);

    // --- avatar override ---

    // Set an avatar override; gitEmail is preserved (partial update).
    let (status, me) = send(
        &app,
        "PATCH",
        "/api/me",
        Some(&token),
        Some(json!({ "avatarUrlOverride": "https://cdn.example.com/a.jpg" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(me["avatarUrlOverride"], "https://cdn.example.com/a.jpg");
    assert_eq!(me["avatarUrl"], "https://cdn.example.com/a.jpg");
    assert_eq!(me["gitEmail"], Value::Null, "gitEmail should be unchanged");

    // Clear the override; avatarUrl falls back to pictureUrl.
    let (status, me) = send(
        &app,
        "PATCH",
        "/api/me",
        Some(&token),
        Some(json!({ "avatarUrlOverride": null })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(me["avatarUrlOverride"], Value::Null);
    assert_eq!(me["avatarUrl"], "https://example.com/alice.png");

    // Re-set the override for the invalid-value tests below.
    let (status, _) = send(
        &app,
        "PATCH",
        "/api/me",
        Some(&token),
        Some(json!({ "avatarUrlOverride": "https://cdn.example.com/a.jpg" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Invalid avatar URLs are rejected; the last valid override stays.
    let invalid_avatars = [
        "javascript:alert(1)",
        "ftp://example.com/a.png",
        "https://exa mple.com/a.png",
    ];
    for url in invalid_avatars {
        let (status, me) = send(
            &app,
            "PATCH",
            "/api/me",
            Some(&token),
            Some(json!({ "avatarUrlOverride": url })),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "invalid avatar: {url:?}");
        assert_eq!(
            me["avatarUrl"],
            Value::Null,
            "error body should have no avatarUrl"
        );
    }
    // Over-long URL (> 2048 bytes).
    let long_url = format!("https://example.com/{}", "a".repeat(2040));
    let (status, me) = send(
        &app,
        "PATCH",
        "/api/me",
        Some(&token),
        Some(json!({ "avatarUrlOverride": long_url })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "over-long avatar URL");
    assert_eq!(me["avatarUrl"], Value::Null);

    // The last valid override is still in place.
    let (status, me) = send(&app, "GET", "/api/me", Some(&token), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(me["avatarUrlOverride"], "https://cdn.example.com/a.jpg");
    assert_eq!(me["avatarUrl"], "https://cdn.example.com/a.jpg");

    // Legacy: PATCH with `{}` still clears gitEmail for old clients.
    let (status, me) = send(&app, "PATCH", "/api/me", Some(&token), Some(json!({}))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        me["gitEmail"],
        Value::Null,
        "legacy {{}} should clear gitEmail"
    );
    // avatarUrlOverride should be unchanged (not cleared by legacy {{}}).
    assert_eq!(me["avatarUrlOverride"], "https://cdn.example.com/a.jpg");

    // No bearer -> 401.
    let (status, _) = send(&app, "GET", "/api/me", None, None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn server_info_returns_stable_id_without_auth() {
    let app = test_app().await;

    // Public: no bearer required.
    let (status, info) = send(&app, "GET", "/api/server-info", None, None).await;
    assert_eq!(status, StatusCode::OK);
    let id = info["serverId"].as_str().expect("serverId string");
    assert!(!id.is_empty());

    // Stable across calls on the same server.
    let (_, again) = send(&app, "GET", "/api/server-info", None, None).await;
    assert_eq!(again["serverId"].as_str(), Some(id));

    // Release version is advertised (operator-facing; not used for gating).
    let version = info["version"].as_str().expect("version string");
    assert_eq!(version, env!("CARGO_PKG_VERSION"));

    // Named capability versions are advertised for client-side gating.
    let caps = info["caps"].as_object().expect("caps object");
    assert_eq!(caps["restApi"].as_str(), Some("3"));
    assert_eq!(caps["oauth"].as_str(), Some("1"));
    assert_eq!(caps["pluginDbSync"].as_str(), Some("crsqlite-1"));
    assert_eq!(
        caps["attachmentShim"].as_str(),
        Some("https://realtime.md/attachment-shim/v1")
    );
    assert_eq!(caps["documentEpoch"].as_str(), Some("1"));
    assert_eq!(caps["documentInvalidation"].as_str(), Some("1"));

    // Epoch-aware clients are required before the server can safely retire a
    // physical Yjs history.
    assert_eq!(
        info["requiredCaps"].as_array(),
        Some(&vec![serde_json::json!("documentEpoch")])
    );
}

#[tokio::test]
async fn server_info_and_session_survive_restart() {
    let config = test_config("http://auth.test", realtime_server::blobs::MAX_BLOB_BYTES);

    let first = app_from_config(&config).await;
    let token = login(&first, "alice").await;
    let (status, before) = send(&first, "GET", "/api/server-info", None, None).await;
    assert_eq!(status, StatusCode::OK);
    let server_id = before["serverId"]
        .as_str()
        .expect("serverId string")
        .to_string();
    drop(first);

    let second = app_from_config(&config).await;
    let (status, after) = send(&second, "GET", "/api/server-info", None, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(after["serverId"].as_str(), Some(server_id.as_str()));

    let (status, identity) = send(&second, "GET", "/api/me", Some(&token), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(identity["displayName"], "alice");
}

#[tokio::test]
async fn logout_revokes_session() {
    let app = test_app().await;

    let token = login(&app, "alice").await;
    let (status, _) = send(&app, "POST", "/api/logout", Some(&token), Some(json!({}))).await;
    assert_eq!(status, StatusCode::OK);

    let (status, _) = send(&app, "GET", "/api/me", Some(&token), None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn active_sessions_renew_without_an_absolute_lifetime() {
    use realtime_server::entities::sessions;
    use sea_orm::{ActiveModelTrait, EntityTrait, Set};

    let (app, state) = test_app_with_state().await;
    let token = login(&app, "alice").await;
    let token_hash = Sha256::digest(token.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let session = sessions::Entity::find_by_id(&token_hash)
        .one(&state.db)
        .await
        .unwrap()
        .unwrap();
    let near_expiry = realtime_server::session::now_millis() + 60_000;
    let mut active: sessions::ActiveModel = session.into();
    // An old creation date must not impose an absolute session lifetime.
    active.created_at = Set(0);
    active.expires_at = Set(near_expiry);
    active.update(&state.db).await.unwrap();

    let (status, _) = send(&app, "GET", "/api/me", Some(&token), None).await;
    assert_eq!(status, StatusCode::OK);
    let renewed = sessions::Entity::find_by_id(&token_hash)
        .one(&state.db)
        .await
        .unwrap()
        .unwrap();
    assert!(
        renewed.expires_at > near_expiry + 29 * 24 * 60 * 60 * 1000,
        "active session should receive a fresh 30-day inactivity deadline"
    );

    let mut expired: sessions::ActiveModel = renewed.into();
    expired.expires_at = Set(realtime_server::session::now_millis() - 1);
    expired.update(&state.db).await.unwrap();
    let (status, _) = send(&app, "GET", "/api/me", Some(&token), None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn login_rejects_unallowed_redirect() {
    let app = test_app().await;

    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/auth/login?redirect=https://evil.example/cb")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn openapi_json_and_swagger_docs_are_served() {
    let app = test_app().await;

    let (status, spec) = send(&app, "GET", "/openapi.json", None, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(spec["openapi"], "3.1.0");
    let paths = spec["paths"].as_object().unwrap();
    assert!(paths.contains_key("/api/vaults/{id}/notes/{path}"));
    assert!(paths.contains_key("/api/vaults/{id}/attachments/upload-link"));
    assert!(paths.contains_key("/api/vaults/{id}/canvas-operations/{path}"));
    assert!(
        paths["/api/vaults/{id}/canvas-operations/{path}"]["post"]["requestBody"]["content"]
            ["application/json"]["schema"]["$ref"]
            .as_str()
            .is_some_and(|value| value.ends_with("/CanvasOperationBatchBody"))
    );
    assert!(paths.contains_key("/oauth/token"));
    assert!(paths.contains_key("/upload"));
    assert!(paths.contains_key("/n/{guid}"));
    assert!(!paths.contains_key("/mcp/i/{app_id}"));
    assert!(!paths.contains_key("/d/{rest}"));
    assert!(!paths.contains_key("/api/doc-token"));
    assert!(!paths.contains_key("/api/vaults/{id}/blobs/{hash}"));
    assert!(spec["components"]["securitySchemes"]["bearerAuth"].is_object());

    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/docs/")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = res.into_body().collect().await.unwrap().to_bytes();
    let html = String::from_utf8_lossy(&body);
    assert!(html.contains("Swagger UI"));
}

#[tokio::test]
async fn oauth_authorize_token_refresh_and_rest_access() {
    let app = test_app().await;
    let verifier = "correct-horse-battery-staple";
    let (access, refresh, vault_id) = oauth_token(&app, "alice", verifier).await;

    let (status, note) = send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/notes"),
        Some(&access),
        Some(json!({"path": "oauth.md", "content": "hello"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(note["path"], "oauth.md");

    let body = format!("grant_type=refresh_token&refresh_token={refresh}");
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/oauth/token")
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn oauth_rejects_bad_pkce_and_is_single_use() {
    let app = test_app().await;
    let session = login(&app, "alice").await;
    let (status, vault) = send(
        &app,
        "POST",
        "/api/vaults",
        Some(&session),
        Some(json!({"name":"v"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let vault_id = vault["id"].as_str().unwrap();
    let (status, cursor) = send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/cursors"),
        Some(&session),
        Some(json!({"name":"c"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let resource = cursor["mcpUrl"].as_str().unwrap();
    let (status, client) = send(
        &app,
        "POST",
        "/oauth/register",
        None,
        Some(json!({"redirect_uris":["http://client/cb"]})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let client_id = client["client_id"].as_str().unwrap();
    let uri = format!("/oauth/authorize?response_type=code&client_id={client_id}&redirect_uri=http%3A%2F%2Fclient%2Fcb&code_challenge={}&code_challenge_method=S256&resource={}&mock_sub=alice", pkce_challenge("good"), url::form_urlencoded::byte_serialize(resource.as_bytes()).collect::<String>());
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(uri)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let loc = res
        .headers()
        .get(header::LOCATION)
        .unwrap()
        .to_str()
        .unwrap();
    let oidc_state = url::Url::parse(&format!("http://x{loc}"))
        .unwrap()
        .query_pairs()
        .find(|(k, _)| k == "state")
        .unwrap()
        .1
        .into_owned();
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/auth/callback?state={oidc_state}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let loc = res
        .headers()
        .get(header::LOCATION)
        .unwrap()
        .to_str()
        .unwrap();
    let code = url::Url::parse(loc)
        .unwrap()
        .query_pairs()
        .find(|(k, _)| k == "code")
        .unwrap()
        .1
        .into_owned();
    let body = format!("grant_type=authorization_code&code={code}&redirect_uri=http%3A%2F%2Fclient%2Fcb&client_id={client_id}&code_verifier=bad");
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/oauth/token")
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    let body = format!("grant_type=authorization_code&code={code}&redirect_uri=http%3A%2F%2Fclient%2Fcb&client_id={client_id}&code_verifier=good");
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/oauth/token")
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn oauth_wrong_owner_authorize_forbidden() {
    let app = test_app().await;
    let session = login(&app, "alice").await;
    let (status, vault) = send(
        &app,
        "POST",
        "/api/vaults",
        Some(&session),
        Some(json!({"name":"v"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let vault_id = vault["id"].as_str().unwrap();
    let (status, cursor) = send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/cursors"),
        Some(&session),
        Some(json!({"name":"c"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let resource = cursor["mcpUrl"].as_str().unwrap();
    let (status, client) = send(
        &app,
        "POST",
        "/oauth/register",
        None,
        Some(json!({"redirect_uris":["http://client/cb"]})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let client_id = client["client_id"].as_str().unwrap();
    let uri = format!("/oauth/authorize?response_type=code&client_id={client_id}&redirect_uri=http%3A%2F%2Fclient%2Fcb&code_challenge={}&code_challenge_method=S256&resource={}&mock_sub=bob", pkce_challenge("good"), url::form_urlencoded::byte_serialize(resource.as_bytes()).collect::<String>());
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(uri)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let loc = res
        .headers()
        .get(header::LOCATION)
        .unwrap()
        .to_str()
        .unwrap();
    let oidc_state = url::Url::parse(&format!("http://x{loc}"))
        .unwrap()
        .query_pairs()
        .find(|(k, _)| k == "state")
        .unwrap()
        .1
        .into_owned();
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/auth/callback?state={oidc_state}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn mcp_unauthenticated_returns_resource_metadata_challenge() {
    let app = test_app().await;
    let session = login(&app, "alice").await;
    let (status, vault) = send(
        &app,
        "POST",
        "/api/vaults",
        Some(&session),
        Some(json!({"name":"v"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let vault_id = vault["id"].as_str().unwrap();
    let (status, cursor) = send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/cursors"),
        Some(&session),
        Some(json!({"name":"c"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let app_id = cursor["appId"].as_str().unwrap();

    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/mcp/i/{app_id}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    let challenge = res
        .headers()
        .get(header::WWW_AUTHENTICATE)
        .unwrap()
        .to_str()
        .unwrap();
    assert!(challenge.contains(&format!(
        "/.well-known/oauth-protected-resource/mcp/i/{app_id}"
    )));
}

async fn mcp_call(
    app: &Router,
    app_id: &str,
    token: &str,
    id: i64,
    method: &str,
    params: Value,
) -> (StatusCode, Value) {
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/mcp/i/{app_id}"))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::HOST, "127.0.0.1")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::ACCEPT, "application/json, text/event-stream")
                .body(Body::from(
                    serde_json::to_vec(&json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "method": method,
                        "params": params,
                    }))
                    .unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let value = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| json!({ "raw": String::from_utf8_lossy(&bytes) }));
    (status, value)
}

#[tokio::test]
async fn mcp_lists_tools_and_round_trips_note_edits() {
    let app = test_app().await;
    let session = login(&app, "alice").await;
    let (status, vault) = send(
        &app,
        "POST",
        "/api/vaults",
        Some(&session),
        Some(json!({"name":"v"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let vault_id = vault["id"].as_str().unwrap();
    let (status, cursor) = send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/cursors"),
        Some(&session),
        Some(json!({"name":"c"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let app_id = cursor["appId"].as_str().unwrap();
    let secret = cursor["secretToken"].as_str().unwrap();

    let (status, list) = mcp_call(&app, app_id, secret, 1, "tools/list", json!({})).await;
    assert_eq!(status, StatusCode::OK, "{list}");
    let tools = list["result"]["tools"].as_array().unwrap();
    assert!(tools.iter().any(|tool| tool["name"] == "create_note"));
    assert!(tools.iter().any(|tool| tool["name"] == "read_attachment"));
    assert!(tools
        .iter()
        .any(|tool| tool["name"] == "apply_canvas_operations"));
    // Every tool advertises annotations so clients can auto-allow read-only
    // tools; titles carry the category prefix (Canvas/Base/Note/Attachment/Search).
    for tool in tools {
        let annotations = &tool["annotations"];
        assert!(
            annotations["readOnlyHint"].is_boolean(),
            "missing readOnlyHint on {}",
            tool["name"]
        );
        let title = annotations["title"].as_str().unwrap();
        assert!(
            [
                "Canvas: ",
                "Base: ",
                "Note: ",
                "Attachment: ",
                "Search: ",
                "Plugin DB: "
            ]
            .iter()
            .any(|prefix| title.starts_with(prefix)),
            "uncategorized title {title:?} on {}",
            tool["name"]
        );
    }
    let read_note = tools.iter().find(|t| t["name"] == "read_note").unwrap();
    assert_eq!(read_note["annotations"]["readOnlyHint"], json!(true));
    let delete_note = tools.iter().find(|t| t["name"] == "delete_note").unwrap();
    assert_eq!(delete_note["annotations"]["readOnlyHint"], json!(false));
    assert_eq!(delete_note["annotations"]["destructiveHint"], json!(true));

    let (status, created) = mcp_call(
        &app,
        app_id,
        secret,
        2,
        "tools/call",
        json!({
            "name": "create_note",
            "arguments": {"path": "mcp.md", "content": "hello"}
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{created}");
    assert_eq!(created["result"]["isError"], false);

    let (status, patched) = mcp_call(
        &app,
        app_id,
        secret,
        3,
        "tools/call",
        json!({
            "name": "patch_note",
            "arguments": {"path": "mcp.md", "old": "hello", "new": "hello mcp"}
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{patched}");
    assert_eq!(patched["result"]["isError"], false);

    let (status, read) = mcp_call(
        &app,
        app_id,
        secret,
        4,
        "tools/call",
        json!({
            "name": "read_note",
            "arguments": {"path": "mcp.md"}
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{read}");
    assert_eq!(
        read["result"]["structuredContent"]["data"]["content"],
        "hello mcp"
    );

    let (status, _) = send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/canvases"),
        Some(&session),
        Some(json!({"path": "mcp.canvas", "value": {"nodes": [], "edges": []}})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, applied) = mcp_call(
        &app,
        app_id,
        secret,
        5,
        "tools/call",
        json!({
            "name": "apply_canvas_operations",
            "arguments": {
                "path": "mcp.canvas",
                "mutationId": "mcp-canvas-1",
                "operations": [
                    {"type": "node-create", "node": {"id": "n1", "type": "text", "text": "live", "x": 1, "y": 2}}
                ]
            }
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{applied}");
    assert_eq!(applied["result"]["isError"], false);
    let (_, canvas) = send(
        &app,
        "GET",
        &format!("/api/vaults/{vault_id}/canvas/mcp.canvas"),
        Some(&session),
        None,
    )
    .await;
    assert_eq!(canvas["value"]["nodes"][0]["text"], "live");
}

#[tokio::test]
async fn create_list_vault() {
    let app = test_app().await;
    let token = login(&app, "alice").await;

    let (status, vault) = send(
        &app,
        "POST",
        "/api/vaults",
        Some(&token),
        Some(json!({"name": "Notes"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(vault["role"], "admin");
    assert_eq!(vault["owner"], true);
    assert!(vault["createdBy"].as_str().is_some());

    let (status, list) = send(&app, "GET", "/api/vaults", Some(&token), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(list.as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn vault_admin_can_inspect_and_cancel_persistent_jobs() {
    let mut config = test_config("http://auth.test", realtime_server::blobs::MAX_BLOB_BYTES);
    config.background_jobs_enabled = false;
    let state = build_state(config).await.unwrap();
    let app = app(state.clone());
    let admin = login(&app, "jobs-admin").await;
    let outsider = login(&app, "jobs-outsider").await;
    let (_, vault) = send(
        &app,
        "POST",
        "/api/vaults",
        Some(&admin),
        Some(json!({"name": "Jobs"})),
    )
    .await;
    let vault_id = vault["id"].as_str().unwrap();
    state.jobs.enqueue_search_vault(vault_id).await.unwrap();
    let jobs_url = format!("/api/vaults/{vault_id}/jobs");

    let (status, _) = send(&app, "GET", &jobs_url, Some(&outsider), None).await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    let (status, jobs) = send(&app, "GET", &jobs_url, Some(&admin), None).await;
    assert_eq!(status, StatusCode::OK);
    let queued = jobs
        .as_array()
        .unwrap()
        .iter()
        .find(|job| job["payload"]["kind"] == "search_vault")
        .unwrap();
    assert_eq!(queued["status"], "pending");
    let intent_key = queued["intentKey"].as_str().unwrap();

    let (status, body) = send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/jobs/cancel"),
        Some(&admin),
        Some(json!({"intentKey": intent_key})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["ok"], true);

    let (_, jobs) = send(&app, "GET", &jobs_url, Some(&admin), None).await;
    let canceled = jobs
        .as_array()
        .unwrap()
        .iter()
        .find(|job| job["intentKey"] == intent_key)
        .unwrap();
    assert_eq!(canceled["status"], "completed");

    let (status, _) = send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/jobs/retry"),
        Some(&admin),
        Some(json!({"intentKey": intent_key})),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
}

#[tokio::test]
async fn note_crud_rest_roundtrip() {
    let app = test_app().await;
    let token = login(&app, "alice").await;
    let (_, vault) = send(
        &app,
        "POST",
        "/api/vaults",
        Some(&token),
        Some(json!({"name": "Notes"})),
    )
    .await;
    let vault_id = vault["id"].as_str().unwrap();

    let notes_url = format!("/api/vaults/{vault_id}/notes");
    let (status, created) = send(
        &app,
        "POST",
        &notes_url,
        Some(&token),
        Some(json!({"path": "dir/a.md", "content": "# A\n"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(created["path"], "dir/a.md");
    assert_eq!(created["content"], "# A\n");
    assert!(created["permalink"].as_str().unwrap().contains("/n/"));
    let guid = created["guid"].as_str().unwrap();

    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/n/{guid}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::TEMPORARY_REDIRECT);
    let loc = res
        .headers()
        .get(header::LOCATION)
        .unwrap()
        .to_str()
        .unwrap();
    assert!(loc.starts_with("obsidian://realtime-open?"));
    assert!(loc.contains(&format!("vaultId={vault_id}")));
    assert!(loc.contains(&format!("guid={guid}")));

    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/p?vault={vault_id}&path=dir%2Fa.md"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::TEMPORARY_REDIRECT);
    let loc = res
        .headers()
        .get(header::LOCATION)
        .unwrap()
        .to_str()
        .unwrap();
    assert!(loc.contains("path=dir%2Fa.md"));

    let (status, list) = send(&app, "GET", &notes_url, Some(&token), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(list.as_array().unwrap().len(), 1);
    assert_eq!(list[0]["path"], "dir/a.md");

    let note_url = format!("/api/vaults/{vault_id}/notes/dir/a.md");
    let (status, read) = send(&app, "GET", &note_url, Some(&token), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(read["content"], "# A\n");

    let (status, replaced) = send(
        &app,
        "PUT",
        &note_url,
        Some(&token),
        Some(json!({"content": "# B\nbody"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(replaced["content"], "# B\nbody");

    let (status, read) = send(&app, "GET", &note_url, Some(&token), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(read["content"], "# B\nbody");

    let (status, patched) = send(
        &app,
        "PATCH",
        &note_url,
        Some(&token),
        Some(json!({"old": "body", "new": "patched"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(patched["content"], "# B\npatched");

    let (status, with_fm) = send(
        &app,
        "PUT",
        &note_url,
        Some(&token),
        Some(json!({"content": "---\ntitle: Old\ntags:\n  - a\n---\n# B\npatched"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(with_fm["content"].as_str().unwrap().starts_with("---\n"));

    let fm_url = format!("/api/vaults/{vault_id}/note-frontmatter/dir/a.md");
    let (status, fm) = send(&app, "GET", &fm_url, Some(&token), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(fm["frontmatter"]["title"], "Old");

    let (status, fm_patched) = send(
        &app,
        "PATCH",
        &fm_url,
        Some(&token),
        Some(json!({"set": {"title": "New", "draft": true}, "unset": ["tags"]})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let content = fm_patched["content"].as_str().unwrap();
    assert!(content.contains("title: New"), "{content}");
    assert!(content.contains("draft: true"), "{content}");
    assert!(!content.contains("tags:"), "{content}");

    let move_url = format!("/api/vaults/{vault_id}/note-moves/dir/a.md");
    let (status, moved) = send(
        &app,
        "POST",
        &move_url,
        Some(&token),
        Some(json!({"toPath": "dir/b.md"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(moved["path"], "dir/b.md");
    assert!(moved["content"].as_str().unwrap().contains("# B\npatched"));

    let (status, _) = send(&app, "GET", &note_url, Some(&token), None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let note_url = format!("/api/vaults/{vault_id}/notes/dir/b.md");
    let (status, read) = send(&app, "GET", &note_url, Some(&token), None).await;
    assert_eq!(status, StatusCode::OK);
    assert!(read["content"].as_str().unwrap().contains("# B\npatched"));

    let (status, deleted) = send(&app, "DELETE", &note_url, Some(&token), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(deleted["ok"], true);

    let (status, _) = send(&app, "GET", &note_url, Some(&token), None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn periodic_daily_note_get_create_and_append() {
    let app = test_app().await;
    let token = login(&app, "alice").await;
    let (_, vault) = send(
        &app,
        "POST",
        "/api/vaults",
        Some(&token),
        Some(json!({"name": "Notes"})),
    )
    .await;
    let vault_id = vault["id"].as_str().unwrap();

    let periodic_url = format!("/api/vaults/{vault_id}/periodic/daily");
    let (status, note) = send(
        &app,
        "POST",
        &periodic_url,
        Some(&token),
        Some(json!({"date": "2026-06-06", "content": "# Today"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(note["path"], "Daily Notes/2026-06-06.md");
    assert_eq!(note["content"], "# Today");

    let (status, same) = send(
        &app,
        "POST",
        &periodic_url,
        Some(&token),
        Some(json!({"date": "2026-06-06", "content": "ignored"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(same["guid"], note["guid"]);
    assert_eq!(same["content"], "# Today");

    let append_url = format!("/api/vaults/{vault_id}/periodic/daily/append");
    let (status, appended) = send(
        &app,
        "POST",
        &append_url,
        Some(&token),
        Some(json!({"date": "2026-06-06", "text": "- item"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(appended["content"], "# Today\n- item");
}

#[tokio::test]
async fn attachment_upload_list_read_delete_roundtrip() {
    let app = test_app().await;
    let token = login(&app, "alice").await;
    let (_, vault) = send(
        &app,
        "POST",
        "/api/vaults",
        Some(&token),
        Some(json!({"name": "Notes"})),
    )
    .await;
    let vault_id = vault["id"].as_str().unwrap();
    let url = format!("/api/vaults/{vault_id}/attachments/images/pic.png");

    let payload = b"\x89PNG\r\n\x1a\nimage bytes".to_vec();
    let (status, uploaded_bytes) = send_raw(&app, "PUT", &url, Some(&token), payload.clone()).await;
    assert_eq!(status, StatusCode::OK);
    let uploaded: Value = serde_json::from_slice(&uploaded_bytes).unwrap();
    assert_eq!(uploaded["path"], "images/pic.png");
    assert_eq!(uploaded["size"], payload.len() as i64);
    let hash = uploaded["hash"].as_str().unwrap();
    assert_eq!(hash.len(), 64);

    let list_url = format!("/api/vaults/{vault_id}/attachments");
    let (status, list) = send(&app, "GET", &list_url, Some(&token), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(list.as_array().unwrap().len(), 1);
    assert_eq!(list[0]["hash"], hash);

    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(&url)
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(
        res.headers().get(header::CONTENT_TYPE).unwrap(),
        "image/png"
    );
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(bytes.as_ref(), payload.as_slice());

    let move_url = format!("/api/vaults/{vault_id}/attachment-moves/images/pic.png");
    let (status, moved) = send(
        &app,
        "POST",
        &move_url,
        Some(&token),
        Some(json!({"toPath": "images/moved.png"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(moved["path"], "images/moved.png");
    assert_eq!(moved["hash"], hash);

    let (status, _) = send(&app, "GET", &url, Some(&token), None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let url = format!("/api/vaults/{vault_id}/attachments/images/moved.png");
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(&url)
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(bytes.as_ref(), payload.as_slice());

    let (status, deleted) = send(&app, "DELETE", &url, Some(&token), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(deleted["ok"], true);

    let (status, list) = send(&app, "GET", &list_url, Some(&token), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(list.as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn authenticated_attachment_upload_accepts_arbitrary_vault_files() {
    let app = test_app().await;
    let token = login(&app, "alice").await;
    let (_, vault) = send(
        &app,
        "POST",
        "/api/vaults",
        Some(&token),
        Some(json!({"name": "Notes"})),
    )
    .await;
    let vault_id = vault["id"].as_str().unwrap();

    for (path, payload) in [
        ("data/config.json", b"{\"enabled\":true}".as_slice()),
        ("archives/backup.zip", b"zip bytes".as_slice()),
        ("LICENSE", b"license text".as_slice()),
    ] {
        let url = format!("/api/vaults/{vault_id}/attachments/{path}");
        let (status, _) = send_raw(&app, "PUT", &url, Some(&token), payload.to_vec()).await;
        assert_eq!(status, StatusCode::OK, "failed to upload {path}");
    }

    let list_url = format!("/api/vaults/{vault_id}/attachments");
    let (status, list) = send(&app, "GET", &list_url, Some(&token), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(list.as_array().unwrap().len(), 3);
}

#[tokio::test]
async fn signed_upload_link_uploads_once_and_rejects_bad_inputs() {
    let app = test_app_with_attachment_max(16).await;
    let token = login(&app, "alice").await;
    let (_, vault) = send(
        &app,
        "POST",
        "/api/vaults",
        Some(&token),
        Some(json!({"name": "Notes"})),
    )
    .await;
    let vault_id = vault["id"].as_str().unwrap();

    let link_url = format!("/api/vaults/{vault_id}/attachments/upload-link");
    let (status, link) = send(
        &app,
        "POST",
        &link_url,
        Some(&token),
        Some(json!({"landingDir": "uploads", "expiresInSeconds": 60})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let upload_token = link["token"].as_str().unwrap();

    let png = b"\x89PNG\r\n\x1a\n123";
    let (status, uploaded) =
        multipart_upload(&app, upload_token, "pic.png", "pic.png", "image/png", png).await;
    assert_eq!(status, StatusCode::OK, "{uploaded}");
    assert_eq!(uploaded["path"], "uploads/pic.png");

    let (status, reused) = multipart_upload(
        &app,
        upload_token,
        "again.png",
        "again.png",
        "image/png",
        png,
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{reused}");

    let (_, link) = send(
        &app,
        "POST",
        &link_url,
        Some(&token),
        Some(json!({"landingDir": "uploads"})),
    )
    .await;
    let upload_token = link["token"].as_str().unwrap();
    let (status, rejected) = multipart_upload(
        &app,
        upload_token,
        "bad.exe",
        "bad.exe",
        "application/octet-stream",
        b"abc",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{rejected}");

    let (_, link) = send(
        &app,
        "POST",
        &link_url,
        Some(&token),
        Some(json!({"landingDir": "uploads"})),
    )
    .await;
    let upload_token = link["token"].as_str().unwrap();
    let (status, rejected) = multipart_upload(
        &app,
        upload_token,
        "page.txt",
        "page.txt",
        "text/html",
        b"<html>no</html>",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{rejected}");

    let (_, link) = send(
        &app,
        "POST",
        &link_url,
        Some(&token),
        Some(json!({"landingDir": "uploads"})),
    )
    .await;
    let upload_token = link["token"].as_str().unwrap();
    let (status, rejected) = multipart_upload(
        &app,
        upload_token,
        "big.txt",
        "big.txt",
        "text/plain",
        b"0123456789abcdefg",
    )
    .await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE, "{rejected}");
}

#[tokio::test]
async fn attachment_upload_from_url_roundtrip() {
    let source = fake_attachment_source().await;
    let app = test_app().await;
    let token = login(&app, "alice").await;
    let (_, vault) = send(
        &app,
        "POST",
        "/api/vaults",
        Some(&token),
        Some(json!({"name": "Notes"})),
    )
    .await;
    let vault_id = vault["id"].as_str().unwrap();

    let from_url = format!("/api/vaults/{vault_id}/attachments/from-url");
    let (status, uploaded) = send(
        &app,
        "POST",
        &from_url,
        Some(&token),
        Some(json!({"sourceUrl": format!("{source}/image.png"), "path": "web/image.png"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(uploaded["path"], "web/image.png");
    assert_eq!(
        uploaded["size"],
        b"\x89PNG\r\n\x1a\nremote image".len() as i64
    );

    let url = format!("/api/vaults/{vault_id}/attachments/web/image.png");
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(&url)
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(bytes.as_ref(), b"\x89PNG\r\n\x1a\nremote image");
}

#[tokio::test]
async fn invite_is_single_use_and_grants_membership() {
    let app = test_app().await;
    let admin = login(&app, "alice").await;
    let bob = login(&app, "bob").await;
    let carol = login(&app, "carol").await;

    let (_, vault) = send(
        &app,
        "POST",
        "/api/vaults",
        Some(&admin),
        Some(json!({"name": "Shared"})),
    )
    .await;
    let vault_id = vault["id"].as_str().unwrap().to_string();

    // Non-admin cannot invite.
    let (status, _) = send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/invites"),
        Some(&bob),
        Some(json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    let (status, invite) = send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/invites"),
        Some(&admin),
        Some(json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let code = invite["code"].as_str().unwrap().to_string();
    assert_eq!(code.split('-').count(), 4);

    // Bob redeems successfully.
    let (status, redeem) = send(
        &app,
        "POST",
        "/api/invites/redeem",
        Some(&bob),
        Some(json!({"code": code})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(redeem["vaultId"], vault_id);

    // Carol cannot reuse the same single-use code.
    let (status, _) = send(
        &app,
        "POST",
        "/api/invites/redeem",
        Some(&carol),
        Some(json!({"code": code})),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
}

#[tokio::test]
async fn promote_member_to_admin() {
    let app = test_app().await;
    let admin = login(&app, "alice").await;
    let bob = login(&app, "bob").await;
    let bob_id = send(&app, "GET", "/api/me", Some(&bob), None).await.1["userId"]
        .as_str()
        .unwrap()
        .to_string();

    let (_, vault) = send(
        &app,
        "POST",
        "/api/vaults",
        Some(&admin),
        Some(json!({"name": "V"})),
    )
    .await;
    let vault_id = vault["id"].as_str().unwrap().to_string();
    let (_, invite) = send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/invites"),
        Some(&admin),
        Some(json!({})),
    )
    .await;
    let code = invite["code"].as_str().unwrap().to_string();
    send(
        &app,
        "POST",
        "/api/invites/redeem",
        Some(&bob),
        Some(json!({"code": code})),
    )
    .await;

    // Bob (member) can list members; promotion still succeeds.
    let (status, _) = send(
        &app,
        "GET",
        &format!("/api/vaults/{vault_id}/members"),
        Some(&bob),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, _) = send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/members/{bob_id}/promote"),
        Some(&admin),
        Some(json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, _) = send(
        &app,
        "GET",
        &format!("/api/vaults/{vault_id}/members"),
        Some(&bob),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn remove_member_permissions() {
    let app = test_app().await;
    let owner = login(&app, "owner").await;
    let admin = login(&app, "admin").await;
    let member = login(&app, "member").await;
    let outsider = login(&app, "outsider").await;

    let admin_id = send(&app, "GET", "/api/me", Some(&admin), None).await.1["userId"]
        .as_str()
        .unwrap()
        .to_string();
    let member_id = send(&app, "GET", "/api/me", Some(&member), None).await.1["userId"]
        .as_str()
        .unwrap()
        .to_string();
    let owner_id = send(&app, "GET", "/api/me", Some(&owner), None).await.1["userId"]
        .as_str()
        .unwrap()
        .to_string();

    let (_, vault) = send(
        &app,
        "POST",
        "/api/vaults",
        Some(&owner),
        Some(json!({"name": "V"})),
    )
    .await;
    let vault_id = vault["id"].as_str().unwrap().to_string();

    for token in [&admin, &member] {
        let (_, invite) = send(
            &app,
            "POST",
            &format!("/api/vaults/{vault_id}/invites"),
            Some(&owner),
            Some(json!({})),
        )
        .await;
        let code = invite["code"].as_str().unwrap().to_string();
        send(
            &app,
            "POST",
            "/api/invites/redeem",
            Some(token),
            Some(json!({"code": code})),
        )
        .await;
    }

    let (status, _) = send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/members/{admin_id}/promote"),
        Some(&owner),
        Some(json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, _) = send(
        &app,
        "DELETE",
        &format!("/api/vaults/{vault_id}/members/{admin_id}"),
        Some(&admin),
        None,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "admin cannot remove another admin"
    );

    let (status, _) = send(
        &app,
        "DELETE",
        &format!("/api/vaults/{vault_id}/members/{member_id}"),
        Some(&admin),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "admin can remove a member");

    let (status, _) = send(
        &app,
        "DELETE",
        &format!("/api/vaults/{vault_id}/members/{admin_id}"),
        Some(&owner),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "owner can remove an admin");

    let (status, _) = send(
        &app,
        "DELETE",
        &format!("/api/vaults/{vault_id}/members/{owner_id}"),
        Some(&owner),
        None,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "owner cannot remove themselves"
    );

    let (status, _) = send(
        &app,
        "DELETE",
        &format!("/api/vaults/{vault_id}/members/{owner_id}"),
        Some(&outsider),
        None,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "outsider cannot remove anyone"
    );
}

// ---------- blob store ----------

/// Send a request with a raw (non-JSON) body and return status + collected bytes.
async fn send_raw(
    app: &Router,
    method: &str,
    uri: &str,
    bearer: Option<&str>,
    body: Vec<u8>,
) -> (StatusCode, Vec<u8>) {
    let mut req = Request::builder().method(method).uri(uri);
    if let Some(b) = bearer {
        req = req.header(header::AUTHORIZATION, format!("Bearer {b}"));
    }
    let req = req.body(Body::from(body)).unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes().to_vec();
    (status, bytes)
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(bytes);
    h.finalize().iter().map(|b| format!("{b:02x}")).collect()
}

#[tokio::test]
async fn blob_put_head_get_roundtrip() {
    let (app, state) = test_app_with_state().await;
    let alice = login(&app, "alice").await;
    let bob = login(&app, "bob").await;
    let carol = login(&app, "carol").await;

    let (_, vault) = send(
        &app,
        "POST",
        "/api/vaults",
        Some(&alice),
        Some(json!({"name": "V"})),
    )
    .await;
    let vault_id = vault["id"].as_str().unwrap().to_string();

    let content = b"\x00\x01\x02 binary payload \xff\xfe".to_vec();
    let hash = sha256_hex(&content);
    let uri = format!("/api/vaults/{vault_id}/blobs/{hash}");

    // Missing before upload.
    let (status, _) = send_raw(&app, "HEAD", &uri, Some(&alice), vec![]).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // Upload.
    let (status, _) = send_raw(&app, "PUT", &uri, Some(&alice), content.clone()).await;
    assert_eq!(status, StatusCode::OK);

    // Present after upload.
    let (status, _) = send_raw(&app, "HEAD", &uri, Some(&alice), vec![]).await;
    assert_eq!(status, StatusCode::OK);

    // Download returns identical bytes.
    let (status, got) = send_raw(&app, "GET", &uri, Some(&alice), vec![]).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(got, content);

    // Idempotent re-upload.
    let (status, _) = send_raw(&app, "PUT", &uri, Some(&alice), content.clone()).await;
    assert_eq!(status, StatusCode::OK);

    // A non-member of the vault is refused.
    let (status, _) = send_raw(&app, "GET", &uri, Some(&bob), vec![]).await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // A member with any path-specific ACL cannot safely receive an unscoped
    // content-addressed blob: the route carries no path proving authorization.
    let (_, invite) = send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/invites"),
        Some(&alice),
        Some(json!({})),
    )
    .await;
    send(
        &app,
        "POST",
        "/api/invites/redeem",
        Some(&carol),
        Some(json!({"code": invite["code"]})),
    )
    .await;
    let carol_id = send(&app, "GET", "/api/me", Some(&carol), None).await.1["userId"]
        .as_str()
        .unwrap()
        .to_string();
    use realtime_server::entities::permissions;
    use sea_orm::{ActiveModelTrait, Set};
    permissions::ActiveModel {
        id: Set(uuid::Uuid::new_v4().to_string()),
        vault_id: Set(vault_id.clone()),
        principal_user_id: Set(Some(carol_id)),
        path_prefix: Set("private".to_string()),
        level: Set("read-only".to_string()),
    }
    .insert(&state.db)
    .await
    .unwrap();
    let (status, _) = send_raw(&app, "GET", &uri, Some(&carol), vec![]).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn path_scoped_blob_get_checks_the_index_entry() {
    let (app, state) = test_app_with_state().await;
    let alice = login(&app, "alice").await;
    let (_, vault) = send(
        &app,
        "POST",
        "/api/vaults",
        Some(&alice),
        Some(json!({"name": "V"})),
    )
    .await;
    let vault_id = vault["id"].as_str().unwrap().to_string();

    let image = b"png bytes".to_vec();
    let image_hash = sha256_hex(&image);
    let config = b"{\"theme\":\"dark\"}".to_vec();
    let config_hash = sha256_hex(&config);
    for (hash, bytes) in [(&image_hash, &image), (&config_hash, &config)] {
        let uri = format!("/api/vaults/{vault_id}/blobs/{hash}");
        let (status, _) = send_raw(&app, "PUT", &uri, Some(&alice), bytes.clone()).await;
        assert_eq!(status, StatusCode::OK);
    }
    state
        .documents
        .apply_update(
            &vault_id,
            &files_and_binaries_update(&[(
                "Attachments/pic.png".to_string(),
                image_hash.clone(),
                image.len() as i64,
            )]),
        )
        .await
        .unwrap();
    {
        use yrs::{Any, Map, ReadTxn, Transact};
        let doc = yrs::Doc::new();
        let configs = doc.get_or_insert_map("configFiles");
        {
            let mut txn = doc.transact_mut();
            let meta = HashMap::from([
                ("hash".to_string(), Any::String(config_hash.as_str().into())),
                ("size".to_string(), Any::Number(config.len() as f64)),
                ("mtime".to_string(), Any::Number(1.0)),
            ]);
            configs.insert(&mut txn, ".obsidian/appearance.json", Any::from(meta));
        }
        let update = doc
            .transact()
            .encode_state_as_update_v1(&yrs::StateVector::default());
        state
            .documents
            .apply_update(&vault_id, &update)
            .await
            .unwrap();
    }

    let get = |hash: &str, path: &str| format!("/api/vaults/{vault_id}/blobs/{hash}?path={path}");
    let (status, got) = send_raw(
        &app,
        "GET",
        &get(&image_hash, "Attachments/pic.png"),
        Some(&alice),
        vec![],
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(got, image);
    let (status, got) = send_raw(
        &app,
        "GET",
        &get(&config_hash, ".obsidian/appearance.json"),
        Some(&alice),
        vec![],
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(got, config);

    // The blob exists, but the path does not reference it.
    for (hash, path) in [
        (&image_hash, "Attachments/other.png"),
        (&config_hash, "Attachments/pic.png"),
        (&image_hash, ".obsidian/appearance.json"),
    ] {
        let (status, _) = send_raw(&app, "GET", &get(hash, path), Some(&alice), vec![]).await;
        assert_eq!(
            status,
            StatusCode::NOT_FOUND,
            "{path} must not serve {hash}"
        );
    }
}

#[tokio::test]
async fn blob_rejects_bad_hash_and_mismatch() {
    let app = test_app().await;
    let alice = login(&app, "alice").await;
    let (_, vault) = send(
        &app,
        "POST",
        "/api/vaults",
        Some(&alice),
        Some(json!({"name": "V"})),
    )
    .await;
    let vault_id = vault["id"].as_str().unwrap().to_string();

    // Non-hex / wrong-length hash is rejected before touching the filesystem.
    let bad = format!("/api/vaults/{vault_id}/blobs/not-a-valid-hash");
    let (status, _) = send_raw(&app, "PUT", &bad, Some(&alice), b"x".to_vec()).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // Well-formed hash that doesn't match the body is rejected.
    let wrong = format!("/api/vaults/{vault_id}/blobs/{}", "a".repeat(64));
    let (status, _) = send_raw(&app, "PUT", &wrong, Some(&alice), b"payload".to_vec()).await;
    assert_eq!(status, StatusCode::CONFLICT);
}

#[tokio::test]
async fn blob_requires_auth() {
    let app = test_app().await;
    let alice = login(&app, "alice").await;
    let (_, vault) = send(
        &app,
        "POST",
        "/api/vaults",
        Some(&alice),
        Some(json!({"name": "V"})),
    )
    .await;
    let vault_id = vault["id"].as_str().unwrap().to_string();
    let uri = format!("/api/vaults/{vault_id}/blobs/{}", "b".repeat(64));

    let (status, _) = send_raw(&app, "HEAD", &uri, None, vec![]).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

// ---------- git audit log ----------

#[tokio::test]
async fn git_audit_commits_attributed_to_principal() {
    let (state, git_dir, vault_id) = git_state().await;
    seed_git_note(&state, &vault_id).await;
    let repo = git_dir.join(&vault_id);

    // A write by Alice triggers a debounced commit materializing the vault tree.
    state
        .git
        .mark_write(
            &vault_id,
            &principal("u-alice", "Alice", "alice@example.com"),
        )
        .await;

    let log = wait_for_commit(&repo).await;
    assert_eq!(log, "Alice|alice@example.com|Add note.md", "author/subject");

    // Committer is pinned to the Realtime bot (not the server's git identity),
    // even though the author is the attributed user.
    let (_, committer) = git_out(&repo, &["log", "-1", "--format=%cn|%ce"]);
    assert_eq!(
        committer, "Realtime|realtime@localhost",
        "committer identity"
    );

    // The note's content was reconstructed from the native store at its vault path.
    let content = std::fs::read_to_string(repo.join("note.md")).unwrap();
    assert!(content.contains("# Note g1"), "got {content:?}");

    // Structured audit trailers are present and parseable.
    let (_, body) = git_out(&repo, &["log", "-1", "--format=%b"]);
    assert!(
        body.contains(&format!("Vault-Id: {vault_id}")),
        "trailers: {body}"
    );
    assert!(body.contains("Principal-Id: u-alice"), "trailers: {body}");

    // Idempotent: another write with no content change adds no commit.
    state
        .git
        .mark_write(
            &vault_id,
            &principal("u-alice", "Alice", "alice@example.com"),
        )
        .await;
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    let (_, count) = git_out(&repo, &["rev-list", "--count", "HEAD"]);
    assert_eq!(count, "1", "no-op write must not create a second commit");
}

#[tokio::test]
async fn git_audit_commits_attachments_inline_or_as_shim() {
    let small_bytes = b"small fake png".to_vec();
    let small_hash = sha256_hex(&small_bytes);
    let large_hash = "c".repeat(64);
    let large_size: i64 = 50 * 1024 * 1024; // over the 5 MB inline threshold

    let (state, git_dir, vault_id) = git_state().await;
    state
        .documents
        .apply_update(
            &vault_id,
            &files_and_binaries_update(&[
                (
                    "img/small.png".to_string(),
                    small_hash.clone(),
                    small_bytes.len() as i64,
                ),
                ("img/large.pdf".to_string(), large_hash.clone(), large_size),
            ]),
        )
        .await
        .unwrap();
    state
        .documents
        .apply_update(
            &format!("{vault_id}__g1"),
            &text_update("contents", "# Note g1\n"),
        )
        .await
        .unwrap();
    let repo = git_dir.join(&vault_id);

    // Seed the small attachment's bytes in the blob store; the large one's
    // bytes are irrelevant (only its shim is committed).
    let blob_vault_dir = std::path::PathBuf::from(&state.config.blob_dir).join(&vault_id);
    std::fs::create_dir_all(&blob_vault_dir).unwrap();
    std::fs::write(blob_vault_dir.join(&small_hash), &small_bytes).unwrap();

    state
        .git
        .mark_write(
            &vault_id,
            &principal("u-alice", "Alice", "alice@example.com"),
        )
        .await;
    let log = wait_for_commit(&repo).await;
    assert!(log.contains("Alice|alice@example.com|Add"), "log: {log}");

    // Small attachment: committed verbatim.
    assert_eq!(
        std::fs::read(repo.join("img/small.png")).unwrap(),
        small_bytes
    );

    // Large attachment: committed as a text shim pointing at the blob API.
    let shim = std::fs::read_to_string(repo.join("img/large.pdf")).unwrap();
    let lines: Vec<&str> = shim.lines().collect();
    assert_eq!(
        lines,
        vec![
            "version https://realtime.md/attachment-shim/v1".to_string(),
            format!("oid sha256:{large_hash}"),
            format!("size {large_size}"),
            format!("vault {vault_id}"),
            format!("url http://auth.test/api/vaults/{vault_id}/blobs/{large_hash}"),
        ]
    );

    // The attachments show up in the commit alongside the note.
    let (_, names) = git_out(&repo, &["show", "--name-only", "--format=", "HEAD"]);
    assert!(names.contains("img/small.png"), "files: {names}");
    assert!(names.contains("img/large.pdf"), "files: {names}");
    assert!(names.contains("note.md"), "files: {names}");
}

// ---------- git backup ----------

/// Insert a backup config row directly (tests drive the push path itself with
/// a credential-less `file://` remote; the route layer is covered separately).
async fn insert_backup_row(db: &sea_orm::DatabaseConnection, vault_id: &str, remote_url: &str) {
    use realtime_server::entities::git_backups;
    use sea_orm::{ActiveModelTrait, Set};
    git_backups::ActiveModel {
        vault_id: Set(vault_id.to_string()),
        remote_url: Set(remote_url.to_string()),
        auth_method: Set("none".to_string()),
        branch: Set("main".to_string()),
        ssh_private_key: Set(None),
        ssh_public_key: Set(None),
        https_token: Set(None),
        enabled: Set(true),
        last_push_at: Set(None),
        last_push_error: Set(None),
        created_by: Set("u-test".to_string()),
        created_at: Set(0),
        updated_at: Set(0),
    }
    .insert(db)
    .await
    .unwrap();
}

async fn backup_row(
    db: &sea_orm::DatabaseConnection,
    vault_id: &str,
) -> realtime_server::entities::git_backups::Model {
    use sea_orm::EntityTrait;
    realtime_server::entities::git_backups::Entity::find_by_id(vault_id.to_string())
        .one(db)
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn git_backup_pushes_to_remote_after_commit() {
    let (state, git_dir, vault_id) = git_state().await;
    seed_git_note(&state, &vault_id).await;

    // A bare repo standing in for the remote.
    let bare = git_dir.join("remote.git");
    std::fs::create_dir_all(&bare).unwrap();
    let (ok, _) = git_out(&bare, &["init", "--bare", "-q"]);
    assert!(ok, "init bare remote");
    insert_backup_row(&state.db, &vault_id, &format!("file://{}", bare.display())).await;

    state
        .git
        .mark_write(&vault_id, &principal("u-a", "Alice", "a@x"))
        .await;
    wait_for_commit(&git_dir.join(&vault_id)).await;

    // The push happens right after the commit; poll for the remote branch.
    let mut pushed = String::new();
    for _ in 0..100 {
        let (ok, head) = git_out(&bare, &["rev-parse", "refs/heads/main"]);
        if ok && !head.is_empty() {
            pushed = head;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let (_, local) = git_out(&git_dir.join(&vault_id), &["rev-parse", "HEAD"]);
    assert_eq!(pushed, local, "remote main should match local HEAD");

    // The status row is written just after the push lands on the remote.
    let mut row = backup_row(&state.db, &vault_id).await;
    for _ in 0..100 {
        if row.last_push_at.is_some() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        row = backup_row(&state.db, &vault_id).await;
    }
    assert!(row.last_push_at.is_some(), "last_push_at recorded");
    assert_eq!(row.last_push_error, None);
}

#[tokio::test]
async fn git_backup_push_failure_recorded_without_breaking_commits() {
    let (state, git_dir, vault_id) = git_state().await;
    seed_git_note(&state, &vault_id).await;
    insert_backup_row(
        &state.db,
        &vault_id,
        &format!("file://{}/does-not-exist.git", git_dir.display()),
    )
    .await;

    state
        .git
        .mark_write(&vault_id, &principal("u-a", "Alice", "a@x"))
        .await;
    // The commit must land even though the push fails.
    wait_for_commit(&git_dir.join(&vault_id)).await;

    let mut row = backup_row(&state.db, &vault_id).await;
    for _ in 0..100 {
        if row.last_push_error.is_some() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        row = backup_row(&state.db, &vault_id).await;
    }
    assert!(row.last_push_error.is_some(), "push failure recorded");
    assert_eq!(row.last_push_at, None);
}

#[tokio::test]
async fn git_backup_routes_admin_only_and_secrets_never_leak() {
    let app = test_app().await;
    let alice = login(&app, "alice").await;
    let bob = login(&app, "bob").await;

    let (_, vault) = send(
        &app,
        "POST",
        "/api/vaults",
        Some(&alice),
        Some(json!({"name": "V"})),
    )
    .await;
    let vault_id = vault["id"].as_str().unwrap().to_string();
    let uri = format!("/api/vaults/{vault_id}/backup");

    // Non-member is refused.
    let (status, _) = send(&app, "GET", &uri, Some(&bob), None).await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // Unconfigured: GET reports configured=false; test endpoint 404s.
    let (status, body) = send(&app, "GET", &uri, Some(&alice), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["configured"], json!(false));
    let (status, _) = send(&app, "POST", &format!("{uri}/test"), Some(&alice), None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // HTTPS config requires a token.
    let (status, _) = send(
        &app,
        "PUT",
        &uri,
        Some(&alice),
        Some(json!({"remoteUrl": "https://example.com/r.git", "authMethod": "https", "enabled": true})),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // URL scheme must match the auth method.
    let (status, _) = send(
        &app,
        "PUT",
        &uri,
        Some(&alice),
        Some(
            json!({"remoteUrl": "https://example.com/r.git", "authMethod": "ssh", "enabled": true}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // SSH config generates a deploy key and returns only the public half.
    let (status, body) = send(
        &app,
        "PUT",
        &uri,
        Some(&alice),
        Some(
            json!({"remoteUrl": "git@example.com:me/r.git", "authMethod": "ssh", "enabled": true}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let public_key = body["sshPublicKey"].as_str().unwrap().to_string();
    assert!(public_key.starts_with("ssh-ed25519 "), "{public_key}");
    let serialized = body.to_string();
    assert!(!serialized.contains("PRIVATE KEY"), "private key leaked");

    // Updating without regenerateKey keeps the same public key.
    let (_, body) = send(
        &app,
        "PUT",
        &uri,
        Some(&alice),
        Some(json!({"remoteUrl": "git@example.com:me/r2.git", "authMethod": "ssh", "enabled": false})),
    )
    .await;
    assert_eq!(body["sshPublicKey"].as_str().unwrap(), public_key);

    // ... while regenerateKey mints a new one.
    let (_, body) = send(
        &app,
        "PUT",
        &uri,
        Some(&alice),
        Some(json!({"remoteUrl": "git@example.com:me/r2.git", "authMethod": "ssh", "enabled": false, "regenerateKey": true})),
    )
    .await;
    assert_ne!(body["sshPublicKey"].as_str().unwrap(), public_key);

    // Switching to HTTPS stores the token but never echoes it back.
    let (status, body) = send(
        &app,
        "PUT",
        &uri,
        Some(&alice),
        Some(json!({"remoteUrl": "https://example.com/r.git", "authMethod": "https", "httpsToken": "sekrit-token", "enabled": true})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["hasHttpsToken"], json!(true));
    assert!(!body.to_string().contains("sekrit-token"), "token leaked");

    // Omitting the token on update keeps the stored one.
    let (status, body) = send(
        &app,
        "PUT",
        &uri,
        Some(&alice),
        Some(json!({"remoteUrl": "https://example.com/r.git", "authMethod": "https", "enabled": false})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["hasHttpsToken"], json!(true));

    // Delete, then GET reports unconfigured again.
    let (status, _) = send(&app, "DELETE", &uri, Some(&alice), None).await;
    assert_eq!(status, StatusCode::OK);
    let (_, body) = send(&app, "GET", &uri, Some(&alice), None).await;
    assert_eq!(body["configured"], json!(false));
}

#[tokio::test]
async fn doc_token_scopes_and_mints() {
    let public = "http://public.example:9999";
    let (app, state) = test_app_with_public_url(public).await;
    let alice = login(&app, "alice").await;
    let bob = login(&app, "bob").await;

    let (_, vault) = send(
        &app,
        "POST",
        "/api/vaults",
        Some(&alice),
        Some(json!({"name": "V"})),
    )
    .await;
    let vault_id = vault["id"].as_str().unwrap().to_string();

    // Non-member is refused.
    let (status, _) = send(
        &app,
        "POST",
        "/api/doc-token",
        Some(&bob),
        Some(json!({"vaultId": vault_id, "docId": vault_id})),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // A docId outside the vault namespace is refused.
    let (status, _) = send(
        &app,
        "POST",
        "/api/doc-token",
        Some(&alice),
        Some(json!({"vaultId": vault_id, "docId": "someoneelse__abc"})),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // Member gets a document-scoped token for the configured public URL.
    let document_id = format!("{vault_id}__deadbeef");
    let (status, token) = send(
        &app,
        "POST",
        "/api/doc-token",
        Some(&alice),
        Some(json!({"vaultId": vault_id, "docId": document_id})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        token["url"]
            .as_str()
            .unwrap()
            .contains("public.example:9999"),
        "host should be rewritten to public url, got {}",
        token["url"]
    );
    assert_eq!(token["epoch"].as_u64(), Some(0));
    let opaque = token["token"].as_str().unwrap();
    assert_eq!(
        state
            .sync_grant(opaque, &document_id)
            .await
            .map(|grant| grant.level),
        Some(realtime_server::crdt::Level::Full)
    );
    assert!(
        state
            .sync_grant(opaque, &format!("{vault_id}__other"))
            .await
            .is_none(),
        "a token must not authorize another document"
    );
    assert!(
        state
            .sync_grant(&format!("{opaque}x"), &document_id)
            .await
            .is_none(),
        "a modified opaque token must be rejected"
    );

    let update = text_update("contents", "native route");
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/d/{document_id}/update"))
                .header(header::AUTHORIZATION, format!("Bearer {opaque}"))
                .body(Body::from(update))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let persisted = state.documents.read_update(&document_id).await.unwrap();
    assert_eq!(
        realtime_server::ydoc::decode_text(&persisted, "contents").unwrap(),
        "native route"
    );

    // Activating a replacement preserves logical content but invalidates every
    // token bound to the retired physical epoch.
    assert_eq!(
        state
            .documents
            .begin_epoch_transition(&document_id)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        realtime_server::ydoc::decode_text(
            &state.documents.read_update(&document_id).await.unwrap(),
            "contents"
        )
        .unwrap(),
        "native route"
    );
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/d/{document_id}/update"))
                .header(header::AUTHORIZATION, format!("Bearer {opaque}"))
                .body(Body::from(text_update("contents", "stale")))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/d/{document_id}/as-update"))
                .header(header::AUTHORIZATION, format!("Bearer {opaque}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);

    let (status, replacement_token) = send(
        &app,
        "POST",
        "/api/doc-token",
        Some(&alice),
        Some(json!({"vaultId": vault_id, "docId": document_id})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(replacement_token["epoch"].as_u64(), Some(1));

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/d/{vault_id}__other/update"))
                .header(header::AUTHORIZATION, format!("Bearer {opaque}"))
                .body(Body::from(text_update("contents", "forbidden")))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    // Once a member has path ACLs, both token minting and registry rewrites
    // must authorize the registered/requested paths rather than falling back
    // to vault-level membership.
    let (_, invite) = send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/invites"),
        Some(&alice),
        Some(json!({})),
    )
    .await;
    send(
        &app,
        "POST",
        "/api/invites/redeem",
        Some(&bob),
        Some(json!({"code": invite["code"]})),
    )
    .await;
    let bob_id = send(&app, "GET", "/api/me", Some(&bob), None).await.1["userId"]
        .as_str()
        .unwrap()
        .to_string();
    send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/files"),
        Some(&alice),
        Some(json!({"guid": "secret-guid", "path": "secret.md"})),
    )
    .await;
    use realtime_server::entities::permissions;
    use sea_orm::{ActiveModelTrait, Set};
    permissions::ActiveModel {
        id: Set(uuid::Uuid::new_v4().to_string()),
        vault_id: Set(vault_id.clone()),
        principal_user_id: Set(Some(bob_id)),
        path_prefix: Set("secret.md".to_string()),
        level: Set("deny".to_string()),
    }
    .insert(&state.db)
    .await
    .unwrap();

    let (status, _) = send(
        &app,
        "POST",
        "/api/doc-token",
        Some(&bob),
        Some(json!({
            "vaultId": vault_id,
            "docId": format!("{vault_id}__secret-guid")
        })),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    let (status, _) = send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/files"),
        Some(&bob),
        Some(json!({"guid": "secret-guid", "path": "allowed.md"})),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (status, _) = send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/files"),
        Some(&bob),
        Some(json!({"guid": "new-guid", "path": "secret.md"})),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn doc_token_bounds_unregistered_document_reservations() {
    let mut config = test_config("http://auth.test", realtime_server::blobs::MAX_BLOB_BYTES);
    config.crdt_max_documents_per_vault = 2;
    let app = app_from_config(&config).await;
    let alice = login(&app, "alice").await;
    let (_, vault) = send(
        &app,
        "POST",
        "/api/vaults",
        Some(&alice),
        Some(json!({"name": "V"})),
    )
    .await;
    let vault_id = vault["id"].as_str().unwrap();

    for index in 0..2 {
        let (status, _) = send(
            &app,
            "POST",
            "/api/doc-token",
            Some(&alice),
            Some(json!({
                "vaultId": vault_id,
                "docId": format!("{vault_id}__pending-{index}"),
                "path": format!("pending-{index}.md")
            })),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let (status, _) = send(
            &app,
            "POST",
            &format!("/api/vaults/{vault_id}/files"),
            Some(&alice),
            Some(json!({
                "guid": format!("pending-{index}"),
                "path": format!("pending-{index}.md")
            })),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
    }
    let (status, _) = send(
        &app,
        "POST",
        "/api/doc-token",
        Some(&alice),
        Some(json!({
            "vaultId": vault_id,
            "docId": format!("{vault_id}__pending-overflow"),
            "path": "overflow.md"
        })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn doc_token_reconnect_honors_only_matching_live_creation_reservation() {
    let (app, state) = test_app_with_state().await;
    let alice = login(&app, "alice").await;
    let bob = login(&app, "bob").await;
    let (_, vault) = send(
        &app,
        "POST",
        "/api/vaults",
        Some(&alice),
        Some(json!({"name": "V"})),
    )
    .await;
    let vault_id = vault["id"].as_str().unwrap().to_string();
    let (_, invite) = send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/invites"),
        Some(&alice),
        Some(json!({})),
    )
    .await;
    send(
        &app,
        "POST",
        "/api/invites/redeem",
        Some(&bob),
        Some(json!({"code": invite["code"]})),
    )
    .await;
    let bob_id = send(&app, "GET", "/api/me", Some(&bob), None).await.1["userId"]
        .as_str()
        .unwrap()
        .to_string();
    use realtime_server::entities::permissions;
    use sea_orm::{ActiveModelTrait, Set};
    permissions::ActiveModel {
        id: Set(uuid::Uuid::new_v4().to_string()),
        vault_id: Set(vault_id.clone()),
        principal_user_id: Set(Some(bob_id)),
        path_prefix: Set("private".to_string()),
        level: Set("deny".to_string()),
    }
    .insert(&state.db)
    .await
    .unwrap();

    let document_id = format!("{vault_id}__race");
    let (status, _) = send(
        &app,
        "POST",
        "/api/doc-token",
        Some(&bob),
        Some(json!({
            "vaultId": vault_id,
            "docId": document_id,
            "path": "shared/race.md"
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // The first token request has physically created the document, but its
    // registry entry has not arrived yet. The same creator can reconnect using
    // the reservation's path even though their vault policy is not uniform.
    let (status, _) = send(
        &app,
        "POST",
        "/api/doc-token",
        Some(&bob),
        Some(json!({
            "vaultId": vault_id,
            "docId": document_id
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, _) = send(
        &app,
        "POST",
        "/api/doc-token",
        Some(&alice),
        Some(json!({
            "vaultId": vault_id,
            "docId": document_id,
            "path": "shared/race.md"
        })),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (status, _) = send(
        &app,
        "POST",
        "/api/doc-token",
        Some(&bob),
        Some(json!({
            "vaultId": vault_id,
            "docId": document_id,
            "path": "shared/other.md"
        })),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    state
        .pending_document_creations
        .lock()
        .await
        .get_mut(&document_id)
        .unwrap()
        .expires_at_ms = 0;
    let (status, _) = send(
        &app,
        "POST",
        "/api/doc-token",
        Some(&bob),
        Some(json!({
            "vaultId": vault_id,
            "docId": document_id,
            "path": "shared/race.md"
        })),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    let legacy_document_id = format!("{vault_id}__legacy");
    state
        .documents
        .ensure_document(&legacy_document_id)
        .await
        .unwrap();
    let (status, _) = send(
        &app,
        "POST",
        "/api/doc-token",
        Some(&bob),
        Some(json!({
            "vaultId": vault_id,
            "docId": legacy_document_id,
            "path": "shared/legacy.md"
        })),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn pathless_creation_reservation_can_bind_once_to_a_path() {
    let (app, state) = test_app_with_state().await;
    let alice = login(&app, "alice").await;
    let (_, vault) = send(
        &app,
        "POST",
        "/api/vaults",
        Some(&alice),
        Some(json!({"name": "V"})),
    )
    .await;
    let vault_id = vault["id"].as_str().unwrap().to_string();
    let document_id = format!("{vault_id}__pathless-race");

    let (status, _) = send(
        &app,
        "POST",
        "/api/doc-token",
        Some(&alice),
        Some(json!({"vaultId": vault_id, "docId": document_id})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, _) = send(
        &app,
        "POST",
        "/api/doc-token",
        Some(&alice),
        Some(json!({
            "vaultId": vault_id,
            "docId": document_id,
            "path": "bound.md"
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        state
            .pending_document_creations
            .lock()
            .await
            .get(&document_id)
            .unwrap()
            .path,
        "bound.md"
    );

    let (status, _) = send(
        &app,
        "POST",
        "/api/doc-token",
        Some(&alice),
        Some(json!({
            "vaultId": vault_id,
            "docId": document_id,
            "path": "conflict.md"
        })),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn preregistered_guid_cannot_bypass_physical_document_quota() {
    let mut config = test_config("http://auth.test", realtime_server::blobs::MAX_BLOB_BYTES);
    config.crdt_max_documents_per_vault = 1;
    let app = app_from_config(&config).await;
    let alice = login(&app, "alice").await;
    let (_, vault) = send(
        &app,
        "POST",
        "/api/vaults",
        Some(&alice),
        Some(json!({"name": "V"})),
    )
    .await;
    let vault_id = vault["id"].as_str().unwrap();

    let (status, _) = send(
        &app,
        "POST",
        "/api/doc-token",
        Some(&alice),
        Some(json!({
            "vaultId": vault_id,
            "docId": format!("{vault_id}__first"),
            "path": "first.md"
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/files"),
        Some(&alice),
        Some(json!({"guid": "preregistered", "path": "preregistered.md"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send(
        &app,
        "POST",
        "/api/doc-token",
        Some(&alice),
        Some(json!({
            "vaultId": vault_id,
            "docId": format!("{vault_id}__preregistered")
        })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // Minting another token for the already-physical creator document remains
    // allowed even before its index registration is published.
    let (status, _) = send(
        &app,
        "POST",
        "/api/doc-token",
        Some(&alice),
        Some(json!({
            "vaultId": vault_id,
            "docId": format!("{vault_id}__first")
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn content_creation_endpoints_share_physical_document_quota() {
    let mut config = test_config("http://auth.test", realtime_server::blobs::MAX_BLOB_BYTES);
    config.crdt_max_documents_per_vault = 1;
    let app = app_from_config(&config).await;
    let alice = login(&app, "alice").await;
    let (_, vault) = send(
        &app,
        "POST",
        "/api/vaults",
        Some(&alice),
        Some(json!({"name": "V"})),
    )
    .await;
    let vault_id = vault["id"].as_str().unwrap();

    let (status, _) = send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/notes"),
        Some(&alice),
        Some(json!({"path": "first.md", "content": "first"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, _) = send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/canvases"),
        Some(&alice),
        Some(json!({"path": "second.canvas", "value": {"nodes": [], "edges": []}})),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let (status, _) = send(
        &app,
        "GET",
        &format!("/api/vaults/{vault_id}/notes/first.md"),
        Some(&alice),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn search_tags_backlinks_reindex_and_rename_rewrite() {
    let app = test_app().await;
    let token = login(&app, "alice").await;
    let (_, vault) = send(
        &app,
        "POST",
        "/api/vaults",
        Some(&token),
        Some(json!({"name": "Notes"})),
    )
    .await;
    let vault_id = vault["id"].as_str().unwrap();
    let notes_url = format!("/api/vaults/{vault_id}/notes");

    // alpha links to beta and carries frontmatter + inline tags.
    let (status, _) = send(
        &app,
        "POST",
        &notes_url,
        Some(&token),
        Some(json!({
            "path": "alpha.md",
            "content": "---\ntitle: Alpha Note\ntags:\n  - project\n  - rust\n---\nSee [[beta]] for details. #journal"
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, _) = send(
        &app,
        "POST",
        &notes_url,
        Some(&token),
        Some(json!({"path": "beta.md", "content": "# Beta\nunique xyzzy content"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // FTS path (>= 3 chars): body-only term finds beta.
    let (status, hits) = send(
        &app,
        "GET",
        &format!("/api/vaults/{vault_id}/search?q=xyzzy"),
        Some(&token),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let hits = hits.as_array().unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0]["path"], "beta.md");

    // LIKE fallback path (< 3 chars): matches alpha's path substring.
    let (status, hits) = send(
        &app,
        "GET",
        &format!("/api/vaults/{vault_id}/search?q=al"),
        Some(&token),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let paths: Vec<&str> = hits
        .as_array()
        .unwrap()
        .iter()
        .map(|h| h["path"].as_str().unwrap())
        .collect();
    assert!(paths.contains(&"alpha.md"), "fallback hits: {paths:?}");

    // Tags aggregation.
    let (status, tags) = send(
        &app,
        "GET",
        &format!("/api/vaults/{vault_id}/tags"),
        Some(&token),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let tag_names: Vec<&str> = tags
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["tag"].as_str().unwrap())
        .collect();
    assert!(tag_names.contains(&"project"));
    assert!(tag_names.contains(&"rust"));
    assert!(tag_names.contains(&"journal"));

    // Backlinks: alpha links to beta.
    let (status, backlinks) = send(
        &app,
        "GET",
        &format!("/api/vaults/{vault_id}/backlinks/beta.md"),
        Some(&token),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let backlinks = backlinks.as_array().unwrap();
    assert_eq!(backlinks.len(), 1);
    assert_eq!(backlinks[0]["path"], "alpha.md");

    // Reindex rebuilds from the authoritative CRDT (2 notes).
    let (status, reindexed) = send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/reindex"),
        Some(&token),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(reindexed["count"], 2);

    // Rename beta -> gamma and assert alpha's backlink was rewritten.
    let (status, _) = send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/note-moves/beta.md"),
        Some(&token),
        Some(json!({"toPath": "gamma.md"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, alpha) = send(
        &app,
        "GET",
        &format!("/api/vaults/{vault_id}/notes/alpha.md"),
        Some(&token),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        alpha["content"].as_str().unwrap().contains("[[gamma]]"),
        "expected rewritten link, got {}",
        alpha["content"]
    );
}

#[tokio::test]
async fn search_refreshes_one_modified_note_without_waiting_for_git_debounce() {
    use sea_orm::{ConnectionTrait, Statement};

    let (state, _git_dir, vault_id) = git_state_ext_with_debounce(None, 5_000).await;
    state
        .documents
        .apply_update(
            &vault_id,
            &files_update(&[("one.md", "g1"), ("two.md", "g2")]),
        )
        .await
        .unwrap();
    state
        .documents
        .apply_update(
            &format!("{vault_id}__g1"),
            &text_update("contents", "old search content"),
        )
        .await
        .unwrap();
    state
        .documents
        .apply_update(
            &format!("{vault_id}__g2"),
            &text_update("contents", "unchanged content"),
        )
        .await
        .unwrap();
    realtime_server::search::reindex_vault(&state, &vault_id)
        .await
        .unwrap();
    let backend = state.db.get_database_backend();
    let unchanged_updated_at = state
        .db
        .query_one(Statement::from_sql_and_values(
            backend,
            "SELECT updated_at FROM note_search WHERE vault_id = ? AND guid = ?",
            [
                sea_orm::Value::from(vault_id.as_str()),
                sea_orm::Value::from("g2"),
            ],
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get::<i64>("", "updated_at")
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;

    realtime_server::ydoc::set_text(&state, &format!("{vault_id}__g1"), "new searchable content")
        .await
        .unwrap();
    let started = std::time::Instant::now();
    state.search.mark_note_write(&vault_id, "g1").await;

    let mut updated = false;
    for _ in 0..40 {
        let row = state
            .db
            .query_one(Statement::from_sql_and_values(
                backend,
                "SELECT body FROM note_fts WHERE vault_id = ? AND guid = ?",
                [
                    sea_orm::Value::from(vault_id.as_str()),
                    sea_orm::Value::from("g1"),
                ],
            ))
            .await
            .unwrap();
        if let Some(row) = row {
            if row.try_get::<String>("", "body").unwrap() == "new searchable content" {
                updated = true;
                break;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(updated, "search index did not refresh within two seconds");
    assert!(
        started.elapsed() < std::time::Duration::from_secs(2),
        "search refresh followed the five-second git debounce"
    );
    let still_unchanged = state
        .db
        .query_one(Statement::from_sql_and_values(
            backend,
            "SELECT updated_at FROM note_search WHERE vault_id = ? AND guid = ?",
            [
                sea_orm::Value::from(vault_id.as_str()),
                sea_orm::Value::from("g2"),
            ],
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get::<i64>("", "updated_at")
        .unwrap();
    assert_eq!(still_unchanged, unchanged_updated_at);
}

#[tokio::test]
async fn note_move_rewrites_pathed_links_preserving_paths() {
    let app = test_app().await;
    let token = login(&app, "alice").await;
    let (_, vault) = send(
        &app,
        "POST",
        "/api/vaults",
        Some(&token),
        Some(json!({"name": "Notes"})),
    )
    .await;
    let vault_id = vault["id"].as_str().unwrap();
    let notes_url = format!("/api/vaults/{vault_id}/notes");

    let (status, _) = send(
        &app,
        "POST",
        &notes_url,
        Some(&token),
        Some(json!({"path": "a.md", "content": "[[dir/Old.md]] [x](dir/Old.md)"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send(
        &app,
        "POST",
        &notes_url,
        Some(&token),
        Some(json!({"path": "dir/Old.md", "content": "# Old"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, _) = send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/note-moves/dir/Old.md"),
        Some(&token),
        Some(json!({"toPath": "new/New.md"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, a) = send(
        &app,
        "GET",
        &format!("/api/vaults/{vault_id}/notes/a.md"),
        Some(&token),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(a["content"], "[[new/New.md]] [x](new/New.md)");
}

#[tokio::test]
async fn create_note_after_client_side_delete_succeeds() {
    // Reproduces the create/list divergence: a client deletes a file by removing
    // it from the index doc CRDT (the common path — clients never call the
    // server delete API), leaving an orphan `vault_files` row. `list_notes`
    // reconciles and drops the ghost; `create_note` must do the same instead of
    // rejecting the path as "already exists" on the stale row.
    use yrs::updates::decoder::Decode;
    use yrs::{Map, ReadTxn, Transact};

    let (app, state) = test_app_with_state().await;
    let token = login(&app, "alice").await;
    let (_, vault) = send(
        &app,
        "POST",
        "/api/vaults",
        Some(&token),
        Some(json!({"name": "V"})),
    )
    .await;
    let vault_id = vault["id"].as_str().unwrap().to_string();
    let notes_url = format!("/api/vaults/{vault_id}/notes");

    // Create the note, then simulate a client-side delete: remove the path from
    // the index doc CRDT directly, leaving the `vault_files` row orphaned.
    let (status, created) = send(
        &app,
        "POST",
        &notes_url,
        Some(&token),
        Some(json!({"path": "ghost.md", "content": "boo"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{created}");
    let first_guid = created["guid"].as_str().unwrap().to_string();

    {
        let raw = state.documents.read_update(&vault_id).await.unwrap();
        let doc = yrs::Doc::new();
        let files = doc.get_or_insert_map("files");
        let mut txn = doc.transact_mut();
        txn.apply_update(yrs::Update::decode_v1(&raw).unwrap());
        files.remove(&mut txn, "ghost.md");
        drop(txn);
        let new_update = doc
            .transact()
            .encode_state_as_update_v1(&yrs::StateVector::default());
        state
            .documents
            .apply_update(&vault_id, &new_update)
            .await
            .unwrap();
    }

    // create_note must agree with list_notes and accept the freed path. Test it
    // BEFORE any list call — list_notes reconciles and would prune the orphan
    // row as a side effect, masking the create-guard divergence.
    let (status, recreated) = send(
        &app,
        "POST",
        &notes_url,
        Some(&token),
        Some(json!({"path": "ghost.md", "content": "boo 2"})),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "recreate should succeed, got {recreated}"
    );
    assert_ne!(
        recreated["guid"].as_str().unwrap(),
        first_guid,
        "recreate must mint a new guid, not resurrect the deleted one"
    );

    // list_notes reconciles against the index doc and must not show the ghost
    // (the recreated note is a different guid, so the old one stays gone).
    let (status, list) = send(&app, "GET", &notes_url, Some(&token), None).await;
    assert_eq!(status, StatusCode::OK, "{list}");
    let paths: Vec<&str> = list
        .as_array()
        .unwrap()
        .iter()
        .map(|n| n["path"].as_str().unwrap())
        .collect();
    assert_eq!(paths.iter().filter(|p| **p == "ghost.md").count(), 1);
}

#[tokio::test]
async fn attachment_move_update_embeds_rewrites_opt_in_only() {
    let app = test_app().await;
    let token = login(&app, "alice").await;
    let (_, vault) = send(
        &app,
        "POST",
        "/api/vaults",
        Some(&token),
        Some(json!({"name": "Notes"})),
    )
    .await;
    let vault_id = vault["id"].as_str().unwrap();

    let (status, _) = send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/notes"),
        Some(&token),
        Some(json!({"path": "true.md", "content": "![[img.png]] ![[old/img.png]]"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send_raw(
        &app,
        "PUT",
        &format!("/api/vaults/{vault_id}/attachments/old/img.png"),
        Some(&token),
        b"\x89PNG\r\n\x1a\nimage bytes".to_vec(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/attachment-moves/old/img.png"),
        Some(&token),
        Some(json!({"toPath": "new/img.png", "updateEmbeds": true})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, note) = send(
        &app,
        "GET",
        &format!("/api/vaults/{vault_id}/notes/true.md"),
        Some(&token),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(note["content"], "![[img.png]] ![[new/img.png]]");

    let (status, _) = send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/notes"),
        Some(&token),
        Some(json!({"path": "false.md", "content": "![[pic.png]] ![[old2/pic.png]]"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send_raw(
        &app,
        "PUT",
        &format!("/api/vaults/{vault_id}/attachments/old2/pic.png"),
        Some(&token),
        b"\x89PNG\r\n\x1a\nimage bytes".to_vec(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/attachment-moves/old2/pic.png"),
        Some(&token),
        Some(json!({"toPath": "new2/pic.png", "updateEmbeds": false})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, note) = send(
        &app,
        "GET",
        &format!("/api/vaults/{vault_id}/notes/false.md"),
        Some(&token),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(note["content"], "![[pic.png]] ![[old2/pic.png]]");
}

#[tokio::test]
async fn canvas_move_update_embeds_rewrites_note_references() {
    let app = test_app().await;
    let token = login(&app, "alice").await;
    let (_, vault) = send(
        &app,
        "POST",
        "/api/vaults",
        Some(&token),
        Some(json!({"name": "Notes"})),
    )
    .await;
    let vault_id = vault["id"].as_str().unwrap();

    let (status, _) = send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/canvases"),
        Some(&token),
        Some(json!({"path": "Board.canvas", "value": {"nodes": [], "edges": []}})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/notes"),
        Some(&token),
        Some(json!({"path": "a.md", "content": "![[Board.canvas]]"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, _) = send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/canvas-moves/Board.canvas"),
        Some(&token),
        Some(json!({"toPath": "Archived/Board.canvas", "updateEmbeds": true})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, note) = send(
        &app,
        "GET",
        &format!("/api/vaults/{vault_id}/notes/a.md"),
        Some(&token),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(note["content"], "![[Archived/Board.canvas]]");
}

#[tokio::test]
async fn canvas_operation_batches_are_atomic_authorized_and_idempotent() {
    let app = test_app().await;
    let token = login(&app, "alice").await;
    let (_, vault) = send(
        &app,
        "POST",
        "/api/vaults",
        Some(&token),
        Some(json!({"name": "Canvas operations"})),
    )
    .await;
    let vault_id = vault["id"].as_str().unwrap();
    let route = format!("/api/vaults/{vault_id}/canvas-operations/Board.canvas");
    let (_, _) = send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/canvases"),
        Some(&token),
        Some(json!({
            "path": "Board.canvas",
            "value": {
                "nodes": [{"id": "a", "type": "text", "text": "before", "x": 0, "y": 0}],
                "edges": []
            }
        })),
    )
    .await;

    let (status, _) = send(
        &app,
        "POST",
        &route,
        None,
        Some(json!({"operations": [{"type": "node-delete", "id": "a"}]})),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    let (status, _) = send(
        &app,
        "POST",
        &route,
        Some(&token),
        Some(json!({
            "operations": [
                {"type": "node-patch", "id": "a", "patch": {"set": {"text": "must roll back"}}},
                {"type": "edge-create", "edge": {"id": "bad", "fromNode": "a", "toNode": "missing"}}
            ]
        })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (_, unchanged) = send(
        &app,
        "GET",
        &format!("/api/vaults/{vault_id}/canvas/Board.canvas"),
        Some(&token),
        None,
    )
    .await;
    assert_eq!(unchanged["value"]["nodes"][0]["text"], "before");
    assert_eq!(unchanged["value"]["edges"], json!([]));

    let batch = json!({
        "mutationId": "retry-safe-1",
        "operations": [
            {"type": "node-patch", "id": "a", "patch": {"set": {"text": "after", "color": "2"}}},
            {"type": "node-create", "node": {"id": "b", "type": "text", "text": "new", "x": 10, "y": 0}},
            {"type": "edge-create", "edge": {"id": "e", "fromNode": "a", "toNode": "b"}}
        ]
    });
    let (first_result, second_result) = tokio::join!(
        send(&app, "POST", &route, Some(&token), Some(batch.clone())),
        send(&app, "POST", &route, Some(&token), Some(batch)),
    );
    let (status, first) = first_result;
    assert_eq!(status, StatusCode::OK);
    let (status, second) = second_result;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(second["value"], first["value"]);
    assert_eq!(second["value"]["nodes"].as_array().unwrap().len(), 2);
    assert_eq!(second["value"]["nodes"][0]["text"], "after");
    assert_eq!(second["value"]["edges"][0]["id"], "e");

    let (_, persisted) = send(
        &app,
        "GET",
        &format!("/api/vaults/{vault_id}/canvas/Board.canvas"),
        Some(&token),
        None,
    )
    .await;
    assert_eq!(persisted["value"], second["value"]);
}

#[tokio::test]
async fn note_move_rewrites_canvas_file_refs_and_undo_restores_them() {
    let app = test_app().await;
    let session = login(&app, "alice").await;
    let (_, vault) = send(
        &app,
        "POST",
        "/api/vaults",
        Some(&session),
        Some(json!({"name": "Notes"})),
    )
    .await;
    let vault_id = vault["id"].as_str().unwrap();

    let (status, _) = send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/notes"),
        Some(&session),
        Some(json!({"path": "ref.md", "content": "[[dir/Old.md]]"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/notes"),
        Some(&session),
        Some(json!({"path": "dir/Old.md", "content": "# Old"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/canvases"),
        Some(&session),
        Some(json!({
            "path": "Board.canvas",
            "value": {
                "nodes": [
                    {"id": "file", "type": "file", "file": "dir/Old.md", "x": 1, "y": 2, "width": 3, "height": 4},
                    {"id": "link", "type": "link", "url": "dir/Old.md", "x": 5, "y": 6, "width": 7, "height": 8}
                ],
                "edges": []
            }
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, cursor) = send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/cursors"),
        Some(&session),
        Some(json!({"name": "Mover"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let cursor_id = cursor["id"].as_str().unwrap();
    let cursor_secret = cursor["secretToken"].as_str().unwrap();

    let (status, _) = send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/note-moves/dir/Old.md"),
        Some(cursor_secret),
        Some(json!({"toPath": "new/New.md"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, canvas) = send(
        &app,
        "GET",
        &format!("/api/vaults/{vault_id}/canvas/Board.canvas"),
        Some(&session),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let nodes = canvas["value"]["nodes"].as_array().unwrap();
    let file_node = nodes.iter().find(|node| node["id"] == "file").unwrap();
    let link_node = nodes.iter().find(|node| node["id"] == "link").unwrap();
    assert_eq!(file_node["file"], "new/New.md");
    assert_eq!(file_node["x"], 1);
    assert_eq!(file_node["y"], 2);
    assert_eq!(file_node["width"], 3);
    assert_eq!(file_node["height"], 4);
    // Canvas link-node URLs are skipped even if path-like.
    assert_eq!(link_node["url"], "dir/Old.md");

    let (status, note) = send(
        &app,
        "GET",
        &format!("/api/vaults/{vault_id}/notes/ref.md"),
        Some(&session),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(note["content"], "[[new/New.md]]");

    let (status, page) = send(
        &app,
        "GET",
        &format!("/api/vaults/{vault_id}/cursors/{cursor_id}/audit"),
        Some(&session),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let audit_id = page["entries"][0]["id"].as_str().unwrap();
    assert_eq!(page["entries"][0]["operation"], "note_move");

    let (status, _) = send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/cursors/{cursor_id}/audit/{audit_id}/undo"),
        Some(&session),
        Some(json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, canvas) = send(
        &app,
        "GET",
        &format!("/api/vaults/{vault_id}/canvas/Board.canvas"),
        Some(&session),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let file_node = canvas["value"]["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|node| node["id"] == "file")
        .unwrap();
    assert_eq!(file_node["file"], "dir/Old.md");

    let (status, note) = send(
        &app,
        "GET",
        &format!("/api/vaults/{vault_id}/notes/ref.md"),
        Some(&session),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(note["content"], "[[dir/Old.md]]");
}

// ---------- plugin databases (cr-sqlite) ----------
//
// The replica/dump/compaction tests need the cr-sqlite loadable extension and
// are gated on CRSQLITE_EXT_PATH (they skip cleanly when it is not set), e.g.:
//   CRSQLITE_EXT_PATH=/path/to/crsqlite.dylib cargo test plugin_db

fn crsqlite_ext() -> Option<String> {
    std::env::var("CRSQLITE_EXT_PATH")
        .ok()
        .filter(|p| std::path::Path::new(p).exists())
}

fn open_crsqlite(path: &std::path::Path, ext: &str) -> rusqlite::Connection {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).ok();
    }
    let conn = rusqlite::Connection::open(path).unwrap();
    // SAFETY: loading the operator-supplied test extension.
    unsafe {
        conn.load_extension_enable().unwrap();
        conn.load_extension(ext, Some("sqlite3_crsqlite_init"))
            .unwrap();
        conn.load_extension_disable().unwrap();
    }
    conn
}

fn b64(bytes: &[u8]) -> String {
    // Tiny local base64 (std has none; avoids another dev-dependency).
    const T: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = ((b[0] as u32) << 16) | ((b[1] as u32) << 8) | b[2] as u32;
        out.push(T[(n >> 18) as usize & 63] as char);
        out.push(T[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 {
            T[(n >> 6) as usize & 63] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            T[n as usize & 63] as char
        } else {
            '='
        });
    }
    out
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Read every `crsql_changes` row from a scratch source DB as wire-format JSON.
/// Returns `(site_hex, max_db_version, change_rows)`.
fn read_source_changes(conn: &rusqlite::Connection) -> (String, i64, Vec<Value>) {
    let mut stmt = conn
        .prepare(
            "SELECT \"table\", pk, cid, val, col_version, db_version, site_id, cl, seq \
             FROM crsql_changes ORDER BY db_version, seq",
        )
        .unwrap();
    let mut site_hex = String::new();
    let mut max_v = 0i64;
    let rows = stmt
        .query_map([], |row| {
            let pk: Vec<u8> = row.get(1)?;
            let val: rusqlite::types::Value = row.get(3)?;
            let site: Vec<u8> = row.get(6)?;
            Ok((
                pk,
                row.get::<_, String>(0)?,
                row.get::<_, String>(2)?,
                val,
                row.get::<_, i64>(4)?,
                row.get::<_, i64>(5)?,
                site,
                row.get::<_, i64>(7)?,
                row.get::<_, i64>(8)?,
            ))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let changes = rows
        .into_iter()
        .map(
            |(pk, table, cid, val, col_version, db_version, site, cl, seq)| {
                site_hex = hex(&site);
                max_v = max_v.max(db_version);
                let val_json = match val {
                    rusqlite::types::Value::Null => Value::Null,
                    rusqlite::types::Value::Integer(i) => json!(i),
                    rusqlite::types::Value::Real(f) => json!(f),
                    rusqlite::types::Value::Text(t) => json!(t),
                    rusqlite::types::Value::Blob(b) => json!({ "$blob": b64(&b) }),
                };
                json!({
                    "table": table,
                    "pk": b64(&pk),
                    "cid": cid,
                    "val": val_json,
                    "col_version": col_version,
                    "db_version": db_version,
                    "site_id": b64(&site),
                    "cl": cl,
                    "seq": seq,
                })
            },
        )
        .collect();
    (site_hex, max_v, changes)
}

fn json_to_any(v: &Value) -> yrs::Any {
    use yrs::Any;
    match v {
        Value::Null => Any::Null,
        Value::Bool(b) => Any::Bool(*b),
        Value::Number(n) => match n.as_i64() {
            Some(i) => Any::BigInt(i),
            None => Any::Number(n.as_f64().unwrap_or(0.0)),
        },
        Value::String(s) => Any::String(s.clone().into()),
        Value::Array(a) => Any::Array(a.iter().map(json_to_any).collect::<Vec<_>>().into()),
        Value::Object(o) => Any::Map(
            o.iter()
                .map(|(k, v)| (k.clone(), json_to_any(v)))
                .collect::<HashMap<_, _>>()
                .into(),
        ),
    }
}

/// Encode a full plugin-db Y.Doc state: `batches`, `meta.schema`, optional tombstone.
fn plugin_db_doc_update(schema: &[String], batches: &Value, deleted_at: Option<i64>) -> Vec<u8> {
    use yrs::{Array, Map, ReadTxn, Transact};
    let doc = yrs::Doc::new();
    let batches_arr = doc.get_or_insert_array("batches");
    let meta = doc.get_or_insert_map("meta");
    {
        let mut txn = doc.transact_mut();
        for b in batches.as_array().unwrap() {
            batches_arr.push_back(&mut txn, json_to_any(b));
        }
        meta.insert(&mut txn, "schema", json_to_any(&json!(schema)));
        meta.insert(&mut txn, "schemaVersion", yrs::Any::BigInt(1));
        if let Some(ms) = deleted_at {
            meta.insert(&mut txn, "deletedAt", yrs::Any::BigInt(ms));
        }
    }
    let update = doc
        .transact()
        .encode_state_as_update_v1(&yrs::StateVector::default());
    update
}

/// Merge `meta.deletedAt` into an existing raw doc state.
async fn set_doc_deleted_at(state: &realtime_server::state::AppState, doc_id: &str, ms: i64) {
    use yrs::updates::decoder::Decode;
    use yrs::{Map, ReadTxn, Transact};
    let base = state.documents.read_update(doc_id).await.unwrap();
    let doc = yrs::Doc::new();
    {
        let mut txn = doc.transact_mut();
        txn.apply_update(yrs::Update::decode_v1(&base).unwrap());
    }
    let state_vector = doc.transact().state_vector();
    let meta = doc.get_or_insert_map("meta");
    {
        let mut txn = doc.transact_mut();
        meta.insert(&mut txn, "deletedAt", yrs::Any::BigInt(ms));
    }
    let delta = doc.transact().encode_state_as_update_v1(&state_vector);
    state.documents.apply_update(doc_id, &delta).await.unwrap();
}

async fn clear_doc_deleted_at(state: &realtime_server::state::AppState, doc_id: &str) {
    use yrs::updates::decoder::Decode;
    use yrs::{Map, ReadTxn, Transact};
    let base = state.documents.read_update(doc_id).await.unwrap();
    let doc = yrs::Doc::new();
    {
        let mut txn = doc.transact_mut();
        txn.apply_update(yrs::Update::decode_v1(&base).unwrap());
    }
    let state_vector = doc.transact().state_vector();
    doc.get_or_insert_map("meta")
        .remove(&mut doc.transact_mut(), "deletedAt");
    let delta = doc.transact().encode_state_as_update_v1(&state_vector);
    state.documents.apply_update(doc_id, &delta).await.unwrap();
}

/// Scratch source DB with a CRR `tasks` table and two rows; returns the wire
/// batch JSON + schema DDL the client would publish.
fn make_source_batch(ext: &str) -> (Vec<String>, Value, String, i64) {
    let src = std::env::temp_dir().join(format!("crsql-src-{}.sqlite", uuid::Uuid::new_v4()));
    let conn = open_crsqlite(&src, ext);
    conn.execute_batch("CREATE TABLE tasks (id PRIMARY KEY NOT NULL, title)")
        .unwrap();
    conn.execute_batch("SELECT crsql_as_crr('tasks')").unwrap();
    conn.execute("INSERT INTO tasks (id, title) VALUES ('a', 'alpha')", [])
        .unwrap();
    conn.execute("INSERT INTO tasks (id, title) VALUES ('b', 'beta')", [])
        .unwrap();
    let (site_hex, max_v, changes) = read_source_changes(&conn);
    let _ = conn.query_row("SELECT crsql_finalize()", [], |_| Ok(()));
    let batch = json!({
        "id": "batch-1",
        "siteId": site_hex,
        "fromDbVersion": 0,
        "toDbVersion": max_v,
        "schemaVersion": 1,
        "createdAt": 0,
        "format": "crsqlite-1",
        "changes": changes,
    });
    let schema = vec![
        "CREATE TABLE tasks (id PRIMARY KEY NOT NULL, title)".to_string(),
        "SELECT crsql_as_crr('tasks')".to_string(),
    ];
    (schema, json!([batch]), site_hex, max_v)
}

fn replica_file(
    git_dir: &std::path::Path,
    vault: &str,
    plugin: &str,
    name: &str,
) -> std::path::PathBuf {
    // replica_root derives from git_data_dir's parent (see plugindb.rs).
    git_dir
        .parent()
        .unwrap()
        .join("plugin-db-replicas")
        .join(vault)
        .join(plugin)
        .join(format!("{name}.sqlite"))
}

fn replica_titles(path: &std::path::Path, ext: &str) -> Vec<String> {
    let conn = open_crsqlite(path, ext);
    let mut stmt = conn.prepare("SELECT title FROM tasks ORDER BY id").unwrap();
    let titles = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    drop(stmt);
    let _ = conn.query_row("SELECT crsql_finalize()", [], |_| Ok(()));
    titles
}

#[tokio::test]
async fn plugin_db_replication_dump_compaction_and_bootstrap() {
    let Some(ext) = crsqlite_ext() else {
        eprintln!("skipping plugin_db_replication test: CRSQLITE_EXT_PATH not set");
        return;
    };

    let (state, git_dir, vault_id) = git_state_ext(Some(ext.clone())).await;
    let repo = git_dir.join(&vault_id);

    // Seed the per-DB doc with one published batch (two task rows).
    let (schema, batches, site_hex, max_v) = make_source_batch(&ext);
    let doc_id = format!("{vault_id}__plugindb__my-plugin__tasks");
    state
        .documents
        .apply_update(&doc_id, &plugin_db_doc_update(&schema, &batches, None))
        .await
        .unwrap();

    // Replay batches -> replica matches the source.
    state
        .plugindb
        .mark_write(&vault_id, "my-plugin", "tasks")
        .await;
    let replica = replica_file(&git_dir, &vault_id, "my-plugin", "tasks");
    for _ in 0..100 {
        if replica.exists() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(replica.exists(), "replica file should be created");
    assert_eq!(replica_titles(&replica, &ext), vec!["alpha", "beta"]);

    // Compaction: the server replica covers the lone batch and no device
    // cursors hold it back, so the doc log gets trimmed.
    let mut compacted = false;
    for _ in 0..100 {
        let raw = state.documents.read_update(&doc_id).await.unwrap();
        let view = realtime_server::plugindb::decode_doc(&raw).unwrap();
        if view.batches.is_empty() {
            assert_eq!(
                view.compacted_through.get(&site_hex),
                Some(&max_v),
                "compactedThrough must record the trimmed high-water mark"
            );
            compacted = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(compacted, "server should compact the fully-replicated log");

    // Bootstrap survives compaction: a fresh client (empty cursor) still gets
    // the full changeset, served from the replica.
    let rows = state
        .plugindb
        .bootstrap_changes(&vault_id, "my-plugin", "tasks", &HashMap::new())
        .await
        .unwrap();
    assert!(!rows.is_empty(), "bootstrap must serve compacted history");
    let tables: std::collections::HashSet<_> = rows.iter().map(|r| r.table.as_str()).collect();
    assert_eq!(tables, std::collections::HashSet::from(["tasks"]));
    // A caught-up cursor gets nothing.
    let mut caught_up = HashMap::new();
    caught_up.insert(site_hex.clone(), max_v);
    let none = state
        .plugindb
        .bootstrap_changes(&vault_id, "my-plugin", "tasks", &caught_up)
        .await
        .unwrap();
    assert!(none.is_empty(), "caught-up cursor should get no rows");

    // Git dump: deterministic, committed under .sql/, restorable.
    let d1 = state.plugindb.dumps_for_vault(&vault_id).await;
    let d2 = state.plugindb.dumps_for_vault(&vault_id).await;
    assert_eq!(d1, d2, "dumps must be deterministic");
    assert_eq!(d1.len(), 1);
    let (rel, sql) = &d1[0];
    assert_eq!(rel.to_string_lossy(), ".sql/my-plugin/tasks.sql");
    assert!(
        sql.contains("-- crr: tasks"),
        "dump records CRR tables: {sql}"
    );

    state
        .git
        .mark_write(
            &vault_id,
            &principal("u-alice", "Alice", "alice@example.com"),
        )
        .await;
    wait_for_commit(&repo).await;
    let committed = repo.join(".sql/my-plugin/tasks.sql");
    assert!(committed.exists(), "git tree must contain the dump");

    // Restore-from-dump round trip: fresh DB + dump + re-run crsql_as_crr.
    let restored =
        std::env::temp_dir().join(format!("crsql-restore-{}.sqlite", uuid::Uuid::new_v4()));
    let conn = open_crsqlite(&restored, &ext);
    let crr_tables: Vec<String> = sql
        .lines()
        .find_map(|l| l.strip_prefix("-- crr: "))
        .unwrap()
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();
    conn.execute_batch(sql).unwrap();
    for t in &crr_tables {
        conn.execute_batch(&format!("SELECT crsql_as_crr('{t}')"))
            .unwrap();
    }
    let mut stmt = conn.prepare("SELECT title FROM tasks ORDER BY id").unwrap();
    let titles: Vec<String> = stmt
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(titles, vec!["alpha", "beta"], "dump restore round-trips");
}

#[tokio::test]
async fn plugin_db_soft_delete_keeps_replica_and_purge_removes_everything() {
    let Some(ext) = crsqlite_ext() else {
        eprintln!("skipping plugin_db_purge test: CRSQLITE_EXT_PATH not set");
        return;
    };

    let (state, git_dir, vault_id) = git_state_ext(Some(ext.clone())).await;
    let repo = git_dir.join(&vault_id);

    let (schema, batches, _site_hex, _max_v) = make_source_batch(&ext);
    let doc_id = format!("{vault_id}__plugindb__my-plugin__tasks");
    state
        .documents
        .apply_update(&doc_id, &plugin_db_doc_update(&schema, &batches, None))
        .await
        .unwrap();

    // Replicate, then commit the dump.
    state
        .plugindb
        .mark_write(&vault_id, "my-plugin", "tasks")
        .await;
    let replica = replica_file(&git_dir, &vault_id, "my-plugin", "tasks");
    for _ in 0..100 {
        if replica.exists() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(replica.exists());
    state
        .git
        .mark_write(
            &vault_id,
            &principal("u-alice", "Alice", "alice@example.com"),
        )
        .await;
    wait_for_commit(&repo).await;
    let committed_dump = repo.join(".sql/my-plugin/tasks.sql");
    assert!(committed_dump.exists());

    // Soft delete: tombstone the doc. Replication stops but the replica stays.
    set_doc_deleted_at(&state, &doc_id, 12345).await;
    state
        .plugindb
        .mark_write(&vault_id, "my-plugin", "tasks")
        .await;
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert!(
        replica.exists(),
        "soft-deleted database must keep its replica (it is restorable)"
    );

    // Purge: replica gone, doc trimmed + tombstoned, dump dropped from git.
    state
        .plugindb
        .purge(&vault_id, "my-plugin", "tasks")
        .await
        .unwrap();
    assert!(!replica.exists(), "purge must delete the replica file");
    for suffix in ["-wal", "-shm", "-journal"] {
        assert!(
            !std::path::PathBuf::from(format!("{}{suffix}", replica.display())).exists(),
            "purge must delete SQLite sidecar {suffix}"
        );
    }
    let raw = state.documents.read_update(&doc_id).await.unwrap();
    let view = realtime_server::plugindb::decode_doc(&raw).unwrap();
    assert!(view.batches.is_empty(), "purge must trim the batch log");
    assert!(view.deleted_at.is_some(), "purge must set the tombstone");
    assert!(
        state.plugindb.dumps_for_vault(&vault_id).await.is_empty(),
        "purged databases must not be dumped"
    );

    state
        .git
        .mark_write(
            &vault_id,
            &principal("u-alice", "Alice", "alice@example.com"),
        )
        .await;
    for _ in 0..100 {
        let (_, count) = git_out(&repo, &["rev-list", "--count", "HEAD"]);
        if count == "2" {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let (_, count) = git_out(&repo, &["rev-list", "--count", "HEAD"]);
    assert_eq!(count, "2", "purge should produce a delete commit");
    assert!(
        !committed_dump.exists(),
        "the delete commit must remove the dump from the git tree"
    );
}

#[tokio::test]
async fn plugin_db_routes_validate_ids_and_membership() {
    // The store-backed fake serves /as-update, which the bootstrap route reads.

    let app = test_app().await;
    let alice = login(&app, "alice").await;

    let (status, vault) = send(
        &app,
        "POST",
        "/api/vaults",
        Some(&alice),
        Some(json!({"name": "Notes"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let vault_id = vault["id"].as_str().unwrap().to_string();

    // Invalid plugin id -> 400 (matches the client-side [A-Za-z0-9_-]{1,80} rule).
    let (status, _) = send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/plugin-dbs/bad!id/tasks/touch"),
        Some(&alice),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // `__` is the doc-id separator and is rejected to keep doc ids unambiguous.
    let (status, _) = send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/plugin-dbs/bad__id/tasks/touch"),
        Some(&alice),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // Non-members are rejected.
    let bob = login(&app, "bob").await;
    let (status, _) = send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/plugin-dbs/my-plugin/tasks/touch"),
        Some(&bob),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // A member can touch (arms replication + git debounces).
    let (status, body) = send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/plugin-dbs/my-plugin/tasks/touch"),
        Some(&alice),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["ok"], true);

    // Bootstrap on an empty/unknown db returns an empty changeset.
    let (status, body) = send(
        &app,
        "GET",
        &format!("/api/vaults/{vault_id}/plugin-dbs/my-plugin/tasks/changes"),
        Some(&alice),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["changes"], json!([]));
}

// ---------- cross-stack wire-format regression ----------
//
// `tests/fixtures/client-published-doc.bin` is a Y.Doc update produced by the
// REAL TypeScript client engine (SyncedPluginDatabase publishing through
// cr-sqlite WASM). Regenerate it after wire-format changes with:
//
//   npx tsx tests/support/genPluginDbDocFixture.mts
//
// This guards against drift between the client's JS/Yjs encoding (e.g. lib0
// float-vs-int number encoding) and the server's serde decode, which fabricated
// Rust-side batches can never catch.

#[tokio::test]
async fn plugin_db_decodes_and_replicates_a_real_client_published_doc() {
    let raw = include_bytes!("fixtures/client-published-doc.bin");
    let view = realtime_server::plugindb::decode_doc(raw).expect("decode client doc");

    // The client published one batch with four change rows (two task rows,
    // title + done columns each).
    assert_eq!(view.batches.len(), 1, "client batch must deserialize");
    let batch = &view.batches[0];
    assert_eq!(batch.changes.len(), 4, "all change rows must deserialize");
    assert!(
        view.schema.iter().any(|s| s.contains("crsql_as_crr")),
        "client-published schema must include the CRR call: {:?}",
        view.schema
    );

    // With the extension available, the batch must replay into a replica and
    // the dump must contain the client's rows.
    let Some(ext) = crsqlite_ext() else {
        eprintln!("skipping replica half: CRSQLITE_EXT_PATH not set");
        return;
    };

    let (state, git_dir, vault_id) = git_state_ext(Some(ext.clone())).await;
    let doc_id = format!("{vault_id}__plugindb__client-plugin__tasks");
    state.documents.apply_update(&doc_id, raw).await.unwrap();

    state
        .plugindb
        .mark_write(&vault_id, "client-plugin", "tasks")
        .await;
    let replica = replica_file(&git_dir, &vault_id, "client-plugin", "tasks");
    for _ in 0..100 {
        if replica.exists() && !replica_titles(&replica, &ext).is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert_eq!(
        replica_titles(&replica, &ext),
        vec!["from-A", "from-B"],
        "replica must contain the client's rows"
    );

    let dumps = state.plugindb.dumps_for_vault(&vault_id).await;
    assert_eq!(
        dumps.len(),
        1,
        "dump must exist for the replicated client db"
    );
    assert!(dumps[0].1.contains("from-A") && dumps[0].1.contains("from-B"));
}
/// Server-side SQL access over REST: list/query/execute, write publication,
/// and the lint rejections — all gated on `CRSQLITE_EXT_PATH` like the other
/// plugin-db tests. Calls the service methods directly (no HTTP layer needed).
#[tokio::test]
async fn plugin_db_sql_endpoints() {
    let Some(ext) = crsqlite_ext() else {
        eprintln!("skipping plugin_db_sql_endpoints test: CRSQLITE_EXT_PATH not set");
        return;
    };

    let (state, _git_dir, vault_id) = git_state_ext(Some(ext.clone())).await;

    // Seed the per-DB doc with one published batch (two task rows).
    let (schema, batches, site_hex, max_v) = make_source_batch(&ext);
    let doc_id = format!("{vault_id}__plugindb__my-plugin__tasks");
    state
        .documents
        .apply_update(&doc_id, &plugin_db_doc_update(&schema, &batches, None))
        .await
        .unwrap();

    // list_dbs before any replication -> empty (rows appear once the server
    // has replicated at least once, which a query triggers via its refresh).
    let before = state.plugindb.list_dbs(&vault_id).await.unwrap();
    assert!(before.is_empty(), "no replica row yet");

    // query_sql refreshes the replica on the way, so the seeded rows are
    // visible and a replica row now exists.
    let res = state
        .plugindb
        .query_sql(
            &vault_id,
            "my-plugin",
            "tasks",
            "SELECT title FROM tasks ORDER BY id",
            &[],
            None,
        )
        .await
        .expect("query");
    assert_eq!(res.columns, vec!["title".to_string()]);
    let titles: Vec<String> = res
        .rows
        .iter()
        .map(|r| r[0].as_str().expect("title str").to_string())
        .collect();
    assert_eq!(titles, vec!["alpha", "beta"]);

    let after = state.plugindb.list_dbs(&vault_id).await.unwrap();
    assert_eq!(
        after
            .iter()
            .map(|d| (d.plugin.clone(), d.name.clone()))
            .collect::<Vec<_>>(),
        vec![("my-plugin".to_string(), "tasks".to_string())],
        "list_dbs reflects the now-replicated db"
    );

    // execute_sql: an INSERT + UPDATE in one transaction.
    let stmts = vec![
        realtime_server::plugindb::ExecuteStatement {
            sql: "INSERT INTO tasks (id, title) VALUES (?1, ?2)".into(),
            params: vec![json!("c"), json!("gamma")],
        },
        realtime_server::plugindb::ExecuteStatement {
            sql: "UPDATE tasks SET title = ?1 WHERE id = ?2".into(),
            params: vec![json!("gamma-renamed"), json!("c")],
        },
    ];
    let exec = state
        .plugindb
        .execute_sql(&vault_id, "my-plugin", "tasks", &stmts)
        .await
        .expect("execute");
    assert_eq!(exec.rows_affected, 2, "both statements mutated rows");

    // Decode the doc: a new server-authored batch was appended.
    let raw = state.documents.read_update(&doc_id).await.unwrap();
    let view = realtime_server::plugindb::decode_doc(&raw).unwrap();
    // The new batch is the last one; its site_id differs from the client's
    // and its changes are non-empty.
    let server_batch = view.batches.last().expect("server batch appended");
    assert!(
        !server_batch.changes.is_empty(),
        "server-authored batch must carry changes"
    );
    assert_ne!(
        server_batch.site_id, site_hex,
        "server batch comes from the server's own site, not the client's"
    );

    // query reflects the write.
    let res = state
        .plugindb
        .query_sql(
            &vault_id,
            "my-plugin",
            "tasks",
            "SELECT title FROM tasks WHERE id = 'c'",
            &[],
            None,
        )
        .await
        .expect("query after write");
    let got: String = res.rows[0][0].as_str().expect("str").to_string();
    assert_eq!(got, "gamma-renamed");

    // Rejections.
    let create = state
        .plugindb
        .execute_sql(
            &vault_id,
            "my-plugin",
            "tasks",
            &[realtime_server::plugindb::ExecuteStatement {
                sql: "CREATE TABLE x (id)".into(),
                params: vec![],
            }],
        )
        .await;
    assert!(create.is_err(), "DDL is rejected");

    let internal = state
        .plugindb
        .query_sql(
            &vault_id,
            "my-plugin",
            "tasks",
            "SELECT * FROM sqlite_master",
            &[],
            None,
        )
        .await;
    assert!(internal.is_err(), "sqlite_ internals rejected by lint");

    let unknown = state
        .plugindb
        .query_sql(&vault_id, "my-plugin", "nope", "SELECT 1", &[], None)
        .await;
    assert!(unknown.is_err(), "unknown name -> NotFound");

    // Tombstone the doc; query must reject.
    set_doc_deleted_at(&state, &doc_id, 12345).await;
    let tomb = state
        .plugindb
        .query_sql(&vault_id, "my-plugin", "tasks", "SELECT 1", &[], None)
        .await;
    assert!(tomb.is_err(), "deleted database rejects queries");

    // Server-cursor advance: after execute, bootstrap_changes with an empty
    // server-authored rows (clients can bootstrap them),
    // and a cursor caught up to the server's own site excludes them.
    // Clear the tombstone so bootstrap proceeds.
    clear_doc_deleted_at(&state, &doc_id).await;
    // Re-run execute to re-publish (the doc was reset above).
    let stmts = vec![realtime_server::plugindb::ExecuteStatement {
        sql: "INSERT INTO tasks (id, title) VALUES (?1, ?2)".into(),
        params: vec![json!("d"), json!("delta")],
    }];
    let _ = state
        .plugindb
        .execute_sql(&vault_id, "my-plugin", "tasks", &stmts)
        .await
        .expect("execute re-publish");
    let rows = state
        .plugindb
        .bootstrap_changes(&vault_id, "my-plugin", "tasks", &HashMap::new())
        .await
        .expect("bootstrap");
    // All rows (client + server) are present in the bootstrap from the replica.
    assert!(
        rows.iter().any(|r| r.table == "tasks"),
        "bootstrap includes server-authored rows after cursor advance"
    );
    // (The caught-up-cursor edge of bootstrap filtering is covered by the
    // unit-level cursor tests in the lib; here we only assert the empty-cursor
    // bootstrap above.)
    let _ = max_v;

    // ---------- HTTP-level coverage of the new REST routes ----------
    //
    // The service-method checks above do not exercise Axum routing, the
    // ApiPrincipal extractor, JSON (de)serialization, or the lib.rs wiring.
    // Build a git+ext-enabled app and drive the new routes through HTTP.
    let (app, http_state, _git_dir2) = git_ext_app(Some(ext.clone())).await;
    let alice = login(&app, "alice").await;
    let (status, vault) = send(
        &app,
        "POST",
        "/api/vaults",
        Some(&alice),
        Some(json!({"name": "V"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let http_vault_id = vault["id"].as_str().unwrap().to_string();

    // Seed a per-DB doc for this vault (two task rows), then query refreshes
    // the replica on the way so the rows are visible over HTTP.
    let (schema2, batches2, _site2, _max2) = make_source_batch(&ext);
    let http_doc_id = format!("{http_vault_id}__plugindb__my-plugin__tasks");
    http_state
        .documents
        .apply_update(
            &http_doc_id,
            &plugin_db_doc_update(&schema2, &batches2, None),
        )
        .await
        .unwrap();

    let (status, body) = send(
        &app,
        "GET",
        &format!("/api/vaults/{http_vault_id}/plugin-dbs"),
        Some(&alice),
        None,
    )
    .await;
    // list before replication -> empty (no replica row yet); a query will seed it.
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["databases"], json!([]));

    let (status, body) = send(
        &app,
        "POST",
        &format!("/api/vaults/{http_vault_id}/plugin-dbs/my-plugin/tasks/query"),
        Some(&alice),
        Some(json!({"sql": "SELECT title FROM tasks ORDER BY id"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["columns"], json!(["title"]));
    let titles: Vec<Value> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r[0].clone())
        .collect();
    assert_eq!(titles, vec![json!("alpha"), json!("beta")]);
    assert_eq!(body["truncated"], json!(false));

    // list now reflects the replica row the query just materialized.
    let (status, body) = send(
        &app,
        "GET",
        &format!("/api/vaults/{http_vault_id}/plugin-dbs"),
        Some(&alice),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["databases"][0]["plugin"], "my-plugin",
        "list_dbs reflects the replicated db over HTTP"
    );
    assert_eq!(body["databases"][0]["name"], "tasks");

    // execute over HTTP: INSERT one row, then query it back.
    let (status, body) = send(
        &app,
        "POST",
        &format!("/api/vaults/{http_vault_id}/plugin-dbs/my-plugin/tasks/execute"),
        Some(&alice),
        Some(json!({"statements":[{"sql":"INSERT INTO tasks (id, title) VALUES (?1, ?2)","params":["z","zeta"]}]})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["rowsAffected"], json!(1));
    assert!(body["dbVersion"].is_i64());

    let (status, body) = send(
        &app,
        "POST",
        &format!("/api/vaults/{http_vault_id}/plugin-dbs/my-plugin/tasks/query"),
        Some(&alice),
        Some(json!({"sql":"SELECT title FROM tasks WHERE id = 'z'"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["rows"][0][0], json!("zeta"));

    // Lint rejections surface as 400 over HTTP.
    let (status, _body) = send(
        &app,
        "POST",
        &format!("/api/vaults/{http_vault_id}/plugin-dbs/my-plugin/tasks/query"),
        Some(&alice),
        Some(json!({"sql":"SELECT * FROM sqlite_master"})),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let (status, _body) = send(
        &app,
        "POST",
        &format!("/api/vaults/{http_vault_id}/plugin-dbs/my-plugin/tasks/execute"),
        Some(&alice),
        Some(json!({"statements":[{"sql":"CREATE TABLE x (id)"}]})),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // A non-member is rejected by the guard (ApiPrincipal require_member).
    let bob = login(&app, "bob").await;
    let (status, _body) = send(
        &app,
        "POST",
        &format!("/api/vaults/{http_vault_id}/plugin-dbs/my-plugin/tasks/query"),
        Some(&bob),
        Some(json!({"sql":"SELECT 1"})),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // Unknown name over HTTP -> 404.
    let (status, _body) = send(
        &app,
        "POST",
        &format!("/api/vaults/{http_vault_id}/plugin-dbs/my-plugin/nope/query"),
        Some(&alice),
        Some(json!({"sql":"SELECT 1"})),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // Lint is token-aware: internals inside a string literal are fine.
    let (status, _body) = send(
        &app,
        "POST",
        &format!("/api/vaults/{http_vault_id}/plugin-dbs/my-plugin/tasks/query"),
        Some(&alice),
        Some(json!({"sql":"SELECT title FROM tasks WHERE title = 'sqlite_master'"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    // …but a quoted identifier is still an identifier.
    let (status, _body) = send(
        &app,
        "POST",
        &format!("/api/vaults/{http_vault_id}/plugin-dbs/my-plugin/tasks/query"),
        Some(&alice),
        Some(json!({"sql":"SELECT * FROM \"sqlite_master\""})),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // ---------- path-ACL behavior: read-only member, denied member ----------
    use realtime_server::entities::permissions;
    use sea_orm::{ActiveModelTrait, Set};
    let carol = login(&app, "carol").await;
    let dave = login(&app, "dave").await;
    for (token, user) in [(&carol, "carol"), (&dave, "dave")] {
        let (_, invite) = send(
            &app,
            "POST",
            &format!("/api/vaults/{http_vault_id}/invites"),
            Some(&alice),
            Some(json!({})),
        )
        .await;
        let code = invite["code"].as_str().unwrap().to_string();
        let (status, _) = send(
            &app,
            "POST",
            "/api/invites/redeem",
            Some(token),
            Some(json!({"code": code})),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{user} redeems invite");
    }
    let carol_id = send(&app, "GET", "/api/me", Some(&carol), None).await.1["userId"]
        .as_str()
        .unwrap()
        .to_string();
    let dave_id = send(&app, "GET", "/api/me", Some(&dave), None).await.1["userId"]
        .as_str()
        .unwrap()
        .to_string();
    for (uid, level) in [(&carol_id, "read-only"), (&dave_id, "deny")] {
        permissions::ActiveModel {
            id: Set(uuid::Uuid::new_v4().to_string()),
            vault_id: Set(http_vault_id.clone()),
            principal_user_id: Set(Some(uid.clone())),
            path_prefix: Set(".realtime/plugin-dbs/my-plugin/tasks".to_string()),
            level: Set(level.to_string()),
        }
        .insert(&http_state.db)
        .await
        .unwrap();
    }

    // Read-only member: query OK, execute 403.
    let (status, _body) = send(
        &app,
        "POST",
        &format!("/api/vaults/{http_vault_id}/plugin-dbs/my-plugin/tasks/query"),
        Some(&carol),
        Some(json!({"sql":"SELECT 1"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "read-only member can query");
    let (status, _body) = send(
        &app,
        "POST",
        &format!("/api/vaults/{http_vault_id}/plugin-dbs/my-plugin/tasks/execute"),
        Some(&carol),
        Some(json!({"statements":[{"sql":"DELETE FROM tasks"}]})),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "read-only member cannot execute"
    );

    // Denied member: query 403, and the database is omitted from list.
    let (status, _body) = send(
        &app,
        "POST",
        &format!("/api/vaults/{http_vault_id}/plugin-dbs/my-plugin/tasks/query"),
        Some(&dave),
        Some(json!({"sql":"SELECT 1"})),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "denied member cannot query");
    let (status, body) = send(
        &app,
        "GET",
        &format!("/api/vaults/{http_vault_id}/plugin-dbs"),
        Some(&dave),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["databases"],
        json!([]),
        "denied member does not even see the database's name"
    );
    // …while the owner still sees it.
    let (_, body) = send(
        &app,
        "GET",
        &format!("/api/vaults/{http_vault_id}/plugin-dbs"),
        Some(&alice),
        None,
    )
    .await;
    assert_eq!(body["databases"][0]["name"], "tasks");
}

/// The client fires `POST /api/vaults/{id}/files` fire-and-forget on every file
/// event, so a single guid (e.g. an attachment being deleted, restored from
/// trash, then restored to a new path) produces a burst of concurrent registry
/// updates. A find-then-insert raced and surfaced as
/// `UNIQUE constraint failed: vault_files.vault_id, vault_files.guid` (HTTP 500).
/// The handler now does an atomic upsert, so every concurrent request succeeds.
#[tokio::test]
async fn concurrent_file_registry_upserts_for_same_guid_do_not_collide() {
    let app = test_app().await;
    let token = login(&app, "alice").await;

    let (status, vault) = send(
        &app,
        "POST",
        "/api/vaults",
        Some(&token),
        Some(json!({ "name": "Files" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let vault_id = vault["id"].as_str().unwrap().to_string();

    let guid = "shared-guid";
    let mut handles = Vec::new();
    for i in 0..16 {
        let app = app.clone();
        let token = token.clone();
        let vault_id = vault_id.clone();
        handles.push(tokio::spawn(async move {
            send(
                &app,
                "POST",
                &format!("/api/vaults/{vault_id}/files"),
                Some(&token),
                Some(json!({ "guid": guid, "path": format!("attachments/img-{i}.png") })),
            )
            .await
            .0
        }));
    }

    for h in handles {
        assert_eq!(
            h.await.unwrap(),
            StatusCode::OK,
            "concurrent registry upsert for the same guid must not 500 on a unique-index collision"
        );
    }

    // A subsequent update for the same guid still succeeds (the row is updated,
    // not duplicated or rejected).
    let (status, _) = send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/files"),
        Some(&token),
        Some(json!({ "guid": guid, "path": "attachments/final.png" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn concurrent_new_file_registry_upserts_respect_vault_quota() {
    let mut config = test_config("http://auth.test", realtime_server::blobs::MAX_BLOB_BYTES);
    config.crdt_max_documents_per_vault = 1;
    let app = app_from_config(&config).await;
    let token = login(&app, "alice").await;
    let (_, vault) = send(
        &app,
        "POST",
        "/api/vaults",
        Some(&token),
        Some(json!({ "name": "Files" })),
    )
    .await;
    let vault_id = vault["id"].as_str().unwrap().to_string();

    let mut handles = Vec::new();
    for i in 0..8 {
        let app = app.clone();
        let token = token.clone();
        let vault_id = vault_id.clone();
        handles.push(tokio::spawn(async move {
            send(
                &app,
                "POST",
                &format!("/api/vaults/{vault_id}/files"),
                Some(&token),
                Some(json!({ "guid": format!("guid-{i}"), "path": format!("file-{i}.md") })),
            )
            .await
            .0
        }));
    }
    let mut accepted = 0;
    for handle in handles {
        if handle.await.unwrap() == StatusCode::OK {
            accepted += 1;
        }
    }
    assert_eq!(accepted, 1);
}

// ---------- remote cursor streaming (WebSocket e2e) ----------

type WsClient =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// Read the next JSON text frame from the stream WebSocket, with a deadline so
/// protocol regressions fail fast instead of hanging the suite.
async fn next_stream_frame(ws: &mut WsClient) -> Value {
    use futures_util::StreamExt;
    use tokio_tungstenite::tungstenite::Message as TtMsg;
    let deadline = tokio::time::Duration::from_secs(10);
    loop {
        let msg = tokio::time::timeout(deadline, ws.next())
            .await
            .expect("timed out waiting for a stream frame")
            .expect("stream socket closed unexpectedly")
            .expect("stream socket errored");
        if let TtMsg::Text(text) = msg {
            return serde_json::from_str(&text).expect("stream frame must be JSON");
        }
    }
}

async fn send_stream_frame(ws: &mut WsClient, frame: Value) {
    use futures_util::SinkExt;
    use tokio_tungstenite::tungstenite::Message as TtMsg;
    ws.send(TtMsg::Text(frame.to_string())).await.unwrap();
}

async fn next_awareness_entry(
    connection: &mut realtime_server::crdt::CrdtConnection,
) -> (u32, String) {
    use yrs::updates::decoder::Decode;
    loop {
        let bytes = tokio::time::timeout(tokio::time::Duration::from_secs(10), connection.recv())
            .await
            .expect("timed out waiting for awareness")
            .expect("observer document connection closed");
        let Ok(yrs::sync::Message::Awareness(update)) = yrs::sync::Message::decode_v1(&bytes)
        else {
            continue;
        };
        if let Some(entry) = update.clients.into_values().next() {
            return (entry.clock, entry.json);
        }
    }
}

/// End-to-end streaming session over real sockets: REST setup → WebSocket
/// token streaming into a native CRDT doc → caret awareness published
/// and cleared → Git/audit attribution → audit-log undo via REST.
#[tokio::test]
async fn stream_ws_e2e_tokens_caret_audit_and_undo() {
    let (app, state) = test_app_with_state().await;

    // WebSocket upgrades need a real connection, not `oneshot`.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    {
        let app = app.clone();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    }

    // REST setup: vault, seed note, remote cursor.
    let session = login(&app, "alice").await;
    let (status, vault) = send(
        &app,
        "POST",
        "/api/vaults",
        Some(&session),
        Some(json!({ "name": "Stream Vault" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let vault_id = vault["id"].as_str().unwrap().to_string();

    let (status, note_created) = send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/notes"),
        Some(&session),
        Some(json!({ "path": "stream.md", "content": "# Title\n" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let note_doc_id = format!("{vault_id}__{}", note_created["guid"].as_str().unwrap());
    let mut awareness_observer = state
        .documents
        .connect_internal(&note_doc_id, realtime_server::crdt::Level::ReadOnly)
        .await
        .unwrap();

    let (status, cursor) = send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/cursors"),
        Some(&session),
        Some(json!({ "name": "Streamy" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let cursor_id = cursor["id"].as_str().unwrap().to_string();
    let secret = cursor["secretToken"].as_str().unwrap().to_string();

    // An unknown token must be rejected during the HTTP upgrade.
    let bad = tokio_tungstenite::connect_async(format!(
        "ws://{addr}/api/vaults/{vault_id}/stream?token=not-a-token"
    ))
    .await;
    assert!(bad.is_err(), "bad token must not upgrade");

    // Stream a sentence in two token chunks.
    let (mut ws, _) = tokio_tungstenite::connect_async(format!(
        "ws://{addr}/api/vaults/{vault_id}/stream?token={secret}"
    ))
    .await
    .expect("cursor token should upgrade");
    send_stream_frame(
        &mut ws,
        json!({ "type": "start", "path": "stream.md", "anchor": { "mode": "append" } }),
    )
    .await;
    let started = next_stream_frame(&mut ws).await;
    assert_eq!(started["type"], "started", "got: {started}");
    assert!(started["guid"].is_string());
    let mut caret_entry = next_awareness_entry(&mut awareness_observer).await;

    send_stream_frame(&mut ws, json!({ "type": "text", "text": "Hello " })).await;
    send_stream_frame(&mut ws, json!({ "type": "text", "text": "world" })).await;
    send_stream_frame(&mut ws, json!({ "type": "end" })).await;

    let mut acked = 0u64;
    let done = loop {
        let frame = next_stream_frame(&mut ws).await;
        match frame["type"].as_str() {
            Some("ack") => acked += frame["applied"].as_u64().unwrap(),
            Some("done") => break frame,
            other => panic!("unexpected stream frame {other:?}: {frame}"),
        }
    };
    assert_eq!(acked, 11, "every streamed byte must be acked");
    assert_eq!(done["inserted"], 11);
    let audit_id = done["auditId"]
        .as_str()
        .expect("session must be audited")
        .to_string();

    // The streamed text landed in the doc, visible over plain REST.
    let (status, note) = send(
        &app,
        "GET",
        &format!("/api/vaults/{vault_id}/notes/stream.md"),
        Some(&session),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(note["content"], "# Title\nHello world");

    // The caret was published over the native transport in the
    // RemoteSelections shape and then cleared with a higher awareness clock.
    let cleared_entry = loop {
        let entry = next_awareness_entry(&mut awareness_observer).await;
        if entry.1 == "null" {
            break entry;
        }
        caret_entry = entry;
    };
    let caret: Value = serde_json::from_str(&caret_entry.1).unwrap();
    assert_eq!(caret["user"]["name"], "Streamy");
    assert!(caret["user"]["color"].as_str().unwrap().starts_with('#'));
    assert_eq!(caret["cursor"]["anchor"], caret["cursor"]["head"]);
    let anchor = &caret["cursor"]["anchor"];
    assert!(
        anchor["item"]["client"].is_u64() || anchor["tname"].is_string(),
        "anchor must be a yjs-style relative position: {anchor}"
    );
    assert!(
        caret_entry.0 < cleared_entry.0,
        "awareness clocks must increase: {:?}",
        [caret_entry.0, cleared_entry.0]
    );

    // The session shows up in the cursor's audit log with the full diff…
    let (status, page) = send(
        &app,
        "GET",
        &format!("/api/vaults/{vault_id}/cursors/{cursor_id}/audit"),
        Some(&session),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let entries = page["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 1);
    let entry = &entries[0];
    assert_eq!(entry["id"], audit_id.as_str());
    assert_eq!(entry["operation"], "stream");
    assert_eq!(entry["path"], "stream.md");
    assert_eq!(entry["beforeContent"], "# Title\n");
    assert_eq!(entry["afterContent"], "# Title\nHello world");
    assert!(entry["undoneAt"].is_null());

    // …and undoing it restores the pre-stream content.
    let (status, _) = send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/cursors/{cursor_id}/audit/{audit_id}/undo"),
        Some(&session),
        Some(json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, note) = send(
        &app,
        "GET",
        &format!("/api/vaults/{vault_id}/notes/stream.md"),
        Some(&session),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(note["content"], "# Title\n");

    let (_, page) = send(
        &app,
        "GET",
        &format!("/api/vaults/{vault_id}/cursors/{cursor_id}/audit"),
        Some(&session),
        None,
    )
    .await;
    assert!(page["entries"][0]["undoneAt"].is_i64());
}

/// Streaming into a note after an anchor while a "human" concurrently edits
/// the same native doc: the stream position must shift with the
/// concurrent edit instead of splitting or clobbering it.
#[tokio::test]
async fn stream_ws_e2e_anchor_survives_concurrent_edit() {
    use yrs::updates::decoder::Decode;
    use yrs::{GetString, ReadTxn, Text, Transact};

    let (app, state) = test_app_with_state().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    {
        let app = app.clone();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    }

    let session = login(&app, "alice").await;
    let (_, vault) = send(
        &app,
        "POST",
        "/api/vaults",
        Some(&session),
        Some(json!({ "name": "V" })),
    )
    .await;
    let vault_id = vault["id"].as_str().unwrap().to_string();
    let (status, note) = send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/notes"),
        Some(&session),
        Some(json!({ "path": "draft.md", "content": "intro\n## Draft\noutro" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let guid = note["guid"].as_str().unwrap().to_string();
    let (_, cursor) = send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/cursors"),
        Some(&session),
        Some(json!({ "name": "Streamy" })),
    )
    .await;
    let secret = cursor["secretToken"].as_str().unwrap().to_string();

    let (mut ws, _) = tokio_tungstenite::connect_async(format!(
        "ws://{addr}/api/vaults/{vault_id}/stream?token={secret}"
    ))
    .await
    .unwrap();
    send_stream_frame(
        &mut ws,
        json!({ "type": "start", "path": "draft.md", "anchor": { "mode": "after", "text": "## Draft" } }),
    )
    .await;
    assert_eq!(next_stream_frame(&mut ws).await["type"], "started");

    send_stream_frame(&mut ws, json!({ "type": "text", "text": "\nstreamed-a" })).await;
    // Wait for the first batch to be applied before editing concurrently.
    assert_eq!(next_stream_frame(&mut ws).await["type"], "ack");

    // A "human" prepends text directly via the native store: everything after
    // this lands while the stream is mid-flight.
    {
        let doc_id = format!("{vault_id}__{guid}");
        let base = state.documents.read_update(&doc_id).await.unwrap();
        let doc = yrs::Doc::new();
        let text = doc.get_or_insert_text("contents");
        let mut txn = doc.transact_mut();
        txn.apply_update(yrs::Update::decode_v1(&base).unwrap());
        let state_vector = txn.state_vector();
        text.insert(&mut txn, 0, "PREFIX ");
        drop(txn);
        let delta = doc.transact().encode_state_as_update_v1(&state_vector);
        state.documents.apply_update(&doc_id, &delta).await.unwrap();
    }

    send_stream_frame(&mut ws, json!({ "type": "text", "text": "-b" })).await;
    send_stream_frame(&mut ws, json!({ "type": "end" })).await;
    loop {
        let frame = next_stream_frame(&mut ws).await;
        if frame["type"] == "done" {
            // "\nstreamed-a" (11 bytes) + "-b" (2 bytes)
            assert_eq!(frame["inserted"], 13);
            break;
        }
        assert_eq!(frame["type"], "ack");
    }

    // Merge result: prefix kept, streamed text contiguous after the anchor.
    let doc_id = format!("{vault_id}__{guid}");
    let merged = state.documents.read_update(&doc_id).await.unwrap();
    let doc = yrs::Doc::new();
    {
        let mut txn = doc.transact_mut();
        txn.apply_update(yrs::Update::decode_v1(&merged).unwrap());
    }
    let text = doc.get_or_insert_text("contents");
    let content = text.get_string(&doc.transact());
    assert_eq!(content, "PREFIX intro\n## Draft\nstreamed-a-b\noutro");
}

// ---------- git history + rollback e2e ----------

/// Git-enabled app returning the dirs the history tests need to inspect
/// (per-vault repo, blob store).
async fn history_test_app() -> (Router, std::path::PathBuf, std::path::PathBuf) {
    let mut db_path = std::env::temp_dir();
    db_path.push(format!("realtime-test-{}.db", uuid::Uuid::new_v4()));
    let mut blob_dir = std::env::temp_dir();
    blob_dir.push(format!("realtime-blobs-{}", uuid::Uuid::new_v4()));
    let mut git_dir = std::env::temp_dir();
    git_dir.push(format!("realtime-git-{}", uuid::Uuid::new_v4()));

    let config = Config {
        database_url: format!("sqlite://{}?mode=rwc", db_path.display()),
        background_jobs_enabled: true,
        background_job_concurrency: 4,
        background_job_max_attempts: 25,
        background_job_retry_min_ms: 250,
        background_job_retry_max_ms: 30_000,
        background_job_shutdown_timeout_ms: 30_000,
        server_shutdown_timeout_ms: 30_000,
        bind_addr: "127.0.0.1:0".into(),
        public_base_url: "http://auth.test".into(),
        crdt_store_dir: std::env::temp_dir()
            .join(format!("realtime-crdt-{}", uuid::Uuid::new_v4()))
            .display()
            .to_string(),
        crdt_epoch_period_days: 365,
        crdt_epoch_recovery_days: 30,
        crdt_epoch_max_updates: 100_000,
        crdt_epoch_max_state_bytes: 32 * 1024 * 1024,
        crdt_epoch_max_delete_set_bytes: 8 * 1024 * 1024,
        crdt_max_documents_per_vault: 100_000,
        blob_dir: blob_dir.display().to_string(),
        oidc_mode: OidcMode::Mock,
        oidc_issuer: None,
        oidc_client_id: None,
        oidc_client_secret: None,
        oidc_redirect_url: None,
        allowed_login_redirects: vec!["http://app".into()],
        cors_allowed_origins: vec![],
        git_data_dir: git_dir.display().to_string(),
        git_enabled: true,
        git_debounce_ms: 50,
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
        attachment_max_bytes: realtime_server::blobs::MAX_BLOB_BYTES,
        attachments_path_mode: "relative".into(),
        attachments_subfolder: None,
        upload_token: "test-upload-token".into(),
        crsqlite_ext_path: None,
        web_dist_path: "../packages/web/dist".into(),
    };
    let state = build_state(config).await.unwrap();
    (app(state), git_dir, blob_dir)
}
/// Like [`history_test_app`] but wires the cr-sqlite extension (for the
/// server-side SQL endpoints) and returns the git dir. Used by the plugin-db
/// SQL integration test to exercise the new REST routes through Axum.
async fn git_ext_app(
    crsqlite_ext_path: Option<String>,
) -> (Router, realtime_server::state::AppState, std::path::PathBuf) {
    let mut db_path = std::env::temp_dir();
    db_path.push(format!("realtime-test-{}.db", uuid::Uuid::new_v4()));
    let mut git_dir = std::env::temp_dir();
    git_dir.push(format!("realtime-git-{}", uuid::Uuid::new_v4()));
    let config = Config {
        database_url: format!("sqlite://{}?mode=rwc", db_path.display()),
        background_jobs_enabled: true,
        background_job_concurrency: 4,
        background_job_max_attempts: 25,
        background_job_retry_min_ms: 250,
        background_job_retry_max_ms: 30_000,
        background_job_shutdown_timeout_ms: 30_000,
        server_shutdown_timeout_ms: 30_000,
        bind_addr: "127.0.0.1:0".into(),
        public_base_url: "http://auth.test".into(),
        crdt_store_dir: std::env::temp_dir()
            .join(format!("realtime-crdt-{}", uuid::Uuid::new_v4()))
            .display()
            .to_string(),
        crdt_epoch_period_days: 365,
        crdt_epoch_recovery_days: 30,
        crdt_epoch_max_updates: 100_000,
        crdt_epoch_max_state_bytes: 32 * 1024 * 1024,
        crdt_epoch_max_delete_set_bytes: 8 * 1024 * 1024,
        crdt_max_documents_per_vault: 100_000,
        blob_dir: std::env::temp_dir().display().to_string(),
        oidc_mode: OidcMode::Mock,
        oidc_issuer: None,
        oidc_client_id: None,
        oidc_client_secret: None,
        oidc_redirect_url: None,
        allowed_login_redirects: vec!["http://app".into()],
        cors_allowed_origins: vec![],
        git_data_dir: git_dir.display().to_string(),
        git_enabled: true,
        git_debounce_ms: 50,
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
        attachment_max_bytes: realtime_server::blobs::MAX_BLOB_BYTES,
        attachments_path_mode: "relative".into(),
        attachments_subfolder: None,
        upload_token: "test-upload-token".into(),
        crsqlite_ext_path,
        web_dist_path: "../packages/web/dist".into(),
    };
    let state = build_state(config).await.unwrap();
    (app(state.clone()), state, git_dir)
}

/// Poll a vault repo until it holds at least `n` commits; returns HEAD's hash.
async fn wait_for_commit_count(repo: &std::path::Path, n: u64) -> String {
    for _ in 0..200 {
        let (ok, count) = git_out(repo, &["rev-list", "--count", "HEAD"]);
        if ok && count.parse::<u64>().map(|c| c >= n).unwrap_or(false) {
            return git_out(repo, &["rev-parse", "HEAD"]).1;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!(
        "repo {} never reached {n} commits (have {:?})",
        repo.display(),
        git_out(repo, &["rev-list", "--count", "HEAD"]).1
    );
}

#[tokio::test]
async fn history_endpoints_browse_commits_changes_trees_and_files() {
    let (app, git_dir, _blobs) = history_test_app().await;
    let alice = login(&app, "alice").await;
    let (_, vault) = send(
        &app,
        "POST",
        "/api/vaults",
        Some(&alice),
        Some(json!({"name": "V"})),
    )
    .await;
    let vault_id = vault["id"].as_str().unwrap().to_string();
    let repo = git_dir.join(&vault_id);

    // Empty history: no repo yet -> empty list, unknown hash -> 404.
    let base = format!("/api/vaults/{vault_id}/history/commits");
    let (status, page) = send(&app, "GET", &base, Some(&alice), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(page["commits"].as_array().unwrap().len(), 0);
    let (status, _) = send(&app, "GET", &format!("{base}/abcdef12"), Some(&alice), None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // Commit 1: create a note (unicode path) through the REST API.
    let (status, _) = send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/notes"),
        Some(&alice),
        Some(json!({"path": "nötes/hello one.md", "content": "# v1\n"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let c1 = wait_for_commit_count(&repo, 1).await;

    // Commit 2: edit it.
    let (status, _) = send(
        &app,
        "PUT",
        &format!("/api/vaults/{vault_id}/notes/n%C3%B6tes/hello%20one.md"),
        Some(&alice),
        Some(json!({"content": "# v2\nmore\n"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let c2 = wait_for_commit_count(&repo, 2).await;
    assert_ne!(c1, c2);

    // List: newest first, attributed to alice via trailers.
    let (status, page) = send(&app, "GET", &base, Some(&alice), None).await;
    assert_eq!(status, StatusCode::OK);
    let commits = page["commits"].as_array().unwrap();
    assert_eq!(commits.len(), 2);
    assert_eq!(page["hasMore"], false);
    assert_eq!(commits[0]["hash"], json!(c2));
    assert_eq!(commits[1]["hash"], json!(c1));
    assert_eq!(commits[0]["parents"][0], json!(c1));
    assert_eq!(commits[0]["authorName"], "alice");
    assert!(!commits[0]["principalId"].as_str().unwrap().is_empty());
    assert_eq!(commits[0]["principalType"], "user");

    // Keyset paging: limit=1 has more; before=c2 yields only c1; before=c1 (root) is empty.
    let (_, page) = send(&app, "GET", &format!("{base}?limit=1"), Some(&alice), None).await;
    assert_eq!(page["commits"].as_array().unwrap().len(), 1);
    assert_eq!(page["hasMore"], true);
    let (_, page) = send(
        &app,
        "GET",
        &format!("{base}?before={c2}"),
        Some(&alice),
        None,
    )
    .await;
    assert_eq!(page["commits"][0]["hash"], json!(c1));
    assert_eq!(page["hasMore"], false);
    let (_, page) = send(
        &app,
        "GET",
        &format!("{base}?before={c1}"),
        Some(&alice),
        None,
    )
    .await;
    assert_eq!(page["commits"].as_array().unwrap().len(), 0);

    // Per-file history (--follow) sees both commits for the note's path.
    let (_, page) = send(
        &app,
        "GET",
        &format!("{base}?path=n%C3%B6tes%2Fhello%20one.md"),
        Some(&alice),
        None,
    )
    .await;
    assert_eq!(page["commits"].as_array().unwrap().len(), 2);

    // Commit detail: change list with unicode path intact.
    let (status, detail) = send(&app, "GET", &format!("{base}/{c2}"), Some(&alice), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(detail["commit"]["hash"], json!(c2));
    let changes = detail["changes"].as_array().unwrap();
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0]["path"], "nötes/hello one.md");
    assert_eq!(changes[0]["status"], "modified");
    assert_eq!(changes[0]["kind"], "markdown");

    // Short (abbreviated) hashes resolve too.
    let (status, _) = send(
        &app,
        "GET",
        &format!("{base}/{}", &c2[..8]),
        Some(&alice),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Tree at each commit.
    let (_, tree) = send(
        &app,
        "GET",
        &format!("{base}/{c1}/tree"),
        Some(&alice),
        None,
    )
    .await;
    let entries = tree["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["path"], "nötes/hello one.md");
    assert_eq!(entries[0]["kind"], "markdown");

    // File content at both versions, and "absent" before it existed.
    let file_q = "path=n%C3%B6tes%2Fhello%20one.md".to_string();
    let (_, f1) = send(
        &app,
        "GET",
        &format!("{base}/{c1}/file?{file_q}"),
        Some(&alice),
        None,
    )
    .await;
    assert_eq!(f1["type"], "text");
    assert_eq!(f1["content"], "# v1\n");
    assert_eq!(f1["lang"], "markdown");
    let (_, f2) = send(
        &app,
        "GET",
        &format!("{base}/{c2}/file?{file_q}"),
        Some(&alice),
        None,
    )
    .await;
    assert_eq!(f2["content"], "# v2\nmore\n");
    let (_, missing) = send(
        &app,
        "GET",
        &format!("{base}/{c1}/file?path=never.md"),
        Some(&alice),
        None,
    )
    .await;
    assert_eq!(missing["type"], "absent");

    // Raw blob bytes round-trip.
    let (status, bytes) = send_raw(
        &app,
        "GET",
        &format!("{base}/{c1}/blob?{file_q}"),
        Some(&alice),
        Vec::new(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(bytes, b"# v1\n");

    // Path traversal is rejected; non-members are forbidden.
    let (status, _) = send(
        &app,
        "GET",
        &format!("{base}/{c1}/file?path=..%2Fetc%2Fpasswd"),
        Some(&alice),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let mallory = login(&app, "mallory").await;
    let (status, _) = send(&app, "GET", &base, Some(&mallory), None).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn rollback_restores_notes_requires_admin_and_stamps_trailer() {
    let (app, git_dir, _blobs) = history_test_app().await;
    let alice = login(&app, "alice").await;
    let (_, vault) = send(
        &app,
        "POST",
        "/api/vaults",
        Some(&alice),
        Some(json!({"name": "V"})),
    )
    .await;
    let vault_id = vault["id"].as_str().unwrap().to_string();
    let repo = git_dir.join(&vault_id);
    let base = format!("/api/vaults/{vault_id}/history/commits");

    // Commit 1: note A at v1.
    send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/notes"),
        Some(&alice),
        Some(json!({"path": "a.md", "content": "alpha v1\n"})),
    )
    .await;
    let c1 = wait_for_commit_count(&repo, 1).await;

    // Commit(s) 2: A edited and B created.
    send(
        &app,
        "PUT",
        &format!("/api/vaults/{vault_id}/notes/a.md"),
        Some(&alice),
        Some(json!({"content": "alpha v2 — changed\n"})),
    )
    .await;
    send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/notes"),
        Some(&alice),
        Some(json!({"path": "b.md", "content": "bravo\n"})),
    )
    .await;
    wait_for_commit_count(&repo, 2).await;

    // A member (non-admin) may browse history but not roll back.
    let (_, invite) = send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/invites"),
        Some(&alice),
        Some(json!({"role": "member"})),
    )
    .await;
    let bob = login(&app, "bob").await;
    let (status, _) = send(
        &app,
        "POST",
        "/api/invites/redeem",
        Some(&bob),
        Some(json!({"code": invite["code"]})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send(&app, "GET", &base, Some(&bob), None).await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send(
        &app,
        "POST",
        &format!("{base}/{c1}/rollback/preview"),
        Some(&bob),
        Some(json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (status, _) = send(
        &app,
        "POST",
        &format!("{base}/{c1}/rollback"),
        Some(&bob),
        Some(json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // Admin preview: modify a.md, delete b.md, nothing unrecoverable.
    let (status, plan) = send(
        &app,
        "POST",
        &format!("{base}/{c1}/rollback/preview"),
        Some(&alice),
        Some(json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(plan["targetCommit"], json!(c1));
    let changes = plan["changes"].as_array().unwrap();
    assert_eq!(changes.len(), 2);
    let by_path = |p: &str| changes.iter().find(|c| c["path"] == p).unwrap();
    assert_eq!(by_path("a.md")["action"], "modify");
    assert_eq!(by_path("b.md")["action"], "delete");
    assert_eq!(plan["unrecoverableBinaries"].as_array().unwrap().len(), 0);

    // Preview is a dry run: nothing changed yet.
    let (_, a) = send(
        &app,
        "GET",
        &format!("/api/vaults/{vault_id}/notes/a.md"),
        Some(&alice),
        None,
    )
    .await;
    assert_eq!(a["content"], "alpha v2 — changed\n");

    // Execute.
    let (status, result) = send(
        &app,
        "POST",
        &format!("{base}/{c1}/rollback"),
        Some(&alice),
        Some(json!({"pluginDbs": []})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(result["applied"], 1);
    assert_eq!(result["deleted"], 1);
    let rb_commit = result["commit"]
        .as_str()
        .expect("rollback commit hash")
        .to_string();

    // Authoritative state is restored: A back at v1, B gone.
    let (_, a) = send(
        &app,
        "GET",
        &format!("/api/vaults/{vault_id}/notes/a.md"),
        Some(&alice),
        None,
    )
    .await;
    assert_eq!(a["content"], "alpha v1\n");
    let (status, _) = send(
        &app,
        "GET",
        &format!("/api/vaults/{vault_id}/notes/b.md"),
        Some(&alice),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // The rollback commit is HEAD, attributed to alice, with subject + trailer,
    // and its tree matches the target commit exactly.
    let (ok, head) = git_out(&repo, &["rev-parse", "HEAD"]);
    assert!(ok);
    assert_eq!(head, rb_commit);
    let (_, meta) = git_out(&repo, &["log", "-1", "--format=%an|%s"]);
    assert!(meta.starts_with("alice|Rollback to "), "got {meta}");
    let (_, body) = git_out(&repo, &["log", "-1", "--format=%B"]);
    assert!(body.contains(&format!("Rollback-Of: {c1}")), "got {body}");
    let (ok, diff) = git_out(&repo, &["diff", "--name-only", &c1, "HEAD"]);
    assert!(ok);
    assert_eq!(diff, "", "rollback tree must match the target commit");

    // History API surfaces the rollbackOf marker.
    let (_, page) = send(&app, "GET", &base, Some(&alice), None).await;
    assert_eq!(page["commits"][0]["rollbackOf"], json!(c1));

    // Rolling back to the same target again is a no-op (no new commit).
    let (status, result) = send(
        &app,
        "POST",
        &format!("{base}/{c1}/rollback"),
        Some(&alice),
        Some(json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(result["applied"], 0);
    assert_eq!(result["deleted"], 0);
    assert_eq!(result["commit"], Value::Null);
    let (_, head_after) = git_out(&repo, &["rev-parse", "HEAD"]);
    assert_eq!(head_after, rb_commit);
}

#[tokio::test]
async fn rollback_restores_attachment_blob_from_git_after_gc() {
    let (app, git_dir, blob_dir) = history_test_app().await;
    let alice = login(&app, "alice").await;
    let (_, vault) = send(
        &app,
        "POST",
        "/api/vaults",
        Some(&alice),
        Some(json!({"name": "V"})),
    )
    .await;
    let vault_id = vault["id"].as_str().unwrap().to_string();
    let repo = git_dir.join(&vault_id);
    let base = format!("/api/vaults/{vault_id}/history/commits");

    // Commit 1: a small attachment (inlined verbatim into git).
    let payload = b"\x89PNG\r\n\x1a\nfake image bytes".to_vec();
    let att_url = format!("/api/vaults/{vault_id}/attachments/img/pic.png");
    let (status, up) = send_raw(&app, "PUT", &att_url, Some(&alice), payload.clone()).await;
    assert_eq!(status, StatusCode::OK);
    let up: Value = serde_json::from_slice(&up).unwrap();
    let hash = up["hash"].as_str().unwrap().to_string();
    let c1 = wait_for_commit_count(&repo, 1).await;

    // The file endpoint reports it as an inline binary.
    let (_, f) = send(
        &app,
        "GET",
        &format!("{base}/{c1}/file?path=img%2Fpic.png"),
        Some(&alice),
        None,
    )
    .await;
    assert_eq!(f["type"], "binary");
    assert_eq!(f["hash"], json!(hash));
    assert_eq!(f["inline"], true);

    // Delete the attachment, then GC the now-orphaned blob.
    let (status, _) = send(&app, "DELETE", &att_url, Some(&alice), None).await;
    assert_eq!(status, StatusCode::OK);
    wait_for_commit_count(&repo, 2).await;
    let (status, gc) = send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/storage/gc-blobs"),
        Some(&alice),
        Some(json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "gc failed: {gc}");
    let blob_path = blob_dir.join(&vault_id).join(&hash);
    assert!(!blob_path.exists(), "blob should be GC'd");

    // Preview: the attachment comes back via blob re-insert from git bytes.
    let (status, plan) = send(
        &app,
        "POST",
        &format!("{base}/{c1}/rollback/preview"),
        Some(&alice),
        Some(json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let restore = plan["changes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["path"] == "img/pic.png")
        .expect("attachment in plan");
    assert_eq!(restore["action"], "restoreBlob");
    assert_eq!(plan["unrecoverableBinaries"].as_array().unwrap().len(), 0);

    // Execute, then the attachment downloads byte-identically again.
    let (status, result) = send(
        &app,
        "POST",
        &format!("{base}/{c1}/rollback"),
        Some(&alice),
        Some(json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(result["blobsRestored"], 1);
    assert!(blob_path.exists(), "blob restored to the store");
    assert_eq!(std::fs::read(&blob_path).unwrap(), payload);
    let (status, body) = send_raw(&app, "GET", &att_url, Some(&alice), Vec::new()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, payload);
}

// ---------- single-file rollback e2e ----------

/// Helper: create a vault + alice admin + bob member, returning (app, repo,
/// vault_id, alice, bob, base_url).
async fn single_file_rollback_setup() -> (Router, std::path::PathBuf, String, String, String, String)
{
    let (app, git_dir, _blobs) = history_test_app().await;
    let alice = login(&app, "alice").await;
    let (_, vault) = send(
        &app,
        "POST",
        "/api/vaults",
        Some(&alice),
        Some(json!({"name": "V"})),
    )
    .await;
    let vault_id = vault["id"].as_str().unwrap().to_string();
    let repo = git_dir.join(&vault_id);
    let base = format!("/api/vaults/{vault_id}/history/commits");

    // Invite bob as a member (non-admin) for the non-admin test.
    let (_, invite) = send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/invites"),
        Some(&alice),
        Some(json!({"role": "member"})),
    )
    .await;
    let bob = login(&app, "bob").await;
    let _ = send(
        &app,
        "POST",
        "/api/invites/redeem",
        Some(&bob),
        Some(json!({"code": invite["code"]})),
    )
    .await;

    (app, repo, vault_id, alice, bob, base)
}

#[tokio::test]
async fn rollback_single_file_restores_one_path_only() {
    let (app, repo, vault_id, alice, _bob, base) = single_file_rollback_setup().await;

    // c1: a.md at v1.
    send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/notes"),
        Some(&alice),
        Some(json!({"path": "a.md", "content": "alpha v1\n"})),
    )
    .await;
    let c1 = wait_for_commit_count(&repo, 1).await;

    // c2: edit a.md and create b.md.
    send(
        &app,
        "PUT",
        &format!("/api/vaults/{vault_id}/notes/a.md"),
        Some(&alice),
        Some(json!({"content": "alpha v2\n"})),
    )
    .await;
    send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/notes"),
        Some(&alice),
        Some(json!({"path": "b.md", "content": "bravo\n"})),
    )
    .await;
    wait_for_commit_count(&repo, 2).await;

    // Single-file rollback a.md to c1.
    let (status, plan) = send(
        &app,
        "POST",
        &format!("{base}/{c1}/rollback/preview?path=a.md"),
        Some(&alice),
        Some(json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let changes = plan["changes"].as_array().unwrap();
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0]["path"], json!("a.md"));
    assert_eq!(changes[0]["action"], json!("modify"));

    let (status, result) = send(
        &app,
        "POST",
        &format!("{base}/{c1}/rollback?path=a.md"),
        Some(&alice),
        Some(json!({"pluginDbs": []})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(result["applied"], 1);
    assert_eq!(result["deleted"], 0);

    // a.md restored; b.md untouched.
    let (_, a) = send(
        &app,
        "GET",
        &format!("/api/vaults/{vault_id}/notes/a.md"),
        Some(&alice),
        None,
    )
    .await;
    assert_eq!(a["content"], "alpha v1\n");
    let (_, b) = send(
        &app,
        "GET",
        &format!("/api/vaults/{vault_id}/notes/b.md"),
        Some(&alice),
        None,
    )
    .await;
    assert_eq!(b["content"], "bravo\n");
}

#[tokio::test]
async fn rollback_single_file_absent_at_target_deletes_only_that_file() {
    let (app, repo, vault_id, alice, _bob, base) = single_file_rollback_setup().await;

    // c1: a.md only.
    send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/notes"),
        Some(&alice),
        Some(json!({"path": "a.md", "content": "alpha\n"})),
    )
    .await;
    let c1 = wait_for_commit_count(&repo, 1).await;

    // c2: create b.md.
    send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/notes"),
        Some(&alice),
        Some(json!({"path": "b.md", "content": "bravo\n"})),
    )
    .await;
    wait_for_commit_count(&repo, 2).await;

    // Single-file rollback b.md to c1 (where b.md is absent) -> delete b.md.
    let (status, result) = send(
        &app,
        "POST",
        &format!("{base}/{c1}/rollback?path=b.md"),
        Some(&alice),
        Some(json!({"pluginDbs": []})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(result["applied"], 0);
    assert_eq!(result["deleted"], 1);

    let (status, _) = send(
        &app,
        "GET",
        &format!("/api/vaults/{vault_id}/notes/b.md"),
        Some(&alice),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (_, a) = send(
        &app,
        "GET",
        &format!("/api/vaults/{vault_id}/notes/a.md"),
        Some(&alice),
        None,
    )
    .await;
    assert_eq!(a["content"], "alpha\n");
}

#[tokio::test]
async fn rollback_single_file_unchanged_returns_noop() {
    let (app, repo, vault_id, alice, _bob, base) = single_file_rollback_setup().await;

    // c1: a.md.
    send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/notes"),
        Some(&alice),
        Some(json!({"path": "a.md", "content": "alpha\n"})),
    )
    .await;
    wait_for_commit_count(&repo, 1).await;

    // c2: create b.md (a.md unchanged).
    send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/notes"),
        Some(&alice),
        Some(json!({"path": "b.md", "content": "bravo\n"})),
    )
    .await;
    let c2 = wait_for_commit_count(&repo, 2).await;

    // Single-file rollback a.md to c2 (where a.md is unchanged) -> no-op.
    let (status, plan) = send(
        &app,
        "POST",
        &format!("{base}/{c2}/rollback/preview?path=a.md"),
        Some(&alice),
        Some(json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(plan["changes"].as_array().unwrap().len(), 0);

    let (status, result) = send(
        &app,
        "POST",
        &format!("{base}/{c2}/rollback?path=a.md"),
        Some(&alice),
        Some(json!({"pluginDbs": []})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(result["applied"], 0);
    assert_eq!(result["deleted"], 0);
    assert_eq!(result["commit"], Value::Null);
}

#[tokio::test]
async fn rollback_single_file_rename_uses_target_path() {
    let (app, repo, vault_id, alice, _bob, base) = single_file_rollback_setup().await;

    // c1: old.md with content "old v1".
    send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/notes"),
        Some(&alice),
        Some(json!({"path": "old.md", "content": "old v1\n"})),
    )
    .await;
    let c1 = wait_for_commit_count(&repo, 1).await;

    // c2: rename old.md -> new.md via the note-moves API (git detects R100).
    let (status, _) = send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/note-moves/old.md"),
        Some(&alice),
        Some(json!({"toPath": "new.md"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    wait_for_commit_count(&repo, 2).await;

    // c3: edit new.md so its content differs from old.md's c1 content.
    let (status, _) = send(
        &app,
        "PUT",
        &format!("/api/vaults/{vault_id}/notes/new.md"),
        Some(&alice),
        Some(json!({"content": "new v2\n"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    wait_for_commit_count(&repo, 3).await;

    // Single-file rollback new.md to c1, reading from old.md at c1.
    let (status, result) = send(
        &app,
        "POST",
        &format!("{base}/{c1}/rollback?path=new.md&targetPath=old.md"),
        Some(&alice),
        Some(json!({"pluginDbs": []})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(result["applied"], 1);

    // new.md now has old.md's c1 content; old.md is not recreated.
    let (_, n) = send(
        &app,
        "GET",
        &format!("/api/vaults/{vault_id}/notes/new.md"),
        Some(&alice),
        None,
    )
    .await;
    assert_eq!(n["content"], "old v1\n");
    let (status, _) = send(
        &app,
        "GET",
        &format!("/api/vaults/{vault_id}/notes/old.md"),
        Some(&alice),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn rollback_single_file_create_when_current_absent_with_rename() {
    let (app, repo, vault_id, alice, _bob, base) = single_file_rollback_setup().await;

    // c1: old.md with content "old v1".
    send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/notes"),
        Some(&alice),
        Some(json!({"path": "old.md", "content": "old v1\n"})),
    )
    .await;
    let c1 = wait_for_commit_count(&repo, 1).await;

    // c2: rename old.md -> new.md.
    send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/note-moves/old.md"),
        Some(&alice),
        Some(json!({"toPath": "new.md"})),
    )
    .await;
    wait_for_commit_count(&repo, 2).await;

    // c3: delete new.md so the current path is absent.
    send(
        &app,
        "DELETE",
        &format!("/api/vaults/{vault_id}/notes/new.md"),
        Some(&alice),
        None,
    )
    .await;
    wait_for_commit_count(&repo, 3).await;

    // Single-file rollback new.md (currently absent) to c1, reading old.md.
    let (status, result) = send(
        &app,
        "POST",
        &format!("{base}/{c1}/rollback?path=new.md&targetPath=old.md"),
        Some(&alice),
        Some(json!({"pluginDbs": []})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(result["applied"], 1);

    let (_, n) = send(
        &app,
        "GET",
        &format!("/api/vaults/{vault_id}/notes/new.md"),
        Some(&alice),
        None,
    )
    .await;
    assert_eq!(n["content"], "old v1\n");
}

#[tokio::test]
async fn rollback_single_file_rejects_kind_change() {
    let (app, repo, vault_id, alice, _bob, base) = single_file_rollback_setup().await;

    // c1: a.md (markdown).
    send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/notes"),
        Some(&alice),
        Some(json!({"path": "a.md", "content": "# markdown\n"})),
    )
    .await;
    let _c1 = wait_for_commit_count(&repo, 1).await;

    // c2: create a.canvas (structured). a.md still exists as markdown.
    send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/canvases"),
        Some(&alice),
        Some(json!({"path": "a.canvas", "value": {"nodes": [], "edges": []}})),
    )
    .await;
    let c2 = wait_for_commit_count(&repo, 2).await;

    // Single-file rollback a.md to c2, where a.canvas exists but a.md does not.
    // Current a.md is markdown; target a.md is absent; target_path=a.md is
    // absent at c2 -> would delete a.md. That's not a kind change. Instead,
    // test the kind-change path: rollback a.md to a state where a.md would be
    // created from a.canvas content. Use targetPath=a.canvas with path=a.md:
    // current a.md is markdown, target a.canvas is canvas -> kind mismatch.
    let (status, body) = send(
        &app,
        "POST",
        &format!("{base}/{c2}/rollback/preview?path=a.md&targetPath=a.canvas"),
        Some(&alice),
        Some(json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let msg = body["error"].as_str().unwrap_or("");
    assert!(
        msg.contains("kind"),
        "expected kind-change rejection, got: {msg}"
    );
}

#[tokio::test]
async fn rollback_path_rejects_traversal() {
    let (app, _repo, _vault_id, alice, _bob, base) = single_file_rollback_setup().await;
    let c1 = "0".repeat(40);
    let (status, _) = send(
        &app,
        "POST",
        &format!("{base}/{c1}/rollback/preview?path=../x"),
        Some(&alice),
        Some(json!({})),
    )
    .await;
    // Either 400 (bad path) or 404 (unknown hash) is acceptable; the path
    // validation must reject `..` before any planning. Axum runs query
    // extraction before hash resolution, so 400 is expected.
    assert!(
        status == StatusCode::BAD_REQUEST || status == StatusCode::NOT_FOUND,
        "expected 400 or 404, got {status}"
    );
}

#[tokio::test]
async fn rollback_targetpath_without_path_rejected() {
    let (app, _repo, _vault_id, alice, _bob, base) = single_file_rollback_setup().await;
    let c1 = "0".repeat(40);
    let (status, _) = send(
        &app,
        "POST",
        &format!("{base}/{c1}/rollback/preview?targetPath=old.md"),
        Some(&alice),
        Some(json!({})),
    )
    .await;
    assert!(
        status == StatusCode::BAD_REQUEST || status == StatusCode::NOT_FOUND,
        "expected 400 or 404, got {status}"
    );
}

#[tokio::test]
async fn rollback_path_rejects_plugin_dbs() {
    let (app, repo, vault_id, alice, _bob, base) = single_file_rollback_setup().await;
    send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/notes"),
        Some(&alice),
        Some(json!({"path": "a.md", "content": "alpha\n"})),
    )
    .await;
    let c1 = wait_for_commit_count(&repo, 1).await;

    let (status, body) = send(
        &app,
        "POST",
        &format!("{base}/{c1}/rollback?path=a.md"),
        Some(&alice),
        Some(json!({"pluginDbs": [{"plugin": "x", "name": "y"}]})),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let msg = body["error"].as_str().unwrap_or("");
    assert!(msg.contains("pluginDbs"), "got: {msg}");
}

#[tokio::test]
async fn rollback_path_rejects_non_admin() {
    let (app, repo, vault_id, alice, bob, base) = single_file_rollback_setup().await;
    send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/notes"),
        Some(&alice),
        Some(json!({"path": "a.md", "content": "alpha\n"})),
    )
    .await;
    let c1 = wait_for_commit_count(&repo, 1).await;

    let (status, _) = send(
        &app,
        "POST",
        &format!("{base}/{c1}/rollback?path=a.md"),
        Some(&bob),
        Some(json!({"pluginDbs": []})),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn list_commits_path_returns_path_at_commit() {
    let (app, repo, vault_id, alice, _bob, base) = single_file_rollback_setup().await;

    // c1: old.md.
    send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/notes"),
        Some(&alice),
        Some(json!({"path": "old.md", "content": "old v1\n"})),
    )
    .await;
    wait_for_commit_count(&repo, 1).await;

    // c2: rename old.md -> new.md via the note-moves API (git detects R100).
    let (status, _) = send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/note-moves/old.md"),
        Some(&alice),
        Some(json!({"toPath": "new.md"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "note-moves failed");
    wait_for_commit_count(&repo, 2).await;

    // Path-filtered list for new.md should include pathAtCommit on each row.
    let (status, page) = send(
        &app,
        "GET",
        &format!("{base}?path=new.md"),
        Some(&alice),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "list_commits path failed");
    let commits = page["commits"].as_array().unwrap();
    assert!(
        commits.len() >= 2,
        "expected at least 2 commits, got {}",
        commits.len()
    );
    // Newest first: c2 (new.md exists), c1 (file was old.md).
    // The HEAD commit's pathAtCommit should be new.md.
    assert_eq!(commits[0]["pathAtCommit"], json!("new.md"));
    // The older commit's pathAtCommit should be old.md (rename walked back).
    assert_eq!(commits[1]["pathAtCommit"], json!("old.md"));

    // Non-path-filtered list omits pathAtCommit.
    let (_, page_all) = send(&app, "GET", &base, Some(&alice), None).await;
    let commits_all = page_all["commits"].as_array().unwrap();
    assert!(
        commits_all[0].get("pathAtCommit").is_none() || commits_all[0]["pathAtCommit"].is_null(),
        "non-path-filtered list must not include pathAtCommit"
    );
}

#[tokio::test]
async fn public_share_lifecycle_create_view_and_revoke() {
    let app = test_app().await;
    let alice = login(&app, "alice").await;
    let (status, vault) = send(
        &app,
        "POST",
        "/api/vaults",
        Some(&alice),
        Some(json!({"name":"v"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let vault_id = vault["id"].as_str().unwrap().to_string();
    let (status, _) = send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/notes"),
        Some(&alice),
        Some(json!({"path": "Folder/Shared.md", "content": "# Hello\npublic"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Create a share; re-creating returns the same id (idempotent).
    let (status, share) = send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/shares"),
        Some(&alice),
        Some(json!({"path": "Folder/Shared.md"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let share_id = share["id"].as_str().unwrap().to_string();
    assert!(share["url"]
        .as_str()
        .unwrap()
        .ends_with(&format!("/view/{share_id}")));
    let (status, again) = send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/shares"),
        Some(&alice),
        Some(json!({"path": "Folder/Shared.md"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(again["id"], share["id"]);

    // GET by path reports the share; sharing requires membership.
    let (status, looked_up) = send(
        &app,
        "GET",
        &format!("/api/vaults/{vault_id}/shares?path=Folder%2FShared.md"),
        Some(&alice),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(looked_up["share"]["id"], share["id"]);
    let mallory = login(&app, "mallory").await;
    let (status, _) = send(
        &app,
        "POST",
        &format!("/api/vaults/{vault_id}/shares"),
        Some(&mallory),
        Some(json!({"path": "Folder/Shared.md"})),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // The public view endpoint needs no auth and carries the note as a Yjs update.
    let (status, view) = send(&app, "GET", &format!("/api/view/{share_id}"), None, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(view["title"], "Shared");
    assert_eq!(view["path"], "Folder/Shared.md");
    assert_eq!(view["epoch"], 0);
    {
        use base64::Engine;
        use yrs::updates::decoder::Decode;
        use yrs::{GetString, Transact};
        let update = base64::engine::general_purpose::STANDARD
            .decode(view["updateB64"].as_str().unwrap())
            .unwrap();
        let doc = yrs::Doc::new();
        {
            let mut txn = doc.transact_mut();
            txn.apply_update(yrs::Update::decode_v1(&update).unwrap());
        }
        let text = doc.get_or_insert_text("contents");
        assert_eq!(text.get_string(&doc.transact()), "# Hello\npublic");
    }

    // Unknown wikilink targets and unshared notes don't resolve.
    let (status, _) = send(
        &app,
        "GET",
        &format!("/api/view/{share_id}/resolve?target=Nope"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    // A note that *is* shared resolves to its share id.
    let (status, resolved) = send(
        &app,
        "GET",
        &format!("/api/view/{share_id}/resolve?target=Shared"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(resolved["shareId"], share["id"]);

    // Revoke; the public endpoints stop working.
    let (status, _) = send(
        &app,
        "DELETE",
        &format!("/api/vaults/{vault_id}/shares?path=Folder%2FShared.md"),
        Some(&alice),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send(&app, "GET", &format!("/api/view/{share_id}"), None, None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = send(
        &app,
        "DELETE",
        &format!("/api/vaults/{vault_id}/shares?path=Folder%2FShared.md"),
        Some(&alice),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn public_attachment_share_is_scoped_to_the_shared_version_and_revocable() {
    let app = test_app().await;
    let alice = login(&app, "alice").await;
    let (_, vault) = send(
        &app,
        "POST",
        "/api/vaults",
        Some(&alice),
        Some(json!({"name": "v"})),
    )
    .await;
    let vault_id = vault["id"].as_str().unwrap();
    let attachment_path = "images/public.png";
    let attachment_url = format!("/api/vaults/{vault_id}/attachments/{attachment_path}");
    let first_bytes = b"\x89PNG\r\n\x1a\nfirst image".to_vec();
    let (status, _) = send_raw(
        &app,
        "PUT",
        &attachment_url,
        Some(&alice),
        first_bytes.clone(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let share_api = format!("/api/vaults/{vault_id}/attachment-shares");
    let (status, share) = send(
        &app,
        "POST",
        &share_api,
        Some(&alice),
        Some(json!({"path": attachment_path})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let share_id = share["id"].as_str().unwrap();
    assert!(share["url"]
        .as_str()
        .unwrap()
        .ends_with(&format!("/a/{share_id}")));

    let public_url = format!("/a/{share_id}");
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(&public_url)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE).unwrap(),
        "image/png"
    );
    assert_eq!(
        response
            .headers()
            .get(header::X_CONTENT_TYPE_OPTIONS)
            .unwrap(),
        "nosniff"
    );
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(body.as_ref(), first_bytes.as_slice());

    // Idempotent while the path still points at the same content.
    let (status, same) = send(
        &app,
        "POST",
        &share_api,
        Some(&alice),
        Some(json!({"path": attachment_path})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(same["id"], share["id"]);

    // Replacing the file invalidates the link until the owner explicitly
    // shares the new version.
    let (status, _) = send_raw(
        &app,
        "PUT",
        &attachment_url,
        Some(&alice),
        b"\x89PNG\r\n\x1a\nsecond image".to_vec(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send(&app, "GET", &public_url, None, None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, replacement) = send(
        &app,
        "POST",
        &share_api,
        Some(&alice),
        Some(json!({"path": attachment_path})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(replacement["id"], share["id"]);
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(&public_url)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(body.as_ref(), b"\x89PNG\r\n\x1a\nsecond image");

    let race_path = "images/race.png";
    let (status, _) = send_raw(
        &app,
        "PUT",
        &format!("/api/vaults/{vault_id}/attachments/{race_path}"),
        Some(&alice),
        b"\x89PNG\r\n\x1a\nrace image".to_vec(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let first_create = send(
        &app,
        "POST",
        &share_api,
        Some(&alice),
        Some(json!({"path": race_path})),
    );
    let second_create = send(
        &app,
        "POST",
        &share_api,
        Some(&alice),
        Some(json!({"path": race_path})),
    );
    let ((first_status, first_share), (second_status, second_share)) =
        tokio::join!(first_create, second_create);
    assert_eq!(first_status, StatusCode::OK);
    assert_eq!(second_status, StatusCode::OK);
    assert_eq!(first_share["id"], second_share["id"]);

    let mallory = login(&app, "mallory").await;
    let (status, _) = send(
        &app,
        "POST",
        &share_api,
        Some(&mallory),
        Some(json!({"path": attachment_path})),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    let (status, _) = send(
        &app,
        "DELETE",
        &format!("{share_api}?path=images%2Fpublic.png"),
        Some(&alice),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let replacement_url = format!("/a/{}", replacement["id"].as_str().unwrap());
    let (status, _) = send(&app, "GET", &replacement_url, None, None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}
