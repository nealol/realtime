use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use axum::Json;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, EntityTrait, PaginatorTrait, QueryFilter, Set, TransactionTrait,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use utoipa::ToSchema;

use crate::audit;
use crate::caps;
use crate::crdt::{mint_client_token, Level};
use crate::entities::{
    git_backups, invites, memberships, permissions, remote_cursor_tokens, remote_cursors, users,
    vault_files, vaults,
};
use crate::error::{AppError, AppResult};
use crate::jobs::JobView;
use crate::session::{
    bearer_token, hash_token, now_millis, revoke_session, ApiActor, ApiPrincipal, AuthUser,
};
use crate::state::{AppState, Principal, PrincipalActor};
use crate::words::generate_invite_code;

const ROLE_ADMIN: &str = "admin";
const ROLE_MEMBER: &str = "member";
const INVITE_TTL_MS: i64 = 1000 * 60 * 60 * 24 * 7;
/// Opaque sync grants expire after one hour, matching the previous provider
/// token lifetime. Clients transparently refresh after expiry or server restart.
const SYNC_GRANT_TTL_MS: i64 = 1000 * 60 * 60;

// ---------- shared response shapes ----------

#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct MeResponse {
    pub user_id: String,
    pub email: String,
    pub git_email: Option<String>,
    pub display_name: String,
    pub picture_url: Option<String>,
    pub avatar_url_override: Option<String>,
    pub avatar_url: Option<String>,
}

#[derive(Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct UpdateMeBody {
    #[serde(default, deserialize_with = "deserialize_some")]
    pub git_email: Option<Option<String>>,
    #[serde(default, deserialize_with = "deserialize_some")]
    pub avatar_url_override: Option<Option<String>>,
}

/// Custom deserializer that wraps the value in `Some(...)`, so that a field
/// present as `null` becomes `Some(None)` (clear it) rather than `None` (field
/// absent, leave unchanged). This distinguishes explicit null from omission in
/// `Option<Option<T>>` partial-update bodies.
fn deserialize_some<'de, T, D>(deserializer: D) -> Result<Option<T>, D::Error>
where
    T: Deserialize<'de>,
    D: serde::Deserializer<'de>,
{
    Deserialize::deserialize(deserializer).map(Some)
}

#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ServerInfoResponse {
    pub server_id: String,
    /// Server release semver (from `Cargo.toml`). Human/operator-facing only;
    /// not used for compatibility gating — clients gate on `caps`.
    pub version: String,
    /// Named capability versions for each compatibility surface. Clients
    /// intersect these against their own accepted values; see `caps` module.
    pub caps: BTreeMap<String, String>,
    /// Cap names the client must understand to use this server. Empty in v1;
    /// exists so future optional caps can be added without hard-blocking old
    /// clients that don't know the name.
    pub required_caps: Vec<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VaultResponse {
    pub id: String,
    pub name: String,
    pub role: String,
    pub created_by: String,
    pub owner: bool,
}

// ---------- membership guards ----------

async fn membership(
    state: &AppState,
    user_id: &str,
    vault_id: &str,
) -> AppResult<Option<memberships::Model>> {
    Ok(memberships::Entity::find()
        .filter(memberships::Column::UserId.eq(user_id))
        .filter(memberships::Column::VaultId.eq(vault_id))
        .one(&state.db)
        .await?)
}

pub(crate) async fn require_member(
    state: &AppState,
    user_id: &str,
    vault_id: &str,
) -> AppResult<memberships::Model> {
    membership(state, user_id, vault_id)
        .await?
        .ok_or(AppError::Forbidden)
}

pub(crate) async fn require_admin(
    state: &AppState,
    user_id: &str,
    vault_id: &str,
) -> AppResult<memberships::Model> {
    let m = require_member(state, user_id, vault_id).await?;
    if m.role != ROLE_ADMIN {
        return Err(AppError::Forbidden);
    }
    Ok(m)
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JobActionBody {
    pub intent_key: String,
}

pub async fn list_jobs(
    State(state): State<AppState>,
    AuthUser(user): AuthUser,
    Path(vault_id): Path<String>,
) -> AppResult<Json<Vec<JobView>>> {
    require_admin(&state, &user.id, &vault_id).await?;
    let mut jobs = state
        .jobs
        .list()
        .await
        .map_err(|error| AppError::Internal(error.to_string()))?;
    jobs.retain(|job| {
        job.payload.get("vault_id").and_then(Value::as_str) == Some(vault_id.as_str())
    });
    Ok(Json(jobs))
}

pub async fn retry_job(
    State(state): State<AppState>,
    AuthUser(user): AuthUser,
    Path(vault_id): Path<String>,
    Json(body): Json<JobActionBody>,
) -> AppResult<Json<Value>> {
    require_admin(&state, &user.id, &vault_id).await?;
    require_job_in_vault(&state, &vault_id, &body.intent_key).await?;
    state
        .jobs
        .retry(&body.intent_key)
        .await
        .map_err(|error| AppError::Conflict(error.to_string()))?;
    Ok(Json(serde_json::json!({ "ok": true })))
}

pub async fn cancel_job(
    State(state): State<AppState>,
    AuthUser(user): AuthUser,
    Path(vault_id): Path<String>,
    Json(body): Json<JobActionBody>,
) -> AppResult<Json<Value>> {
    require_admin(&state, &user.id, &vault_id).await?;
    require_job_in_vault(&state, &vault_id, &body.intent_key).await?;
    state
        .jobs
        .cancel(&body.intent_key)
        .await
        .map_err(|error| AppError::Conflict(error.to_string()))?;
    Ok(Json(serde_json::json!({ "ok": true })))
}

async fn require_job_in_vault(state: &AppState, vault_id: &str, intent_key: &str) -> AppResult<()> {
    let jobs = state
        .jobs
        .list()
        .await
        .map_err(|error| AppError::Internal(error.to_string()))?;
    jobs.iter()
        .any(|job| {
            job.intent_key == intent_key
                && job.payload.get("vault_id").and_then(Value::as_str) == Some(vault_id)
        })
        .then_some(())
        .ok_or(AppError::NotFound)
}

// ---------- auth / session ----------

/// Public: advertise this server's stable id so clients can scope cached
/// session tokens per server, plus release version and named capability
/// versions for client-side compatibility gating. No authentication required.
pub async fn server_info(State(state): State<AppState>) -> Json<ServerInfoResponse> {
    let caps = caps::caps()
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect::<BTreeMap<_, _>>();
    Json(ServerInfoResponse {
        server_id: state.server_id.clone(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        caps,
        // v1: all four caps are mandatory and known by the v1 client, so none
        // need to be flagged here. Future optional caps can be added to `caps`
        // without listing them here, then moved here once all deployed clients
        // Epoch replacement requires clients to discard old Yjs struct
        // history. A client that does not understand this cap cannot write
        // safely after the server switches epochs.
        required_caps: caps::REQUIRED
            .iter()
            .map(|name| (*name).to_string())
            .collect(),
    })
}

pub async fn me(AuthUser(user): AuthUser) -> Json<MeResponse> {
    Json(me_response(user))
}

pub async fn update_me(
    State(state): State<AppState>,
    AuthUser(user): AuthUser,
    Json(body): Json<UpdateMeBody>,
) -> AppResult<Json<MeResponse>> {
    let mut active: users::ActiveModel = user.into();

    // Partial-update semantics: a field present as `null` or `""` clears it;
    // a field present as a non-empty string validates and stores it; a field
    // absent (None) is left unchanged — unless BOTH are absent, in which case
    // we preserve the legacy behavior of clearing `git_email` for old clients
    // that sent `{}` to clear.
    let legacy_clear_git_email = body.git_email.is_none() && body.avatar_url_override.is_none();

    match body.git_email {
        Some(Some(email)) => {
            let trimmed = email.trim();
            if trimmed.is_empty() {
                active.git_email = Set(None);
            } else {
                validate_git_email(trimmed)?;
                active.git_email = Set(Some(trimmed.to_string()));
            }
        }
        Some(None) => {
            active.git_email = Set(None);
        }
        None if legacy_clear_git_email => {
            active.git_email = Set(None);
        }
        None => {}
    }

    match body.avatar_url_override {
        Some(Some(url)) => {
            let trimmed = url.trim();
            if trimmed.is_empty() {
                active.avatar_url_override = Set(None);
            } else {
                validate_avatar_url(trimmed)?;
                active.avatar_url_override = Set(Some(trimmed.to_string()));
            }
        }
        Some(None) => {
            active.avatar_url_override = Set(None);
        }
        None => {}
    }

    let user = active.update(&state.db).await?;
    Ok(Json(me_response(user)))
}

/// Build a `MeResponse` from a user model, computing the effective avatar URL
/// (override first, then IdP picture).
fn me_response(user: users::Model) -> MeResponse {
    let avatar_url = effective_avatar_url(&user);
    MeResponse {
        user_id: user.id,
        email: user.email,
        git_email: user.git_email,
        display_name: user.display_name,
        picture_url: user.picture_url,
        avatar_url_override: user.avatar_url_override,
        avatar_url,
    }
}

/// Effective avatar URL: override first, then the OpenID `picture` claim.
fn effective_avatar_url(user: &users::Model) -> Option<String> {
    user.avatar_url_override
        .clone()
        .or_else(|| user.picture_url.clone())
}

/// Validate a self-settable git author email before persisting it. The value
/// flows unescaped into `format!("{name} <{email}>")` passed to
/// `git commit --author`, and into `Co-authored-by: {name} <{email}>` trailers
/// (see `git::build_commit_meta`). A malicious value like
/// `a@b.com>\nSigned-off-by: attacker <a@x>\n` would inject arbitrary trailers
/// / corrupt the author line in the shared audit history, so reject anything
/// that could break out of the `Name <email>` envelope or inject `Key: Value`
/// lines, and require a basic email shape.
fn validate_git_email(value: &str) -> Result<(), AppError> {
    if value.len() > 254 {
        return Err(AppError::BadRequest("git_email too long".into()));
    }
    // No control chars, whitespace, or angle brackets: these could close the
    // `<...>` envelope early or start a new trailer line.
    if value
        .chars()
        .any(|c| c.is_control() || c.is_whitespace() || c == '<' || c == '>')
    {
        return Err(AppError::BadRequest(
            "git_email contains invalid characters".into(),
        ));
    }
    // Basic email shape: exactly one '@', non-empty local and domain, domain
    // contains at least one '.'.
    if value.matches('@').count() != 1 {
        return Err(AppError::BadRequest(
            "git_email is not a valid email address".into(),
        ));
    }
    let (local, domain) = value.split_once('@').expect("checked exactly one '@'");
    if local.is_empty() || domain.is_empty() || !domain.contains('.') {
        return Err(AppError::BadRequest(
            "git_email is not a valid email address".into(),
        ));
    }
    Ok(())
}

/// Validate a self-settable avatar URL. Must be a parseable `http` or `https`
/// URL, at most 2048 bytes, with no ASCII control characters or whitespace.
fn validate_avatar_url(value: &str) -> Result<(), AppError> {
    if value.len() > 2048 {
        return Err(AppError::BadRequest(
            "avatar_url is not a valid http(s) URL".into(),
        ));
    }
    if value
        .bytes()
        .any(|b| b.is_ascii_control() || b.is_ascii_whitespace())
    {
        return Err(AppError::BadRequest(
            "avatar_url contains invalid characters".into(),
        ));
    }
    let url = url::Url::parse(value)
        .map_err(|_| AppError::BadRequest("avatar_url is not a valid http(s) URL".into()))?;
    if url.scheme() != "http" && url.scheme() != "https" {
        return Err(AppError::BadRequest(
            "avatar_url is not a valid http(s) URL".into(),
        ));
    }
    Ok(())
}

// ---------- vaults ----------

#[derive(Deserialize)]
pub struct CreateVaultBody {
    pub name: String,
}

pub async fn create_vault(
    State(state): State<AppState>,
    AuthUser(user): AuthUser,
    Json(body): Json<CreateVaultBody>,
) -> AppResult<Json<VaultResponse>> {
    let now = now_millis();
    let vault_id = uuid::Uuid::new_v4().to_string();

    let txn = state.db.begin().await?;

    vaults::ActiveModel {
        id: Set(vault_id.clone()),
        name: Set(body.name.clone()),
        created_by: Set(user.id.clone()),
        created_at: Set(now),
    }
    .insert(&txn)
    .await?;

    memberships::ActiveModel {
        id: Set(uuid::Uuid::new_v4().to_string()),
        user_id: Set(user.id.clone()),
        vault_id: Set(vault_id.clone()),
        role: Set(ROLE_ADMIN.to_string()),
        created_at: Set(now),
    }
    .insert(&txn)
    .await?;

    txn.commit().await?;

    // Create the vault's index doc up front. Historically clients created it
    // lazily via /api/doc-token on first connect, which left REST/MCP-only
    // vaults without one — any cursor write touching the index (e.g.
    // create_note) then failed because the index document did not exist.
    state.ensure_vault_document(&vault_id, &vault_id).await?;

    Ok(Json(VaultResponse {
        id: vault_id,
        name: body.name,
        role: ROLE_ADMIN.to_string(),
        created_by: user.id,
        owner: true,
    }))
}

pub async fn list_vaults(
    State(state): State<AppState>,
    AuthUser(user): AuthUser,
) -> AppResult<Json<Vec<VaultResponse>>> {
    let mems = memberships::Entity::find()
        .filter(memberships::Column::UserId.eq(&user.id))
        .all(&state.db)
        .await?;

    let mut out = Vec::with_capacity(mems.len());
    for m in mems {
        if let Some(v) = vaults::Entity::find_by_id(m.vault_id.clone())
            .one(&state.db)
            .await?
        {
            out.push(VaultResponse {
                id: v.id,
                name: v.name,
                role: m.role,
                owner: v.created_by == user.id,
                created_by: v.created_by,
            });
        }
    }
    Ok(Json(out))
}

// ---------- invites ----------

#[derive(Deserialize)]
pub struct CreateInviteBody {
    pub role: Option<String>,
}

#[derive(Serialize)]
pub struct InviteResponse {
    pub code: String,
}

pub async fn create_invite(
    State(state): State<AppState>,
    AuthUser(user): AuthUser,
    Path(vault_id): Path<String>,
    Json(body): Json<CreateInviteBody>,
) -> AppResult<Json<InviteResponse>> {
    require_admin(&state, &user.id, &vault_id).await?;

    let role = match body.role.as_deref() {
        Some(ROLE_ADMIN) => ROLE_ADMIN,
        _ => ROLE_MEMBER,
    };

    // Retry on the (rare) code collision.
    for _ in 0..5 {
        let code = generate_invite_code();
        let res = invites::ActiveModel {
            id: Set(uuid::Uuid::new_v4().to_string()),
            vault_id: Set(vault_id.clone()),
            code: Set(code.clone()),
            role_granted: Set(role.to_string()),
            created_by: Set(user.id.clone()),
            used_by: Set(None),
            used_at: Set(None),
            created_at: Set(now_millis()),
            expires_at: Set(Some(now_millis() + INVITE_TTL_MS)),
        }
        .insert(&state.db)
        .await;

        match res {
            Ok(_) => return Ok(Json(InviteResponse { code })),
            Err(_) => continue, // unique violation -> new code
        }
    }
    Err(AppError::Internal(
        "could not generate a unique invite".into(),
    ))
}

#[derive(Deserialize)]
pub struct RedeemBody {
    pub code: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RedeemResponse {
    pub vault_id: String,
    pub name: String,
}

pub async fn redeem_invite(
    State(state): State<AppState>,
    AuthUser(user): AuthUser,
    Json(body): Json<RedeemBody>,
) -> AppResult<Json<RedeemResponse>> {
    let invite = invites::Entity::find()
        .filter(invites::Column::Code.eq(&body.code))
        .one(&state.db)
        .await?
        .ok_or(AppError::NotFound)?;

    if invite
        .expires_at
        .is_some_and(|expires| expires < now_millis())
    {
        return Err(AppError::NotFound);
    }

    let vault = vaults::Entity::find_by_id(invite.vault_id.clone())
        .one(&state.db)
        .await?
        .ok_or(AppError::NotFound)?;

    // Already a member: idempotent success without consuming the invite.
    if membership(&state, &user.id, &invite.vault_id)
        .await?
        .is_some()
    {
        return Ok(Json(RedeemResponse {
            vault_id: vault.id,
            name: vault.name,
        }));
    }

    let txn = state.db.begin().await?;

    // Atomic single-use claim: only succeeds while used_by IS NULL and unexpired.
    let claimed = invites::Entity::update_many()
        .col_expr(
            invites::Column::UsedBy,
            sea_orm::sea_query::Expr::value(user.id.clone()),
        )
        .col_expr(
            invites::Column::UsedAt,
            sea_orm::sea_query::Expr::value(now_millis()),
        )
        .filter(invites::Column::Code.eq(&body.code))
        .filter(invites::Column::UsedBy.is_null())
        .filter(
            invites::Column::ExpiresAt
                .is_null()
                .or(invites::Column::ExpiresAt.gt(now_millis())),
        )
        .exec(&txn)
        .await?;

    if claimed.rows_affected == 0 {
        return Err(AppError::Conflict("invite already used or expired".into()));
    }

    memberships::ActiveModel {
        id: Set(uuid::Uuid::new_v4().to_string()),
        user_id: Set(user.id.clone()),
        vault_id: Set(invite.vault_id.clone()),
        role: Set(invite.role_granted.clone()),
        created_at: Set(now_millis()),
    }
    .insert(&txn)
    .await?;

    txn.commit().await?;

    Ok(Json(RedeemResponse {
        vault_id: vault.id,
        name: vault.name,
    }))
}

// ---------- members ----------

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MemberResponse {
    pub user_id: String,
    pub email: String,
    pub display_name: String,
    pub role: String,
    pub owner: bool,
    pub avatar_url: Option<String>,
}

pub async fn list_members(
    State(state): State<AppState>,
    AuthUser(user): AuthUser,
    Path(vault_id): Path<String>,
) -> AppResult<Json<Vec<MemberResponse>>> {
    require_member(&state, &user.id, &vault_id).await?;

    let vault = vaults::Entity::find_by_id(vault_id.clone())
        .one(&state.db)
        .await?
        .ok_or(AppError::NotFound)?;

    let mems = memberships::Entity::find()
        .filter(memberships::Column::VaultId.eq(&vault_id))
        .all(&state.db)
        .await?;

    let mut out = Vec::with_capacity(mems.len());
    for m in mems {
        if let Some(u) = users::Entity::find_by_id(m.user_id.clone())
            .one(&state.db)
            .await?
        {
            let owner = u.id == vault.created_by;
            let avatar_url = effective_avatar_url(&u);
            out.push(MemberResponse {
                user_id: u.id,
                email: u.email,
                display_name: u.display_name,
                role: m.role,
                owner,
                avatar_url,
            });
        }
    }
    Ok(Json(out))
}

pub async fn promote_member(
    State(state): State<AppState>,
    AuthUser(user): AuthUser,
    Path((vault_id, target_user_id)): Path<(String, String)>,
) -> AppResult<Json<MemberResponse>> {
    require_admin(&state, &user.id, &vault_id).await?;

    let m = require_member(&state, &target_user_id, &vault_id).await?;
    let mut active: memberships::ActiveModel = m.into();
    active.role = Set(ROLE_ADMIN.to_string());
    let updated = active.update(&state.db).await?;

    let u = users::Entity::find_by_id(target_user_id)
        .one(&state.db)
        .await?
        .ok_or(AppError::NotFound)?;
    let avatar_url = effective_avatar_url(&u);
    Ok(Json(MemberResponse {
        user_id: u.id,
        email: u.email,
        display_name: u.display_name,
        role: updated.role,
        owner: false,
        avatar_url,
    }))
}

pub async fn remove_member(
    State(state): State<AppState>,
    AuthUser(user): AuthUser,
    Path((vault_id, target_user_id)): Path<(String, String)>,
) -> AppResult<Json<Value>> {
    let actor = require_admin(&state, &user.id, &vault_id).await?;
    let target = require_member(&state, &target_user_id, &vault_id).await?;
    let vault = vaults::Entity::find_by_id(vault_id.clone())
        .one(&state.db)
        .await?
        .ok_or(AppError::NotFound)?;

    if target_user_id == vault.created_by {
        return Err(AppError::Forbidden);
    }

    let actor_is_owner = user.id == vault.created_by;
    if target.role == ROLE_ADMIN && !actor_is_owner {
        return Err(AppError::Forbidden);
    }

    if target.role == ROLE_MEMBER && actor.role != ROLE_ADMIN {
        return Err(AppError::Forbidden);
    }

    memberships::Entity::delete_by_id(target.id)
        .exec(&state.db)
        .await?;

    Ok(Json(serde_json::json!({ "ok": true })))
}

// ---------- remote cursors ----------

#[derive(Deserialize)]
pub struct CursorNameBody {
    pub name: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RemoteCursorResponse {
    pub id: String,
    pub app_id: String,
    pub name: String,
    pub mcp_url: String,
    pub created_at: i64,
    pub plugin_id: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CreatedRemoteCursorResponse {
    pub id: String,
    pub app_id: String,
    pub name: String,
    pub mcp_url: String,
    pub created_at: i64,
    pub secret_token: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SecretTokenResponse {
    pub secret_token: String,
}

pub async fn list_cursors(
    State(state): State<AppState>,
    AuthUser(user): AuthUser,
    Path(vault_id): Path<String>,
) -> AppResult<Json<Vec<RemoteCursorResponse>>> {
    require_admin(&state, &user.id, &vault_id).await?;

    let cursors = remote_cursors::Entity::find()
        .filter(remote_cursors::Column::VaultId.eq(&vault_id))
        .all(&state.db)
        .await?;

    Ok(Json(
        cursors
            .into_iter()
            .map(|cursor| remote_cursor_response(&state, cursor))
            .collect(),
    ))
}

pub async fn create_cursor(
    State(state): State<AppState>,
    AuthUser(user): AuthUser,
    Path(vault_id): Path<String>,
    Json(body): Json<CursorNameBody>,
) -> AppResult<Json<CreatedRemoteCursorResponse>> {
    require_admin(&state, &user.id, &vault_id).await?;
    let name = clean_cursor_name(body.name)?;
    let secret_token = random_cursor_token();
    let now = now_millis();

    for _ in 0..5 {
        let model = remote_cursors::ActiveModel {
            id: Set(uuid::Uuid::new_v4().to_string()),
            vault_id: Set(vault_id.clone()),
            app_id: Set(nanoid::nanoid!()),
            name: Set(name.clone()),
            token_hash: Set(hash_token(&secret_token)),
            created_by: Set(user.id.clone()),
            plugin_id: Set(None),
            created_at: Set(now),
            updated_at: Set(now),
        };

        if let Ok(cursor) = model.insert(&state.db).await {
            return Ok(Json(CreatedRemoteCursorResponse {
                id: cursor.id,
                app_id: cursor.app_id.clone(),
                name: cursor.name,
                mcp_url: mcp_url(&state, &cursor.app_id),
                created_at: cursor.created_at,
                secret_token,
            }));
        }
    }

    Err(AppError::Internal(
        "could not generate a unique cursor id".into(),
    ))
}

pub async fn rename_cursor(
    State(state): State<AppState>,
    AuthUser(user): AuthUser,
    Path((vault_id, cursor_id)): Path<(String, String)>,
    Json(body): Json<CursorNameBody>,
) -> AppResult<Json<RemoteCursorResponse>> {
    require_admin(&state, &user.id, &vault_id).await?;
    let cursor = cursor_in_vault(&state, &vault_id, &cursor_id).await?;
    let mut active: remote_cursors::ActiveModel = cursor.into();
    active.name = Set(clean_cursor_name(body.name)?);
    active.updated_at = Set(now_millis());
    let updated = active.update(&state.db).await?;
    Ok(Json(remote_cursor_response(&state, updated)))
}

pub async fn regenerate_cursor_token(
    State(state): State<AppState>,
    AuthUser(user): AuthUser,
    Path((vault_id, cursor_id)): Path<(String, String)>,
) -> AppResult<Json<SecretTokenResponse>> {
    require_admin(&state, &user.id, &vault_id).await?;
    let cursor = cursor_in_vault(&state, &vault_id, &cursor_id).await?;
    let secret_token = random_cursor_token();
    let mut active: remote_cursors::ActiveModel = cursor.into();
    active.token_hash = Set(hash_token(&secret_token));
    active.updated_at = Set(now_millis());
    active.update(&state.db).await?;
    Ok(Json(SecretTokenResponse { secret_token }))
}

pub async fn delete_cursor(
    State(state): State<AppState>,
    AuthUser(user): AuthUser,
    Path((vault_id, cursor_id)): Path<(String, String)>,
) -> AppResult<Json<Value>> {
    require_admin(&state, &user.id, &vault_id).await?;
    let cursor = cursor_in_vault(&state, &vault_id, &cursor_id).await?;
    remote_cursors::Entity::delete_by_id(cursor.id)
        .exec(&state.db)
        .await?;
    Ok(Json(serde_json::json!({ "ok": true })))
}

fn remote_cursor_response(state: &AppState, cursor: remote_cursors::Model) -> RemoteCursorResponse {
    RemoteCursorResponse {
        id: cursor.id,
        app_id: cursor.app_id.clone(),
        name: cursor.name,
        mcp_url: mcp_url(state, &cursor.app_id),
        created_at: cursor.created_at,
        plugin_id: cursor.plugin_id,
    }
}

async fn cursor_in_vault(
    state: &AppState,
    vault_id: &str,
    cursor_id: &str,
) -> AppResult<remote_cursors::Model> {
    remote_cursors::Entity::find_by_id(cursor_id.to_string())
        .filter(remote_cursors::Column::VaultId.eq(vault_id))
        .one(&state.db)
        .await?
        .ok_or(AppError::NotFound)
}

fn clean_cursor_name(name: String) -> AppResult<String> {
    let name = name.trim().to_string();
    if name.is_empty() {
        return Err(AppError::BadRequest("cursor name is required".into()));
    }
    if name.len() > 128 {
        return Err(AppError::BadRequest("cursor name is too long".into()));
    }
    if name.chars().any(|c| c.is_control() || c == '<' || c == '>') {
        return Err(AppError::BadRequest(
            "cursor name contains invalid characters".into(),
        ));
    }
    Ok(name)
}

fn random_cursor_token() -> String {
    nanoid::nanoid!(48)
}

pub(crate) fn mcp_url(state: &AppState, app_id: &str) -> String {
    format!(
        "{}/mcp/i/{app_id}",
        state.config.public_base_url.trim_end_matches('/')
    )
}

// ---------- plugin-managed cursors ----------

/// How long a plugin-acquired bearer token stays valid. Plugins re-acquire on
/// expiry/401, so this mostly bounds the damage of a leaked token.
const PLUGIN_CURSOR_TOKEN_TTL_MS: i64 = 1000 * 60 * 60 * 24 * 30;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PluginCursorBody {
    pub plugin_id: String,
    pub name: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PluginCursorResponse {
    pub id: String,
    pub app_id: String,
    pub name: String,
    pub vault_id: String,
    pub plugin_id: String,
    pub mcp_url: String,
    pub stream_url: String,
    pub secret_token: String,
    pub expires_at: i64,
}

/// Get-or-create the plugin-managed cursor for `(vault, plugin_id)` and mint a
/// fresh bearer token for it. Any vault member may acquire one: the cursor's
/// `created_by` (the On-Behalf-Of user in Git) is whoever acquired it first,
/// and each device gets its own token row so they don't invalidate each other.
pub async fn acquire_plugin_cursor(
    State(state): State<AppState>,
    AuthUser(user): AuthUser,
    Path(vault_id): Path<String>,
    Json(body): Json<PluginCursorBody>,
) -> AppResult<Json<PluginCursorResponse>> {
    require_member(&state, &user.id, &vault_id).await?;
    let plugin_id = body.plugin_id.trim().to_string();
    if plugin_id.is_empty() || plugin_id.len() > 128 {
        return Err(AppError::BadRequest("invalid plugin id".into()));
    }
    let name = clean_cursor_name(body.name.unwrap_or_else(|| plugin_id.clone()))?;

    let cursor = match plugin_cursor(&state, &vault_id, &plugin_id).await? {
        Some(cursor) => cursor,
        None => {
            let now = now_millis();
            let model = remote_cursors::ActiveModel {
                id: Set(uuid::Uuid::new_v4().to_string()),
                vault_id: Set(vault_id.clone()),
                app_id: Set(nanoid::nanoid!()),
                name: Set(name),
                // Plugin cursors have no copy-once admin secret; burn the
                // legacy slot with the hash of a token nobody ever sees.
                token_hash: Set(hash_token(&random_cursor_token())),
                created_by: Set(user.id.clone()),
                plugin_id: Set(Some(plugin_id.clone())),
                created_at: Set(now),
                updated_at: Set(now),
            };
            match model.insert(&state.db).await {
                Ok(cursor) => cursor,
                // Lost the race on the (vault_id, plugin_id) unique index:
                // another device created it concurrently, so use theirs.
                Err(_) => plugin_cursor(&state, &vault_id, &plugin_id)
                    .await?
                    .ok_or_else(|| {
                        AppError::Internal("plugin cursor create race lost twice".into())
                    })?,
            }
        }
    };

    let secret_token = random_cursor_token();
    let now = now_millis();
    let expires_at = now + PLUGIN_CURSOR_TOKEN_TTL_MS;
    remote_cursor_tokens::ActiveModel {
        id: Set(uuid::Uuid::new_v4().to_string()),
        cursor_id: Set(cursor.id.clone()),
        token_hash: Set(hash_token(&secret_token)),
        label: Set(format!("{} <{}>", user.display_name, user.email)),
        created_at: Set(now),
        expires_at: Set(expires_at),
    }
    .insert(&state.db)
    .await?;

    Ok(Json(PluginCursorResponse {
        id: cursor.id,
        app_id: cursor.app_id.clone(),
        name: cursor.name,
        vault_id: cursor.vault_id,
        plugin_id,
        mcp_url: mcp_url(&state, &cursor.app_id),
        stream_url: stream_url(&state, &vault_id),
        secret_token,
        expires_at,
    }))
}

async fn plugin_cursor(
    state: &AppState,
    vault_id: &str,
    plugin_id: &str,
) -> AppResult<Option<remote_cursors::Model>> {
    Ok(remote_cursors::Entity::find()
        .filter(remote_cursors::Column::VaultId.eq(vault_id))
        .filter(remote_cursors::Column::PluginId.eq(plugin_id))
        .one(&state.db)
        .await?)
}

pub(crate) fn stream_url(state: &AppState, vault_id: &str) -> String {
    format!(
        "{}/api/vaults/{vault_id}/stream",
        state.config.public_base_url.trim_end_matches('/')
    )
}

// ---------- remote cursor audit log ----------

#[derive(Deserialize)]
pub struct AuditListQuery {
    /// Keyset cursor: only entries created strictly before this epoch-millis.
    pub before: Option<i64>,
    pub limit: Option<u64>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CursorAuditEntryResponse {
    pub id: String,
    pub created_at: i64,
    pub operation: String,
    pub path: String,
    pub to_path: Option<String>,
    pub before_content: Option<String>,
    pub after_content: Option<String>,
    pub details: Option<Value>,
    pub undone_at: Option<i64>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CursorAuditListResponse {
    pub entries: Vec<CursorAuditEntryResponse>,
    pub has_more: bool,
}

#[derive(Deserialize)]
pub struct UndoAuditBody {
    #[serde(default)]
    pub force: bool,
}

pub async fn list_cursor_audit(
    State(state): State<AppState>,
    AuthUser(user): AuthUser,
    Path((vault_id, cursor_id)): Path<(String, String)>,
    Query(query): Query<AuditListQuery>,
) -> AppResult<Json<CursorAuditListResponse>> {
    require_admin(&state, &user.id, &vault_id).await?;
    cursor_in_vault(&state, &vault_id, &cursor_id).await?;
    let limit = query.limit.unwrap_or(50).clamp(1, 100);
    // Over-fetch one row to learn whether another page exists.
    let mut entries = audit::list(&state, &cursor_id, query.before, limit + 1).await?;
    let has_more = entries.len() as u64 > limit;
    entries.truncate(limit as usize);
    Ok(Json(CursorAuditListResponse {
        entries: entries
            .into_iter()
            .map(|entry| CursorAuditEntryResponse {
                id: entry.id,
                created_at: entry.created_at,
                operation: entry.operation,
                path: entry.path,
                to_path: entry.to_path,
                before_content: entry.before_content,
                after_content: entry.after_content,
                details: entry
                    .details
                    .as_deref()
                    .and_then(|d| serde_json::from_str(d).ok()),
                undone_at: entry.undone_at,
            })
            .collect(),
        has_more,
    }))
}

pub async fn undo_cursor_audit(
    State(state): State<AppState>,
    AuthUser(user): AuthUser,
    Path((vault_id, cursor_id, entry_id)): Path<(String, String, String)>,
    Json(body): Json<UndoAuditBody>,
) -> AppResult<Json<Value>> {
    require_admin(&state, &user.id, &vault_id).await?;
    cursor_in_vault(&state, &vault_id, &cursor_id).await?;
    // The inverse operation is applied as the undoing human, so Git history
    // attributes the revert to them rather than to the cursor.
    let undoer = ApiPrincipal {
        user,
        actor: ApiActor::User,
    };
    audit::undo(
        &state, &undoer, &vault_id, &cursor_id, &entry_id, body.force,
    )
    .await?;
    Ok(Json(serde_json::json!({ "ok": true })))
}

// ---------- git backup ----------

const BACKUP_AUTH_SSH: &str = "ssh";
const BACKUP_AUTH_HTTPS: &str = "https";

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GitBackupResponse {
    pub configured: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remote_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_method: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ssh_public_key: Option<String>,
    pub has_https_token: bool,
    pub enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_push_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_push_error: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PutGitBackupBody {
    pub remote_url: String,
    pub auth_method: String,
    pub branch: Option<String>,
    pub https_token: Option<String>,
    #[serde(default)]
    pub regenerate_key: bool,
    pub enabled: bool,
}

/// Secrets (private key, token) are deliberately never serialized back out.
fn git_backup_response(cfg: git_backups::Model) -> GitBackupResponse {
    GitBackupResponse {
        configured: true,
        remote_url: Some(cfg.remote_url),
        auth_method: Some(cfg.auth_method),
        branch: Some(cfg.branch),
        ssh_public_key: cfg.ssh_public_key,
        has_https_token: cfg.https_token.is_some(),
        enabled: cfg.enabled,
        last_push_at: cfg.last_push_at,
        last_push_error: cfg.last_push_error,
    }
}

fn unconfigured_backup_response() -> GitBackupResponse {
    GitBackupResponse {
        configured: false,
        remote_url: None,
        auth_method: None,
        branch: None,
        ssh_public_key: None,
        has_https_token: false,
        enabled: false,
        last_push_at: None,
        last_push_error: None,
    }
}

fn validate_backup_url(auth_method: &str, url: &str) -> AppResult<()> {
    let ok = match auth_method {
        BACKUP_AUTH_SSH => url.starts_with("ssh://") || (url.contains('@') && url.contains(':')),
        BACKUP_AUTH_HTTPS => url.starts_with("https://"),
        _ => {
            return Err(AppError::BadRequest(
                "authMethod must be 'ssh' or 'https'".into(),
            ))
        }
    };
    if !ok {
        return Err(AppError::BadRequest(format!(
            "remote URL does not match {auth_method} auth (expected {})",
            if auth_method == BACKUP_AUTH_SSH {
                "ssh:// or git@host:path"
            } else {
                "https://"
            }
        )));
    }
    Ok(())
}

pub async fn get_backup(
    State(state): State<AppState>,
    AuthUser(user): AuthUser,
    Path(vault_id): Path<String>,
) -> AppResult<Json<GitBackupResponse>> {
    require_admin(&state, &user.id, &vault_id).await?;
    let cfg = git_backups::Entity::find_by_id(vault_id)
        .one(&state.db)
        .await?;
    Ok(Json(match cfg {
        Some(cfg) => git_backup_response(cfg),
        None => unconfigured_backup_response(),
    }))
}

pub async fn put_backup(
    State(state): State<AppState>,
    AuthUser(user): AuthUser,
    Path(vault_id): Path<String>,
    Json(body): Json<PutGitBackupBody>,
) -> AppResult<Json<GitBackupResponse>> {
    require_admin(&state, &user.id, &vault_id).await?;

    let remote_url = body.remote_url.trim().to_string();
    if remote_url.is_empty() {
        return Err(AppError::BadRequest("remote URL is required".into()));
    }
    validate_backup_url(&body.auth_method, &remote_url)?;
    let branch = match body.branch.map(|b| b.trim().to_string()) {
        Some(b) if !b.is_empty() => {
            if b.contains(|c: char| c.is_whitespace()) || b.starts_with('-') {
                return Err(AppError::BadRequest("invalid branch name".into()));
            }
            b
        }
        _ => "main".to_string(),
    };

    let existing = git_backups::Entity::find_by_id(vault_id.clone())
        .one(&state.db)
        .await?;
    let now = now_millis();

    // Carry forward / generate the per-method secret.
    let (ssh_private_key, ssh_public_key, https_token) = match body.auth_method.as_str() {
        BACKUP_AUTH_SSH => {
            let existing_pair = existing
                .as_ref()
                .and_then(|e| Some((e.ssh_private_key.clone()?, e.ssh_public_key.clone()?)));
            let (private, public) = match (existing_pair, body.regenerate_key) {
                (Some(pair), false) => pair,
                _ => crate::git::generate_ssh_keypair()
                    .map_err(|e| AppError::Internal(format!("keygen failed: {e}")))?,
            };
            (Some(private), Some(public), None)
        }
        _ => {
            let token = body
                .https_token
                .map(|t| t.trim().to_string())
                .filter(|t| !t.is_empty())
                .or_else(|| existing.as_ref().and_then(|e| e.https_token.clone()));
            let Some(token) = token else {
                return Err(AppError::BadRequest("an HTTPS token is required".into()));
            };
            (None, None, Some(token))
        }
    };

    let model = git_backups::ActiveModel {
        vault_id: Set(vault_id.clone()),
        remote_url: Set(remote_url),
        auth_method: Set(body.auth_method),
        branch: Set(branch),
        ssh_private_key: Set(ssh_private_key),
        ssh_public_key: Set(ssh_public_key),
        https_token: Set(https_token),
        enabled: Set(body.enabled),
        // Reset push status: it described the previous configuration.
        last_push_at: Set(None),
        last_push_error: Set(None),
        created_by: Set(existing
            .as_ref()
            .map(|e| e.created_by.clone())
            .unwrap_or_else(|| user.id.clone())),
        created_at: Set(existing.as_ref().map(|e| e.created_at).unwrap_or(now)),
        updated_at: Set(now),
    };
    let saved = if existing.is_some() {
        model.update(&state.db).await?
    } else {
        model.insert(&state.db).await?
    };

    Ok(Json(git_backup_response(saved)))
}

pub async fn delete_backup(
    State(state): State<AppState>,
    AuthUser(user): AuthUser,
    Path(vault_id): Path<String>,
) -> AppResult<Json<Value>> {
    require_admin(&state, &user.id, &vault_id).await?;
    git_backups::Entity::delete_by_id(vault_id.clone())
        .exec(&state.db)
        .await?;
    state.git.remove_backup_key_file(&vault_id).await;
    Ok(Json(serde_json::json!({ "ok": true })))
}

pub async fn test_backup(
    State(state): State<AppState>,
    AuthUser(user): AuthUser,
    Path(vault_id): Path<String>,
) -> AppResult<Json<Value>> {
    require_admin(&state, &user.id, &vault_id).await?;
    let cfg = git_backups::Entity::find_by_id(vault_id.clone())
        .one(&state.db)
        .await?
        .ok_or(AppError::NotFound)?;
    match state.git.test_remote(&vault_id, &cfg).await {
        Ok(()) => Ok(Json(serde_json::json!({ "ok": true }))),
        Err(e) => Ok(Json(
            serde_json::json!({ "ok": false, "error": format!("{e:#}") }),
        )),
    }
}

// ---------- file registry ----------

#[derive(Deserialize)]
pub struct UpsertFileBody {
    pub guid: String,
    pub path: String,
}

pub async fn upsert_file(
    State(state): State<AppState>,
    AuthUser(user): AuthUser,
    Path(vault_id): Path<String>,
    Json(body): Json<UpsertFileBody>,
) -> AppResult<Json<Value>> {
    require_member(&state, &user.id, &vault_id).await?;
    if body.path.is_empty() {
        return Err(AppError::BadRequest("file path must not be empty".into()));
    }

    if let Some(existing) = vault_files::Entity::find()
        .filter(vault_files::Column::VaultId.eq(&vault_id))
        .filter(vault_files::Column::Guid.eq(&body.guid))
        .one(&state.db)
        .await?
    {
        if authorize_path(&state, &user, &vault_id, &existing.path).await? != Level::Full {
            return Err(AppError::Forbidden);
        }
    }
    if authorize_path(&state, &user, &vault_id, &body.path).await? != Level::Full {
        return Err(AppError::Forbidden);
    }
    if body.guid.is_empty() || body.guid.contains("__") || !safe_doc_id(&body.guid) {
        return Err(AppError::BadRequest("invalid file guid".into()));
    }
    // Share the creation admission gate with direct CRDT document creation so
    // count + insert is one process-wide critical section.
    let _creation_guard = state.document_creation_lock.lock().await;
    let existing = vault_files::Entity::find()
        .filter(vault_files::Column::VaultId.eq(&vault_id))
        .filter(vault_files::Column::Guid.eq(&body.guid))
        .one(&state.db)
        .await?;
    if existing.is_none() {
        let registered = vault_files::Entity::find()
            .filter(vault_files::Column::VaultId.eq(&vault_id))
            .count(&state.db)
            .await?;
        if registered >= state.config.crdt_max_documents_per_vault {
            return Err(AppError::BadRequest("vault document limit reached".into()));
        }
    }

    // Atomic upsert: the client fires registry updates fire-and-forget on every
    // file event, so the same guid can arrive concurrently (delete/restore/move)
    // and a find-then-insert would collide on the unique (vault_id, guid) index.
    crate::ydoc::upsert_vault_file(&state, &vault_id, &body.path, &body.guid).await?;
    let document_id = format!("{vault_id}__{}", body.guid);
    state
        .pending_document_creations
        .lock()
        .await
        .remove(&document_id);

    Ok(Json(serde_json::json!({ "ok": true })))
}

// ---------- doc-token minting ----------

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DocTokenBody {
    pub vault_id: String,
    pub doc_id: String,
    /// Path claimed by a client that is creating a brand-new file document.
    /// Existing registry entries always win over this value.
    pub path: Option<String>,
    /// Optional client-requested downgrade. A caller can request read-only
    /// access but can never upgrade the level granted by the path ACL.
    pub authorization: Option<String>,
}

pub async fn doc_token(
    State(state): State<AppState>,
    AuthUser(user): AuthUser,
    Json(body): Json<DocTokenBody>,
) -> AppResult<Json<Value>> {
    require_member(&state, &user.id, &body.vault_id).await?;

    if !safe_doc_id(&body.doc_id) {
        return Err(AppError::BadRequest("invalid doc id".into()));
    }

    // Cheap scope gate: the doc must belong to this vault's namespace.
    let prefix = format!("{}__", body.vault_id);
    if body.doc_id != body.vault_id && !body.doc_id.starts_with(&prefix) {
        return Err(AppError::Forbidden);
    }

    let authorization = authorize_doc_with_claim(
        &state,
        &user,
        &body.vault_id,
        &body.doc_id,
        body.path.as_deref(),
    )
    .await?;
    if authorization.requires_creation_reservation
        && !state
            .reserve_document_creation(
                body.doc_id.clone(),
                user.id.clone(),
                authorization.path.clone(),
            )
            .await
    {
        return Err(AppError::BadRequest(
            "too many pending document creations".into(),
        ));
    }
    let level = match body.authorization.as_deref() {
        None | Some("full") => authorization.level,
        Some("read-only") => Level::ReadOnly,
        Some(_) => return Err(AppError::BadRequest("invalid authorization level".into())),
    };

    state
        .ensure_vault_document(&body.vault_id, &body.doc_id)
        .await?;
    let mut token = mint_client_token(&state, &body.doc_id, level).await?;
    let expires_at_ms = now_millis() + SYNC_GRANT_TTL_MS;
    // Clients refresh before this instead of discovering an expired token
    // through failed channel opens after a reconnect.
    token["expiresAt"] = Value::from(expires_at_ms);

    // Bind this opaque token to exactly one document, authorization level, and
    // principal before returning it to the client.
    if let Some(conn_token) = token.get("token").and_then(Value::as_str) {
        let epoch = token
            .get("epoch")
            .and_then(Value::as_u64)
            .ok_or_else(|| AppError::Internal("document token omitted its epoch".into()))?;
        state
            .record_sync_grant(
                conn_token.to_string(),
                body.doc_id.clone(),
                level,
                epoch,
                Principal {
                    user_id: user.id.clone(),
                    display_name: user.display_name.clone(),
                    email: user.email.clone(),
                    git_email: user.git_email.clone(),
                    actor: PrincipalActor::User,
                    expires_at_ms,
                },
            )
            .await;
    }

    Ok(Json(token))
}

pub async fn logout(State(state): State<AppState>, headers: HeaderMap) -> AppResult<Json<Value>> {
    let header = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .ok_or(AppError::Unauthorized)?;
    let token = bearer_token(header).ok_or(AppError::Unauthorized)?;
    revoke_session(&state.db, token).await?;
    Ok(Json(serde_json::json!({ "ok": true })))
}

fn safe_doc_id(doc_id: &str) -> bool {
    !doc_id.is_empty()
        && doc_id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
}

/// Resolve the docId to a path and evaluate the ACL.
pub(crate) async fn authorize_doc(
    state: &AppState,
    user: &users::Model,
    vault_id: &str,
    doc_id: &str,
) -> AppResult<Level> {
    Ok(
        authorize_doc_with_claim(state, user, vault_id, doc_id, None)
            .await?
            .level,
    )
}

struct DocumentAuthorization {
    level: Level,
    path: String,
    requires_creation_reservation: bool,
}

async fn authorize_doc_with_claim(
    state: &AppState,
    user: &users::Model,
    vault_id: &str,
    doc_id: &str,
    claimed_path: Option<&str>,
) -> AppResult<DocumentAuthorization> {
    if doc_id == vault_id {
        // A Y.Map cannot hide individual entries. Only uniformly-authorized
        // principals may receive the shared index document.
        return Ok(DocumentAuthorization {
            level: authorize_uniform_vault(state, user, vault_id).await?,
            path: String::new(),
            requires_creation_reservation: false,
        });
    }

    let prefix = format!("{vault_id}__");
    let suffix = doc_id.strip_prefix(&prefix).ok_or(AppError::Forbidden)?;

    if let Some(plugin_db) = suffix.strip_prefix("plugindb__") {
        let mut parts = plugin_db.split("__");
        let plugin = parts.next().filter(|part| !part.is_empty());
        let name = parts.next().filter(|part| !part.is_empty());
        if plugin.is_none() || name.is_none() || parts.next().is_some() {
            return Err(AppError::Forbidden);
        }
        let path = crate::plugindb::routes::pseudo_path(
            plugin.expect("checked above"),
            name.expect("checked above"),
        );
        return Ok(DocumentAuthorization {
            level: authorize_path(state, user, vault_id, &path).await?,
            path,
            requires_creation_reservation: false,
        });
    }

    // Normal file guids never contain the plugin-db separator.
    if suffix.is_empty() || suffix.contains("__") {
        return Err(AppError::Forbidden);
    }

    let registered = vault_files::Entity::find()
        .filter(vault_files::Column::VaultId.eq(vault_id))
        .filter(vault_files::Column::Guid.eq(suffix))
        .one(&state.db)
        .await?;
    if registered.is_none() && state.documents.document_exists(doc_id).await? {
        let now = now_millis();
        let reservation = {
            let mut reservations = state.pending_document_creations.lock().await;
            reservations.retain(|_, reservation| reservation.expires_at_ms > now);
            reservations.get(doc_id).cloned()
        };
        if let Some(reservation) = reservation {
            let claimed_path_matches = match claimed_path {
                Some(path) if reservation.path.is_empty() => !path.is_empty(),
                Some(path) => !path.is_empty() && path == reservation.path,
                None => true,
            };
            if reservation.user_id == user.id && claimed_path_matches {
                let path = claimed_path
                    .filter(|_| reservation.path.is_empty())
                    .unwrap_or(&reservation.path)
                    .to_string();
                return Ok(DocumentAuthorization {
                    level: authorize_path(state, user, vault_id, &path).await?,
                    path,
                    requires_creation_reservation: true,
                });
            }
            return Err(AppError::Forbidden);
        }
        return Ok(DocumentAuthorization {
            level: authorize_uniform_vault(state, user, vault_id).await?,
            path: String::new(),
            requires_creation_reservation: false,
        });
    }
    let (path, requires_creation_reservation) = match registered {
        Some(file) => (file.path, false),
        // Creator documents connect before their index entry is published.
        // The claimed path permits that one empty-document bootstrap without
        // turning every unknown guid into vault-level access.
        None => match claimed_path.filter(|path| !path.is_empty()) {
            Some(path) => (path.to_string(), true),
            // Preserve direct document creation for principals whose policy is
            // uniform across the vault. Partial ACLs must provide a path.
            None => {
                return Ok(DocumentAuthorization {
                    level: authorize_uniform_vault(state, user, vault_id).await?,
                    path: String::new(),
                    requires_creation_reservation: true,
                });
            }
        },
    };
    Ok(DocumentAuthorization {
        level: authorize_path(state, user, vault_id, &path).await?,
        path,
        requires_creation_reservation,
    })
}

pub(crate) async fn authorize_path(
    state: &AppState,
    user: &users::Model,
    vault_id: &str,
    path: &str,
) -> AppResult<Level> {
    let rows = permissions::Entity::find()
        .filter(permissions::Column::VaultId.eq(vault_id))
        .all(&state.db)
        .await?;
    resolve_path_level(&rows, user, path)
}

/// Resolve the vault root and every configured prefix to the same level.
/// Shared index documents and unscoped blob requests are safe only under this
/// uniform policy because their payloads contain entries from every path.
pub(crate) async fn authorize_uniform_vault(
    state: &AppState,
    user: &users::Model,
    vault_id: &str,
) -> AppResult<Level> {
    let rows = permissions::Entity::find()
        .filter(permissions::Column::VaultId.eq(vault_id))
        .all(&state.db)
        .await?;
    let root = resolve_path_level(&rows, user, "")?;
    for row in &rows {
        let applies = row
            .principal_user_id
            .as_ref()
            .map(|id| id == &user.id)
            .unwrap_or(true);
        if !applies || row.path_prefix.is_empty() {
            continue;
        }
        match resolve_path_level(&rows, user, &row.path_prefix) {
            Ok(level) if level == root => {}
            Ok(_) | Err(AppError::Forbidden) => return Err(AppError::Forbidden),
            Err(error) => return Err(error),
        }
    }
    Ok(root)
}

fn resolve_path_level(
    rows: &[permissions::Model],
    user: &users::Model,
    path: &str,
) -> AppResult<Level> {
    // Most specific match wins: longer prefix first, user-specific over everyone.
    // NOTE: path matching is case-sensitive, consistent with how Obsidian and the
    // sync layer store paths. Clients on case-insensitive filesystems (macOS,
    // Windows) normalize to the stored casing before sending, so this is safe.
    let mut best: Option<(i64, String)> = None;
    for r in rows {
        let principal_ok = match &r.principal_user_id {
            None => true,
            Some(uid) => uid == &user.id,
        };
        if !principal_ok || !path.starts_with(&r.path_prefix) {
            continue;
        }
        let specificity =
            r.path_prefix.len() as i64 * 2 + if r.principal_user_id.is_some() { 1 } else { 0 };
        if best.as_ref().map(|(s, _)| specificity > *s).unwrap_or(true) {
            best = Some((specificity, r.level.clone()));
        }
    }

    match best.map(|(_, level)| level).as_deref() {
        None | Some("full") => Ok(Level::Full),
        Some("read-only") => Ok(Level::ReadOnly),
        Some("deny") => Err(AppError::Forbidden),
        Some(other) => Err(AppError::Internal(format!("bad permission level {other}"))),
    }
}
