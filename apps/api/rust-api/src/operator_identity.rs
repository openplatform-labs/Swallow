//! Optional JWT identity and permission-scoped subscriber lookup.
use crate::{
    error_at,
    operator_access_token::{AccessTokenVerifier, VerifiedSubject},
};
use axum::{
    Json,
    body::Bytes,
    extract::{Extension, OriginalUri, State},
    http::{HeaderMap, StatusCode, Uri, header},
    response::{IntoResponse, Response},
};
use std::{collections::HashMap, sync::Arc};
use tokio_postgres::Client;
use uuid::Uuid;

#[derive(Clone, Copy)]
enum Lookup {
    Identity,
    Subscribers,
    Alerts,
    DecisionRequest,
    PermissionIdentity,
    PermissionAudit,
    PermissionChangeRequest,
    ResendAutoHealthJobs,
    ResendAutoHealth,
    SlackCooldownHealth,
    ResendCooldownHealth,
    FcmCooldownHealth,
    DiscordRetryHealth,
    DiscordRetryList,
    FcmUnrecordedClaimHealth,
    FcmUnrecordedClaimList,
    TwilioDelayedHealth,
    FcmPartialHealth,
    FcmPartialJobs,
    FcmUnreviewed,
    FcmReviewRequest,
    ResendResult,
    ResendJobs,
    TwilioResult,
    TwilioJobs,
    FcmResult,
    FcmJobs,
    SlackDecisionRequest,
    TwilioRetryRequest,
    Resend429Request,
    Resend429Audit,
    SlackResult,
    SlackJobs,
    SendgridResult,
    SendgridJobs,
    WorkflowRead,
    WorkflowAudit,
    WorkflowRequest,
    WorkflowGrantRead,
    WorkflowGrantRequest,
    WorkflowGrantAudit,
    ResendAutoPolicyRead,
    ResendAutoPolicyRequest,
    ResendAutoPolicyAudit,
    ResendCooldownPolicyRead,
    ResendCooldownPolicyRequest,
    ResendCooldownPolicyAudit,
    DiscordRetryPolicyRead,
    DiscordRetryPolicyRequest,
    DiscordRetryPolicyAudit,
    DiscordPolicyRead,
    DiscordPolicyRequest,
    DiscordPolicyAudit,
    Resend429GrantRead,
    Resend429GrantRequest,
    Resend429GrantAudit,
    DiscordReviewGrantRead,
    DiscordReviewGrantRequest,
    DiscordReviewGrantAudit,
    DiscordPolicyGrantRead,
    DiscordPolicyGrantRequest,
    DiscordPolicyGrantAudit,
    DiscordRetryGrantRead,
    DiscordRetryGrantRequest,
    DiscordRetryGrantAudit,
    FcmReviewGrantRead,
    FcmReviewGrantRequest,
    FcmReviewGrantAudit,
    SlackDecisionGrantRead,
    SlackDecisionGrantRequest,
    SlackDecisionGrantAudit,
    ChannelAccessGrantRead,
    ChannelAccessGrantRequest,
    ChannelAccessGrantAudit,
    SubscriberProfileRead,
    SubscriberProfileRequest,
    SubscriberCreateRequest,
    SubscriberDeleteRequest,
    SubscriberGrantRead,
    SubscriberGrantRequest,
    SubscriberGrantAudit,
    SubscriberActivityAudit,
}

struct Authenticated {
    identity: VerifiedSubject,
    revocation_guard: bool,
}
impl std::ops::Deref for Authenticated {
    type Target = VerifiedSubject;
    fn deref(&self) -> &Self::Target {
        &self.identity
    }
}

async fn authenticate(
    db: &Client,
    headers: &HeaderMap,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
) -> Result<Authenticated, StatusCode> {
    let Some(Extension(verifier)) = verifier else {
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    };
    let mut values = headers.get_all(header::AUTHORIZATION).iter();
    let token = values
        .next()
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    if values.next().is_some() {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let token = token.ok_or(StatusCode::UNAUTHORIZED)?;
    if !verifier.is_available() { return Err(StatusCode::SERVICE_UNAVAILABLE); }
    let mut identity = match verifier.verify(token) {
        Some(identity) => identity,
        None if !verifier.is_available() => return Err(StatusCode::SERVICE_UNAVAILABLE),
        None => return Err(StatusCode::UNAUTHORIZED),
    };
    if !verifier.is_available() { return Err(StatusCode::SERVICE_UNAVAILABLE); }
    if verifier.checks_token_revocation() {
        let hash = |s: &str| ring::digest::digest(&ring::digest::SHA256, s.as_bytes());
        let issuer = hash(&identity.issuer);
        let subject = hash(&identity.subject);
        let client = hash(&identity.client_id);
        let id = hash(&identity.token_id);
        let row = db
            .query_one(
                "SELECT public.novu_rust_operator_token_revoked($1,$2,$3,$4)",
                &[
                    &issuer.as_ref(),
                    &subject.as_ref(),
                    &client.as_ref(),
                    &id.as_ref(),
                ],
            )
            .await
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        if row.get::<_, bool>(0) {
            return Err(StatusCode::UNAUTHORIZED);
        }
    }
    match verifier.introspect(token, &identity).await {
        crate::operator_introspection::Outcome::Active(expiration) => {
            if let Some(expiration) = expiration {
                identity.expires_at = identity.expires_at.min(expiration);
            }
        }
        crate::operator_introspection::Outcome::Inactive => return Err(StatusCode::UNAUTHORIZED),
        crate::operator_introspection::Outcome::Unavailable => {
            return Err(StatusCode::SERVICE_UNAVAILABLE);
        }
    }
    // Introspection can take time; never accept an access token that expired during it.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?
        .as_secs();
    if now >= identity.expires_at {
        return Err(StatusCode::UNAUTHORIZED);
    }
    Ok(Authenticated {
        identity,
        revocation_guard: verifier.checks_token_revocation(),
    })
}

// All operator SQL here is a single SELECT of one security-definer function.
// The materialized guard is evaluated in the same transaction as that call.
async fn guarded_query(
    db: &Client,
    identity: &Authenticated,
    sql: &str,
    params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
) -> Result<tokio_postgres::Row, tokio_postgres::Error> {
    if !identity.revocation_guard {
        return db.query_one(sql, params).await;
    }
    let hash = |s: &str| ring::digest::digest(&ring::digest::SHA256, s.as_bytes());
    let issuer = hash(&identity.issuer);
    let subject = hash(&identity.subject);
    let client = hash(&identity.client_id);
    let id = hash(&identity.token_id);
    let issuer_bytes = issuer.as_ref();
    let subject_bytes = subject.as_ref();
    let client_bytes = client.as_ref();
    let id_bytes = id.as_ref();
    let mut bound: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = params.to_vec();
    bound.extend([
        &issuer_bytes as &(dyn tokio_postgres::types::ToSql + Sync),
        &subject_bytes,
        &client_bytes,
        &id_bytes,
    ]);
    let n = params.len();
    let statement = format!(
        "WITH swallow_token_gate AS MATERIALIZED (SELECT public.novu_rust_operator_token_guard(${},${},${},${}) AS allowed) {sql} FROM swallow_token_gate WHERE allowed",
        n + 1,
        n + 2,
        n + 3,
        n + 4
    );
    db.query_one(&statement, &bound).await
}

fn failure(status: StatusCode, path: &str) -> Response {
    let (message, label) = match status {
        StatusCode::UNAUTHORIZED => ("Unauthorized", "Unauthorized"),
        StatusCode::BAD_REQUEST => ("Invalid or stale request", "Bad Request"),
        StatusCode::SERVICE_UNAVAILABLE => {
            ("Operator authentication unavailable", "Service Unavailable")
        }
        StatusCode::CONFLICT => ("Workflow changed", "Conflict"),
        _ => ("Internal server error", "Internal Server Error"),
    };
    let mut response = error_at(status, message, label, path).into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        header::HeaderValue::from_static("no-store"),
    );
    response
}

pub(super) async fn me(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
) -> Response {
    query(db, headers, uri, verifier, Lookup::Identity).await
}

pub(super) async fn revoke_self(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
    body: Bytes,
) -> Response {
    let identity = match authenticate(&db, &headers, verifier).await {
        Ok(value) => value,
        Err(status) => return failure(status, uri.path()),
    };
    if !identity.revocation_guard {
        return failure(StatusCode::SERVICE_UNAVAILABLE, uri.path());
    }
    let params: Vec<_> = form_urlencoded::parse(uri.query().unwrap_or("").as_bytes()).collect();
    if !body.is_empty()
        || params.len() != 1
        || params[0].0 != "environmentId"
        || params[0].1.is_empty()
        || params[0].1.len() > 512
        || params[0].1.contains('\0')
    {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    }
    let Ok(exp) = i64::try_from(identity.expires_at) else {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    };
    let id = ring::digest::digest(&ring::digest::SHA256, identity.token_id.as_bytes());
    let env = params[0].1.as_ref();
    match db
        .query_one(
            "SELECT public.novu_rust_operator_jwt_self_revoke($1,$2,$3,$4,$5,$6)",
            &[
                &identity.issuer,
                &identity.subject,
                &identity.client_id,
                &env,
                &exp,
                &id.as_ref(),
            ],
        )
        .await
    {
        Ok(row) => (
            [(header::CACHE_CONTROL, "no-store")],
            Json(serde_json::json!({"created":row.get::<_,bool>(0)})),
        )
            .into_response(),
        Err(e) if e.code().is_some_and(|c| c.code() == "42501") => {
            failure(StatusCode::UNAUTHORIZED, uri.path())
        }
        Err(_) => failure(StatusCode::INTERNAL_SERVER_ERROR, uri.path()),
    }
}
pub(super) async fn subscribers(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
) -> Response {
    query(db, headers, uri, verifier, Lookup::Subscribers).await
}
pub(super) async fn alerts(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
) -> Response {
    query(db, headers, uri, verifier, Lookup::Alerts).await
}
pub(super) async fn decision_request(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
) -> Response {
    query(db, headers, uri, verifier, Lookup::DecisionRequest).await
}
pub(super) async fn permission_identity(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
) -> Response {
    query(db, headers, uri, verifier, Lookup::PermissionIdentity).await
}
pub(super) async fn permission_audit(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
) -> Response {
    query(db, headers, uri, verifier, Lookup::PermissionAudit).await
}
pub(super) async fn permission_change_request(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
) -> Response {
    query(db, headers, uri, verifier, Lookup::PermissionChangeRequest).await
}
pub(super) async fn resend_429_request(
    State(db): State<Arc<Client>>, headers: HeaderMap, OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
) -> Response { query(db, headers, uri, verifier, Lookup::Resend429Request).await }

pub(super) async fn resend_429_audit(
    State(db): State<Arc<Client>>, headers: HeaderMap, OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
) -> Response { query(db, headers, uri, verifier, Lookup::Resend429Audit).await }

pub(super) async fn twilio_retry_request(State(db): State<Arc<Client>>, headers: HeaderMap, OriginalUri(uri): OriginalUri, verifier: Option<Extension<Arc<AccessTokenVerifier>>>) -> Response {
    query(db, headers, uri, verifier, Lookup::TwilioRetryRequest).await
}

pub(super) async fn slack_decision_request(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
) -> Response {
    query(db, headers, uri, verifier, Lookup::SlackDecisionRequest).await
}

pub(super) async fn resend_auto_health_jobs(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
) -> Response {
    query(db, headers, uri, verifier, Lookup::ResendAutoHealthJobs).await
}

pub(super) async fn resend_auto_health(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
) -> Response {
    query(db, headers, uri, verifier, Lookup::ResendAutoHealth).await
}

pub(super) async fn resend_cooldown_health(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
) -> Response {
    query(db, headers, uri, verifier, Lookup::ResendCooldownHealth).await
}

pub(super) async fn slack_cooldown_health(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
) -> Response {
    query(db, headers, uri, verifier, Lookup::SlackCooldownHealth).await
}

pub(super) async fn discord_retry_health(State(db):State<Arc<Client>>,headers:HeaderMap,OriginalUri(uri):OriginalUri,verifier:Option<Extension<Arc<AccessTokenVerifier>>>) -> Response {
    query(db,headers,uri,verifier,Lookup::DiscordRetryHealth).await
}
pub(super) async fn discord_retry_list(State(db):State<Arc<Client>>,headers:HeaderMap,OriginalUri(uri):OriginalUri,verifier:Option<Extension<Arc<AccessTokenVerifier>>>) -> Response {
    query(db,headers,uri,verifier,Lookup::DiscordRetryList).await
}
pub(super) async fn fcm_unrecorded_claim_health(State(db):State<Arc<Client>>,headers:HeaderMap,OriginalUri(uri):OriginalUri,verifier:Option<Extension<Arc<AccessTokenVerifier>>>) -> Response {
    query(db,headers,uri,verifier,Lookup::FcmUnrecordedClaimHealth).await
}
pub(super) async fn fcm_unrecorded_claim_list(State(db):State<Arc<Client>>,headers:HeaderMap,OriginalUri(uri):OriginalUri,verifier:Option<Extension<Arc<AccessTokenVerifier>>>) -> Response {
    query(db,headers,uri,verifier,Lookup::FcmUnrecordedClaimList).await
}

pub(super) async fn fcm_cooldown_health(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
) -> Response {
    query(db, headers, uri, verifier, Lookup::FcmCooldownHealth).await
}

pub(super) async fn twilio_delayed_health(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
) -> Response {
    query(db, headers, uri, verifier, Lookup::TwilioDelayedHealth).await
}

pub(super) async fn fcm_partial_health(
    State(db): State<Arc<Client>>, headers: HeaderMap, OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
) -> Response {
    query(db, headers, uri, verifier, Lookup::FcmPartialHealth).await
}

pub(super) async fn fcm_partial_jobs(
    State(db): State<Arc<Client>>, headers: HeaderMap, OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
) -> Response {
    query(db, headers, uri, verifier, Lookup::FcmPartialJobs).await
}

pub(super) async fn fcm_unreviewed(State(db): State<Arc<Client>>, headers: HeaderMap, OriginalUri(uri): OriginalUri, verifier: Option<Extension<Arc<AccessTokenVerifier>>>) -> Response {
    query(db,headers,uri,verifier,Lookup::FcmUnreviewed).await
}
pub(super) async fn fcm_review_request(State(db): State<Arc<Client>>, headers: HeaderMap, OriginalUri(uri): OriginalUri, verifier: Option<Extension<Arc<AccessTokenVerifier>>>) -> Response {
    query(db,headers,uri,verifier,Lookup::FcmReviewRequest).await
}

pub(super) async fn resend_result(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
) -> Response {
    query(db, headers, uri, verifier, Lookup::ResendResult).await
}
pub(super) async fn resend_jobs(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
) -> Response {
    query(db, headers, uri, verifier, Lookup::ResendJobs).await
}
pub(super) async fn twilio_result(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
) -> Response {
    query(db, headers, uri, verifier, Lookup::TwilioResult).await
}
pub(super) async fn twilio_jobs(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
) -> Response {
    query(db, headers, uri, verifier, Lookup::TwilioJobs).await
}
pub(super) async fn fcm_result(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
) -> Response {
    query(db, headers, uri, verifier, Lookup::FcmResult).await
}
pub(super) async fn fcm_jobs(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
) -> Response {
    query(db, headers, uri, verifier, Lookup::FcmJobs).await
}
pub(super) async fn slack_result(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
) -> Response {
    query(db, headers, uri, verifier, Lookup::SlackResult).await
}
pub(super) async fn slack_jobs(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
) -> Response {
    query(db, headers, uri, verifier, Lookup::SlackJobs).await
}
pub(super) async fn sendgrid_result(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
) -> Response {
    query(db, headers, uri, verifier, Lookup::SendgridResult).await
}
pub(super) async fn sendgrid_jobs(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
) -> Response {
    query(db, headers, uri, verifier, Lookup::SendgridJobs).await
}
pub(super) async fn workflow_read(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
) -> Response {
    query(db, headers, uri, verifier, Lookup::WorkflowRead).await
}
pub(super) async fn workflow_audit(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
) -> Response {
    query(db, headers, uri, verifier, Lookup::WorkflowAudit).await
}
pub(super) async fn workflow_request(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
) -> Response {
    query(db, headers, uri, verifier, Lookup::WorkflowRequest).await
}
pub(super) async fn workflow_grant_read(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
) -> Response {
    query(db, headers, uri, verifier, Lookup::WorkflowGrantRead).await
}

pub(super) async fn channel_access_grant_read(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
) -> Response {
    query(db, headers, uri, verifier, Lookup::ChannelAccessGrantRead).await
}
pub(super) async fn workflow_grant_request(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
) -> Response {
    query(db, headers, uri, verifier, Lookup::WorkflowGrantRequest).await
}

pub(super) async fn channel_access_grant_request(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
) -> Response {
    query(
        db,
        headers,
        uri,
        verifier,
        Lookup::ChannelAccessGrantRequest,
    )
    .await
}

pub(super) async fn resend_auto_policy_read(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
) -> Response {
    query(db, headers, uri, verifier, Lookup::ResendAutoPolicyRead).await
}
pub(super) async fn resend_auto_policy_request(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
) -> Response {
    query(db, headers, uri, verifier, Lookup::ResendAutoPolicyRequest).await
}
pub(super) async fn resend_auto_policy_audit(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
) -> Response {
    query(db, headers, uri, verifier, Lookup::ResendAutoPolicyAudit).await
}

pub(super) async fn resend_cooldown_policy_read(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
) -> Response {
    query(db, headers, uri, verifier, Lookup::ResendCooldownPolicyRead).await
}
pub(super) async fn resend_cooldown_policy_request(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
) -> Response {
    query(db, headers, uri, verifier, Lookup::ResendCooldownPolicyRequest).await
}
pub(super) async fn resend_cooldown_policy_audit(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
) -> Response {
    query(db, headers, uri, verifier, Lookup::ResendCooldownPolicyAudit).await
}

pub(super) async fn discord_retry_policy_read(State(db):State<Arc<Client>>,headers:HeaderMap,OriginalUri(uri):OriginalUri,verifier:Option<Extension<Arc<AccessTokenVerifier>>>) -> Response {query(db,headers,uri,verifier,Lookup::DiscordRetryPolicyRead).await}
pub(super) async fn discord_retry_policy_request(State(db):State<Arc<Client>>,headers:HeaderMap,OriginalUri(uri):OriginalUri,verifier:Option<Extension<Arc<AccessTokenVerifier>>>) -> Response {query(db,headers,uri,verifier,Lookup::DiscordRetryPolicyRequest).await}
pub(super) async fn discord_retry_policy_audit(State(db):State<Arc<Client>>,headers:HeaderMap,OriginalUri(uri):OriginalUri,verifier:Option<Extension<Arc<AccessTokenVerifier>>>) -> Response {query(db,headers,uri,verifier,Lookup::DiscordRetryPolicyAudit).await}
pub(super) async fn discord_policy_read(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
) -> Response {
    query(db, headers, uri, verifier, Lookup::DiscordPolicyRead).await
}
pub(super) async fn discord_policy_request(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
) -> Response {
    query(db, headers, uri, verifier, Lookup::DiscordPolicyRequest).await
}
pub(super) async fn discord_policy_audit(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
) -> Response {
    query(db, headers, uri, verifier, Lookup::DiscordPolicyAudit).await
}

pub(super) async fn resend_429_grant_read(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
) -> Response {
    query(db, headers, uri, verifier, Lookup::Resend429GrantRead).await
}
pub(super) async fn resend_429_grant_request(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
) -> Response {
    query(
        db,
        headers,
        uri,
        verifier,
        Lookup::Resend429GrantRequest,
    )
    .await
}
pub(super) async fn resend_429_grant_audit(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
) -> Response {
    query(db, headers, uri, verifier, Lookup::Resend429GrantAudit).await
}

pub(super) async fn fcm_review_grant_read(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
) -> Response {
    query(db, headers, uri, verifier, Lookup::FcmReviewGrantRead).await
}
pub(super) async fn fcm_review_grant_request(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
) -> Response {
    query(
        db,
        headers,
        uri,
        verifier,
        Lookup::FcmReviewGrantRequest,
    )
    .await
}
pub(super) async fn fcm_review_grant_audit(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
) -> Response {
    query(db, headers, uri, verifier, Lookup::FcmReviewGrantAudit).await
}

pub(super) async fn slack_decision_grant_read(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
) -> Response {
    query(db, headers, uri, verifier, Lookup::SlackDecisionGrantRead).await
}
pub(super) async fn slack_decision_grant_request(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
) -> Response {
    query(
        db,
        headers,
        uri,
        verifier,
        Lookup::SlackDecisionGrantRequest,
    )
    .await
}
pub(super) async fn slack_decision_grant_audit(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
) -> Response {
    query(db, headers, uri, verifier, Lookup::SlackDecisionGrantAudit).await
}

pub(super) async fn subscriber_activity_audit(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
) -> Response {
    query(db, headers, uri, verifier, Lookup::SubscriberActivityAudit).await
}

pub(super) async fn subscriber_grant_audit(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
) -> Response {
    query(db, headers, uri, verifier, Lookup::SubscriberGrantAudit).await
}

pub(super) async fn subscriber_grant_read(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
) -> Response {
    query(db, headers, uri, verifier, Lookup::SubscriberGrantRead).await
}
pub(super) async fn subscriber_grant_request(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
) -> Response {
    query(db, headers, uri, verifier, Lookup::SubscriberGrantRequest).await
}
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SubscriberGrantChange {
    request_id: String,
    target_issuer: String,
    target_subject: String,
    target_client_id: String,
    permission: String,
    expected: bool,
    desired: bool,
    reason: String,
}
pub(super) async fn subscriber_grant_change(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
    body: Bytes,
) -> Response {
    let identity = match authenticate(&db, &headers, verifier).await {
        Ok(v) => v,
        Err(status) => return failure(status, uri.path()),
    };
    let params: Vec<_> = form_urlencoded::parse(uri.query().unwrap_or("").as_bytes()).collect();
    if params.len() != 1
        || params[0].0 != "environmentId"
        || params[0].1.is_empty()
        || params[0].1.len() > 512
        || params[0].1.contains('\0')
    {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    }
    let Some(input) = (body.len() <= 8192)
        .then(|| serde_json::from_slice::<SubscriberGrantChange>(&body).ok())
        .flatten()
    else {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    };
    let (Ok(request), Ok(exp)) = (
        Uuid::parse_str(&input.request_id),
        i64::try_from(identity.expires_at),
    ) else {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    };
    if [
        &input.target_issuer,
        &input.target_subject,
        &input.target_client_id,
        &input.permission,
        &input.reason,
    ]
    .iter()
    .any(|s| s.contains('\0'))
    {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    }
    let env = params[0].1.as_ref();
    match guarded_query(&db, &identity, "SELECT public.novu_rust_operator_jwt_subscriber_grant_change($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)",&[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&request,&input.target_issuer,&input.target_subject,&input.target_client_id,&input.permission,&input.expected,&input.desired,&input.reason]).await {
        Ok(row)=>([(header::CACHE_CONTROL,"no-store")],Json(row.get::<_,serde_json::Value>(0))).into_response(),
        Err(e) if e.code().is_some_and(|c|c.code()=="42501")=>failure(StatusCode::UNAUTHORIZED,uri.path()),
        Err(e) if e.code().is_some_and(|c|c.code()=="22023")=>failure(StatusCode::BAD_REQUEST,uri.path()),
        Err(e) if e.code().is_some_and(|c|c.code()=="P4090")=>failure(StatusCode::CONFLICT,uri.path()),
        Err(_)=>failure(StatusCode::INTERNAL_SERVER_ERROR,uri.path()),
    }
}

pub(super) async fn subscriber_delete_request(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
) -> Response {
    query(db, headers, uri, verifier, Lookup::SubscriberDeleteRequest).await
}
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SubscriberDelete {
    request_id: String,
    subscriber_id: String,
    generation: String,
    revision: String,
    reason: String,
}
pub(super) async fn subscriber_delete(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
    body: Bytes,
) -> Response {
    let identity = match authenticate(&db, &headers, verifier).await {
        Ok(v) => v,
        Err(status) => return failure(status, uri.path()),
    };
    let params: Vec<_> = form_urlencoded::parse(uri.query().unwrap_or("").as_bytes()).collect();
    if params.len() != 1
        || params[0].0 != "environmentId"
        || params[0].1.is_empty()
        || params[0].1.len() > 512
        || params[0].1.contains('\0')
    {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    }
    let Some(input) = (body.len() <= 8192)
        .then(|| serde_json::from_slice::<SubscriberDelete>(&body).ok())
        .flatten()
    else {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    };
    let (Ok(request), Ok(generation), Ok(revision), Ok(exp)) = (
        Uuid::parse_str(&input.request_id),
        Uuid::parse_str(&input.generation),
        input.revision.parse::<i64>(),
        i64::try_from(identity.expires_at),
    ) else {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    };
    if revision < 0
        || revision.to_string() != input.revision
        || input.subscriber_id.contains('\0')
        || input.reason.contains('\0')
    {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    }
    let env = params[0].1.as_ref();
    match guarded_query(
        &db,
        &identity,
        "SELECT public.novu_rust_operator_jwt_subscriber_delete($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",
        &[
            &identity.issuer,
            &identity.subject,
            &identity.client_id,
            &env,
            &exp,
            &request,
            &input.subscriber_id,
            &generation,
            &revision,
            &input.reason,
        ],
    )
    .await
    {
        Ok(row) => (
            [(header::CACHE_CONTROL, "no-store")],
            Json(row.get::<_, serde_json::Value>(0)),
        )
            .into_response(),
        Err(e) if e.code().is_some_and(|c| c.code() == "42501") => {
            failure(StatusCode::UNAUTHORIZED, uri.path())
        }
        Err(e) if e.code().is_some_and(|c| c.code() == "22023") => {
            failure(StatusCode::BAD_REQUEST, uri.path())
        }
        Err(e) if e.code().is_some_and(|c| c.code() == "P4090") => {
            failure(StatusCode::CONFLICT, uri.path())
        }
        Err(_) => failure(StatusCode::INTERNAL_SERVER_ERROR, uri.path()),
    }
}

pub(super) async fn subscriber_create_request(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
) -> Response {
    query(db, headers, uri, verifier, Lookup::SubscriberCreateRequest).await
}
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SubscriberCreate {
    request_id: String,
    subscriber_id: String,
    profile: serde_json::Value,
    reason: String,
}
pub(super) async fn subscriber_create(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
    body: Bytes,
) -> Response {
    let identity = match authenticate(&db, &headers, verifier).await {
        Ok(v) => v,
        Err(status) => return failure(status, uri.path()),
    };
    let params: Vec<_> = form_urlencoded::parse(uri.query().unwrap_or("").as_bytes()).collect();
    if params.len() != 1
        || params[0].0 != "environmentId"
        || params[0].1.is_empty()
        || params[0].1.len() > 512
        || params[0].1.contains('\0')
    {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    }
    let Some(input) = (body.len() <= 32768)
        .then(|| serde_json::from_slice::<SubscriberCreate>(&body).ok())
        .flatten()
    else {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    };
    let (Ok(request), Ok(exp)) = (
        Uuid::parse_str(&input.request_id),
        i64::try_from(identity.expires_at),
    ) else {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    };
    let Some(fields) = input.profile.as_object() else {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    };
    if input.subscriber_id.contains('\0')
        || input.reason.contains('\0')
        || json_contains_nul(&input.profile)
        || fields
            .keys()
            .any(|k| !["firstName", "lastName", "email", "phone"].contains(&k.as_str()))
        || !crate::profile_errors(fields).is_empty()
        || fields
            .get("email")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|v| novu_rust_contracts::email::validate(v).is_none())
    {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    }
    let env = params[0].1.as_ref();
    match guarded_query(
        &db,
        &identity,
        "SELECT public.novu_rust_operator_jwt_subscriber_create($1,$2,$3,$4,$5,$6,$7,$8,$9)",
        &[
            &identity.issuer,
            &identity.subject,
            &identity.client_id,
            &env,
            &exp,
            &request,
            &input.subscriber_id,
            &input.profile,
            &input.reason,
        ],
    )
    .await
    {
        Ok(row) => (
            [(header::CACHE_CONTROL, "no-store")],
            Json(row.get::<_, serde_json::Value>(0)),
        )
            .into_response(),
        Err(e) if e.code().is_some_and(|c| c.code() == "42501") => {
            failure(StatusCode::UNAUTHORIZED, uri.path())
        }
        Err(e) if e.code().is_some_and(|c| c.code() == "22023") => {
            failure(StatusCode::BAD_REQUEST, uri.path())
        }
        Err(e) if e.code().is_some_and(|c| c.code() == "P4090") => {
            failure(StatusCode::CONFLICT, uri.path())
        }
        Err(_) => failure(StatusCode::INTERNAL_SERVER_ERROR, uri.path()),
    }
}

pub(super) async fn subscriber_profile_request(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
) -> Response {
    query(db, headers, uri, verifier, Lookup::SubscriberProfileRequest).await
}

pub(super) async fn subscriber_profile_read(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
) -> Response {
    query(db, headers, uri, verifier, Lookup::SubscriberProfileRead).await
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SubscriberProfileWrite {
    request_id: Option<String>,
    subscriber_id: String,
    generation: String,
    revision: String,
    profile: serde_json::Value,
    reason: String,
}

pub(super) async fn subscriber_profile_write(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
    body: Bytes,
) -> Response {
    let identity = match authenticate(&db, &headers, verifier).await {
        Ok(v) => v,
        Err(status) => return failure(status, uri.path()),
    };
    let params: Vec<_> = form_urlencoded::parse(uri.query().unwrap_or("").as_bytes()).collect();
    if params.len() != 1
        || params[0].0 != "environmentId"
        || params[0].1.is_empty()
        || params[0].1.len() > 512
        || params[0].1.contains('\0')
    {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    }
    let Some(input) = (body.len() <= 32768)
        .then(|| serde_json::from_slice::<SubscriberProfileWrite>(&body).ok())
        .flatten()
    else {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    };
    let (Ok(generation), Ok(revision), Ok(exp)) = (
        Uuid::parse_str(&input.generation),
        input.revision.parse::<i64>(),
        i64::try_from(identity.expires_at),
    ) else {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    };
    let Some(fields) = input.profile.as_object() else {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    };
    if revision < 0
        || revision.to_string() != input.revision
        || input.subscriber_id.contains('\0')
        || input.reason.contains('\0')
        || json_contains_nul(&input.profile)
        || fields
            .keys()
            .any(|k| !["firstName", "lastName", "email", "phone"].contains(&k.as_str()))
        || !crate::profile_errors(fields).is_empty()
        || fields
            .get("email")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|s| novu_rust_contracts::email::validate(s).is_none())
    {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    }
    let env = params[0].1.as_ref();
    let v2 = uri.path().ends_with("/subscriber-profile-v2");
    let request = match (&input.request_id, v2) {
        (Some(value), true) => match Uuid::parse_str(value) {
            Ok(id) => Some(id),
            Err(_) => return failure(StatusCode::BAD_REQUEST, uri.path()),
        },
        (None, false) => None,
        _ => return failure(StatusCode::BAD_REQUEST, uri.path()),
    };
    let result = if let Some(request) = request {
        guarded_query(&db, &identity, "SELECT public.novu_rust_operator_jwt_subscriber_profile_write_v2($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",&[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&request,&input.subscriber_id,&generation,&revision,&input.profile,&input.reason]).await
    } else {
        guarded_query(&db, &identity, "SELECT public.novu_rust_operator_jwt_subscriber_profile_write($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",&[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&input.subscriber_id,&generation,&revision,&input.profile,&input.reason]).await
    };
    match result {
        Ok(row) => (
            [(header::CACHE_CONTROL, "no-store")],
            Json(row.get::<_, serde_json::Value>(0)),
        )
            .into_response(),
        Err(e) if e.code().is_some_and(|c| c.code() == "42501") => {
            failure(StatusCode::UNAUTHORIZED, uri.path())
        }
        Err(e) if e.code().is_some_and(|c| c.code() == "22023") => {
            failure(StatusCode::BAD_REQUEST, uri.path())
        }
        Err(e) if e.code().is_some_and(|c| c.code() == "P4090") => {
            failure(StatusCode::CONFLICT, uri.path())
        }
        Err(_) => failure(StatusCode::INTERNAL_SERVER_ERROR, uri.path()),
    }
}

pub(super) async fn workflow_grant_audit(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
) -> Response {
    query(db, headers, uri, verifier, Lookup::WorkflowGrantAudit).await
}

pub(super) async fn channel_access_grant_audit(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
) -> Response {
    query(db, headers, uri, verifier, Lookup::ChannelAccessGrantAudit).await
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WorkflowGrantChange {
    request_id: String,
    target_issuer: String,
    target_subject: String,
    target_client_id: String,
    resource: String,
    permission: String,
    expected: bool,
    desired: bool,
    reason: String,
}

pub(super) async fn workflow_grant_change(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
    body: Bytes,
) -> Response {
    let identity = match authenticate(&db, &headers, verifier).await {
        Ok(v) => v,
        Err(status) => return failure(status, uri.path()),
    };
    let params: Vec<_> = form_urlencoded::parse(uri.query().unwrap_or("").as_bytes()).collect();
    if params.len() != 1
        || params[0].0 != "environmentId"
        || params[0].1.is_empty()
        || params[0].1.len() > 512
        || params[0].1.contains('\0')
    {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    }
    let Some(input) = (body.len() <= 8192)
        .then(|| serde_json::from_slice::<WorkflowGrantChange>(&body).ok())
        .flatten()
    else {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    };
    if [
        &input.target_issuer,
        &input.target_subject,
        &input.target_client_id,
        &input.resource,
        &input.permission,
        &input.reason,
    ]
    .iter()
    .any(|s| s.contains('\0'))
    {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    }
    let (Ok(request), Ok(exp)) = (
        Uuid::parse_str(&input.request_id),
        i64::try_from(identity.expires_at),
    ) else {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    };
    let env = params[0].1.as_ref();
    match guarded_query(&db, &identity, "SELECT public.novu_rust_operator_jwt_workflow_grant_change($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)",&[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&request,&input.target_issuer,&input.target_subject,&input.target_client_id,&input.resource,&input.permission,&input.expected,&input.desired,&input.reason]).await{
        Ok(row)=>([(header::CACHE_CONTROL,"no-store")],Json(row.get::<_,serde_json::Value>(0))).into_response(),
        Err(e) if e.code().is_some_and(|c|c.code()=="42501")=>failure(StatusCode::UNAUTHORIZED,uri.path()),
        Err(e) if e.code().is_some_and(|c|c.code()=="22023")=>failure(StatusCode::BAD_REQUEST,uri.path()),
        Err(e) if e.code().is_some_and(|c|c.code()=="P4090")=>failure(StatusCode::CONFLICT,uri.path()),
        Err(_)=>failure(StatusCode::INTERNAL_SERVER_ERROR,uri.path()),
    }
}

pub(super) async fn channel_access_grant_change(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
    body: Bytes,
) -> Response {
    let identity = match authenticate(&db, &headers, verifier).await {
        Ok(v) => v,
        Err(status) => return failure(status, uri.path()),
    };
    let params: Vec<_> = form_urlencoded::parse(uri.query().unwrap_or("").as_bytes()).collect();
    if params.len() != 1
        || params[0].0 != "environmentId"
        || params[0].1.is_empty()
        || params[0].1.len() > 512
        || params[0].1.contains('\0')
    {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    }
    let Some(input) = (body.len() <= 8192)
        .then(|| serde_json::from_slice::<WorkflowGrantChange>(&body).ok())
        .flatten()
    else {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    };
    if [
        &input.target_issuer,
        &input.target_subject,
        &input.target_client_id,
        &input.resource,
        &input.permission,
        &input.reason,
    ]
    .iter()
    .any(|s| s.contains('\0'))
    {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    }
    let (Ok(request), Ok(exp)) = (
        Uuid::parse_str(&input.request_id),
        i64::try_from(identity.expires_at),
    ) else {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    };
    let env = params[0].1.as_ref();
    match guarded_query(&db, &identity, "SELECT public.novu_rust_operator_jwt_channel_access_grant_change($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)",&[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&request,&input.target_issuer,&input.target_subject,&input.target_client_id,&input.resource,&input.permission,&input.expected,&input.desired,&input.reason]).await{
        Ok(row)=>([(header::CACHE_CONTROL,"no-store")],Json(row.get::<_,serde_json::Value>(0))).into_response(),
        Err(e) if e.code().is_some_and(|c|c.code()=="42501")=>failure(StatusCode::UNAUTHORIZED,uri.path()),
        Err(e) if e.code().is_some_and(|c|c.code()=="22023")=>failure(StatusCode::BAD_REQUEST,uri.path()),
        Err(e) if e.code().is_some_and(|c|c.code()=="P4090")=>failure(StatusCode::CONFLICT,uri.path()),
        Err(_)=>failure(StatusCode::INTERNAL_SERVER_ERROR,uri.path()),
    }
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ResendAutoPolicyChange {
    request_id: String,
    expected_revision: String,
    enabled: bool,
    max_retries: i32,
    cutoff: String,
    reason: String,
}
pub(super) async fn resend_auto_policy_change(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
    body: Bytes,
) -> Response {
    let identity = match authenticate(&db, &headers, verifier).await {
        Ok(v) => v,
        Err(status) => return failure(status, uri.path()),
    };
    let params: Vec<_> = form_urlencoded::parse(uri.query().unwrap_or("").as_bytes()).collect();
    if params.len() != 1
        || params[0].0 != "environmentId"
        || params[0].1.is_empty()
        || params[0].1.len() > 512
        || params[0].1.contains('\0')
    {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    }
    let Some(input) = (body.len() <= 4096)
        .then(|| serde_json::from_slice::<ResendAutoPolicyChange>(&body).ok())
        .flatten()
    else {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    };
    let (Ok(request), Ok(exp), Ok(expected)) = (
        Uuid::parse_str(&input.request_id),
        i64::try_from(identity.expires_at),
        input.expected_revision.parse::<i64>(),
    ) else {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    };
    if expected < -1
        || expected.to_string() != input.expected_revision
        || !(1..=10).contains(&input.max_retries)
        || !["now", "preserve"].contains(&input.cutoff.as_str())
        || input.reason.trim().is_empty()
        || input.reason.len() > 512
        || input.reason.contains('\0')
    {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    }
    let env = params[0].1.as_ref();
    match guarded_query(&db,&identity,"SELECT public.novu_rust_operator_jwt_resend_auto_policy_change($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",&[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&request,&expected,&input.enabled,&input.max_retries,&input.cutoff,&input.reason]).await {
        Ok(row)=>([(header::CACHE_CONTROL,"no-store")],Json(row.get::<_,serde_json::Value>(0))).into_response(),
        Err(e) if e.code().is_some_and(|c|c.code()=="42501")=>failure(StatusCode::UNAUTHORIZED,uri.path()),
        Err(e) if e.code().is_some_and(|c|c.code()=="22023")=>failure(StatusCode::BAD_REQUEST,uri.path()),
        Err(e) if e.code().is_some_and(|c|c.code()=="P4090")=>failure(StatusCode::CONFLICT,uri.path()),
        Err(_)=>failure(StatusCode::INTERNAL_SERVER_ERROR,uri.path()),
    }
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ResendCooldownPolicyChange {
    request_id: String,
    expected_revision: String,
    fallback_seconds: i32,
    reason: String,
}
pub(super) async fn resend_cooldown_policy_change(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
    body: Bytes,
) -> Response {
    let identity = match authenticate(&db, &headers, verifier).await {
        Ok(v) => v,
        Err(status) => return failure(status, uri.path()),
    };
    let params: Vec<_> = form_urlencoded::parse(uri.query().unwrap_or("").as_bytes()).collect();
    if params.len() != 1
        || params[0].0 != "environmentId"
        || params[0].1.is_empty()
        || params[0].1.len() > 512
        || params[0].1.contains('\0')
    {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    }
    let Some(input) = (body.len() <= 4096)
        .then(|| serde_json::from_slice::<ResendCooldownPolicyChange>(&body).ok())
        .flatten()
    else {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    };
    let (Ok(request), Ok(exp), Ok(expected)) = (
        Uuid::parse_str(&input.request_id),
        i64::try_from(identity.expires_at),
        input.expected_revision.parse::<i64>(),
    ) else {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    };
    if expected < -1
        || expected.to_string() != input.expected_revision
        || !(1..=86400).contains(&input.fallback_seconds)
        || input.reason.trim().is_empty()
        || input.reason.len() > 512
        || input.reason.contains('\0')
    {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    }
    let env = params[0].1.as_ref();
    match guarded_query(&db,&identity,"SELECT public.novu_rust_operator_jwt_resend_cooldown_policy_change($1,$2,$3,$4,$5,$6,$7,$8,$9)",&[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&request,&expected,&input.fallback_seconds,&input.reason]).await {
        Ok(row)=>([(header::CACHE_CONTROL,"no-store")],Json(row.get::<_,serde_json::Value>(0))).into_response(),
        Err(e) if e.code().is_some_and(|c|c.code()=="42501")=>failure(StatusCode::UNAUTHORIZED,uri.path()),
        Err(e) if e.code().is_some_and(|c|c.code()=="22023")=>failure(StatusCode::BAD_REQUEST,uri.path()),
        Err(e) if e.code().is_some_and(|c|c.code()=="P4090")=>failure(StatusCode::CONFLICT,uri.path()),
        Err(_)=>failure(StatusCode::INTERNAL_SERVER_ERROR,uri.path()),
    }
}

#[derive(serde::Deserialize)]
#[serde(rename_all="camelCase",deny_unknown_fields)]
struct DiscordRetryPolicyChange {request_id:String,expected_revision:String,mode:String,max_retries:i32,fallback_seconds:i32,reason:String}
pub(super) async fn discord_retry_policy_change(State(db):State<Arc<Client>>,headers:HeaderMap,OriginalUri(uri):OriginalUri,verifier:Option<Extension<Arc<AccessTokenVerifier>>>,body:Bytes)->Response {
    let identity=match authenticate(&db,&headers,verifier).await {Ok(v)=>v,Err(status)=>return failure(status,uri.path())};
    let params:Vec<_>=form_urlencoded::parse(uri.query().unwrap_or("").as_bytes()).collect();
    if params.len()!=2 || params.iter().filter(|p|p.0=="environmentId").count()!=1 || params.iter().filter(|p|p.0=="destinationId").count()!=1 {return failure(StatusCode::BAD_REQUEST,uri.path());}
    let env=params.iter().find(|p|p.0=="environmentId").unwrap().1.as_ref();let dest=params.iter().find(|p|p.0=="destinationId").unwrap().1.as_ref();
    if env.is_empty()||env.len()>512||env.contains('\0')||!valid_discord_retry_destination(dest) {return failure(StatusCode::BAD_REQUEST,uri.path());}
    let Some(input)=(body.len()<=4096).then(||serde_json::from_slice::<DiscordRetryPolicyChange>(&body).ok()).flatten() else {return failure(StatusCode::BAD_REQUEST,uri.path());};
    let (Ok(request),Ok(exp),Ok(expected))=(Uuid::parse_str(&input.request_id),i64::try_from(identity.expires_at),input.expected_revision.parse::<i64>()) else {return failure(StatusCode::BAD_REQUEST,uri.path());};
    if expected<0||expected.to_string()!=input.expected_revision||!matches!(input.mode.as_str(),"off"|"known"|"fallback")||!(0..=10).contains(&input.max_retries)||!(1..=86400).contains(&input.fallback_seconds)||input.reason.trim().is_empty()||input.reason.len()>512||input.reason.contains('\0') {return failure(StatusCode::BAD_REQUEST,uri.path());}
    match guarded_query(&db,&identity,"SELECT public.novu_rust_operator_jwt_discord_retry_policy_change($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)",&[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&dest,&request,&expected,&input.mode,&input.max_retries,&input.fallback_seconds,&input.reason]).await {
        Ok(row)=>([(header::CACHE_CONTROL,"no-store")],Json(row.get::<_,serde_json::Value>(0))).into_response(),
        Err(e) if e.code().is_some_and(|c|c.code()=="42501")=>failure(StatusCode::UNAUTHORIZED,uri.path()),
        Err(e) if e.code().is_some_and(|c|c.code()=="22023")=>failure(StatusCode::BAD_REQUEST,uri.path()),
        Err(e) if e.code().is_some_and(|c|matches!(c.code(),"P4090"|"40001"))=>failure(StatusCode::CONFLICT,uri.path()),
        Err(_)=>failure(StatusCode::INTERNAL_SERVER_ERROR,uri.path()),
    }
}
fn valid_discord_retry_destination(v:&str)->bool {!v.is_empty()&&v.len()<=64&&v.bytes().all(|b|b.is_ascii_alphanumeric()||b"._-".contains(&b))}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DiscordPolicyChange {
    request_id: String,
    expected_revision: String,
    enabled: bool,
    reason: String,
}
pub(super) async fn discord_policy_change(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
    body: Bytes,
) -> Response {
    let identity = match authenticate(&db, &headers, verifier).await {
        Ok(v) => v,
        Err(status) => return failure(status, uri.path()),
    };
    let params: Vec<_> = form_urlencoded::parse(uri.query().unwrap_or("").as_bytes()).collect();
    if params.len() != 1
        || params[0].0 != "environmentId"
        || params[0].1.is_empty()
        || params[0].1.len() > 512
        || params[0].1.contains('\0')
    {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    }
    let Some(input) = (body.len() <= 4096)
        .then(|| serde_json::from_slice::<DiscordPolicyChange>(&body).ok())
        .flatten()
    else {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    };
    let (Ok(request), Ok(exp), Ok(expected)) = (
        Uuid::parse_str(&input.request_id),
        i64::try_from(identity.expires_at),
        input.expected_revision.parse::<i64>(),
    ) else {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    };
    if expected < 0
        || expected.to_string() != input.expected_revision
        || input.reason.trim().is_empty()
        || input.reason.len() > 512
        || input.reason.contains('\0')
    {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    }
    let env = params[0].1.as_ref();
    match guarded_query(&db,&identity,"SELECT public.novu_rust_operator_jwt_discord_policy_change($1,$2,$3,$4,$5,$6,$7,$8,$9)",&[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&request,&expected,&input.enabled,&input.reason]).await {
        Ok(row)=>([(header::CACHE_CONTROL,"no-store")],Json(row.get::<_,serde_json::Value>(0))).into_response(),
        Err(e) if e.code().is_some_and(|c|c.code()=="42501")=>failure(StatusCode::UNAUTHORIZED,uri.path()),
        Err(e) if e.code().is_some_and(|c|c.code()=="22023")=>failure(StatusCode::BAD_REQUEST,uri.path()),
        Err(e) if e.code().is_some_and(|c|c.code()=="P4090")=>failure(StatusCode::CONFLICT,uri.path()),
        Err(_)=>failure(StatusCode::INTERNAL_SERVER_ERROR,uri.path()),
    }
}

pub(super) async fn resend_429_grant_change(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
    body: Bytes,
) -> Response {
    let identity = match authenticate(&db, &headers, verifier).await {
        Ok(v) => v,
        Err(status) => return failure(status, uri.path()),
    };
    let params: Vec<_> = form_urlencoded::parse(uri.query().unwrap_or("").as_bytes()).collect();
    if params.len() != 1
        || params[0].0 != "environmentId"
        || params[0].1.is_empty()
        || params[0].1.len() > 512
        || params[0].1.contains('\0')
    {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    }
    let Some(input) = (body.len() <= 8192)
        .then(|| serde_json::from_slice::<WorkflowGrantChange>(&body).ok())
        .flatten()
    else {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    };
    if [
        &input.target_issuer,
        &input.target_subject,
        &input.target_client_id,
        &input.resource,
        &input.permission,
        &input.reason,
    ]
    .iter()
    .any(|s| s.contains('\0'))
    {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    }
    let (Ok(request), Ok(exp)) = (
        Uuid::parse_str(&input.request_id),
        i64::try_from(identity.expires_at),
    ) else {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    };
    let env = params[0].1.as_ref();
    match guarded_query(&db, &identity, "SELECT public.novu_rust_operator_jwt_resend_429_grant_change($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)",&[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&request,&input.target_issuer,&input.target_subject,&input.target_client_id,&input.resource,&input.permission,&input.expected,&input.desired,&input.reason]).await{
        Ok(row)=>([(header::CACHE_CONTROL,"no-store")],Json(row.get::<_,serde_json::Value>(0))).into_response(),
        Err(e) if e.code().is_some_and(|c|c.code()=="42501")=>failure(StatusCode::UNAUTHORIZED,uri.path()),
        Err(e) if e.code().is_some_and(|c|c.code()=="22023")=>failure(StatusCode::BAD_REQUEST,uri.path()),
        Err(e) if e.code().is_some_and(|c|c.code()=="P4090")=>{
            let mut response=error_at(StatusCode::CONFLICT,"Grant changed or request ID conflict","Conflict",uri.path()).into_response();
            response.headers_mut().insert(header::CACHE_CONTROL,header::HeaderValue::from_static("no-store"));
            response
        },
        Err(_)=>failure(StatusCode::INTERNAL_SERVER_ERROR,uri.path()),
    }
}

pub(super) async fn fcm_review_grant_change(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
    body: Bytes,
) -> Response {
    let identity = match authenticate(&db, &headers, verifier).await {
        Ok(v) => v,
        Err(status) => return failure(status, uri.path()),
    };
    let params: Vec<_> = form_urlencoded::parse(uri.query().unwrap_or("").as_bytes()).collect();
    if params.len() != 1
        || params[0].0 != "environmentId"
        || params[0].1.is_empty()
        || params[0].1.len() > 512
        || params[0].1.contains('\0')
    {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    }
    let Some(input) = (body.len() <= 8192)
        .then(|| serde_json::from_slice::<WorkflowGrantChange>(&body).ok())
        .flatten()
    else {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    };
    if [
        &input.target_issuer,
        &input.target_subject,
        &input.target_client_id,
        &input.resource,
        &input.permission,
        &input.reason,
    ]
    .iter()
    .any(|s| s.contains('\0'))
    {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    }
    let (Ok(request), Ok(exp)) = (
        Uuid::parse_str(&input.request_id),
        i64::try_from(identity.expires_at),
    ) else {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    };
    let env = params[0].1.as_ref();
    match guarded_query(&db, &identity, "SELECT public.novu_rust_operator_jwt_fcm_review_grant_change($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)",&[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&request,&input.target_issuer,&input.target_subject,&input.target_client_id,&input.resource,&input.permission,&input.expected,&input.desired,&input.reason]).await{
        Ok(row)=>([(header::CACHE_CONTROL,"no-store")],Json(row.get::<_,serde_json::Value>(0))).into_response(),
        Err(e) if e.code().is_some_and(|c|c.code()=="42501")=>failure(StatusCode::UNAUTHORIZED,uri.path()),
        Err(e) if e.code().is_some_and(|c|c.code()=="22023")=>failure(StatusCode::BAD_REQUEST,uri.path()),
        Err(e) if e.code().is_some_and(|c|c.code()=="P4090")=>{
            let mut response=error_at(StatusCode::CONFLICT,"Grant changed or request ID conflict","Conflict",uri.path()).into_response();
            response.headers_mut().insert(header::CACHE_CONTROL,header::HeaderValue::from_static("no-store"));
            response
        },
        Err(_)=>failure(StatusCode::INTERNAL_SERVER_ERROR,uri.path()),
    }
}

pub(super) async fn discord_review_grant_change(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
    body: Bytes,
) -> Response {
    let identity = match authenticate(&db, &headers, verifier).await {
        Ok(v) => v,
        Err(status) => return failure(status, uri.path()),
    };
    let params: Vec<_> = form_urlencoded::parse(uri.query().unwrap_or("").as_bytes()).collect();
    if params.len() != 1
        || params[0].0 != "environmentId"
        || params[0].1.is_empty()
        || params[0].1.len() > 512
        || params[0].1.contains('\0')
    {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    }
    let Some(input) = (body.len() <= 8192)
        .then(|| serde_json::from_slice::<WorkflowGrantChange>(&body).ok())
        .flatten()
    else {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    };
    if [
        &input.target_issuer,
        &input.target_subject,
        &input.target_client_id,
        &input.resource,
        &input.permission,
        &input.reason,
    ]
    .iter()
    .any(|s| s.contains('\0'))
    {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    }
    let (Ok(request), Ok(exp)) = (
        Uuid::parse_str(&input.request_id),
        i64::try_from(identity.expires_at),
    ) else {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    };
    let env = params[0].1.as_ref();
    match guarded_query(&db, &identity, "SELECT public.novu_rust_operator_jwt_discord_review_grant_change($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)",&[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&request,&input.target_issuer,&input.target_subject,&input.target_client_id,&input.resource,&input.permission,&input.expected,&input.desired,&input.reason]).await{
        Ok(row)=>([(header::CACHE_CONTROL,"no-store")],Json(row.get::<_,serde_json::Value>(0))).into_response(),
        Err(e) if e.code().is_some_and(|c|c.code()=="42501")=>failure(StatusCode::UNAUTHORIZED,uri.path()),
        Err(e) if e.code().is_some_and(|c|c.code()=="22023")=>failure(StatusCode::BAD_REQUEST,uri.path()),
        Err(e) if e.code().is_some_and(|c|c.code()=="P4090")=>{
            let mut response=error_at(StatusCode::CONFLICT,"Grant changed or request ID conflict","Conflict",uri.path()).into_response();
            response.headers_mut().insert(header::CACHE_CONTROL,header::HeaderValue::from_static("no-store"));
            response
        },
        Err(_)=>failure(StatusCode::INTERNAL_SERVER_ERROR,uri.path()),
    }
}

pub(super) async fn discord_policy_grant_change(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
    body: Bytes,
) -> Response {
    let identity = match authenticate(&db, &headers, verifier).await {
        Ok(v) => v,
        Err(status) => return failure(status, uri.path()),
    };
    let params: Vec<_> = form_urlencoded::parse(uri.query().unwrap_or("").as_bytes()).collect();
    if params.len() != 1
        || params[0].0 != "environmentId"
        || params[0].1.is_empty()
        || params[0].1.len() > 512
        || params[0].1.contains('\0')
    {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    }
    let Some(input) = (body.len() <= 8192)
        .then(|| serde_json::from_slice::<WorkflowGrantChange>(&body).ok())
        .flatten()
    else {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    };
    if [
        &input.target_issuer,
        &input.target_subject,
        &input.target_client_id,
        &input.resource,
        &input.permission,
        &input.reason,
    ]
    .iter()
    .any(|s| s.contains('\0'))
    {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    }
    let (Ok(request), Ok(exp)) = (
        Uuid::parse_str(&input.request_id),
        i64::try_from(identity.expires_at),
    ) else {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    };
    let env = params[0].1.as_ref();
    match guarded_query(&db, &identity, "SELECT public.novu_rust_operator_jwt_discord_policy_grant_change($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)",&[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&request,&input.target_issuer,&input.target_subject,&input.target_client_id,&input.resource,&input.permission,&input.expected,&input.desired,&input.reason]).await{
        Ok(row)=>([(header::CACHE_CONTROL,"no-store")],Json(row.get::<_,serde_json::Value>(0))).into_response(),
        Err(e) if e.code().is_some_and(|c|c.code()=="42501")=>failure(StatusCode::UNAUTHORIZED,uri.path()),
        Err(e) if e.code().is_some_and(|c|c.code()=="22023")=>failure(StatusCode::BAD_REQUEST,uri.path()),
        Err(e) if e.code().is_some_and(|c|c.code()=="P4090")=>{
            let mut response=error_at(StatusCode::CONFLICT,"Grant changed or request ID conflict","Conflict",uri.path()).into_response();
            response.headers_mut().insert(header::CACHE_CONTROL,header::HeaderValue::from_static("no-store"));
            response
        },
        Err(_)=>failure(StatusCode::INTERNAL_SERVER_ERROR,uri.path()),
    }
}

pub(super) async fn discord_retry_grant_change(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
    body: Bytes,
) -> Response {
    let identity = match authenticate(&db, &headers, verifier).await {
        Ok(v) => v,
        Err(status) => return failure(status, uri.path()),
    };
    let params: Vec<_> = form_urlencoded::parse(uri.query().unwrap_or("").as_bytes()).collect();
    if params.len() != 1
        || params[0].0 != "environmentId"
        || params[0].1.is_empty()
        || params[0].1.len() > 512
        || params[0].1.contains('\0')
    {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    }
    let Some(input) = (body.len() <= 8192)
        .then(|| serde_json::from_slice::<WorkflowGrantChange>(&body).ok())
        .flatten()
    else {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    };
    if [
        &input.target_issuer,
        &input.target_subject,
        &input.target_client_id,
        &input.resource,
        &input.permission,
        &input.reason,
    ]
    .iter()
    .any(|s| s.contains('\0'))
    {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    }
    let (Ok(request), Ok(exp)) = (
        Uuid::parse_str(&input.request_id),
        i64::try_from(identity.expires_at),
    ) else {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    };
    let env = params[0].1.as_ref();
    match guarded_query(&db, &identity, "SELECT public.novu_rust_operator_jwt_discord_retry_grant_change($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)",&[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&request,&input.target_issuer,&input.target_subject,&input.target_client_id,&input.resource,&input.permission,&input.expected,&input.desired,&input.reason]).await{
        Ok(row)=>([(header::CACHE_CONTROL,"no-store")],Json(row.get::<_,serde_json::Value>(0))).into_response(),
        Err(e) if e.code().is_some_and(|c|c.code()=="42501")=>failure(StatusCode::UNAUTHORIZED,uri.path()),
        Err(e) if e.code().is_some_and(|c|c.code()=="22023")=>failure(StatusCode::BAD_REQUEST,uri.path()),
        Err(e) if e.code().is_some_and(|c|c.code()=="P4090")=>{
            let mut response=error_at(StatusCode::CONFLICT,"Grant changed or request ID conflict","Conflict",uri.path()).into_response();
            response.headers_mut().insert(header::CACHE_CONTROL,header::HeaderValue::from_static("no-store"));
            response
        },
        Err(_)=>failure(StatusCode::INTERNAL_SERVER_ERROR,uri.path()),
    }
}

pub(super) async fn slack_decision_grant_change(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
    body: Bytes,
) -> Response {
    let identity = match authenticate(&db, &headers, verifier).await {
        Ok(v) => v,
        Err(status) => return failure(status, uri.path()),
    };
    let params: Vec<_> = form_urlencoded::parse(uri.query().unwrap_or("").as_bytes()).collect();
    if params.len() != 1
        || params[0].0 != "environmentId"
        || params[0].1.is_empty()
        || params[0].1.len() > 512
        || params[0].1.contains('\0')
    {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    }
    let Some(input) = (body.len() <= 8192)
        .then(|| serde_json::from_slice::<WorkflowGrantChange>(&body).ok())
        .flatten()
    else {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    };
    if [
        &input.target_issuer,
        &input.target_subject,
        &input.target_client_id,
        &input.resource,
        &input.permission,
        &input.reason,
    ]
    .iter()
    .any(|s| s.contains('\0'))
    {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    }
    let (Ok(request), Ok(exp)) = (
        Uuid::parse_str(&input.request_id),
        i64::try_from(identity.expires_at),
    ) else {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    };
    let env = params[0].1.as_ref();
    match guarded_query(&db, &identity, "SELECT public.novu_rust_operator_jwt_slack_decision_grant_change($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)",&[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&request,&input.target_issuer,&input.target_subject,&input.target_client_id,&input.resource,&input.permission,&input.expected,&input.desired,&input.reason]).await{
        Ok(row)=>([(header::CACHE_CONTROL,"no-store")],Json(row.get::<_,serde_json::Value>(0))).into_response(),
        Err(e) if e.code().is_some_and(|c|c.code()=="42501")=>failure(StatusCode::UNAUTHORIZED,uri.path()),
        Err(e) if e.code().is_some_and(|c|c.code()=="22023")=>failure(StatusCode::BAD_REQUEST,uri.path()),
        Err(e) if e.code().is_some_and(|c|c.code()=="P4090")=>{
            let mut response=error_at(StatusCode::CONFLICT,"Grant changed or request ID conflict","Conflict",uri.path()).into_response();
            response.headers_mut().insert(header::CACHE_CONTROL,header::HeaderValue::from_static("no-store"));
            response
        },
        Err(_)=>failure(StatusCode::INTERNAL_SERVER_ERROR,uri.path()),
    }
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WorkflowWriteBody {
    workflow: String,
    resource: String,
    expected: serde_json::Value,
    desired: serde_json::Value,
    reason: String,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WorkflowWriteV2Body {
    request_id: String,
    workflow: String,
    resource: String,
    expected: serde_json::Value,
    desired: serde_json::Value,
    reason: String,
}

fn json_contains_nul(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::String(s) => s.contains('\0'),
        serde_json::Value::Array(v) => v.iter().any(json_contains_nul),
        serde_json::Value::Object(v) => v
            .iter()
            .any(|(k, v)| k.contains('\0') || json_contains_nul(v)),
        _ => false,
    }
}

pub(super) async fn workflow_write_v2(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
    body: Bytes,
) -> Response {
    let identity = match authenticate(&db, &headers, verifier).await {
        Ok(v) => v,
        Err(status) => return failure(status, uri.path()),
    };
    let params: Vec<_> = form_urlencoded::parse(uri.query().unwrap_or("").as_bytes()).collect();
    if params.len() != 1
        || params[0].0 != "environmentId"
        || params[0].1.is_empty()
        || params[0].1.len() > 512
        || params[0].1.contains('\0')
    {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    }
    let Some(input) = (body.len() <= 1_000_000)
        .then(|| serde_json::from_slice::<WorkflowWriteV2Body>(&body).ok())
        .flatten()
    else {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    };
    if input.workflow.contains('\0')
        || input.resource.contains('\0')
        || input.reason.contains('\0')
        || json_contains_nul(&input.expected)
        || json_contains_nul(&input.desired)
    {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    }
    let (Ok(request), Ok(exp)) = (
        Uuid::parse_str(&input.request_id),
        i64::try_from(identity.expires_at),
    ) else {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    };
    let env = params[0].1.as_ref();
    match guarded_query(&db, &identity, "SELECT public.novu_rust_operator_jwt_workflow_write_v2($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",&[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&request,&input.workflow,&input.resource,&input.expected,&input.desired,&input.reason]).await {
        Ok(row)=>([(header::CACHE_CONTROL,"no-store")],Json(row.get::<_,serde_json::Value>(0))).into_response(),
        Err(e) if e.code().is_some_and(|c|c.code()=="42501")=>failure(StatusCode::UNAUTHORIZED,uri.path()),
        Err(e) if e.code().is_some_and(|c|c.code()=="22023")=>failure(StatusCode::BAD_REQUEST,uri.path()),
        Err(e) if e.code().is_some_and(|c|c.code()=="P4090")=>failure(StatusCode::CONFLICT,uri.path()),
        Err(_)=>failure(StatusCode::INTERNAL_SERVER_ERROR,uri.path()),
    }
}

pub(super) async fn workflow_write(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
    body: Bytes,
) -> Response {
    let identity = match authenticate(&db, &headers, verifier).await {
        Ok(v) => v,
        Err(status) => return failure(status, uri.path()),
    };
    let params: Vec<_> = form_urlencoded::parse(uri.query().unwrap_or("").as_bytes()).collect();
    if params.len() != 1
        || params[0].0 != "environmentId"
        || params[0].1.is_empty()
        || params[0].1.len() > 512
        || params[0].1.contains('\0')
    {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    }
    let Some(input) = (body.len() <= 1_000_000)
        .then(|| serde_json::from_slice::<WorkflowWriteBody>(&body).ok())
        .flatten()
    else {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    };
    if input.workflow.contains('\0')
        || input.reason.contains('\0')
        || input.resource.contains('\0')
        || json_contains_nul(&input.expected)
        || json_contains_nul(&input.desired)
    {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    }
    let Ok(exp) = i64::try_from(identity.expires_at) else {
        return failure(StatusCode::UNAUTHORIZED, uri.path());
    };
    let env = params[0].1.as_ref();
    match guarded_query(
        &db,
        &identity,
        "SELECT public.novu_rust_operator_jwt_workflow_write($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",
        &[
            &identity.issuer,
            &identity.subject,
            &identity.client_id,
            &env,
            &exp,
            &input.workflow,
            &input.resource,
            &input.expected,
            &input.desired,
            &input.reason,
        ],
    )
    .await
    {
        Ok(row) => (
            [(header::CACHE_CONTROL, "no-store")],
            Json(row.get::<_, serde_json::Value>(0)),
        )
            .into_response(),
        Err(e) if e.code().is_some_and(|c| c.code() == "42501") => {
            failure(StatusCode::UNAUTHORIZED, uri.path())
        }
        Err(e) if e.code().is_some_and(|c| c.code() == "22023") => {
            failure(StatusCode::BAD_REQUEST, uri.path())
        }
        Err(e) if e.code().is_some_and(|c| c.code() == "P4090") => {
            failure(StatusCode::CONFLICT, uri.path())
        }
        Err(_) => failure(StatusCode::INTERNAL_SERVER_ERROR, uri.path()),
    }
}
async fn query(
    db: Arc<Client>,
    headers: HeaderMap,
    uri: Uri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
    lookup: Lookup,
) -> Response {
    let failure = |status, message, label| {
        let mut response = error_at(status, message, label, uri.path()).into_response();
        response.headers_mut().insert(
            header::CACHE_CONTROL,
            header::HeaderValue::from_static("no-store"),
        );
        response
    };
    let identity = match authenticate(&db, &headers, verifier).await {
        Ok(identity) => identity,
        Err(status) => return self::failure(status, uri.path()),
    };
    let mut params = HashMap::new();
    for (key, value) in form_urlencoded::parse(uri.query().unwrap_or("").as_bytes()) {
        if params
            .insert(key.into_owned(), value.into_owned())
            .is_some()
        {
            return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
        }
    }
    let allowed: &[&str] = match lookup {
        Lookup::Subscribers => &["environmentId", "name", "after", "before", "limit"],
        Lookup::Alerts => &["environmentId", "afterChannel", "afterTarget"],
        Lookup::DecisionRequest | Lookup::SlackDecisionRequest | Lookup::Resend429Request | Lookup::TwilioRetryRequest | Lookup::FcmReviewRequest => &["environmentId", "requestId"],
        Lookup::DiscordRetryPolicyRead => &["environmentId","destinationId"],
        Lookup::DiscordRetryPolicyRequest => &["environmentId","destinationId","requestId"],
        Lookup::DiscordRetryPolicyAudit => &["environmentId","destinationId","after"],
        Lookup::Identity | Lookup::ResendAutoPolicyRead | Lookup::ResendCooldownPolicyRead | Lookup::DiscordPolicyRead => &["environmentId"],
        Lookup::PermissionIdentity => &["environmentId", "issuer", "subject", "clientId"],
        Lookup::PermissionAudit => &["environmentId", "before"],
        Lookup::PermissionChangeRequest | Lookup::ResendAutoPolicyRequest | Lookup::ResendCooldownPolicyRequest | Lookup::DiscordPolicyRequest => {
            &["environmentId", "requestId"]
        }
        Lookup::ResendResult
        | Lookup::TwilioResult
        | Lookup::FcmResult
        | Lookup::SlackResult
        | Lookup::SendgridResult => &["environmentId", "jobId"],
        Lookup::ResendAutoHealth | Lookup::SlackCooldownHealth | Lookup::ResendCooldownHealth | Lookup::FcmCooldownHealth | Lookup::TwilioDelayedHealth => &["environmentId", "graceSeconds"],
        Lookup::DiscordRetryHealth => &["environmentId", "graceSeconds"],
        Lookup::DiscordRetryList => &["environmentId", "graceSeconds", "filter", "after"],
        Lookup::FcmUnrecordedClaimHealth => &["environmentId", "minGap", "graceSeconds"],
        Lookup::FcmUnrecordedClaimList => &["environmentId", "minGap", "graceSeconds", "after"],
        Lookup::FcmPartialHealth => &["environmentId", "windowSeconds"],
        Lookup::FcmPartialJobs => &["environmentId", "windowSeconds", "after"],
        Lookup::FcmUnreviewed => &["environmentId", "after"],
        Lookup::ResendAutoHealthJobs => &["environmentId", "graceSeconds", "filter", "after"],
        Lookup::ResendJobs => &["environmentId", "after", "status"],
        Lookup::TwilioJobs | Lookup::FcmJobs | Lookup::SlackJobs | Lookup::SendgridJobs => {
            &["environmentId", "after"]
        }
        Lookup::WorkflowRead => &["environmentId", "workflow", "resource"],
        Lookup::WorkflowAudit => &["environmentId", "workflow", "resource", "after"],
        Lookup::WorkflowRequest => &["environmentId", "resource", "requestId"],
        Lookup::WorkflowGrantRead
        | Lookup::ChannelAccessGrantRead
        | Lookup::Resend429GrantRead
        | Lookup::FcmReviewGrantRead
        | Lookup::DiscordReviewGrantRead
        | Lookup::DiscordPolicyGrantRead
        | Lookup::DiscordRetryGrantRead
        | Lookup::SlackDecisionGrantRead => &[
            "environmentId",
            "targetIssuer",
            "targetSubject",
            "targetClientId",
            "resource",
            "permission",
        ],
        Lookup::WorkflowGrantRequest
        | Lookup::ChannelAccessGrantRequest
        | Lookup::Resend429GrantRequest
        | Lookup::FcmReviewGrantRequest
        | Lookup::DiscordReviewGrantRequest
        | Lookup::DiscordPolicyGrantRequest
        | Lookup::DiscordRetryGrantRequest
        | Lookup::SlackDecisionGrantRequest => &["environmentId", "requestId"],
        Lookup::WorkflowGrantAudit
        | Lookup::SlackDecisionGrantAudit
        | Lookup::Resend429GrantAudit
        | Lookup::FcmReviewGrantAudit
        | Lookup::DiscordReviewGrantAudit
        | Lookup::DiscordPolicyGrantAudit
        | Lookup::DiscordRetryGrantAudit
        | Lookup::ResendAutoPolicyAudit | Lookup::ResendCooldownPolicyAudit | Lookup::DiscordPolicyAudit | Lookup::Resend429Audit => &["environmentId", "after"],
        Lookup::ChannelAccessGrantAudit => &["environmentId", "after", "resource"],
        Lookup::SubscriberProfileRead => &["environmentId", "subscriberId"],
        Lookup::SubscriberProfileRequest => &["environmentId", "requestId"],
        Lookup::SubscriberCreateRequest => &["environmentId", "requestId"],
        Lookup::SubscriberDeleteRequest => &["environmentId", "requestId"],
        Lookup::SubscriberGrantRequest => &["environmentId", "requestId"],
        Lookup::SubscriberGrantAudit => &["environmentId", "after"],
        Lookup::SubscriberActivityAudit => &["environmentId", "kind", "after"],
        Lookup::SubscriberGrantRead => &[
            "environmentId",
            "targetIssuer",
            "targetSubject",
            "targetClientId",
            "permission",
        ],
    };
    if params.keys().any(|key| !allowed.contains(&key.as_str()))
        || params.get("limit").is_some_and(|limit| limit != "20")
    {
        return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
    }
    let Some(env) = params
        .get("environmentId")
        .filter(|env| !env.is_empty() && env.len() <= 512 && !env.contains('\0'))
    else {
        return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
    };
    let Ok(exp) = i64::try_from(identity.expires_at) else {
        return failure(StatusCode::UNAUTHORIZED, "Unauthorized", "Unauthorized");
    };
    let result = match lookup {
        Lookup::ResendAutoHealthJobs => {
            let Some(grace) = params.get("graceSeconds").and_then(|v| {
                v.parse::<i32>()
                    .ok()
                    .filter(|n| (1..=86400).contains(n) && n.to_string() == *v)
            }) else {
                return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
            };
            let Some(filter) = params.get("filter").filter(|v| {
                [
                    "rate-limited",
                    "exhausted",
                    "guard-deferred",
                    "overdue-auto",
                ]
                .contains(&v.as_str())
            }) else {
                return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
            };
            let after = match params.get("after") {
                Some(value) => match Uuid::parse_str(value) {
                    Ok(v) => Some(v),
                    Err(_) => {
                        return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
                    }
                },
                None => None,
            };
            guarded_query(&db,&identity,"SELECT public.novu_rust_operator_jwt_resend_auto_health_jobs($1,$2,$3,$4,$5,$6,$7,$8)",&[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&grace,&filter,&after]).await
        }
        Lookup::ResendAutoHealth => {
            let Some(grace) = params.get("graceSeconds").and_then(|v| {
                v.parse::<i32>()
                    .ok()
                    .filter(|n| (1..=86400).contains(n) && n.to_string() == *v)
            }) else {
                return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
            };
            guarded_query(
                &db,
                &identity,
                "SELECT public.novu_rust_operator_jwt_resend_auto_health($1,$2,$3,$4,$5,$6)",
                &[
                    &identity.issuer,
                    &identity.subject,
                    &identity.client_id,
                    &env,
                    &exp,
                    &grace,
                ],
            )
            .await
        }
        Lookup::SlackCooldownHealth => {
            let Some(grace) = params.get("graceSeconds").and_then(|v| {
                v.parse::<i32>()
                    .ok()
                    .filter(|n| (1..=86400).contains(n) && n.to_string() == *v)
            }) else {
                return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
            };
            guarded_query(
                &db,
                &identity,
                "SELECT public.novu_rust_operator_jwt_slack_cooldown_health($1,$2,$3,$4,$5,$6)",
                &[
                    &identity.issuer,
                    &identity.subject,
                    &identity.client_id,
                    &env,
                    &exp,
                    &grace,
                ],
            )
            .await
        }
        Lookup::ResendCooldownHealth => {
            let Some(grace) = params.get("graceSeconds").and_then(|v| {
                v.parse::<i32>()
                    .ok()
                    .filter(|n| (1..=86400).contains(n) && n.to_string() == *v)
            }) else {
                return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
            };
            guarded_query(
                &db,
                &identity,
                "SELECT public.novu_rust_operator_jwt_resend_cooldown_health($1,$2,$3,$4,$5,$6)",
                &[
                    &identity.issuer,
                    &identity.subject,
                    &identity.client_id,
                    &env,
                    &exp,
                    &grace,
                ],
            )
            .await
        }
        Lookup::DiscordRetryHealth | Lookup::DiscordRetryList => {
            let Some(grace)=params.get("graceSeconds").and_then(|v|v.parse::<i32>().ok().filter(|n|(1..=86400).contains(n)&&n.to_string()==*v)) else {return failure(StatusCode::BAD_REQUEST,"Invalid request","Bad Request");};
            let list=matches!(lookup,Lookup::DiscordRetryList);
            let overdue=if list {match params.get("filter").map(String::as_str) {Some("all")=>false,Some("overdue")=>true,_=>return failure(StatusCode::BAD_REQUEST,"Invalid request","Bad Request")}} else {false};
            let after=match params.get("after") { Some(v)=>match Uuid::parse_str(v){Ok(v)=>Some(v),Err(_)=>return failure(StatusCode::BAD_REQUEST,"Invalid request","Bad Request")},None=>None};
            guarded_query(&db,&identity,"SELECT public.novu_rust_operator_jwt_discord_retry_diagnostics($1,$2,$3,$4,$5,$6,$7,$8,$9)",&[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&grace,&list,&overdue,&after]).await
        }
        Lookup::FcmUnrecordedClaimHealth | Lookup::FcmUnrecordedClaimList => {
            let Some(gap)=params.get("minGap").and_then(|v|v.parse::<i32>().ok().filter(|n|(2..=100000).contains(n)&&n.to_string()==*v)) else {return failure(StatusCode::BAD_REQUEST,"Invalid request","Bad Request");};
            let Some(grace)=params.get("graceSeconds").and_then(|v|v.parse::<i32>().ok().filter(|n|(1..=86400).contains(n)&&n.to_string()==*v)) else {return failure(StatusCode::BAD_REQUEST,"Invalid request","Bad Request");};
            let after=match params.get("after") { Some(v)=>match Uuid::parse_str(v){Ok(v)=>Some(v),Err(_)=>return failure(StatusCode::BAD_REQUEST,"Invalid request","Bad Request")},None=>None};
            let list=matches!(lookup,Lookup::FcmUnrecordedClaimList);
            guarded_query(&db,&identity,"SELECT public.novu_rust_operator_jwt_fcm_unrecorded_claims($1,$2,$3,$4,$5,$6,$7,$8,$9)",&[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&gap,&grace,&list,&after]).await
        }
        Lookup::FcmCooldownHealth => {
            let Some(grace) = params.get("graceSeconds").and_then(|v| {
                v.parse::<i32>()
                    .ok()
                    .filter(|n| (1..=86400).contains(n) && n.to_string() == *v)
            }) else {
                return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
            };
            guarded_query(
                &db,
                &identity,
                "SELECT public.novu_rust_operator_jwt_fcm_cooldown_health($1,$2,$3,$4,$5,$6)",
                &[
                    &identity.issuer,
                    &identity.subject,
                    &identity.client_id,
                    &env,
                    &exp,
                    &grace,
                ],
            )
            .await
        }
        Lookup::TwilioDelayedHealth => {
            let Some(grace) = params.get("graceSeconds").and_then(|v| {
                v.parse::<i32>()
                    .ok()
                    .filter(|n| (1..=86400).contains(n) && n.to_string() == *v)
            }) else {
                return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
            };
            guarded_query(
                &db,
                &identity,
                "SELECT public.novu_rust_operator_jwt_twilio_delayed_health($1,$2,$3,$4,$5,$6)",
                &[
                    &identity.issuer,
                    &identity.subject,
                    &identity.client_id,
                    &env,
                    &exp,
                    &grace,
                ],
            )
            .await
        }
        Lookup::FcmPartialHealth => {
            let Some(window) = params.get("windowSeconds").and_then(|v| v.parse::<i32>().ok().filter(|n| (1..=604800).contains(n) && n.to_string() == *v)) else {
                return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
            };
            guarded_query(&db, &identity, "SELECT public.novu_rust_operator_jwt_fcm_partial_health($1,$2,$3,$4,$5,$6)", &[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&window]).await
        }
        Lookup::FcmPartialJobs => {
            let Some(window)=params.get("windowSeconds").and_then(|v|v.parse::<i32>().ok().filter(|n|(1..=604800).contains(n)&&n.to_string()==*v)) else {return failure(StatusCode::BAD_REQUEST,"Invalid request","Bad Request");};
            let after=match params.get("after") {Some(value)=>match Uuid::parse_str(value) {Ok(id)=>Some(id),Err(_)=>return failure(StatusCode::BAD_REQUEST,"Invalid request","Bad Request")},None=>None};
            guarded_query(&db,&identity,"SELECT public.novu_rust_operator_jwt_fcm_partial_jobs($1,$2,$3,$4,$5,$6,$7)",&[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&window,&after]).await
        }
        Lookup::FcmUnreviewed => {
            let after=match params.get("after") {Some(value)=>match Uuid::parse_str(value) {Ok(id)=>Some(id),Err(_)=>return failure(StatusCode::BAD_REQUEST,"Invalid request","Bad Request")},None=>None};
            guarded_query(&db,&identity,"SELECT public.novu_rust_operator_jwt_fcm_unreviewed($1,$2,$3,$4,$5,$6)",&[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&after]).await
        }
        Lookup::FcmReviewRequest => {
            let Some(request)=params.get("requestId").and_then(|id|Uuid::parse_str(id).ok()) else {return failure(StatusCode::BAD_REQUEST,"Invalid request","Bad Request");};
            guarded_query(&db,&identity,"SELECT public.novu_rust_operator_jwt_fcm_review_request($1,$2,$3,$4,$5,$6)",&[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&request]).await
        }
        Lookup::ResendAutoPolicyRead => {
            guarded_query(
                &db,
                &identity,
                "SELECT public.novu_rust_operator_jwt_resend_auto_policy_read($1,$2,$3,$4,$5)",
                &[
                    &identity.issuer,
                    &identity.subject,
                    &identity.client_id,
                    &env,
                    &exp,
                ],
            )
            .await
        }
        Lookup::ResendAutoPolicyRequest => {
            let Some(request) = params
                .get("requestId")
                .and_then(|s| Uuid::parse_str(s).ok())
            else {
                return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
            };
            guarded_query(&db,&identity,"SELECT public.novu_rust_operator_jwt_resend_auto_policy_request($1,$2,$3,$4,$5,$6)",&[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&request]).await
        }
        Lookup::ResendAutoPolicyAudit => {
            let after = match params.get("after") {
                Some(value) => match Uuid::parse_str(value) {
                    Ok(v) => Some(v),
                    Err(_) => {
                        return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
                    }
                },
                None => None,
            };
            guarded_query(&db,&identity,"SELECT public.novu_rust_operator_jwt_resend_auto_policy_audit_read($1,$2,$3,$4,$5,$6)",&[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&after]).await
        }
        Lookup::ResendCooldownPolicyRead => {
            guarded_query(
                &db,
                &identity,
                "SELECT public.novu_rust_operator_jwt_resend_cooldown_policy_read($1,$2,$3,$4,$5)",
                &[
                    &identity.issuer,
                    &identity.subject,
                    &identity.client_id,
                    &env,
                    &exp,
                ],
            )
            .await
        }
        Lookup::ResendCooldownPolicyRequest => {
            let Some(request) = params
                .get("requestId")
                .and_then(|s| Uuid::parse_str(s).ok())
            else {
                return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
            };
            guarded_query(&db,&identity,"SELECT public.novu_rust_operator_jwt_resend_cooldown_policy_request($1,$2,$3,$4,$5,$6)",&[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&request]).await
        }
        Lookup::ResendCooldownPolicyAudit => {
            let after = match params.get("after") {
                Some(value) => match Uuid::parse_str(value) {
                    Ok(v) => Some(v),
                    Err(_) => {
                        return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
                    }
                },
                None => None,
            };
            guarded_query(&db,&identity,"SELECT public.novu_rust_operator_jwt_resend_cooldown_policy_audit_read($1,$2,$3,$4,$5,$6)",&[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&after]).await
        }
        Lookup::DiscordRetryPolicyRead | Lookup::DiscordRetryPolicyRequest | Lookup::DiscordRetryPolicyAudit => {
            let Some(dest)=params.get("destinationId").filter(|v|valid_discord_retry_destination(v)) else {return failure(StatusCode::BAD_REQUEST,"Invalid request","Bad Request");};
            match lookup {
                Lookup::DiscordRetryPolicyRead => guarded_query(&db,&identity,"SELECT public.novu_rust_operator_jwt_discord_retry_policy_read($1,$2,$3,$4,$5,$6)",&[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&dest]).await,
                Lookup::DiscordRetryPolicyRequest => {let Some(request)=params.get("requestId").and_then(|v|Uuid::parse_str(v).ok()) else {return failure(StatusCode::BAD_REQUEST,"Invalid request","Bad Request");};guarded_query(&db,&identity,"SELECT public.novu_rust_operator_jwt_discord_retry_policy_request($1,$2,$3,$4,$5,$6,$7)",&[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&dest,&request]).await},
                Lookup::DiscordRetryPolicyAudit => {let after=match params.get("after") {Some(v)=>match Uuid::parse_str(v){Ok(v)=>Some(v),Err(_)=>return failure(StatusCode::BAD_REQUEST,"Invalid request","Bad Request")},None=>None};guarded_query(&db,&identity,"SELECT public.novu_rust_operator_jwt_discord_retry_policy_audit_read($1,$2,$3,$4,$5,$6,$7)",&[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&dest,&after]).await},
                _=>unreachable!(),
            }
        }
        Lookup::DiscordPolicyRead => {
            guarded_query(
                &db,
                &identity,
                "SELECT public.novu_rust_operator_jwt_discord_policy_read($1,$2,$3,$4,$5)",
                &[
                    &identity.issuer,
                    &identity.subject,
                    &identity.client_id,
                    &env,
                    &exp,
                ],
            )
            .await
        }
        Lookup::DiscordPolicyRequest => {
            let Some(request) = params
                .get("requestId")
                .and_then(|s| Uuid::parse_str(s).ok())
            else {
                return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
            };
            guarded_query(&db,&identity,"SELECT public.novu_rust_operator_jwt_discord_policy_request($1,$2,$3,$4,$5,$6)",&[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&request]).await
        }
        Lookup::DiscordPolicyAudit => {
            let after = match params.get("after") {
                Some(value) => match Uuid::parse_str(value) {
                    Ok(v) => Some(v),
                    Err(_) => {
                        return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
                    }
                },
                None => None,
            };
            guarded_query(&db,&identity,"SELECT public.novu_rust_operator_jwt_discord_policy_audit_read($1,$2,$3,$4,$5,$6)",&[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&after]).await
        }
        Lookup::SubscriberActivityAudit => {
            let Some(kind) = params
                .get("kind")
                .filter(|s| ["create", "profile", "delete"].contains(&s.as_str()))
            else {
                return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
            };
            let after = match params.get("after") {
                Some(value) => match Uuid::parse_str(value) {
                    Ok(id) => Some(id),
                    Err(_) => {
                        return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
                    }
                },
                None => None,
            };
            guarded_query(&db, &identity, "SELECT public.novu_rust_operator_jwt_subscriber_activity_read($1,$2,$3,$4,$5,$6,$7)",&[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&kind,&after]).await
        }
        Lookup::SubscriberGrantAudit => {
            let after = match params.get("after") {
                Some(value) => match Uuid::parse_str(value) {
                    Ok(id) => Some(id),
                    Err(_) => {
                        return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
                    }
                },
                None => None,
            };
            guarded_query(&db, &identity, "SELECT public.novu_rust_operator_jwt_subscriber_grant_audit_read($1,$2,$3,$4,$5,$6)",&[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&after]).await
        }
        Lookup::SubscriberGrantRequest => {
            let Some(request) = params
                .get("requestId")
                .and_then(|s| Uuid::parse_str(s).ok())
            else {
                return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
            };
            guarded_query(
                &db,
                &identity,
                "SELECT public.novu_rust_operator_jwt_subscriber_grant_request($1,$2,$3,$4,$5,$6)",
                &[
                    &identity.issuer,
                    &identity.subject,
                    &identity.client_id,
                    &env,
                    &exp,
                    &request,
                ],
            )
            .await
        }
        Lookup::SubscriberGrantRead => {
            let (Some(issuer), Some(subject), Some(client), Some(permission)) = (
                params.get("targetIssuer"),
                params.get("targetSubject"),
                params.get("targetClientId"),
                params.get("permission"),
            ) else {
                return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
            };
            if [issuer, subject, client, permission]
                .iter()
                .any(|s| s.contains('\0'))
            {
                return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
            }
            guarded_query(&db, &identity, "SELECT public.novu_rust_operator_jwt_subscriber_grant_read($1,$2,$3,$4,$5,$6,$7,$8,$9)",&[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&issuer,&subject,&client,&permission]).await
        }
        Lookup::SubscriberDeleteRequest => {
            let Some(request) = params
                .get("requestId")
                .and_then(|s| Uuid::parse_str(s).ok())
            else {
                return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
            };
            guarded_query(&db, &identity, "SELECT public.novu_rust_operator_jwt_subscriber_delete_request_read($1,$2,$3,$4,$5,$6)",&[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&request]).await
        }
        Lookup::SubscriberCreateRequest => {
            let Some(request) = params
                .get("requestId")
                .and_then(|s| Uuid::parse_str(s).ok())
            else {
                return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
            };
            guarded_query(&db, &identity, "SELECT public.novu_rust_operator_jwt_subscriber_create_request_read($1,$2,$3,$4,$5,$6)",&[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&request]).await
        }
        Lookup::SubscriberProfileRequest => {
            let Some(request) = params
                .get("requestId")
                .and_then(|s| Uuid::parse_str(s).ok())
            else {
                return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
            };
            guarded_query(&db, &identity, "SELECT public.novu_rust_operator_jwt_subscriber_profile_request_read($1,$2,$3,$4,$5,$6)",&[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&request]).await
        }
        Lookup::SubscriberProfileRead => {
            let Some(subscriber) = params.get("subscriberId").filter(|s| !s.contains('\0')) else {
                return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
            };
            guarded_query(
                &db,
                &identity,
                "SELECT public.novu_rust_operator_jwt_subscriber_profile_read($1,$2,$3,$4,$5,$6)",
                &[
                    &identity.issuer,
                    &identity.subject,
                    &identity.client_id,
                    &env,
                    &exp,
                    &subscriber,
                ],
            )
            .await
        }
        Lookup::WorkflowGrantAudit => {
            let after = match params.get("after") {
                Some(value) => match Uuid::parse_str(value) {
                    Ok(id) => Some(id),
                    Err(_) => {
                        return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
                    }
                },
                None => None,
            };
            guarded_query(
                &db,
                &identity,
                "SELECT public.novu_rust_operator_jwt_workflow_grant_audit_read($1,$2,$3,$4,$5,$6)",
                &[
                    &identity.issuer,
                    &identity.subject,
                    &identity.client_id,
                    &env,
                    &exp,
                    &after,
                ],
            )
            .await
        }
        Lookup::Resend429Audit => {
            let after = match params.get("after") {
                Some(value) => match Uuid::parse_str(value) {
                    Ok(id) => Some(id),
                    Err(_) => return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request"),
                },
                None => None,
            };
            guarded_query(&db, &identity, "SELECT public.novu_rust_operator_jwt_resend_429_audit_read($1,$2,$3,$4,$5,$6)",
                &[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&after]).await
        }
        Lookup::Resend429GrantAudit => {
            let after = match params.get("after") {
                Some(value) => match Uuid::parse_str(value) {
                    Ok(id) => Some(id),
                    Err(_) => {
                        return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
                    }
                },
                None => None,
            };
            guarded_query(
                &db,
                &identity,
                "SELECT public.novu_rust_operator_jwt_resend_429_grant_audit_read($1,$2,$3,$4,$5,$6)",
                &[
                    &identity.issuer,
                    &identity.subject,
                    &identity.client_id,
                    &env,
                    &exp,
                    &after,
                ],
            )
            .await
        }
        Lookup::FcmReviewGrantAudit => {
            let after = match params.get("after") {
                Some(value) => match Uuid::parse_str(value) {
                    Ok(id) => Some(id),
                    Err(_) => {
                        return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
                    }
                },
                None => None,
            };
            guarded_query(
                &db,
                &identity,
                "SELECT public.novu_rust_operator_jwt_fcm_review_grant_audit_read($1,$2,$3,$4,$5,$6)",
                &[
                    &identity.issuer,
                    &identity.subject,
                    &identity.client_id,
                    &env,
                    &exp,
                    &after,
                ],
            )
            .await
        }
        Lookup::DiscordReviewGrantAudit => {
            let after = match params.get("after") {
                Some(value) => match Uuid::parse_str(value) {
                    Ok(id) => Some(id),
                    Err(_) => {
                        return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
                    }
                },
                None => None,
            };
            guarded_query(
                &db,
                &identity,
                "SELECT public.novu_rust_operator_jwt_discord_review_grant_audit_read($1,$2,$3,$4,$5,$6)",
                &[
                    &identity.issuer,
                    &identity.subject,
                    &identity.client_id,
                    &env,
                    &exp,
                    &after,
                ],
            )
            .await
        }
        Lookup::DiscordPolicyGrantAudit => {
            let after = match params.get("after") {
                Some(value) => match Uuid::parse_str(value) {
                    Ok(id) => Some(id),
                    Err(_) => {
                        return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
                    }
                },
                None => None,
            };
            guarded_query(
                &db,
                &identity,
                "SELECT public.novu_rust_operator_jwt_discord_policy_grant_audit_read($1,$2,$3,$4,$5,$6)",
                &[
                    &identity.issuer,
                    &identity.subject,
                    &identity.client_id,
                    &env,
                    &exp,
                    &after,
                ],
            )
            .await
        }
        Lookup::DiscordRetryGrantAudit => {
            let after = match params.get("after") {
                Some(value) => match Uuid::parse_str(value) {
                    Ok(id) => Some(id),
                    Err(_) => {
                        return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
                    }
                },
                None => None,
            };
            guarded_query(
                &db,
                &identity,
                "SELECT public.novu_rust_operator_jwt_discord_retry_grant_audit_read($1,$2,$3,$4,$5,$6)",
                &[
                    &identity.issuer,
                    &identity.subject,
                    &identity.client_id,
                    &env,
                    &exp,
                    &after,
                ],
            )
            .await
        }
        Lookup::SlackDecisionGrantAudit => {
            let after = match params.get("after") {
                Some(value) => match Uuid::parse_str(value) {
                    Ok(id) => Some(id),
                    Err(_) => {
                        return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
                    }
                },
                None => None,
            };
            guarded_query(
                &db,
                &identity,
                "SELECT public.novu_rust_operator_jwt_slack_decision_grant_audit_read($1,$2,$3,$4,$5,$6)",
                &[
                    &identity.issuer,
                    &identity.subject,
                    &identity.client_id,
                    &env,
                    &exp,
                    &after,
                ],
            )
            .await
        }
        Lookup::ChannelAccessGrantAudit => {
            let after = match params.get("after") {
                Some(value) => match Uuid::parse_str(value) {
                    Ok(id) => Some(id),
                    Err(_) => {
                        return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
                    }
                },
                None => None,
            };
            if let Some(resource) = params.get("resource") {
                if !["resend", "sendgrid", "twilio", "fcm", "slack"].contains(&resource.as_str()) {
                    return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
                }
                guarded_query(
                    &db, &identity,
                    "SELECT public.novu_rust_operator_jwt_channel_access_grant_audit_filtered($1,$2,$3,$4,$5,$6,$7)",
                    &[&identity.issuer, &identity.subject, &identity.client_id, &env, &exp, &resource, &after],
                ).await
            } else {
                guarded_query(
                &db,
                &identity,
                "SELECT public.novu_rust_operator_jwt_channel_access_grant_audit_read($1,$2,$3,$4,$5,$6)",
                &[
                    &identity.issuer,
                    &identity.subject,
                    &identity.client_id,
                    &env,
                    &exp,
                    &after,
                ],
            )
            .await
            }
        }
        Lookup::WorkflowGrantRead => {
            let (Some(issuer), Some(subject), Some(client), Some(resource), Some(permission)) = (
                params.get("targetIssuer"),
                params.get("targetSubject"),
                params.get("targetClientId"),
                params.get("resource"),
                params.get("permission"),
            ) else {
                return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
            };
            if [issuer, subject, client, resource, permission]
                .iter()
                .any(|v| v.contains('\0'))
            {
                return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
            }
            guarded_query(&db, &identity, "SELECT public.novu_rust_operator_jwt_workflow_grant_read($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",&[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&issuer,&subject,&client,&resource,&permission]).await
        }
        Lookup::ChannelAccessGrantRead => {
            let (Some(issuer), Some(subject), Some(client), Some(resource), Some(permission)) = (
                params.get("targetIssuer"),
                params.get("targetSubject"),
                params.get("targetClientId"),
                params.get("resource"),
                params.get("permission"),
            ) else {
                return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
            };
            if [issuer, subject, client, resource, permission]
                .iter()
                .any(|v| v.contains('\0'))
            {
                return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
            }
            guarded_query(&db, &identity, "SELECT public.novu_rust_operator_jwt_channel_access_grant_read($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",&[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&issuer,&subject,&client,&resource,&permission]).await
        }
        Lookup::Resend429GrantRead => {
            let (Some(issuer), Some(subject), Some(client), Some(resource), Some(permission)) = (
                params.get("targetIssuer"),
                params.get("targetSubject"),
                params.get("targetClientId"),
                params.get("resource"),
                params.get("permission"),
            ) else {
                return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
            };
            if [issuer, subject, client, resource, permission]
                .iter()
                .any(|v| v.contains('\0'))
            {
                return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
            }
            guarded_query(&db, &identity, "SELECT public.novu_rust_operator_jwt_resend_429_grant_read($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",&[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&issuer,&subject,&client,&resource,&permission]).await
        }
        Lookup::FcmReviewGrantRead => {
            let (Some(issuer), Some(subject), Some(client), Some(resource), Some(permission)) = (
                params.get("targetIssuer"),
                params.get("targetSubject"),
                params.get("targetClientId"),
                params.get("resource"),
                params.get("permission"),
            ) else {
                return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
            };
            if [issuer, subject, client, resource, permission]
                .iter()
                .any(|v| v.contains('\0'))
            {
                return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
            }
            guarded_query(&db, &identity, "SELECT public.novu_rust_operator_jwt_fcm_review_grant_read($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",&[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&issuer,&subject,&client,&resource,&permission]).await
        }
        Lookup::DiscordReviewGrantRead => {
            let (Some(issuer), Some(subject), Some(client), Some(resource), Some(permission)) = (
                params.get("targetIssuer"),
                params.get("targetSubject"),
                params.get("targetClientId"),
                params.get("resource"),
                params.get("permission"),
            ) else {
                return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
            };
            if [issuer, subject, client, resource, permission]
                .iter()
                .any(|v| v.contains('\0'))
            {
                return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
            }
            guarded_query(&db, &identity, "SELECT public.novu_rust_operator_jwt_discord_review_grant_read($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",&[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&issuer,&subject,&client,&resource,&permission]).await
        }
        Lookup::DiscordPolicyGrantRead => {
            let (Some(issuer), Some(subject), Some(client), Some(resource), Some(permission)) = (
                params.get("targetIssuer"),
                params.get("targetSubject"),
                params.get("targetClientId"),
                params.get("resource"),
                params.get("permission"),
            ) else {
                return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
            };
            if [issuer, subject, client, resource, permission]
                .iter()
                .any(|v| v.contains('\0'))
            {
                return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
            }
            guarded_query(&db, &identity, "SELECT public.novu_rust_operator_jwt_discord_policy_grant_read($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",&[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&issuer,&subject,&client,&resource,&permission]).await
        }
        Lookup::DiscordRetryGrantRead => {
            let (Some(issuer), Some(subject), Some(client), Some(resource), Some(permission)) = (
                params.get("targetIssuer"),
                params.get("targetSubject"),
                params.get("targetClientId"),
                params.get("resource"),
                params.get("permission"),
            ) else {
                return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
            };
            if [issuer, subject, client, resource, permission]
                .iter()
                .any(|v| v.contains('\0'))
            {
                return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
            }
            guarded_query(&db, &identity, "SELECT public.novu_rust_operator_jwt_discord_retry_grant_read($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",&[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&issuer,&subject,&client,&resource,&permission]).await
        }
        Lookup::SlackDecisionGrantRead => {
            let (Some(issuer), Some(subject), Some(client), Some(resource), Some(permission)) = (
                params.get("targetIssuer"),
                params.get("targetSubject"),
                params.get("targetClientId"),
                params.get("resource"),
                params.get("permission"),
            ) else {
                return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
            };
            if [issuer, subject, client, resource, permission]
                .iter()
                .any(|v| v.contains('\0'))
            {
                return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
            }
            guarded_query(&db, &identity, "SELECT public.novu_rust_operator_jwt_slack_decision_grant_read($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",&[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&issuer,&subject,&client,&resource,&permission]).await
        }
        Lookup::WorkflowGrantRequest => {
            let Some(request) = params
                .get("requestId")
                .and_then(|v| Uuid::parse_str(v).ok())
            else {
                return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
            };
            guarded_query(
                &db,
                &identity,
                "SELECT public.novu_rust_operator_jwt_workflow_grant_request($1,$2,$3,$4,$5,$6)",
                &[
                    &identity.issuer,
                    &identity.subject,
                    &identity.client_id,
                    &env,
                    &exp,
                    &request,
                ],
            )
            .await
        }
        Lookup::ChannelAccessGrantRequest => {
            let Some(request) = params
                .get("requestId")
                .and_then(|v| Uuid::parse_str(v).ok())
            else {
                return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
            };
            guarded_query(
                &db,
                &identity,
                "SELECT public.novu_rust_operator_jwt_channel_access_grant_request($1,$2,$3,$4,$5,$6)",
                &[
                    &identity.issuer,
                    &identity.subject,
                    &identity.client_id,
                    &env,
                    &exp,
                    &request,
                ],
            )
            .await
        }
        Lookup::Resend429GrantRequest => {
            let Some(request) = params
                .get("requestId")
                .and_then(|v| Uuid::parse_str(v).ok())
            else {
                return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
            };
            guarded_query(
                &db,
                &identity,
                "SELECT public.novu_rust_operator_jwt_resend_429_grant_request($1,$2,$3,$4,$5,$6)",
                &[
                    &identity.issuer,
                    &identity.subject,
                    &identity.client_id,
                    &env,
                    &exp,
                    &request,
                ],
            )
            .await
        }
        Lookup::FcmReviewGrantRequest => {
            let Some(request) = params
                .get("requestId")
                .and_then(|v| Uuid::parse_str(v).ok())
            else {
                return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
            };
            guarded_query(
                &db,
                &identity,
                "SELECT public.novu_rust_operator_jwt_fcm_review_grant_request($1,$2,$3,$4,$5,$6)",
                &[
                    &identity.issuer,
                    &identity.subject,
                    &identity.client_id,
                    &env,
                    &exp,
                    &request,
                ],
            )
            .await
        }
        Lookup::DiscordReviewGrantRequest => {
            let Some(request) = params
                .get("requestId")
                .and_then(|v| Uuid::parse_str(v).ok())
            else {
                return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
            };
            guarded_query(
                &db,
                &identity,
                "SELECT public.novu_rust_operator_jwt_discord_review_grant_request($1,$2,$3,$4,$5,$6)",
                &[
                    &identity.issuer,
                    &identity.subject,
                    &identity.client_id,
                    &env,
                    &exp,
                    &request,
                ],
            )
            .await
        }
        Lookup::DiscordPolicyGrantRequest => {
            let Some(request) = params
                .get("requestId")
                .and_then(|v| Uuid::parse_str(v).ok())
            else {
                return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
            };
            guarded_query(
                &db,
                &identity,
                "SELECT public.novu_rust_operator_jwt_discord_policy_grant_request($1,$2,$3,$4,$5,$6)",
                &[
                    &identity.issuer,
                    &identity.subject,
                    &identity.client_id,
                    &env,
                    &exp,
                    &request,
                ],
            )
            .await
        }
        Lookup::DiscordRetryGrantRequest => {
            let Some(request) = params
                .get("requestId")
                .and_then(|v| Uuid::parse_str(v).ok())
            else {
                return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
            };
            guarded_query(
                &db,
                &identity,
                "SELECT public.novu_rust_operator_jwt_discord_retry_grant_request($1,$2,$3,$4,$5,$6)",
                &[
                    &identity.issuer,
                    &identity.subject,
                    &identity.client_id,
                    &env,
                    &exp,
                    &request,
                ],
            )
            .await
        }
        Lookup::SlackDecisionGrantRequest => {
            let Some(request) = params
                .get("requestId")
                .and_then(|v| Uuid::parse_str(v).ok())
            else {
                return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
            };
            guarded_query(
                &db,
                &identity,
                "SELECT public.novu_rust_operator_jwt_slack_decision_grant_request($1,$2,$3,$4,$5,$6)",
                &[
                    &identity.issuer,
                    &identity.subject,
                    &identity.client_id,
                    &env,
                    &exp,
                    &request,
                ],
            )
            .await
        }
        Lookup::WorkflowRequest => {
            let Some(resource) = params.get("resource").filter(|r| {
                ["email", "sms", "push", "slack", "email-provider"].contains(&r.as_str())
            }) else {
                return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
            };
            let Some(request) = params
                .get("requestId")
                .and_then(|id| Uuid::parse_str(id).ok())
            else {
                return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
            };
            guarded_query(
                &db,
                &identity,
                "SELECT public.novu_rust_operator_jwt_workflow_request_read($1,$2,$3,$4,$5,$6,$7)",
                &[
                    &identity.issuer,
                    &identity.subject,
                    &identity.client_id,
                    &env,
                    &exp,
                    &resource,
                    &request,
                ],
            )
            .await
        }
        Lookup::WorkflowAudit => {
            let (Some(workflow), Some(resource)) = (params.get("workflow"), params.get("resource"))
            else {
                return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
            };
            if workflow.is_empty()
                || workflow.len() > 512
                || workflow.contains('\0')
                || !["email", "sms", "push", "slack", "email-provider"].contains(&resource.as_str())
            {
                return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
            }
            let after = match params.get("after") {
                None => None,
                Some(value) => match Uuid::parse_str(value) {
                    Ok(v) => Some(v),
                    Err(_) => {
                        return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
                    }
                },
            };
            guarded_query(
                &db,
                &identity,
                "SELECT public.novu_rust_operator_jwt_workflow_audit_read($1,$2,$3,$4,$5,$6,$7,$8)",
                &[
                    &identity.issuer,
                    &identity.subject,
                    &identity.client_id,
                    &env,
                    &exp,
                    &workflow,
                    &resource,
                    &after,
                ],
            )
            .await
        }
        Lookup::WorkflowRead => {
            let (Some(workflow), Some(resource)) = (params.get("workflow"), params.get("resource"))
            else {
                return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
            };
            if workflow.is_empty()
                || workflow.len() > 512
                || workflow.contains('\0')
                || !["email", "sms", "push", "slack", "email-provider"].contains(&resource.as_str())
            {
                return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
            }
            guarded_query(
                &db,
                &identity,
                "SELECT public.novu_rust_operator_jwt_workflow_read($1,$2,$3,$4,$5,$6,$7)",
                &[
                    &identity.issuer,
                    &identity.subject,
                    &identity.client_id,
                    &env,
                    &exp,
                    &workflow,
                    &resource,
                ],
            )
            .await
        }
        Lookup::ResendJobs
        | Lookup::TwilioJobs
        | Lookup::FcmJobs
        | Lookup::SlackJobs
        | Lookup::SendgridJobs => {
            let after = match params.get("after") {
                None => None,
                Some(value) => match Uuid::parse_str(value) {
                    Ok(id) => Some(id),
                    Err(_) => {
                        return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
                    }
                },
            };
            if matches!(lookup, Lookup::ResendJobs) && params.contains_key("status") {
                let filter = params.get("status").unwrap();
                if !["unknown", "rejected", "rate-limited"].contains(&filter.as_str()) {
                    return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
                }
                guarded_query(&db, &identity,
                    "SELECT public.novu_rust_operator_jwt_resend_jobs_filtered($1,$2,$3,$4,$5,$6,$7)",
                    &[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&after,&filter]).await
            } else {
                guarded_query(
                    &db,
                    &identity,
                    if matches!(lookup, Lookup::SendgridJobs) {
                        "SELECT public.novu_rust_operator_jwt_sendgrid_jobs($1,$2,$3,$4,$5,$6)"
                    } else if matches!(lookup, Lookup::SlackJobs) {
                        "SELECT public.novu_rust_operator_jwt_slack_jobs($1,$2,$3,$4,$5,$6)"
                    } else if matches!(lookup, Lookup::FcmJobs) {
                        "SELECT public.novu_rust_operator_jwt_fcm_jobs($1,$2,$3,$4,$5,$6)"
                    } else if matches!(lookup, Lookup::TwilioJobs) {
                        "SELECT public.novu_rust_operator_jwt_twilio_jobs($1,$2,$3,$4,$5,$6)"
                    } else {
                        "SELECT public.novu_rust_operator_jwt_resend_jobs($1,$2,$3,$4,$5,$6)"
                    },
                    &[
                        &identity.issuer,
                        &identity.subject,
                        &identity.client_id,
                        &env,
                        &exp,
                        &after,
                    ],
                )
                .await
            }
        }
        Lookup::ResendResult
        | Lookup::TwilioResult
        | Lookup::FcmResult
        | Lookup::SlackResult
        | Lookup::SendgridResult => {
            let Some(job) = params.get("jobId").and_then(|id| Uuid::parse_str(id).ok()) else {
                return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
            };
            guarded_query(
                &db,
                &identity,
                if matches!(lookup, Lookup::SendgridResult) {
                    "SELECT public.novu_rust_operator_jwt_sendgrid_result($1,$2,$3,$4,$5,$6)"
                } else if matches!(lookup, Lookup::SlackResult) {
                    "SELECT public.novu_rust_operator_jwt_slack_result($1,$2,$3,$4,$5,$6)"
                } else if matches!(lookup, Lookup::FcmResult) {
                    "SELECT public.novu_rust_operator_jwt_fcm_result($1,$2,$3,$4,$5,$6)"
                } else if matches!(lookup, Lookup::TwilioResult) {
                    "SELECT public.novu_rust_operator_jwt_twilio_result($1,$2,$3,$4,$5,$6)"
                } else {
                    "SELECT public.novu_rust_operator_jwt_resend_result($1,$2,$3,$4,$5,$6)"
                },
                &[
                    &identity.issuer,
                    &identity.subject,
                    &identity.client_id,
                    &env,
                    &exp,
                    &job,
                ],
            )
            .await
        }
        Lookup::PermissionChangeRequest => {
            let Some(request) = params
                .get("requestId")
                .and_then(|id| Uuid::parse_str(id).ok())
            else {
                return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
            };
            guarded_query(
                &db,
                &identity,
                "SELECT public.novu_rust_operator_jwt_permission_change_request($1,$2,$3,$4,$5,$6)",
                &[
                    &identity.issuer,
                    &identity.subject,
                    &identity.client_id,
                    &env,
                    &exp,
                    &request,
                ],
            )
            .await
        }
        Lookup::PermissionIdentity => {
            let (Some(issuer), Some(subject), Some(client)) = (
                params.get("issuer"),
                params.get("subject"),
                params.get("clientId"),
            ) else {
                return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
            };
            guarded_query(
                &db,
                &identity,
                "SELECT public.novu_rust_operator_jwt_review_identity($1,$2,$3,$4,$5,$6,$7,$8)",
                &[
                    &identity.issuer,
                    &identity.subject,
                    &identity.client_id,
                    &env,
                    &exp,
                    issuer,
                    subject,
                    client,
                ],
            )
            .await
        }
        Lookup::PermissionAudit => {
            let before = match params.get("before") {
                None => None,
                Some(value) => match value.parse::<i64>() {
                    Ok(value) if value > 0 => Some(value),
                    _ => return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request"),
                },
            };
            guarded_query(
                &db,
                &identity,
                "SELECT public.novu_rust_operator_jwt_review_audit($1,$2,$3,$4,$5,$6)",
                &[
                    &identity.issuer,
                    &identity.subject,
                    &identity.client_id,
                    &env,
                    &exp,
                    &before,
                ],
            )
            .await
        }
        Lookup::Subscribers => {
            let name = params.get("name").filter(|value| !value.is_empty());
            let after = params.get("after");
            let before = params.get("before");
            guarded_query(
                &db,
                &identity,
                "SELECT public.novu_rust_operator_jwt_subscribers($1,$2,$3,$4,$5,$6,$7,$8)",
                &[
                    &identity.issuer,
                    &identity.subject,
                    &identity.client_id,
                    &env,
                    &exp,
                    &name,
                    &after,
                    &before,
                ],
            )
            .await
        }
        Lookup::Alerts => {
            let channel = params.get("afterChannel");
            let target = params.get("afterTarget");
            guarded_query(
                &db,
                &identity,
                "SELECT public.novu_rust_operator_jwt_alerts($1,$2,$3,$4,$5,$6,$7)",
                &[
                    &identity.issuer,
                    &identity.subject,
                    &identity.client_id,
                    &env,
                    &exp,
                    &channel,
                    &target,
                ],
            )
            .await
        }
        Lookup::Resend429Request => {
            let Some(request) = params.get("requestId").and_then(|id| Uuid::parse_str(id).ok()) else {
                return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
            };
            guarded_query(&db, &identity, "SELECT public.novu_rust_operator_jwt_resend_429_request($1,$2,$3,$4,$5,$6)",
                &[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&request]).await
        }
        Lookup::SlackDecisionRequest => {
            let Some(request) = params
                .get("requestId")
                .and_then(|id| Uuid::parse_str(id).ok())
            else {
                return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
            };
            guarded_query(
                &db,
                &identity,
                "SELECT public.novu_rust_operator_jwt_slack_decision_request($1,$2,$3,$4,$5,$6)",
                &[
                    &identity.issuer,
                    &identity.subject,
                    &identity.client_id,
                    &env,
                    &exp,
                    &request,
                ],
            )
            .await
        }
        Lookup::TwilioRetryRequest => {
            let Some(request) = params
                .get("requestId")
                .and_then(|id| Uuid::parse_str(id).ok())
            else {
                return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
            };
            guarded_query(
                &db,
                &identity,
                "SELECT public.novu_rust_operator_jwt_twilio_retry_request($1,$2,$3,$4,$5,$6)",
                &[
                    &identity.issuer,
                    &identity.subject,
                    &identity.client_id,
                    &env,
                    &exp,
                    &request,
                ],
            )
            .await
        }
        Lookup::DecisionRequest => {
            let Some(request) = params
                .get("requestId")
                .and_then(|id| Uuid::parse_str(id).ok())
            else {
                return self::failure(StatusCode::BAD_REQUEST, uri.path());
            };
            guarded_query(
                &db,
                &identity,
                "SELECT public.novu_rust_operator_jwt_decision_request($1,$2,$3,$4,$5,$6)",
                &[
                    &identity.issuer,
                    &identity.subject,
                    &identity.client_id,
                    &env,
                    &exp,
                    &request,
                ],
            )
            .await
        }
        Lookup::Identity => {
            guarded_query(
                &db,
                &identity,
                "SELECT public.novu_rust_operator_jwt_self($1,$2,$3,$4,$5)",
                &[
                    &identity.issuer,
                    &identity.subject,
                    &identity.client_id,
                    &env,
                    &exp,
                ],
            )
            .await
        }
    };
    match result {
        Ok(row) => (
            [(header::CACHE_CONTROL, "no-store")],
            Json(row.get::<_, serde_json::Value>(0)),
        )
            .into_response(),
        Err(e) if e.code().is_some_and(|c| c.code() == "42501") => {
            failure(StatusCode::UNAUTHORIZED, "Unauthorized", "Unauthorized")
        }
        Err(e) if e.code().is_some_and(|c| c.code() == "22023") => {
            failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request")
        }
        Err(_) => failure(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal server error",
            "Internal Server Error",
        ),
    }
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Resend429Body {
    request_id: String,
    job_id: String,
    attempt_id: String,
    mode: String,
    reason: String,
}

pub(super) async fn resend_429_resolve(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
    body: Bytes,
) -> Response {
    let identity = match authenticate(&db, &headers, verifier).await {
        Ok(identity) => identity,
        Err(status) => return failure(status, uri.path()),
    };
    let params: Vec<_> = form_urlencoded::parse(uri.query().unwrap_or("").as_bytes()).collect();
    if params.len() != 1
        || params[0].0 != "environmentId"
        || params[0].1.is_empty()
        || params[0].1.len() > 512
        || params[0].1.contains('\0')
    {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    }
    let Some(input) = (body.len() <= 4096)
        .then(|| serde_json::from_slice::<Resend429Body>(&body).ok())
        .flatten()
    else {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    };
    if input.reason.contains('\0') || input.mode.contains('\0') {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    }
    let (Ok(request), Ok(job), Ok(attempt), Ok(exp)) = (
        Uuid::parse_str(&input.request_id),
        Uuid::parse_str(&input.job_id),
        Uuid::parse_str(&input.attempt_id),
        i64::try_from(identity.expires_at),
    ) else {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    };
    let env = params[0].1.as_ref();
    match guarded_query(
        &db,
        &identity,
        "SELECT public.novu_rust_operator_jwt_resend_429_resolve($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",
        &[
            &identity.issuer,
            &identity.subject,
            &identity.client_id,
            &env,
            &exp,
            &request,
            &job,
            &attempt,
            &input.mode,
            &input.reason,
        ],
    )
    .await
    {
        Ok(row) => (
            [(header::CACHE_CONTROL, "no-store")],
            Json(row.get::<_,serde_json::Value>(0)),
        )
            .into_response(),
        Err(e) if e.code().is_some_and(|c| c.code() == "42501") => {
            failure(StatusCode::UNAUTHORIZED, uri.path())
        }
        Err(e) if e.code().is_some_and(|c| c.code() == "22023") => {
            failure(StatusCode::BAD_REQUEST, uri.path())
        }
        Err(e) if e.code().is_some_and(|c| c.code() == "P4090") => {
            let mut response = error_at(
                StatusCode::CONFLICT,
                "Request ID conflict",
                "Conflict",
                uri.path(),
            )
            .into_response();
            response.headers_mut().insert(
                header::CACHE_CONTROL,
                header::HeaderValue::from_static("no-store"),
            );
            response
        }
        Err(_) => failure(StatusCode::INTERNAL_SERVER_ERROR, uri.path()),
    }
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SlackDecisionBody {
    request_id: String,
    job_id: String,
    attempt_id: String,
    decision: String,
    reason: String,
}

pub(super) async fn slack_retry_after(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
    body: Bytes,
) -> Response {
    let identity = match authenticate(&db, &headers, verifier).await {
        Ok(identity) => identity,
        Err(status) => return failure(status, uri.path()),
    };
    let params: Vec<_> = form_urlencoded::parse(uri.query().unwrap_or("").as_bytes()).collect();
    if params.len() != 1
        || params[0].0 != "environmentId"
        || params[0].1.is_empty()
        || params[0].1.len() > 512
        || params[0].1.contains('\0')
    {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    }
    let Some(input) = (body.len() <= 4096)
        .then(|| serde_json::from_slice::<SlackDecisionBody>(&body).ok())
        .flatten()
    else {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    };
    if input.reason.contains('\0') || input.decision.contains('\0') {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    }
    let (Ok(request), Ok(job), Ok(attempt), Ok(exp)) = (
        Uuid::parse_str(&input.request_id),
        Uuid::parse_str(&input.job_id),
        Uuid::parse_str(&input.attempt_id),
        i64::try_from(identity.expires_at),
    ) else {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    };
    let env = params[0].1.as_ref();
    match guarded_query(
        &db,
        &identity,
        "SELECT public.novu_rust_operator_jwt_slack_retry_after($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",
        &[
            &identity.issuer,
            &identity.subject,
            &identity.client_id,
            &env,
            &exp,
            &request,
            &job,
            &attempt,
            &input.decision,
            &input.reason,
        ],
    )
    .await
    {
        Ok(row) => (
            [(header::CACHE_CONTROL, "no-store")],
            Json(row.get::<_,serde_json::Value>(0)),
        )
            .into_response(),
        Err(e) if e.code().is_some_and(|c| c.code() == "42501") => {
            failure(StatusCode::UNAUTHORIZED, uri.path())
        }
        Err(e) if e.code().is_some_and(|c| c.code() == "22023") => {
            failure(StatusCode::BAD_REQUEST, uri.path())
        }
        Err(e) if e.code().is_some_and(|c| c.code() == "P4090") => {
            let mut response = error_at(
                StatusCode::CONFLICT,
                "Request ID conflict",
                "Conflict",
                uri.path(),
            )
            .into_response();
            response.headers_mut().insert(
                header::CACHE_CONTROL,
                header::HeaderValue::from_static("no-store"),
            );
            response
        }
        Err(_) => failure(StatusCode::INTERNAL_SERVER_ERROR, uri.path()),
    }
}

#[derive(serde::Deserialize)]
#[serde(rename_all="camelCase",deny_unknown_fields)]
struct FcmReviewBody { request_id:String,job_id:String,revision:String,reason:String }
pub(super) async fn fcm_review(
    State(db):State<Arc<Client>>,headers:HeaderMap,OriginalUri(uri):OriginalUri,
    verifier:Option<Extension<Arc<AccessTokenVerifier>>>,body:Bytes,
) -> Response {
    let identity=match authenticate(&db,&headers,verifier).await {Ok(v)=>v,Err(status)=>return failure(status,uri.path())};
    let params:Vec<_>=form_urlencoded::parse(uri.query().unwrap_or("").as_bytes()).collect();
    if params.len()!=1 || params[0].0!="environmentId" || params[0].1.is_empty() || params[0].1.len()>512 || params[0].1.contains('\0') {return failure(StatusCode::BAD_REQUEST,uri.path());}
    let Some(input)=(body.len()<=4096).then(||serde_json::from_slice::<FcmReviewBody>(&body).ok()).flatten() else {return failure(StatusCode::BAD_REQUEST,uri.path());};
    if input.reason.contains('\0') || input.revision.contains('\0') {return failure(StatusCode::BAD_REQUEST,uri.path());}
    let (Ok(request),Ok(job),Ok(exp))=(Uuid::parse_str(&input.request_id),Uuid::parse_str(&input.job_id),i64::try_from(identity.expires_at)) else {return failure(StatusCode::BAD_REQUEST,uri.path());};
    let env=params[0].1.as_ref();
    match guarded_query(&db,&identity,"SELECT public.novu_rust_operator_jwt_fcm_review($1,$2,$3,$4,$5,$6,$7,$8,$9)",&[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&request,&job,&input.revision,&input.reason]).await {
        Ok(row)=>([(header::CACHE_CONTROL,"no-store")],Json(row.get::<_,serde_json::Value>(0))).into_response(),
        Err(e) if e.code().is_some_and(|c|c.code()=="42501")=>failure(StatusCode::UNAUTHORIZED,uri.path()),
        Err(e) if e.code().is_some_and(|c|c.code()=="22023")=>failure(StatusCode::BAD_REQUEST,uri.path()),
        Err(e) if e.code().is_some_and(|c|matches!(c.code(),"P4090"|"40001"))=>{let mut response=error_at(StatusCode::CONFLICT,"Review conflict","Conflict",uri.path()).into_response();response.headers_mut().insert(header::CACHE_CONTROL,header::HeaderValue::from_static("no-store"));response},
        Err(_)=>failure(StatusCode::INTERNAL_SERVER_ERROR,uri.path()),
    }
}

pub(super) async fn twilio_retry_after(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
    body: Bytes,
) -> Response {
    let identity = match authenticate(&db, &headers, verifier).await {
        Ok(identity) => identity,
        Err(status) => return failure(status, uri.path()),
    };
    let params: Vec<_> = form_urlencoded::parse(uri.query().unwrap_or("").as_bytes()).collect();
    if params.len() != 1
        || params[0].0 != "environmentId"
        || params[0].1.is_empty()
        || params[0].1.len() > 512
        || params[0].1.contains('\0')
    {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    }
    let Some(input) = (body.len() <= 4096)
        .then(|| serde_json::from_slice::<SlackDecisionBody>(&body).ok())
        .flatten()
    else {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    };
    if input.reason.contains('\0') || input.decision.contains('\0') {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    }
    let (Ok(request), Ok(job), Ok(attempt), Ok(exp)) = (
        Uuid::parse_str(&input.request_id),
        Uuid::parse_str(&input.job_id),
        Uuid::parse_str(&input.attempt_id),
        i64::try_from(identity.expires_at),
    ) else {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    };
    let env = params[0].1.as_ref();
    match guarded_query(
        &db,
        &identity,
        "SELECT public.novu_rust_operator_jwt_twilio_retry_after($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",
        &[
            &identity.issuer,
            &identity.subject,
            &identity.client_id,
            &env,
            &exp,
            &request,
            &job,
            &attempt,
            &input.decision,
            &input.reason,
        ],
    )
    .await
    {
        Ok(row) => (
            [(header::CACHE_CONTROL, "no-store")],
            Json(row.get::<_,serde_json::Value>(0)),
        )
            .into_response(),
        Err(e) if e.code().is_some_and(|c| c.code() == "42501") => {
            failure(StatusCode::UNAUTHORIZED, uri.path())
        }
        Err(e) if e.code().is_some_and(|c| c.code() == "22023") => {
            failure(StatusCode::BAD_REQUEST, uri.path())
        }
        Err(e) if e.code().is_some_and(|c| c.code() == "P4090") => {
            let mut response = error_at(
                StatusCode::CONFLICT,
                "Request ID conflict",
                "Conflict",
                uri.path(),
            )
            .into_response();
            response.headers_mut().insert(
                header::CACHE_CONTROL,
                header::HeaderValue::from_static("no-store"),
            );
            response
        }
        Err(_) => failure(StatusCode::INTERNAL_SERVER_ERROR, uri.path()),
    }
}

pub(super) async fn slack_resolve(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
    body: Bytes,
) -> Response {
    let identity = match authenticate(&db, &headers, verifier).await {
        Ok(identity) => identity,
        Err(status) => return failure(status, uri.path()),
    };
    let params: Vec<_> = form_urlencoded::parse(uri.query().unwrap_or("").as_bytes()).collect();
    if params.len() != 1
        || params[0].0 != "environmentId"
        || params[0].1.is_empty()
        || params[0].1.len() > 512
        || params[0].1.contains('\0')
    {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    }
    let Some(input) = (body.len() <= 4096)
        .then(|| serde_json::from_slice::<SlackDecisionBody>(&body).ok())
        .flatten()
    else {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    };
    if input.reason.contains('\0') || input.decision.contains('\0') {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    }
    let (Ok(request), Ok(job), Ok(attempt), Ok(exp)) = (
        Uuid::parse_str(&input.request_id),
        Uuid::parse_str(&input.job_id),
        Uuid::parse_str(&input.attempt_id),
        i64::try_from(identity.expires_at),
    ) else {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    };
    let env = params[0].1.as_ref();
    match guarded_query(
        &db,
        &identity,
        "SELECT public.novu_rust_operator_jwt_slack_resolve($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",
        &[
            &identity.issuer,
            &identity.subject,
            &identity.client_id,
            &env,
            &exp,
            &request,
            &job,
            &attempt,
            &input.decision,
            &input.reason,
        ],
    )
    .await
    {
        Ok(row) => (
            [(header::CACHE_CONTROL, "no-store")],
            Json(serde_json::json!({"recorded":true,"replayed":row.get::<_,bool>(0)})),
        )
            .into_response(),
        Err(e) if e.code().is_some_and(|c| c.code() == "42501") => {
            failure(StatusCode::UNAUTHORIZED, uri.path())
        }
        Err(e) if e.code().is_some_and(|c| c.code() == "22023") => {
            failure(StatusCode::BAD_REQUEST, uri.path())
        }
        Err(e) if e.code().is_some_and(|c| c.code() == "P4090") => {
            let mut response = error_at(
                StatusCode::CONFLICT,
                "Request ID conflict",
                "Conflict",
                uri.path(),
            )
            .into_response();
            response.headers_mut().insert(
                header::CACHE_CONTROL,
                header::HeaderValue::from_static("no-store"),
            );
            response
        }
        Err(_) => failure(StatusCode::INTERNAL_SERVER_ERROR, uri.path()),
    }
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DecisionBody {
    request_id: String,
    channel: String,
    target_id: String,
    attempt_id: String,
    decision: String,
    reason: String,
}

pub(super) async fn resolve(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
    body: Bytes,
) -> Response {
    let identity = match authenticate(&db, &headers, verifier).await {
        Ok(identity) => identity,
        Err(status) => return failure(status, uri.path()),
    };
    let params: Vec<_> = form_urlencoded::parse(uri.query().unwrap_or("").as_bytes()).collect();
    if params.len() != 1
        || params[0].0 != "environmentId"
        || params[0].1.is_empty()
        || params[0].1.len() > 512
    {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    }
    let Some(input) = (body.len() <= 4096)
        .then(|| serde_json::from_slice::<DecisionBody>(&body).ok())
        .flatten()
    else {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    };
    let (Ok(request), Ok(attempt), Ok(exp)) = (
        Uuid::parse_str(&input.request_id),
        Uuid::parse_str(&input.attempt_id),
        i64::try_from(identity.expires_at),
    ) else {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    };
    let env = params[0].1.as_ref();
    match guarded_query(
        &db,
        &identity,
        "SELECT public.novu_rust_operator_jwt_resolve_alert($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",
        &[
            &identity.issuer,
            &identity.subject,
            &identity.client_id,
            &env,
            &exp,
            &request,
            &input.channel,
            &input.target_id,
            &attempt,
            &input.decision,
            &input.reason,
        ],
    )
    .await
    {
        Ok(row) => (
            [(header::CACHE_CONTROL, "no-store")],
            Json(serde_json::json!({"recorded":true,"replayed":row.get::<_,bool>(0)})),
        )
            .into_response(),
        Err(e) if e.code().is_some_and(|c| c.code() == "42501") => {
            failure(StatusCode::UNAUTHORIZED, uri.path())
        }
        Err(e) if e.code().is_some_and(|c| c.code() == "22023") => {
            failure(StatusCode::BAD_REQUEST, uri.path())
        }
        Err(_) => failure(StatusCode::INTERNAL_SERVER_ERROR, uri.path()),
    }
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PermissionChangeBody {
    request_id: String,
    target_issuer: String,
    target_subject: String,
    target_client_id: String,
    expected: serde_json::Value,
    desired: serde_json::Value,
    reason: String,
}

pub(super) async fn change_permissions(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
    body: Bytes,
) -> Response {
    let identity = match authenticate(&db, &headers, verifier).await {
        Ok(identity) => identity,
        Err(status) => return failure(status, uri.path()),
    };
    let params: Vec<_> = form_urlencoded::parse(uri.query().unwrap_or("").as_bytes()).collect();
    if params.len() != 1
        || params[0].0 != "environmentId"
        || params[0].1.is_empty()
        || params[0].1.len() > 512
    {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    }
    let Some(input) = (body.len() <= 8192)
        .then(|| serde_json::from_slice::<PermissionChangeBody>(&body).ok())
        .flatten()
    else {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    };
    let (Ok(request), Ok(exp)) = (
        Uuid::parse_str(&input.request_id),
        i64::try_from(identity.expires_at),
    ) else {
        return failure(StatusCode::BAD_REQUEST, uri.path());
    };
    let env = params[0].1.as_ref();
    match guarded_query(&db, &identity, "SELECT public.novu_rust_operator_jwt_change_permissions($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)",&[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&request,&input.target_issuer,&input.target_subject,&input.target_client_id,&input.expected,&input.desired,&input.reason]).await {
        Ok(row)=>([(header::CACHE_CONTROL,"no-store")],Json(row.get::<_,serde_json::Value>(0))).into_response(),
        Err(e) if e.code().is_some_and(|c|c.code()=="42501")=>failure(StatusCode::UNAUTHORIZED,uri.path()),
        Err(e) if e.code().is_some_and(|c|c.code()=="22023")=>failure(StatusCode::BAD_REQUEST,uri.path()),
        Err(_)=>failure(StatusCode::INTERNAL_SERVER_ERROR,uri.path()),
    }
}

#[derive(Clone,Copy)]
enum DiscordQuery { Attention, Request }
pub(super) async fn discord_attention(State(db):State<Arc<Client>>,headers:HeaderMap,OriginalUri(uri):OriginalUri,verifier:Option<Extension<Arc<AccessTokenVerifier>>>) -> Response {
    discord_query(db,headers,uri,verifier,DiscordQuery::Attention).await
}
pub(super) async fn discord_review_request(State(db):State<Arc<Client>>,headers:HeaderMap,OriginalUri(uri):OriginalUri,verifier:Option<Extension<Arc<AccessTokenVerifier>>>) -> Response {
    discord_query(db,headers,uri,verifier,DiscordQuery::Request).await
}
async fn discord_query(db:Arc<Client>,headers:HeaderMap,uri:Uri,verifier:Option<Extension<Arc<AccessTokenVerifier>>>,kind:DiscordQuery) -> Response {
    let identity=match authenticate(&db,&headers,verifier).await {Ok(v)=>v,Err(status)=>return failure(status,uri.path())};
    let mut env=None;let mut cursor=None;
    for (key,value) in form_urlencoded::parse(uri.query().unwrap_or("").as_bytes()) {
        match key.as_ref() {
            "environmentId" if env.is_none() && !value.is_empty() && value.len()<=512 && !value.contains('\0') => env=Some(value.into_owned()),
            "after" if matches!(kind,DiscordQuery::Attention) && cursor.is_none() => match Uuid::parse_str(&value) {Ok(v)=>cursor=Some(v),Err(_)=>return failure(StatusCode::BAD_REQUEST,uri.path())},
            "requestId" if matches!(kind,DiscordQuery::Request) && cursor.is_none() => match Uuid::parse_str(&value) {Ok(v)=>cursor=Some(v),Err(_)=>return failure(StatusCode::BAD_REQUEST,uri.path())},
            _=>return failure(StatusCode::BAD_REQUEST,uri.path()),
        }
    }
    let Some(env)=env else{return failure(StatusCode::BAD_REQUEST,uri.path())};
    if matches!(kind,DiscordQuery::Request) && cursor.is_none() {return failure(StatusCode::BAD_REQUEST,uri.path());}
    let Ok(exp)=i64::try_from(identity.expires_at) else{return failure(StatusCode::BAD_REQUEST,uri.path())};
    let sql=match kind {DiscordQuery::Attention=>"SELECT public.novu_rust_operator_jwt_discord_attention($1,$2,$3,$4,$5,$6)",DiscordQuery::Request=>"SELECT public.novu_rust_operator_jwt_discord_review_request($1,$2,$3,$4,$5,$6)"};
    match guarded_query(&db,&identity,sql,&[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&cursor]).await {
        Ok(row)=>([(header::CACHE_CONTROL,"no-store")],Json(row.get::<_,serde_json::Value>(0))).into_response(),
        Err(e) if e.code().is_some_and(|c|c.code()=="42501")=>failure(StatusCode::UNAUTHORIZED,uri.path()),
        Err(e) if e.code().is_some_and(|c|c.code()=="22023")=>failure(StatusCode::BAD_REQUEST,uri.path()),
        Err(_)=>failure(StatusCode::INTERNAL_SERVER_ERROR,uri.path()),
    }
}
#[derive(serde::Deserialize)]
#[serde(rename_all="camelCase",deny_unknown_fields)]
struct DiscordReviewBody {request_id:String,job_id:String,attempt_revision:Option<String>,failure_revision:String,reason:String}
pub(super) async fn discord_review(State(db):State<Arc<Client>>,headers:HeaderMap,OriginalUri(uri):OriginalUri,verifier:Option<Extension<Arc<AccessTokenVerifier>>>,body:Bytes) -> Response {
    let identity=match authenticate(&db,&headers,verifier).await {Ok(v)=>v,Err(status)=>return failure(status,uri.path())};
    let params:Vec<_>=form_urlencoded::parse(uri.query().unwrap_or("").as_bytes()).collect();
    if params.len()!=1 || params[0].0!="environmentId" || params[0].1.is_empty() || params[0].1.len()>512 || params[0].1.contains('\0') {return failure(StatusCode::BAD_REQUEST,uri.path());}
    let Some(input)=(body.len()<=4096).then(||serde_json::from_slice::<DiscordReviewBody>(&body).ok()).flatten() else{return failure(StatusCode::BAD_REQUEST,uri.path());};
    if input.reason.trim().is_empty() || input.reason.len()>512 || input.reason.contains('\0') {return failure(StatusCode::BAD_REQUEST,uri.path());}
    let (Ok(request),Ok(job),Ok(failure_revision),Ok(exp))=(Uuid::parse_str(&input.request_id),Uuid::parse_str(&input.job_id),input.failure_revision.parse::<i64>(),i64::try_from(identity.expires_at)) else{return failure(StatusCode::BAD_REQUEST,uri.path());};
    if failure_revision<0 || failure_revision.to_string()!=input.failure_revision {return failure(StatusCode::BAD_REQUEST,uri.path());}
    let attempt=match input.attempt_revision {Some(value)=>match Uuid::parse_str(&value){Ok(id)=>Some(id),Err(_)=>return failure(StatusCode::BAD_REQUEST,uri.path())},None=>None};
    let env=params[0].1.as_ref();
    match guarded_query(&db,&identity,"SELECT public.novu_rust_operator_jwt_discord_review($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",&[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&request,&job,&attempt,&failure_revision,&input.reason]).await {
        Ok(row)=>([(header::CACHE_CONTROL,"no-store")],Json(row.get::<_,serde_json::Value>(0))).into_response(),
        Err(e) if e.code().is_some_and(|c|c.code()=="42501")=>failure(StatusCode::UNAUTHORIZED,uri.path()),
        Err(e) if e.code().is_some_and(|c|c.code()=="22023")=>failure(StatusCode::BAD_REQUEST,uri.path()),
        Err(e) if e.code().is_some_and(|c|matches!(c.code(),"P4090"|"40001"))=>{let mut response=error_at(StatusCode::CONFLICT,"Review conflict","Conflict",uri.path()).into_response();response.headers_mut().insert(header::CACHE_CONTROL,header::HeaderValue::from_static("no-store"));response},
        Err(_)=>failure(StatusCode::INTERNAL_SERVER_ERROR,uri.path()),
    }
}

pub(super) async fn discord_review_grant_read(State(db):State<Arc<Client>>,headers:HeaderMap,OriginalUri(uri):OriginalUri,verifier:Option<Extension<Arc<AccessTokenVerifier>>>) -> Response { query(db,headers,uri,verifier,Lookup::DiscordReviewGrantRead).await }
pub(super) async fn discord_review_grant_request(State(db):State<Arc<Client>>,headers:HeaderMap,OriginalUri(uri):OriginalUri,verifier:Option<Extension<Arc<AccessTokenVerifier>>>) -> Response { query(db,headers,uri,verifier,Lookup::DiscordReviewGrantRequest).await }
pub(super) async fn discord_review_grant_audit(State(db):State<Arc<Client>>,headers:HeaderMap,OriginalUri(uri):OriginalUri,verifier:Option<Extension<Arc<AccessTokenVerifier>>>) -> Response { query(db,headers,uri,verifier,Lookup::DiscordReviewGrantAudit).await }
pub(super) async fn discord_policy_grant_read(State(db):State<Arc<Client>>,headers:HeaderMap,OriginalUri(uri):OriginalUri,verifier:Option<Extension<Arc<AccessTokenVerifier>>>) -> Response { query(db,headers,uri,verifier,Lookup::DiscordPolicyGrantRead).await }
pub(super) async fn discord_policy_grant_request(State(db):State<Arc<Client>>,headers:HeaderMap,OriginalUri(uri):OriginalUri,verifier:Option<Extension<Arc<AccessTokenVerifier>>>) -> Response { query(db,headers,uri,verifier,Lookup::DiscordPolicyGrantRequest).await }
pub(super) async fn discord_policy_grant_audit(State(db):State<Arc<Client>>,headers:HeaderMap,OriginalUri(uri):OriginalUri,verifier:Option<Extension<Arc<AccessTokenVerifier>>>) -> Response { query(db,headers,uri,verifier,Lookup::DiscordPolicyGrantAudit).await }
pub(super) async fn discord_retry_grant_read(State(db):State<Arc<Client>>,headers:HeaderMap,OriginalUri(uri):OriginalUri,verifier:Option<Extension<Arc<AccessTokenVerifier>>>) -> Response { query(db,headers,uri,verifier,Lookup::DiscordRetryGrantRead).await }
pub(super) async fn discord_retry_grant_request(State(db):State<Arc<Client>>,headers:HeaderMap,OriginalUri(uri):OriginalUri,verifier:Option<Extension<Arc<AccessTokenVerifier>>>) -> Response { query(db,headers,uri,verifier,Lookup::DiscordRetryGrantRequest).await }
pub(super) async fn discord_retry_grant_audit(State(db):State<Arc<Client>>,headers:HeaderMap,OriginalUri(uri):OriginalUri,verifier:Option<Extension<Arc<AccessTokenVerifier>>>) -> Response { query(db,headers,uri,verifier,Lookup::DiscordRetryGrantAudit).await }
