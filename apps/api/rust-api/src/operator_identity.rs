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
    ResendResult,
    ResendJobs,
    WorkflowRead,
    WorkflowAudit,
    WorkflowRequest,
    WorkflowGrantRead,
    WorkflowGrantRequest,
    WorkflowGrantAudit,
    SubscriberProfileRead,
    SubscriberProfileRequest,
    SubscriberCreateRequest,
    SubscriberDeleteRequest,
    SubscriberGrantRead,
    SubscriberGrantRequest,
    SubscriberGrantAudit,
}

fn authenticate(
    headers: &HeaderMap,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
) -> Result<VerifiedSubject, StatusCode> {
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
    token
        .and_then(|t| verifier.verify(t))
        .ok_or(StatusCode::UNAUTHORIZED)
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
pub(super) async fn workflow_grant_request(
    State(db): State<Arc<Client>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    verifier: Option<Extension<Arc<AccessTokenVerifier>>>,
) -> Response {
    query(db, headers, uri, verifier, Lookup::WorkflowGrantRequest).await
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
    let identity = match authenticate(&headers, verifier) {
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
    match db.query_one("SELECT public.novu_rust_operator_jwt_subscriber_grant_change($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)",&[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&request,&input.target_issuer,&input.target_subject,&input.target_client_id,&input.permission,&input.expected,&input.desired,&input.reason]).await {
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
    let identity = match authenticate(&headers, verifier) {
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
    match db.query_one("SELECT public.novu_rust_operator_jwt_subscriber_delete($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",&[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&request,&input.subscriber_id,&generation,&revision,&input.reason]).await {
        Ok(row)=>([(header::CACHE_CONTROL,"no-store")],Json(row.get::<_,serde_json::Value>(0))).into_response(),
        Err(e) if e.code().is_some_and(|c|c.code()=="42501")=>failure(StatusCode::UNAUTHORIZED,uri.path()),
        Err(e) if e.code().is_some_and(|c|c.code()=="22023")=>failure(StatusCode::BAD_REQUEST,uri.path()),
        Err(e) if e.code().is_some_and(|c|c.code()=="P4090")=>failure(StatusCode::CONFLICT,uri.path()),
        Err(_)=>failure(StatusCode::INTERNAL_SERVER_ERROR,uri.path()),
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
    let identity = match authenticate(&headers, verifier) {
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
    match db
        .query_one(
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
    let identity = match authenticate(&headers, verifier) {
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
        db.query_one("SELECT public.novu_rust_operator_jwt_subscriber_profile_write_v2($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",&[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&request,&input.subscriber_id,&generation,&revision,&input.profile,&input.reason]).await
    } else {
        db.query_one("SELECT public.novu_rust_operator_jwt_subscriber_profile_write($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",&[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&input.subscriber_id,&generation,&revision,&input.profile,&input.reason]).await
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
    let identity = match authenticate(&headers, verifier) {
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
    match db.query_one("SELECT public.novu_rust_operator_jwt_workflow_grant_change($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)",&[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&request,&input.target_issuer,&input.target_subject,&input.target_client_id,&input.resource,&input.permission,&input.expected,&input.desired,&input.reason]).await{
        Ok(row)=>([(header::CACHE_CONTROL,"no-store")],Json(row.get::<_,serde_json::Value>(0))).into_response(),
        Err(e) if e.code().is_some_and(|c|c.code()=="42501")=>failure(StatusCode::UNAUTHORIZED,uri.path()),
        Err(e) if e.code().is_some_and(|c|c.code()=="22023")=>failure(StatusCode::BAD_REQUEST,uri.path()),
        Err(e) if e.code().is_some_and(|c|c.code()=="P4090")=>failure(StatusCode::CONFLICT,uri.path()),
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
    let identity = match authenticate(&headers, verifier) {
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
    match db.query_one("SELECT public.novu_rust_operator_jwt_workflow_write_v2($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",&[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&request,&input.workflow,&input.resource,&input.expected,&input.desired,&input.reason]).await {
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
    let identity = match authenticate(&headers, verifier) {
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
    match db
        .query_one(
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
    let identity = match authenticate(&headers, verifier) {
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
        Lookup::DecisionRequest => &["environmentId", "requestId"],
        Lookup::Identity => &["environmentId"],
        Lookup::PermissionIdentity => &["environmentId", "issuer", "subject", "clientId"],
        Lookup::PermissionAudit => &["environmentId", "before"],
        Lookup::PermissionChangeRequest => &["environmentId", "requestId"],
        Lookup::ResendResult => &["environmentId", "jobId"],
        Lookup::ResendJobs => &["environmentId", "after"],
        Lookup::WorkflowRead => &["environmentId", "workflow", "resource"],
        Lookup::WorkflowAudit => &["environmentId", "workflow", "resource", "after"],
        Lookup::WorkflowRequest => &["environmentId", "resource", "requestId"],
        Lookup::WorkflowGrantRead => &[
            "environmentId",
            "targetIssuer",
            "targetSubject",
            "targetClientId",
            "resource",
            "permission",
        ],
        Lookup::WorkflowGrantRequest => &["environmentId", "requestId"],
        Lookup::WorkflowGrantAudit => &["environmentId", "after"],
        Lookup::SubscriberProfileRead => &["environmentId", "subscriberId"],
        Lookup::SubscriberProfileRequest => &["environmentId", "requestId"],
        Lookup::SubscriberCreateRequest => &["environmentId", "requestId"],
        Lookup::SubscriberDeleteRequest => &["environmentId", "requestId"],
        Lookup::SubscriberGrantRequest => &["environmentId", "requestId"],
        Lookup::SubscriberGrantAudit => &["environmentId", "after"],
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
            db.query_one("SELECT public.novu_rust_operator_jwt_subscriber_grant_audit_read($1,$2,$3,$4,$5,$6)",&[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&after]).await
        }
        Lookup::SubscriberGrantRequest => {
            let Some(request) = params
                .get("requestId")
                .and_then(|s| Uuid::parse_str(s).ok())
            else {
                return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
            };
            db.query_one(
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
            db.query_one("SELECT public.novu_rust_operator_jwt_subscriber_grant_read($1,$2,$3,$4,$5,$6,$7,$8,$9)",&[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&issuer,&subject,&client,&permission]).await
        }
        Lookup::SubscriberDeleteRequest => {
            let Some(request) = params
                .get("requestId")
                .and_then(|s| Uuid::parse_str(s).ok())
            else {
                return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
            };
            db.query_one("SELECT public.novu_rust_operator_jwt_subscriber_delete_request_read($1,$2,$3,$4,$5,$6)",&[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&request]).await
        }
        Lookup::SubscriberCreateRequest => {
            let Some(request) = params
                .get("requestId")
                .and_then(|s| Uuid::parse_str(s).ok())
            else {
                return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
            };
            db.query_one("SELECT public.novu_rust_operator_jwt_subscriber_create_request_read($1,$2,$3,$4,$5,$6)",&[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&request]).await
        }
        Lookup::SubscriberProfileRequest => {
            let Some(request) = params
                .get("requestId")
                .and_then(|s| Uuid::parse_str(s).ok())
            else {
                return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
            };
            db.query_one("SELECT public.novu_rust_operator_jwt_subscriber_profile_request_read($1,$2,$3,$4,$5,$6)",&[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&request]).await
        }
        Lookup::SubscriberProfileRead => {
            let Some(subscriber) = params.get("subscriberId").filter(|s| !s.contains('\0')) else {
                return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
            };
            db.query_one(
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
            db.query_one(
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
            db.query_one("SELECT public.novu_rust_operator_jwt_workflow_grant_read($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",&[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&issuer,&subject,&client,&resource,&permission]).await
        }
        Lookup::WorkflowGrantRequest => {
            let Some(request) = params
                .get("requestId")
                .and_then(|v| Uuid::parse_str(v).ok())
            else {
                return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
            };
            db.query_one(
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
            db.query_one(
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
            db.query_one(
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
            db.query_one(
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
        Lookup::ResendJobs => {
            let after = match params.get("after") {
                None => None,
                Some(value) => match Uuid::parse_str(value) {
                    Ok(id) => Some(id),
                    Err(_) => {
                        return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
                    }
                },
            };
            db.query_one(
                "SELECT public.novu_rust_operator_jwt_resend_jobs($1,$2,$3,$4,$5,$6)",
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
        Lookup::ResendResult => {
            let Some(job) = params.get("jobId").and_then(|id| Uuid::parse_str(id).ok()) else {
                return failure(StatusCode::BAD_REQUEST, "Invalid request", "Bad Request");
            };
            db.query_one(
                "SELECT public.novu_rust_operator_jwt_resend_result($1,$2,$3,$4,$5,$6)",
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
            db.query_one(
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
            db.query_one(
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
            db.query_one(
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
            db.query_one(
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
            db.query_one(
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
        Lookup::DecisionRequest => {
            let Some(request) = params
                .get("requestId")
                .and_then(|id| Uuid::parse_str(id).ok())
            else {
                return self::failure(StatusCode::BAD_REQUEST, uri.path());
            };
            db.query_one(
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
            db.query_one(
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
    let identity = match authenticate(&headers, verifier) {
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
    match db.query_one("SELECT public.novu_rust_operator_jwt_resolve_alert($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)", &[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&request,&input.channel,&input.target_id,&attempt,&input.decision,&input.reason]).await {
        Ok(row)=>([(header::CACHE_CONTROL,"no-store")],Json(serde_json::json!({"recorded":true,"replayed":row.get::<_,bool>(0)}))).into_response(),
        Err(e) if e.code().is_some_and(|c|c.code()=="42501")=>failure(StatusCode::UNAUTHORIZED,uri.path()),
        Err(e) if e.code().is_some_and(|c|c.code()=="22023")=>failure(StatusCode::BAD_REQUEST,uri.path()),
        Err(_)=>failure(StatusCode::INTERNAL_SERVER_ERROR,uri.path()),
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
    let identity = match authenticate(&headers, verifier) {
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
    match db.query_one("SELECT public.novu_rust_operator_jwt_change_permissions($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)",&[&identity.issuer,&identity.subject,&identity.client_id,&env,&exp,&request,&input.target_issuer,&input.target_subject,&input.target_client_id,&input.expected,&input.desired,&input.reason]).await {
        Ok(row)=>([(header::CACHE_CONTROL,"no-store")],Json(row.get::<_,serde_json::Value>(0))).into_response(),
        Err(e) if e.code().is_some_and(|c|c.code()=="42501")=>failure(StatusCode::UNAUTHORIZED,uri.path()),
        Err(e) if e.code().is_some_and(|c|c.code()=="22023")=>failure(StatusCode::BAD_REQUEST,uri.path()),
        Err(_)=>failure(StatusCode::INTERNAL_SERVER_ERROR,uri.path()),
    }
}
