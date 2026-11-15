//! Slack operator decisions against a fully migrated disposable database.
use axum::{
    body::Body,
    http::{header, Request, StatusCode},
    Extension,
};
use http_body_util::BodyExt;
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use novu_rust_api::operator_access_token::AccessTokenVerifier;
use serde_json::{json, Value};
use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio_postgres::{Client, NoTls};
use tower::ServiceExt;
use uuid::Uuid;
const PRIVATE: &[u8] = include_bytes!("fixtures/operator_jwt/test-only-private.pem");
const PUBLIC: &[u8] = include_bytes!("fixtures/operator_jwt/test-only-public.pem");
async fn connect(url: &str) -> Client {
    let (db, driver) = tokio_postgres::connect(url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    db
}
fn jwt(subject: &str) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let mut h = Header::new(Algorithm::RS256);
    h.typ = Some("at+jwt".into());
    h.kid = Some("key".into());
    encode(&h,&json!({"iss":"https://issuer.test","sub":subject,"aud":"swallow","exp":now+300,"iat":now-1,"jti":"token","client_id":"console"}),&EncodingKey::from_rsa_pem(PRIVATE).unwrap()).unwrap()
}
fn request(token: &str, method: &str, path: &str, body: Option<Value>) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(path)
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.map(|b| b.to_string()).unwrap_or_default()))
        .unwrap()
}
async fn response(
    app: axum::Router,
    token: &str,
    method: &str,
    path: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let r = app
        .oneshot(request(token, method, path, body))
        .await
        .unwrap();
    let status = r.status();
    let bytes = r.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}
#[tokio::test]
#[ignore = "requires fully migrated disposable PostgreSQL and role creation"]
async fn decision_is_scoped_idempotent_and_attempt_fenced() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let db = connect(&url).await;
    db.batch_execute("INSERT INTO public.novu_rust_environments VALUES ('resend429-a','org'),('resend429-b','org'); INSERT INTO public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) VALUES ('https://issuer.test','alice','console','resend429-a'),('https://issuer.test','bob','console','resend429-a')").await.unwrap();
    let role = format!("resend429_api_{}", Uuid::new_v4().simple());
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_resend_429_resolve(text,text,text,text,bigint,uuid,uuid,uuid,text,text),public.novu_rust_operator_jwt_resend_429_request(text,text,text,text,bigint,uuid),public.novu_rust_operator_jwt_resend_429_audit_read(text,text,text,text,bigint,uuid) TO {role}")).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_operator_jwt_api_logins SELECT rolname,oid FROM pg_roles WHERE rolname::text=$1", &[&role]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'resend429-a',rolname,'retry',oid FROM pg_roles WHERE rolname::text=$1", &[&role]).await.unwrap();
    let api = connect(&url).await;
    api.batch_execute(&format!("SET SESSION AUTHORIZATION {role}"))
        .await
        .unwrap();
    assert!(api
        .query(
            "SELECT * FROM public.novu_rust_operator_jwt_resend_429_requests",
            &[]
        )
        .await
        .is_err());
    let app = novu_rust_api::router(Arc::new(api)).layer(Extension(Arc::new(
        AccessTokenVerifier::new("https://issuer.test", "swallow", &[("key", PUBLIC)]).unwrap(),
    )));
    let job = Uuid::new_v4();
    db.execute("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status) VALUES ($1,'resend429-a','experimental_resend_email','{}','failed')", &[&job]).await.unwrap();
    let attempt: Uuid=db.query_one("INSERT INTO public.novu_rust_resend_attempts(job_id,environment_id,state,http_status,retry_after_seconds,retry_not_before) VALUES($1,'resend429-a','rejected',429,120,clock_timestamp()+interval '120 seconds') RETURNING attempt_id", &[&job]).await.unwrap().get(0);
    let request_id = Uuid::new_v4();
    let payload = json!({"requestId":request_id,"jobId":job,"attemptId":attempt,"mode":"after","reason":"reviewed"});
    let path = "/v1/experimental/operator/resend-429-decision?environmentId=resend429-a";
    let token = jwt("alice");
    assert_eq!(
        response(app.clone(), &token, "POST", path, Some(payload.clone()))
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    db.execute("INSERT INTO public.novu_rust_operator_jwt_resend_429_grants VALUES('https://issuer.test','alice','console','resend429-a','after')",&[]).await.unwrap();
    let mut immediate = payload.clone();
    immediate["mode"] = json!("immediate");
    assert_eq!(
        response(app.clone(), &token, "POST", path, Some(immediate))
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    let mut stale = payload.clone();
    stale["attemptId"] = json!(Uuid::new_v4());
    assert_eq!(
        response(app.clone(), &token, "POST", path, Some(stale))
            .await
            .0,
        StatusCode::CONFLICT
    );
    let (status, first) = response(app.clone(), &token, "POST", path, Some(payload.clone())).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(first["replayed"], false);
    let (status, replay) = response(app.clone(), &token, "POST", path, Some(payload.clone())).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(replay["replayed"], true);
    assert_eq!(first["availableAt"], replay["availableAt"]);
    let mut conflict = payload.clone();
    conflict["reason"] = json!("other");
    assert_eq!(
        response(app.clone(), &token, "POST", path, Some(conflict))
            .await
            .0,
        StatusCode::CONFLICT
    );
    let lookup=format!("/v1/experimental/operator/resend-429-decision/request?environmentId=resend429-a&requestId={request_id}");
    let (status, found) = response(app.clone(), &token, "GET", &lookup, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(found["attemptId"], attempt.to_string());
    assert_eq!(
        response(app.clone(), &jwt("bob"), "GET", &lookup, None)
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    let audit = "/v1/experimental/operator/resend-429-decision/audit?environmentId=resend429-a";
    assert_eq!(
        response(app.clone(), &token, "GET", audit, None).await.0,
        StatusCode::UNAUTHORIZED
    );
    db.execute("INSERT INTO public.novu_rust_operator_jwt_resend_429_audit_grants VALUES('https://issuer.test','alice','console','resend429-a')",&[]).await.unwrap();
    let (status, records) = response(app.clone(), &token, "GET", audit, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(records["data"].as_array().unwrap().len(), 1);
    assert_eq!(records["data"][0]["attemptId"], attempt.to_string());
    let row=db.query_one("SELECT status,available_at > clock_timestamp()+interval '100 seconds' FROM public.novu_queue_jobs WHERE id=$1",&[&job]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "pending");
    assert!(row.get::<_, bool>(1));
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM public.novu_rust_delivery_reconciliations WHERE job_id=$1",
            &[&job]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        1
    );
    db.execute(
        "UPDATE public.novu_queue_jobs SET status='failed' WHERE id=$1",
        &[&job],
    )
    .await
    .unwrap();
    let newer:Uuid=db.query_one("INSERT INTO public.novu_rust_resend_attempts(job_id,environment_id,state,http_status) VALUES($1,'resend429-a','rejected',429) RETURNING attempt_id",&[&job]).await.unwrap().get(0);
    assert_ne!(attempt, newer);
    let mut old = payload.clone();
    old["requestId"] = json!(Uuid::new_v4());
    assert_eq!(
        response(app.clone(), &token, "POST", path, Some(old))
            .await
            .0,
        StatusCode::CONFLICT
    );
    db.execute("INSERT INTO public.novu_rust_operator_jwt_resend_429_grants VALUES('https://issuer.test','alice','console','resend429-a','immediate')",&[]).await.unwrap();
    let fresh = json!({"requestId":Uuid::new_v4(),"jobId":job,"attemptId":newer,"mode":"immediate","reason":"reviewed"});
    db.batch_execute("ALTER TABLE public.novu_rust_operator_jwt_resend_429_audit ADD CONSTRAINT test_audit_failure CHECK (reason <> 'abort-test') NOT VALID").await.unwrap();
    let mut abort = fresh.clone();
    abort["reason"] = json!("abort-test");
    assert_eq!(
        response(app.clone(), &token, "POST", path, Some(abort))
            .await
            .0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(
        db.query_one(
            "SELECT status FROM public.novu_queue_jobs WHERE id=$1",
            &[&job]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "failed"
    );
    assert_eq!(db.query_one("SELECT count(*) FROM public.novu_rust_resend_attempts WHERE job_id=$1 AND attempt_id=$2",&[&job,&newer]).await.unwrap().get::<_,i64>(0),1);
    db.batch_execute("ALTER TABLE public.novu_rust_operator_jwt_resend_429_audit DROP CONSTRAINT test_audit_failure").await.unwrap();
    db.execute("UPDATE public.novu_rust_resend_attempts SET provider_message_id='callback-proof' WHERE job_id=$1", &[&job]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_resend_delivery_events(environment_id,event_id,email_id,event_type) VALUES ('resend429-a','callback-fixture','callback-proof','email.delivered')", &[]).await.unwrap();
    assert_eq!(
        response(app.clone(), &token, "POST", path, Some(fresh.clone()))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(db.query_one("SELECT count(*) FROM public.novu_rust_operator_jwt_resend_429_requests WHERE job_id=$1 AND attempt_id=$2", &[&job,&newer]).await.unwrap().get::<_,i64>(0), 0);
    db.execute("DELETE FROM public.novu_rust_resend_delivery_events WHERE event_id='callback-fixture' AND environment_id='resend429-a'", &[]).await.unwrap();
    assert_eq!(
        response(app.clone(), &token, "POST", path, Some(fresh))
            .await
            .0,
        StatusCode::OK
    );
    db.execute("DELETE FROM public.novu_rust_operator_jwt_resend_429_grants WHERE subject='alice' AND mode='after'",&[]).await.unwrap();
    assert_eq!(
        response(app.clone(), &token, "POST", path, Some(payload))
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        response(app.clone(), &token, "GET", &lookup, None).await.0,
        StatusCode::UNAUTHORIZED
    );
}
