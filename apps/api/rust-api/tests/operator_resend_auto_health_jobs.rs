//! Restricted pages of Resend retry attention jobs.
use axum::{
    Extension,
    body::Body,
    http::{Request, StatusCode, header},
};
use http_body_util::BodyExt;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use novu_rust_api::operator_access_token::AccessTokenVerifier;
use serde_json::{Value, json};
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
async fn job_pages_require_separate_grant_and_match_health_filters() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let db = connect(&url).await;
    let env = format!("jwt-health-jobs-{}", Uuid::new_v4());
    let role = format!("health_jobs_{}", Uuid::new_v4().simple());
    db.execute(
        "INSERT INTO public.novu_rust_environments VALUES ($1,'org')",
        &[&env],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) VALUES ('https://issuer.test','alice','console',$1)", &[&env]).await.unwrap();
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_resend_auto_health_jobs(text,text,text,text,bigint,integer,text,uuid),public.novu_rust_operator_jwt_resend_auto_health(text,text,text,text,bigint,integer) TO {role}")).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_operator_jwt_api_logins SELECT rolname,oid FROM pg_roles WHERE rolname::text=$1", &[&role]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT $1,$2,'alert_view',oid FROM pg_roles WHERE rolname::text=$2", &[&env,&role]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_operator_jwt_resend_auto_health_grants VALUES('https://issuer.test','alice','console',$1)", &[&env]).await.unwrap();
    db.execute("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status,last_error) SELECT ('44444444-4444-4444-8444-'||lpad(n::text,12,'0'))::uuid,$1,'experimental_resend_email','{\"secret\":\"not-returned\"}','pending','Resend automatic retry requires evidence guards' FROM generate_series(1,21) n", &[&env]).await.unwrap();
    let rate = Uuid::new_v4();
    db.execute("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status) VALUES ($1,$2,'experimental_resend_email','{}','failed')", &[&rate,&env]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_resend_attempts(job_id,environment_id,state,http_status,created_at,updated_at) VALUES ($1,$2,'rejected',429,clock_timestamp()-interval '1 hour',clock_timestamp()-interval '1 hour')", &[&rate,&env]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_resend_auto_retry_policy(environment_id,enabled,max_retries,not_before) VALUES ($1,true,1,clock_timestamp()-interval '1 day')", &[&env]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_delivery_reconciliations(id,environment_id,job_id,decision,operator_name,reason,previous_http_status) VALUES ($1,$2,$3,'retry','fixture','fixture',429)", &[&Uuid::new_v4(),&env,&rate]).await.unwrap();
    let overdue = Uuid::new_v4();
    db.execute("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status,available_at) VALUES ($1,$2,'experimental_resend_email','{}','pending',clock_timestamp()-interval '2 minutes')", &[&overdue,&env]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_delivery_reconciliations(id,environment_id,job_id,decision,operator_name,reason,previous_http_status,resend_auto_retry) VALUES ($1,$2,$3,'retry','fixture','fixture',429,true)", &[&Uuid::new_v4(),&env,&overdue]).await.unwrap();
    let api = connect(&url).await;
    api.batch_execute(&format!("SET SESSION AUTHORIZATION {role}"))
        .await
        .unwrap();
    assert!(
        api.query("SELECT payload FROM public.novu_queue_jobs", &[])
            .await
            .is_err()
    );
    let app = novu_rust_api::router(Arc::new(api)).layer(Extension(Arc::new(
        AccessTokenVerifier::new("https://issuer.test", "swallow", &[("key", PUBLIC)]).unwrap(),
    )));
    let token = jwt("alice");
    let base = format!(
        "/v1/experimental/operator/resend-auto-health/jobs?environmentId={env}&graceSeconds=60&filter=guard-deferred"
    );
    assert_eq!(
        response(app.clone(), &token, "GET", &base, None).await.0,
        StatusCode::UNAUTHORIZED
    );
    db.execute("INSERT INTO public.novu_rust_operator_jwt_resend_auto_health_job_grants VALUES('https://issuer.test','alice','console',$1)", &[&env]).await.unwrap();
    let first = response(app.clone(), &token, "GET", &base, None).await;
    assert_eq!(first.0, StatusCode::OK);
    assert_eq!(first.1["data"].as_array().unwrap().len(), 20);
    assert_eq!(first.1["hasMore"], true);
    assert!(!first.1.to_string().contains("not-returned"));
    let next = first.1["next"].as_str().unwrap();
    let second = response(
        app.clone(),
        &token,
        "GET",
        &format!("{base}&after={next}"),
        None,
    )
    .await;
    assert_eq!(second.1["data"].as_array().unwrap().len(), 1);
    assert_eq!(second.1["hasMore"], false);
    for filter in ["rate-limited", "exhausted", "overdue-auto"] {
        let path = format!(
            "/v1/experimental/operator/resend-auto-health/jobs?environmentId={env}&graceSeconds=60&filter={filter}"
        );
        let page = response(app.clone(), &token, "GET", &path, None).await;
        assert_eq!(page.0, StatusCode::OK);
        assert_eq!(page.1["data"].as_array().unwrap().len(), 1);
    }
    let invalid = format!(
        "/v1/experimental/operator/resend-auto-health/jobs?environmentId={env}&graceSeconds=60&filter=invalid"
    );
    assert_eq!(
        response(app.clone(), &token, "GET", &invalid, None).await.0,
        StatusCode::BAD_REQUEST
    );
    db.execute("DELETE FROM public.novu_rust_operator_jwt_resend_auto_health_grants WHERE environment_id=$1", &[&env]).await.unwrap();
    assert_eq!(
        response(app.clone(), &token, "GET", &base, None).await.0,
        StatusCode::UNAUTHORIZED
    );
}
