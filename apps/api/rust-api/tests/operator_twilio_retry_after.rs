//! Operator Twilio retry-after result against a disposable database.
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
async fn retry_guidance_is_scoped_to_authorized_operator() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let db = connect(&url).await;
    let env = format!("twilio-429-{}", Uuid::new_v4());
    let role = format!("twilio_result_{}", Uuid::new_v4().simple());
    let job = Uuid::new_v4();
    db.execute(
        "INSERT INTO public.novu_rust_environments VALUES($1,'org')",
        &[&env],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) VALUES('https://issuer.test','alice','console',$1)",&[&env]).await.unwrap();
    db.execute("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status) VALUES($1,$2,'experimental_twilio_sms','{}','failed')",&[&job,&env]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_twilio_attempts(job_id,environment_id,state,http_status,retry_after_seconds,retry_not_before) VALUES($1,$2,'rejected',429,120,clock_timestamp()+interval '2 minutes')",&[&job,&env]).await.unwrap();
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_twilio_result(text,text,text,text,bigint,uuid) TO {role}")).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_operator_jwt_api_logins SELECT rolname,oid FROM pg_roles WHERE rolname::text=$1",&[&role]).await.unwrap();
    let api = connect(&url).await;
    api.batch_execute(&format!("SET SESSION AUTHORIZATION {role}"))
        .await
        .unwrap();
    let app = novu_rust_api::router(Arc::new(api)).layer(Extension(Arc::new(
        AccessTokenVerifier::new("https://issuer.test", "swallow", &[("key", PUBLIC)]).unwrap(),
    )));
    let token = jwt("alice");
    let path = format!("/v1/experimental/operator/twilio-result?environmentId={env}&jobId={job}");
    assert_eq!(
        response(app.clone(), &token, "GET", &path, None).await.0,
        StatusCode::UNAUTHORIZED
    );
    db.execute("INSERT INTO public.novu_rust_operator_jwt_twilio_result_grants VALUES('https://issuer.test','alice','console',$1)",&[&env]).await.unwrap();
    let (status, value) = response(app.clone(), &token, "GET", &path, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["attemptHttpStatus"], 429);
    assert_eq!(value["retryAfterSeconds"], 120);
    assert!(value["retryNotBefore"].is_string());
    db.execute(
        "DELETE FROM public.novu_rust_operator_jwt_twilio_result_grants WHERE environment_id=$1",
        &[&env],
    )
    .await
    .unwrap();
    assert_eq!(
        response(app, &token, "GET", &path, None).await.0,
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
#[ignore = "requires fully migrated disposable PostgreSQL and role creation"]
async fn twilio_delayed_retry_is_authorized_replayable_and_evidence_fenced() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let db = connect(&url).await;
    let env = format!("twilio-jwt-delay-{}", Uuid::new_v4());
    let role = format!("twilio_jwt_delay_{}", Uuid::new_v4().simple());
    let job = Uuid::new_v4();
    let attempt = Uuid::new_v4();
    let id = Uuid::new_v4();
    db.execute(
        "INSERT INTO public.novu_rust_environments VALUES($1,'org')",
        &[&env],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) VALUES('https://issuer.test','alice','console',$1)",&[&env]).await.unwrap();
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_twilio_retry_after(text,text,text,text,bigint,uuid,uuid,uuid,text,text),public.novu_rust_operator_jwt_twilio_retry_request(text,text,text,text,bigint,uuid) TO {role}")).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_operator_jwt_api_logins SELECT rolname,oid FROM pg_roles WHERE rolname::text=$1",&[&role]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT $1,$2,'retry',oid FROM pg_roles WHERE rolname::text=$2",&[&env,&role]).await.unwrap();
    db.execute("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status) VALUES($1,$2,'experimental_twilio_sms','{}','failed')",&[&job,&env]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_twilio_dispatch_ids(id,job_id,environment_id) VALUES($1,$2,$3)",&[&attempt,&job,&env]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_twilio_attempts(job_id,environment_id,state,http_status,dispatch_id,retry_after_seconds,retry_not_before) VALUES($1,$2,'rejected',429,$3,120,clock_timestamp()+interval '120 seconds')",&[&job,&env,&attempt]).await.unwrap();
    let api = connect(&url).await;
    api.batch_execute(&format!("SET SESSION AUTHORIZATION {role}"))
        .await
        .unwrap();
    let app = novu_rust_api::router(Arc::new(api)).layer(Extension(Arc::new(
        AccessTokenVerifier::new("https://issuer.test", "swallow", &[("key", PUBLIC)]).unwrap(),
    )));
    let token = jwt("alice");
    let path = format!("/v1/experimental/operator/twilio-retry-after?environmentId={env}");
    let lookup = format!(
        "/v1/experimental/operator/twilio-retry-request?environmentId={env}&requestId={id}"
    );
    let body = json!({"requestId":id,"jobId":job,"attemptId":attempt,"decision":"retry-after","reason":"ticket"});
    assert_eq!(
        response(app.clone(), &token, "POST", &path, Some(body.clone()))
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    db.execute("INSERT INTO public.novu_rust_operator_jwt_twilio_retry_grants VALUES('https://issuer.test','alice','console',$1)",&[&env]).await.unwrap();
    let mut stale = body.clone();
    stale["attemptId"] = json!(Uuid::new_v4());
    assert_eq!(
        response(app.clone(), &token, "POST", &path, Some(stale))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    db.execute("INSERT INTO public.novu_rust_twilio_delivery_events(environment_id,account_sid,message_sid,status,job_id,dispatch_id) VALUES($1,'ACfixture','SMfixture','delivered',$2,$3)",&[&env,&job,&attempt]).await.unwrap();
    assert_eq!(
        response(app.clone(), &token, "POST", &path, Some(body.clone()))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    db.execute(
        "DELETE FROM public.novu_rust_twilio_delivery_events WHERE environment_id=$1 AND job_id=$2",
        &[&env, &job],
    )
    .await
    .unwrap();
    db.batch_execute("ALTER TABLE public.novu_rust_delivery_reconciliations ADD CONSTRAINT fixture_twilio_jwt_reject CHECK(twilio_retry_not_before IS NULL) NOT VALID").await.unwrap();
    assert_eq!(
        response(app.clone(), &token, "POST", &path, Some(body.clone()))
            .await
            .0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(db.query_one("SELECT count(*) FROM public.novu_rust_operator_jwt_twilio_retry_requests WHERE request_id=$1",&[&id]).await.unwrap().get::<_,i64>(0),0);
    db.batch_execute("ALTER TABLE public.novu_rust_delivery_reconciliations DROP CONSTRAINT fixture_twilio_jwt_reject").await.unwrap();
    let (status, first) = response(app.clone(), &token, "POST", &path, Some(body.clone())).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(first["replayed"], false);
    assert!(first["availableAt"].is_string());
    let (status, replay) = response(app.clone(), &token, "POST", &path, Some(body.clone())).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(replay["replayed"], true);
    assert_eq!(replay["availableAt"], first["availableAt"]);
    let mut conflict = body.clone();
    conflict["reason"] = json!("changed");
    assert_eq!(
        response(app.clone(), &token, "POST", &path, Some(conflict))
            .await
            .0,
        StatusCode::CONFLICT
    );
    let (status, record) = response(app.clone(), &token, "GET", &lookup, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(record["found"], true);
    assert_eq!(record["attemptId"], attempt.to_string());
    let row=db.query_one("SELECT status,twilio_delayed_retry,available_at>clock_timestamp()+interval '100 seconds' FROM public.novu_queue_jobs WHERE id=$1",&[&job]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "pending");
    assert!(row.get::<_, bool>(1));
    assert!(row.get::<_, bool>(2));
    db.execute("DELETE FROM public.novu_rust_delivery_operator_grants WHERE environment_id=$1 AND database_principal=$2 AND decision='retry'",&[&env,&role]).await.unwrap();
    assert_eq!(
        response(app, &token, "GET", &lookup, None).await.0,
        StatusCode::UNAUTHORIZED
    );
}
