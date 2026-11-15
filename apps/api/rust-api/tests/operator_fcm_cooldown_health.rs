//! Operator FCM shared cooldown health against a disposable database.
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
async fn health_is_scoped_and_requires_independent_operator_and_database_grants() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let db = connect(&url).await;
    let env = format!("jwt-health-{}", Uuid::new_v4());
    let other = format!("other-health-{}", Uuid::new_v4());
    let role = format!("health_api_{}", Uuid::new_v4().simple());
    db.execute(
        "INSERT INTO public.novu_rust_environments VALUES ($1,'org'),($2,'org')",
        &[&env, &other],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) VALUES ('https://issuer.test','alice','console',$1),('https://issuer.test','alice','console',$2)", &[&env,&other]).await.unwrap();
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_fcm_cooldown_health(text,text,text,text,bigint,integer) TO {role}")).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_operator_jwt_api_logins SELECT rolname,oid FROM pg_roles WHERE rolname::text=$1", &[&role]).await.unwrap();
    let api = connect(&url).await;
    api.batch_execute(&format!("SET SESSION AUTHORIZATION {role}"))
        .await
        .unwrap();
    assert!(
        api.query("SELECT * FROM public.novu_queue_jobs", &[])
            .await
            .is_err()
    );
    let app = novu_rust_api::router(Arc::new(api)).layer(Extension(Arc::new(
        AccessTokenVerifier::new("https://issuer.test", "swallow", &[("key", PUBLIC)]).unwrap(),
    )));
    let token = jwt("alice");
    let path = format!(
        "/v1/experimental/operator/fcm-cooldown-health?environmentId={env}&graceSeconds=60"
    );
    assert_eq!(
        response(app.clone(), &token, "GET", &path, None).await.0,
        StatusCode::UNAUTHORIZED
    );
    db.execute("INSERT INTO public.novu_rust_operator_jwt_fcm_cooldown_health_grants VALUES('https://issuer.test','alice','console',$1)", &[&env]).await.unwrap();
    assert_eq!(
        response(app.clone(), &token, "GET", &path, None).await.0,
        StatusCode::UNAUTHORIZED
    );
    db.execute("INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT $1,$2,'alert_view',oid FROM pg_roles WHERE rolname::text=$2", &[&env,&role]).await.unwrap();
    // A missing policy is a configuration error, not a healthy result.
    assert_eq!(
        response(app.clone(), &token, "GET", &path, None).await.0,
        StatusCode::BAD_REQUEST
    );
    db.execute("INSERT INTO public.novu_rust_fcm_cooldown_policies(environment_id,fallback_seconds,not_before) VALUES ($1,60,clock_timestamp()+interval '5 minutes')", &[&env]).await.unwrap();
    db.execute("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status,last_error,available_at) VALUES ($1,$2,'experimental_fcm_push','{}','pending','FCM shared cooldown active',clock_timestamp()-interval '2 minutes')", &[&Uuid::new_v4(),&env]).await.unwrap();
    let result = response(app.clone(), &token, "GET", &path, None).await;
    assert_eq!(result.0, StatusCode::OK);
    assert_eq!(result.1["active"], true);
    assert!(result.1["notBefore"].is_string());
    assert_eq!(result.1["fallbackSeconds"], 60);
    assert_eq!(result.1["deferred"], 1);
    assert_eq!(result.1["overdue"], 1);
    for suffix in ["0", "86401", "01", "-1"] {
        let invalid = format!(
            "/v1/experimental/operator/fcm-cooldown-health?environmentId={env}&graceSeconds={suffix}"
        );
        assert_eq!(
            response(app.clone(), &token, "GET", &invalid, None).await.0,
            StatusCode::BAD_REQUEST
        );
    }
    let other_path = format!(
        "/v1/experimental/operator/fcm-cooldown-health?environmentId={other}&graceSeconds=60"
    );
    assert_eq!(
        response(app.clone(), &token, "GET", &other_path, None)
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    db.execute("DELETE FROM public.novu_rust_delivery_operator_grants WHERE environment_id=$1 AND database_principal=$2 AND decision='alert_view'", &[&env,&role]).await.unwrap();
    assert_eq!(
        response(app.clone(), &token, "GET", &path, None).await.0,
        StatusCode::UNAUTHORIZED
    );
    db.execute("INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT $1,$2,'alert_view',oid FROM pg_roles WHERE rolname::text=$2", &[&env,&role]).await.unwrap();
    db.execute("DELETE FROM public.novu_rust_operator_jwt_fcm_cooldown_health_grants WHERE environment_id=$1", &[&env]).await.unwrap();
    assert_eq!(
        response(app.clone(), &token, "GET", &path, None).await.0,
        StatusCode::UNAUTHORIZED
    );
}
