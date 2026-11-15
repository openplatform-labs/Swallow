//! Operator FCM partial failure health against a disposable database.
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
async fn fcm_partial_health_requires_both_grants_and_is_environment_scoped() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let db = connect(&url).await;
    let env = format!("fcm-partial-jwt-{}", Uuid::new_v4());
    let other = format!("fcm-partial-other-{}", Uuid::new_v4());
    let role = format!("fcm_partial_api_{}", Uuid::new_v4().simple());
    db.execute(
        "INSERT INTO public.novu_rust_environments VALUES($1,'org'),($2,'org')",
        &[&env, &other],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) VALUES('https://issuer.test','alice','console',$1),('https://issuer.test','alice','console',$2)",&[&env,&other]).await.unwrap();
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_fcm_partial_health(text,text,text,text,bigint,integer) TO {role}")).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_operator_jwt_api_logins SELECT rolname,oid FROM pg_roles WHERE rolname::text=$1",&[&role]).await.unwrap();
    let api = connect(&url).await;
    api.batch_execute(&format!("SET SESSION AUTHORIZATION {role}"))
        .await
        .unwrap();
    let app = novu_rust_api::router(Arc::new(api)).layer(Extension(Arc::new(
        AccessTokenVerifier::new("https://issuer.test", "swallow", &[("key", PUBLIC)]).unwrap(),
    )));
    let token = jwt("alice");
    let path = format!(
        "/v1/experimental/operator/fcm-partial-health?environmentId={env}&windowSeconds=60"
    );
    assert_eq!(
        response(app.clone(), &token, "GET", &path, None).await.0,
        StatusCode::UNAUTHORIZED
    );
    db.execute("INSERT INTO public.novu_rust_operator_jwt_fcm_partial_health_grants VALUES('https://issuer.test','alice','console',$1)",&[&env]).await.unwrap();
    assert_eq!(
        response(app.clone(), &token, "GET", &path, None).await.0,
        StatusCode::UNAUTHORIZED
    );
    db.execute("INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT $1,$2,'alert_view',oid FROM pg_roles WHERE rolname::text=$2",&[&env,&role]).await.unwrap();
    let job = Uuid::new_v4();
    let other_job = Uuid::new_v4();
    db.execute("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status) VALUES($1,$3,'experimental_fcm_push','{}','completed'),($2,$4,'experimental_fcm_push','{}','completed')",&[&job,&other_job,&env,&other]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_fcm_attempts(job_id,environment_id,target_index,state) VALUES($1,$3,0,'accepted'),($1,$3,1,'rejected'),($2,$4,0,'accepted'),($2,$4,1,'rejected')",&[&job,&other_job,&env,&other]).await.unwrap();
    let (status, value) = response(app.clone(), &token, "GET", &path, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value, json!({"partialJobs":1}));
    for invalid in ["0", "604801", "01"] {
        let url = format!(
            "/v1/experimental/operator/fcm-partial-health?environmentId={env}&windowSeconds={invalid}"
        );
        assert_eq!(
            response(app.clone(), &token, "GET", &url, None).await.0,
            StatusCode::BAD_REQUEST
        );
    }
    let otherpath = format!(
        "/v1/experimental/operator/fcm-partial-health?environmentId={other}&windowSeconds=60"
    );
    assert_eq!(
        response(app.clone(), &token, "GET", &otherpath, None)
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    db.execute("DELETE FROM public.novu_rust_delivery_operator_grants WHERE environment_id=$1 AND database_principal=$2 AND decision='alert_view'",&[&env,&role]).await.unwrap();
    assert_eq!(
        response(app.clone(), &token, "GET", &path, None).await.0,
        StatusCode::UNAUTHORIZED
    );
    db.execute("INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT $1,$2,'alert_view',oid FROM pg_roles WHERE rolname::text=$2",&[&env,&role]).await.unwrap();
    db.execute("DELETE FROM public.novu_rust_operator_jwt_fcm_partial_health_grants WHERE environment_id=$1",&[&env]).await.unwrap();
    assert_eq!(
        response(app, &token, "GET", &path, None).await.0,
        StatusCode::UNAUTHORIZED
    );
}
