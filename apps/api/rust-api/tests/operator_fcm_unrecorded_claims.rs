//! Operator FCM unrecorded claim diagnostics against a disposable database.
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
async fn fcm_claim_diagnostics_require_independent_grants_and_preserve_pagination() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let db = connect(&url).await;
    let env = format!("jwt-claims-{}", Uuid::new_v4());
    let other = format!("jwt-other-{}", Uuid::new_v4());
    let role = format!("fcm_claim_api_{}", Uuid::new_v4().simple());
    db.execute(
        "INSERT INTO public.novu_rust_environments VALUES($1,'org'),($2,'org')",
        &[&env, &other],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) VALUES ('https://issuer.test','alice','console',$1),('https://issuer.test','alice','console',$2)", &[&env,&other]).await.unwrap();
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_fcm_unrecorded_claims(text,text,text,text,bigint,integer,integer,boolean,uuid) TO {role}")).await.unwrap();
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
    let health = format!(
        "/v1/experimental/operator/fcm-unrecorded-claims-health?environmentId={env}&minGap=2&graceSeconds=60"
    );
    let list = format!(
        "/v1/experimental/operator/fcm-unrecorded-claims?environmentId={env}&minGap=2&graceSeconds=60"
    );
    assert_eq!(
        response(app.clone(), &token, "GET", &health, None).await.0,
        StatusCode::UNAUTHORIZED
    );
    db.execute("INSERT INTO public.novu_rust_operator_jwt_fcm_unrecorded_claim_grants VALUES('https://issuer.test','alice','console',$1,true,false)", &[&env]).await.unwrap();
    assert_eq!(
        response(app.clone(), &token, "GET", &health, None).await.0,
        StatusCode::UNAUTHORIZED
    );
    db.execute("INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT $1,rolname,'alert_view',oid FROM pg_roles WHERE rolname::text=$2", &[&env,&role]).await.unwrap();
    assert_eq!(
        response(app.clone(), &token, "GET", &health, None).await.1["unrecordedClaimJobs"],
        0
    );
    assert_eq!(
        response(app.clone(), &token, "GET", &list, None).await.0,
        StatusCode::UNAUTHORIZED
    );
    db.execute("UPDATE public.novu_rust_operator_jwt_fcm_unrecorded_claim_grants SET can_list=true WHERE environment_id=$1", &[&env]).await.unwrap();
    let other_list = format!(
        "/v1/experimental/operator/fcm-unrecorded-claims?environmentId={other}&minGap=2&graceSeconds=60"
    );
    assert_eq!(
        response(app.clone(), &token, "GET", &other_list, None)
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );

    db.execute("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status,attempts,available_at) SELECT gen_random_uuid(),$1,'experimental_fcm_push','{\"secret\":\"fixture\"}','pending',3,clock_timestamp()-interval '2 minutes' FROM generate_series(1,101)", &[&env]).await.unwrap();
    let (status, first) = response(app.clone(), &token, "GET", &list, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(first["data"].as_array().unwrap().len(), 100);
    assert_eq!(first["hasMore"], true);
    assert!(!first.to_string().contains("fixture"));
    let next = first["next"].as_str().unwrap();
    let (status, last) = response(
        app.clone(),
        &token,
        "GET",
        &format!("{list}&after={next}"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(last["data"].as_array().unwrap().len(), 1);
    assert_eq!(last["hasMore"], false);
    assert_eq!(
        response(
            app.clone(),
            &token,
            "GET",
            &format!("{list}&after=bad"),
            None
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        response(
            app.clone(),
            &token,
            "GET",
            &format!("{health}&minGap=2"),
            None
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    db.execute("UPDATE public.novu_rust_operator_jwt_fcm_unrecorded_claim_grants SET can_health=false WHERE environment_id=$1", &[&env]).await.unwrap();
    assert_eq!(
        response(app.clone(), &token, "GET", &health, None).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        response(app.clone(), &token, "GET", &list, None).await.0,
        StatusCode::OK
    );
    db.execute("DELETE FROM public.novu_rust_delivery_operator_grants WHERE environment_id=$1 AND database_principal=$2 AND decision='alert_view'", &[&env,&role]).await.unwrap();
    assert_eq!(
        response(app.clone(), &token, "GET", &list, None).await.0,
        StatusCode::UNAUTHORIZED
    );
}
