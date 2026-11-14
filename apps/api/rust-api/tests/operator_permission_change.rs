//! JWT permission mutation, optimistic concurrency and audit on disposable PostgreSQL.
use axum::{
    body::Body,
    http::{Request, StatusCode, header},
};
use http_body_util::BodyExt;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use novu_rust_api::operator_access_token::AccessTokenVerifier;
use serde_json::{Value, json};
use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
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
fn token(subject: &str) -> String {
    token_with_ttl(subject, 120)
}
fn token_with_ttl(subject: &str, ttl: u64) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let mut h = Header::new(Algorithm::RS256);
    h.typ = Some("at+jwt".into());
    h.kid = Some("key".into());
    encode(&h,&json!({"iss":"https://issuer.test","sub":subject,"aud":"swallow","exp":now+ttl,"iat":now-1,"jti":"fixture","client_id":"console"}),&EncodingKey::from_rsa_pem(PRIVATE).unwrap()).unwrap()
}
fn put(token: &str, value: &Value) -> Request<Body> {
    Request::builder()
        .method("PUT")
        .uri("/v1/experimental/operator/permissions?environmentId=change-a")
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(value.to_string()))
        .unwrap()
}
fn get(token: &str, id: Uuid) -> Request<Body> {
    Request::builder()
        .uri(format!(
            "/v1/experimental/operator/permissions/request?environmentId=change-a&requestId={id}"
        ))
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap()
}
async fn response(response: axum::response::Response) -> (StatusCode, Value) {
    let status = response.status();
    assert_eq!(
        response.headers().get(header::CACHE_CONTROL).unwrap(),
        "no-store"
    );
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap())
}
#[tokio::test]
#[ignore = "requires fully migrated disposable PostgreSQL and role creation"]
async fn change_is_scoped_compare_and_set_idempotent_and_audited() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let mut db = connect(&url).await;
    let role = format!("permission_api_{}", Uuid::new_v4().simple());
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_change_permissions(text,text,text,text,bigint,uuid,text,text,text,jsonb,jsonb,text),public.novu_rust_operator_jwt_permission_change_request(text,text,text,text,bigint,uuid),public.novu_rust_operator_jwt_review_audit(text,text,text,text,bigint,bigint) TO {role}; INSERT INTO public.novu_rust_environments VALUES('change-a','org'),('change-b','org'); INSERT INTO public.novu_rust_operator_jwt_api_logins SELECT rolname,oid FROM pg_roles WHERE rolname='{role}'; INSERT INTO public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) VALUES('https://issuer.test','admin','console','change-a'),('https://issuer.test','viewer','console','change-a'); INSERT INTO public.novu_rust_operator_jwt_permission_review_grants(issuer,subject,client_id,environment_id,permission_write,audit_read) VALUES('https://issuer.test','admin','console','change-a',true,true),('https://issuer.test','viewer','console','change-a',false,false);")).await.unwrap();
    let api_url = url.replacen("postgres://postgres@", &format!("postgres://{role}@"), 1);
    let verifier = Arc::new(
        AccessTokenVerifier::new("https://issuer.test", "swallow", &[("key", PUBLIC)]).unwrap(),
    );
    let app = novu_rust_api::router(Arc::new(connect(&api_url).await))
        .layer(axum::Extension(verifier.clone()));
    let app2 =
        novu_rust_api::router(Arc::new(connect(&api_url).await)).layer(axum::Extension(verifier));
    let admin = token("admin");
    let viewer = token("viewer");
    let id = Uuid::new_v4();
    let desired = json!({"enabled":true,"subscriberRead":true,"alertRead":false,"alertRetry":false,"alertAccepted":false});
    let input = json!({"requestId":id,"targetIssuer":"https://issuer.test","targetSubject":"target","targetClientId":"console","expected":null,"desired":desired,"reason":"ticket"});
    assert_eq!(
        app.clone()
            .oneshot(put(&viewer, &input))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    let (a, b) = tokio::join!(
        app.clone().oneshot(put(&admin, &input)),
        app2.clone().oneshot(put(&admin, &input))
    );
    let (sa, ra) = response(a.unwrap()).await;
    let (sb, rb) = response(b.unwrap()).await;
    assert_eq!(sa, StatusCode::OK);
    assert_eq!(sb, StatusCode::OK);
    assert_eq!(ra["changed"], true);
    assert_eq!(rb["changed"], true);
    assert_ne!(ra["replayed"], rb["replayed"]);
    let audit=db.query_one("SELECT count(*),min(jwt_subject),min(reason) FROM public.novu_rust_operator_jwt_permission_audit WHERE environment_id='change-a'",&[]).await.unwrap();
    assert_eq!(audit.get::<_, i64>(0), 1);
    assert_eq!(audit.get::<_, String>(1), "admin");
    assert_eq!(audit.get::<_, String>(2), "ticket");
    let audit_request = Request::builder()
        .uri("/v1/experimental/operator/permissions/audit?environmentId=change-a")
        .header(header::AUTHORIZATION, format!("Bearer {admin}"))
        .body(Body::empty())
        .unwrap();
    let (status, audit_response) =
        response(app.clone().oneshot(audit_request).await.unwrap()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(audit_response["data"][0]["jwtSubject"], "admin");
    assert_eq!(audit_response["data"][0]["requestId"], id.to_string());
    let (status, body) = response(app.clone().oneshot(get(&admin, id)).await.unwrap()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["found"], true);
    assert_eq!(body["targetSubject"], "target");
    let mut noop = input.clone();
    noop["requestId"] = json!(Uuid::new_v4());
    noop["expected"] = desired.clone();
    let (status, result) = response(app.clone().oneshot(put(&admin, &noop)).await.unwrap()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(result["changed"], false);
    assert_eq!(db.query_one("SELECT count(*) FROM public.novu_rust_operator_jwt_permission_audit WHERE environment_id='change-a'",&[]).await.unwrap().get::<_,i64>(0),1);
    let mut conflict = input.clone();
    conflict["reason"] = json!("different");
    assert_eq!(
        app.clone()
            .oneshot(put(&admin, &conflict))
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
    let mut stale = input.clone();
    stale["requestId"] = json!(Uuid::new_v4());
    assert_eq!(
        app.clone()
            .oneshot(put(&admin, &stale))
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
    let mut self_change = input.clone();
    self_change["requestId"] = json!(Uuid::new_v4());
    self_change["targetSubject"] = json!("admin");
    assert_eq!(
        app.clone()
            .oneshot(put(&admin, &self_change))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    let mut malformed = input.clone();
    malformed["requestId"] = json!(Uuid::new_v4());
    malformed["desired"]["unexpected"] = json!(true);
    assert_eq!(
        app.clone()
            .oneshot(put(&admin, &malformed))
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
    // Failure of the audit insert rolls the target and request record back together.
    db.batch_execute("CREATE FUNCTION public.reject_jwt_permission_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture'; END $$; CREATE TRIGGER reject_jwt_permission_audit BEFORE INSERT ON public.novu_rust_operator_jwt_permission_audit FOR EACH ROW EXECUTE FUNCTION public.reject_jwt_permission_audit()").await.unwrap();
    let failed = Uuid::new_v4();
    let mut update = input.clone();
    update["requestId"] = json!(failed);
    update["expected"] = desired.clone();
    update["desired"]["enabled"] = json!(false);
    assert_eq!(
        app.clone()
            .oneshot(put(&admin, &update))
            .await
            .unwrap()
            .status(),
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert!(db.query_one("SELECT enabled FROM public.novu_rust_operator_jwt_identities WHERE environment_id='change-a' AND subject='target'",&[]).await.unwrap().get::<_,bool>(0));
    assert_eq!(db.query_one("SELECT count(*) FROM public.novu_rust_operator_jwt_permission_change_requests WHERE request_id=$1",&[&failed]).await.unwrap().get::<_,i64>(0),0);
    db.batch_execute("DROP TRIGGER reject_jwt_permission_audit ON public.novu_rust_operator_jwt_permission_audit; DROP FUNCTION public.reject_jwt_permission_audit()").await.unwrap();
    let tx = db.transaction().await.unwrap();
    tx.execute("UPDATE public.novu_rust_operator_jwt_permission_review_grants SET permission_write=false WHERE subject='admin'",&[]).await.unwrap();
    let app3 = app.clone();
    let key = admin.clone();
    let pending_update = update.clone();
    let mut waiting = tokio::spawn(async move {
        app3.oneshot(put(&key, &pending_update))
            .await
            .unwrap()
            .status()
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(200), &mut waiting)
            .await
            .is_err()
    );
    tx.commit().await.unwrap();
    assert_eq!(waiting.await.unwrap(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        app.clone().oneshot(get(&admin, id)).await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
    db.execute("UPDATE public.novu_rust_operator_jwt_permission_review_grants SET permission_write=true WHERE subject='admin'",&[]).await.unwrap();
    let tx = db.transaction().await.unwrap();
    tx.execute("UPDATE public.novu_rust_operator_jwt_identities SET enabled=enabled WHERE subject='target' AND environment_id='change-a'",&[]).await.unwrap();
    let app3 = app.clone();
    let short = token_with_ttl("admin", 3);
    let mut waiting =
        tokio::spawn(async move { app3.oneshot(put(&short, &update)).await.unwrap().status() });
    assert!(
        tokio::time::timeout(Duration::from_millis(200), &mut waiting)
            .await
            .is_err()
    );
    tokio::time::sleep(Duration::from_secs(4)).await;
    tx.commit().await.unwrap();
    assert_eq!(waiting.await.unwrap(), StatusCode::UNAUTHORIZED);
    assert_eq!(db.query_one("SELECT count(*) FROM public.novu_rust_operator_jwt_permission_change_requests WHERE request_id=$1",&[&failed]).await.unwrap().get::<_,i64>(0),0);
}
