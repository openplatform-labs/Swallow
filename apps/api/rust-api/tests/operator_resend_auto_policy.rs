//! Operator Resend auto-policy API against a disposable database.
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
async fn policy_api_is_scoped_replayable_and_audited_per_operator() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let db = connect(&url).await;
    let env = format!("jwt-policy-{}", Uuid::new_v4());
    let role = format!("jwt_policy_{}", Uuid::new_v4().simple());
    db.execute(
        "INSERT INTO public.novu_rust_environments VALUES ($1,'org')",
        &[&env],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) VALUES ('https://issuer.test','alice','console',$1),('https://issuer.test','bob','console',$1)", &[&env]).await.unwrap();
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_resend_auto_policy_read(text,text,text,text,bigint),public.novu_rust_operator_jwt_resend_auto_policy_change(text,text,text,text,bigint,uuid,bigint,boolean,integer,text,text),public.novu_rust_operator_jwt_resend_auto_policy_request(text,text,text,text,bigint,uuid),public.novu_rust_operator_jwt_resend_auto_policy_audit_read(text,text,text,text,bigint,uuid) TO {role}")).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_operator_jwt_api_logins SELECT rolname,oid FROM pg_roles WHERE rolname::text=$1", &[&role]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_resend_auto_policy_admins(environment_id,database_principal,principal_oid,can_read,can_write) SELECT $1,$2,oid,true,false FROM pg_roles WHERE rolname::text=$2", &[&env,&role]).await.unwrap();
    let api = connect(&url).await;
    api.batch_execute(&format!("SET SESSION AUTHORIZATION {role}"))
        .await
        .unwrap();
    assert!(
        api.query(
            "SELECT * FROM public.novu_rust_operator_jwt_resend_auto_policy_requests",
            &[]
        )
        .await
        .is_err()
    );
    let app = novu_rust_api::router(Arc::new(api)).layer(Extension(Arc::new(
        AccessTokenVerifier::new("https://issuer.test", "swallow", &[("key", PUBLIC)]).unwrap(),
    )));
    let alice = jwt("alice");
    let bob = jwt("bob");
    let path = format!("/v1/experimental/operator/resend-auto-policy?environmentId={env}");
    assert_eq!(
        response(app.clone(), &alice, "GET", &path, None).await.0,
        StatusCode::UNAUTHORIZED
    );
    db.execute("INSERT INTO public.novu_rust_operator_jwt_resend_auto_policy_grants VALUES('https://issuer.test','alice','console',$1,true,false,false)", &[&env]).await.unwrap();
    assert_eq!(
        response(app.clone(), &alice, "GET", &path, None).await.1,
        json!({"exists":false})
    );
    let id = Uuid::new_v4();
    let body = json!({"requestId":id,"expectedRevision":"-1","enabled":true,"maxRetries":3,"cutoff":"now","reason":"ticket"});
    assert_eq!(
        response(app.clone(), &alice, "PUT", &path, Some(body.clone()))
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    db.execute("UPDATE public.novu_rust_operator_jwt_resend_auto_policy_grants SET can_write=true WHERE environment_id=$1 AND subject='alice'", &[&env]).await.unwrap();
    assert_eq!(
        response(app.clone(), &alice, "PUT", &path, Some(body.clone()))
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    db.execute("UPDATE public.novu_rust_resend_auto_policy_admins SET can_write=true WHERE environment_id=$1 AND database_principal=$2", &[&env,&role]).await.unwrap();
    let (one, two) = tokio::join!(
        response(app.clone(), &alice, "PUT", &path, Some(body.clone())),
        response(app.clone(), &alice, "PUT", &path, Some(body.clone()))
    );
    assert_eq!(one.0, StatusCode::OK);
    assert_eq!(two.0, StatusCode::OK);
    assert_ne!(one.1["replayed"], two.1["replayed"]);
    assert_eq!(one.1["policy"]["revision"], "0");
    let mut altered = body.clone();
    altered["reason"] = json!("different");
    assert_eq!(
        response(app.clone(), &alice, "PUT", &path, Some(altered))
            .await
            .0,
        StatusCode::CONFLICT
    );
    let req = format!(
        "/v1/experimental/operator/resend-auto-policy/request?environmentId={env}&requestId={id}"
    );
    assert_eq!(
        response(app.clone(), &alice, "GET", &req, None).await.1["found"],
        true
    );
    assert_eq!(
        response(app.clone(), &bob, "GET", &req, None).await.0,
        StatusCode::UNAUTHORIZED
    );
    db.execute("INSERT INTO public.novu_rust_operator_jwt_resend_auto_policy_grants VALUES('https://issuer.test','bob','console',$1,false,true,false)", &[&env]).await.unwrap();
    assert_eq!(
        response(app.clone(), &bob, "GET", &req, None).await.1["found"],
        false
    );
    let bob_body = json!({"requestId":id,"expectedRevision":"0","enabled":false,"maxRetries":2,"cutoff":"preserve","reason":"bob ticket"});
    assert_eq!(
        response(app.clone(), &bob, "PUT", &path, Some(bob_body))
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(
        response(app.clone(), &alice, "GET", &req, None).await.1["policy"]["revision"],
        "0"
    );
    assert_eq!(
        response(app.clone(), &bob, "GET", &req, None).await.1["policy"]["revision"],
        "1"
    );
    let audit = format!("/v1/experimental/operator/resend-auto-policy/audit?environmentId={env}");
    assert_eq!(
        response(app.clone(), &alice, "GET", &audit, None).await.0,
        StatusCode::UNAUTHORIZED
    );
    db.execute("UPDATE public.novu_rust_operator_jwt_resend_auto_policy_grants SET can_audit=true WHERE environment_id=$1 AND subject='alice'", &[&env]).await.unwrap();
    let list = response(app.clone(), &alice, "GET", &audit, None).await;
    assert_eq!(list.0, StatusCode::OK);
    assert_eq!(list.1["data"].as_array().unwrap().len(), 2);
    db.execute("UPDATE public.novu_rust_resend_auto_policy_admins SET can_write=false WHERE environment_id=$1 AND database_principal=$2", &[&env,&role]).await.unwrap();
    assert_eq!(
        response(app.clone(), &alice, "GET", &req, None).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        response(app.clone(), &alice, "PUT", &path, Some(body.clone()))
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    db.execute("UPDATE public.novu_rust_resend_auto_policy_admins SET can_write=true WHERE environment_id=$1 AND database_principal=$2", &[&env,&role]).await.unwrap();
    let wrong = json!({"requestId":Uuid::new_v4(),"expectedRevision":"0","enabled":true,"maxRetries":3,"cutoff":"now","reason":"stale"});
    assert_eq!(
        response(app.clone(), &alice, "PUT", &path, Some(wrong))
            .await
            .0,
        StatusCode::CONFLICT
    );
    let malformed = json!({"requestId":Uuid::new_v4(),"expectedRevision":"01","enabled":true,"maxRetries":3,"cutoff":"now","reason":"ticket"});
    assert_eq!(
        response(app.clone(), &alice, "PUT", &path, Some(malformed))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    db.batch_execute("ALTER TABLE public.novu_rust_operator_jwt_resend_auto_policy_audit ADD CONSTRAINT fixture_audit_failure CHECK (reason<>'force rollback') NOT VALID").await.unwrap();
    let failure_id = Uuid::new_v4();
    let failed = json!({"requestId":failure_id,"expectedRevision":"1","enabled":true,"maxRetries":3,"cutoff":"now","reason":"force rollback"});
    assert_eq!(
        response(app.clone(), &alice, "PUT", &path, Some(failed))
            .await
            .0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    db.batch_execute("ALTER TABLE public.novu_rust_operator_jwt_resend_auto_policy_audit DROP CONSTRAINT fixture_audit_failure").await.unwrap();
    assert_eq!(db.query_one("SELECT revision FROM public.novu_rust_resend_auto_retry_policy WHERE environment_id=$1", &[&env]).await.unwrap().get::<_,i64>(0),1);
    assert_eq!(db.query_one("SELECT count(*) FROM public.novu_rust_resend_auto_policy_requests WHERE environment_id=$1", &[&env]).await.unwrap().get::<_,i64>(0),2);
    // A permission revocation waits for an authenticated change already
    // blocked behind the policy lock.
    let mut holder = connect(&url).await;
    let tx = holder.transaction().await.unwrap();
    tx.query_one("SELECT revision FROM public.novu_rust_resend_auto_retry_policy WHERE environment_id=$1 FOR UPDATE", &[&env]).await.unwrap();
    let pending_app = app.clone();
    let pending_token = alice.clone();
    let pending_path = path.clone();
    let pending = tokio::spawn(async move {
        response(pending_app,&pending_token,"PUT",&pending_path,Some(json!({"requestId":Uuid::new_v4(),"expectedRevision":"1","enabled":true,"maxRetries":3,"cutoff":"now","reason":"locked"}))).await
    });
    let mut waiting = false;
    for _ in 0..100 {
        if db.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE '%novu_rust_operator_jwt_resend_auto_policy_change%')",&[]).await.unwrap().get::<_,bool>(0){waiting=true;break;}
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(waiting);
    let revoker = connect(&url).await;
    let revoke_env = env.clone();
    let revoke = tokio::spawn(async move {
        revoker.execute("DELETE FROM public.novu_rust_operator_jwt_resend_auto_policy_grants WHERE environment_id=$1 AND subject='alice'", &[&revoke_env]).await.unwrap()
    });
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(!revoke.is_finished());
    tx.commit().await.unwrap();
    assert_eq!(pending.await.unwrap().0, StatusCode::OK);
    assert_eq!(revoke.await.unwrap(), 1);

    assert_eq!(
        response(app.clone(), &alice, "GET", &req, None).await.0,
        StatusCode::UNAUTHORIZED
    );
}
