//! Discord destination retry policy JWT API through restricted API login and disposable PostgreSQL.
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
    path::Path,
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

async fn migrate(db: &Client, dir: &Path, range: std::ops::RangeInclusive<u32>) {
    let mut files: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "sql"))
        .collect();
    files.sort();
    for file in files {
        let name = file.file_name().unwrap().to_str().unwrap();
        if range.contains(&name[..4].parse::<u32>().unwrap()) {
            db.batch_execute(&std::fs::read_to_string(&file).unwrap())
                .await
                .unwrap_or_else(|e| panic!("{name}: {e}"));
        }
    }
}
#[tokio::test]
#[ignore = "requires fresh disposable PostgreSQL and role creation"]
async fn scoped_retry_policy_uses_both_grants_and_audits_atomically() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let db = connect(&url).await;
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    migrate(&db, &root.join("migrations"), 1..=17).await;
    migrate(
        &db,
        &root.join("../../worker/rust-queue/migrations"),
        1..=u32::MAX,
    )
    .await;
    migrate(&db, &root.join("migrations"), 18..=u32::MAX).await;
    let env = format!("jwt-retry-policy-{}", Uuid::new_v4());
    let other = format!("jwt-retry-other-{}", Uuid::new_v4());
    let role = format!("retry_policy_api_{}", Uuid::new_v4().simple());
    db.execute(
        "INSERT INTO public.novu_rust_environments VALUES($1,'org'),($2,'org')",
        &[&env, &other],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO public.novu_rust_discord_retry_policy(environment_id,destination_id,mode,max_retries,fallback_seconds) VALUES($1,'ops','off',3,60),($1,'other','off',3,60),($2,'ops','off',3,60)",&[&env,&other]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) VALUES('https://issuer.test','alice','console',$1),('https://issuer.test','bob','console',$1),('https://issuer.test','alice','console',$2)",&[&env,&other]).await.unwrap();
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_discord_retry_policy_read(text,text,text,text,bigint,text),public.novu_rust_operator_jwt_discord_retry_policy_change(text,text,text,text,bigint,text,uuid,bigint,text,integer,integer,text),public.novu_rust_operator_jwt_discord_retry_policy_request(text,text,text,text,bigint,text,uuid),public.novu_rust_operator_jwt_discord_retry_policy_audit_read(text,text,text,text,bigint,text,uuid) TO {role}")).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_operator_jwt_api_logins SELECT rolname,oid FROM pg_roles WHERE rolname::text=$1",&[&role]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_discord_retry_policy_admins(environment_id,destination_id,database_principal,principal_oid,can_read,can_write,can_audit) SELECT $1,'ops',rolname,oid,false,false,false FROM pg_roles WHERE rolname::text=$2",&[&env,&role]).await.unwrap();
    let api = connect(&url).await;
    api.batch_execute(&format!("SET SESSION AUTHORIZATION {role}"))
        .await
        .unwrap();
    assert!(
        api.query("SELECT * FROM public.novu_rust_discord_retry_policy", &[])
            .await
            .is_err()
    );
    let app = novu_rust_api::router(Arc::new(api)).layer(Extension(Arc::new(
        AccessTokenVerifier::new("https://issuer.test", "swallow", &[("key", PUBLIC)]).unwrap(),
    )));
    let api2 = connect(&url).await;
    api2.batch_execute(&format!("SET SESSION AUTHORIZATION {role}"))
        .await
        .unwrap();
    let app2 = novu_rust_api::router(Arc::new(api2)).layer(Extension(Arc::new(
        AccessTokenVerifier::new("https://issuer.test", "swallow", &[("key", PUBLIC)]).unwrap(),
    )));
    let alice = jwt("alice");
    let bob = jwt("bob");
    let path = format!(
        "/v1/experimental/operator/discord-retry-policy?environmentId={env}&destinationId=ops"
    );
    assert_eq!(
        response(app.clone(), &alice, "GET", &path, None).await.0,
        StatusCode::UNAUTHORIZED
    );
    db.execute("INSERT INTO public.novu_rust_operator_jwt_discord_retry_policy_grants VALUES('https://issuer.test','alice','console',$1,'ops',true,true,false)",&[&env]).await.unwrap();
    assert_eq!(
        response(app.clone(), &alice, "GET", &path, None).await.0,
        StatusCode::UNAUTHORIZED
    );
    db.execute("UPDATE public.novu_rust_discord_retry_policy_admins SET can_read=true WHERE environment_id=$1 AND destination_id='ops' AND database_principal::text=$2",&[&env,&role]).await.unwrap();
    assert_eq!(
        response(app.clone(), &alice, "GET", &path, None).await.1,
        json!({"mode":"off","maxRetries":3,"fallbackSeconds":60,"revision":"0"})
    );
    assert_eq!(
        response(
            app.clone(),
            &alice,
            "GET",
            &path.replace("destinationId=ops", "destinationId=other"),
            None
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        response(
            app.clone(),
            &alice,
            "GET",
            &path.replace(&env, &other),
            None
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
    let id = Uuid::new_v4();
    let change = json!({"requestId":id,"expectedRevision":"0","mode":"known","maxRetries":2,"fallbackSeconds":30,"reason":"ticket"});
    assert_eq!(
        response(app.clone(), &alice, "PUT", &path, Some(change.clone()))
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    db.execute("UPDATE public.novu_rust_discord_retry_policy_admins SET can_write=true WHERE environment_id=$1 AND destination_id='ops' AND database_principal::text=$2",&[&env,&role]).await.unwrap();
    let (a, b) = tokio::join!(
        response(app.clone(), &alice, "PUT", &path, Some(change.clone())),
        response(app2.clone(), &alice, "PUT", &path, Some(change.clone()))
    );
    assert_eq!(a.0, StatusCode::OK);
    assert_eq!(b.0, StatusCode::OK);
    assert_ne!(a.1["replayed"], b.1["replayed"]);
    assert_eq!(a.1["policy"]["revision"], "1");
    let mut altered = change.clone();
    altered["reason"] = json!("other");
    assert_eq!(
        response(app.clone(), &alice, "PUT", &path, Some(altered))
            .await
            .0,
        StatusCode::CONFLICT
    );
    let request = format!(
        "/v1/experimental/operator/discord-retry-policy/request?environmentId={env}&destinationId=ops&requestId={id}"
    );
    assert_eq!(
        response(app.clone(), &alice, "GET", &request, None).await.1["found"],
        true
    );
    assert_eq!(
        response(app.clone(), &bob, "GET", &request, None).await.0,
        StatusCode::UNAUTHORIZED
    );
    let audit = format!(
        "/v1/experimental/operator/discord-retry-policy/audit?environmentId={env}&destinationId=ops"
    );
    assert_eq!(
        response(app.clone(), &alice, "GET", &audit, None).await.0,
        StatusCode::UNAUTHORIZED
    );
    db.execute("UPDATE public.novu_rust_operator_jwt_discord_retry_policy_grants SET can_audit=true WHERE environment_id=$1 AND subject='alice'",&[&env]).await.unwrap();
    assert_eq!(
        response(app.clone(), &alice, "GET", &audit, None).await.0,
        StatusCode::UNAUTHORIZED
    );
    db.execute("UPDATE public.novu_rust_discord_retry_policy_admins SET can_audit=true WHERE environment_id=$1 AND destination_id='ops' AND database_principal::text=$2",&[&env,&role]).await.unwrap();
    assert_eq!(
        response(app.clone(), &alice, "GET", &audit, None).await.1["data"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let stale = json!({"requestId":Uuid::new_v4(),"expectedRevision":"0","mode":"off","maxRetries":1,"fallbackSeconds":60,"reason":"stale"});
    assert_eq!(
        response(app.clone(), &alice, "PUT", &path, Some(stale))
            .await
            .0,
        StatusCode::CONFLICT
    );
    let malformed = json!({"requestId":Uuid::new_v4(),"expectedRevision":"01","mode":"off","maxRetries":1,"fallbackSeconds":60,"reason":"bad"});
    assert_eq!(
        response(app.clone(), &alice, "PUT", &path, Some(malformed))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    db.batch_execute("ALTER TABLE public.novu_rust_operator_jwt_discord_retry_policy_audit ADD CONSTRAINT fixture_audit_failure CHECK (reason<>'force rollback') NOT VALID").await.unwrap();
    let failure_id = Uuid::new_v4();
    let failed = json!({"requestId":failure_id,"expectedRevision":"1","mode":"fallback","maxRetries":2,"fallbackSeconds":60,"reason":"force rollback"});
    assert_eq!(
        response(app.clone(), &alice, "PUT", &path, Some(failed))
            .await
            .0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    db.batch_execute("ALTER TABLE public.novu_rust_operator_jwt_discord_retry_policy_audit DROP CONSTRAINT fixture_audit_failure").await.unwrap();
    assert_eq!(db.query_one("SELECT revision FROM public.novu_rust_discord_retry_policy WHERE environment_id=$1 AND destination_id='ops'",&[&env]).await.unwrap().get::<_,i64>(0),1);
    assert_eq!(db.query_one("SELECT count(*) FROM public.novu_rust_discord_retry_policy_audit WHERE environment_id=$1 AND destination_id='ops'",&[&env]).await.unwrap().get::<_,i64>(0),1);
    let noop = json!({"requestId":Uuid::new_v4(),"expectedRevision":"1","mode":"known","maxRetries":2,"fallbackSeconds":30,"reason":"no change"});
    assert_eq!(
        response(app.clone(), &alice, "PUT", &path, Some(noop))
            .await
            .1["policy"]["revision"],
        "1"
    );
    db.execute("UPDATE public.novu_rust_discord_retry_policy_admins SET can_write=false WHERE environment_id=$1 AND destination_id='ops' AND database_principal::text=$2",&[&env,&role]).await.unwrap();
    assert_eq!(
        response(app, &alice, "GET", &request, None).await.0,
        StatusCode::UNAUTHORIZED
    );
}
