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
#[ignore = "requires disposable PostgreSQL and role creation"]
async fn jwt_cooldown_admin_enforces_both_grants_and_audits() {
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
    let env = format!("cooldown-{}", Uuid::new_v4());
    let role = format!("cooldown_api_{}", Uuid::new_v4().simple());
    db.execute(
        "INSERT INTO public.novu_rust_environments VALUES($1,'org')",
        &[&env],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO public.novu_rust_discord_destinations VALUES($1,'ops','hash'),($1,'other','hash')",&[&env]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) VALUES('https://issuer.test','alice','console',$1),('https://issuer.test','bob','console',$1)",&[&env]).await.unwrap();
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_discord_cooldown_read(text,text,text,text,bigint,text),public.novu_rust_operator_jwt_discord_cooldown_change(text,text,text,text,bigint,text,uuid,bigint,boolean,text),public.novu_rust_operator_jwt_discord_cooldown_request(text,text,text,text,bigint,text,uuid),public.novu_rust_operator_jwt_discord_cooldown_audit_read(text,text,text,text,bigint,text,uuid) TO {role}")).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_operator_jwt_api_logins SELECT rolname,oid FROM pg_roles WHERE rolname::text=$1",&[&role]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_discord_cooldown_admins(environment_id,destination_id,database_principal,principal_oid,can_read,can_write,can_audit) SELECT $1,'ops',rolname,oid,false,false,false FROM pg_roles WHERE rolname::text=$2",&[&env,&role]).await.unwrap();
    let api = connect(&url).await;
    api.batch_execute(&format!("SET SESSION AUTHORIZATION {role}"))
        .await
        .unwrap();
    let app = novu_rust_api::router(Arc::new(api)).layer(Extension(Arc::new(
        AccessTokenVerifier::new("https://issuer.test", "swallow", &[("key", PUBLIC)]).unwrap(),
    )));
    let alice = jwt("alice");
    let bob = jwt("bob");
    let path =
        format!("/v1/experimental/operator/discord-cooldown?environmentId={env}&destinationId=ops");
    assert_eq!(
        response(app.clone(), &alice, "GET", &path, None).await.0,
        StatusCode::UNAUTHORIZED
    );
    db.execute("INSERT INTO public.novu_rust_operator_jwt_discord_cooldown_grants VALUES('https://issuer.test','alice','console',$1,'ops',true,true,true)",&[&env]).await.unwrap();
    assert_eq!(
        response(app.clone(), &alice, "GET", &path, None).await.0,
        StatusCode::UNAUTHORIZED
    );
    db.execute("UPDATE public.novu_rust_discord_cooldown_admins SET can_read=true,can_write=true,can_audit=true WHERE environment_id=$1 AND database_principal::text=$2",&[&env,&role]).await.unwrap();
    assert_eq!(
        response(app.clone(), &alice, "GET", &path, None).await.1,
        json!({"enabled":false,"revision":"0","notBefore":null})
    );
    assert_eq!(
        response(app.clone(), &bob, "GET", &path, None).await.0,
        StatusCode::UNAUTHORIZED
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
    let request = Uuid::new_v4().to_string();
    let body =
        json!({"requestId":request,"expectedRevision":"0","enabled":true,"reason":"reviewed"});
    let (status, result) = response(app.clone(), &alice, "PUT", &path, Some(body.clone())).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(result["policy"], json!({"enabled":true,"revision":"1"}));
    assert_eq!(
        response(app.clone(), &alice, "PUT", &path, Some(body.clone()))
            .await
            .1["replayed"],
        true
    );
    let mut conflict = body.clone();
    conflict["enabled"] = false.into();
    assert_eq!(
        response(app.clone(), &alice, "PUT", &path, Some(conflict))
            .await
            .0,
        StatusCode::CONFLICT
    );
    let lookup = format!(
        "/v1/experimental/operator/discord-cooldown/request?environmentId={env}&destinationId=ops&requestId={request}"
    );
    assert_eq!(
        response(app.clone(), &alice, "GET", &lookup, None).await.1["found"],
        true
    );
    let audit = format!(
        "/v1/experimental/operator/discord-cooldown/audit?environmentId={env}&destinationId=ops"
    );
    let rows = response(app.clone(), &alice, "GET", &audit, None).await.1;
    assert_eq!(rows["data"][0]["actorSubject"], "alice");
    let stale = json!({"requestId":Uuid::new_v4().to_string(),"expectedRevision":"0","enabled":false,"reason":"stale"});
    assert_eq!(
        response(app.clone(), &alice, "PUT", &path, Some(stale))
            .await
            .0,
        StatusCode::CONFLICT
    );
    let invalid = json!({"requestId":Uuid::new_v4().to_string(),"expectedRevision":"01","enabled":false,"reason":"invalid"});
    assert_eq!(
        response(app.clone(), &alice, "PUT", &path, Some(invalid))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    db.batch_execute("ALTER TABLE public.novu_rust_operator_jwt_discord_cooldown_audit ADD CONSTRAINT test_audit_failure CHECK (reason<>'force rollback') NOT VALID").await.unwrap();
    let failed = json!({"requestId":Uuid::new_v4().to_string(),"expectedRevision":"1","enabled":false,"reason":"force rollback"});
    assert_eq!(
        response(app.clone(), &alice, "PUT", &path, Some(failed))
            .await
            .0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(
        response(app.clone(), &alice, "GET", &path, None).await.1["revision"],
        "1"
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM public.novu_rust_discord_cooldown_audit WHERE environment_id=$1",
            &[&env]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        1
    );
    db.execute("UPDATE public.novu_rust_operator_jwt_discord_cooldown_grants SET can_write=false WHERE environment_id=$1 AND subject='alice'",&[&env]).await.unwrap();
    assert_eq!(
        response(app.clone(), &alice, "PUT", &path, Some(body))
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
}
