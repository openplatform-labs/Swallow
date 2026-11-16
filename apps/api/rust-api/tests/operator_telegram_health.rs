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
async fn telegram_health_requires_identity_and_database_grants_and_isolated_environment() {
 let url=std::env::var("TEST_DATABASE_URL").unwrap();let db=connect(&url).await;let root=Path::new(env!("CARGO_MANIFEST_DIR"));
 migrate(&db,&root.join("migrations"),1..=17).await;migrate(&db,&root.join("../../worker/rust-queue/migrations"),1..=u32::MAX).await;migrate(&db,&root.join("migrations"),18..=u32::MAX).await;
 let env=format!("telegram-health-{}",Uuid::new_v4());let other=format!("other-{}",Uuid::new_v4());let role=format!("telegram_health_{}",Uuid::new_v4().simple());
 db.execute("INSERT INTO public.novu_rust_environments VALUES($1,'org'),($2,'org')",&[&env,&other]).await.unwrap();
 db.execute("INSERT INTO public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) VALUES('https://issuer.test','alice','console',$1),('https://issuer.test','bob','console',$1),('https://issuer.test','alice','console',$2)",&[&env,&other]).await.unwrap();
 db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_telegram_health(text,text,text,text,bigint) TO {role}")).await.unwrap();
 db.execute("INSERT INTO public.novu_rust_operator_jwt_api_logins SELECT rolname,oid FROM pg_roles WHERE rolname::text=$1",&[&role]).await.unwrap();
 let api=connect(&url).await;api.batch_execute(&format!("SET SESSION AUTHORIZATION {role}")).await.unwrap();assert!(api.query("SELECT * FROM public.novu_rust_telegram_attempts",&[]).await.is_err());
 let app=novu_rust_api::router(Arc::new(api)).layer(Extension(Arc::new(AccessTokenVerifier::new("https://issuer.test","swallow",&[("key",PUBLIC)]).unwrap())));let alice=jwt("alice");let bob=jwt("bob");let path=format!("/v1/experimental/operator/telegram-health?environmentId={env}");
 assert_eq!(response(app.clone(),&alice,"GET",&path,None).await.0,StatusCode::UNAUTHORIZED);
 db.execute("INSERT INTO public.novu_rust_operator_jwt_telegram_health_grants VALUES('https://issuer.test','alice','console',$1)",&[&env]).await.unwrap();
 assert_eq!(response(app.clone(),&alice,"GET",&path,None).await.0,StatusCode::UNAUTHORIZED);
 db.execute("INSERT INTO public.novu_rust_delivery_operator_grants SELECT $1,rolname,'alert_view',oid FROM pg_roles WHERE rolname::text=$2",&[&env,&role]).await.unwrap();
 assert_eq!(response(app.clone(),&alice,"GET",&path,None).await.1,json!({"unknownAttempts":0,"rejectedAttempts":0,"strandedDispatches":0,"preDispatchFailures":0}));
 assert_eq!(response(app.clone(),&bob,"GET",&path,None).await.0,StatusCode::UNAUTHORIZED);
 assert_eq!(response(app.clone(),&alice,"GET",&format!("/v1/experimental/operator/telegram-health?environmentId={other}"),None).await.0,StatusCode::UNAUTHORIZED);
 let id=Uuid::new_v4();db.execute("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status) VALUES($1,$2,'experimental_telegram_workflow','{\"destination\":\"private\"}','failed')",&[&id,&env]).await.unwrap();
 db.execute("INSERT INTO public.novu_rust_telegram_attempts(job_id,environment_id,state) VALUES($1,$2,'unknown')",&[&id,&env]).await.unwrap();
 let (status,value)=response(app.clone(),&alice,"GET",&path,None).await;assert_eq!(status,StatusCode::OK);assert_eq!(value["unknownAttempts"],1);assert!(!value.to_string().contains("private"));
 db.execute("DELETE FROM public.novu_rust_operator_jwt_telegram_health_grants WHERE environment_id=$1",&[&env]).await.unwrap();assert_eq!(response(app,&alice,"GET",&path,None).await.0,StatusCode::UNAUTHORIZED);
}
