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
async fn independent_resolution_grant_delegation_is_scoped_and_audited() {
 let url=std::env::var("TEST_DATABASE_URL").unwrap();let db=connect(&url).await;let root=Path::new(env!("CARGO_MANIFEST_DIR"));migrate(&db,&root.join("migrations"),1..=17).await;migrate(&db,&root.join("../../worker/rust-queue/migrations"),1..=u32::MAX).await;migrate(&db,&root.join("migrations"),18..=u32::MAX).await;
 let env=format!("delegate-{}",Uuid::new_v4());let other=format!("other-{}",Uuid::new_v4());let role=format!("delegate_{}",Uuid::new_v4().simple());db.execute("INSERT INTO public.novu_rust_environments VALUES($1,'org'),($2,'org')",&[&env,&other]).await.unwrap();db.execute("INSERT INTO public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) VALUES('https://issuer.test','admin','console',$1),('https://issuer.test','target','console',$1),('https://issuer.test','admin','console',$2)",&[&env,&other]).await.unwrap();
 db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_telegram_resolution_grant_read(text,text,text,text,bigint,text,text,text,text,text),public.novu_rust_operator_jwt_telegram_resolution_grant_change(text,text,text,text,bigint,uuid,text,text,text,text,text,boolean,boolean,text),public.novu_rust_operator_jwt_telegram_resolution_grant_request(text,text,text,text,bigint,uuid),public.novu_rust_operator_jwt_telegram_resolution_grant_audit_read(text,text,text,text,bigint,uuid),public.novu_rust_operator_jwt_telegram_attention(text,text,text,text,bigint,uuid) TO {role}")).await.unwrap();db.execute("INSERT INTO public.novu_rust_operator_jwt_api_logins SELECT rolname,oid FROM pg_roles WHERE rolname::text=$1",&[&role]).await.unwrap();db.execute("INSERT INTO public.novu_rust_telegram_operator_grants SELECT $1,rolname,oid,true,true,true FROM pg_roles WHERE rolname::text=$2",&[&env,&role]).await.unwrap();
 let make_app=||async {let api=connect(&url).await;api.batch_execute(&format!("SET SESSION AUTHORIZATION {role}")).await.unwrap();novu_rust_api::router(Arc::new(api)).layer(Extension(Arc::new(AccessTokenVerifier::new("https://issuer.test","swallow",&[("key",PUBLIC)]).unwrap())))};let app=make_app().await;let app2=make_app().await;let admin=jwt("admin");let target=jwt("target");let path=format!("/v1/experimental/operator/telegram-resolution-grants?environmentId={env}");let read=format!("{path}&targetIssuer=https%3A%2F%2Fissuer.test&targetSubject=target&targetClientId=console&resource=telegram-resolution&permission=list");let list=format!("/v1/experimental/operator/telegram-attention?environmentId={env}");
 assert_eq!(response(app.clone(),&admin,"GET",&read,None).await.0,StatusCode::UNAUTHORIZED);db.execute("INSERT INTO public.novu_rust_operator_jwt_telegram_resolution_grant_admins VALUES('https://issuer.test','admin','console',$1,true,true)",&[&env]).await.unwrap();assert_eq!(response(app.clone(),&admin,"GET",&read,None).await.1["granted"],false);assert_eq!(response(app.clone(),&target,"GET",&list,None).await.0,StatusCode::UNAUTHORIZED);
 let body=json!({"requestId":Uuid::new_v4(),"targetIssuer":"https://issuer.test","targetSubject":"target","targetClientId":"console","resource":"telegram-resolution","permission":"list","expected":false,"desired":true,"reason":"ticket"});
 db.batch_execute("CREATE FUNCTION public.reject_resolution_grant_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'audit failure'; END $$; CREATE TRIGGER reject_resolution_grant_audit BEFORE INSERT ON public.novu_rust_operator_jwt_telegram_resolution_grant_audit FOR EACH ROW EXECUTE FUNCTION public.reject_resolution_grant_audit()").await.unwrap();assert_eq!(response(app.clone(),&admin,"PUT",&path,Some(body.clone())).await.0,StatusCode::INTERNAL_SERVER_ERROR);assert!(db.query_opt("SELECT 1 FROM public.novu_rust_operator_jwt_telegram_resolution_grants WHERE environment_id=$1 AND subject='target'",&[&env]).await.unwrap().is_none());db.batch_execute("DROP TRIGGER reject_resolution_grant_audit ON public.novu_rust_operator_jwt_telegram_resolution_grant_audit").await.unwrap();
 let (a,b)=tokio::join!(response(app.clone(),&admin,"PUT",&path,Some(body.clone())),response(app2.clone(),&admin,"PUT",&path,Some(body.clone())));assert_eq!(a.0,StatusCode::OK);assert_eq!(b.0,StatusCode::OK);assert_ne!(a.1["replayed"],b.1["replayed"]);assert_eq!(response(app.clone(),&target,"GET",&list,None).await.0,StatusCode::OK);
 let mut own=body.clone();own["requestId"]=json!(Uuid::new_v4());own["targetSubject"]=json!("admin");assert_eq!(response(app.clone(),&admin,"PUT",&path,Some(own)).await.0,StatusCode::UNAUTHORIZED);let mut stale=body.clone();stale["requestId"]=json!(Uuid::new_v4());assert_eq!(response(app.clone(),&admin,"PUT",&path,Some(stale)).await.0,StatusCode::CONFLICT);
 for permission in ["retry","accepted"] {let mut grant=body.clone();grant["requestId"]=json!(Uuid::new_v4());grant["permission"]=json!(permission);assert_eq!(response(app.clone(),&admin,"PUT",&path,Some(grant)).await.0,StatusCode::OK);}
 let row=db.query_one("SELECT can_list,can_retry,can_accept FROM public.novu_rust_operator_jwt_telegram_resolution_grants WHERE environment_id=$1 AND subject='target'",&[&env]).await.unwrap();assert!(row.get::<_,bool>(0)&&row.get::<_,bool>(1)&&row.get::<_,bool>(2));let mut revoke=body.clone();revoke["requestId"]=json!(Uuid::new_v4());revoke["expected"]=json!(true);revoke["desired"]=json!(false);assert_eq!(response(app.clone(),&admin,"PUT",&path,Some(revoke)).await.0,StatusCode::OK);assert_eq!(response(app.clone(),&target,"GET",&list,None).await.0,StatusCode::UNAUTHORIZED);
 let history_grant=json!({"requestId":Uuid::new_v4(),"targetIssuer":"https://issuer.test","targetSubject":"target","targetClientId":"console","resource":"telegram-resolution","permission":"history","expected":false,"desired":true,"reason":"ticket"});assert_eq!(response(app.clone(),&admin,"PUT",&path,Some(history_grant.clone())).await.0,StatusCode::OK);assert!(db.query_opt("SELECT 1 FROM public.novu_rust_operator_jwt_telegram_resolution_history_viewers WHERE environment_id=$1 AND subject='target'",&[&env]).await.unwrap().is_some());let mut revoke_history=history_grant.clone();revoke_history["requestId"]=json!(Uuid::new_v4());revoke_history["expected"]=json!(true);revoke_history["desired"]=json!(false);assert_eq!(response(app.clone(),&admin,"PUT",&path,Some(revoke_history)).await.0,StatusCode::OK);assert!(db.query_opt("SELECT 1 FROM public.novu_rust_operator_jwt_telegram_resolution_history_viewers WHERE environment_id=$1 AND subject='target'",&[&env]).await.unwrap().is_none());
 let audit=format!("/v1/experimental/operator/telegram-resolution-grants/audit?environmentId={env}");assert_eq!(response(app.clone(),&admin,"GET",&audit,None).await.0,StatusCode::UNAUTHORIZED);db.execute("INSERT INTO public.novu_rust_operator_jwt_telegram_resolution_grant_audit_viewers VALUES('https://issuer.test','admin','console',$1)",&[&env]).await.unwrap();assert_eq!(response(app.clone(),&admin,"GET",&audit,None).await.1["data"].as_array().unwrap().len(),6);
 let cross=format!("/v1/experimental/operator/telegram-resolution-grants?environmentId={other}&targetIssuer=https%3A%2F%2Fissuer.test&targetSubject=target&targetClientId=console&resource=telegram-resolution&permission=list");assert_eq!(response(app,&admin,"GET",&cross,None).await.0,StatusCode::UNAUTHORIZED);
}
