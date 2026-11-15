//! SMTP JWT review through restricted API login and disposable PostgreSQL.
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
async fn smtp_scope_jwt_and_db_permissions_are_independent_and_audited() {
    let url=std::env::var("TEST_DATABASE_URL").unwrap(); let db=connect(&url).await;
    let root=Path::new(env!("CARGO_MANIFEST_DIR"));
    migrate(&db,&root.join("migrations"),1..=17).await;
    migrate(&db,&root.join("../../worker/rust-queue/migrations"),1..=u32::MAX).await;
    migrate(&db,&root.join("migrations"),18..=u32::MAX).await;
    let env=format!("smtp-scope-{}",Uuid::new_v4());let other=format!("smtp-other-{}",Uuid::new_v4());
    let role=format!("smtp_scope_api_{}",Uuid::new_v4().simple());let target=format!("smtp_target_{}",Uuid::new_v4().simple());
    db.execute("INSERT INTO public.novu_rust_environments VALUES($1,'org'),($2,'org')",&[&env,&other]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) VALUES('https://issuer.test','alice','console',$1),('https://issuer.test','bob','console',$1),('https://issuer.test','alice','console',$2)",&[&env,&other]).await.unwrap();
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN; CREATE ROLE {target} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_smtp_scope_read(text,text,text,text,bigint,text),public.novu_rust_operator_jwt_smtp_scope_change(text,text,text,text,bigint,uuid,text,text,boolean,text),public.novu_rust_operator_jwt_smtp_scope_request(text,text,text,text,bigint,uuid),public.novu_rust_operator_jwt_smtp_scope_audit_read(text,text,text,text,bigint,uuid) TO {role}")).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_operator_jwt_api_logins SELECT rolname,oid FROM pg_roles WHERE rolname::text=$1",&[&role]).await.unwrap();
    db.query_one("SELECT public.novu_rust_install_smtp_scoped_rls()",&[]).await.unwrap();
    let api=connect(&url).await;api.batch_execute(&format!("SET SESSION AUTHORIZATION {role}")).await.unwrap();
    assert!(api.query("SELECT * FROM public.novu_rust_smtp_scoped_logins",&[]).await.is_err());
    let app=novu_rust_api::router(Arc::new(api)).layer(Extension(Arc::new(AccessTokenVerifier::new("https://issuer.test","swallow",&[("key",PUBLIC)]).unwrap())));
    let token=jwt("alice");let bob=jwt("bob");
    let base=format!("/v1/experimental/operator/smtp-scope?environmentId={env}");let read=format!("{base}&targetLogin={target}");let audit=format!("/v1/experimental/operator/smtp-scope/audit?environmentId={env}");
    assert_eq!(response(app.clone(),&token,"GET",&read,None).await.0,StatusCode::UNAUTHORIZED);
    db.execute("INSERT INTO public.novu_rust_operator_jwt_smtp_scope_grants VALUES('https://issuer.test','alice','console',$1,true,true,true)",&[&env]).await.unwrap();
    assert_eq!(response(app.clone(),&token,"GET",&read,None).await.0,StatusCode::UNAUTHORIZED);
    db.execute("INSERT INTO public.novu_rust_smtp_scope_admins SELECT $1,rolname,oid,true,true FROM pg_roles WHERE rolname::text=$2",&[&env,&role]).await.unwrap();
    assert_eq!(response(app.clone(),&token,"GET",&read,None).await.1,json!({"available":true,"state":"absent"}));
    assert_eq!(response(app.clone(),&bob,"GET",&read,None).await.0,StatusCode::UNAUTHORIZED);
    let request=Uuid::new_v4();let input=json!({"requestId":request,"targetLogin":target,"expected":"absent","desired":true,"reason":"ticket"});
    let a=response(app.clone(),&token,"PUT",&base,Some(input.clone()));let b=response(app.clone(),&token,"PUT",&base,Some(input.clone()));
    let (a,b)=tokio::join!(a,b);assert_eq!((a.0,b.0),(StatusCode::OK,StatusCode::OK));
    assert_ne!(a.1["replayed"],b.1["replayed"]);assert_eq!(a.1["result"],json!({"previous":"absent","state":"enabled","changed":true}));
    let mut conflict=input.clone();conflict["reason"]=json!("different");assert_eq!(response(app.clone(),&token,"PUT",&base,Some(conflict)).await.0,StatusCode::CONFLICT);
    let lookup=format!("/v1/experimental/operator/smtp-scope/request?environmentId={env}&requestId={request}");
    assert_eq!(response(app.clone(),&token,"GET",&lookup,None).await.1["reason"],"ticket");
    assert_eq!(response(app.clone(),&token,"GET",&audit,None).await.0,StatusCode::UNAUTHORIZED);
    db.execute("INSERT INTO public.novu_rust_smtp_scope_audit_viewers SELECT $1,rolname,oid FROM pg_roles WHERE rolname::text=$2",&[&env,&role]).await.unwrap();
    let (status,rows)=response(app.clone(),&token,"GET",&audit,None).await;assert_eq!(status,StatusCode::OK);assert_eq!(rows["data"].as_array().unwrap().len(),1);assert_eq!(rows["data"][0]["actorSubject"],"alice");
    let second_target=format!("smtp_target_{}",Uuid::new_v4().simple());
    db.batch_execute(&format!("CREATE ROLE {second_target} LOGIN")).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_operator_jwt_smtp_scope_grants VALUES('https://issuer.test','bob','console',$1,false,true,false)",&[&env]).await.unwrap();
    assert_eq!(response(app.clone(),&bob,"GET",&lookup,None).await.1,json!({"found":false}));
    let bob_input=json!({"requestId":request,"targetLogin":second_target,"expected":"absent","desired":true,"reason":"bob"});
    assert_eq!(response(app.clone(),&bob,"PUT",&base,Some(bob_input)).await.1["replayed"],false);
    assert_eq!(response(app.clone(),&bob,"GET",&lookup,None).await.1["target"],second_target);
    assert_eq!(response(app.clone(),&token,"GET",&lookup,None).await.1["target"],target);
    let stale=json!({"requestId":Uuid::new_v4(),"targetLogin":target,"expected":"absent","desired":false,"reason":"stale"});assert_eq!(response(app.clone(),&token,"PUT",&base,Some(stale)).await.0,StatusCode::CONFLICT);
    db.batch_execute("CREATE FUNCTION public.smtp_jwt_audit_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture'; END $$; CREATE TRIGGER smtp_jwt_audit_fail BEFORE INSERT ON public.novu_rust_operator_jwt_smtp_scope_audit FOR EACH ROW EXECUTE FUNCTION public.smtp_jwt_audit_fail()").await.unwrap();
    let disable=json!({"requestId":Uuid::new_v4(),"targetLogin":target,"expected":"enabled","desired":false,"reason":"done"});
    assert_eq!(response(app.clone(),&token,"PUT",&base,Some(disable.clone())).await.0,StatusCode::INTERNAL_SERVER_ERROR);
    assert!(db.query_one("SELECT enabled FROM public.novu_rust_smtp_scoped_logins WHERE database_principal::text=$1",&[&target]).await.unwrap().get::<_,bool>(0));
    db.batch_execute("DROP TRIGGER smtp_jwt_audit_fail ON public.novu_rust_operator_jwt_smtp_scope_audit; DROP FUNCTION public.smtp_jwt_audit_fail()").await.unwrap();
    assert_eq!(response(app.clone(),&token,"PUT",&base,Some(disable)).await.0,StatusCode::OK);
    db.execute("UPDATE public.novu_rust_smtp_scope_admins SET can_write=false WHERE environment_id=$1 AND database_principal::text=$2",&[&env,&role]).await.unwrap();
    assert_eq!(response(app.clone(),&token,"GET",&lookup,None).await.0,StatusCode::UNAUTHORIZED);
    db.execute("UPDATE public.novu_rust_operator_jwt_smtp_scope_grants SET can_read=false WHERE environment_id=$1",&[&env]).await.unwrap();
    assert_eq!(response(app.clone(),&token,"GET",&read,None).await.0,StatusCode::UNAUTHORIZED);
    let wrong=format!("/v1/experimental/operator/smtp-scope?environmentId={other}&targetLogin={target}");assert_eq!(response(app,&token,"GET",&wrong,None).await.0,StatusCode::UNAUTHORIZED);
}
