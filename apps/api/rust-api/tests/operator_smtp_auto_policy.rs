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
async fn smtp_auto_policy_jwt_and_history_permissions() {
    let url=std::env::var("TEST_DATABASE_URL").unwrap();let db=connect(&url).await;
    let env=format!("smtp-auto-jwt-{}",Uuid::new_v4().simple());let other=format!("smtp-auto-other-{}",Uuid::new_v4().simple());let role=format!("smtp_auto_jwt_{}",Uuid::new_v4().simple());
    db.execute("INSERT INTO public.novu_rust_environments VALUES($1,'org'),($2,'org')",&[&env,&other]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) VALUES('https://issuer.test','alice','console',$1),('https://issuer.test','bob','console',$1)",&[&env]).await.unwrap();
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_smtp_auto_policy_read(text,text,text,text,bigint),public.novu_rust_operator_jwt_smtp_auto_policy_change(text,text,text,text,bigint,uuid,bigint,boolean,integer,integer,text,text),public.novu_rust_operator_jwt_smtp_auto_policy_request(text,text,text,text,bigint,uuid),public.novu_rust_operator_jwt_smtp_auto_policy_audit_read(text,text,text,text,bigint,uuid),public.novu_rust_operator_jwt_smtp_auto_history(text,text,text,text,bigint,bigint) TO {role}")).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_operator_jwt_api_logins SELECT rolname,oid FROM pg_roles WHERE rolname::text=$1",&[&role]).await.unwrap();
    let api=connect(&url).await;api.batch_execute(&format!("SET SESSION AUTHORIZATION {role}")).await.unwrap();assert!(api.query("SELECT * FROM public.novu_rust_smtp_auto_retry_policy",&[]).await.is_err());
    let app=novu_rust_api::router_with_subscriber_secret(Arc::new(api),None).layer(Extension(Arc::new(AccessTokenVerifier::new("https://issuer.test","swallow",&[("key",PUBLIC)]).unwrap())));
    let token=jwt("alice");let bob=jwt("bob");let base=format!("/v1/experimental/operator/smtp-auto-policy?environmentId={env}");let audit=format!("/v1/experimental/operator/smtp-auto-policy/audit?environmentId={env}");let history=format!("/v1/experimental/operator/smtp-auto-history?environmentId={env}");
    assert_eq!(response(app.clone(),&token,"GET",&base,None).await.0,StatusCode::UNAUTHORIZED);
    db.execute("INSERT INTO public.novu_rust_operator_jwt_smtp_auto_policy_grants VALUES('https://issuer.test','alice','console',$1,true,true,true,false)",&[&env]).await.unwrap();
    assert_eq!(response(app.clone(),&token,"GET",&base,None).await.0,StatusCode::UNAUTHORIZED);
    db.execute("INSERT INTO public.novu_rust_smtp_auto_admins SELECT $1,rolname,oid,true,true,false FROM pg_roles WHERE rolname::text=$2",&[&env,&role]).await.unwrap();
    let (status,current)=response(app.clone(),&token,"GET",&base,None).await;assert_eq!(status,StatusCode::OK);assert_eq!(current["enabled"],false);assert_eq!(current["revision"],"0");
    assert_eq!(response(app.clone(),&bob,"GET",&base,None).await.0,StatusCode::UNAUTHORIZED);assert_eq!(response(app.clone(),&token,"GET",&base.replace(&env,&other),None).await.0,StatusCode::UNAUTHORIZED);
    let id=Uuid::new_v4();let input=json!({"requestId":id,"expectedRevision":"0","enabled":true,"maxRetries":2,"delaySeconds":10,"cutoff":"now","reason":"ticket"});
    let (a,b)=tokio::join!(response(app.clone(),&token,"PUT",&base,Some(input.clone())),response(app.clone(),&token,"PUT",&base,Some(input.clone())));
    assert_eq!((a.0,b.0),(StatusCode::OK,StatusCode::OK));assert_ne!(a.1["replayed"],b.1["replayed"]);assert_eq!(a.1["policy"]["revision"],"1");assert_eq!(a.1["policy"]["delaySeconds"],10);
    let mut conflict=input.clone();conflict["cutoff"]=json!("all");assert_eq!(response(app.clone(),&token,"PUT",&base,Some(conflict)).await.0,StatusCode::CONFLICT);
    let stale=json!({"requestId":Uuid::new_v4(),"expectedRevision":"0","enabled":false,"maxRetries":1,"delaySeconds":60,"cutoff":"preserve","reason":"stale"});assert_eq!(response(app.clone(),&token,"PUT",&base,Some(stale)).await.0,StatusCode::CONFLICT);
    let lookup=format!("/v1/experimental/operator/smtp-auto-policy/request?environmentId={env}&requestId={id}");assert_eq!(response(app.clone(),&token,"GET",&lookup,None).await.1["delaySeconds"],10);
    assert_eq!(response(app.clone(),&token,"GET",&audit,None).await.1["data"][0]["actorSubject"],"alice");assert_eq!(response(app.clone(),&token,"GET",&history,None).await.0,StatusCode::UNAUTHORIZED);
    db.execute("UPDATE public.novu_rust_operator_jwt_smtp_auto_policy_grants SET can_history=true WHERE environment_id=$1",&[&env]).await.unwrap();assert_eq!(response(app.clone(),&token,"GET",&history,None).await.0,StatusCode::UNAUTHORIZED);
    db.execute("UPDATE public.novu_rust_smtp_auto_admins SET can_audit=true WHERE environment_id=$1",&[&env]).await.unwrap();let (status,empty)=response(app.clone(),&token,"GET",&history,None).await;assert_eq!(status,StatusCode::OK);assert_eq!(empty["data"],json!([]));
    let job=Uuid::new_v4();db.execute("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload) VALUES($1,$2,'experimental_smtp_email','{\"private\":\"secret\"}')",&[&job,&env]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_smtp_auto_retries(environment_id,job_id,attempt_revision,failure_revision,database_principal,principal_oid,policy_max,policy_delay_seconds) SELECT $1,$2,gen_random_uuid(),0,rolname,oid,1,60 FROM pg_roles WHERE rolname::text=$3",&[&env,&job,&role]).await.unwrap();
    let (status,recorded)=response(app.clone(),&token,"GET",&history,None).await;assert_eq!(status,StatusCode::OK);assert_eq!(recorded["data"][0]["jobId"],job.to_string());assert!(!recorded.to_string().contains("secret"));
    assert_eq!(response(app.clone(),&token,"GET",&format!("{history}&before=0"),None).await.0,StatusCode::BAD_REQUEST);
    db.batch_execute("CREATE FUNCTION public.fixture_smtp_auto_jwt_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture'; END $$; CREATE TRIGGER fixture_smtp_auto_jwt_fail BEFORE INSERT ON public.novu_rust_operator_jwt_smtp_auto_policy_audit FOR EACH ROW EXECUTE FUNCTION public.fixture_smtp_auto_jwt_fail()").await.unwrap();
    let failure=json!({"requestId":Uuid::new_v4(),"expectedRevision":"1","enabled":false,"maxRetries":1,"delaySeconds":60,"cutoff":"preserve","reason":"rollback"});assert_eq!(response(app.clone(),&token,"PUT",&base,Some(failure.clone())).await.0,StatusCode::INTERNAL_SERVER_ERROR);assert_eq!(response(app.clone(),&token,"GET",&base,None).await.1["revision"],"1");
    db.batch_execute("DROP TRIGGER fixture_smtp_auto_jwt_fail ON public.novu_rust_operator_jwt_smtp_auto_policy_audit; DROP FUNCTION public.fixture_smtp_auto_jwt_fail()").await.unwrap();assert_eq!(response(app.clone(),&token,"PUT",&base,Some(failure)).await.0,StatusCode::OK);
    db.execute("UPDATE public.novu_rust_operator_jwt_smtp_auto_policy_grants SET can_history=false WHERE environment_id=$1",&[&env]).await.unwrap();assert_eq!(response(app.clone(),&token,"GET",&history,None).await.0,StatusCode::UNAUTHORIZED);
}
