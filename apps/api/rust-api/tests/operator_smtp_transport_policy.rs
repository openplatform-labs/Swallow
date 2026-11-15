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
async fn smtp_transport_policy_jwt_permissions_and_audit() {
    let url=std::env::var("TEST_DATABASE_URL").unwrap();let db=connect(&url).await;
    let env=format!("smtp-jwt-{}",Uuid::new_v4().simple());let other=format!("smtp-other-{}",Uuid::new_v4().simple());let role=format!("smtp_jwt_{}",Uuid::new_v4().simple());
    db.execute("INSERT INTO public.novu_rust_environments VALUES($1,'org'),($2,'org')",&[&env,&other]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) VALUES('https://issuer.test','alice','console',$1),('https://issuer.test','bob','console',$1)",&[&env]).await.unwrap();
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_smtp_transport_policy_read(text,text,text,text,bigint),public.novu_rust_operator_jwt_smtp_transport_policy_change(text,text,text,text,bigint,uuid,bigint,text,text),public.novu_rust_operator_jwt_smtp_transport_policy_request(text,text,text,text,bigint,uuid),public.novu_rust_operator_jwt_smtp_transport_policy_audit_read(text,text,text,text,bigint,uuid) TO {role}")).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_operator_jwt_api_logins SELECT rolname,oid FROM pg_roles WHERE rolname::text=$1",&[&role]).await.unwrap();
    let api=connect(&url).await;api.batch_execute(&format!("SET SESSION AUTHORIZATION {role}")).await.unwrap();
    assert!(api.query("SELECT * FROM public.novu_rust_smtp_transport_policy",&[]).await.is_err());
    let app=novu_rust_api::router_with_subscriber_secret(Arc::new(api),None).layer(Extension(Arc::new(AccessTokenVerifier::new("https://issuer.test","swallow",&[("key",PUBLIC)]).unwrap())));
    let token=jwt("alice");let bob=jwt("bob");let base=format!("/v1/experimental/operator/smtp-transport-policy?environmentId={env}");let audit=format!("/v1/experimental/operator/smtp-transport-policy/audit?environmentId={env}");
    assert_eq!(response(app.clone(),&token,"GET",&base,None).await.0,StatusCode::UNAUTHORIZED);
    db.execute("INSERT INTO public.novu_rust_operator_jwt_smtp_transport_policy_grants VALUES('https://issuer.test','alice','console',$1,true,true,true)",&[&env]).await.unwrap();
    assert_eq!(response(app.clone(),&token,"GET",&base,None).await.0,StatusCode::UNAUTHORIZED);
    db.execute("INSERT INTO public.novu_rust_smtp_transport_admins SELECT $1,rolname,oid,true,true,false FROM pg_roles WHERE rolname::text=$2",&[&env,&role]).await.unwrap();
    assert_eq!(response(app.clone(),&token,"GET",&base,None).await.1,json!({"mode":"disabled","revision":"0"}));
    assert_eq!(response(app.clone(),&bob,"GET",&base,None).await.0,StatusCode::UNAUTHORIZED);
    let cross=base.replace(&env,&other);assert_eq!(response(app.clone(),&token,"GET",&cross,None).await.0,StatusCode::UNAUTHORIZED);
    let id=Uuid::new_v4();let input=json!({"requestId":id,"expectedRevision":"0","mode":"starttls","reason":"deployment"});
    let (a,b)=tokio::join!(response(app.clone(),&token,"PUT",&base,Some(input.clone())),response(app.clone(),&token,"PUT",&base,Some(input.clone())));
    assert_eq!((a.0,b.0),(StatusCode::OK,StatusCode::OK));assert_ne!(a.1["replayed"],b.1["replayed"]);assert_eq!(a.1["policy"],json!({"mode":"starttls","revision":"1"}));
    let mut conflict=input.clone();conflict["mode"]=json!("none");assert_eq!(response(app.clone(),&token,"PUT",&base,Some(conflict)).await.0,StatusCode::CONFLICT);
    let stale=json!({"requestId":Uuid::new_v4(),"expectedRevision":"0","mode":"tls","reason":"stale"});assert_eq!(response(app.clone(),&token,"PUT",&base,Some(stale)).await.0,StatusCode::CONFLICT);
    let lookup=format!("/v1/experimental/operator/smtp-transport-policy/request?environmentId={env}&requestId={id}");assert_eq!(response(app.clone(),&token,"GET",&lookup,None).await.1["mode"],"starttls");
    let (status,rows)=response(app.clone(),&token,"GET",&audit,None).await;assert_eq!(status,StatusCode::OK);assert_eq!(rows["data"][0]["actorSubject"],"alice");
    db.batch_execute("CREATE FUNCTION public.fixture_smtp_jwt_audit_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture'; END $$; CREATE TRIGGER fixture_smtp_jwt_audit_fail BEFORE INSERT ON public.novu_rust_operator_jwt_smtp_transport_policy_audit FOR EACH ROW EXECUTE FUNCTION public.fixture_smtp_jwt_audit_fail()").await.unwrap();
    let failure=json!({"requestId":Uuid::new_v4(),"expectedRevision":"1","mode":"tls","reason":"rollback"});assert_eq!(response(app.clone(),&token,"PUT",&base,Some(failure.clone())).await.0,StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(response(app.clone(),&token,"GET",&base,None).await.1["revision"],"1");
    db.batch_execute("DROP TRIGGER fixture_smtp_jwt_audit_fail ON public.novu_rust_operator_jwt_smtp_transport_policy_audit; DROP FUNCTION public.fixture_smtp_jwt_audit_fail()").await.unwrap();
    assert_eq!(response(app.clone(),&token,"PUT",&base,Some(failure)).await.0,StatusCode::OK);
    db.execute("UPDATE public.novu_rust_operator_jwt_smtp_transport_policy_grants SET can_read=false WHERE environment_id=$1",&[&env]).await.unwrap();
    assert_eq!(response(app.clone(),&token,"GET",&base,None).await.0,StatusCode::UNAUTHORIZED);
    db.execute("UPDATE public.novu_rust_smtp_transport_admins SET can_write=false WHERE environment_id=$1",&[&env]).await.unwrap();
    assert_eq!(response(app.clone(),&token,"GET",&lookup,None).await.0,StatusCode::UNAUTHORIZED);
}
