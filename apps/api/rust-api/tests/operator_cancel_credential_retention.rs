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
async fn retention_api_is_scoped_idempotent_and_atomic() {
    let url=std::env::var("TEST_DATABASE_URL").unwrap();let db=connect(&url).await;
    let env=format!("jwt-cancel-retention-{}",Uuid::new_v4());let other=format!("jwt-cancel-other-{}",Uuid::new_v4());let role=format!("jwt_cancel_ret_{}",Uuid::new_v4().simple());
    db.execute("INSERT INTO public.novu_rust_environments VALUES ($1,'org'),($2,'org')",&[&env,&other]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) VALUES('https://issuer.test','alice','console',$1),('https://issuer.test','bob','console',$1)",&[&env]).await.unwrap();
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_cancel_cred_retention_submit(text,text,text,text,bigint,uuid,integer,integer,text),public.novu_rust_operator_jwt_cancel_cred_retention_get(text,text,text,text,bigint,uuid),public.novu_rust_operator_jwt_cancel_cred_retention_audit(text,text,text,text,bigint,uuid),public.novu_rust_operator_jwt_resend_cancel_grant_read(text,text,text,text,bigint,text,text,text,text,text),public.novu_rust_operator_jwt_resend_cancel_grant_change(text,text,text,text,bigint,uuid,text,text,text,text,text,boolean,boolean,text),public.novu_rust_operator_jwt_resend_cancel_grant_request(text,text,text,text,bigint,uuid),public.novu_rust_operator_jwt_resend_cancel_grant_audit_read(text,text,text,text,bigint,uuid) TO {role}")).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_operator_jwt_api_logins SELECT rolname,oid FROM pg_roles WHERE rolname::text=$1",&[&role]).await.unwrap();
    let api=connect(&url).await;api.batch_execute(&format!("SET SESSION AUTHORIZATION {role}")).await.unwrap();
    assert!(api.query("SELECT * FROM public.novu_rust_resend_cancel_credential_health",&[]).await.is_err());
    let verifier=Arc::new(AccessTokenVerifier::new("https://issuer.test","swallow",&[("key",PUBLIC)]).unwrap());
    let app=novu_rust_api::router(Arc::new(api)).layer(Extension(verifier.clone()));
    let api2=connect(&url).await;api2.batch_execute(&format!("SET SESSION AUTHORIZATION {role}")).await.unwrap();
    let app2=novu_rust_api::router(Arc::new(api2)).layer(Extension(verifier));
    let alice=jwt("alice");let bob=jwt("bob");let path=format!("/v1/experimental/operator/resend-cancel-credential-retention?environmentId={env}");let id=Uuid::new_v4();
    db.execute("INSERT INTO public.novu_rust_operator_jwt_resend_cancel_grant_admins VALUES('https://issuer.test','alice','console',$1,true,true)",&[&env]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_operator_jwt_resend_cancel_grant_audit_viewers VALUES('https://issuer.test','alice','console',$1)",&[&env]).await.unwrap();
    let body=json!({"requestId":id,"olderThanSeconds":86401,"batchLimit":1,"reason":"ticket"});
    assert_eq!(response(app.clone(),&alice,"POST",&path,Some(body.clone())).await.0,StatusCode::UNAUTHORIZED);
    db.execute("INSERT INTO public.novu_rust_operator_jwt_cancel_cred_retention_grants VALUES('https://issuer.test','alice','console',$1)",&[&env]).await.unwrap();
    assert_eq!(response(app.clone(),&alice,"POST",&path,Some(body.clone())).await.0,StatusCode::UNAUTHORIZED);
    db.execute("INSERT INTO public.novu_rust_resend_cancel_cred_retention_operators SELECT $1,rolname,oid FROM pg_roles WHERE rolname::text=$2",&[&env,&role]).await.unwrap();
    let a=Uuid::new_v4();let b=Uuid::new_v4();db.execute("INSERT INTO public.novu_rust_resend_cancel_credential_health VALUES($1,$2,session_user,-1,clock_timestamp()-interval '5 days','unhealthy',clock_timestamp()-interval '3 days'),($1,$3,session_user,-1,clock_timestamp()-interval '5 days','unhealthy',clock_timestamp()-interval '3 days')",&[&env,&a,&b]).await.unwrap();
    let (one,two)=tokio::join!(response(app.clone(),&alice,"POST",&path,Some(body.clone())),response(app2.clone(),&alice,"POST",&path,Some(body.clone())));
    assert_eq!(one.0,StatusCode::OK,"{one:?}");assert_eq!(two.0,StatusCode::OK,"{two:?}");assert_ne!(one.1["replayed"],two.1["replayed"]);assert_eq!(one.1["deleted"],1);assert_eq!(two.1["deleted"],1);
    assert_eq!(db.query_one("SELECT count(*) FROM public.novu_rust_resend_cancel_credential_health WHERE instance_id=ANY($1)",&[&vec![a,b]]).await.unwrap().get::<_,i64>(0),1);
    let mut changed=body.clone();changed["reason"]=json!("different");assert_eq!(response(app.clone(),&alice,"POST",&path,Some(changed)).await.0,StatusCode::CONFLICT);
    let lookup=format!("{path}&requestId={id}");assert_eq!(response(app.clone(),&alice,"GET",&lookup,None).await.1["found"],true);
    assert_eq!(response(app.clone(),&bob,"GET",&lookup,None).await.0,StatusCode::UNAUTHORIZED);
    let grant_path=format!("/v1/experimental/operator/resend-cancel-grants?environmentId={env}");let grant_id=Uuid::new_v4();
    let grant=json!({"requestId":grant_id,"targetIssuer":"https://issuer.test","targetSubject":"bob","targetClientId":"console","resource":"resend-cancel","permission":"credential-retention","expected":false,"desired":true,"reason":"delegate"});
    let (one,two)=tokio::join!(response(app.clone(),&alice,"PUT",&grant_path,Some(grant.clone())),response(app2.clone(),&alice,"PUT",&grant_path,Some(grant.clone())));
    assert_eq!(one.0,StatusCode::OK,"{one:?}");assert_eq!(two.0,StatusCode::OK,"{two:?}");assert_ne!(one.1["replayed"],two.1["replayed"]);
    let read=format!("{grant_path}&targetIssuer=https%3A%2F%2Fissuer.test&targetSubject=bob&targetClientId=console&resource=resend-cancel&permission=credential-retention");assert_eq!(response(app.clone(),&alice,"GET",&read,None).await.1["granted"],true);
    let grant_lookup=format!("/v1/experimental/operator/resend-cancel-grants/request?environmentId={env}&requestId={grant_id}");assert_eq!(response(app.clone(),&alice,"GET",&grant_lookup,None).await.1["found"],true);
    let audit=format!("/v1/experimental/operator/resend-cancel-grants/audit?environmentId={env}");let audits=response(app.clone(),&alice,"GET",&audit,None).await;assert_eq!(audits.0,StatusCode::OK);assert!(audits.1["data"].as_array().unwrap().iter().any(|x|x["requestId"]==grant_id.to_string()&&x["permission"]=="credential-retention"));
    let mut stale=grant.clone();stale["requestId"]=json!(Uuid::new_v4());assert_eq!(response(app.clone(),&alice,"PUT",&grant_path,Some(stale)).await.0,StatusCode::CONFLICT);
    let mut self_grant=grant.clone();self_grant["requestId"]=json!(Uuid::new_v4());self_grant["targetSubject"]=json!("alice");assert_ne!(response(app.clone(),&alice,"PUT",&grant_path,Some(self_grant)).await.0,StatusCode::OK);
    let revoke=json!({"requestId":Uuid::new_v4(),"targetIssuer":"https://issuer.test","targetSubject":"bob","targetClientId":"console","resource":"resend-cancel","permission":"credential-retention","expected":true,"desired":false,"reason":"revoke"});
    assert_eq!(response(app.clone(),&bob,"GET",&lookup,None).await.1["found"],false);
    assert_eq!(response(app.clone(),&alice,"PUT",&grant_path,Some(revoke)).await.0,StatusCode::OK);assert_eq!(response(app.clone(),&bob,"GET",&lookup,None).await.0,StatusCode::UNAUTHORIZED);
    let failed=Uuid::new_v4();let failed_body=json!({"requestId":failed,"olderThanSeconds":86401,"batchLimit":1,"reason":"rollback"});
    db.batch_execute("ALTER TABLE public.novu_rust_resend_cancel_cred_retention_audit ADD CONSTRAINT fixture_fail CHECK (deleted_count=0) NOT VALID").await.unwrap();
    assert_eq!(response(app.clone(),&alice,"POST",&path,Some(failed_body)).await.0,StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(db.query_one("SELECT count(*) FROM public.novu_rust_resend_cancel_credential_health WHERE instance_id=ANY($1)",&[&vec![a,b]]).await.unwrap().get::<_,i64>(0),1);
    assert_eq!(response(app.clone(),&alice,"GET",&format!("{path}&requestId={failed}"),None).await.1["found"],false);
    db.batch_execute("ALTER TABLE public.novu_rust_resend_cancel_cred_retention_audit DROP CONSTRAINT fixture_fail").await.unwrap();
    let audit_path=format!("/v1/experimental/operator/resend-cancel-credential-retention/audit?environmentId={env}");
    assert_eq!(response(app.clone(),&alice,"GET",&audit_path,None).await.0,StatusCode::UNAUTHORIZED);
    db.execute("INSERT INTO public.novu_rust_operator_jwt_cancel_cred_retention_audit_viewers VALUES('https://issuer.test','alice','console',$1)",&[&env]).await.unwrap();
    assert_eq!(response(app.clone(),&alice,"GET",&audit_path,None).await.0,StatusCode::UNAUTHORIZED);
    db.execute("INSERT INTO public.novu_rust_cancel_cred_retention_audit_db_viewers SELECT $1,rolname,oid FROM pg_roles WHERE rolname::text=$2",&[&env,&role]).await.unwrap();
    let page=response(app.clone(),&alice,"GET",&audit_path,None).await;assert_eq!(page.0,StatusCode::OK);assert!(page.1["data"].as_array().unwrap().iter().any(|r|r["requestId"]==id.to_string()&&r["actorSubject"]=="alice"));
    assert_eq!(response(app.clone(),&bob,"GET",&audit_path,None).await.0,StatusCode::UNAUTHORIZED);
    let audit_grant_id=Uuid::new_v4();let audit_grant=json!({"requestId":audit_grant_id,"targetIssuer":"https://issuer.test","targetSubject":"bob","targetClientId":"console","resource":"resend-cancel","permission":"credential-retention-audit","expected":false,"desired":true,"reason":"audit access"});
    let (left,right)=tokio::join!(response(app.clone(),&alice,"PUT",&grant_path,Some(audit_grant.clone())),response(app2.clone(),&alice,"PUT",&grant_path,Some(audit_grant.clone())));assert_eq!(left.0,StatusCode::OK);assert_eq!(right.0,StatusCode::OK);assert_ne!(left.1["replayed"],right.1["replayed"]);
    assert_eq!(response(app.clone(),&bob,"GET",&audit_path,None).await.0,StatusCode::OK);assert_eq!(response(app.clone(),&bob,"GET",&lookup,None).await.0,StatusCode::UNAUTHORIZED);
    let mut conflict=audit_grant.clone();conflict["reason"]=json!("different");assert_eq!(response(app.clone(),&alice,"PUT",&grant_path,Some(conflict)).await.0,StatusCode::CONFLICT);
    let audit_grant_record=response(app.clone(),&alice,"GET",&format!("/v1/experimental/operator/resend-cancel-grants/request?environmentId={env}&requestId={audit_grant_id}"),None).await;assert_eq!(audit_grant_record.1["permission"],"credential-retention-audit");
    let audit_revoke=json!({"requestId":Uuid::new_v4(),"targetIssuer":"https://issuer.test","targetSubject":"bob","targetClientId":"console","resource":"resend-cancel","permission":"credential-retention-audit","expected":true,"desired":false,"reason":"audit revoke"});assert_eq!(response(app.clone(),&alice,"PUT",&grant_path,Some(audit_revoke)).await.0,StatusCode::OK);assert_eq!(response(app.clone(),&bob,"GET",&audit_path,None).await.0,StatusCode::UNAUTHORIZED);
    db.execute("INSERT INTO public.novu_rust_operator_jwt_cancel_cred_retention_requests(issuer,subject,client_id,environment_id,request_id,worker_request_id,older_than_seconds,batch_limit,reason,deleted_count) SELECT 'https://issuer.test','alice','console',$1,gen_random_uuid(),gen_random_uuid(),86401,1,'page',0 FROM generate_series(1,52)",&[&env]).await.unwrap();
    let first=response(app.clone(),&alice,"GET",&audit_path,None).await.1;assert_eq!(first["data"].as_array().unwrap().len(),50);assert_eq!(first["hasMore"],true);let cursor=first["next"].as_str().unwrap();let second=response(app.clone(),&alice,"GET",&format!("{audit_path}&after={cursor}"),None).await.1;assert_eq!(second["data"].as_array().unwrap().len(),3);assert_eq!(second["hasMore"],false);
    db.execute("DELETE FROM public.novu_rust_operator_jwt_cancel_cred_retention_audit_viewers WHERE environment_id=$1 AND subject='alice'",&[&env]).await.unwrap();assert_eq!(response(app.clone(),&alice,"GET",&audit_path,None).await.0,StatusCode::UNAUTHORIZED);
    db.execute("INSERT INTO public.novu_rust_operator_jwt_cancel_cred_retention_audit_viewers VALUES('https://issuer.test','alice','console',$1)",&[&env]).await.unwrap();db.execute("DELETE FROM public.novu_rust_cancel_cred_retention_audit_db_viewers WHERE environment_id=$1",&[&env]).await.unwrap();assert_eq!(response(app.clone(),&alice,"GET",&audit_path,None).await.0,StatusCode::UNAUTHORIZED);
    db.execute("DELETE FROM public.novu_rust_operator_jwt_cancel_cred_retention_grants WHERE environment_id=$1 AND subject='alice'",&[&env]).await.unwrap();
    assert_eq!(response(app.clone(),&alice,"GET",&lookup,None).await.0,StatusCode::UNAUTHORIZED);
    db.execute("INSERT INTO public.novu_rust_operator_jwt_cancel_cred_retention_grants VALUES('https://issuer.test','alice','console',$1)",&[&env]).await.unwrap();
    db.execute("DELETE FROM public.novu_rust_resend_cancel_cred_retention_operators WHERE environment_id=$1",&[&env]).await.unwrap();
    assert_eq!(response(app.clone(),&alice,"GET",&lookup,None).await.0,StatusCode::UNAUTHORIZED);
    assert_eq!(response(app.clone(),&alice,"POST",&path,Some(body)).await.0,StatusCode::UNAUTHORIZED);
}
