//! Resend 429 grant administration against a disposable database.
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
async fn resend_429_grants_are_independent_conditional_and_audited() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let db = connect(&url).await;
    db.batch_execute("INSERT INTO public.novu_rust_environments VALUES ('grant-a','org'),('grant-b','org'); INSERT INTO public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) VALUES ('https://issuer.test','admin','console','grant-a'),('https://issuer.test','target','console','grant-a'),('https://issuer.test','target','console','grant-b'); INSERT INTO public.novu_rust_operator_jwt_channel_access_grant_admins VALUES ('https://issuer.test','admin','console','grant-a',true,true)").await.unwrap();
    let role = format!("resend_grant_{}", Uuid::new_v4().simple());
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_resend_429_grant_read(text,text,text,text,bigint,text,text,text,text,text),public.novu_rust_operator_jwt_resend_429_grant_change(text,text,text,text,bigint,uuid,text,text,text,text,text,boolean,boolean,text),public.novu_rust_operator_jwt_resend_429_grant_request(text,text,text,text,bigint,uuid),public.novu_rust_operator_jwt_resend_429_grant_audit_read(text,text,text,text,bigint,uuid),public.novu_rust_operator_jwt_resend_429_resolve(text,text,text,text,bigint,uuid,uuid,uuid,text,text) TO {role}")).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_operator_jwt_api_logins SELECT rolname,oid FROM pg_roles WHERE rolname::text=$1", &[&role]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'grant-a',$1,'retry',oid FROM pg_roles WHERE rolname::text=$1", &[&role]).await.unwrap();
    let api = connect(&url).await;
    api.batch_execute(&format!("SET SESSION AUTHORIZATION {role}"))
        .await
        .unwrap();
    assert!(
        api.query(
            "SELECT * FROM public.novu_rust_operator_jwt_resend_429_grants",
            &[]
        )
        .await
        .is_err()
    );
    let app = novu_rust_api::router(Arc::new(api)).layer(Extension(Arc::new(
        AccessTokenVerifier::new("https://issuer.test", "swallow", &[("key", PUBLIC)]).unwrap(),
    )));
    let token = jwt("admin");
    let path = "/v1/experimental/operator/resend-429-grants?environmentId=grant-a";
    let read = format!(
        "{path}&targetIssuer=https%3A%2F%2Fissuer.test&targetSubject=target&targetClientId=console&resource=resend-429&permission=immediate"
    );
    assert_eq!(
        response(app.clone(), &token, "GET", &read, None).await.0,
        StatusCode::UNAUTHORIZED
    );
    db.execute("INSERT INTO public.novu_rust_operator_jwt_resend_429_grant_admins VALUES('https://issuer.test','admin','console','grant-a',true,false)",&[]).await.unwrap();
    assert_eq!(
        response(app.clone(), &token, "GET", &read, None).await.1,
        json!({"targetExists":true,"granted":false})
    );
    let id = Uuid::new_v4();
    let change = json!({"requestId":id,"targetIssuer":"https://issuer.test","targetSubject":"target","targetClientId":"console","resource":"resend-429","permission":"immediate","expected":false,"desired":true,"reason":"ticket"});
    assert_eq!(
        response(app.clone(), &token, "PUT", path, Some(change.clone()))
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    db.execute("UPDATE public.novu_rust_operator_jwt_resend_429_grant_admins SET can_write=true WHERE subject='admin'",&[]).await.unwrap();
    let (status, body) = response(app.clone(), &token, "PUT", path, Some(change.clone())).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["changed"], true);
    assert_eq!(body["replayed"], false);
    assert_eq!(
        response(app.clone(), &token, "PUT", path, Some(change.clone()))
            .await
            .1["replayed"],
        true
    );
    let mut changed = change.clone();
    changed["reason"] = json!("different");
    assert_eq!(
        response(app.clone(), &token, "PUT", path, Some(changed))
            .await
            .0,
        StatusCode::CONFLICT
    );
    let mut wrong = change.clone();
    wrong["requestId"] = json!(Uuid::new_v4());
    wrong["permission"] = json!("audit");
    assert_eq!(
        response(app.clone(), &token, "PUT", path, Some(wrong))
            .await
            .0,
        StatusCode::OK
    );
    let mut invalid = change.clone();
    invalid["requestId"] = json!(Uuid::new_v4());
    invalid["resource"] = json!("sendgrid");
    assert_eq!(
        response(app.clone(), &token, "PUT", path, Some(invalid))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    let mut own = change.clone();
    own["requestId"] = json!(Uuid::new_v4());
    own["targetSubject"] = json!("admin");
    assert_eq!(
        response(app.clone(), &token, "PUT", path, Some(own))
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    let request = format!(
        "/v1/experimental/operator/resend-429-grants/request?environmentId=grant-a&requestId={id}"
    );
    assert_eq!(
        response(app.clone(), &token, "GET", &request, None).await.1["found"],
        true
    );
    let audit = "/v1/experimental/operator/resend-429-grants/audit?environmentId=grant-a";
    assert_eq!(
        response(app.clone(), &token, "GET", audit, None).await.0,
        StatusCode::UNAUTHORIZED
    );
    db.execute("INSERT INTO public.novu_rust_operator_jwt_resend_429_grant_audit_viewers VALUES('https://issuer.test','admin','console','grant-a')",&[]).await.unwrap();
    assert_eq!(
        response(app.clone(), &token, "GET", audit, None).await.1["data"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    let job = Uuid::new_v4();
    db.execute("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status) VALUES ($1,'grant-a','experimental_resend_email','{}','failed')", &[&job]).await.unwrap();
    let attempt:Uuid=db.query_one("INSERT INTO public.novu_rust_resend_attempts(job_id,environment_id,state,http_status) VALUES($1,'grant-a','rejected',429) RETURNING attempt_id", &[&job]).await.unwrap().get(0);
    let decision = json!({"requestId":Uuid::new_v4(),"jobId":job,"attemptId":attempt,"mode":"immediate","reason":"investigated"});
    let decision_path = "/v1/experimental/operator/resend-429-decision?environmentId=grant-a";
    assert_eq!(
        response(
            app.clone(),
            &jwt("target"),
            "POST",
            decision_path,
            Some(decision)
        )
        .await
        .0,
        StatusCode::OK
    );
    let revoke = json!({"requestId":Uuid::new_v4(),"targetIssuer":"https://issuer.test","targetSubject":"target","targetClientId":"console","resource":"resend-429","permission":"immediate","expected":true,"desired":false,"reason":"done"});
    // Audit failure cannot revoke the grant or persist a request.
    db.batch_execute("CREATE FUNCTION public.test_grant_reject_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'test audit failure'; END $$; CREATE TRIGGER test_grant_reject_audit BEFORE INSERT ON public.novu_rust_operator_jwt_resend_429_grant_audit FOR EACH ROW EXECUTE FUNCTION public.test_grant_reject_audit()").await.unwrap();
    assert_eq!(
        response(app.clone(), &token, "PUT", path, Some(revoke.clone()))
            .await
            .0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(db.query_one("SELECT count(*) FROM public.novu_rust_operator_jwt_resend_429_grants WHERE subject='target' AND mode='immediate'",&[]).await.unwrap().get::<_,i64>(0),1);
    db.batch_execute("DROP TRIGGER test_grant_reject_audit ON public.novu_rust_operator_jwt_resend_429_grant_audit; DROP FUNCTION public.test_grant_reject_audit()").await.unwrap();
    // Grant revocation waits for an ongoing target identity transaction.
    let mut locker = connect(&url).await;
    let tx = locker.transaction().await.unwrap();
    tx.query_one("SELECT subject FROM public.novu_rust_operator_jwt_identities WHERE subject='target' AND environment_id='grant-a' FOR SHARE",&[]).await.unwrap();
    let pending = tokio::spawn({
        let app = app.clone();
        let token = token.clone();
        async move { response(app, &token, "PUT", path, Some(revoke)).await.0 }
    });
    let mut waiting = false;
    for _ in 0..100 {
        if db.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE '%novu_rust_operator_jwt_resend_429_grant_change%')",&[]).await.unwrap().get::<_,bool>(0){waiting=true;break;}
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(waiting);
    tx.commit().await.unwrap();
    assert_eq!(pending.await.unwrap(), StatusCode::OK);
    db.execute(
        "UPDATE public.novu_queue_jobs SET status='failed' WHERE id=$1",
        &[&job],
    )
    .await
    .unwrap();
    let next:Uuid=db.query_one("INSERT INTO public.novu_rust_resend_attempts(job_id,environment_id,state,http_status) VALUES($1,'grant-a','rejected',429) RETURNING attempt_id", &[&job]).await.unwrap().get(0);
    let denied = json!({"requestId":Uuid::new_v4(),"jobId":job,"attemptId":next,"mode":"immediate","reason":"denied"});
    assert_eq!(
        response(
            app.clone(),
            &jwt("target"),
            "POST",
            decision_path,
            Some(denied)
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
    db.execute("DELETE FROM public.novu_rust_operator_jwt_resend_429_grant_admins WHERE subject='admin'",&[]).await.unwrap();
    assert_eq!(
        response(app.clone(), &token, "GET", &request, None).await.0,
        StatusCode::UNAUTHORIZED
    );
}
