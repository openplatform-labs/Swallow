//! Delegated channel-result permissions against a fully migrated disposable database.
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
async fn delegated_channel_access_is_scoped_audited_and_idempotent() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let db = connect(&url).await;
    db.batch_execute("INSERT INTO public.novu_rust_environments VALUES ('channel-a','org'),('channel-b','org'); INSERT INTO public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) VALUES ('https://issuer.test','admin','console','channel-a'),('https://issuer.test','target','console','channel-a'),('https://issuer.test','target','console','channel-b')").await.unwrap();
    let role = format!("channel_api_{}", Uuid::new_v4().simple());
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_channel_access_grant_read(text,text,text,text,bigint,text,text,text,text,text),public.novu_rust_operator_jwt_channel_access_grant_change(text,text,text,text,bigint,uuid,text,text,text,text,text,boolean,boolean,text),public.novu_rust_operator_jwt_channel_access_grant_request(text,text,text,text,bigint,uuid),public.novu_rust_operator_jwt_channel_access_grant_audit_read(text,text,text,text,bigint,uuid) TO {role}")).await.unwrap();
    for provider in ["sendgrid", "resend", "twilio", "fcm", "slack"] {
        db.batch_execute(&format!("GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_{provider}_result(text,text,text,text,bigint,uuid),public.novu_rust_operator_jwt_{provider}_jobs(text,text,text,text,bigint,uuid) TO {role}")).await.unwrap();
    }
    db.execute("INSERT INTO public.novu_rust_operator_jwt_api_logins SELECT rolname,oid FROM pg_catalog.pg_roles WHERE rolname=$1",&[&role]).await.unwrap();
    let api = connect(&url).await;
    api.batch_execute(&format!("SET SESSION AUTHORIZATION {role}"))
        .await
        .unwrap();
    assert!(
        api.query(
            "SELECT * FROM public.novu_rust_operator_jwt_sendgrid_result_grants",
            &[]
        )
        .await
        .is_err()
    );
    let app = novu_rust_api::router(Arc::new(api)).layer(Extension(Arc::new(
        AccessTokenVerifier::new("https://issuer.test", "swallow", &[("key", PUBLIC)]).unwrap(),
    )));
    let token = jwt("admin");
    let path = "/v1/experimental/operator/channel-access-grants?environmentId=channel-a";
    let list_path = format!(
        "{path}&targetIssuer=https%3A%2F%2Fissuer.test&targetSubject=target&targetClientId=console&resource=sendgrid&permission=result"
    );
    assert_eq!(
        response(app.clone(), &token, "GET", &list_path, None)
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    db.batch_execute("INSERT INTO public.novu_rust_operator_jwt_channel_access_grant_admins VALUES ('https://issuer.test','admin','console','channel-a',true,false)").await.unwrap();
    let (_, before) = response(app.clone(), &token, "GET", &list_path, None).await;
    assert_eq!(before, json!({"targetExists":true,"granted":false}));
    let denied_body = json!({"requestId":Uuid::new_v4(),"targetIssuer":"https://issuer.test","targetSubject":"target","targetClientId":"console","resource":"sendgrid","permission":"result","expected":false,"desired":true,"reason":"not authorized"});
    assert_eq!(
        response(app.clone(), &token, "PUT", path, Some(denied_body))
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    let audit = "/v1/experimental/operator/channel-access-grants/audit?environmentId=channel-a";
    assert_eq!(
        response(app.clone(), &token, "GET", audit, None).await.0,
        StatusCode::UNAUTHORIZED
    );
    db.batch_execute("UPDATE public.novu_rust_operator_jwt_channel_access_grant_admins SET can_write=true WHERE subject='admin'; INSERT INTO public.novu_rust_operator_jwt_channel_access_grant_audit_viewers VALUES ('https://issuer.test','admin','console','channel-a')").await.unwrap();
    for resource in ["sendgrid", "resend", "twilio", "fcm", "slack"] {
        for permission in ["result", "jobs"] {
            let id = Uuid::new_v4();
            let body = json!({"requestId":id,"targetIssuer":"https://issuer.test","targetSubject":"target","targetClientId":"console","resource":resource,"permission":permission,"expected":false,"desired":true,"reason":"role assignment"});
            let (status, result) =
                response(app.clone(), &token, "PUT", path, Some(body.clone())).await;
            assert_eq!(status, StatusCode::OK, "{resource}/{permission}: {result}");
            assert_eq!(result["changed"], true);
            let (_, replayed) =
                response(app.clone(), &token, "PUT", path, Some(body.clone())).await;
            assert_eq!(replayed["replayed"], true);
            let req_path = format!(
                "/v1/experimental/operator/channel-access-grants/request?environmentId=channel-a&requestId={id}"
            );
            let (_, receipt) = response(app.clone(), &token, "GET", &req_path, None).await;
            assert_eq!(receipt["found"], true);
            let mut changed = body.clone();
            changed["reason"] = "different".into();
            assert_eq!(
                response(app.clone(), &token, "PUT", path, Some(changed))
                    .await
                    .0,
                StatusCode::CONFLICT
            );
        }
    }
    let (_, after) = response(app.clone(), &token, "GET", &list_path, None).await;
    assert_eq!(after["granted"], true);
    let target_token = jwt("target");
    for (provider, kind) in [
        ("sendgrid", "experimental_sendgrid_email"),
        ("resend", "experimental_resend_email"),
        ("twilio", "experimental_twilio_sms"),
        ("fcm", "experimental_fcm_push"),
        ("slack", "experimental_slack_chat"),
    ] {
        let job = Uuid::new_v4();
        novu_postgres_queue::enqueue(&db, job, "channel-a", kind, &json!({}))
            .await
            .unwrap();
        let detail = format!(
            "/v1/experimental/operator/{provider}-result?environmentId=channel-a&jobId={job}"
        );
        let list = format!("/v1/experimental/operator/{provider}-jobs?environmentId=channel-a");
        assert_eq!(
            response(app.clone(), &target_token, "GET", &detail, None)
                .await
                .0,
            StatusCode::OK
        );
        assert_eq!(
            response(app.clone(), &target_token, "GET", &list, None)
                .await
                .0,
            StatusCode::OK
        );
    }
    let revoke_body = json!({"requestId":Uuid::new_v4(),"targetIssuer":"https://issuer.test","targetSubject":"target","targetClientId":"console","resource":"sendgrid","permission":"jobs","expected":true,"desired":false,"reason":"role change"});
    assert_eq!(
        response(app.clone(), &token, "PUT", path, Some(revoke_body))
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(
        response(
            app.clone(),
            &target_token,
            "GET",
            "/v1/experimental/operator/sendgrid-jobs?environmentId=channel-a",
            None
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
    let api2 = connect(&url).await;
    api2.batch_execute(&format!("SET SESSION AUTHORIZATION {role}"))
        .await
        .unwrap();
    let app2 = novu_rust_api::router(Arc::new(api2)).layer(Extension(Arc::new(
        AccessTokenVerifier::new("https://issuer.test", "swallow", &[("key", PUBLIC)]).unwrap(),
    )));
    let concurrent = json!({"requestId":Uuid::new_v4(),"targetIssuer":"https://issuer.test","targetSubject":"target","targetClientId":"console","resource":"sendgrid","permission":"jobs","expected":false,"desired":true,"reason":"concurrent request"});
    let (one, two) = tokio::join!(
        response(app.clone(), &token, "PUT", path, Some(concurrent.clone())),
        response(app2, &token, "PUT", path, Some(concurrent))
    );
    assert_eq!(one.0, StatusCode::OK);
    assert_eq!(two.0, StatusCode::OK);
    assert_ne!(one.1["replayed"], two.1["replayed"]);
    let audit = "/v1/experimental/operator/channel-access-grants/audit?environmentId=channel-a";
    let (_, audit_data) = response(app.clone(), &token, "GET", audit, None).await;
    assert_eq!(audit_data["data"].as_array().unwrap().len(), 12);
    let self_body = json!({"requestId":Uuid::new_v4(),"targetIssuer":"https://issuer.test","targetSubject":"admin","targetClientId":"console","resource":"sendgrid","permission":"result","expected":false,"desired":true,"reason":"self"});
    assert_eq!(
        response(app.clone(), &token, "PUT", path, Some(self_body))
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        response(
            app.clone(),
            &token,
            "GET",
            &list_path.replace("environmentId=channel-a", "environmentId=channel-b"),
            None
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
    let mut revoker = connect(&url).await;
    let tx = revoker.transaction().await.unwrap();
    tx.execute("DELETE FROM public.novu_rust_operator_jwt_channel_access_grant_admins WHERE subject='admin' AND environment_id='channel-a'",&[]).await.unwrap();
    let pending = tokio::spawn({
        let app = app.clone();
        let token = token.clone();
        let path = list_path.clone();
        async move { response(app, &token, "GET", &path, None).await.0 }
    });
    let mut blocked = false;
    for _ in 0..100 {
        if db.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE '%novu_rust_operator_jwt_channel_access_grant_read%')",&[]).await.unwrap().get::<_,bool>(0){blocked=true;break;}
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(blocked);
    tx.commit().await.unwrap();
    assert_eq!(pending.await.unwrap(), StatusCode::UNAUTHORIZED);
}
