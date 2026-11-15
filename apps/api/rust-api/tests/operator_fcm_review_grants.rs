//! FCM review grant administration against a disposable database.
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
async fn fcm_review_grants_are_independent_conditional_and_audited() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let db = connect(&url).await;
    db.batch_execute("INSERT INTO public.novu_rust_environments VALUES ('grant-a','org'),('grant-b','org'); INSERT INTO public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) VALUES ('https://issuer.test','admin','console','grant-a'),('https://issuer.test','target','console','grant-a'),('https://issuer.test','target','console','grant-b'); INSERT INTO public.novu_rust_operator_jwt_channel_access_grant_admins VALUES ('https://issuer.test','admin','console','grant-a',true,true)").await.unwrap();
    let role = format!("fcm_grant_{}", Uuid::new_v4().simple());
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_fcm_review_grant_read(text,text,text,text,bigint,text,text,text,text,text),public.novu_rust_operator_jwt_fcm_review_grant_change(text,text,text,text,bigint,uuid,text,text,text,text,text,boolean,boolean,text),public.novu_rust_operator_jwt_fcm_review_grant_request(text,text,text,text,bigint,uuid),public.novu_rust_operator_jwt_fcm_review_grant_audit_read(text,text,text,text,bigint,uuid),public.novu_rust_operator_jwt_fcm_unreviewed(text,text,text,text,bigint,uuid),public.novu_rust_operator_jwt_fcm_cooldown_health(text,text,text,text,bigint,integer),public.novu_rust_operator_jwt_fcm_unrecorded_claims(text,text,text,text,bigint,integer,integer,boolean,uuid) TO {role}")).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_operator_jwt_api_logins SELECT rolname,oid FROM pg_roles WHERE rolname::text=$1", &[&role]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'grant-a',$1,'alert_view',oid FROM pg_roles WHERE rolname::text=$1", &[&role]).await.unwrap();
    let api = connect(&url).await;
    api.batch_execute(&format!("SET SESSION AUTHORIZATION {role}"))
        .await
        .unwrap();
    assert!(
        api.query(
            "SELECT * FROM public.novu_rust_operator_jwt_fcm_unreviewed_grants",
            &[]
        )
        .await
        .is_err()
    );
    let app = novu_rust_api::router(Arc::new(api)).layer(Extension(Arc::new(
        AccessTokenVerifier::new("https://issuer.test", "swallow", &[("key", PUBLIC)]).unwrap(),
    )));
    let token = jwt("admin");
    let path = "/v1/experimental/operator/fcm-review-grants?environmentId=grant-a";
    let read = format!(
        "{path}&targetIssuer=https%3A%2F%2Fissuer.test&targetSubject=target&targetClientId=console&resource=fcm-partial&permission=unreviewed"
    );
    assert_eq!(
        response(app.clone(), &token, "GET", &read, None).await.0,
        StatusCode::UNAUTHORIZED
    );
    db.execute("INSERT INTO public.novu_rust_operator_jwt_fcm_review_grant_admins VALUES('https://issuer.test','admin','console','grant-a',true,false)",&[]).await.unwrap();
    assert_eq!(
        response(app.clone(), &token, "GET", &read, None).await.1,
        json!({"targetExists":true,"granted":false})
    );
    let id = Uuid::new_v4();
    let change = json!({"requestId":id,"targetIssuer":"https://issuer.test","targetSubject":"target","targetClientId":"console","resource":"fcm-partial","permission":"unreviewed","expected":false,"desired":true,"reason":"ticket"});
    assert_eq!(
        response(app.clone(), &token, "PUT", path, Some(change.clone()))
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    db.execute("UPDATE public.novu_rust_operator_jwt_fcm_review_grant_admins SET can_write=true WHERE subject='admin'",&[]).await.unwrap();
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
    let mut stale = change.clone();
    stale["requestId"] = json!(Uuid::new_v4());
    assert_eq!(
        response(app.clone(), &token, "PUT", path, Some(stale))
            .await
            .0,
        StatusCode::CONFLICT
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
    wrong["permission"] = json!("review");
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
        "/v1/experimental/operator/fcm-review-grants/request?environmentId=grant-a&requestId={id}"
    );
    assert_eq!(
        response(app.clone(), &token, "GET", &request, None).await.1["found"],
        true
    );
    let audit = "/v1/experimental/operator/fcm-review-grants/audit?environmentId=grant-a";
    assert_eq!(
        response(app.clone(), &token, "GET", audit, None).await.0,
        StatusCode::UNAUTHORIZED
    );
    db.execute("INSERT INTO public.novu_rust_operator_jwt_fcm_review_grant_audit_viewers VALUES('https://issuer.test','admin','console','grant-a')",&[]).await.unwrap();
    assert_eq!(
        response(app.clone(), &token, "GET", audit, None).await.1["data"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    for permission in ["health", "recent-list", "cooldown-health"] {
        let request = json!({"requestId":Uuid::new_v4(),"targetIssuer":"https://issuer.test","targetSubject":"target","targetClientId":"console","resource":"fcm-partial","permission":permission,"expected":false,"desired":true,"reason":"ticket"});
        assert_eq!(
            response(app.clone(), &token, "PUT", path, Some(request))
                .await
                .0,
            StatusCode::OK
        );
    }
    for permission in ["claim-health", "claim-list"] {
        let request = json!({"requestId":Uuid::new_v4(),"targetIssuer":"https://issuer.test","targetSubject":"target","targetClientId":"console","resource":"fcm-partial","permission":permission,"expected":false,"desired":true,"reason":"claim access"});
        let first = response(app.clone(), &token, "PUT", path, Some(request.clone())).await;
        assert_eq!(first.0, StatusCode::OK);
        assert_eq!(first.1["changed"], true);
        let replay = response(app.clone(), &token, "PUT", path, Some(request)).await;
        assert_eq!(replay.0, StatusCode::OK);
        assert_eq!(replay.1["replayed"], true);
    }
    let claims_health = "/v1/experimental/operator/fcm-unrecorded-claims-health?environmentId=grant-a&minGap=2&graceSeconds=60";
    let claims_list = "/v1/experimental/operator/fcm-unrecorded-claims?environmentId=grant-a&minGap=2&graceSeconds=60";
    assert_eq!(
        response(app.clone(), &jwt("target"), "GET", claims_health, None)
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(
        response(app.clone(), &jwt("target"), "GET", claims_list, None)
            .await
            .0,
        StatusCode::OK
    );
    let revoke_claim = json!({"requestId":Uuid::new_v4(),"targetIssuer":"https://issuer.test","targetSubject":"target","targetClientId":"console","resource":"fcm-partial","permission":"claim-health","expected":true,"desired":false,"reason":"claim revoke"});
    assert_eq!(
        response(app.clone(), &token, "PUT", path, Some(revoke_claim))
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(
        response(app.clone(), &jwt("target"), "GET", claims_health, None)
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        response(app.clone(), &jwt("target"), "GET", claims_list, None)
            .await
            .0,
        StatusCode::OK
    );
    let self_grant = json!({"requestId":Uuid::new_v4(),"targetIssuer":"https://issuer.test","targetSubject":"admin","targetClientId":"console","resource":"fcm-partial","permission":"claim-health","expected":false,"desired":true,"reason":"self"});
    assert_eq!(
        response(app.clone(), &token, "PUT", path, Some(self_grant))
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    db.execute("INSERT INTO public.novu_rust_fcm_cooldown_policies(environment_id,fallback_seconds) VALUES('grant-a',60)",&[]).await.unwrap();
    let cooldown =
        "/v1/experimental/operator/fcm-cooldown-health?environmentId=grant-a&graceSeconds=60";
    assert_eq!(
        response(app.clone(), &jwt("target"), "GET", cooldown, None)
            .await
            .0,
        StatusCode::OK
    );
    let revoke_cooldown = json!({"requestId":Uuid::new_v4(),"targetIssuer":"https://issuer.test","targetSubject":"target","targetClientId":"console","resource":"fcm-partial","permission":"cooldown-health","expected":true,"desired":false,"reason":"done"});
    assert_eq!(
        response(app.clone(), &token, "PUT", path, Some(revoke_cooldown))
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(
        response(app.clone(), &jwt("target"), "GET", cooldown, None)
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    let revoke = json!({"requestId":Uuid::new_v4(),"targetIssuer":"https://issuer.test","targetSubject":"target","targetClientId":"console","resource":"fcm-partial","permission":"unreviewed","expected":true,"desired":false,"reason":"done"});
    db.batch_execute("CREATE FUNCTION public.test_fcm_grant_reject_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'test audit failure'; END $$; CREATE TRIGGER test_fcm_grant_reject_audit BEFORE INSERT ON public.novu_rust_operator_jwt_fcm_review_grant_audit FOR EACH ROW EXECUTE FUNCTION public.test_fcm_grant_reject_audit()").await.unwrap();
    assert_eq!(
        response(app.clone(), &token, "PUT", path, Some(revoke.clone()))
            .await
            .0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(db.query_one("SELECT count(*) FROM public.novu_rust_operator_jwt_fcm_unreviewed_grants WHERE subject='target'",&[]).await.unwrap().get::<_,i64>(0),1);
    db.batch_execute("DROP TRIGGER test_fcm_grant_reject_audit ON public.novu_rust_operator_jwt_fcm_review_grant_audit; DROP FUNCTION public.test_fcm_grant_reject_audit()").await.unwrap();
    let target_list = "/v1/experimental/operator/fcm-unreviewed?environmentId=grant-a";
    assert_eq!(
        response(app.clone(), &jwt("target"), "GET", target_list, None)
            .await
            .0,
        StatusCode::OK
    );
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
        if db.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE '%novu_rust_operator_jwt_fcm_review_grant_change%')",&[]).await.unwrap().get::<_,bool>(0){waiting=true;break;}
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(waiting);
    tx.commit().await.unwrap();
    assert_eq!(pending.await.unwrap(), StatusCode::OK);
    assert_eq!(
        response(app.clone(), &jwt("target"), "GET", target_list, None)
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        response(app.clone(), &token, "GET", &read, None).await.1,
        json!({"targetExists":true,"granted":false})
    );
    let cross = "/v1/experimental/operator/fcm-review-grants?environmentId=grant-b&targetIssuer=https%3A%2F%2Fissuer.test&targetSubject=target&targetClientId=console&resource=fcm-partial&permission=review";
    assert_eq!(
        response(app.clone(), &token, "GET", cross, None).await.0,
        StatusCode::UNAUTHORIZED
    );
    db.execute(
        "DELETE FROM public.novu_rust_operator_jwt_fcm_review_grant_admins WHERE subject='admin'",
        &[],
    )
    .await
    .unwrap();
    assert_eq!(
        response(app, &token, "GET", &request, None).await.0,
        StatusCode::UNAUTHORIZED
    );
}
