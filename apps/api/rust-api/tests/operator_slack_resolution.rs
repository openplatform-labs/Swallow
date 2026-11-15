//! Slack operator decisions against a fully migrated disposable database.
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
fn short_jwt() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let mut h = Header::new(Algorithm::RS256);
    h.typ = Some("at+jwt".into());
    h.kid = Some("key".into());
    encode(&h,&json!({"iss":"https://issuer.test","sub":"alice","aud":"swallow","exp":now+2,"iat":now-1,"jti":"short","client_id":"console"}),&EncodingKey::from_rsa_pem(PRIVATE).unwrap()).unwrap()
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
async fn slack_decision_is_scoped_idempotent_and_attempt_fenced() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let db = connect(&url).await;
    db.batch_execute("INSERT INTO public.novu_rust_environments VALUES ('slack-a','org'),('slack-b','org'); INSERT INTO public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) VALUES ('https://issuer.test','alice','console','slack-a'),('https://issuer.test','bob','console','slack-a')").await.unwrap();
    let role = format!("slack_api_{}", Uuid::new_v4().simple());
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_slack_resolve(text,text,text,text,bigint,uuid,uuid,uuid,text,text),public.novu_rust_operator_jwt_slack_decision_request(text,text,text,text,bigint,uuid) TO {role}")).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_operator_jwt_api_logins SELECT rolname,oid FROM pg_roles WHERE rolname::text=$1", &[&role]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'slack-a',$1,p,oid FROM pg_roles CROSS JOIN unnest(ARRAY['slack_retry','slack_accepted']) p WHERE rolname::text=$1", &[&role]).await.unwrap();
    let api = connect(&url).await;
    api.batch_execute(&format!("SET SESSION AUTHORIZATION {role}"))
        .await
        .unwrap();
    assert!(
        api.query(
            "SELECT * FROM public.novu_rust_operator_jwt_slack_decision_requests",
            &[]
        )
        .await
        .is_err()
    );
    let app = novu_rust_api::router(Arc::new(api)).layer(Extension(Arc::new(
        AccessTokenVerifier::new("https://issuer.test", "swallow", &[("key", PUBLIC)]).unwrap(),
    )));
    let job = Uuid::new_v4();
    db.execute("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status) VALUES ($1,'slack-a','experimental_slack_chat','{}','failed')", &[&job]).await.unwrap();
    let attempt:Uuid=db.query_one("INSERT INTO public.novu_rust_slack_attempts(job_id,environment_id,state) VALUES($1,'slack-a','unknown') RETURNING attempt_id", &[&job]).await.unwrap().get(0);
    let request_id = Uuid::new_v4();
    let payload = json!({"requestId":request_id,"jobId":job,"attemptId":attempt,"decision":"retry","reason":"investigated"});
    let path = "/v1/experimental/operator/slack-decision?environmentId=slack-a";
    let token = jwt("alice");
    assert_eq!(
        response(app.clone(), &token, "POST", path, Some(payload.clone()))
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    db.execute("INSERT INTO public.novu_rust_operator_jwt_slack_decision_grants VALUES('https://issuer.test','alice','console','slack-a','retry')",&[]).await.unwrap();
    let mut accepted_without_grant = payload.clone();
    accepted_without_grant["requestId"] = json!(Uuid::new_v4());
    accepted_without_grant["decision"] = json!("accepted");
    assert_eq!(
        response(
            app.clone(),
            &token,
            "POST",
            path,
            Some(accepted_without_grant)
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
    let (status, body) = response(app.clone(), &token, "POST", path, Some(payload.clone())).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!({"recorded":true,"replayed":false}));
    assert_eq!(
        response(app.clone(), &token, "POST", path, Some(payload.clone()))
            .await
            .1,
        json!({"recorded":true,"replayed":true})
    );
    let mut changed = payload.clone();
    changed["reason"] = json!("other");
    assert_eq!(
        response(app.clone(), &token, "POST", path, Some(changed))
            .await
            .0,
        StatusCode::CONFLICT
    );
    let lookup = format!(
        "/v1/experimental/operator/slack-decision-request?environmentId=slack-a&requestId={request_id}"
    );
    let (status, body) = response(app.clone(), &token, "GET", &lookup, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["found"], true);
    assert_eq!(body["attemptId"], attempt.to_string());
    assert_eq!(
        response(app.clone(), &jwt("bob"), "GET", &lookup, None)
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    db.execute(
        "UPDATE public.novu_queue_jobs SET status='failed' WHERE id=$1",
        &[&job],
    )
    .await
    .unwrap();
    let newer:Uuid=db.query_one("INSERT INTO public.novu_rust_slack_attempts(job_id,environment_id,state) VALUES($1,'slack-a','unknown') RETURNING attempt_id", &[&job]).await.unwrap().get(0);
    assert_ne!(attempt, newer);
    let stale = json!({"requestId":Uuid::new_v4(),"jobId":job,"attemptId":attempt,"decision":"retry","reason":"stale"});
    assert_eq!(
        response(app.clone(), &token, "POST", path, Some(stale))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        db.query_one(
            "SELECT state FROM public.novu_rust_slack_attempts WHERE job_id=$1",
            &[&job]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "unknown"
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM public.novu_rust_slack_resolutions WHERE job_id=$1",
            &[&job]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        1
    );
    // Separate API connections race the same request; exactly one action is recorded.
    let concurrent_job = Uuid::new_v4();
    db.execute("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status) VALUES ($1,'slack-a','experimental_slack_webhook','{}','failed')", &[&concurrent_job]).await.unwrap();
    let concurrent_attempt:Uuid=db.query_one("INSERT INTO public.novu_rust_slack_attempts(job_id,environment_id,state) VALUES($1,'slack-a','unknown') RETURNING attempt_id", &[&concurrent_job]).await.unwrap().get(0);
    let api2 = connect(&url).await;
    api2.batch_execute(&format!("SET SESSION AUTHORIZATION {role}"))
        .await
        .unwrap();
    let app2 = novu_rust_api::router(Arc::new(api2)).layer(Extension(Arc::new(
        AccessTokenVerifier::new("https://issuer.test", "swallow", &[("key", PUBLIC)]).unwrap(),
    )));
    let same = json!({"requestId":Uuid::new_v4(),"jobId":concurrent_job,"attemptId":concurrent_attempt,"decision":"retry","reason":"concurrent"});
    let (first, second) = tokio::join!(
        response(app.clone(), &token, "POST", path, Some(same.clone())),
        response(app2.clone(), &token, "POST", path, Some(same))
    );
    assert_eq!(first.0, StatusCode::OK);
    assert_eq!(second.0, StatusCode::OK);
    assert_ne!(first.1["replayed"], second.1["replayed"]);
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM public.novu_rust_slack_resolutions WHERE job_id=$1",
            &[&concurrent_job]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        1
    );

    // Token expiry while waiting for a job lock prevents the decision.
    let mut locker = connect(&url).await;
    let tx = locker.transaction().await.unwrap();
    tx.query_one(
        "SELECT id FROM public.novu_queue_jobs WHERE id=$1 FOR UPDATE",
        &[&job],
    )
    .await
    .unwrap();
    let expire_id = Uuid::new_v4();
    let expire_body = json!({"requestId":expire_id,"jobId":job,"attemptId":newer,"decision":"retry","reason":"expiry check"});
    let short = short_jwt();
    let blocked = tokio::spawn({
        let app = app2.clone();
        async move {
            response(app, &short, "POST", path, Some(expire_body))
                .await
                .0
        }
    });
    let mut waiting = false;
    for _ in 0..100 {
        if db.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE '%novu_rust_operator_jwt_slack_resolve%')",&[]).await.unwrap().get::<_,bool>(0){waiting=true;break;}
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(waiting, "decision did not reach the job lock");
    tokio::time::sleep(Duration::from_secs(3)).await;
    tx.commit().await.unwrap();
    assert_eq!(blocked.await.unwrap(), StatusCode::UNAUTHORIZED);
    assert_eq!(db.query_one("SELECT count(*) FROM public.novu_rust_operator_jwt_slack_decision_requests WHERE request_id=$1", &[&expire_id]).await.unwrap().get::<_,i64>(0),0);

    // Failure to save the worker audit must roll the request and job transition back.
    db.batch_execute("CREATE FUNCTION public.test_slack_reject_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.reason='force audit failure' THEN RAISE EXCEPTION 'test failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER test_slack_reject_audit BEFORE INSERT ON public.novu_rust_slack_resolutions FOR EACH ROW EXECUTE FUNCTION public.test_slack_reject_audit()").await.unwrap();
    let failed_request = Uuid::new_v4();
    let fail = json!({"requestId":failed_request,"jobId":job,"attemptId":newer,"decision":"retry","reason":"force audit failure"});
    assert_eq!(
        response(app.clone(), &token, "POST", path, Some(fail))
            .await
            .0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(db.query_one("SELECT count(*) FROM public.novu_rust_operator_jwt_slack_decision_requests WHERE request_id=$1", &[&failed_request]).await.unwrap().get::<_,i64>(0),0);
    assert_eq!(
        db.query_one(
            "SELECT status FROM public.novu_queue_jobs WHERE id=$1",
            &[&job]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "failed"
    );
    db.execute("DELETE FROM public.novu_rust_operator_jwt_slack_decision_grants WHERE subject='alice' AND decision='retry'",&[]).await.unwrap();
    assert_eq!(
        response(app.clone(), &token, "GET", &lookup, None).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        response(app.clone(), &token, "POST", path, Some(payload))
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
}
