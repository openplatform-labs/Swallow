//! Slack delayed JWT decisions against a fully migrated disposable database.
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
async fn delayed_decision_replays_and_shares_request_namespace() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let db = connect(&url).await;
    db.batch_execute("INSERT INTO public.novu_rust_environments VALUES ('slack-delay-a','org'),('slack-delay-b','org'); INSERT INTO public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) VALUES ('https://issuer.test','alice','console','slack-delay-a'),('https://issuer.test','bob','console','slack-delay-a')").await.unwrap();
    let role = format!("slack_delay_api_{}", Uuid::new_v4().simple());
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_slack_retry_after(text,text,text,text,bigint,uuid,uuid,uuid,text,text),public.novu_rust_operator_jwt_slack_resolve(text,text,text,text,bigint,uuid,uuid,uuid,text,text),public.novu_rust_operator_jwt_slack_decision_request(text,text,text,text,bigint,uuid) TO {role}")).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_operator_jwt_api_logins SELECT rolname,oid FROM pg_roles WHERE rolname::text=$1",&[&role]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'slack-delay-a',rolname,'slack_retry',oid FROM pg_roles WHERE rolname::text=$1",&[&role]).await.unwrap();
    let api = connect(&url).await;
    api.batch_execute(&format!("SET SESSION AUTHORIZATION {role}"))
        .await
        .unwrap();
    let app = novu_rust_api::router(Arc::new(api)).layer(Extension(Arc::new(
        AccessTokenVerifier::new("https://issuer.test", "swallow", &[("key", PUBLIC)]).unwrap(),
    )));
    let job = Uuid::new_v4();
    db.execute("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status) VALUES($1,'slack-delay-a','experimental_slack_chat','{}','failed')",&[&job]).await.unwrap();
    let attempt:Uuid=db.query_one("INSERT INTO public.novu_rust_slack_attempts(job_id,environment_id,state,http_status,retry_after_seconds,retry_not_before) VALUES($1,'slack-delay-a','rejected',429,120,clock_timestamp()+interval '120 seconds') RETURNING attempt_id",&[&job]).await.unwrap().get(0);
    let id = Uuid::new_v4();
    let body = json!({"requestId":id,"jobId":job,"attemptId":attempt,"decision":"retry-after","reason":"ticket"});
    let token = jwt("alice");
    let path = "/v1/experimental/operator/slack-retry-after?environmentId=slack-delay-a";
    assert_eq!(
        response(app.clone(), &token, "POST", path, Some(body.clone()))
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    db.execute("INSERT INTO public.novu_rust_operator_jwt_slack_decision_grants VALUES('https://issuer.test','alice','console','slack-delay-a','retry')",&[]).await.unwrap();
    let mut stale = body.clone();
    stale["attemptId"] = json!(Uuid::new_v4());
    assert_eq!(
        response(app.clone(), &token, "POST", path, Some(stale))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    let mut bad = body.clone();
    bad["decision"] = json!("retry");
    assert_eq!(
        response(app.clone(), &token, "POST", path, Some(bad))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    db.batch_execute("ALTER TABLE public.novu_rust_slack_resolutions ADD CONSTRAINT test_slack_jwt_delay_reject CHECK(retry_not_before IS NULL) NOT VALID").await.unwrap();
    assert_eq!(
        response(app.clone(), &token, "POST", path, Some(body.clone()))
            .await
            .0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
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
    assert_eq!(db.query_one("SELECT count(*) FROM public.novu_rust_operator_jwt_slack_decision_requests WHERE request_id=$1",&[&id]).await.unwrap().get::<_,i64>(0),0);
    db.batch_execute("ALTER TABLE public.novu_rust_slack_resolutions DROP CONSTRAINT test_slack_jwt_delay_reject").await.unwrap();
    let (status, first) = response(app.clone(), &token, "POST", path, Some(body.clone())).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(first["recorded"], true);
    assert_eq!(first["replayed"], false);
    assert!(first["availableAt"].is_string());
    let (status, replay) = response(app.clone(), &token, "POST", path, Some(body.clone())).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(replay["replayed"], true);
    assert_eq!(replay["availableAt"], first["availableAt"]);
    let mut conflict = body.clone();
    conflict["reason"] = json!("different");
    assert_eq!(
        response(app.clone(), &token, "POST", path, Some(conflict))
            .await
            .0,
        StatusCode::CONFLICT
    );
    let immediate_path = "/v1/experimental/operator/slack-decision?environmentId=slack-delay-a";
    let mut immediate = body.clone();
    immediate["decision"] = json!("retry");
    assert_eq!(
        response(app.clone(), &token, "POST", immediate_path, Some(immediate))
            .await
            .0,
        StatusCode::CONFLICT
    );
    let lookup = format!(
        "/v1/experimental/operator/slack-decision-request?environmentId=slack-delay-a&requestId={id}"
    );
    let (status, record) = response(app.clone(), &token, "GET", &lookup, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(record["decision"], "retry-after");
    assert_eq!(record["availableAt"], first["availableAt"]);
    assert_eq!(
        response(app.clone(), &jwt("bob"), "GET", &lookup, None)
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    let row=db.query_one("SELECT status,available_at>clock_timestamp()+interval '100 seconds',(SELECT count(*) FROM public.novu_rust_slack_resolutions WHERE job_id=$1 AND attempt_id=$2 AND retry_not_before IS NOT NULL) FROM public.novu_queue_jobs WHERE id=$1",&[&job,&attempt]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "pending");
    assert!(row.get::<_, bool>(1));
    assert_eq!(row.get::<_, i64>(2), 1);
    db.execute("DELETE FROM public.novu_rust_operator_jwt_slack_decision_grants WHERE subject='alice' AND environment_id='slack-delay-a'",&[]).await.unwrap();
    assert_eq!(
        response(app.clone(), &token, "POST", path, Some(body))
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        response(app.clone(), &token, "GET", &lookup, None).await.0,
        StatusCode::UNAUTHORIZED
    );
}
