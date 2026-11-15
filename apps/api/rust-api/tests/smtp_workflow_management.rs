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

async fn api_key_call(
    app: axum::Router,
    method: &str,
    path: &str,
    body: Value,
) -> (StatusCode, Value) {
    let response = app
        .oneshot(
            Request::builder()
                .method(method)
                .uri(path)
                .header(header::AUTHORIZATION, "ApiKey fixture-key")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}
#[tokio::test]
#[ignore = "requires fresh disposable PostgreSQL"]
async fn smtp_provider_management_supports_key_jwt_and_workflow_generation() {
    use sha2::{Digest, Sha256};
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let db = connect(&url).await;
    let mut worker = connect(&url).await;
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    migrate(&db, &root.join("migrations"), 1..=17).await;
    migrate(
        &db,
        &root.join("../../worker/rust-queue/migrations"),
        1..=u32::MAX,
    )
    .await;
    migrate(&db, &root.join("migrations"), 18..=u32::MAX).await;
    db.batch_execute("INSERT INTO public.novu_rust_users VALUES('user'); INSERT INTO public.novu_rust_environments VALUES('env','org'); INSERT INTO public.novu_rust_workflows(environment_id,identifier,active,has_steps,email_subject,email_html,email_provider,inbox_metadata) VALUES('env','workflow',true,true,'Hello','<p>hello</p>','sendgrid','{\"id\":\"workflow-id\"}'); INSERT INTO public.novu_rust_subscribers(environment_id,subscriber_id,internal_id,profile) VALUES('env','alice','alice-id','{\"email\":\"alice@example.test\"}')").await.unwrap();
    let hash = format!("{:x}", Sha256::digest(b"fixture-key"));
    db.execute("INSERT INTO public.novu_rust_api_keys(key_hash,environment_id,user_id) VALUES($1,'env','user')",&[&hash]).await.unwrap();
    db.execute(
        "INSERT INTO public.novu_rust_workflow_provider_permissions VALUES($1,'read'),($1,'write')",
        &[&hash],
    )
    .await
    .unwrap();
    db.batch_execute("INSERT INTO public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) VALUES('https://issuer.test','alice','console','env'); INSERT INTO public.novu_rust_operator_jwt_api_logins SELECT rolname,oid FROM pg_roles WHERE rolname=session_user; INSERT INTO public.novu_rust_operator_jwt_workflow_grants VALUES('https://issuer.test','alice','console','env','email-provider','read'),('https://issuer.test','alice','console','env','email-provider','write')").await.unwrap();
    let app = novu_rust_api::router(Arc::new(db)).layer(Extension(Arc::new(
        AccessTokenVerifier::new("https://issuer.test", "swallow", &[("key", PUBLIC)]).unwrap(),
    )));
    let path = "/v1/experimental/workflows/workflow/email-provider";
    assert_eq!(
        api_key_call(
            app.clone(),
            "PUT",
            path,
            json!({"provider":"smtp","expected":"sendgrid"})
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        api_key_call(app.clone(), "GET", path, Value::Null).await.1["data"]["provider"],
        "smtp"
    );
    assert_eq!(
        api_key_call(
            app.clone(),
            "PUT",
            path,
            json!({"provider":"resend","expected":"sendgrid"})
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    assert_eq!(
        api_key_call(
            app.clone(),
            "POST",
            "/v1/events/trigger",
            json!({"name":"workflow","to":"alice","transactionId":"smtp-api","payload":{}})
        )
        .await
        .0,
        StatusCode::CREATED
    );
    assert!(
        novu_postgres_queue::in_app::process_one(&mut worker)
            .await
            .unwrap()
    );
    assert_eq!(worker.query_one("SELECT kind FROM public.novu_queue_jobs WHERE environment_id='env' AND kind='experimental_smtp_email'",&[]).await.unwrap().get::<_,String>(0),"experimental_smtp_email");
    assert_eq!(worker.query_one("SELECT count(*) FROM public.novu_rust_workflow_provider_audit WHERE new_provider='smtp'",&[]).await.unwrap().get::<_,i64>(0),1);
    let token = jwt("alice");
    let read = "/v1/experimental/operator/workflow?environmentId=env&workflow=workflow&resource=email-provider";
    assert_eq!(
        response(app.clone(), &token, "GET", read, None).await.1["data"]["provider"],
        "smtp"
    );
    let id = Uuid::new_v4();
    let change = json!({"requestId":id,"workflow":"workflow","resource":"email-provider","expected":{"provider":"smtp"},"desired":{"provider":"resend"},"reason":"ticket"});
    let jwt_path = "/v1/experimental/operator/workflow-v2?environmentId=env";
    assert_eq!(
        response(app.clone(), &token, "PUT", jwt_path, Some(change.clone()))
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(
        response(app.clone(), &token, "PUT", jwt_path, Some(change))
            .await
            .1["replayed"],
        true
    );
    assert_eq!(worker.query_one("SELECT count(*) FROM public.novu_rust_operator_jwt_workflow_audit WHERE previous_provider='smtp' AND new_provider='resend'",&[]).await.unwrap().get::<_,i64>(0),1);
    let restore = json!({"requestId":Uuid::new_v4(),"workflow":"workflow","resource":"email-provider","expected":{"provider":"resend"},"desired":{"provider":"smtp"},"reason":"ticket"});
    assert_eq!(
        response(app.clone(), &token, "PUT", jwt_path, Some(restore))
            .await
            .0,
        StatusCode::OK
    );
    worker.batch_execute("ALTER TABLE public.novu_rust_workflow_provider_audit ADD CONSTRAINT fixture_audit_failure CHECK (new_provider<>'sendgrid') NOT VALID").await.unwrap();
    assert_eq!(
        api_key_call(
            app.clone(),
            "PUT",
            path,
            json!({"provider":"sendgrid","expected":"smtp"})
        )
        .await
        .0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(worker.query_one("SELECT email_provider FROM public.novu_rust_workflows WHERE environment_id='env' AND identifier='workflow'",&[]).await.unwrap().get::<_,String>(0),"smtp");
    worker.batch_execute("ALTER TABLE public.novu_rust_workflow_provider_audit DROP CONSTRAINT fixture_audit_failure; ALTER TABLE public.novu_rust_operator_jwt_workflow_audit ADD CONSTRAINT fixture_jwt_audit_failure CHECK (new_provider<>'sendgrid') NOT VALID").await.unwrap();
    let failed = json!({"requestId":Uuid::new_v4(),"workflow":"workflow","resource":"email-provider","expected":{"provider":"smtp"},"desired":{"provider":"sendgrid"},"reason":"ticket"});
    assert_eq!(
        response(app.clone(), &token, "PUT", jwt_path, Some(failed))
            .await
            .0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(worker.query_one("SELECT email_provider FROM public.novu_rust_workflows WHERE environment_id='env' AND identifier='workflow'",&[]).await.unwrap().get::<_,String>(0),"smtp");
    worker.batch_execute("DELETE FROM public.novu_rust_operator_jwt_workflow_grants WHERE environment_id='env' AND subject='alice' AND resource='email-provider' AND permission='write'").await.unwrap();
    let denied = json!({"requestId":Uuid::new_v4(),"workflow":"workflow","resource":"email-provider","expected":{"provider":"smtp"},"desired":{"provider":"resend"},"reason":"ticket"});
    assert_eq!(
        response(app, &token, "PUT", jwt_path, Some(denied)).await.0,
        StatusCode::UNAUTHORIZED
    );
}
