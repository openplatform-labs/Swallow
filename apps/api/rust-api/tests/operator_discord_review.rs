//! Discord JWT review through restricted API login and disposable PostgreSQL.
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
async fn discord_review_is_scoped_revision_fenced_and_idempotent() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let db = connect(&url).await;
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    migrate(&db, &root.join("migrations"), 1..=17).await;
    migrate(
        &db,
        &root.join("../../worker/rust-queue/migrations"),
        1..=u32::MAX,
    )
    .await;
    migrate(&db, &root.join("migrations"), 18..=u32::MAX).await;
    let env = format!("discord-jwt-{}", Uuid::new_v4());
    let other = format!("discord-other-{}", Uuid::new_v4());
    let role = format!("discord_jwt_{}", Uuid::new_v4().simple());
    let job = Uuid::new_v4();
    db.execute(
        "INSERT INTO public.novu_rust_environments VALUES($1,'org'),($2,'org')",
        &[&env, &other],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) VALUES('https://issuer.test','alice','console',$1),('https://issuer.test','alice','console',$2)",&[&env,&other]).await.unwrap();
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_discord_attention(text,text,text,text,bigint,uuid),public.novu_rust_operator_jwt_discord_review(text,text,text,text,bigint,uuid,uuid,uuid,bigint,text),public.novu_rust_operator_jwt_discord_review_request(text,text,text,text,bigint,uuid) TO {role}")).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_operator_jwt_api_logins SELECT rolname,oid FROM pg_roles WHERE rolname::text=$1",&[&role]).await.unwrap();
    db.execute("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status) VALUES($1,$2,'experimental_discord_workflow',jsonb_build_object('secret','fixture-private'),'failed')",&[&job,&env]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_discord_attempts(job_id,environment_id,state) VALUES($1,$2,'unknown')",&[&job,&env]).await.unwrap();
    let api = connect(&url).await;
    api.batch_execute(&format!("SET SESSION AUTHORIZATION {role}"))
        .await
        .unwrap();
    assert!(
        api.query("SELECT * FROM public.novu_rust_discord_attempts", &[])
            .await
            .is_err()
    );
    let app = novu_rust_api::router(Arc::new(api)).layer(Extension(Arc::new(
        AccessTokenVerifier::new("https://issuer.test", "swallow", &[("key", PUBLIC)]).unwrap(),
    )));
    let token = jwt("alice");
    let list = format!("/v1/experimental/operator/discord-attention?environmentId={env}");
    let post = format!("/v1/experimental/operator/discord-review?environmentId={env}");
    let request_id = Uuid::new_v4();
    assert_eq!(
        response(app.clone(), &token, "GET", &list, None).await.0,
        StatusCode::UNAUTHORIZED
    );
    db.execute("INSERT INTO public.novu_rust_operator_jwt_discord_review_grants VALUES('https://issuer.test','alice','console',$1,true,false)",&[&env]).await.unwrap();
    assert_eq!(
        response(app.clone(), &token, "GET", &list, None).await.0,
        StatusCode::UNAUTHORIZED
    );
    db.execute("INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT $1,rolname,p,oid FROM pg_roles CROSS JOIN unnest(ARRAY['alert_view','queue_ack']) p WHERE rolname::text=$2",&[&env,&role]).await.unwrap();
    let (status, first) = response(app.clone(), &token, "GET", &list, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(first["data"].as_array().unwrap().len(), 1);
    assert!(!first.to_string().contains("fixture-private"));
    let first_row = &first["data"][0];
    let input = json!({"requestId":request_id,"jobId":job,"attemptRevision":first_row["attemptRevision"],"failureRevision":first_row["failureRevision"],"reason":"reviewed"});
    assert_eq!(
        response(app.clone(), &token, "POST", &post, Some(input.clone()))
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    db.execute("UPDATE public.novu_rust_operator_jwt_discord_review_grants SET can_review=true WHERE environment_id=$1",&[&env]).await.unwrap();
    db.execute(
        "UPDATE public.novu_rust_discord_attempts SET http_status=429,retry_after_seconds=90 WHERE job_id=$1",
        &[&job],
    )
    .await
    .unwrap();
    assert_eq!(
        response(app.clone(), &token, "POST", &post, Some(input.clone()))
            .await
            .0,
        StatusCode::CONFLICT
    );
    let lookup = format!(
        "/v1/experimental/operator/discord-review-request?environmentId={env}&requestId={request_id}"
    );
    assert_eq!(
        response(app.clone(), &token, "GET", &lookup, None).await.1,
        json!({"found":false})
    );
    let (_, updated) = response(app.clone(), &token, "GET", &list, None).await;
    let row = &updated["data"][0];
    assert_eq!(row["retryAfterSeconds"],90);
    assert!(row["retryNotBefore"].as_str().unwrap().ends_with('Z'));
    let input = json!({"requestId":request_id,"jobId":job,"attemptRevision":row["attemptRevision"],"failureRevision":row["failureRevision"],"reason":"reviewed"});
    assert_eq!(
        response(app.clone(), &token, "POST", &post, Some(input.clone()))
            .await
            .1,
        json!({"recorded":true,"replayed":false})
    );
    assert_eq!(
        response(app.clone(), &token, "POST", &post, Some(input.clone()))
            .await
            .1,
        json!({"recorded":true,"replayed":true})
    );
    let mut altered = input.clone();
    altered["reason"] = json!("different");
    assert_eq!(
        response(app.clone(), &token, "POST", &post, Some(altered))
            .await
            .0,
        StatusCode::CONFLICT
    );
    let (_, record) = response(app.clone(), &token, "GET", &lookup, None).await;
    assert_eq!(record["found"], true);
    assert_eq!(record["jobId"], job.to_string());
    assert_eq!(
        response(app.clone(), &token, "GET", &list, None).await.1["data"],
        json!([])
    );
    let pre_job = Uuid::new_v4();
    db.execute("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status) VALUES($1,$2,'experimental_discord_workflow','{}','failed')",&[&pre_job,&env]).await.unwrap();
    let (_, pending) = response(app.clone(), &token, "GET", &list, None).await;
    assert_eq!(pending["data"][0]["attemptRevision"], Value::Null);
    let pre_input = json!({"requestId":Uuid::new_v4(),"jobId":pre_job,"attemptRevision":null,"failureRevision":pending["data"][0]["failureRevision"],"reason":"checked"});
    assert_eq!(
        response(app.clone(), &token, "POST", &post, Some(pre_input))
            .await
            .1,
        json!({"recorded":true,"replayed":false})
    );
    let wrong = format!("/v1/experimental/operator/discord-attention?environmentId={other}");
    assert_eq!(
        response(app.clone(), &token, "GET", &wrong, None).await.0,
        StatusCode::UNAUTHORIZED
    );
    db.execute("DELETE FROM public.novu_rust_delivery_operator_grants WHERE environment_id=$1 AND database_principal=$2 AND decision='queue_ack'",&[&env,&role]).await.unwrap();
    assert_eq!(
        response(app.clone(), &token, "GET", &lookup, None).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        response(app.clone(), &token, "POST", &post, Some(input))
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    db.execute("UPDATE public.novu_rust_operator_jwt_discord_review_grants SET can_list=false WHERE environment_id=$1",&[&env]).await.unwrap();
    assert_eq!(
        response(app, &token, "GET", &list, None).await.0,
        StatusCode::UNAUTHORIZED
    );
}
