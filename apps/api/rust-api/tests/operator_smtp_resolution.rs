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
async fn smtp_resolution_requires_both_permissions_and_isolates_jwt_requests() {
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
    let env = format!("smtp-jwt-{}", Uuid::new_v4());
    let other = format!("smtp-other-{}", Uuid::new_v4());
    let role = format!("smtp_api_{}", Uuid::new_v4().simple());
    db.execute(
        "INSERT INTO public.novu_rust_environments VALUES($1,'org'),($2,'org')",
        &[&env, &other],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) VALUES('https://issuer.test','alice','console',$1),('https://issuer.test','bob','console',$1),('https://issuer.test','alice','console',$2)",&[&env,&other]).await.unwrap();
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_smtp_resolve(text,text,text,text,bigint,uuid,uuid,uuid,bigint,text,text),public.novu_rust_operator_jwt_smtp_resolution_request(text,text,text,text,bigint,uuid,text),public.novu_rust_operator_jwt_smtp_resolution_history(text,text,text,text,bigint,uuid,oid,uuid) TO {role}")).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_operator_jwt_api_logins SELECT rolname,oid FROM pg_roles WHERE rolname::text=$1",&[&role]).await.unwrap();
    let first = Uuid::new_v4();
    let second = Uuid::new_v4();
    for job in [first, second] {
        db.execute("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status) VALUES($1,$2,'experimental_smtp_email','{}','failed')",&[&job,&env]).await.unwrap();
        db.execute("INSERT INTO public.novu_rust_smtp_attempts(job_id,environment_id,state) VALUES($1,$2,'unknown')",&[&job,&env]).await.unwrap();
    }
    let api = connect(&url).await;
    api.batch_execute(&format!("SET SESSION AUTHORIZATION {role}"))
        .await
        .unwrap();
    let app = novu_rust_api::router(Arc::new(api)).layer(Extension(Arc::new(
        AccessTokenVerifier::new("https://issuer.test", "swallow", &[("key", PUBLIC)]).unwrap(),
    )));
    let alice = jwt("alice");
    let bob = jwt("bob");
    let path = format!("/v1/experimental/operator/smtp-resolution?environmentId={env}");
    let wrong_env = format!("/v1/experimental/operator/smtp-resolution?environmentId={other}");
    let id = Uuid::new_v4();
    let row=db.query_one("SELECT a.revision,j.failure_revision FROM public.novu_rust_smtp_attempts a JOIN public.novu_queue_jobs j ON j.id=a.job_id WHERE j.id=$1",&[&first]).await.unwrap();
    let rev: Uuid = row.get(0);
    let failure: i64 = row.get(1);
    let input = json!({"requestId":id,"jobId":first,"attemptRevision":rev,"failureRevision":failure.to_string(),"decision":"accepted","reason":"relay evidence"});
    assert_eq!(
        response(app.clone(), &alice, "POST", &path, Some(input.clone()))
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    db.execute("INSERT INTO public.novu_rust_operator_jwt_smtp_resolution_grants VALUES('https://issuer.test','alice','console',$1,true,false),('https://issuer.test','bob','console',$1,true,false)",&[&env]).await.unwrap();
    assert_eq!(
        response(app.clone(), &alice, "POST", &path, Some(input.clone()))
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    db.execute("INSERT INTO public.novu_rust_smtp_resolvers SELECT $1,rolname,oid,true,false FROM pg_roles WHERE rolname::text=$2",&[&env,&role]).await.unwrap();
    assert_eq!(
        response(app.clone(), &alice, "POST", &wrong_env, Some(input.clone()))
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    let mut retry = input.clone();
    retry["decision"] = json!("retry");
    assert_eq!(
        response(app.clone(), &alice, "POST", &path, Some(retry))
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    let mut stale = input.clone();
    stale["attemptRevision"] = json!(Uuid::new_v4());
    assert_eq!(
        response(app.clone(), &alice, "POST", &path, Some(stale))
            .await
            .0,
        StatusCode::CONFLICT
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM public.novu_rust_operator_jwt_smtp_resolution_requests",
            &[]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    let (status, result) = response(app.clone(), &alice, "POST", &path, Some(input.clone())).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(result, json!({"recorded":true,"replayed":false}));
    let (status, result) = response(app.clone(), &alice, "POST", &path, Some(input.clone())).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(result, json!({"recorded":true,"replayed":true}));
    let mut conflict = input.clone();
    conflict["reason"] = json!("different");
    assert_eq!(
        response(app.clone(), &alice, "POST", &path, Some(conflict))
            .await
            .0,
        StatusCode::CONFLICT
    );
    let row=db.query_one("SELECT a.revision,j.failure_revision FROM public.novu_rust_smtp_attempts a JOIN public.novu_queue_jobs j ON j.id=a.job_id WHERE j.id=$1",&[&second]).await.unwrap();
    let second_rev: Uuid = row.get(0);
    let second_failure: i64 = row.get(1);
    let bob_input = json!({"requestId":id,"jobId":second,"attemptRevision":second_rev,"failureRevision":second_failure.to_string(),"decision":"accepted","reason":"another relay record"});
    assert_eq!(
        response(app.clone(), &bob, "POST", &path, Some(bob_input))
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(
        db.query_one(
            "SELECT count(DISTINCT request_id) FROM public.novu_rust_smtp_resolutions",
            &[]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        2
    );
    let lookup = format!(
        "/v1/experimental/operator/smtp-resolution-request?environmentId={env}&requestId={id}&decision=accepted"
    );
    let (status, found) = response(app.clone(), &bob, "GET", &lookup, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(found["jobId"], second.to_string());
    db.execute("UPDATE public.novu_rust_operator_jwt_smtp_resolution_grants SET can_accept=false WHERE environment_id=$1 AND subject='bob'",&[&env]).await.unwrap();
    assert_eq!(
        response(app.clone(), &bob, "GET", &lookup, None).await.0,
        StatusCode::UNAUTHORIZED
    );
    db.execute(
        "UPDATE public.novu_rust_smtp_resolvers SET can_accept=false WHERE environment_id=$1",
        &[&env],
    )
    .await
    .unwrap();
    assert_eq!(
        response(app.clone(), &alice, "GET", &lookup, None).await.0,
        StatusCode::UNAUTHORIZED
    );
    let third = Uuid::new_v4();
    db.execute("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status) VALUES($1,$2,'experimental_smtp_email','{}','failed')",&[&third,&env]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_smtp_attempts(job_id,environment_id,state) VALUES($1,$2,'unknown')",&[&third,&env]).await.unwrap();
    db.execute("UPDATE public.novu_rust_operator_jwt_smtp_resolution_grants SET can_retry=true WHERE environment_id=$1 AND subject='alice'",&[&env]).await.unwrap();
    db.execute(
        "UPDATE public.novu_rust_smtp_resolvers SET can_retry=true WHERE environment_id=$1",
        &[&env],
    )
    .await
    .unwrap();
    let row=db.query_one("SELECT a.revision,j.failure_revision FROM public.novu_rust_smtp_attempts a JOIN public.novu_queue_jobs j ON j.id=a.job_id WHERE j.id=$1",&[&third]).await.unwrap();
    let third_rev: Uuid = row.get(0);
    let third_failure: i64 = row.get(1);
    let retry_id = Uuid::new_v4();
    let third_input = json!({"requestId":retry_id,"jobId":third,"attemptRevision":third_rev,"failureRevision":third_failure.to_string(),"decision":"retry","reason":"relay confirms no acceptance"});
    db.batch_execute("CREATE FUNCTION public.smtp_api_resolution_test_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'audit unavailable'; END $$; CREATE TRIGGER smtp_api_resolution_test_fail BEFORE INSERT ON public.novu_rust_smtp_resolutions FOR EACH ROW EXECUTE FUNCTION public.smtp_api_resolution_test_fail()").await.unwrap();
    assert_eq!(
        response(
            app.clone(),
            &alice,
            "POST",
            &path,
            Some(third_input.clone())
        )
        .await
        .0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(
        db.query_one(
            "SELECT status FROM public.novu_queue_jobs WHERE id=$1",
            &[&third]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "failed"
    );
    assert_eq!(db.query_one("SELECT count(*) FROM public.novu_rust_operator_jwt_smtp_resolution_requests WHERE request_id=$1",&[&retry_id]).await.unwrap().get::<_,i64>(0),0);
    db.batch_execute("DROP TRIGGER smtp_api_resolution_test_fail ON public.novu_rust_smtp_resolutions; DROP FUNCTION public.smtp_api_resolution_test_fail()").await.unwrap();
    assert_eq!(
        response(app.clone(), &alice, "POST", &path, Some(third_input))
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(
        db.query_one(
            "SELECT status FROM public.novu_queue_jobs WHERE id=$1",
            &[&third]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "pending"
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM public.novu_rust_smtp_attempts WHERE job_id=$1",
            &[&third]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    db.execute("INSERT INTO public.novu_rust_operator_jwt_smtp_resolution_history_viewers VALUES('https://issuer.test','alice','console',$1)",&[&env]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_smtp_resolution_audit_viewers SELECT $1,rolname,oid FROM pg_roles WHERE rolname::text=$2",&[&env,&role]).await.unwrap();
    let history = format!(
        "/v1/experimental/operator/smtp-resolution-history?environmentId={env}&jobId={first}"
    );
    let (status, page) = response(app.clone(), &alice, "GET", &history, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        page["data"][0]["operator"],
        json!({"issuer":"https://issuer.test","subject":"alice","clientId":"console","requestId":id})
    );
    let history_bob = format!(
        "/v1/experimental/operator/smtp-resolution-history?environmentId={env}&jobId={second}"
    );
    let (_, page_bob) = response(app.clone(), &alice, "GET", &history_bob, None).await;
    assert_eq!(page_bob["data"][0]["operator"]["subject"], "bob");
    let fourth = Uuid::new_v4();
    db.execute("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status) VALUES($1,$2,'experimental_smtp_email','{}','failed')",&[&fourth,&env]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_smtp_attempts(job_id,environment_id,state) VALUES($1,$2,'unknown')",&[&fourth,&env]).await.unwrap();
    let row=db.query_one("SELECT a.revision,j.failure_revision FROM public.novu_rust_smtp_attempts a JOIN public.novu_queue_jobs j ON j.id=a.job_id WHERE j.id=$1",&[&fourth]).await.unwrap();
    let fourth_rev: Uuid = row.get(0);
    let fourth_failure: i64 = row.get(1);
    let fourth_id = Uuid::new_v4();
    let fourth_input = json!({"requestId":fourth_id,"jobId":fourth,"attemptRevision":fourth_rev,"failureRevision":fourth_failure.to_string(),"decision":"retry","reason":"relay did not accept"});
    db.batch_execute("CREATE FUNCTION public.smtp_attribution_test_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'attribution unavailable'; END $$; CREATE TRIGGER smtp_attribution_test_fail BEFORE INSERT ON public.novu_rust_smtp_resolution_jwt_attribution FOR EACH ROW EXECUTE FUNCTION public.smtp_attribution_test_fail()").await.unwrap();
    assert_eq!(
        response(
            app.clone(),
            &alice,
            "POST",
            &path,
            Some(fourth_input.clone())
        )
        .await
        .0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(
        db.query_one(
            "SELECT status FROM public.novu_queue_jobs WHERE id=$1",
            &[&fourth]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "failed"
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM public.novu_rust_smtp_resolutions WHERE job_id=$1",
            &[&fourth]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    db.batch_execute("DROP TRIGGER smtp_attribution_test_fail ON public.novu_rust_smtp_resolution_jwt_attribution; DROP FUNCTION public.smtp_attribution_test_fail()").await.unwrap();
    assert_eq!(
        response(app.clone(), &alice, "POST", &path, Some(fourth_input))
            .await
            .0,
        StatusCode::OK
    );
    let pre = Uuid::new_v4();
    let pre_id = Uuid::new_v4();
    db.execute("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status) VALUES($1,$2,'experimental_smtp_email','{}','failed')",&[&pre,&env]).await.unwrap();
    let failure: i64 = db
        .query_one(
            "SELECT failure_revision FROM public.novu_queue_jobs WHERE id=$1",
            &[&pre],
        )
        .await
        .unwrap()
        .get(0);
    let pre_input = json!({"requestId":pre_id,"jobId":pre,"attemptRevision":null,"failureRevision":failure.to_string(),"decision":"retry","reason":"configuration corrected"});
    let mut invalid = pre_input.clone();
    invalid["decision"] = json!("accepted");
    assert_eq!(
        response(app.clone(), &alice, "POST", &path, Some(invalid))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    let mut stale = pre_input.clone();
    stale["failureRevision"] = json!((failure + 1).to_string());
    assert_eq!(
        response(app.clone(), &alice, "POST", &path, Some(stale))
            .await
            .0,
        StatusCode::CONFLICT
    );
    assert_eq!(
        response(app.clone(), &alice, "POST", &path, Some(pre_input.clone()))
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(
        response(app.clone(), &alice, "POST", &path, Some(pre_input))
            .await
            .1["replayed"],
        true
    );
    assert_eq!(
        db.query_one(
            "SELECT status FROM public.novu_queue_jobs WHERE id=$1",
            &[&pre]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "pending"
    );
    let history_pre = format!(
        "/v1/experimental/operator/smtp-resolution-history?environmentId={env}&jobId={pre}"
    );
    let (_, history) = response(app.clone(), &alice, "GET", &history_pre, None).await;
    assert!(history["data"][0]["attemptRevision"].is_null());
    assert_eq!(
        history["data"][0]["operator"]["requestId"],
        pre_id.to_string()
    );
    let lookup_pre = format!(
        "/v1/experimental/operator/smtp-resolution-request?environmentId={env}&requestId={pre_id}&decision=retry"
    );
    let (_, looked_up) = response(app.clone(), &alice, "GET", &lookup_pre, None).await;
    assert!(looked_up["attemptRevision"].is_null());
    let attempted = Uuid::new_v4();
    db.execute("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status) VALUES($1,$2,'experimental_smtp_email','{}','failed')",&[&attempted,&env]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_smtp_attempts(job_id,environment_id,state) VALUES($1,$2,'unknown')",&[&attempted,&env]).await.unwrap();
    let attempt_failure: i64 = db
        .query_one(
            "SELECT failure_revision FROM public.novu_queue_jobs WHERE id=$1",
            &[&attempted],
        )
        .await
        .unwrap()
        .get(0);
    let invalid_attempt = json!({"requestId":Uuid::new_v4(),"jobId":attempted,"attemptRevision":null,"failureRevision":attempt_failure.to_string(),"decision":"retry","reason":"wrong observation"});
    assert_eq!(
        response(app.clone(), &alice, "POST", &path, Some(invalid_attempt))
            .await
            .0,
        StatusCode::CONFLICT
    );
}
