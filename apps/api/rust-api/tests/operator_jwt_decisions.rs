//! Idempotent JWT operator decisions on disposable PostgreSQL.
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
    process::{Child, Command, Stdio},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio_postgres::{Client, NoTls};
use tower::ServiceExt;
use uuid::Uuid;
const PRIVATE: &[u8] = include_bytes!("fixtures/operator_jwt/test-only-private.pem");
const PUBLIC: &[u8] = include_bytes!("fixtures/operator_jwt/test-only-public.pem");
struct ApiProcess(Child);
impl Drop for ApiProcess {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
async fn connect(url: &str) -> Client {
    let (db, driver) = tokio_postgres::connect(url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    db
}
fn jwt(subject: &str, id: &str, exp: u64) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let mut h = Header::new(Algorithm::RS256);
    h.typ = Some("at+jwt".into());
    h.kid = Some("key".into());
    encode(&h,&json!({"iss":"https://issuer.test","sub":subject,"aud":"swallow","exp":exp,"iat":now-1,"jti":id,"client_id":"console"}),&EncodingKey::from_rsa_pem(PRIVATE).unwrap()).unwrap()
}
fn post(token: &str, id: Uuid, attempt: Uuid, decision: &str, reason: &str) -> Request<Body> {
    Request::builder().method("POST").uri("/v1/experimental/operator/alerts/resolve?environmentId=jwt-decisions").header(header::AUTHORIZATION,format!("Bearer {token}")).body(Body::from(json!({"requestId":id,"channel":"slack","targetId":"ops","attemptId":attempt,"decision":decision,"reason":reason}).to_string())).unwrap()
}
fn lookup(token: &str, id: Uuid) -> Request<Body> {
    Request::builder()
        .uri(format!(
            "/v1/experimental/operator/alerts/request?environmentId=jwt-decisions&requestId={id}"
        ))
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap()
}
async fn json_body(response: axum::response::Response) -> (StatusCode, Value) {
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap())
}
#[tokio::test]
#[ignore = "requires fully migrated disposable PostgreSQL and role creation"]
async fn jwt_decisions_are_scoped_atomic_and_idempotent() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let db = connect(&url).await;
    db.batch_execute("INSERT INTO public.novu_rust_environments VALUES('jwt-decisions','org'),('jwt-other','org')").await.unwrap();
    let role = format!("jwt_decision_{}", Uuid::new_v4().simple());
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_resolve_alert(text,text,text,text,bigint,uuid,text,text,uuid,text,text),public.novu_rust_operator_jwt_decision_request(text,text,text,text,bigint,uuid) TO {role}")).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_operator_jwt_api_logins SELECT rolname,oid FROM pg_roles WHERE rolname::text=$1",&[&role]).await.unwrap();
    for permission in ["alert_retry", "alert_accepted"] {
        db.execute("INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'jwt-decisions',rolname,$2,oid FROM pg_roles WHERE rolname::text=$1",&[&role,&permission]).await.unwrap();
    }
    db.batch_execute("INSERT INTO public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id,alert_retry,alert_accepted) VALUES('https://issuer.test','alice','console','jwt-decisions',true,false),('https://issuer.test','bob','console','jwt-decisions',true,false),('https://issuer.test','eve','console','jwt-decisions',false,false)").await.unwrap();
    let attempt = Uuid::new_v4();
    let id = Uuid::new_v4();
    db.execute("INSERT INTO public.novu_rust_alert_delivery_slots(environment_id,channel,target_id,state,attempt_id) VALUES('jwt-decisions','slack','ops','unknown',$1)",&[&attempt]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_alert_delivery_attempts(id,environment_id,channel,target_id,state) VALUES($1,'jwt-decisions','slack','ops','unknown')",&[&attempt]).await.unwrap();
    let api_url = url.replacen("postgres://postgres@", &format!("postgres://{role}@"), 1);
    let api = connect(&api_url).await;
    assert!(
        api.query(
            "SELECT * FROM public.novu_rust_operator_jwt_decision_requests",
            &[]
        )
        .await
        .is_err()
    );
    let verifier = Arc::new(
        AccessTokenVerifier::new("https://issuer.test", "swallow", &[("key", PUBLIC)]).unwrap(),
    );
    let app = novu_rust_api::router(Arc::new(api)).layer(Extension(verifier.clone()));
    let other = novu_rust_api::router(Arc::new(connect(&api_url).await)).layer(Extension(verifier));
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let alice = jwt("alice", "first", now + 120);
    let refreshed = jwt("alice", "second", now + 120);
    let bob = jwt("bob", "first", now + 120);
    let eve = jwt("eve", "first", now + 120);
    assert_eq!(
        app.clone()
            .oneshot(post(&eve, id, attempt, "retry", "review"))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        app.clone()
            .oneshot(post(&alice, id, attempt, "accepted", "review"))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    let (a, b) = tokio::join!(
        app.clone()
            .oneshot(post(&alice, id, attempt, "retry", "review")),
        other
            .clone()
            .oneshot(post(&refreshed, id, attempt, "retry", "review"))
    );
    let (as_, av) = json_body(a.unwrap()).await;
    let (bs, bv) = json_body(b.unwrap()).await;
    assert_eq!((as_, bs), (StatusCode::OK, StatusCode::OK));
    assert_ne!(av["replayed"], bv["replayed"]);
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = socket.local_addr().unwrap().port();
    drop(socket);
    let config=json!({"issuer":"https://issuer.test","audience":"swallow","keys":[{"kid":"key","pem":std::str::from_utf8(PUBLIC).unwrap()}]}).to_string();
    let _server = ApiProcess(
        Command::new(env!("CARGO_BIN_EXE_novu-rust-api"))
            .env("DATABASE_URL", &api_url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("LISTEN_ADDR", format!("127.0.0.1:{port}"))
            .env("SWALLOW_OPERATOR_JWT_CONFIG", config)
            .env_remove("SWALLOW_OPERATOR_JWT_CONFIG_FILE")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let body=json!({"requestId":id,"channel":"slack","targetId":"ops","attemptId":attempt,"decision":"retry","reason":"review"}).to_string();
    let mut replayed = false;
    for _ in 0..100 {
        let output=Command::new("curl").args(["--silent","--max-time","1","--header",&format!("Authorization: Bearer {refreshed}"),"--header","Content-Type: application/json","--data",&body,&format!("http://127.0.0.1:{port}/v1/experimental/operator/alerts/resolve?environmentId=jwt-decisions")]).output().unwrap();
        if output.status.success()
            && serde_json::from_slice::<Value>(&output.stdout)
                .is_ok_and(|value| value["replayed"] == true)
        {
            replayed = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(replayed, "API process did not replay the committed request");
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM public.novu_rust_operator_jwt_decision_audit",
            &[]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        1
    );
    assert_eq!(db.query_one("SELECT count(*) FROM public.novu_rust_alert_delivery_resolutions WHERE environment_id='jwt-decisions'",&[]).await.unwrap().get::<_,i64>(0),1);
    assert_eq!(
        app.clone()
            .oneshot(post(&alice, id, attempt, "retry", "changed"))
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
    let (_, found) = json_body(app.clone().oneshot(lookup(&refreshed, id)).await.unwrap()).await;
    assert_eq!(found["found"], true);
    let (_, not_found) = json_body(app.clone().oneshot(lookup(&bob, id)).await.unwrap()).await;
    assert_eq!(not_found["found"], false);
    assert_eq!(
        app.clone()
            .oneshot(lookup(&eve, id))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    let next = Uuid::new_v4();
    db.execute("INSERT INTO public.novu_rust_alert_delivery_attempts(id,environment_id,channel,target_id,state) VALUES($1,'jwt-decisions','slack','ops','unknown')",&[&next]).await.unwrap();
    db.execute("UPDATE public.novu_rust_alert_delivery_slots SET state='unknown',attempt_id=$1 WHERE environment_id='jwt-decisions' AND target_id='ops'",&[&next]).await.unwrap();
    assert_eq!(
        app.clone()
            .oneshot(post(
                &alice,
                Uuid::new_v4(),
                attempt,
                "retry",
                "old attempt"
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
    db.batch_execute("CREATE FUNCTION public.test_jwt_decision_reject() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture'; END $$; CREATE TRIGGER test_jwt_decision_reject BEFORE INSERT ON public.novu_rust_operator_jwt_decision_audit FOR EACH ROW EXECUTE FUNCTION public.test_jwt_decision_reject()").await.unwrap();
    let failed = Uuid::new_v4();
    assert_eq!(
        app.clone()
            .oneshot(post(&alice, failed, next, "retry", "audit fail"))
            .await
            .unwrap()
            .status(),
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(db.query_one("SELECT count(*) FROM public.novu_rust_operator_jwt_decision_requests WHERE request_id=$1",&[&failed]).await.unwrap().get::<_,i64>(0),0);
    assert_eq!(db.query_one("SELECT state FROM public.novu_rust_alert_delivery_slots WHERE environment_id='jwt-decisions' AND target_id='ops'",&[]).await.unwrap().get::<_,String>(0),"unknown");
    db.batch_execute("DROP TRIGGER test_jwt_decision_reject ON public.novu_rust_operator_jwt_decision_audit; DROP FUNCTION public.test_jwt_decision_reject()").await.unwrap();
    let mut blocker = connect(&url).await;
    let tx = blocker.transaction().await.unwrap();
    tx.execute("UPDATE public.novu_rust_operator_jwt_identities SET alert_retry=false WHERE subject='alice' AND environment_id='jwt-decisions'",&[]).await.unwrap();
    let pending = tokio::spawn({
        let app = app.clone();
        let alice = alice.clone();
        async move {
            app.oneshot(post(
                &alice,
                Uuid::new_v4(),
                next,
                "retry",
                "concurrent revoke",
            ))
            .await
            .unwrap()
            .status()
        }
    });
    let mut waiting = false;
    for _ in 0..100 {
        waiting=db.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE '%novu_rust_operator_jwt_resolve_alert%')",&[]).await.unwrap().get(0);
        if waiting {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(waiting);
    tx.commit().await.unwrap();
    assert_eq!(pending.await.unwrap(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        app.clone()
            .oneshot(lookup(&alice, id))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    db.execute("UPDATE public.novu_rust_operator_jwt_identities SET alert_retry=true,alert_accepted=true WHERE subject='alice' AND environment_id='jwt-decisions'",&[]).await.unwrap();
    let mut slot_locker = connect(&url).await;
    let tx = slot_locker.transaction().await.unwrap();
    tx.execute("UPDATE public.novu_rust_alert_delivery_slots SET state=state WHERE environment_id='jwt-decisions' AND target_id='ops'",&[]).await.unwrap();
    let short = jwt(
        "alice",
        "short",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 3,
    );
    let expiry_id = Uuid::new_v4();
    let pending = tokio::spawn({
        let app = app.clone();
        async move {
            app.oneshot(post(
                &short,
                expiry_id,
                next,
                "retry",
                "expires while waiting",
            ))
            .await
            .unwrap()
            .status()
        }
    });
    let mut waiting = false;
    for _ in 0..100 {
        waiting=db.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE '%novu_rust_operator_jwt_resolve_alert%')",&[]).await.unwrap().get(0);
        if waiting {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(waiting);
    tokio::time::sleep(std::time::Duration::from_secs(4)).await;
    tx.commit().await.unwrap();
    assert_eq!(pending.await.unwrap(), StatusCode::UNAUTHORIZED);
    assert_eq!(db.query_one("SELECT count(*) FROM public.novu_rust_operator_jwt_decision_requests WHERE request_id=$1",&[&expiry_id]).await.unwrap().get::<_,i64>(0),0);
    let (status, accepted) = json_body(
        app.oneshot(post(&alice, Uuid::new_v4(), next, "accepted", "verified"))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(accepted["replayed"], false);
    assert_eq!(db.query_one("SELECT state FROM public.novu_rust_alert_delivery_slots WHERE environment_id='jwt-decisions' AND target_id='ops'",&[]).await.unwrap().get::<_,String>(0),"accepted");
}
