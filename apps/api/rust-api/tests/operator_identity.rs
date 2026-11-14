//! Restricted JWT identity lookup against disposable PostgreSQL.
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
fn token(subject: &str, client: &str, expires: u64) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let mut h = Header::new(Algorithm::RS256);
    h.typ = Some("at+jwt".into());
    h.kid = Some("key".into());
    encode(&h,&json!({"iss":"https://issuer.test","sub":subject,"aud":"swallow","exp":expires,"iat":now-1,"jti":"one","client_id":client}),&EncodingKey::from_rsa_pem(PRIVATE).unwrap()).unwrap()
}
fn request(token: &str, query: &str) -> Request<Body> {
    Request::builder()
        .uri(format!("/v1/experimental/operator/me?{query}"))
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap()
}
#[tokio::test]
#[ignore = "requires fully migrated disposable PostgreSQL and role creation"]
async fn verifies_token_mapping_scope_revocation_and_expiry() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let db = connect(&url).await;
    db.batch_execute(
        "INSERT INTO public.novu_rust_environments VALUES('jwt-a','org'),('jwt-b','org')",
    )
    .await
    .unwrap();
    let role = format!("jwt_api_{}", Uuid::new_v4().simple());
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_self(text,text,text,text,bigint),public.novu_rust_operator_jwt_subscribers(text,text,text,text,bigint,text,text,text) TO {role}")).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_operator_jwt_api_logins SELECT rolname,oid FROM pg_roles WHERE rolname::text=$1",&[&role]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id,subscriber_read,alert_retry) VALUES('https://issuer.test','user-1','console','jwt-a',true,true)",&[]).await.unwrap();
    db.batch_execute("INSERT INTO public.novu_rust_subscribers(environment_id,subscriber_id,internal_id,profile) VALUES ('jwt-a','alice','alice-id','{\"firstName\":\"Alice\",\"email\":\"alice@example.test\",\"secret\":\"hidden\"}'),('jwt-b','bob','bob-id','{\"firstName\":\"Bob\"}')").await.unwrap();
    let actor_url = url.replacen("postgres://postgres@", &format!("postgres://{role}@"), 1);
    let api_db = connect(&actor_url).await;
    assert!(
        api_db
            .query(
                "SELECT * FROM public.novu_rust_operator_jwt_identities",
                &[]
            )
            .await
            .is_err()
    );
    let verifier = Arc::new(
        AccessTokenVerifier::new("https://issuer.test", "swallow", &[("key", PUBLIC)]).unwrap(),
    );
    let bare = novu_rust_api::router(Arc::new(connect(&actor_url).await));
    let app = novu_rust_api::router(Arc::new(api_db)).layer(Extension(verifier));
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let good = token("user-1", "console", now + 120);
    assert_eq!(
        bare.oneshot(request(&good, "environmentId=jwt-a"))
            .await
            .unwrap()
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    let response = app
        .clone()
        .oneshot(request(&good, "environmentId=jwt-a"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
    let body: Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(body["subject"], "user-1");
    assert_eq!(body["provisionedPermissions"]["subscriberRead"], true);
    assert_eq!(body["provisionedPermissions"]["alertRead"], false);
    let subscriber_request = || {
        Request::builder()
            .uri("/v1/experimental/operator/subscribers?environmentId=jwt-a&name=Alice")
            .header(header::AUTHORIZATION, format!("Bearer {good}"))
            .body(Body::empty())
            .unwrap()
    };
    let response = app.clone().oneshot(subscriber_request()).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let list: Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(list["data"].as_array().unwrap().len(), 1);
    assert_eq!(list["data"][0]["subscriberId"], "alice");
    assert!(list["data"][0].get("secret").is_none());
    db.execute("UPDATE public.novu_rust_operator_jwt_identities SET subscriber_read=false WHERE environment_id='jwt-a'",&[]).await.unwrap();
    assert_eq!(
        app.clone()
            .oneshot(subscriber_request())
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    db.execute("UPDATE public.novu_rust_operator_jwt_identities SET subscriber_read=true WHERE environment_id='jwt-a'",&[]).await.unwrap();
    let mut scope_blocker = connect(&url).await;
    let scope_tx = scope_blocker.transaction().await.unwrap();
    scope_tx.execute("UPDATE public.novu_rust_operator_jwt_identities SET subscriber_read=false WHERE environment_id='jwt-a'",&[]).await.unwrap();
    let pending_scope = tokio::spawn({
        let app = app.clone();
        let good = good.clone();
        async move {
            let request = Request::builder()
                .uri("/v1/experimental/operator/subscribers?environmentId=jwt-a")
                .header(header::AUTHORIZATION, format!("Bearer {good}"))
                .body(Body::empty())
                .unwrap();
            app.oneshot(request).await.unwrap().status()
        }
    });
    let mut waiting_scope = false;
    for _ in 0..100 {
        waiting_scope=db.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE '%novu_rust_operator_jwt_subscribers%')",&[]).await.unwrap().get(0);
        if waiting_scope {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(waiting_scope);
    scope_tx.commit().await.unwrap();
    assert_eq!(pending_scope.await.unwrap(), StatusCode::UNAUTHORIZED);
    db.execute("UPDATE public.novu_rust_operator_jwt_identities SET subscriber_read=true WHERE environment_id='jwt-a'",&[]).await.unwrap();
    for (key, query, expected) in [
        (&good, "environmentId=jwt-b", StatusCode::UNAUTHORIZED),
        (
            &good,
            "environmentId=jwt-a&environmentId=jwt-b",
            StatusCode::BAD_REQUEST,
        ),
        (
            &token("user-2", "console", now + 120),
            "environmentId=jwt-a",
            StatusCode::UNAUTHORIZED,
        ),
        (
            &token("user-1", "other", now + 120),
            "environmentId=jwt-a",
            StatusCode::UNAUTHORIZED,
        ),
    ] {
        assert_eq!(
            app.clone()
                .oneshot(request(key, query))
                .await
                .unwrap()
                .status(),
            expected
        );
    }
    let duplicate = Request::builder()
        .uri("/v1/experimental/operator/me?environmentId=jwt-a")
        .header(header::AUTHORIZATION, format!("Bearer {good}"))
        .header(header::AUTHORIZATION, format!("Bearer {good}"))
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        app.clone().oneshot(duplicate).await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
    let mut blocker = connect(&url).await;
    let tx = blocker.transaction().await.unwrap();
    tx.execute("UPDATE public.novu_rust_operator_jwt_identities SET enabled=false WHERE environment_id='jwt-a'",&[]).await.unwrap();
    let pending = tokio::spawn({
        let app = app.clone();
        let good = good.clone();
        async move {
            app.oneshot(request(&good, "environmentId=jwt-a"))
                .await
                .unwrap()
                .status()
        }
    });
    let mut waiting = false;
    for _ in 0..100 {
        waiting=db.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE '%novu_rust_operator_jwt_self%')",&[]).await.unwrap().get(0);
        if waiting {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(waiting);
    tx.commit().await.unwrap();
    assert_eq!(pending.await.unwrap(), StatusCode::UNAUTHORIZED);
    db.execute("UPDATE public.novu_rust_operator_jwt_identities SET enabled=true WHERE environment_id='jwt-a'",&[]).await.unwrap();
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = socket.local_addr().unwrap().port();
    drop(socket);
    let config=json!({"issuer":"https://issuer.test","audience":"swallow","keys":[{"kid":"key","pem":std::str::from_utf8(PUBLIC).unwrap()}]}).to_string();
    let _server = ApiProcess(
        Command::new(env!("CARGO_BIN_EXE_novu-rust-api"))
            .env("DATABASE_URL", &actor_url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("LISTEN_ADDR", format!("127.0.0.1:{port}"))
            .env("SWALLOW_OPERATOR_JWT_CONFIG", config)
            .env_remove("SWALLOW_OPERATOR_JWT_CONFIG_FILE")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let mut success = false;
    for _ in 0..100 {
        let output = Command::new("curl")
            .args([
                "--silent",
                "--max-time",
                "1",
                "--write-out",
                "%{http_code}",
                "--output",
                "/dev/null",
                "--header",
                &format!("Authorization: Bearer {good}"),
                &format!("http://127.0.0.1:{port}/v1/experimental/operator/me?environmentId=jwt-a"),
            ])
            .output()
            .unwrap();
        if output.status.success() && output.stdout == b"200" {
            success = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(
        success,
        "configured API process did not authenticate the signed token"
    );
    let result = Command::new("curl")
        .args([
            "--silent",
            "--max-time",
            "2",
            "--write-out",
            "%{http_code}",
            "--output",
            "/dev/null",
            "--header",
            &format!("Authorization: Bearer {good}"),
            &format!(
                "http://127.0.0.1:{port}/v1/experimental/operator/subscribers?environmentId=jwt-a"
            ),
        ])
        .output()
        .unwrap();
    assert!(result.status.success());
    assert_eq!(result.stdout, b"200");
    let tx = blocker.transaction().await.unwrap();
    tx.execute("UPDATE public.novu_rust_operator_jwt_identities SET enabled=enabled WHERE environment_id='jwt-a'",&[]).await.unwrap();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let short = token("user-1", "console", now + 3);
    let pending = tokio::spawn({
        let app = app.clone();
        async move {
            app.oneshot(request(&short, "environmentId=jwt-a"))
                .await
                .unwrap()
                .status()
        }
    });
    let mut waiting = false;
    for _ in 0..100 {
        waiting=db.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE '%novu_rust_operator_jwt_self%')",&[]).await.unwrap().get(0);
        if waiting {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(waiting);
    tokio::time::sleep(std::time::Duration::from_secs(4)).await;
    tx.commit().await.unwrap();
    assert_eq!(pending.await.unwrap(), StatusCode::UNAUTHORIZED);
    db.execute(
        "DELETE FROM public.novu_rust_operator_jwt_api_logins WHERE database_principal::text=$1",
        &[&role],
    )
    .await
    .unwrap();
    assert_eq!(
        app.oneshot(request(&good, "environmentId=jwt-a"))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
}
