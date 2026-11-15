//! Test-only readiness-aware proxy, two real API processes and a disposable DB.
use axum::{
    Router,
    body::to_bytes,
    extract::{Request, State},
    http::{HeaderValue, StatusCode},
    response::{IntoResponse, Response},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use ring::hmac;
use std::{
    net::TcpListener as StdListener,
    path::Path,
    process::{Child, Command, Stdio},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::net::TcpListener;
use tokio_postgres::NoTls;
use uuid::Uuid;
struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
struct Directory(std::path::PathBuf);
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn replace(path: &Path, value: &str) {
    let tmp = path.with_extension("new");
    std::fs::write(&tmp, value).unwrap();
    std::fs::rename(tmp, path).unwrap();
}
#[derive(Clone)]
struct Balancer {
    client: reqwest::Client,
    backends: Vec<String>,
}
async fn proxy(State(state): State<Balancer>, req: Request) -> Response {
    let (parts, body) = req.into_parts();
    let body = match to_bytes(body, 1_048_576).await {
        Ok(v) => v,
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };
    for (index, base) in state.backends.iter().enumerate() {
        if !state
            .client
            .get(format!("{base}/health/ready"))
            .send()
            .await
            .ok()
            .is_some_and(|r| r.status() == StatusCode::OK)
        {
            continue;
        }
        let uri = parts
            .uri
            .path_and_query()
            .map(|v| v.as_str())
            .unwrap_or("/");
        let mut request = state
            .client
            .request(parts.method.clone(), format!("{base}{uri}"));
        for (name, value) in &parts.headers {
            if name != axum::http::header::HOST && name != axum::http::header::CONTENT_LENGTH {
                request = request.header(name, value);
            }
        }
        return match request.body(body.clone()).send().await {
            Ok(r) => {
                let mut result = r.status().into_response();
                result.headers_mut().insert(
                    "x-test-backend",
                    HeaderValue::from_static(if index == 0 { "a" } else { "b" }),
                );
                result
            }
            Err(_) => StatusCode::BAD_GATEWAY.into_response(),
        };
    }
    StatusCode::SERVICE_UNAVAILABLE.into_response()
}
async fn wait_status(client: &reqwest::Client, url: &str, expected: u16) {
    let end = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        if client
            .get(url)
            .send()
            .await
            .ok()
            .is_some_and(|r| r.status().as_u16() == expected)
        {
            return;
        }
        assert!(tokio::time::Instant::now() < end, "backend status mismatch");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
fn resend_request(client: &reqwest::Client, base: &str, event: &str) -> reqwest::RequestBuilder {
    let path = "/webhooks/organizations/org/environments/lb-env/email/resend";
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        .to_string();
    let body = r#"{"type":"email.delivered","data":{"email_id":"fixture-email"}}"#;
    let mut data = format!("{event}.{timestamp}.").into_bytes();
    data.extend_from_slice(body.as_bytes());
    let sig = format!(
        "v1,{}",
        STANDARD.encode(
            hmac::sign(
                &hmac::Key::new(hmac::HMAC_SHA256, b"0123456789abcdef"),
                &data
            )
            .as_ref()
        )
    );
    client
        .post(format!("{base}{path}"))
        .header("svix-id", event)
        .header("svix-timestamp", timestamp)
        .header("svix-signature", sig)
        .body(body)
}
fn twilio_request(client: &reqwest::Client, base: &str) -> reqwest::RequestBuilder {
    let path = "/webhooks/organizations/org/environments/lb-env/sms/twilio";
    let public = format!("https://events.example.test{path}");
    let account = format!("AC{}", "1".repeat(32));
    let message = format!("SM{}", "2".repeat(32));
    let body = format!("AccountSid={account}&MessageSid={message}&MessageStatus=delivered");
    let signed = format!("{public}AccountSid{account}MessageSid{message}MessageStatusdelivered");
    let sig = STANDARD.encode(
        hmac::sign(
            &hmac::Key::new(hmac::HMAC_SHA1_FOR_LEGACY_USE_ONLY, b"fixture-token"),
            signed.as_bytes(),
        )
        .as_ref(),
    );
    client
        .post(format!("{base}{path}"))
        .header("content-type", "application/x-www-form-urlencoded")
        .header("x-twilio-signature", sig)
        .body(body)
}
#[tokio::test]
#[ignore = "requires a disposable PostgreSQL database"]
async fn readiness_aware_proxy_avoids_broken_webhook_bundles() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    db.execute(
        "INSERT INTO public.novu_rust_environments VALUES('lb-env','org')",
        &[],
    )
    .await
    .unwrap();
    let dir =
        Directory(std::env::temp_dir().join(format!("swallow-webhook-lb-{}", Uuid::new_v4())));
    std::fs::create_dir(&dir.0).unwrap();
    let valid = r#"{"current":"whsec_MDEyMzQ1Njc4OWFiY2RlZg=="}"#;
    let twilio = r#"{"current":"fixture-token"}"#;
    let mut backends = Vec::new();
    let mut processes = Vec::new();
    let mut files = Vec::new();
    for i in 0..2 {
        let resend = dir.0.join(format!("resend-{i}"));
        let token = dir.0.join(format!("twilio-{i}"));
        replace(&resend, valid);
        replace(&token, twilio);
        files.push((resend.clone(), token.clone()));
        let port = StdListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let child=Command::new(env!("CARGO_BIN_EXE_novu-rust-api")).env_clear().env("DATABASE_URL",&url).env("NOVU_POSTGRES_ALLOW_INSECURE","true").env("LISTEN_ADDR",format!("127.0.0.1:{port}"))
 .env("SWALLOW_API_HEALTH_ENDPOINTS_ENABLED","true").env("SWALLOW_API_READY_REQUIRE_RESEND_WEBHOOK_BUNDLE","true").env("SWALLOW_API_READY_REQUIRE_TWILIO_WEBHOOK_BUNDLE","true")
 .env("NOVU_RESEND_WEBHOOK_ENVIRONMENT_ID","lb-env").env("NOVU_RESEND_WEBHOOK_SECRET_BUNDLE_FILE",&resend)
 .env("NOVU_TWILIO_WEBHOOK_ENVIRONMENT_ID","lb-env").env("NOVU_TWILIO_WEBHOOK_ACCOUNT_SID","AC11111111111111111111111111111111").env("NOVU_TWILIO_WEBHOOK_URL","https://events.example.test/webhooks/organizations/org/environments/lb-env/sms/twilio").env("NOVU_TWILIO_WEBHOOK_TOKEN_BUNDLE_FILE",&token)
 .stdout(Stdio::null()).stderr(Stdio::null()).spawn().unwrap();
        processes.push(Process(child));
        backends.push(format!("http://127.0.0.1:{port}"));
    }
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    for base in &backends {
        wait_status(&client, &format!("{base}/health/ready"), 200).await;
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_url = format!("http://{}", listener.local_addr().unwrap());
    let app = Router::new().fallback(proxy).with_state(Balancer {
        client: client.clone(),
        backends: backends.clone(),
    });
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    replace(&files[0].0, "invalid");
    wait_status(&client, &format!("{}/health/ready", backends[0]), 503).await;
    assert_eq!(
        resend_request(&client, &backends[0], "evt_direct")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    let response = resend_request(&client, &proxy_url, "evt_through_b")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(response.headers().get("x-test-backend").unwrap(), "b");
    let response = twilio_request(&client, &proxy_url).send().await.unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(response.headers().get("x-test-backend").unwrap(), "b");
    replace(&files[0].0, valid);
    wait_status(&client, &format!("{}/health/ready", backends[0]), 200).await;
    let response = resend_request(&client, &proxy_url, "evt_through_a")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(response.headers().get("x-test-backend").unwrap(), "a");
    replace(&files[0].1, "invalid");
    replace(&files[1].1, "invalid");
    for base in &backends {
        wait_status(&client, &format!("{base}/health/ready"), 503).await;
    }
    assert_eq!(
        twilio_request(&client, &proxy_url)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(db.query_one("SELECT count(*) FROM public.novu_rust_resend_delivery_events WHERE environment_id='lb-env'",&[]).await.unwrap().get::<_,i64>(0),2);
    assert_eq!(db.query_one("SELECT count(*) FROM public.novu_rust_twilio_delivery_events WHERE environment_id='lb-env'",&[]).await.unwrap().get::<_,i64>(0),1);
    server.abort();
    drop(processes);
}
