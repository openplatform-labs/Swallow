//! Actual API processes and an isolated PostgreSQL instance.
use std::{
    net::TcpListener,
    path::Path,
    process::{Child, Command, Stdio},
    time::Duration,
};
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
    let temp = path.with_extension("next");
    std::fs::write(&temp, value).unwrap();
    std::fs::rename(temp, path).unwrap();
}
async fn wait_status(client: &reqwest::Client, url: &str, expected: u16) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
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
        assert!(
            tokio::time::Instant::now() < deadline,
            "readiness did not reach {expected}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
#[tokio::test]
#[ignore = "requires disposable PostgreSQL"]
async fn selected_webhook_bundles_control_readiness_independently() {
    let db = std::env::var("TEST_DATABASE_URL").unwrap();
    let dir =
        Directory(std::env::temp_dir().join(format!("swallow-bundle-ready-{}", Uuid::new_v4())));
    std::fs::create_dir(&dir.0).unwrap();
    let resend = dir.0.join("resend");
    let twilio = dir.0.join("twilio");
    let resend_value = r#"{"current":"whsec_MDEyMzQ1Njc4OWFiY2RlZg=="}"#;
    let twilio_value = r#"{"current":"fixture-token"}"#;
    replace(&resend, resend_value);
    replace(&twilio, twilio_value);
    let mut processes = Vec::new();
    for (r, t) in [(false, false), (true, false), (false, true), (true, true)] {
        let port = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let process=Command::new(env!("CARGO_BIN_EXE_novu-rust-api")).env_clear().env("DATABASE_URL",&db).env("NOVU_POSTGRES_ALLOW_INSECURE","true").env("LISTEN_ADDR",format!("127.0.0.1:{port}"))
   .env("SWALLOW_API_HEALTH_ENDPOINTS_ENABLED","true").env("SWALLOW_API_READY_REQUIRE_RESEND_WEBHOOK_BUNDLE",r.to_string()).env("SWALLOW_API_READY_REQUIRE_TWILIO_WEBHOOK_BUNDLE",t.to_string())
   .env("NOVU_RESEND_WEBHOOK_ENVIRONMENT_ID","env").env("NOVU_RESEND_WEBHOOK_SECRET_BUNDLE_FILE",&resend)
   .env("NOVU_TWILIO_WEBHOOK_ENVIRONMENT_ID","env").env("NOVU_TWILIO_WEBHOOK_ACCOUNT_SID","AC11111111111111111111111111111111").env("NOVU_TWILIO_WEBHOOK_URL","https://events.example.test/webhooks/organizations/org/environments/env/sms/twilio").env("NOVU_TWILIO_WEBHOOK_TOKEN_BUNDLE_FILE",&twilio)
   .stdout(Stdio::null()).stderr(Stdio::null()).spawn().unwrap();
        processes.push((
            Process(process),
            format!("http://127.0.0.1:{port}/health/ready"),
            r,
            t,
        ));
    }
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    for (_, url, _, _) in &processes {
        wait_status(&client, url, 200).await;
    }
    replace(&resend, "invalid");
    for (_, url, r, _) in &processes {
        wait_status(&client, url, if *r { 503 } else { 200 }).await;
    }
    replace(&resend, resend_value);
    for (_, url, _, _) in &processes {
        wait_status(&client, url, 200).await;
    }
    replace(&twilio, "invalid");
    for (_, url, _, t) in &processes {
        wait_status(&client, url, if *t { 503 } else { 200 }).await;
    }
    replace(&twilio, twilio_value);
    for (_, url, _, _) in &processes {
        wait_status(&client, url, 200).await;
        let response = client.get(url).send().await.unwrap();
        assert_eq!(response.headers().get("cache-control").unwrap(), "no-store");
        let body = response.text().await.unwrap();
        assert!(!body.contains("fixture-token"));
        assert!(!body.contains(resend.to_str().unwrap()));
    }
}
#[test]
fn readiness_rejects_unconfigured_or_invalid_bundle_options() {
    for (key, value) in [
        ("SWALLOW_API_READY_REQUIRE_RESEND_WEBHOOK_BUNDLE", "true"),
        ("SWALLOW_API_READY_REQUIRE_TWILIO_WEBHOOK_BUNDLE", "true"),
        ("SWALLOW_API_READY_REQUIRE_RESEND_WEBHOOK_BUNDLE", "bad"),
        ("SWALLOW_API_READY_REQUIRE_TWILIO_WEBHOOK_BUNDLE", "bad"),
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_novu-rust-api"))
            .env_clear()
            .env("DATABASE_URL", "postgres://postgres@127.0.0.1:1/postgres")
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("SWALLOW_API_HEALTH_ENDPOINTS_ENABLED", "true")
            .env(key, value)
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .output()
            .unwrap();
        assert!(!output.status.success());
    }
}
