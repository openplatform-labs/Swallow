//! A real alert process with a restricted DB login and a loopback health fixture.
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    process::Command,
};
use tokio_postgres::NoTls;
use uuid::Uuid;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a fully migrated disposable PostgreSQL database and role creation"]
async fn resend_bundle_probe_triggers_coordinated_alert_and_recovery() {
    let database = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, driver) = tokio_postgres::connect(&database, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    let suffix = Uuid::new_v4().simple().to_string();
    let environment = format!("key-probe-{suffix}");
    let role = format!("key_alert_{suffix}");
    db.execute(
        "INSERT INTO novu_rust_environments VALUES($1,'org')",
        &[&environment],
    )
    .await
    .unwrap();
    db.query_one("SELECT pg_advisory_lock(736281)", &[])
        .await
        .unwrap();
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_reserve_alert(text,text,text,integer,integer),public.novu_rust_finish_alert(text,text,text,uuid,text,integer),public.novu_rust_observe_alert_conditions(text,text,text,integer,integer) TO {role}")).await.unwrap();
    db.query_one("SELECT pg_advisory_unlock(736281)", &[])
        .await
        .unwrap();
    db.execute("INSERT INTO novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT $1,$2,'alert_send',oid FROM pg_roles WHERE rolname::text=$2", &[&environment,&role]).await.unwrap();
    let mut role_url = reqwest::Url::parse(&database).unwrap();
    role_url.set_username(&role).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let state = Arc::new(AtomicUsize::new(0));
    let serving = state.clone();
    let server = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mode = serving.load(Ordering::SeqCst);
            tokio::spawn(async move {
                let mut request = [0u8; 2048];
                let _ = socket.read(&mut request).await;
                let (status, body) = match mode {
                    0 => ("200 OK", "{\"status\":\"healthy\"}".to_string()),
                    1 => (
                        "503 Service Unavailable",
                        "{\"status\":\"unhealthy\"}".to_string(),
                    ),
                    2 => ("200 OK", "{\"status\":\"wrong\"}".to_string()),
                    3 => ("302 Found", "redirect".to_string()),
                    _ => ("200 OK", "x".repeat(4097)),
                };
                let response = format!(
                    "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = socket.write_all(response.as_bytes()).await;
            });
        }
    });
    let probe = format!("http://{address}/health/resend-webhook-bundle");
    let run = |url: &str, add_db_probe: bool| {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_send-database-alert"));
        cmd.env_clear()
            .args([&environment, "one", "60"])
            .env("DATABASE_URL", role_url.as_str())
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("SWALLOW_RESEND_WEBHOOK_KEY_PROBE_URL", url)
            .env(
                "SWALLOW_ALERT_SLACK_WEBHOOK",
                "https://hooks.slack.com/services/FAKE/FAKE/FAKE",
            )
            .env("HTTPS_PROXY", "http://127.0.0.1:1")
            .env("NO_PROXY", "127.0.0.1,localhost")
            .env("SWALLOW_ALERT_SCOPED_DELIVERY", "true")
            .env("SWALLOW_ALERT_COOLDOWN_POLICY", "new-condition")
            .env("SWALLOW_DB_ALERT_OPERATION_TIMEOUT_SECONDS", "3");
        if add_db_probe {
            cmd.env("SWALLOW_DB_PROBE_URL", &database);
        }
        cmd
    };
    let healthy = tokio::time::timeout(Duration::from_secs(10), run(&probe, false).output())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        healthy.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&healthy.stderr)
    );
    assert_eq!(
        run(&probe, true).output().await.unwrap().status.code(),
        Some(1)
    );
    assert_eq!(
        run("http://example.test/health/resend-webhook-bundle", false)
            .output()
            .await
            .unwrap()
            .status
            .code(),
        Some(1)
    );
    state.store(1, Ordering::SeqCst);
    let bad = tokio::time::timeout(Duration::from_secs(10), run(&probe, false).output())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        bad.status.code(),
        Some(2),
        "{}",
        String::from_utf8_lossy(&bad.stderr)
    );
    let row=db.query_one("SELECT state,last_condition_mask FROM novu_rust_alert_delivery_slots WHERE environment_id=$1 AND channel='slack' AND target_id='resend-key-one'", &[&environment]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "unknown");
    assert_eq!(row.get::<_, i32>(1) & (1 << 20), 1 << 20);
    state.store(2, Ordering::SeqCst);
    assert_eq!(
        run(&probe, false).output().await.unwrap().status.code(),
        Some(2)
    );
    for mode in [3, 4] {
        state.store(mode, Ordering::SeqCst);
        assert_eq!(
            run(&probe, false).output().await.unwrap().status.code(),
            Some(2)
        );
    }
    let attempts:i64=db.query_one("SELECT count(*) FROM novu_rust_alert_delivery_attempts WHERE environment_id=$1 AND target_id='resend-key-one'", &[&environment]).await.unwrap().get(0);
    assert_eq!(attempts, 1);
    state.store(0, Ordering::SeqCst);
    assert_eq!(
        run(&probe, false).output().await.unwrap().status.code(),
        Some(0)
    );
    let mask:i32=db.query_one("SELECT last_condition_mask FROM novu_rust_alert_delivery_slots WHERE environment_id=$1 AND target_id='resend-key-one'", &[&environment]).await.unwrap().get(0);
    assert_eq!(
        mask & (1 << 20),
        1 << 20,
        "unknown delivery must remain pending review"
    );
    // Fixture-only transition represents an independently reviewed accepted attempt.
    db.execute("UPDATE novu_rust_alert_delivery_slots SET state='accepted' WHERE environment_id=$1 AND target_id='resend-key-one'", &[&environment]).await.unwrap();
    assert_eq!(
        run(&probe, false).output().await.unwrap().status.code(),
        Some(0)
    );
    let recovered:i32=db.query_one("SELECT last_condition_mask FROM novu_rust_alert_delivery_slots WHERE environment_id=$1 AND target_id='resend-key-one'", &[&environment]).await.unwrap().get(0);
    assert_eq!(recovered & (1 << 20), 0);
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires migrated disposable PostgreSQL and a built API binary"]
async fn real_resend_bundle_failure_triggers_alert_and_recovers() {
    use std::{fs, net::TcpListener as StdListener, process::Stdio};
    struct Child(tokio::process::Child);
    impl Drop for Child {
        fn drop(&mut self) {
            let _ = self.0.start_kill();
        }
    }
    struct Dir(std::path::PathBuf);
    impl Drop for Dir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    async fn wait_for(client: &reqwest::Client, url: &str, expected: u16) {
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
                "health did not reach {expected}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    let database = std::env::var("TEST_DATABASE_URL").unwrap();
    let binary = std::env::var("TEST_API_BINARY").unwrap();
    let (db, driver) = tokio_postgres::connect(&database, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    let suffix = Uuid::new_v4().simple().to_string();
    let env = format!("key-e2e-{suffix}");
    let alert_role = format!("key_e2e_alert_{suffix}");
    let api_role = format!("key_e2e_api_{suffix}");
    db.execute(
        "INSERT INTO novu_rust_environments VALUES($1,'org')",
        &[&env],
    )
    .await
    .unwrap();
    db.query_one("SELECT pg_advisory_lock(736281)", &[])
        .await
        .unwrap();
    db.batch_execute(&format!("CREATE ROLE {alert_role} LOGIN; CREATE ROLE {api_role} LOGIN; GRANT USAGE ON SCHEMA public TO {alert_role}; GRANT EXECUTE ON FUNCTION public.novu_rust_reserve_alert(text,text,text,integer,integer),public.novu_rust_finish_alert(text,text,text,uuid,text,integer),public.novu_rust_observe_alert_conditions(text,text,text,integer,integer) TO {alert_role}")).await.unwrap();
    db.query_one("SELECT pg_advisory_unlock(736281)", &[])
        .await
        .unwrap();
    db.execute("INSERT INTO novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT $1,$2,'alert_send',oid FROM pg_roles WHERE rolname::text=$2", &[&env,&alert_role]).await.unwrap();
    let mut alert_url = reqwest::Url::parse(&database).unwrap();
    alert_url.set_username(&alert_role).unwrap();
    let mut api_url = reqwest::Url::parse(&database).unwrap();
    api_url.set_username(&api_role).unwrap();
    let dir = Dir(std::env::temp_dir().join(format!("swallow-real-key-alert-{suffix}")));
    fs::create_dir(&dir.0).unwrap();
    let file = dir.0.join("keys.json");
    let valid = serde_json::json!({"current":"whsec_MDEyMzQ1Njc4OWFiY2RlZg=="}).to_string();
    fs::write(&file, &valid).unwrap();
    let port = StdListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let mut api = Child(
        Command::new(binary)
            .env_clear()
            .env("DATABASE_URL", api_url.as_str())
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("LISTEN_ADDR", format!("127.0.0.1:{port}"))
            .env("NOVU_RESEND_WEBHOOK_ENVIRONMENT_ID", &env)
            .env("NOVU_RESEND_WEBHOOK_SECRET_BUNDLE_FILE", &file)
            .env("SWALLOW_API_HEALTH_ENDPOINTS_ENABLED", "true")
            .env("SWALLOW_API_RESEND_WEBHOOK_BUNDLE_HEALTH_ENABLED", "true")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let url = format!("http://127.0.0.1:{port}/health/resend-webhook-bundle");
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    let run = || {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_send-database-alert"));
        cmd.env_clear()
            .args([&env, "instance", "60"])
            .env("DATABASE_URL", alert_url.as_str())
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("SWALLOW_RESEND_WEBHOOK_KEY_PROBE_URL", &url)
            .env(
                "SWALLOW_ALERT_SLACK_WEBHOOK",
                "https://hooks.slack.com/services/FAKE/FAKE/FAKE",
            )
            .env("HTTPS_PROXY", "http://127.0.0.1:1")
            .env("NO_PROXY", "127.0.0.1,localhost")
            .env("SWALLOW_ALERT_SCOPED_DELIVERY", "true")
            .env("SWALLOW_ALERT_COOLDOWN_POLICY", "new-condition")
            .env("SWALLOW_DB_ALERT_OPERATION_TIMEOUT_SECONDS", "3");
        cmd
    };
    wait_for(&client, &url, 200).await;
    assert_eq!(run().output().await.unwrap().status.code(), Some(0));
    fs::write(dir.0.join("new"), "{").unwrap();
    fs::rename(dir.0.join("new"), &file).unwrap();
    wait_for(&client, &url, 503).await;
    assert_eq!(run().output().await.unwrap().status.code(), Some(2));
    assert_eq!(run().output().await.unwrap().status.code(), Some(2));
    let row=db.query_one("SELECT state, (SELECT count(*) FROM novu_rust_alert_delivery_attempts a WHERE a.environment_id=s.environment_id AND a.target_id=s.target_id) FROM novu_rust_alert_delivery_slots s WHERE s.environment_id=$1 AND s.target_id='resend-key-instance'", &[&env]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "unknown");
    assert_eq!(row.get::<_, i64>(1), 1);
    fs::write(dir.0.join("new"), &valid).unwrap();
    fs::rename(dir.0.join("new"), &file).unwrap();
    wait_for(&client, &url, 200).await;
    assert_eq!(run().output().await.unwrap().status.code(), Some(0));
    let state:String=db.query_one("SELECT state FROM novu_rust_alert_delivery_slots WHERE environment_id=$1 AND target_id='resend-key-instance'", &[&env]).await.unwrap().get(0);
    assert_eq!(state, "unknown");
    // Run the production scheduler and fanout against the same live API.
    let hook = dir.0.join("slack-webhook");
    fs::write(&hook, "https://hooks.slack.com/services/FAKE/FAKE/FAKE").unwrap();
    let targets = dir.0.join("targets.json");
    fs::write(&targets,serde_json::json!({"targets":[{"channel":"slack","id":"scheduled-instance","webhook_file":hook}]}).to_string()).unwrap();
    let log = dir.0.join("scheduler.log");
    let log_out = fs::File::create(&log).unwrap();
    let log_err = log_out.try_clone().unwrap();
    let mut scheduler = Child(
        Command::new(env!("CARGO_BIN_EXE_alert-scheduler"))
            .env_clear()
            .args([
                "1",
                "database",
                &env,
                "instance",
                "60",
                targets.to_str().unwrap(),
            ])
            .env("DATABASE_URL", alert_url.as_str())
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("SWALLOW_RESEND_WEBHOOK_KEY_PROBE_URL", &url)
            .env("HTTPS_PROXY", "http://127.0.0.1:1")
            .env("NO_PROXY", "127.0.0.1,localhost")
            .env("SWALLOW_ALERT_SCOPED_DELIVERY", "true")
            .env("SWALLOW_ALERT_COOLDOWN_POLICY", "new-condition")
            .env("SWALLOW_DB_ALERT_OPERATION_TIMEOUT_SECONDS", "3")
            .stdout(Stdio::from(log_out))
            .stderr(Stdio::from(log_err))
            .spawn()
            .unwrap(),
    );
    async fn wait_log(path: &std::path::Path, needle: &str, count: usize) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            if fs::read_to_string(path)
                .unwrap_or_default()
                .matches(needle)
                .count()
                >= count
            {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "scheduler did not reach expected status"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    wait_log(&log, "alert scheduler: completed", 1).await;
    fs::write(dir.0.join("new"), "{").unwrap();
    fs::rename(dir.0.join("new"), &file).unwrap();
    wait_for(&client, &url, 503).await;
    wait_log(&log, "alert scheduler: attention required", 2).await;
    let row=db.query_one("SELECT state, (SELECT count(*) FROM novu_rust_alert_delivery_attempts a WHERE a.environment_id=s.environment_id AND a.target_id=s.target_id) FROM novu_rust_alert_delivery_slots s WHERE s.environment_id=$1 AND s.target_id='scheduled-instance'", &[&env]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "unknown");
    assert_eq!(row.get::<_, i64>(1), 1);
    fs::write(dir.0.join("new"), &valid).unwrap();
    fs::rename(dir.0.join("new"), &file).unwrap();
    wait_for(&client, &url, 200).await;
    wait_log(&log, "alert scheduler: completed", 2).await;
    let state:String=db.query_one("SELECT state FROM novu_rust_alert_delivery_slots WHERE environment_id=$1 AND target_id='scheduled-instance'", &[&env]).await.unwrap().get(0);
    assert_eq!(state, "unknown");
    #[cfg(unix)]
    {
        unsafe {
            libc::kill(scheduler.0.id().unwrap() as i32, libc::SIGTERM);
        }
    }
    #[cfg(not(unix))]
    {
        scheduler.0.start_kill().unwrap();
    }
    tokio::time::timeout(Duration::from_secs(10), scheduler.0.wait())
        .await
        .unwrap()
        .unwrap();
    api.0.kill().await.unwrap();
    api.0.wait().await.unwrap();
}
