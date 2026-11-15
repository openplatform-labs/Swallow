//! Local HTTP fixture. All credentials and JWT keys are public test data.
use axum::{
    Router,
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::post,
};
use base64::{Engine, engine::general_purpose::STANDARD};
use novu_rust_api::{
    operator_access_token::VerifiedSubject,
    operator_introspection::{Introspection, Outcome},
};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

#[derive(Default)]
struct Fixture {
    mode: AtomicUsize,
    calls: AtomicUsize,
    request: Mutex<Option<(String, String)>>,
}
async fn endpoint(
    State(state): State<Arc<Fixture>>,
    headers: HeaderMap,
    body: Bytes,
) -> (StatusCode, String) {
    state.calls.fetch_add(1, Ordering::SeqCst);
    *state.request.lock().unwrap() = Some((
        headers
            .get("authorization")
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned(),
        String::from_utf8(body.to_vec()).unwrap(),
    ));
    match state.mode.load(Ordering::SeqCst) {
        0 => (StatusCode::OK,r#"{"active":true,"iss":"https://issuer.test","sub":"alice","client_id":"console","jti":"id-1","aud":["api","other"]}"#.into()),
        1 => (StatusCode::OK,r#"{"active":false}"#.into()),
        2 => (StatusCode::OK,r#"{"active":true,"sub":"other"}"#.into()),
        3 => (StatusCode::OK,"not json".into()),
        4 => (StatusCode::OK,"x".repeat(16385)),
        5 => (StatusCode::FOUND,"".into()),
        6 => {tokio::time::sleep(Duration::from_secs(2)).await;(StatusCode::OK,r#"{"active":true}"#.into())},
        7 => (StatusCode::OK,r#"{"active":true,"aud":["other"]}"#.into()),
        8 => (StatusCode::OK,r#"{"active":true,"exp":0}"#.into()),
        9 => (StatusCode::OK,r#"{"active":true,"exp":4102444800}"#.into()),
        10 => (StatusCode::OK,r#"{"active":true,"nbf":4102444800}"#.into()),
        _ => {let now=std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();(StatusCode::OK,serde_json::json!({"active":true,"exp":now+2}).to_string())},
    }
}
fn identity() -> VerifiedSubject {
    VerifiedSubject {
        issuer: "https://issuer.test".into(),
        subject: "alice".into(),
        client_id: "console".into(),
        token_id: "id-1".into(),
        expires_at: u64::MAX,
    }
}
#[tokio::test]
async fn checks_active_identity_bounded_errors_and_timeout() {
    let fixture = Arc::new(Fixture::default());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/introspect", listener.local_addr().unwrap());
    let task = tokio::spawn(
        axum::serve(
            listener,
            Router::new()
                .route("/introspect", post(endpoint))
                .with_state(fixture.clone()),
        )
        .into_future(),
    );
    assert!(
        Introspection::new(&url, "client id", "secret:+", false, Duration::from_secs(1)).is_err()
    );
    assert!(
        Introspection::new(
            "http://example.test/introspect",
            "id",
            "secret",
            true,
            Duration::from_secs(1)
        )
        .is_err()
    );
    assert!(
        Introspection::new(
            "https://user:pass@example.test/introspect",
            "id",
            "secret",
            false,
            Duration::from_secs(1)
        )
        .is_err()
    );
    let checker =
        Introspection::new(&url, "client id", "secret:+", true, Duration::from_secs(1)).unwrap();
    assert_eq!(
        checker.check("fixture-token", &identity(), "api").await,
        Outcome::Active(None)
    );
    let (auth, body) = fixture.request.lock().unwrap().clone().unwrap();
    assert_eq!(
        auth,
        format!("Basic {}", STANDARD.encode("client+id:secret%3A%2B"))
    );
    let form: std::collections::HashMap<_, _> = form_urlencoded::parse(body.as_bytes())
        .into_owned()
        .collect();
    assert_eq!(form.get("token").map(String::as_str), Some("fixture-token"));
    assert_eq!(
        form.get("token_type_hint").map(String::as_str),
        Some("access_token")
    );
    for (mode, expected) in [
        (1, Outcome::Inactive),
        (2, Outcome::Inactive),
        (3, Outcome::Unavailable),
        (4, Outcome::Unavailable),
        (5, Outcome::Unavailable),
        (6, Outcome::Unavailable),
        (7, Outcome::Inactive),
        (8, Outcome::Inactive),
    ] {
        fixture.mode.store(mode, Ordering::SeqCst);
        assert_eq!(
            checker.check("fixture-token", &identity(), "api").await,
            expected,
            "mode {mode}"
        );
    }
    fixture.mode.store(9, Ordering::SeqCst);
    assert_eq!(
        checker.check("fixture-token", &identity(), "api").await,
        Outcome::Active(Some(4102444800))
    );
    fixture.mode.store(10, Ordering::SeqCst);
    assert_eq!(
        checker.check("fixture-token", &identity(), "api").await,
        Outcome::Inactive
    );
    task.abort();
}

#[tokio::test]
async fn concurrency_limit_rejects_without_calling_idp_and_recovers() {
    let fixture = Arc::new(Fixture::default());
    fixture.mode.store(6, Ordering::SeqCst);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/introspect", listener.local_addr().unwrap());
    let server = tokio::spawn(
        axum::serve(
            listener,
            Router::new()
                .route("/introspect", post(endpoint))
                .with_state(fixture.clone()),
        )
        .into_future(),
    );
    assert!(
        Introspection::new(&url, "id", "secret", true, Duration::from_secs(1))
            .unwrap()
            .with_max_in_flight(0)
            .is_err()
    );
    assert!(
        Introspection::new(&url, "id", "secret", true, Duration::from_secs(1))
            .unwrap()
            .with_max_in_flight(1025)
            .is_err()
    );
    let checker = Arc::new(
        Introspection::new(&url, "id", "secret", true, Duration::from_secs(1))
            .unwrap()
            .with_max_in_flight(1)
            .unwrap(),
    );
    let first_checker = checker.clone();
    let first = tokio::spawn(async move { first_checker.check("first", &identity(), "api").await });
    tokio::time::timeout(Duration::from_secs(2), async {
        while fixture.calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        checker.check("second", &identity(), "api").await,
        Outcome::Unavailable
    );
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
    assert_eq!(first.await.unwrap(), Outcome::Unavailable);
    fixture.mode.store(0, Ordering::SeqCst);
    assert_eq!(
        checker.check("third", &identity(), "api").await,
        Outcome::Active(None)
    );
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 2);
    fixture.mode.store(6, Ordering::SeqCst);
    let cancelled_checker = checker.clone();
    let cancelled = tokio::spawn(async move {
        cancelled_checker
            .check("cancelled", &identity(), "api")
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while fixture.calls.load(Ordering::SeqCst) < 3 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    cancelled.abort();
    assert!(cancelled.await.unwrap_err().is_cancelled());
    fixture.mode.store(0, Ordering::SeqCst);
    assert_eq!(
        checker.check("after-cancel", &identity(), "api").await,
        Outcome::Active(None)
    );
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 4);
    server.abort();
}

#[tokio::test]
#[ignore = "requires fully migrated disposable PostgreSQL and role creation"]
async fn real_api_enforces_live_introspection_and_fails_closed() {
    use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
    use std::{
        process::{Child, Command, Stdio},
        time::{Instant, SystemTime, UNIX_EPOCH},
    };
    use uuid::Uuid;
    struct Process(Child);
    impl Drop for Process {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    struct Files(std::path::PathBuf);
    impl Drop for Files {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, driver) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let role = format!("introspection_{}", Uuid::new_v4().simple());
    let env = format!("intro-{}", Uuid::new_v4().simple());
    db.execute(
        "INSERT INTO public.novu_rust_environments VALUES ($1,'org')",
        &[&env],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) VALUES ('https://issuer.test','alice','console',$1)",&[&env]).await.unwrap();
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_operator_jwt_self(text,text,text,text,bigint),public.novu_rust_operator_token_revoked(bytea,bytea,bytea,bytea),public.novu_rust_operator_token_guard(bytea,bytea,bytea,bytea),public.novu_rust_operator_jwt_self_revoke(text,text,text,text,bigint,bytea) TO {role}")).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_operator_jwt_api_logins SELECT rolname,oid FROM pg_roles WHERE rolname=$1",&[&role]).await.unwrap();
    let fixture = Arc::new(Fixture::default());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint_url = format!("http://{}/introspect", listener.local_addr().unwrap());
    let server = tokio::spawn(
        axum::serve(
            listener,
            Router::new()
                .route("/introspect", post(endpoint))
                .with_state(fixture.clone()),
        )
        .into_future(),
    );
    let files =
        Files(std::env::temp_dir().join(format!("swallow-introspection-{}", Uuid::new_v4())));
    std::fs::create_dir(&files.0).unwrap();
    let config = files.0.join("jwt.json");
    let secret = files.0.join("secret");
    let logs = files.0.join("api.log");
    let public =
        std::str::from_utf8(include_bytes!("fixtures/operator_jwt/test-only-public.pem")).unwrap();
    std::fs::write(&config,serde_json::to_vec(&serde_json::json!({"issuer":"https://issuer.test","audience":"api","check_token_revocation":true,"keys":[{"kid":"key","pem":public}]})).unwrap()).unwrap();
    std::fs::write(&secret, "fixture-introspection-secret").unwrap();
    let mut db_url = reqwest::Url::parse(&url).unwrap();
    db_url.set_username(&role).unwrap();
    db_url.set_password(None).unwrap();
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = socket.local_addr().unwrap().port();
    drop(socket);
    let output = std::fs::File::create(&logs).unwrap();
    let _api = Process(
        Command::new(env!("CARGO_BIN_EXE_novu-rust-api"))
            .env_clear()
            .env("DATABASE_URL", db_url.as_str())
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("LISTEN_ADDR", format!("127.0.0.1:{port}"))
            .env("SWALLOW_API_HEALTH_ENDPOINTS_ENABLED", "true")
            .env("SWALLOW_OPERATOR_JWT_CONFIG_FILE", &config)
            .env("SWALLOW_OPERATOR_INTROSPECTION_URL", &endpoint_url)
            .env("SWALLOW_OPERATOR_INTROSPECTION_CLIENT_ID", "fixture-client")
            .env("SWALLOW_OPERATOR_INTROSPECTION_CLIENT_SECRET_FILE", &secret)
            .env("SWALLOW_OPERATOR_INTROSPECTION_ALLOW_LOOPBACK_HTTP", "true")
            .env("SWALLOW_OPERATOR_INTROSPECTION_TIMEOUT_SECONDS", "1")
            .env("SWALLOW_OPERATOR_INTROSPECTION_MAX_IN_FLIGHT", "1")
            .stdout(Stdio::from(output.try_clone().unwrap()))
            .stderr(Stdio::from(output))
            .spawn()
            .unwrap(),
    );
    let client = reqwest::Client::new();
    let origin = format!("http://127.0.0.1:{port}");
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if client
            .get(format!("{origin}/health/live"))
            .send()
            .await
            .is_ok_and(|r| r.status() == 200)
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "API failed to start: {}",
            std::fs::read_to_string(&logs).unwrap_or_default()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let mut header = Header::new(Algorithm::RS256);
    header.typ = Some("at+jwt".into());
    header.kid = Some("key".into());
    let token=encode(&header,&serde_json::json!({"iss":"https://issuer.test","sub":"alice","client_id":"console","jti":"id-1","aud":"api","iat":now-1,"exp":now+120}),&EncodingKey::from_rsa_pem(include_bytes!("fixtures/operator_jwt/test-only-private.pem")).unwrap()).unwrap();
    let request = |token: String| {
        let client = client.clone();
        let path = format!("{origin}/v1/experimental/operator/me?environmentId={env}");
        async move {
            client
                .get(path)
                .bearer_auth(token)
                .send()
                .await
                .unwrap()
                .status()
        }
    };
    assert_eq!(request("invalid".into()).await, StatusCode::UNAUTHORIZED);
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
    assert_eq!(request(token.clone()).await, StatusCode::OK);
    for (mode, status) in [
        (1, StatusCode::UNAUTHORIZED),
        (2, StatusCode::UNAUTHORIZED),
        (3, StatusCode::SERVICE_UNAVAILABLE),
        (4, StatusCode::SERVICE_UNAVAILABLE),
        (5, StatusCode::SERVICE_UNAVAILABLE),
        (6, StatusCode::SERVICE_UNAVAILABLE),
    ] {
        fixture.mode.store(mode, Ordering::SeqCst);
        assert_eq!(request(token.clone()).await, status, "mode {mode}");
    }
    fixture.mode.store(6, Ordering::SeqCst);
    let calls = fixture.calls.load(Ordering::SeqCst);
    let pending = tokio::spawn({
        let client = client.clone();
        let path = format!("{origin}/v1/experimental/operator/me?environmentId={env}");
        let token = token.clone();
        async move {
            client
                .get(path)
                .bearer_auth(token)
                .send()
                .await
                .unwrap()
                .status()
        }
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while fixture.calls.load(Ordering::SeqCst) == calls {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        request(token.clone()).await,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(fixture.calls.load(Ordering::SeqCst), calls + 1);
    assert_eq!(pending.await.unwrap(), StatusCode::SERVICE_UNAVAILABLE);
    fixture.mode.store(0, Ordering::SeqCst);
    assert_eq!(request(token.clone()).await, StatusCode::OK);
    // The introspected expiry must also fence an operator request waiting for a DB lock.
    let (mut locker, driver) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let tx = locker.transaction().await.unwrap();
    tx.execute("UPDATE public.novu_rust_operator_jwt_identities SET enabled=enabled WHERE environment_id=$1 AND subject='alice'",&[&env]).await.unwrap();
    fixture.mode.store(11, Ordering::SeqCst);
    let pending = tokio::spawn({
        let client = client.clone();
        let path = format!("{origin}/v1/experimental/operator/me?environmentId={env}");
        let token = token.clone();
        async move {
            client
                .get(path)
                .bearer_auth(token)
                .send()
                .await
                .unwrap()
                .status()
        }
    });
    let mut blocked = false;
    for _ in 0..100 {
        if db.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE '%novu_rust_operator_jwt_self%')",&[]).await.unwrap().get::<_,bool>(0){blocked=true;break;}
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(blocked, "operator request never reached the identity lock");
    tokio::time::sleep(Duration::from_secs(3)).await;
    tx.commit().await.unwrap();
    assert_eq!(pending.await.unwrap(), StatusCode::UNAUTHORIZED);
    fixture.mode.store(0, Ordering::SeqCst);
    let revoke = client
        .post(format!(
            "{origin}/v1/experimental/operator/token/revoke-self?environmentId={env}"
        ))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(revoke.status(), StatusCode::OK);
    let calls = fixture.calls.load(Ordering::SeqCst);
    fixture.mode.store(6, Ordering::SeqCst);
    assert_eq!(request(token.clone()).await, StatusCode::UNAUTHORIZED);
    assert_eq!(
        fixture.calls.load(Ordering::SeqCst),
        calls,
        "locally revoked token must not reach IdP"
    );
    let log = std::fs::read_to_string(&logs).unwrap_or_default();
    assert!(!log.contains("fixture-introspection-secret"));
    assert!(!log.contains(&token));
    server.abort();
}

#[test]
fn invalid_or_partial_introspection_configuration_fails_startup_without_secrets() {
    use std::{
        process::{Command, Stdio},
        time::Instant,
    };
    let dir = std::env::temp_dir().join(format!("swallow-intro-config-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&dir).unwrap();
    let config = dir.join("jwt.json");
    let secret = dir.join("secret");
    let public =
        std::str::from_utf8(include_bytes!("fixtures/operator_jwt/test-only-public.pem")).unwrap();
    std::fs::write(&config,serde_json::to_vec(&serde_json::json!({"issuer":"https://issuer.test","audience":"api","keys":[{"kid":"key","pem":public}]})).unwrap()).unwrap();
    std::fs::write(&secret, "fixture-sensitive-secret").unwrap();
    for mode in 0..6 {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_novu-rust-api"));
        cmd.env_clear()
            .env("DATABASE_URL", "postgres://127.0.0.1:1/test")
            .env(
                "SWALLOW_OPERATOR_INTROSPECTION_URL",
                "http://127.0.0.1:4321/test",
            )
            .env("SWALLOW_OPERATOR_INTROSPECTION_CLIENT_ID", "client")
            .env("SWALLOW_OPERATOR_INTROSPECTION_CLIENT_SECRET_FILE", &secret)
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        if mode != 0 {
            cmd.env("SWALLOW_OPERATOR_JWT_CONFIG_FILE", &config);
        }
        match mode {
            0 => {
                cmd.env("SWALLOW_OPERATOR_INTROSPECTION_ALLOW_LOOPBACK_HTTP", "true");
            }
            1 => {}
            2 => {
                cmd.env("SWALLOW_OPERATOR_INTROSPECTION_ALLOW_LOOPBACK_HTTP", "true")
                    .env("SWALLOW_OPERATOR_INTROSPECTION_TIMEOUT_SECONDS", "0");
            }
            3 => {
                cmd.env("SWALLOW_OPERATOR_INTROSPECTION_ALLOW_LOOPBACK_HTTP", "true")
                    .env(
                        "SWALLOW_OPERATOR_INTROSPECTION_CLIENT_SECRET",
                        "fixture-sensitive-secret",
                    );
            }
            4 => {
                cmd.env_remove("SWALLOW_OPERATOR_INTROSPECTION_CLIENT_ID")
                    .env("SWALLOW_OPERATOR_INTROSPECTION_ALLOW_LOOPBACK_HTTP", "true");
            }
            _ => {
                cmd.env("SWALLOW_OPERATOR_INTROSPECTION_ALLOW_LOOPBACK_HTTP", "true")
                    .env("SWALLOW_OPERATOR_INTROSPECTION_MAX_IN_FLIGHT", "0");
            }
        }
        let mut child = cmd.spawn().unwrap();
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                assert!(!status.success());
                break;
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!("invalid configuration {mode} did not stop");
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        let output = child.wait_with_output().unwrap();
        let error = String::from_utf8_lossy(&output.stderr);
        assert!(
            error.contains("invalid API security configuration"),
            "{error}"
        );
        assert!(!error.contains("fixture-sensitive-secret"));
        assert!(!error.contains(dir.to_str().unwrap()));
    }
    std::fs::remove_dir_all(dir).unwrap();
}
