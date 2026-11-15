//! Disposable PostgreSQL and local fixture credentials; no FCM requests.
use std::{
    fs,
    process::{Command, Stdio},
    time::Duration,
};
use tokio_postgres::NoTls;

struct Fixture(std::path::PathBuf);
struct Worker(std::process::Child);
impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL and openssl"]
async fn worker_supports_token_pem_and_integration_files() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    db.batch_execute(novu_postgres_queue::MIGRATION)
        .await
        .unwrap();
    db.batch_execute("CREATE TABLE novu_rust_environments(id text PRIMARY KEY); INSERT INTO novu_rust_environments VALUES ('env')").await.unwrap();
    db.batch_execute(include_str!(
        "../migrations/0124_push_workflow_worker_policy.sql"
    ))
    .await
    .unwrap();
    db.batch_execute("ALTER TABLE public.novu_rust_push_workflow_worker_config ADD COLUMN preference_policy text DEFAULT 'snapshot', ADD COLUMN target_policy text DEFAULT 'reference'").await.unwrap();
    let fixture =
        Fixture(std::env::temp_dir().join(format!("swallow-fcm-{}", uuid::Uuid::new_v4())));
    fs::create_dir(&fixture.0).unwrap();
    let token = fixture.0.join("token");
    let key = fixture.0.join("key");
    let integrations = fixture.0.join("integrations");
    let output = Command::new("openssl")
        .args([
            "genpkey",
            "-algorithm",
            "RSA",
            "-pkeyopt",
            "rsa_keygen_bits:2048",
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    let private = String::from_utf8(output.stdout).unwrap();
    assert!(private.contains('\n'));
    fs::write(&key, &private).unwrap();
    fs::write(&token, "fixture-token\n").unwrap();
    let data = serde_json::json!({"a":{"project_id":"project-a","access_token":"fixture-token"},"b":{"project_id":"project-b","client_email":"example@example.test","private_key":private}});
    fs::write(&integrations, serde_json::to_string_pretty(&data).unwrap()).unwrap();
    let command = || {
        let mut c = Command::new(env!("CARGO_BIN_EXE_fcm-worker"));
        c.env(
            "DATABASE_URL",
            format!("{url}?application_name=fcm_secret_fixture"),
        )
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .env("NOVU_FCM_ENVIRONMENT_ID", "env")
        .env("NOVU_FCM_PROJECT_ID", "project-a")
        .env_remove("NOVU_FCM_INTEGRATIONS")
        .env_remove("NOVU_FCM_INTEGRATIONS_FILE")
        .env_remove("NOVU_FCM_ACCESS_TOKEN")
        .env_remove("NOVU_FCM_ACCESS_TOKEN_FILE")
        .env_remove("NOVU_FCM_RELOAD_ACCESS_TOKEN")
        .env_remove("NOVU_FCM_CLIENT_EMAIL")
        .env_remove("NOVU_FCM_PRIVATE_KEY")
        .env_remove("NOVU_FCM_PRIVATE_KEY_FILE")
        .env_remove("SWALLOW_FCM_ALLOW_SHARED_PROVIDER")
        .env_remove("SWALLOW_FCM_ENFORCE_ROUTING_CONFIG")
        .env_remove("SWALLOW_PUSH_WORKFLOW_STEP_POLICY")
        .env_remove("SWALLOW_PUSH_PREFERENCE_POLICY")
        .env_remove("SWALLOW_PUSH_TARGET_POLICY")
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
        c
    };
    for mode in 0..4 {
        let mut c = command();
        match mode {
            0 => {
                c.env("NOVU_FCM_ACCESS_TOKEN_FILE", &token);
            }
            1 => {
                c.env("NOVU_FCM_CLIENT_EMAIL", "example@example.test")
                    .env("NOVU_FCM_PRIVATE_KEY_FILE", &key);
            }
            2 => {
                c.env("NOVU_FCM_INTEGRATIONS_FILE", &integrations);
            }
            _ => {
                c.env("NOVU_FCM_ACCESS_TOKEN_FILE", &token)
                    .env("SWALLOW_FCM_ALLOW_SHARED_PROVIDER", "true");
            }
        }
        let mut child = c.spawn().unwrap();
        tokio::time::sleep(Duration::from_millis(500)).await;
        let alive = child.try_wait().unwrap().is_none();
        let _ = child.kill();
        let output = child.wait_with_output().unwrap();
        assert!(
            alive,
            "worker did not start with fixture file: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let log = String::from_utf8_lossy(&output.stderr);
        assert!(!log.contains("fixture-token") && !log.contains("BEGIN PRIVATE KEY"));
    }
    for invalid in ["yes", "TRUE", ""] {
        let result = command()
            .env("SWALLOW_FCM_ALLOW_SHARED_PROVIDER", invalid)
            .output()
            .unwrap();
        assert!(!result.status.success());
    }
    for mode in 0..4 {
        let mut c = command();
        c.env("NOVU_FCM_RELOAD_ACCESS_TOKEN", "true");
        match mode {
            0 => {}
            1 => {
                c.env("NOVU_FCM_ACCESS_TOKEN_FILE", &token)
                    .env("NOVU_FCM_ACCESS_TOKEN", "fixture-secret");
            }
            2 => {
                c.env("NOVU_FCM_ACCESS_TOKEN_FILE", &token)
                    .env("NOVU_FCM_INTEGRATIONS_FILE", &integrations);
            }
            _ => {
                c.env("NOVU_FCM_ACCESS_TOKEN_FILE", &token)
                    .env("NOVU_FCM_CLIENT_EMAIL", "fixture@example.test");
            }
        }
        let out = c.output().unwrap();
        assert!(!out.status.success());
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            !stderr.contains("fixture-secret") && !stderr.contains(fixture.0.to_str().unwrap())
        );
    }
    let invalid_flag = command()
        .env("NOVU_FCM_RELOAD_ACCESS_TOKEN", "yes")
        .output()
        .unwrap();
    assert!(!invalid_flag.status.success());
    let mut worker = Worker(
        command()
            .env("NOVU_FCM_ACCESS_TOKEN_FILE", &token)
            .env("NOVU_FCM_RELOAD_ACCESS_TOKEN", "true")
            .spawn()
            .unwrap(),
    );
    let mut registered = false;
    for _ in 0..100 {
        registered=db.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='fcm_secret_fixture')",&[]).await.unwrap().get(0);
        if registered {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(registered);
    assert!(worker.0.try_wait().unwrap().is_none());
    fs::write(&token, "invalid\ntoken").unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;
    let job = uuid::Uuid::new_v4();
    novu_postgres_queue::enqueue(
        &db,
        job,
        "env",
        novu_postgres_queue::fcm_queue::JOB_KIND,
        &serde_json::json!({"unrecognized":true}),
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(
        db.query_one("SELECT status FROM novu_queue_jobs WHERE id=$1", &[&job])
            .await
            .unwrap()
            .get::<_, String>(0),
        "pending"
    );
    fs::write(fixture.0.join("next"), "rotated-token\n").unwrap();
    fs::rename(fixture.0.join("next"), &token).unwrap();
    let mut failed = false;
    for _ in 0..100 {
        failed = db
            .query_one(
                "SELECT status='failed' FROM novu_queue_jobs WHERE id=$1",
                &[&job],
            )
            .await
            .unwrap()
            .get(0);
        if failed {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        failed,
        "worker should resume and reject the invalid job without sending"
    );
    drop(worker);
    let result = command()
        .env("NOVU_FCM_ACCESS_TOKEN_FILE", &token)
        .env("SWALLOW_FCM_ENFORCE_ROUTING_CONFIG", "invalid")
        .output()
        .unwrap();
    assert!(!result.status.success());
    let result = command()
        .env("NOVU_FCM_INTEGRATIONS_FILE", &integrations)
        .env("SWALLOW_FCM_ALLOW_SHARED_PROVIDER", "true")
        .output()
        .unwrap();
    assert!(!result.status.success());
    for mode in 0..3 {
        let mut c = command();
        match mode {
            0 => {
                c.env("NOVU_FCM_ACCESS_TOKEN", "fixture-token")
                    .env("NOVU_FCM_ACCESS_TOKEN_FILE", &token);
            }
            1 => {
                c.env("NOVU_FCM_CLIENT_EMAIL", "example@example.test")
                    .env("NOVU_FCM_PRIVATE_KEY", "fixture-secret")
                    .env("NOVU_FCM_PRIVATE_KEY_FILE", &key);
            }
            _ => {
                c.env("NOVU_FCM_INTEGRATIONS", "fixture-secret")
                    .env("NOVU_FCM_INTEGRATIONS_FILE", &integrations);
            }
        }
        let result = c.output().unwrap();
        assert!(!result.status.success());
        let log = String::from_utf8_lossy(&result.stderr);
        assert!(
            !log.contains("fixture-secret")
                && !log.contains("fixture-token")
                && !log.contains("BEGIN PRIVATE KEY")
                && !log.contains(fixture.0.to_str().unwrap())
        );
    }
    fs::write(&integrations, vec![b'x'; 1_048_577]).unwrap();
    let result = command()
        .env("NOVU_FCM_INTEGRATIONS_FILE", &integrations)
        .output()
        .unwrap();
    assert!(!result.status.success());
    assert!(!String::from_utf8_lossy(&result.stderr).contains(fixture.0.to_str().unwrap()));
}
