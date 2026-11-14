//! Real alert commands and PostgreSQL, with blocked destinations and no provider traffic.
use std::{process::Command, time::Duration};
use tokio_postgres::NoTls;
use uuid::Uuid;
#[tokio::test]
#[ignore = "requires fully migrated disposable PostgreSQL"]
async fn destinations_have_independent_slots_and_failures_do_not_suppress_others() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    db.batch_execute("INSERT INTO novu_rust_environments VALUES ('fanout-env','org'); INSERT INTO novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'fanout-env',rolname,'alert_send',oid FROM pg_roles WHERE rolname=session_user; INSERT INTO novu_rust_alert_delivery_slots(environment_id,channel,target_id,state) VALUES ('fanout-env','slack','ops-a','rejected'),('fanout-env','slack','ops-b','rejected'),('fanout-env','email','mail-a','rejected'),('fanout-env','email','mail-b','rejected')").await.unwrap();
    let job = Uuid::new_v4();
    novu_postgres_queue::enqueue(&db, job, "fanout-env", "fixture", &serde_json::json!({}))
        .await
        .unwrap();
    db.execute("UPDATE novu_queue_jobs SET available_at=clock_timestamp()-interval '5 minutes' WHERE id=$1",&[&job]).await.unwrap();
    let dir = std::env::temp_dir().join(format!("swallow-fanout-{}", Uuid::new_v4()));
    std::fs::create_dir(&dir).unwrap();
    let slack = dir.join("slack-secret");
    let key = dir.join("email-secret");
    let config = dir.join("config.json");
    std::fs::write(&slack, "https://hooks.slack.com/services/A/B/C").unwrap();
    std::fs::write(&key, "fixture-secret-key").unwrap();
    let write = |targets: serde_json::Value| {
        std::fs::write(
            &config,
            serde_json::to_vec(&serde_json::json!({"targets":targets})).unwrap(),
        )
        .unwrap();
    };
    let run = |mode: &str| {
        let mut command = Command::new(env!("CARGO_BIN_EXE_alert-fanout"));
        if mode == "delivery" {
            command.args(["delivery", "fanout-env", "300", "120", "60"]);
        } else {
            command.args(["database", "fanout-env", "probe", "60"]);
        }
        command
            .arg(&config)
            .env("DATABASE_URL", &url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("SWALLOW_ALERT_RESEND_ENABLED", "false")
            .env("SWALLOW_ALERT_QUEUE_MAX_WAIT_SECONDS", "60")
            .env(
                "SWALLOW_DB_PROBE_URL",
                "postgres://postgres@127.0.0.1:1/postgres",
            )
            .env("SWALLOW_DB_PROBE_TIMEOUT_SECONDS", "1")
            // Conflicting inherited credentials must not override each target.
            .env(
                "SWALLOW_ALERT_SLACK_WEBHOOK",
                "bad-secret-should-not-appear",
            )
            .env("NOVU_RESEND_API_KEY", "inherited-secret")
            .env_remove("SWALLOW_ALERT_CHECK_SCOPED_ACCESS")
            .env_remove("SWALLOW_ALERT_SCOPED_DELIVERY")
            .env_remove("SWALLOW_ALERT_COOLDOWN_POLICY")
            .output()
            .unwrap()
    };
    let targets = serde_json::json!([
        {"channel":"slack","id":"ops-a","webhook_file":slack},
        {"channel":"email","id":"mail-a","provider":"resend","to":"one@example.test","from":"sender@example.test","api_key_file":key},
        {"channel":"slack","id":"ops-b","webhook_file":slack},
        {"channel":"email","id":"mail-b","provider":"sendgrid","to":"two@example.test","from":"sender@example.test","api_key_file":key,"region":"eu"}
    ]);
    write(targets.clone());
    for mode in ["delivery", "database"] {
        let out = run(mode);
        assert_eq!(
            out.status.code(),
            Some(2),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        let text = String::from_utf8(out.stdout).unwrap();
        for id in ["ops-a", "mail-a", "ops-b", "mail-b"] {
            assert!(text.contains(&format!("{id}: attention")), "{text}");
        }
        for secret in [
            "fixture-secret-key",
            "inherited-secret",
            "one@example.test",
            "https://hooks.slack.com",
        ] {
            assert!(!text.contains(secret));
        }
    }
    let mut broken = targets.as_array().unwrap().clone();
    broken[1]["api_key_file"] = serde_json::json!(dir.join("missing"));
    write(serde_json::json!(broken));
    let out = run("delivery");
    assert_eq!(out.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&out.stderr).contains("email mail-a: failed"));
    assert!(String::from_utf8_lossy(&out.stdout).contains("slack ops-b: attention"));
    write(serde_json::json!([targets[0].clone(), targets[0].clone()]));
    let out = run("delivery");
    assert_eq!(out.status.code(), Some(1));
    assert!(!String::from_utf8_lossy(&out.stderr).contains("hooks.slack.com"));
    std::fs::remove_dir_all(dir).unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
}
