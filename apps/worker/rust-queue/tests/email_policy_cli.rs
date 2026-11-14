//! Cross-provider policy agreement; fixture credentials, no external sends.
use std::{
    process::{Command, Stdio},
    time::Duration,
};
use tokio_postgres::NoTls;

#[tokio::test]
#[ignore = "requires disposable PostgreSQL"]
async fn email_workers_reject_policy_mismatch() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    db.batch_execute("CREATE TABLE public.novu_rust_environments(id text PRIMARY KEY,organization_id text NOT NULL); INSERT INTO public.novu_rust_environments VALUES ('env','org')").await.unwrap();
    db.batch_execute(include_str!(
        "../migrations/0120_email_workflow_worker_policy.sql"
    ))
    .await
    .unwrap();
    db.batch_execute(
        "INSERT INTO public.novu_rust_email_workflow_worker_config VALUES ('env','snapshot')",
    )
    .await
    .unwrap();
    for binary in [
        env!("CARGO_BIN_EXE_sendgrid-worker"),
        env!("CARGO_BIN_EXE_resend-worker"),
    ] {
        let command = |policy: &str| {
            let mut c = Command::new(binary);
            c.env("DATABASE_URL", &url)
                .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
                .env("NOVU_SENDGRID_ENVIRONMENT_ID", "env")
                .env("NOVU_SENDGRID_API_KEY", "fixture")
                .env("NOVU_SENDGRID_FROM", "sender@example.test")
                .env("NOVU_SENDGRID_SENDER_NAME", "Fixture")
                .env("NOVU_RESEND_ENVIRONMENT_ID", "env")
                .env("NOVU_RESEND_API_KEY", "fixture")
                .env("NOVU_RESEND_FROM", "sender@example.test")
                .env_remove("NOVU_SENDGRID_API_KEY_FILE")
                .env_remove("NOVU_RESEND_API_KEY_FILE")
                .env("SWALLOW_EMAIL_WORKFLOW_STEP_POLICY", policy)
                .stdout(Stdio::null())
                .stderr(Stdio::piped());
            c
        };
        let mismatch = command("active").output().unwrap();
        assert!(!mismatch.status.success());
        assert!(
            String::from_utf8_lossy(&mismatch.stderr).contains("different workflow step policies")
        );
        let invalid = command("invalid").output().unwrap();
        assert!(!invalid.status.success());
        assert!(
            String::from_utf8_lossy(&invalid.stderr).contains("invalid email workflow step policy")
        );
        let mut same = command("snapshot").spawn().unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(same.try_wait().unwrap().is_none());
        same.kill().unwrap();
        same.wait().unwrap();
    }
}
