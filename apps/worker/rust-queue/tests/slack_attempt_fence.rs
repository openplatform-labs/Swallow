//! Disposable database, restricted CLI login, and no Slack requests.
use std::{path::Path, process::Command};
use tokio_postgres::NoTls;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires a fresh disposable PostgreSQL database and role creation"]
async fn slack_resolution_rejects_stale_attempts_and_audits_current_one() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    for args in [
        vec![
            "apply",
            "api",
            "../../api/rust-api/migrations",
            "--through",
            "17",
        ],
        vec!["apply", "worker", "migrations"],
    ] {
        let out = Command::new(env!("CARGO_BIN_EXE_migrate-worker"))
            .current_dir(Path::new(env!("CARGO_MANIFEST_DIR")))
            .args(args)
            .env("DATABASE_URL", &url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let role = format!("slack_fence_{}", Uuid::new_v4().simple());
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_list_slack_attention_v2(text,uuid),public.novu_rust_resolve_slack_v2(text,uuid,uuid,text,text,text) TO {role}")).await.unwrap();
    db.execute(
        "INSERT INTO public.novu_rust_environments VALUES ('a','org'),('b','org')",
        &[],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'a',$1,p,oid FROM pg_roles CROSS JOIN unnest(ARRAY['slack_view','slack_retry','slack_accepted']) p WHERE rolname::text=$1", &[&role]).await.unwrap();
    let mut role_url = reqwest::Url::parse(&url).unwrap();
    role_url.set_username(&role).unwrap();
    let cli = |args: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
            .args(args)
            .env("DATABASE_URL", role_url.as_str())
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .output()
            .unwrap()
    };
    let job = Uuid::new_v4();
    db.execute("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status) VALUES ($1,'a','experimental_slack_chat','{\"secret\":\"private\"}','failed')", &[&job]).await.unwrap();
    let first:Uuid=db.query_one("INSERT INTO public.novu_rust_slack_attempts(job_id,environment_id,state) VALUES ($1,'a','unknown') RETURNING attempt_id", &[&job]).await.unwrap().get(0);
    let output = cli(&["list-slack-v2", "a"]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let rows: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(rows[0]["attemptId"], first.to_string());
    assert!(!String::from_utf8_lossy(&output.stdout).contains("private"));
    assert!(!cli(&["list-slack-v2", "b"]).status.success());
    assert!(
        cli(&[
            "slack-resolve-v2",
            "a",
            &job.to_string(),
            &first.to_string(),
            "retry",
            "operator",
            "verified"
        ])
        .status
        .success()
    );
    db.execute(
        "UPDATE public.novu_queue_jobs SET status='failed' WHERE id=$1",
        &[&job],
    )
    .await
    .unwrap();
    let second:Uuid=db.query_one("INSERT INTO public.novu_rust_slack_attempts(job_id,environment_id,state) VALUES ($1,'a','unknown') RETURNING attempt_id", &[&job]).await.unwrap().get(0);
    assert_ne!(first, second);
    for decision in ["retry", "accepted"] {
        assert!(
            !cli(&[
                "slack-resolve-v2",
                "a",
                &job.to_string(),
                &first.to_string(),
                decision,
                "operator",
                "stale"
            ])
            .status
            .success()
        );
    }
    assert_eq!(
        db.query_one(
            "SELECT state FROM public.novu_rust_slack_attempts WHERE job_id=$1",
            &[&job]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "unknown"
    );
    assert!(
        cli(&[
            "slack-resolve-v2",
            "a",
            &job.to_string(),
            &second.to_string(),
            "accepted",
            "operator",
            "verified"
        ])
        .status
        .success()
    );
    let rows=db.query("SELECT attempt_id,decision FROM public.novu_rust_slack_resolutions WHERE job_id=$1 ORDER BY created_at", &[&job]).await.unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, Uuid>(0), first);
    assert_eq!(rows[0].get::<_, String>(1), "retry");
    assert_eq!(rows[1].get::<_, Uuid>(0), second);
    assert_eq!(rows[1].get::<_, String>(1), "accepted");
    // Two operators racing on one attempt cannot both resolve it.
    let racing = Uuid::new_v4();
    db.execute("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status) VALUES ($1,'a','experimental_slack_webhook','{}','failed')", &[&racing]).await.unwrap();
    let racing_attempt: Uuid = db.query_one("INSERT INTO public.novu_rust_slack_attempts(job_id,environment_id,state) VALUES ($1,'a','unknown') RETURNING attempt_id", &[&racing]).await.unwrap().get(0);
    let mut commands = Vec::new();
    for decision in ["retry", "accepted"] {
        let url = role_url.to_string();
        commands.push(std::thread::spawn(move || {
            Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
                .args([
                    "slack-resolve-v2",
                    "a",
                    &racing.to_string(),
                    &racing_attempt.to_string(),
                    decision,
                    "operator",
                    "race",
                ])
                .env("DATABASE_URL", url)
                .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
                .output()
                .unwrap()
                .status
                .success()
        }));
    }
    let outcomes: Vec<bool> = commands
        .into_iter()
        .map(|thread| thread.join().unwrap())
        .collect();
    assert_eq!(outcomes.iter().filter(|v| **v).count(), 1);
    assert_eq!(db.query_one("SELECT count(*) FROM public.novu_rust_slack_resolutions WHERE job_id=$1 AND attempt_id=$2", &[&racing,&racing_attempt]).await.unwrap().get::<_,i64>(0),1);
    db.execute("DELETE FROM public.novu_rust_delivery_operator_grants WHERE environment_id='a' AND database_principal=$1 AND decision='slack_retry'", &[&role]).await.unwrap();
    assert!(
        !cli(&[
            "slack-resolve-v2",
            "a",
            &job.to_string(),
            &second.to_string(),
            "retry",
            "operator",
            "denied"
        ])
        .status
        .success()
    );
}
