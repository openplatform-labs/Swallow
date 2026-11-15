//! Two real Slack worker processes sharing one disposable PostgreSQL instance.
use std::{
    process::{Child, Command, Stdio},
    time::Duration,
};
use tokio_postgres::NoTls;
use uuid::Uuid;

struct Workers(Vec<Child>);
impl Drop for Workers {
    fn drop(&mut self) {
        for child in &mut self.0 {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[tokio::test]
#[ignore = "requires a fully migrated disposable PostgreSQL database"]
async fn surviving_worker_defers_jobs_after_peer_loss() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let environment = format!("slack-process-ha-{}", Uuid::new_v4());
    db.execute(
        "INSERT INTO public.novu_rust_environments VALUES($1,'org')",
        &[&environment],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO public.novu_rust_slack_cooldown_policies(environment_id,fallback_seconds,not_before) VALUES($1,30,clock_timestamp()+interval '10 minutes')", &[&environment]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_slack_cooldown_workers SELECT $1,rolname,oid FROM pg_roles WHERE rolname=current_user", &[&environment]).await.unwrap();
    let names = [
        format!("swallow-ha-a-{}", Uuid::new_v4()),
        format!("swallow-ha-b-{}", Uuid::new_v4()),
    ];
    let mut workers = Workers(Vec::new());
    for name in &names {
        let separator = if url.contains('?') { '&' } else { '?' };
        let child = Command::new(env!("CARGO_BIN_EXE_slack-worker"))
            .env(
                "DATABASE_URL",
                format!("{url}{separator}application_name={name}"),
            )
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("NOVU_SLACK_ENVIRONMENT_ID", &environment)
            .env("NOVU_SLACK_BOT_TOKEN", "test-only-token")
            .env_remove("NOVU_SLACK_BOT_TOKEN_FILE")
            .env_remove("NOVU_SLACK_RELOAD_BOT_TOKEN")
            .env("NOVU_SLACK_SHARED_COOLDOWN", "true")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        workers.0.push(child);
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let count: i64 = db
            .query_one(
                "SELECT count(*) FROM pg_stat_activity WHERE application_name=ANY($1)",
                &[&names.to_vec()],
            )
            .await
            .unwrap()
            .get(0);
        if count == 2 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "both worker processes must connect"
        );
        assert!(
            workers
                .0
                .iter_mut()
                .all(|child| child.try_wait().unwrap().is_none()),
            "worker exited"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    async fn enqueue_and_wait(db: &tokio_postgres::Client, env: &str, count: i32) {
        db.execute("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload) SELECT gen_random_uuid(),$1,'experimental_slack_chat','{\"channel\":\"C1\",\"text\":\"test\"}'::jsonb FROM generate_series(1,$2)", &[&env,&count]).await.unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let row = db.query_one("SELECT count(*) FILTER (WHERE status='pending' AND last_error='Slack shared cooldown active' AND available_at>clock_timestamp()+interval '8 minutes'),count(*) FROM public.novu_queue_jobs WHERE environment_id=$1", &[&env]).await.unwrap();
            let deferred: i64 = row.get(0);
            let total: i64 = row.get(1);
            if deferred == total && total > 0 {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "deferred {deferred}/{total} jobs"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    enqueue_and_wait(&db, &environment, 10).await;
    workers.0[0].kill().unwrap();
    workers.0[0].wait().unwrap();
    enqueue_and_wait(&db, &environment, 10).await;
    assert!(workers.0[1].try_wait().unwrap().is_none());
    let count: i64 = db
        .query_one(
            "SELECT count(*) FROM public.novu_rust_slack_attempts WHERE environment_id=$1",
            &[&environment],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 0, "deferred jobs must not create provider attempts");
}
