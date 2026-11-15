//! Real workers and a disposable PostgreSQL database; no provider requests.
use std::{
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};
use tokio_postgres::NoTls;
use uuid::Uuid;

struct Worker(Child);
impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn start(url: &str, environment: &str, scope: &str) -> Worker {
    Worker(
        Command::new(env!("CARGO_BIN_EXE_resend-worker"))
            .env_clear()
            .env("DATABASE_URL", url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("NOVU_RESEND_ENVIRONMENT_ID", environment)
            .env("NOVU_RESEND_API_KEY", "re_test")
            .env("NOVU_RESEND_FROM", "test@example.test")
            .env("NOVU_RESEND_IDEMPOTENCY_SCOPE", scope)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    )
}
async fn wait_for_scope(db: &tokio_postgres::Client, environment: &str, expected: &str) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if db.query_opt("SELECT scope FROM public.novu_rust_resend_idempotency_policy WHERE environment_id=$1", &[&environment]).await.unwrap().is_some_and(|r| r.get::<_, String>(0) == expected) { break; }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    }).await.unwrap();
}
#[tokio::test]
#[ignore = "requires a fresh disposable PostgreSQL database"]
async fn workers_reject_conflicting_scopes_and_isolate_environments() {
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
        let result = Command::new(env!("CARGO_BIN_EXE_migrate-worker"))
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .args(args)
            .env("DATABASE_URL", &url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .output()
            .unwrap();
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
    }
    let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let a = format!("scope-a-{}", Uuid::new_v4());
    let b = format!("scope-b-{}", Uuid::new_v4());
    db.execute("INSERT INTO public.novu_rust_environments(id,organization_id) VALUES ($1,'org'),($2,'org')", &[&a,&b]).await.unwrap();
    let mut original = start(&url, &a, "job");
    wait_for_scope(&db, &a, "job").await;
    assert!(original.0.try_wait().unwrap().is_none());
    let mut conflicting = start(&url, &a, "retry");
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = conflicting.0.try_wait().unwrap() {
            assert!(!status.success());
            break;
        }
        assert!(Instant::now() < deadline, "conflicting worker did not exit");
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    assert!(original.0.try_wait().unwrap().is_none());
    let mut same = start(&url, &a, "job");
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(same.0.try_wait().unwrap().is_none());
    let mut other = start(&url, &b, "retry");
    wait_for_scope(&db, &b, "retry").await;
    assert!(other.0.try_wait().unwrap().is_none());
    let c = format!("scope-race-{}", Uuid::new_v4());
    db.execute(
        "INSERT INTO public.novu_rust_environments(id,organization_id) VALUES ($1,'org')",
        &[&c],
    )
    .await
    .unwrap();
    let mut job_worker = start(&url, &c, "job");
    let mut retry_worker = start(&url, &c, "retry");
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let job_exit = job_worker.0.try_wait().unwrap();
        let retry_exit = retry_worker.0.try_wait().unwrap();
        if job_exit.is_some() || retry_exit.is_some() {
            assert_ne!(
                job_exit.is_some(),
                retry_exit.is_some(),
                "exactly one scope must win"
            );
            assert!(!job_exit.or(retry_exit).unwrap().success());
            break;
        }
        assert!(
            Instant::now() < deadline,
            "conflicting concurrent workers did not resolve"
        );
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    assert_eq!(
        db.query_one(
            "SELECT scope FROM public.novu_rust_resend_idempotency_policy WHERE environment_id=$1",
            &[&a]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "job"
    );
}
