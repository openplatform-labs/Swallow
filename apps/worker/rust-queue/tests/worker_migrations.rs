//! Migration runner executes fixture SQL only in a disposable database.
use std::{
    fs,
    process::{Command, Output},
};
use tokio_postgres::NoTls;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires disposable PostgreSQL"]
async fn history_atomicity_concurrency_and_explicit_baseline() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let dir = std::env::temp_dir().join(format!("swallow-migrations-{}", Uuid::new_v4()));
    fs::create_dir(&dir).unwrap();
    fs::write(
        dir.join("0001_queue.sql"),
        "CREATE TABLE public.novu_queue_jobs(id integer PRIMARY KEY);",
    )
    .unwrap();
    fs::write(
        dir.join("0002_extra.sql"),
        "CREATE TABLE public.fixture_extra(id integer);",
    )
    .unwrap();
    let exec = |args: &[&str]| -> Output {
        Command::new(env!("CARGO_BIN_EXE_migrate-worker"))
            .args(args)
            .env("DATABASE_URL", &url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .output()
            .unwrap()
    };
    let path = dir.to_str().unwrap();
    assert!(exec(&["apply", path]).status.success());
    assert!(String::from_utf8_lossy(&exec(&["apply", path]).stdout).contains("applied 0"));
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM public.novu_rust_worker_migration_history",
            &[]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        2
    );
    fs::write(dir.join("0002_extra.sql"), "SELECT 1;").unwrap();
    assert!(!exec(&["apply", path]).status.success());
    fs::write(
        dir.join("0002_extra.sql"),
        "CREATE TABLE public.fixture_extra(id integer);",
    )
    .unwrap();
    fs::write(
        dir.join("0003_fail.sql"),
        "CREATE TABLE public.rolled_back(id int); SELECT invalid_function();",
    )
    .unwrap();
    assert!(!exec(&["apply", path]).status.success());
    assert!(
        db.query_one("SELECT to_regclass('public.rolled_back') IS NULL", &[])
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM public.novu_rust_worker_migration_history",
            &[]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        2
    );
    fs::write(
        dir.join("0003_fail.sql"),
        "SELECT pg_sleep(0.2); CREATE TABLE public.third(id int);",
    )
    .unwrap();
    let spawn = || {
        Command::new(env!("CARGO_BIN_EXE_migrate-worker"))
            .args(["apply", path])
            .env("DATABASE_URL", &url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .stdout(std::process::Stdio::piped())
            .spawn()
            .unwrap()
    };
    let a = spawn();
    let b = spawn();
    let (a, b) = (a.wait_with_output().unwrap(), b.wait_with_output().unwrap());
    assert!(a.status.success() && b.status.success());
    let output = format!(
        "{} {}",
        String::from_utf8_lossy(&a.stdout),
        String::from_utf8_lossy(&b.stdout)
    );
    assert!(output.contains("applied 1") && output.contains("applied 0"));
    db.batch_execute("DROP TABLE public.novu_rust_worker_migration_history")
        .await
        .unwrap();
    assert!(!exec(&["apply", path]).status.success());
    assert!(!exec(&["baseline", path, "3", "wrong"]).status.success());
    assert!(
        exec(&["baseline", path, "3", "I-VERIFIED-THE-EXISTING-SCHEMA"])
            .status
            .success()
    );
    assert_eq!(db.query_one("SELECT count(*) FROM public.novu_rust_worker_migration_history WHERE method='baseline'",&[]).await.unwrap().get::<_,i64>(0),3);
    assert!(
        !exec(&["baseline", path, "3", "I-VERIFIED-THE-EXISTING-SCHEMA"])
            .status
            .success()
    );
    assert!(exec(&["apply", path]).status.success());
    fs::remove_dir_all(dir).unwrap();
}
