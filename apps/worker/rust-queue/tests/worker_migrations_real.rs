//! Exercise the actual worker migration files against a disposable PostgreSQL database.
use std::{fs, path::Path, process::Command};
use tokio_postgres::NoTls;

#[tokio::test]
#[ignore = "requires disposable PostgreSQL"]
async fn api_then_all_worker_migrations_can_be_reapplied_safely() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut files: Vec<_> = fs::read_dir(root.join("../../api/rust-api/migrations"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "sql"))
        .collect();
    files.sort();
    for path in files {
        db.batch_execute(&fs::read_to_string(path).unwrap())
            .await
            .unwrap();
    }
    let run = || {
        Command::new(env!("CARGO_BIN_EXE_migrate-worker"))
            .args(["apply", root.join("migrations").to_str().unwrap()])
            .env("DATABASE_URL", &url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .output()
            .unwrap()
    };
    let first = run();
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    let count: i64 = db
        .query_one(
            "SELECT count(*) FROM public.novu_rust_worker_migration_history WHERE method='applied'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    let expected = fs::read_dir(root.join("migrations"))
        .unwrap()
        .filter(|v| {
            v.as_ref()
                .unwrap()
                .path()
                .extension()
                .is_some_and(|x| x == "sql")
        })
        .count() as i64;
    assert_eq!(count, expected);
    let second = run();
    assert!(second.status.success());
    assert!(String::from_utf8_lossy(&second.stdout).contains("applied 0"));
}
