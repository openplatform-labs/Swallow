//! Exercise the actual service migration files against a disposable PostgreSQL database.
use std::{fs, path::Path, process::Command};
use tokio_postgres::NoTls;
#[tokio::test]
#[ignore = "requires disposable PostgreSQL"]
async fn service_migrations_apply_in_order_and_reapply_safely() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    for (service, path, history) in [
        (
            "api",
            root.join("../../api/rust-api/migrations"),
            "public.novu_rust_api_migration_history",
        ),
        (
            "worker",
            root.join("migrations"),
            "public.novu_rust_worker_migration_history",
        ),
        (
            "ws",
            root.join("../../ws/rust-ws/migrations"),
            "public.novu_rust_ws_migration_history",
        ),
    ] {
        let run = || {
            Command::new(env!("CARGO_BIN_EXE_migrate-worker"))
                .args(["apply", service, path.to_str().unwrap()])
                .env("DATABASE_URL", &url)
                .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
                .output()
                .unwrap()
        };
        let first = run();
        assert!(
            first.status.success(),
            "{}: {}",
            service,
            String::from_utf8_lossy(&first.stderr)
        );
        let count: i64 = db
            .query_one(
                &format!("SELECT count(*) FROM {history} WHERE method='applied'"),
                &[],
            )
            .await
            .unwrap()
            .get(0);
        let expected = fs::read_dir(&path)
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
    for (service, path, history) in [
        (
            "api",
            root.join("../../api/rust-api/migrations"),
            "public.novu_rust_api_migration_history",
        ),
        (
            "ws",
            root.join("../../ws/rust-ws/migrations"),
            "public.novu_rust_ws_migration_history",
        ),
    ] {
        db.batch_execute(&format!("DROP TABLE {history}"))
            .await
            .unwrap();
        let invoke = |args: &[&str]| {
            Command::new(env!("CARGO_BIN_EXE_migrate-worker"))
                .args(args)
                .env("DATABASE_URL", &url)
                .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
                .output()
                .unwrap()
        };
        assert!(
            !invoke(&["apply", service, path.to_str().unwrap()])
                .status
                .success()
        );
        let version = fs::read_dir(&path)
            .unwrap()
            .filter(|v| {
                v.as_ref()
                    .unwrap()
                    .path()
                    .extension()
                    .is_some_and(|x| x == "sql")
            })
            .count()
            .to_string();
        let base = invoke(&[
            "baseline",
            service,
            path.to_str().unwrap(),
            &version,
            "I-VERIFIED-THE-EXISTING-SCHEMA",
        ]);
        assert!(
            base.status.success(),
            "{}",
            String::from_utf8_lossy(&base.stderr)
        );
        assert!(
            invoke(&["apply", service, path.to_str().unwrap()])
                .status
                .success()
        );
        let count: i64 = db
            .query_one(
                &format!("SELECT count(*) FROM {history} WHERE method='baseline'"),
                &[],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(count, version.parse::<i64>().unwrap());
    }
}
