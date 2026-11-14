//! DDL and credential writes must serialize while the guard is active.
use std::{path::Path, process::Command, time::Duration};
use tokio_postgres::NoTls;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires disposable PostgreSQL and role creation"]
async fn guard_holds_schema_lock_and_rejects_disabled_trigger() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
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
            .current_dir(root)
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
    let (owner, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let role = format!("fcm_guard_{}", Uuid::new_v4().simple());
    owner.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT SELECT,INSERT,UPDATE ON public.novu_rust_fcm_devices TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_assert_fcm_registration_guard() TO {role}")).await.unwrap();
    let (mut api, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    api.batch_execute(&format!("SET SESSION AUTHORIZATION {role}"))
        .await
        .unwrap();
    let tx = api.transaction().await.unwrap();
    assert!(
        tx.query_one(
            "SELECT public.novu_rust_assert_fcm_registration_guard()",
            &[]
        )
        .await
        .unwrap()
        .get::<_, bool>(0)
    );
    let (ddl, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let disabled = tokio::spawn(async move {
        ddl.batch_execute("ALTER TABLE public.novu_rust_fcm_devices DISABLE TRIGGER novu_rust_fcm_device_cleanup_generation").await
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !disabled.is_finished(),
        "DDL did not wait for the guarded transaction"
    );
    tx.commit().await.unwrap();
    disabled.await.unwrap().unwrap();
    let err = api
        .query_one(
            "SELECT public.novu_rust_assert_fcm_registration_guard()",
            &[],
        )
        .await
        .unwrap_err();
    assert_eq!(err.code().unwrap().code(), "55000");
    owner.batch_execute("ALTER TABLE public.novu_rust_fcm_devices ENABLE TRIGGER novu_rust_fcm_device_cleanup_generation").await.unwrap();
    assert!(
        api.query_one(
            "SELECT public.novu_rust_assert_fcm_registration_guard()",
            &[]
        )
        .await
        .unwrap()
        .get::<_, bool>(0)
    );
    owner
        .batch_execute("SET session_replication_role=replica")
        .await
        .unwrap();
    assert_eq!(
        owner
            .query_one(
                "SELECT public.novu_rust_assert_fcm_registration_guard()",
                &[]
            )
            .await
            .unwrap_err()
            .code()
            .unwrap()
            .code(),
        "55000"
    );
    owner
        .batch_execute("SET session_replication_role=origin")
        .await
        .unwrap();
    owner.batch_execute(&format!("REVOKE EXECUTE ON FUNCTION public.novu_rust_assert_fcm_registration_guard() FROM {role}")).await.unwrap();
    assert_eq!(
        api.query_one(
            "SELECT public.novu_rust_assert_fcm_registration_guard()",
            &[]
        )
        .await
        .unwrap_err()
        .code()
        .unwrap()
        .code(),
        "42501"
    );
}
