//! Real FCM workers and scoped observations in a disposable PostgreSQL database.
use std::{
    fs,
    process::{Child, Command, Stdio},
    time::Duration,
};
use tokio_postgres::NoTls;

struct Fixture(std::path::PathBuf);
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
struct Worker(Child);
impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn worker(url: &str, path: &std::path::Path, application: &str, exit: bool) -> Worker {
    Worker(
        Command::new(env!("CARGO_BIN_EXE_fcm-worker"))
            .env_clear()
            .env(
                "DATABASE_URL",
                format!("{url}?application_name={application}"),
            )
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("NOVU_FCM_ENVIRONMENT_ID", "env-a")
            .env("NOVU_FCM_PROJECT_ID", "project-a")
            .env("NOVU_FCM_ACCESS_TOKEN_FILE", path)
            .env("NOVU_FCM_RELOAD_ACCESS_TOKEN", "true")
            .env("NOVU_FCM_CREDENTIAL_HEALTH_ENABLED", "true")
            .env(
                "NOVU_FCM_RELOAD_FAILURE_POLICY",
                if exit { "exit" } else { "pause" },
            )
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    )
}
async fn counts(db: &tokio_postgres::Client) -> (i64, i64, i64) {
    let row=db.query_one("SELECT healthy,unhealthy,stale FROM public.novu_rust_fcm_credential_health_check('env-a',60)",&[]).await.unwrap();
    (row.get(0), row.get(1), row.get(2))
}
#[tokio::test]
#[ignore = "requires disposable PostgreSQL"]
async fn credential_health_tracks_individual_workers_and_permissions() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    db.batch_execute("CREATE TABLE public.novu_rust_environments(id text PRIMARY KEY,organization_id text NOT NULL); INSERT INTO public.novu_rust_environments VALUES ('env-a','org'),('env-b','org')").await.unwrap();
    for sql in [
        novu_postgres_queue::MIGRATION,
        novu_postgres_queue::reconciliation::MIGRATION,
        novu_postgres_queue::reconciliation::OPERATOR_GRANTS_MIGRATION,
        novu_postgres_queue::reconciliation::OPERATOR_GRANT_LOCK_MIGRATION,
        include_str!("../migrations/0072_operator_grant_management.sql"),
        include_str!("../migrations/0073_operator_role_identity.sql"),
        include_str!("../migrations/0087_alert_view_permission.sql"),
        include_str!("../migrations/0089_scoped_delivery_health.sql"),
        include_str!("../migrations/0095_fcm_routing_guard.sql"),
        include_str!("../migrations/0124_push_workflow_worker_policy.sql"),
        include_str!("../migrations/0142_fcm_credential_health.sql"),
    ] {
        db.batch_execute(sql).await.unwrap();
    }
    db.batch_execute("ALTER TABLE public.novu_rust_push_workflow_worker_config ADD COLUMN preference_policy text DEFAULT 'snapshot', ADD COLUMN target_policy text DEFAULT 'reference'; CREATE ROLE fcm_health_reader LOGIN; CREATE ROLE fcm_health_writer LOGIN; GRANT USAGE ON SCHEMA public TO fcm_health_reader,fcm_health_writer; GRANT EXECUTE ON FUNCTION public.novu_rust_fcm_credential_health_check(text,integer) TO fcm_health_reader; GRANT EXECUTE ON FUNCTION public.novu_rust_fcm_credential_heartbeat(text,uuid,text) TO fcm_health_writer").await.unwrap();
    db.execute("INSERT INTO public.novu_rust_fcm_routing_workers SELECT 'env-a',rolname,oid FROM pg_roles WHERE rolname=session_user",&[]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_delivery_operator_grants VALUES ('env-a',session_user,'alert_view',(SELECT oid FROM pg_roles WHERE rolname=session_user)),('env-a','fcm_health_reader','alert_view',(SELECT oid FROM pg_roles WHERE rolname='fcm_health_reader'))",&[]).await.unwrap();
    let reader_url = url.replace("postgres@", "fcm_health_reader@");
    let writer_url = url.replace("postgres@", "fcm_health_writer@");
    let (reader, driver) = tokio_postgres::connect(&reader_url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let (writer, driver) = tokio_postgres::connect(&writer_url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let id = uuid::Uuid::new_v4();
    assert!(
        writer
            .query_one(
                "SELECT public.novu_rust_fcm_credential_heartbeat('env-b',$1,'healthy')",
                &[&id]
            )
            .await
            .is_err()
    );
    db.execute("INSERT INTO public.novu_rust_fcm_routing_workers SELECT 'env-b',rolname,oid FROM pg_roles WHERE rolname='fcm_health_writer'",&[]).await.unwrap();
    assert!(
        writer
            .query_one(
                "SELECT public.novu_rust_fcm_credential_heartbeat('env-b',$1,'healthy')",
                &[&id]
            )
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    let (other_writer, other_driver) = tokio_postgres::connect(&writer_url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = other_driver.await;
    });
    assert!(
        !other_writer
            .query_one(
                "SELECT public.novu_rust_fcm_credential_heartbeat('env-b',$1,'unhealthy')",
                &[&id]
            )
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    assert!(
        reader
            .query_one(
                "SELECT * FROM public.novu_rust_fcm_credential_health_check('env-b',60)",
                &[]
            )
            .await
            .is_err()
    );
    assert!(
        reader
            .query_one("SELECT * FROM public.novu_rust_fcm_credential_health", &[])
            .await
            .is_err()
    );
    let fixture =
        Fixture(std::env::temp_dir().join(format!("swallow-fcm-health-{}", uuid::Uuid::new_v4())));
    fs::create_dir(&fixture.0).unwrap();
    let first = fixture.0.join("one");
    let second = fixture.0.join("two");
    fs::write(&first, "fixture-one").unwrap();
    fs::write(&second, "fixture-two").unwrap();
    let first_worker = worker(&url, &first, "fcm_health_one", false);
    let second_worker = worker(&url, &second, "fcm_health_two", false);
    let mut ready = false;
    for _ in 0..100 {
        if counts(&db).await.0 == 2 {
            ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(ready);
    db.batch_execute("CREATE ROLE fcm_health_owner NOLOGIN; GRANT USAGE ON SCHEMA public TO fcm_health_owner; GRANT SELECT ON public.novu_rust_fcm_credential_health TO fcm_health_owner; GRANT EXECUTE ON FUNCTION public.novu_rust_assert_alert_view(text) TO fcm_health_owner; ALTER FUNCTION public.novu_rust_fcm_credential_health_check(text,integer) OWNER TO fcm_health_owner").await.unwrap();
    assert!(
        db.query_one(
            "SELECT * FROM public.novu_rust_fcm_credential_health_check('env-a',60)",
            &[]
        )
        .await
        .is_err()
    );
    db.batch_execute("GRANT pg_read_all_stats TO fcm_health_owner")
        .await
        .unwrap();
    db.batch_execute("GRANT SELECT,UPDATE ON public.novu_rust_fcm_routing_workers TO fcm_health_owner; GRANT INSERT,UPDATE ON public.novu_rust_fcm_credential_health TO fcm_health_owner; ALTER FUNCTION public.novu_rust_fcm_credential_heartbeat(text,uuid,text) OWNER TO fcm_health_owner").await.unwrap();
    assert_eq!(
        counts(&db).await.0,
        2,
        "authorized function owner must see live session identity"
    );
    fs::write(&first, "bad\ntoken").unwrap();
    let mut failed = false;
    for _ in 0..100 {
        let (ok, bad, _) = counts(&db).await;
        if ok == 1 && bad == 1 {
            failed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(failed);
    let cli = || {
        Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
            .args(["check-fcm-credentials", "env-a", "60", "1"])
            .env("DATABASE_URL", &reader_url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .output()
            .unwrap()
    };
    assert_eq!(cli().status.code(), Some(2));
    fs::write(fixture.0.join("next"), "fixture-new").unwrap();
    fs::rename(fixture.0.join("next"), &first).unwrap();
    let mut recovered = false;
    for _ in 0..100 {
        let (ok, bad, _) = counts(&db).await;
        if ok == 2 && bad == 0 {
            recovered = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(recovered);
    assert_eq!(cli().status.code(), Some(0));
    let third = fixture.0.join("three");
    fs::write(&third, "fixture-three").unwrap();
    let mut exit_worker = worker(&url, &third, "fcm_health_exit", true);
    let mut third_ready = false;
    for _ in 0..100 {
        if counts(&db).await.0 == 3 {
            third_ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(third_ready);
    fs::write(&third, "bad\ntoken").unwrap();
    let mut exit = None;
    for _ in 0..100 {
        exit = exit_worker.0.try_wait().unwrap();
        if exit.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(exit.is_some_and(|status| !status.success()));
    assert_eq!(
        counts(&db).await.1,
        1,
        "recent failure remains visible after worker exit"
    );
    db.execute("UPDATE public.novu_rust_fcm_credential_health SET observed_at=clock_timestamp()-interval '2 minutes' WHERE state='unhealthy'",&[]).await.unwrap();
    drop(exit_worker);
    drop(first_worker);
    let mut live = 2;
    for _ in 0..100 {
        live = counts(&db).await.0;
        if live == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(live, 1);
    let output = Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
        .args(["check-fcm-credentials", "env-a", "60", "2"])
        .env("DATABASE_URL", &reader_url)
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    drop(second_worker);
    db.execute("DELETE FROM public.novu_rust_delivery_operator_grants WHERE environment_id='env-a' AND database_principal='fcm_health_reader' AND decision='alert_view'",&[]).await.unwrap();
    assert!(
        reader
            .query_one(
                "SELECT * FROM public.novu_rust_fcm_credential_health_check('env-a',60)",
                &[]
            )
            .await
            .is_err()
    );
}
