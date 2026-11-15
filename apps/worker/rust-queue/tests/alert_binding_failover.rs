//! Disposable loopback PostgreSQL synchronous promotion and alert binding.
use std::{
    process::{Command, Stdio},
    time::Duration,
};
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;

async fn connect(url: &str) -> Client {
    for _ in 0..100 {
        if let Ok((db, driver)) = tokio_postgres::connect(url, NoTls).await {
            tokio::spawn(async move {
                let _ = driver.await;
            });
            return db;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("test database unavailable")
}
fn run_alert(url: &str, pool: &str) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_send-delivery-alert"))
        .env_clear()
        .args(["ha-binding-env", "300", "120", "60"])
        .env("DATABASE_URL", url)
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .env("SWALLOW_ALERT_EMAIL_TO", "ops@example.test")
        .env("SWALLOW_ALERT_EMAIL_PROVIDER", "sendgrid")
        .env("SWALLOW_ALERT_EMAIL_TARGET_ID", "mail-ha")
        .env("SWALLOW_ALERT_EMAIL_BINDING_PROFILE", "configuration")
        .env(
            "SWALLOW_ALERT_BINDING_KEY",
            "fixture-binding-key-0123456789abcdef0123456789",
        )
        .env("NOVU_SENDGRID_API_KEY", "fixture-provider-secret")
        .env("NOVU_SENDGRID_FROM", "from@example.test")
        .env("NOVU_SENDGRID_SENDER_NAME", "Fixture")
        .env("NOVU_SENDGRID_IP_POOL_NAME", pool)
        .env("SWALLOW_ALERT_RESEND_ENABLED", "false")
        .env("SWALLOW_ALERT_QUEUE_MAX_WAIT_SECONDS", "60")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .unwrap()
}
#[tokio::test]
#[ignore = "requires disposable synchronously replicated local PostgreSQL; stops its primary"]
async fn alert_configuration_binding_survives_synchronous_promotion() {
    let primary_url = std::env::var("TEST_PRIMARY_URL").unwrap();
    let standby_url = std::env::var("TEST_STANDBY_URL").unwrap();
    let failover_url = std::env::var("TEST_FAILOVER_URL").unwrap();
    let primary = connect(&primary_url).await;
    primary.batch_execute("INSERT INTO public.novu_rust_environments VALUES ('ha-binding-env','org'); INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'ha-binding-env',rolname,'alert_send',oid FROM pg_roles WHERE rolname=session_user; INSERT INTO public.novu_rust_alert_delivery_slots(environment_id,channel,target_id,state) VALUES ('ha-binding-env','email','mail-ha','rejected')").await.unwrap();
    let job = Uuid::new_v4();
    novu_postgres_queue::enqueue(
        &primary,
        job,
        "ha-binding-env",
        "fixture",
        &serde_json::json!({}),
    )
    .await
    .unwrap();
    primary.execute("UPDATE public.novu_queue_jobs SET available_at=clock_timestamp()-interval '5 minutes' WHERE id=$1",&[&job]).await.unwrap();
    let first = run_alert(&failover_url, "pool-a");
    assert_eq!(
        first.status.code(),
        Some(2),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    let fingerprint:String=primary.query_one("SELECT fingerprint FROM public.novu_rust_alert_destination_bindings WHERE environment_id='ha-binding-env' AND channel='email' AND target_id='mail-ha'",&[]).await.unwrap().get(0);
    assert_eq!(fingerprint.len(), 64);
    let standby = connect(&standby_url).await;
    let mut replicated = false;
    for _ in 0..100 {
        let row=standby.query_one("SELECT pg_is_in_recovery(), EXISTS(SELECT 1 FROM public.novu_rust_alert_destination_bindings WHERE environment_id='ha-binding-env' AND target_id='mail-ha' AND fingerprint=$1), EXISTS(SELECT 1 FROM public.novu_rust_alert_delivery_slots WHERE environment_id='ha-binding-env' AND target_id='mail-ha' AND state='rejected')",&[&fingerprint]).await.unwrap();
        assert!(row.get::<_, bool>(0));
        if row.get::<_, bool>(1) && row.get::<_, bool>(2) {
            replicated = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        replicated,
        "standby did not replay committed binding and blocked slot"
    );
    let pgctl = std::env::var("TEST_LOCAL_PG_CTL").unwrap();
    assert!(
        Command::new(&pgctl)
            .args([
                "-D",
                &std::env::var("TEST_LOCAL_PRIMARY_DATA").unwrap(),
                "-m",
                "immediate",
                "stop"
            ])
            .stdout(Stdio::null())
            .status()
            .unwrap()
            .success()
    );
    assert!(
        Command::new(&pgctl)
            .args([
                "-D",
                &std::env::var("TEST_LOCAL_STANDBY_DATA").unwrap(),
                "promote",
                "-w"
            ])
            .stdout(Stdio::null())
            .status()
            .unwrap()
            .success()
    );
    let promoted = connect(&standby_url).await;
    assert!(
        !promoted
            .query_one("SELECT pg_is_in_recovery()", &[])
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    let (same, peer, different) = std::thread::scope(|scope| {
        let first = scope.spawn(|| run_alert(&failover_url, "pool-a"));
        let second = scope.spawn(|| run_alert(&failover_url, "pool-a"));
        let mismatched = scope.spawn(|| run_alert(&failover_url, "pool-b"));
        (
            first.join().unwrap(),
            second.join().unwrap(),
            mismatched.join().unwrap(),
        )
    });
    assert_eq!(
        same.status.code(),
        Some(2),
        "{}",
        String::from_utf8_lossy(&same.stderr)
    );
    assert_eq!(
        peer.status.code(),
        Some(2),
        "{}",
        String::from_utf8_lossy(&peer.stderr)
    );
    assert_eq!(different.status.code(), Some(1));
    let text = String::from_utf8_lossy(&different.stderr);
    assert!(!text.contains("fixture-provider-secret"));
    assert!(!text.contains("ops@example.test"));
    let current:String=promoted.query_one("SELECT fingerprint FROM public.novu_rust_alert_destination_bindings WHERE environment_id='ha-binding-env' AND target_id='mail-ha'",&[]).await.unwrap().get(0);
    assert_eq!(current, fingerprint);
    let sent:i64=promoted.query_one("SELECT count(*) FROM public.novu_rust_alert_delivery_attempts WHERE environment_id='ha-binding-env'",&[]).await.unwrap().get(0);
    assert_eq!(sent, 0);
}
