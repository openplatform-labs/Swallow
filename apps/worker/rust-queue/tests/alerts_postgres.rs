//! Uses a disposable PostgreSQL database; sends no external messages.
use novu_postgres_queue::alerts::{self, AlertError, Reservation};
use tokio_postgres::NoTls;

#[tokio::test]
#[ignore = "requires disposable PostgreSQL"]
async fn alert_reservations_fence_concurrency_and_ambiguous_recovery() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (mut a, da) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    let (mut b, db) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = da.await;
    });
    tokio::spawn(async move {
        let _ = db.await;
    });
    a.batch_execute("CREATE TABLE novu_rust_environments(id text PRIMARY KEY,organization_id text NOT NULL); INSERT INTO novu_rust_environments VALUES ('env-a','org'),('env-b','org')").await.unwrap();
    a.batch_execute(novu_postgres_queue::MIGRATION)
        .await
        .unwrap();
    a.batch_execute(novu_postgres_queue::reconciliation::MIGRATION)
        .await
        .unwrap();
    a.batch_execute(novu_postgres_queue::reconciliation::OPERATOR_GRANTS_MIGRATION)
        .await
        .unwrap();
    a.batch_execute(alerts::MIGRATION).await.unwrap();
    a.batch_execute(alerts::MIGRATION).await.unwrap();
    a.batch_execute(alerts::RESOLUTION_MIGRATION).await.unwrap();
    a.batch_execute(alerts::RESOLUTION_MIGRATION).await.unwrap();
    assert!(
        alerts::reserve(&mut a, "env-a", "slack", "ops", 60)
            .await
            .is_err()
    );
    a.execute("INSERT INTO novu_rust_delivery_operator_grants VALUES ('env-a',session_user,'alert_send'),('env-b',session_user,'alert_send')", &[]).await.unwrap();
    let (first, second) = tokio::join!(
        alerts::reserve(&mut a, "env-a", "slack", "ops", 60),
        alerts::reserve(&mut b, "env-a", "slack", "ops", 60)
    );
    let results = [first.unwrap(), second.unwrap()];
    let id = results
        .iter()
        .find_map(|x| {
            if let Reservation::Claimed(id) = x {
                Some(*id)
            } else {
                None
            }
        })
        .unwrap();
    assert_eq!(
        results
            .iter()
            .filter(|x| matches!(x, Reservation::Claimed(_)))
            .count(),
        1
    );
    assert_eq!(
        results
            .iter()
            .filter(|x| **x == Reservation::InFlight)
            .count(),
        1
    );
    assert!(
        alerts::finish(&mut a, "env-a", "slack", "ops", id, &Ok(()))
            .await
            .unwrap()
    );
    assert_eq!(
        alerts::reserve(&mut a, "env-a", "slack", "ops", 60)
            .await
            .unwrap(),
        Reservation::CoolingDown
    );
    a.execute("UPDATE novu_rust_alert_delivery_slots SET last_started_at=clock_timestamp()-interval '2 minutes' WHERE environment_id='env-a' AND channel='slack' AND target_id='ops'", &[]).await.unwrap();
    let next = match alerts::reserve(&mut a, "env-a", "slack", "ops", 60)
        .await
        .unwrap()
    {
        Reservation::Claimed(id) => id,
        _ => panic!("expected next claim"),
    };
    a.execute("UPDATE novu_rust_alert_delivery_slots SET lease_until=clock_timestamp()-interval '1 second' WHERE environment_id='env-a' AND channel='slack' AND target_id='ops'", &[]).await.unwrap();
    assert_eq!(alerts::health(&a, "env-a").await.unwrap()["expired"], 1);
    assert!(
        !alerts::finish(&mut b, "env-a", "slack", "ops", next, &Ok(()))
            .await
            .unwrap()
    );
    assert_eq!(
        alerts::reserve(&mut a, "env-a", "slack", "ops", 60)
            .await
            .unwrap(),
        Reservation::Blocked
    );
    let row=a.query_one("SELECT s.state,a.state FROM novu_rust_alert_delivery_slots s JOIN novu_rust_alert_delivery_attempts a ON a.id=s.attempt_id WHERE s.environment_id='env-a' AND s.channel='slack' AND s.target_id='ops'", &[]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "unknown");
    assert_eq!(row.get::<_, String>(1), "unknown");
    assert_eq!(alerts::health(&a, "env-a").await.unwrap()["unknown"], 1);
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
        .args(["alert-status", "env-a"])
        .env("DATABASE_URL", &url)
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .output()
        .unwrap();
    assert!(output.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()[0]["state"],
        "unknown"
    );
    assert!(
        alerts::resolve(
            &mut a,
            "env-a",
            "slack",
            "ops",
            "retry",
            "operator",
            "provider checked"
        )
        .await
        .is_err()
    );
    a.execute("INSERT INTO novu_rust_delivery_operator_grants VALUES ('env-a',session_user,'alert_retry'),('env-b',session_user,'alert_retry'),('env-b',session_user,'alert_accepted')", &[]).await.unwrap();
    assert!(
        alerts::resolve(
            &mut a,
            "env-a",
            "slack",
            "ops",
            "accepted",
            "operator",
            "provider checked"
        )
        .await
        .is_err()
    );
    let resolved = std::process::Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
        .args([
            "alert-resolve",
            "env-a",
            "slack",
            "ops",
            "retry",
            "operator",
            "provider checked",
        ])
        .env("DATABASE_URL", &url)
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .output()
        .unwrap();
    assert!(resolved.status.success());
    assert_eq!(alerts::health(&a, "env-a").await.unwrap()["unknown"], 0);
    let audit=a.query_one("SELECT previous_state,decision,database_session_user FROM novu_rust_alert_delivery_resolutions WHERE attempt_id=$1", &[&next]).await.unwrap();
    assert_eq!(audit.get::<_, String>(0), "unknown");
    assert_eq!(audit.get::<_, String>(1), "retry");
    assert_eq!(
        audit.get::<_, String>(2),
        a.query_one("SELECT session_user", &[])
            .await
            .unwrap()
            .get::<_, String>(0)
    );
    assert!(matches!(
        alerts::reserve(&mut a, "env-a", "slack", "ops", 60)
            .await
            .unwrap(),
        Reservation::Claimed(_)
    ));
    let other = match alerts::reserve(&mut a, "env-b", "email", "ops", 60)
        .await
        .unwrap()
    {
        Reservation::Claimed(id) => id,
        _ => panic!("expected isolated claim"),
    };
    assert!(
        alerts::finish(
            &mut a,
            "env-b",
            "email",
            "ops",
            other,
            &Err(AlertError::Rejected(429))
        )
        .await
        .unwrap()
    );
    assert_eq!(
        alerts::reserve(&mut a, "env-b", "email", "ops", 60)
            .await
            .unwrap(),
        Reservation::Blocked
    );
    assert!(
        alerts::resolve(
            &mut a,
            "env-b",
            "email",
            "ops",
            "accepted",
            "operator",
            "provider checked"
        )
        .await
        .is_err()
    );
    alerts::resolve(
        &mut a,
        "env-b",
        "email",
        "ops",
        "retry",
        "operator",
        "provider checked",
    )
    .await
    .unwrap();
    assert!(matches!(
        alerts::reserve(&mut a, "env-b", "email", "ops", 60)
            .await
            .unwrap(),
        Reservation::Claimed(_)
    ));
    assert!(
        alerts::reserve(&mut a, "missing", "email", "ops", 60)
            .await
            .is_err()
    );
    assert!(
        alerts::reserve(&mut a, "env-a", "email", "not safe!", 60)
            .await
            .is_err()
    );
}
