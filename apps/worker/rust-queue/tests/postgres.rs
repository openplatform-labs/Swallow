use novu_postgres_queue::{
    MIGRATION, cancel_pending, claim, complete, enqueue, enqueue_delayed, fail, renew,
    requeue_expired, retry,
};
use serde_json::json;
use std::time::Duration;
use tokio_postgres::NoTls;
use uuid::Uuid;

// Run only against a disposable database: this test creates and clears its queue table.
#[tokio::test]
#[ignore = "requires a disposable PostgreSQL database in TEST_DATABASE_URL"]
async fn postgres_queue_contract() {
    let url =
        std::env::var("TEST_DATABASE_URL").expect("set TEST_DATABASE_URL to a disposable database");
    let (mut db, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { connection.await.unwrap() });
    db.batch_execute("DROP TABLE IF EXISTS novu_queue_jobs")
        .await
        .unwrap();
    db.batch_execute(MIGRATION).await.unwrap();
    let id = Uuid::new_v4();
    let tx = db.transaction().await.unwrap();
    enqueue(&tx, id, "env-a", "workflow", &json!({"x": 1}))
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    assert!(
        claim(&db, "workflow", Duration::from_secs(30))
            .await
            .unwrap()
            .is_none()
    );

    let delayed_id = Uuid::new_v4();
    enqueue_delayed(
        &db,
        delayed_id,
        "env-a",
        "workflow",
        &json!({}),
        Duration::from_secs(3600),
    )
    .await
    .unwrap();
    assert!(
        claim(&db, "workflow", Duration::from_secs(30))
            .await
            .unwrap()
            .is_none()
    );
    assert!(!cancel_pending(&db, "env-b", delayed_id).await.unwrap());
    assert!(cancel_pending(&db, "env-a", delayed_id).await.unwrap());
    assert!(!cancel_pending(&db, "env-a", delayed_id).await.unwrap());

    let retry_id = Uuid::new_v4();
    enqueue(&db, retry_id, "env-a", "workflow", &json!({}))
        .await
        .unwrap();
    let claimed = claim(&db, "workflow", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    assert!(!cancel_pending(&db, "env-a", retry_id).await.unwrap());
    assert!(
        !retry(&db, retry_id, Uuid::new_v4(), Duration::ZERO, "oops")
            .await
            .unwrap()
    );
    assert!(
        retry(
            &db,
            retry_id,
            claimed.lease_token,
            Duration::from_secs(3600),
            "temporary"
        )
        .await
        .unwrap()
    );
    assert!(
        claim(&db, "workflow", Duration::from_secs(30))
            .await
            .unwrap()
            .is_none()
    );
    db.execute(
        "UPDATE novu_queue_jobs SET available_at = now() - interval '1 second' WHERE id = $1",
        &[&retry_id],
    )
    .await
    .unwrap();
    let retried = claim(&db, "workflow", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retried.attempts, 2);
    assert!(complete(&db, retry_id, retried.lease_token).await.unwrap());

    // Concurrent connections must claim distinct rows, never the same job twice.
    let (other, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { connection.await.unwrap() });
    let first_id = Uuid::new_v4();
    let second_id = Uuid::new_v4();
    enqueue(&db, first_id, "env-a", "parallel", &json!({}))
        .await
        .unwrap();
    enqueue(&db, second_id, "env-a", "parallel", &json!({}))
        .await
        .unwrap();
    let (a, b) = tokio::join!(
        claim(&db, "parallel", Duration::from_secs(30)),
        claim(&other, "parallel", Duration::from_secs(30))
    );
    let a = a.unwrap().unwrap();
    let b = b.unwrap().unwrap();
    assert_ne!(a.id, b.id);
    assert!([first_id, second_id].contains(&a.id));
    assert!([first_id, second_id].contains(&b.id));
    assert!(complete(&db, a.id, a.lease_token).await.unwrap());
    assert!(complete(&db, b.id, b.lease_token).await.unwrap());
    enqueue(&db, id, "env-a", "workflow", &json!({"x": 1}))
        .await
        .unwrap();
    let first = claim(&db, "workflow", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.environment_id, "env-a");
    assert_eq!(first.payload, json!({"x": 1}));
    assert_eq!(first.attempts, 1);
    assert!(
        claim(&db, "workflow", Duration::from_secs(30))
            .await
            .unwrap()
            .is_none()
    );
    assert!(!complete(&db, id, Uuid::new_v4()).await.unwrap());
    assert!(
        renew(&db, id, first.lease_token, Duration::from_secs(30))
            .await
            .unwrap()
    );
    db.execute(
        "UPDATE novu_queue_jobs SET lease_until = now() - interval '1 second' WHERE id = $1",
        &[&id],
    )
    .await
    .unwrap();
    assert!(!complete(&db, id, first.lease_token).await.unwrap());
    assert!(
        !renew(&db, id, first.lease_token, Duration::from_secs(30))
            .await
            .unwrap()
    );
    assert_eq!(requeue_expired(&db, "other").await.unwrap(), 0);
    assert_eq!(requeue_expired(&db, "workflow").await.unwrap(), 1);
    let second = claim(&db, "workflow", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(second.attempts, 2);
    assert!(!complete(&db, id, first.lease_token).await.unwrap());
    assert!(complete(&db, id, second.lease_token).await.unwrap());
    assert!(!fail(&db, id, second.lease_token, "late").await.unwrap());
    let failed_id = Uuid::new_v4();
    enqueue(&db, failed_id, "env-b", "workflow", &json!({}))
        .await
        .unwrap();
    let failed = claim(&db, "workflow", Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    assert!(
        fail(&db, failed_id, failed.lease_token, "invalid")
            .await
            .unwrap()
    );
    assert!(
        claim(&db, "workflow", Duration::from_secs(30))
            .await
            .unwrap()
            .is_none()
    );
    db.batch_execute("TRUNCATE novu_queue_jobs").await.unwrap();
}
