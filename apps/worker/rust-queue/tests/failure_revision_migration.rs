//! Upgrade a legacy acknowledged failure and detect an identical recurrence.
use novu_postgres_queue::{self as queue, alerts, reconciliation};
use serde_json::json;
use tokio_postgres::NoTls;
use uuid::Uuid;
#[tokio::test]
#[ignore = "requires disposable PostgreSQL"]
async fn prior_acknowledgement_survives_upgrade_but_not_a_new_failure() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    db.batch_execute("CREATE TABLE novu_rust_environments(id text PRIMARY KEY,organization_id text NOT NULL); INSERT INTO novu_rust_environments VALUES ('a','org')").await.unwrap();
    for sql in [
        queue::MIGRATION,
        reconciliation::MIGRATION,
        reconciliation::OPERATOR_GRANTS_MIGRATION,
        alerts::MIGRATION,
        alerts::RESOLUTION_MIGRATION,
        reconciliation::OPERATOR_GRANT_LOCK_MIGRATION,
        include_str!("../migrations/0072_operator_grant_management.sql"),
        include_str!("../migrations/0073_operator_role_identity.sql"),
        include_str!("../migrations/0089_scoped_delivery_health.sql"),
        include_str!("../migrations/0090_failed_queue_attention.sql"),
    ] {
        db.batch_execute(sql).await.unwrap();
    }
    db.execute("INSERT INTO novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'a',session_user,decision,oid FROM pg_roles CROSS JOIN unnest(ARRAY['alert_view','queue_ack']) decision WHERE rolname=session_user",&[]).await.unwrap();
    let job = Uuid::new_v4();
    queue::enqueue(&db, job, "a", "fixture", &json!({}))
        .await
        .unwrap();
    db.execute(
        "UPDATE novu_queue_jobs SET status='failed',last_error='same error',attempts=1 WHERE id=$1",
        &[&job],
    )
    .await
    .unwrap();
    assert!(
        reconciliation::acknowledge_failed_queue(&db, "a", job, "operator", "before upgrade")
            .await
            .unwrap()
    );
    db.batch_execute(include_str!(
        "../migrations/0091_queue_failure_revision.sql"
    ))
    .await
    .unwrap();
    db.batch_execute(include_str!(
        "../migrations/0091_queue_failure_revision.sql"
    ))
    .await
    .unwrap();
    assert_eq!(
        reconciliation::failed_queue_count(&db, "a").await.unwrap(),
        0
    );
    db.execute(
        "UPDATE novu_queue_jobs SET status='pending' WHERE id=$1",
        &[&job],
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE novu_queue_jobs SET status='failed' WHERE id=$1",
        &[&job],
    )
    .await
    .unwrap();
    assert_eq!(
        reconciliation::failed_queue_count(&db, "a").await.unwrap(),
        1
    );
    assert!(
        reconciliation::acknowledge_failed_queue(&db, "a", job, "operator", "after upgrade")
            .await
            .unwrap()
    );
    let r=db.query_one("SELECT count(*),count(DISTINCT job_failure_revision) FROM novu_rust_queue_failure_acknowledgements WHERE job_id=$1",&[&job]).await.unwrap();
    assert_eq!(r.get::<_, i64>(0), 2);
    assert_eq!(r.get::<_, i64>(1), 2);
    db.execute(
        "UPDATE novu_queue_jobs SET status='failed' WHERE id=$1",
        &[&job],
    )
    .await
    .unwrap();
    assert_eq!(
        reconciliation::failed_queue_count(&db, "a").await.unwrap(),
        0
    );
}
