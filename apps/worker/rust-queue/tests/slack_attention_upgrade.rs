//! Upgrade old Slack acknowledgements and detect identical recurrent failures.
use novu_postgres_queue::{self as queue, alerts, reconciliation, slack_queue, slack_workflow};
use serde_json::json;
use tokio_postgres::NoTls;
use uuid::Uuid;
#[tokio::test]
#[ignore = "requires disposable PostgreSQL"]
async fn old_acknowledgements_survive_upgrade_but_not_recurrence() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (mut db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    db.batch_execute("CREATE TABLE novu_rust_environments(id text PRIMARY KEY,organization_id text NOT NULL); INSERT INTO novu_rust_environments VALUES ('a','org')").await.unwrap();
    for sql in [
        queue::MIGRATION,
        slack_queue::MIGRATION,
        reconciliation::MIGRATION,
        reconciliation::OPERATOR_GRANTS_MIGRATION,
        alerts::MIGRATION,
        alerts::RESOLUTION_MIGRATION,
        reconciliation::OPERATOR_GRANT_LOCK_MIGRATION,
        slack_queue::OPERATOR_MIGRATION,
        include_str!("../migrations/0072_operator_grant_management.sql"),
        include_str!("../migrations/0073_operator_role_identity.sql"),
        slack_queue::WORKFLOW_ATTENTION_MIGRATION,
        include_str!("../migrations/0080_slack_ack_permission.sql"),
        include_str!("../migrations/0081_slack_view_permission.sql"),
        include_str!("../migrations/0082_slack_ack_function.sql"),
        include_str!("../migrations/0084_slack_scoped_health.sql"),
    ] {
        db.batch_execute(sql).await.unwrap();
    }
    db.execute("INSERT INTO novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'a',session_user,decision,oid FROM pg_roles CROSS JOIN unnest(ARRAY['slack_ack','slack_view']) decision WHERE rolname=session_user",&[]).await.unwrap();
    let failed = Uuid::new_v4();
    let missing = Uuid::new_v4();
    for (id, kind, status, error) in [
        (
            failed,
            slack_workflow::DELIVERY_KIND,
            "failed",
            "fixture error",
        ),
        (
            missing,
            slack_workflow::FANOUT_KIND,
            "cancelled",
            "no Slack endpoints",
        ),
    ] {
        queue::enqueue(&db, id, "a", kind, &json!({}))
            .await
            .unwrap();
        db.execute(
            "UPDATE novu_queue_jobs SET status=$2,last_error=$3,attempts=1 WHERE id=$1",
            &[&id, &status, &error],
        )
        .await
        .unwrap();
        assert!(
            slack_queue::acknowledge_workflow(&mut db, "a", id, "operator", "old ack")
                .await
                .unwrap()
        );
    }
    assert_eq!(
        slack_queue::scoped_health(&db, "a").await.unwrap()["preDispatchFailures"],
        0
    );
    db.batch_execute(slack_queue::ATTENTION_REVISION_MIGRATION)
        .await
        .unwrap();
    db.batch_execute(slack_queue::ATTENTION_REVISION_MIGRATION)
        .await
        .unwrap();
    let health = slack_queue::health(&db, "a").await.unwrap();
    assert_eq!(health, slack_queue::scoped_health(&db, "a").await.unwrap());
    assert_eq!(health["preDispatchFailures"], 0);
    assert_eq!(health["missingEndpoints"], 0);
    for (id, status) in [(failed, "failed"), (missing, "cancelled")] {
        db.execute(
            "UPDATE novu_queue_jobs SET status='pending' WHERE id=$1",
            &[&id],
        )
        .await
        .unwrap();
        db.execute(
            "UPDATE novu_queue_jobs SET status=$2 WHERE id=$1",
            &[&id, &status],
        )
        .await
        .unwrap();
    }
    let health = slack_queue::health(&db, "a").await.unwrap();
    assert_eq!(health, slack_queue::scoped_health(&db, "a").await.unwrap());
    assert_eq!(health["preDispatchFailures"], 1);
    assert_eq!(health["missingEndpoints"], 1);
    assert_eq!(
        slack_queue::list_attention(&db, "a", None)
            .await
            .unwrap()
            .len(),
        2
    );
    for id in [failed, missing] {
        assert!(
            slack_queue::acknowledge_workflow(&mut db, "a", id, "operator", "new ack")
                .await
                .unwrap()
        );
        assert!(
            !slack_queue::acknowledge_workflow(&mut db, "a", id, "operator", "duplicate")
                .await
                .unwrap()
        );
    }
    assert_eq!(
        slack_queue::scoped_health(&db, "a").await.unwrap()["preDispatchFailures"],
        0
    );
    assert_eq!(
        slack_queue::health(&db, "a").await.unwrap()["missingEndpoints"],
        0
    );
    let rows=db.query_one("SELECT count(*),count(DISTINCT job_attention_revision) FROM novu_rust_slack_workflow_acknowledgements WHERE job_id=$1",&[&failed]).await.unwrap();
    assert_eq!(rows.get::<_, i64>(0), 2);
    assert_eq!(rows.get::<_, i64>(1), 2);
    db.batch_execute(slack_queue::ATTENTION_REVISION_MIGRATION)
        .await
        .unwrap();
    assert_eq!(
        slack_queue::list_attention(&db, "a", None)
            .await
            .unwrap()
            .len(),
        0
    );
}
