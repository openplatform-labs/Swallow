//! Storage invariants for future Slack channel endpoint APIs and workflows.
use novu_postgres_queue::in_app;
use serde_json::json;
use tokio_postgres::NoTls;

#[tokio::test]
#[ignore = "requires disposable PostgreSQL"]
async fn endpoints_are_tenant_scoped_and_bound_to_subscriber_identity() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (mut db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    db.batch_execute(in_app::MIGRATION).await.unwrap();
    let migration = include_str!("../migrations/0075_slack_channel_endpoints.sql");
    db.batch_execute(migration).await.unwrap();
    db.batch_execute(migration).await.unwrap();
    db.execute("INSERT INTO novu_rust_subscribers(environment_id,subscriber_id,internal_id) VALUES ('a','alice','old'),('b','alice','other')",&[]).await.unwrap();
    let insert = "INSERT INTO novu_rust_slack_channel_endpoints(environment_id,identifier,subscriber_id,subscriber_internal_id,integration_identifier,endpoint_type,endpoint) VALUES ($1,$2,'alice',$3,'slack-main',$4,$5) RETURNING internal_id";
    let old: uuid::Uuid = db
        .query_one(
            insert,
            &[
                &"a",
                &"channel",
                &"old",
                &"slack_channel",
                &json!({"channelId":"C123"}),
            ],
        )
        .await
        .unwrap()
        .get(0);
    db.query_one(
        insert,
        &[
            &"b",
            &"channel",
            &"other",
            &"slack_user",
            &json!({"userId":"U123"}),
        ],
    )
    .await
    .unwrap();
    assert!(
        db.query_one(
            insert,
            &[
                &"a",
                &"bad-owner",
                &"other",
                &"slack_user",
                &json!({"userId":"U123"})
            ]
        )
        .await
        .is_err()
    );
    for endpoint in [
        json!({"channelId":"C123","token":"secret"}),
        json!({"userId":"U123"}),
        json!({"channelId":null}),
        json!({"channelId":""}),
    ] {
        assert!(
            db.query_one(
                insert,
                &[&"a", &"invalid", &"old", &"slack_channel", &endpoint]
            )
            .await
            .is_err()
        );
    }
    let tx = db.transaction().await.unwrap();
    tx.execute(
        "DELETE FROM novu_rust_subscribers WHERE environment_id='a' AND subscriber_id='alice'",
        &[],
    )
    .await
    .unwrap();
    tx.execute("INSERT INTO novu_rust_subscribers(environment_id,subscriber_id,internal_id) VALUES ('a','alice','new')",&[]).await.unwrap();
    tx.commit().await.unwrap();
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM novu_rust_slack_channel_endpoints WHERE environment_id='a'",
            &[]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM novu_rust_slack_channel_endpoints WHERE environment_id='b'",
            &[]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        1
    );
    assert!(
        db.query_one(
            insert,
            &[
                &"a",
                &"channel",
                &"old",
                &"slack_channel",
                &json!({"channelId":"C123"})
            ]
        )
        .await
        .is_err()
    );
    let replacement: uuid::Uuid = db
        .query_one(
            insert,
            &[
                &"a",
                &"channel",
                &"new",
                &"slack_channel",
                &json!({"channelId":"C456"}),
            ],
        )
        .await
        .unwrap()
        .get(0);
    assert_ne!(old, replacement);
    let (other, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let tx = db.transaction().await.unwrap();
    tx.execute(
        "DELETE FROM novu_rust_subscribers WHERE environment_id='a' AND subscriber_id='alice'",
        &[],
    )
    .await
    .unwrap();
    let task = tokio::spawn(async move {
        other.execute("INSERT INTO novu_rust_slack_channel_endpoints(environment_id,identifier,subscriber_id,subscriber_internal_id,integration_identifier,endpoint_type,endpoint) VALUES ('a','race','alice','new','slack-main','slack_user','{\"userId\":\"U123\"}')", &[]).await
    });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert!(
        !task.is_finished(),
        "endpoint insertion should wait for subscriber deletion"
    );
    tx.commit().await.unwrap();
    assert!(task.await.unwrap().is_err());
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM novu_rust_slack_channel_endpoints WHERE environment_id='a'",
            &[]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
}
