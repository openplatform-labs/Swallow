
use super::*;
use serde_json::json;
use tokio_postgres::NoTls;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires a disposable PostgreSQL database and loopback access; safe mode"]
async fn changed_actor_is_rejected_by_all_channels() {
    assert!(!allow_recreated_subscriber());
    let (mut db, connection) =
        tokio_postgres::connect(&std::env::var("TEST_DATABASE_URL").unwrap(), NoTls)
            .await
            .unwrap();
    tokio::spawn(async move { connection.await.unwrap() });
    db.batch_execute("CREATE TABLE novu_rust_workflows (environment_id text,identifier text,in_app_content text,has_steps boolean NOT NULL DEFAULT true,PRIMARY KEY(environment_id,identifier));").await.unwrap();
    for migration in [
        crate::MIGRATION,
        crate::in_app::MIGRATION,
        crate::in_app::STATE_MIGRATION,
        crate::in_app::STATE_DATES_MIGRATION,
        crate::in_app::IDENTIFIER_MIGRATION,
        crate::in_app::INBOX_METADATA_MIGRATION,
        crate::in_app::INBOX_DATE_PRESENCE_MIGRATION,
        crate::in_app::INBOX_DATA_MIGRATION,
        crate::in_app::INBOX_DATA_ARRAY_MIGRATION,
        crate::in_app::CONTEXT_MIGRATION,
        crate::in_app::CONTEXT_DATA_MIGRATION,
        crate::in_app::CONTEXT_TIMESTAMPS_MIGRATION,
        crate::in_app::CONTEXT_CURSOR_MIGRATION,
        crate::in_app::SUBSCRIBER_OBJECTID_MIGRATION,
        crate::in_app::GLOBAL_PREFERENCES_MIGRATION,
        crate::in_app::WORKFLOW_PREFERENCES_MIGRATION,
        crate::in_app::SUBSCRIBER_WORKFLOW_PREFERENCES_MIGRATION,
        crate::in_app::STEP_OUTCOMES_MIGRATION,
        crate::in_app::SUBSCRIBER_ONLINE_MIGRATION,
        crate::in_app::TOPICS_MIGRATION,
        crate::in_app::TOPIC_DELETE_RESTRICT_MIGRATION,
        crate::in_app::TOPIC_DATA_MIGRATION,
        crate::in_app::TOPIC_SUBSCRIPTIONS_MIGRATION,
        crate::in_app::TOPIC_SUBSCRIPTIONS_CURSOR_MIGRATION,
        crate::in_app::TOPIC_SUBSCRIPTION_NAME_MIGRATION,
        crate::in_app::TOPIC_SUBSCRIPTION_PREFERENCES_MIGRATION,
        crate::in_app::SUBSCRIBER_SUBSCRIPTION_CURSOR_MIGRATION,
        crate::in_app::SNOOZED_UNTIL_MIGRATION,
        crate::in_app::DELIVERY_HISTORY_MIGRATION,
        crate::in_app::WS_EVENTS_MIGRATION,
        crate::in_app::WS_BEST_EFFORT_MIGRATION,
        crate::in_app::MESSAGE_SEVERITY_MIGRATION,
        crate::in_app::IN_APP_STEP_ACTIVE_MIGRATION,
        crate::in_app::NOTIFICATIONS_MIGRATION,
        crate::in_app::EMAIL_STEP_MIGRATION,
        crate::in_app::EMAIL_MESSAGES_MIGRATION,
        crate::in_app::EMAIL_PROVIDER_RESULT_MIGRATION,
        crate::in_app::EMAIL_STEP_ID_MIGRATION,
        crate::in_app::EMAIL_REVALIDATION_MIGRATION,
        crate::in_app::SMS_STEP_MIGRATION,
        crate::in_app::SMS_MESSAGES_MIGRATION,
        crate::in_app::SMS_STEP_ID_MIGRATION,
        crate::in_app::PUSH_STEP_MIGRATION,
        crate::in_app::FCM_CREDENTIALS_MIGRATION,
        crate::in_app::PUSH_MESSAGES_MIGRATION,
        crate::in_app::IN_APP_SUBSCRIBER_IDENTITY_MIGRATION,
        crate::in_app::MULTIPLE_FCM_CHANNELS_MIGRATION,
        crate::in_app::PUSH_STEP_ID_MIGRATION,
        crate::sendgrid_queue::MIGRATION,
        crate::twilio_queue::MIGRATION,
        crate::twilio_queue::DISPATCH_ID_MIGRATION,
        crate::fcm_queue::MIGRATION,
        crate::in_app::DELIVERY_STATE_MIGRATION,
    ] {
        db.batch_execute(migration).await.unwrap();
    }

    db.batch_execute("INSERT INTO novu_rust_subscribers(environment_id,subscriber_id,internal_id,profile) VALUES ('env','recipient','recipient-internal','{\"email\":\"r@example.test\",\"phone\":\"+15555555555\"}'),('env','actor','old-id','{\"firstName\":\"Original\"}'); INSERT INTO novu_rust_workflows(environment_id,identifier,in_app_content,email_subject,email_html,sms_content,push_title,push_content) VALUES ('env','workflow','hello','{{actor.firstName}}','hello','{{actor.firstName}}','{{actor.firstName}}','hello'); INSERT INTO novu_rust_fcm_devices(environment_id,subscriber_id,device_tokens) VALUES ('env','recipient',ARRAY['device']);").await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let email = sendgrid::Sendgrid::test_endpoint(
        sendgrid::Config {
            api_key: "test".into(),
            from: "sender@example.test".into(),
            sender_name: "Sender".into(),
            region: sendgrid::Region::Global,
        },
        &origin,
    )
    .unwrap();
    let sms = twilio::Twilio::test_origin(
        twilio::Config {
            account_sid: "ACtest".into(),
            auth_token: "test".into(),
            api_key_sid: None,
            api_key_secret: None,
            from: "+15555555555".into(),
            region: twilio::Region::Us,
        },
        &origin,
    )
    .unwrap();
    let push = fcm::Fcm::with_origin("project", "test".into(), &origin).unwrap();
    let trigger = Uuid::new_v4();
    enqueue(&db,trigger,"env",in_app::JOB_KIND,&json!({"workflow":"workflow","transactionId":"actor-change","content":"hello","severityOverride":null,"payload":{},"recipients":[{"subscriberId":"recipient"}],"actor":{"subscriberId":"actor","firstName":"Original"},"recipientIdentitySnapshot":{"recipient":"recipient-internal"},"actorIdentitySnapshot":"old-id"})).await.unwrap();
    assert!(in_app::process_one(&mut db).await.unwrap());
    let rows=db.query("SELECT id,kind,payload->'workflow_context'->>'actor_internal_id' FROM novu_queue_jobs WHERE kind=ANY($1)",&[&vec![sendgrid_queue::JOB_KIND,twilio_queue::JOB_KIND,fcm_queue::JOB_KIND]]).await.unwrap();
    assert_eq!(rows.len(), 3);
    let mut ids = Vec::new();
    for row in rows {
        assert_eq!(row.get::<_, String>(2), "old-id");
        ids.push(row.get::<_, Uuid>(0));
    }
    db.batch_execute("DELETE FROM novu_rust_subscribers WHERE environment_id='env' AND subscriber_id='actor'; INSERT INTO novu_rust_subscribers(environment_id,subscriber_id,internal_id,profile) VALUES ('env','actor','new-id','{\"firstName\":\"New Person\"}')").await.unwrap();
    assert!(
        sendgrid_queue::process_one(&mut db, "env", &email)
            .await
            .unwrap()
    );
    assert!(
        twilio_queue::process_one(&mut db, "env", &sms)
            .await
            .unwrap()
    );
    assert!(fcm_queue::process_one(&mut db, "env", &push).await.unwrap());
    let rows = db
        .query(
            "SELECT status,last_error FROM novu_queue_jobs WHERE id=ANY($1)",
            &[&ids],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);
    for row in rows {
        assert_eq!(row.get::<_, String>(0), "failed");
        assert_eq!(row.get::<_, String>(1), "actor identity changed or missing");
    }
    let tx = db.transaction().await.unwrap();
    let actor = json!({"subscriberId":"actor"});
    let recipient = json!({"subscriberId":"recipient"});
    assert!(
        resolve_actor(&tx, "env", Some(&actor), None, "recipient", &recipient)
            .await
            .unwrap()
            .is_err()
    );
    assert!(
        resolve_actor(
            &tx,
            "env",
            Some(&actor),
            Some("old-id"),
            "recipient",
            &recipient
        )
        .await
        .unwrap()
        .is_err()
    );
    let resolved = resolve_actor(
        &tx,
        "env",
        Some(&actor),
        Some("new-id"),
        "recipient",
        &recipient,
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();
    assert_eq!(resolved["firstName"], "New Person");
    tx.rollback().await.unwrap();
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(200), listener.accept())
            .await
            .is_err()
    );
}
