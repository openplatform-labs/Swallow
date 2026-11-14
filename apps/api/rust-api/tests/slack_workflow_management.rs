//! Real HTTP router, PostgreSQL migrations and Slack fan-out; no external sends.
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{path::Path, sync::Arc, time::Duration};
use tokio_postgres::{Client, NoTls};
use tower::ServiceExt;

async fn call(
    app: axum::Router,
    method: &str,
    path: &str,
    key: &str,
    body: Value,
) -> (StatusCode, Value) {
    let response = app
        .oneshot(
            Request::builder()
                .method(method)
                .uri(path)
                .header("authorization", format!("ApiKey {key}"))
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}
async fn migrate(db: &Client, dir: &Path, range: std::ops::RangeInclusive<u32>) {
    let mut files: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "sql"))
        .collect();
    files.sort();
    for file in files {
        let name = file.file_name().unwrap().to_str().unwrap();
        let version = name[..4].parse::<u32>().unwrap();
        if range.contains(&version) {
            db.batch_execute(&std::fs::read_to_string(&file).unwrap())
                .await
                .unwrap_or_else(|e| panic!("{name}: {e}"));
        }
    }
}
#[tokio::test]
#[ignore = "requires disposable PostgreSQL"]
async fn slack_step_management_is_authorized_audited_and_connected_to_fanout() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, driver) =
        tokio_postgres::connect(&format!("{url}?application_name=slack_step_api"), NoTls)
            .await
            .unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let api = root.join("migrations");
    let worker = root.join("../../worker/rust-queue/migrations");
    migrate(&db, &api, 1..=17).await;
    migrate(&db, &worker, 1..=u32::MAX).await;
    migrate(&db, &api, 18..=u32::MAX).await;
    db.batch_execute("INSERT INTO public.novu_rust_users VALUES ('u'); INSERT INTO public.novu_rust_environments VALUES ('a','org'),('b','org'); INSERT INTO public.novu_rust_workflows(environment_id,identifier,active,has_steps,in_app_step_active) VALUES ('a','workflow',true,true,false),('b','workflow',true,true,false); INSERT INTO public.novu_rust_subscribers(environment_id,subscriber_id,internal_id,profile) VALUES ('a','alice','alice-id','{\"firstName\":\"Jane\"}'); INSERT INTO public.novu_rust_slack_endpoint_integrations(environment_id,identifier) VALUES ('a','main'); INSERT INTO public.novu_rust_slack_channel_endpoints(environment_id,identifier,subscriber_id,subscriber_internal_id,integration_identifier,endpoint_type,endpoint) VALUES ('a','endpoint','alice','alice-id','main','slack_channel','{\"channelId\":\"C123\"}')").await.unwrap();
    let hash = |s: &[u8]| format!("{:x}", Sha256::digest(s));
    let (a, b, read) = (hash(b"a-key"), hash(b"b-key"), hash(b"read-key"));
    db.execute("INSERT INTO public.novu_rust_api_keys(key_hash,environment_id,user_id) VALUES ($1,'a','u'),($2,'b','u'),($3,'a','u')",&[&a,&b,&read]).await.unwrap();
    let app = novu_rust_api::router_with_subscriber_secret(Arc::new(db), None);
    let (mut setup, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let path = "/v1/experimental/workflows/workflow/slack-step";
    assert_eq!(
        call(app.clone(), "GET", path, "a-key", Value::Null).await.0,
        StatusCode::FORBIDDEN
    );
    setup.execute("INSERT INTO public.novu_rust_slack_workflow_permissions VALUES ($1,'read'),($1,'write'),($2,'read'),($2,'write'),($3,'read')",&[&a,&b,&read]).await.unwrap();
    let (status, response) = call(app.clone(), "GET", path, "a-key", Value::Null).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(response["data"]["content"], Value::Null);
    assert_eq!(
        call(
            app.clone(),
            "PUT",
            path,
            "read-key",
            json!({"content":"wrong","active":true})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            app.clone(),
            "PUT",
            path,
            "a-key",
            json!({"content":"x","active":true,"token":"secret"})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        call(
            app.clone(),
            "PUT",
            path,
            "a-key",
            json!({"content":"x".repeat(40001),"active":true})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    let (status, result) = call(
        app.clone(),
        "PUT",
        path,
        "a-key",
        json!({"content":"Hello {{subscriber.firstName}}","active":true}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(result["data"]["active"], true);
    assert_eq!(
        call(app.clone(), "GET", path, "b-key", Value::Null).await.1["data"]["content"],
        Value::Null
    );
    let audit=setup.query_one("SELECT previous_content_bytes,new_content_bytes,previous_active,new_active FROM public.novu_rust_slack_workflow_audit WHERE environment_id='a'",&[]).await.unwrap();
    assert_eq!(audit.get::<_, Option<i32>>(0), None);
    assert_eq!(audit.get::<_, Option<i32>>(1), Some(30));
    assert!(audit.get::<_, bool>(2) && audit.get::<_, bool>(3));
    // An API login needs the lock function and narrowly granted tables, not
    // permission-table access or audit-table reads.
    let role = format!("chat_api_{}", uuid::Uuid::new_v4().simple());
    setup.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT SELECT ON public.novu_rust_users,public.novu_rust_api_keys,public.novu_rust_workflows TO {role}; GRANT UPDATE(chat_content,chat_step_active) ON public.novu_rust_workflows TO {role}; GRANT INSERT ON public.novu_rust_slack_workflow_audit TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_lock_slack_workflow_grant(text,text,text) TO {role}")).await.unwrap();
    let (limited, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    limited
        .batch_execute(&format!("SET SESSION AUTHORIZATION {role}"))
        .await
        .unwrap();
    assert!(
        limited
            .query(
                "SELECT * FROM public.novu_rust_slack_workflow_permissions",
                &[]
            )
            .await
            .is_err()
    );
    assert!(
        limited
            .query("SELECT * FROM public.novu_rust_slack_workflow_audit", &[])
            .await
            .is_err()
    );
    let limited_app = novu_rust_api::router_with_subscriber_secret(Arc::new(limited), None);
    assert_eq!(
        call(
            limited_app,
            "PUT",
            path,
            "a-key",
            json!({"content":"Hello {{subscriber.firstName}}","active":true})
        )
        .await
        .0,
        StatusCode::OK
    );
    // An audit failure rolls back the workflow mutation.
    setup.batch_execute("ALTER TABLE public.novu_rust_slack_workflow_audit ADD CONSTRAINT fixture_reject CHECK (false) NOT VALID").await.unwrap();
    assert_eq!(
        call(
            app.clone(),
            "PUT",
            path,
            "a-key",
            json!({"content":"changed","active":true})
        )
        .await
        .0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(
        call(app.clone(), "GET", path, "a-key", Value::Null).await.1["data"]["content"],
        "Hello {{subscriber.firstName}}"
    );
    setup
        .batch_execute(
            "ALTER TABLE public.novu_rust_slack_workflow_audit DROP CONSTRAINT fixture_reject",
        )
        .await
        .unwrap();
    // Trigger through the HTTP API and advance to the endpoint-specific Slack job.
    assert_eq!(
        call(
            app.clone(),
            "POST",
            "/v1/events/trigger",
            "a-key",
            json!({"name":"workflow","to":"alice","transactionId":"t-1","payload":{}})
        )
        .await
        .0,
        StatusCode::CREATED
    );
    assert!(
        novu_postgres_queue::in_app::process_one(&mut setup)
            .await
            .unwrap()
    );
    assert!(
        novu_postgres_queue::slack_workflow::fanout_one(&mut setup, "a")
            .await
            .unwrap()
    );
    let row = setup
        .query_one(
            "SELECT payload FROM public.novu_queue_jobs WHERE environment_id='a' AND kind=$1",
            &[&novu_postgres_queue::slack_workflow::DELIVERY_KIND],
        )
        .await
        .unwrap();
    let payload: Value = row.get(0);
    assert_eq!(payload["target"], "C123");
    assert_eq!(
        payload["context"]["content_template"],
        "Hello {{subscriber.firstName}}"
    );
    assert_eq!(
        call(
            app.clone(),
            "PUT",
            path,
            "a-key",
            json!({"content":"Hello {{subscriber.firstName}}","active":false})
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        call(
            app.clone(),
            "POST",
            "/v1/events/trigger",
            "a-key",
            json!({"name":"workflow","to":"alice","transactionId":"t-2","payload":{}})
        )
        .await
        .0,
        StatusCode::CREATED
    );
    assert!(
        novu_postgres_queue::in_app::process_one(&mut setup)
            .await
            .unwrap()
    );
    assert_eq!(
        setup
            .query_one(
                "SELECT count(*) FROM public.novu_queue_jobs WHERE environment_id='a' AND kind=$1",
                &[&novu_postgres_queue::slack_workflow::FANOUT_KIND]
            )
            .await
            .unwrap()
            .get::<_, i64>(0),
        1
    );
    assert!(
        novu_postgres_queue::slack_workflow::delivery_one_with_policy(
            &mut setup,
            "a",
            &novu_postgres_queue::slack_workflow::Providers::new(),
            novu_postgres_queue::slack_workflow::StepPolicy::Active,
        )
        .await
        .unwrap()
    );
    assert_eq!(setup.query_one("SELECT last_error FROM public.novu_queue_jobs WHERE environment_id='a' AND kind=$1", &[&novu_postgres_queue::slack_workflow::DELIVERY_KIND]).await.unwrap().get::<_,Option<String>>(0).as_deref(), Some("Slack workflow step disabled"));
    // Snapshot endpoint identity when the trigger is processed, then add a
    // destination before fanout. Only the original may produce a child.
    assert_eq!(
        call(
            app.clone(),
            "PUT",
            path,
            "a-key",
            json!({"content":"Hello {{subscriber.firstName}}","active":true})
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        call(
            app.clone(),
            "POST",
            "/v1/events/trigger",
            "a-key",
            json!({"name":"workflow","to":"alice","transactionId":"t-3","payload":{}})
        )
        .await
        .0,
        StatusCode::CREATED
    );
    assert!(
        novu_postgres_queue::in_app::process_one_with_slack_snapshot(
            &mut setup,
            Duration::from_secs(60),
            true
        )
        .await
        .unwrap()
    );
    let snapshot_job: uuid::Uuid = setup
        .query_one(
            "SELECT id FROM public.novu_queue_jobs WHERE kind=$1 AND status='pending'",
            &[&novu_postgres_queue::slack_workflow::FANOUT_KIND],
        )
        .await
        .unwrap()
        .get(0);
    setup.batch_execute("INSERT INTO public.novu_rust_slack_channel_endpoints(environment_id,identifier,subscriber_id,subscriber_internal_id,integration_identifier,endpoint_type,endpoint) VALUES ('a','added','alice','alice-id','main','slack_channel','{\"channelId\":\"C999\"}')").await.unwrap();
    assert!(
        novu_postgres_queue::slack_workflow::fanout_one_with_policies(
            &mut setup,
            "a",
            novu_postgres_queue::slack_workflow::StepPolicy::Active,
            novu_postgres_queue::slack_workflow::EndpointPolicy::Snapshot
        )
        .await
        .unwrap()
    );
    assert_eq!(
        setup
            .query_one(
                "SELECT status,last_error FROM public.novu_queue_jobs WHERE id=$1",
                &[&snapshot_job]
            )
            .await
            .unwrap()
            .get::<_, String>(1),
        "Slack endpoint snapshot changed"
    );
    assert_eq!(setup.query_one("SELECT count(*) FROM public.novu_queue_jobs WHERE kind=$1 AND payload->>'target'='C999'", &[&novu_postgres_queue::slack_workflow::DELIVERY_KIND]).await.unwrap().get::<_,i64>(0),0);
    let child: Value = setup
        .query_one(
            "SELECT payload FROM public.novu_queue_jobs WHERE kind=$1 AND status='pending'",
            &[&novu_postgres_queue::slack_workflow::DELIVERY_KIND],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(child["target"], "C123");
    assert!(child["context"].get("endpoint_snapshot").is_none());
    // Reusing an endpoint identifier (even with the same target) changes its
    // internal identity, so the old snapshot cannot authorize the new row.
    assert_eq!(
        call(
            app.clone(),
            "POST",
            "/v1/events/trigger",
            "a-key",
            json!({"name":"workflow","to":"alice","transactionId":"t-4","payload":{}})
        )
        .await
        .0,
        StatusCode::CREATED
    );
    assert!(
        novu_postgres_queue::in_app::process_one_with_slack_snapshot(
            &mut setup,
            Duration::from_secs(60),
            true
        )
        .await
        .unwrap()
    );
    let recreated_job: uuid::Uuid = setup
        .query_one(
            "SELECT id FROM public.novu_queue_jobs WHERE kind=$1 AND status='pending'",
            &[&novu_postgres_queue::slack_workflow::FANOUT_KIND],
        )
        .await
        .unwrap()
        .get(0);
    setup.batch_execute("DELETE FROM public.novu_rust_slack_channel_endpoints WHERE environment_id='a'; INSERT INTO public.novu_rust_slack_channel_endpoints(environment_id,identifier,subscriber_id,subscriber_internal_id,integration_identifier,endpoint_type,endpoint) VALUES ('a','endpoint','alice','alice-id','main','slack_channel','{\"channelId\":\"C123\"}')").await.unwrap();
    assert!(
        novu_postgres_queue::slack_workflow::fanout_one_with_policies(
            &mut setup,
            "a",
            novu_postgres_queue::slack_workflow::StepPolicy::Active,
            novu_postgres_queue::slack_workflow::EndpointPolicy::Snapshot
        )
        .await
        .unwrap()
    );
    assert_eq!(
        setup
            .query_one(
                "SELECT last_error FROM public.novu_queue_jobs WHERE id=$1",
                &[&recreated_job]
            )
            .await
            .unwrap()
            .get::<_, String>(0),
        "Slack endpoint snapshot changed"
    );
    assert_eq!(
        setup
            .query_one(
                "SELECT count(*) FROM public.novu_queue_jobs WHERE kind=$1",
                &[&novu_postgres_queue::slack_workflow::DELIVERY_KIND]
            )
            .await
            .unwrap()
            .get::<_, i64>(0),
        2
    );
    assert!(
        novu_postgres_queue::slack_queue::health(&setup, "a")
            .await
            .unwrap()["preDispatchFailures"]
            .as_i64()
            .unwrap()
            >= 2
    );
    // A legacy fanout without a snapshot is rejected in strict mode.
    assert_eq!(
        call(
            app.clone(),
            "POST",
            "/v1/events/trigger",
            "a-key",
            json!({"name":"workflow","to":"alice","transactionId":"t-5","payload":{}})
        )
        .await
        .0,
        StatusCode::CREATED
    );
    assert!(
        novu_postgres_queue::in_app::process_one(&mut setup)
            .await
            .unwrap()
    );
    let legacy: uuid::Uuid = setup
        .query_one(
            "SELECT id FROM public.novu_queue_jobs WHERE kind=$1 AND status='pending'",
            &[&novu_postgres_queue::slack_workflow::FANOUT_KIND],
        )
        .await
        .unwrap()
        .get(0);
    assert!(
        novu_postgres_queue::slack_workflow::fanout_one_with_policies(
            &mut setup,
            "a",
            novu_postgres_queue::slack_workflow::StepPolicy::Active,
            novu_postgres_queue::slack_workflow::EndpointPolicy::Snapshot
        )
        .await
        .unwrap()
    );
    assert_eq!(
        setup
            .query_one(
                "SELECT last_error FROM public.novu_queue_jobs WHERE id=$1",
                &[&legacy]
            )
            .await
            .unwrap()
            .get::<_, String>(0),
        "Slack endpoint snapshot missing"
    );
    // A subscriber-serialized endpoint insertion committed before the trigger
    // worker obtains that subscriber lock is included in the snapshot.
    assert_eq!(
        call(
            app.clone(),
            "POST",
            "/v1/events/trigger",
            "a-key",
            json!({"name":"workflow","to":"alice","transactionId":"t-6","payload":{}})
        )
        .await
        .0,
        StatusCode::CREATED
    );
    let (mut writer, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let tx = writer.transaction().await.unwrap();
    tx.query_one("SELECT 1 FROM public.novu_rust_subscribers WHERE environment_id='a' AND subscriber_id='alice' FOR UPDATE",&[]).await.unwrap();
    tx.execute("INSERT INTO public.novu_rust_slack_channel_endpoints(environment_id,identifier,subscriber_id,subscriber_internal_id,integration_identifier,endpoint_type,endpoint) VALUES ('a','before-snapshot','alice','alice-id','main','slack_channel','{\"channelId\":\"C777\"}')",&[]).await.unwrap();
    let (mut snapshot_worker, driver) = tokio_postgres::connect(
        &format!("{url}?application_name=slack_snapshot_worker"),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let pending = tokio::spawn(async move {
        novu_postgres_queue::in_app::process_one_with_slack_snapshot(
            &mut snapshot_worker,
            Duration::from_secs(60),
            true,
        )
        .await
    });
    let mut waiting = false;
    for _ in 0..100 {
        if setup.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='slack_snapshot_worker' AND wait_event_type='Lock')",&[]).await.unwrap().get::<_,bool>(0){waiting=true;break;}
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(waiting && !pending.is_finished());
    tx.commit().await.unwrap();
    assert!(pending.await.unwrap().unwrap());
    let snapshot:Value=setup.query_one("SELECT payload->'endpoint_snapshot' FROM public.novu_queue_jobs WHERE kind=$1 AND status='pending'", &[&novu_postgres_queue::slack_workflow::FANOUT_KIND]).await.unwrap().get(0);
    assert_eq!(snapshot.as_array().unwrap().len(), 2);
    assert!(
        snapshot
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["endpoint"]["channelId"] == "C777")
    );
    // The snapshot bound rejects the trigger atomically instead of emitting a
    // partial destination set when an unexpectedly large fanout is encountered.
    setup.batch_execute("INSERT INTO public.novu_rust_slack_channel_endpoints(environment_id,identifier,subscriber_id,subscriber_internal_id,integration_identifier,endpoint_type,endpoint) SELECT 'a','mass-'||i,'alice','alice-id','main','slack_channel',jsonb_build_object('channelId','C'||i) FROM generate_series(1,999) i").await.unwrap();
    assert_eq!(
        call(
            app.clone(),
            "POST",
            "/v1/events/trigger",
            "a-key",
            json!({"name":"workflow","to":"alice","transactionId":"t-7","payload":{}})
        )
        .await
        .0,
        StatusCode::CREATED
    );
    assert!(
        novu_postgres_queue::in_app::process_one_with_slack_snapshot(
            &mut setup,
            Duration::from_secs(60),
            true
        )
        .await
        .unwrap()
    );
    let failed=setup.query_one("SELECT status,last_error FROM public.novu_queue_jobs WHERE kind=$1 AND payload->>'transactionId'='t-7'", &[&novu_postgres_queue::in_app::JOB_KIND]).await.unwrap();
    assert_eq!(failed.get::<_, String>(0), "failed");
    assert_eq!(
        failed.get::<_, String>(1),
        "too many Slack endpoints for snapshot"
    );
    assert_eq!(
        setup
            .query_one(
                "SELECT count(*) FROM public.novu_rust_notifications WHERE transaction_id='t-7'",
                &[]
            )
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );
    // Pause the HTTP write on the workflow row after it acquires the grant.
    let (mut holder, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let tx = holder.transaction().await.unwrap();
    tx.query_one("SELECT 1 FROM public.novu_rust_workflows WHERE environment_id='a' AND identifier='workflow' FOR UPDATE", &[]).await.unwrap();
    let running = tokio::spawn(call(
        app.clone(),
        "PUT",
        path,
        "a-key",
        json!({"content":null,"active":false}),
    ));
    let mut waiting = false;
    for _ in 0..100 {
        if setup.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='slack_step_api' AND wait_event_type='Lock')", &[]).await.unwrap().get::<_,bool>(0) { waiting=true; break; }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(waiting, "HTTP update did not wait on the workflow row");
    let (revoker, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let key = a.clone();
    let revoke = tokio::spawn(async move {
        revoker.execute("DELETE FROM public.novu_rust_slack_workflow_permissions WHERE key_hash=$1 AND permission='write'", &[&key]).await.unwrap()
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !revoke.is_finished(),
        "revocation overtook the in-flight HTTP update"
    );
    tx.commit().await.unwrap();
    assert_eq!(running.await.unwrap().0, StatusCode::OK);
    assert_eq!(revoke.await.unwrap(), 1);
    assert_eq!(
        call(
            app.clone(),
            "PUT",
            path,
            "a-key",
            json!({"content":null,"active":false})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            app.clone(),
            "PUT",
            path,
            "b-key",
            json!({"content":null,"active":false})
        )
        .await
        .0,
        StatusCode::OK
    );
}
