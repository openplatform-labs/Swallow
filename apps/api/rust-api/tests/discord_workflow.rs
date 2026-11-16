//! API trigger through workflow worker and Discord queue, with no external sends.
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use std::{path::Path, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use tokio_postgres::{Client, NoTls};
use tower::ServiceExt;

async fn migrate(db: &Client, dir: &Path, range: std::ops::RangeInclusive<u32>) {
    let mut files: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|v| v.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "sql"))
        .collect();
    files.sort();
    for file in files {
        let name = file.file_name().unwrap().to_str().unwrap();
        if range.contains(&name[..4].parse::<u32>().unwrap()) {
            db.batch_execute(&std::fs::read_to_string(&file).unwrap())
                .await
                .unwrap_or_else(|e| panic!("{name}: {e}"));
        }
    }
}
async fn trigger(app: axum::Router, id: &str) {
    let response=app.oneshot(Request::builder().method("POST").uri("/v1/events/trigger").header("authorization","ApiKey discord-key").header("content-type","application/json").body(Body::from(json!({"name":"discord-workflow","to":"alice","transactionId":id,"payload":{"name":"Hello"}}).to_string())).unwrap()).await.unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(
        status,
        StatusCode::CREATED,
        "{}",
        String::from_utf8_lossy(&body)
    );
}
async fn produce(db: &mut Client, app: axum::Router, id: &str) {
    trigger(app, id).await;
    assert!(
        novu_postgres_queue::in_app::process_one_with_chat_options(
            db,
            Duration::from_secs(60),
            false,
            true
        )
        .await
        .unwrap()
    );
}
#[tokio::test]
#[ignore = "requires a disposable PostgreSQL database"]
async fn trigger_renders_and_guards_subscriber_endpoint_and_preferences() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(driver);
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    migrate(&db, &root.join("migrations"), 1..=17).await;
    migrate(
        &db,
        &root.join("../../worker/rust-queue/migrations"),
        1..=u32::MAX,
    )
    .await;
    migrate(&db, &root.join("migrations"), 18..=u32::MAX).await;
    db.batch_execute("INSERT INTO public.novu_rust_users VALUES ('u'); INSERT INTO public.novu_rust_environments VALUES ('a','org'); INSERT INTO public.novu_rust_api_keys(key_hash,environment_id,user_id) VALUES (encode(sha256(convert_to('discord-key','UTF8')),'hex'),'a','u'); INSERT INTO public.novu_rust_workflows(environment_id,identifier,active,has_steps,in_app_step_active,chat_content,inbox_metadata) VALUES ('a','discord-workflow',true,true,false,'{{name}} {{subscriber.firstName}}','{\"id\":\"wf-id\"}'); INSERT INTO public.novu_rust_subscribers(environment_id,subscriber_id,internal_id,profile) VALUES ('a','alice','old','{\"firstName\":\"Jane\"}'); INSERT INTO public.novu_rust_discord_destinations(environment_id,destination_id,endpoint_hash) VALUES('a','dest','fixture'); INSERT INTO public.novu_rust_discord_endpoint_permissions(key_hash,permission) VALUES (encode(sha256(convert_to('discord-key','UTF8')),'hex'),'write')").await.unwrap();
    let app = novu_rust_api::router_with_subscriber_secret(Arc::new(db), None);
    let registration = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/experimental/discord-endpoints")
                .header("authorization", "ApiKey discord-key")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"identifier":"ep","subscriberId":"alice","destinationId":"dest"})
                        .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let registration_status = registration.status();
    let registration_body = registration.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(
        registration_status,
        StatusCode::CREATED,
        "{}",
        String::from_utf8_lossy(&registration_body)
    );
    let (mut db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(driver);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!(
        "http://{}/api/webhooks/1/token",
        listener.local_addr().unwrap()
    );
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut data = Vec::new();
        let mut buf = [0; 4096];
        loop {
            let n = socket.read(&mut buf).await.unwrap();
            assert!(n > 0);
            data.extend_from_slice(&buf[..n]);
            if let Some(pos) = data.windows(4).position(|v| v == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&data[..pos]);
                let len: usize = headers
                    .lines()
                    .find_map(|l| {
                        l.to_ascii_lowercase()
                            .strip_prefix("content-length: ")
                            .and_then(|v| v.parse().ok())
                    })
                    .unwrap();
                if data.len() >= pos + 4 + len {
                    break;
                }
            }
        }
        let body = r#"{"id":"1","timestamp":"2026-01-01Z"}"#;
        socket
            .write_all(
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        let pos = data.windows(4).position(|v| v == b"\r\n\r\n").unwrap();
        serde_json::from_slice::<Value>(&data[pos + 4..]).unwrap()
    });
    let provider = novu_postgres_queue::discord::Webhook::test_endpoint(&endpoint).unwrap();
    trigger(app.clone(), "disabled").await;
    assert!(
        novu_postgres_queue::in_app::process_one_with_chat_options(
            &mut db,
            Duration::from_secs(60),
            false,
            true
        )
        .await
        .unwrap()
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM public.novu_queue_jobs WHERE kind=$1",
            &[&novu_postgres_queue::discord_queue::WORKFLOW_KIND]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    db.batch_execute(
        "UPDATE public.novu_rust_discord_workflow_policy SET enabled=true WHERE environment_id='a'",
    )
    .await
    .unwrap();
    let invalid = uuid::Uuid::new_v4();
    novu_postgres_queue::enqueue(
        &db,
        invalid,
        "a",
        novu_postgres_queue::in_app::JOB_KIND,
        &json!({"recipientIdentitySnapshot":{}}),
    )
    .await
    .unwrap();
    assert!(
        novu_postgres_queue::in_app::process_one_with_chat_options(
            &mut db,
            Duration::from_secs(60),
            false,
            true
        )
        .await
        .unwrap()
    );
    assert_eq!(
        db.query_one(
            "SELECT status FROM public.novu_queue_jobs WHERE id=$1",
            &[&invalid]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "failed"
    );
    trigger(app.clone(), "legacy-worker").await;
    // A disabled or pre-fence worker may claim, but its transaction cannot
    // commit the parent or its domain writes while Discord is enabled.
    assert!(
        novu_postgres_queue::in_app::process_one_with_chat_options(
            &mut db,
            Duration::from_secs(60),
            false,
            false
        )
        .await
        .is_err()
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM public.novu_queue_jobs WHERE kind=$1",
            &[&novu_postgres_queue::discord_queue::WORKFLOW_KIND]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    db.batch_execute("UPDATE public.novu_queue_jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE kind='experimental_static_in_app' AND status='running'").await.unwrap();
    assert_eq!(
        novu_postgres_queue::requeue_expired(&db, novu_postgres_queue::in_app::JOB_KIND)
            .await
            .unwrap(),
        1
    );
    assert!(
        novu_postgres_queue::in_app::process_one_with_chat_options(
            &mut db,
            Duration::from_secs(60),
            false,
            true
        )
        .await
        .unwrap()
    );

    assert!(
        novu_postgres_queue::discord_queue::process_one(&mut db, "a", "dest", &provider)
            .await
            .unwrap()
    );
    assert_eq!(server.await.unwrap()["content"], "Hello Jane");
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM public.novu_rust_discord_attempts WHERE state='accepted'",
            &[]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        1
    );
    // A workflow default change that has locked the parent before reservation
    // must be visible to the sender after the change commits, including insertion
    // of a preference row that did not previously exist.
    produce(&mut db, app.clone(), "preference-race").await;
    let (mut changer, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(driver);
    let tx=changer.transaction().await.unwrap();
    tx.execute("INSERT INTO public.novu_rust_workflow_preferences(environment_id,workflow_identifier,chat_enabled) VALUES('a','discord-workflow',false)",&[]).await.unwrap();
    let (mut sender,driver)=tokio_postgres::connect(&url,NoTls).await.unwrap();tokio::spawn(driver);
    sender.batch_execute("SET application_name='discord_preference_fence'").await.unwrap();
    let sender_endpoint=endpoint.clone();
    let task=tokio::spawn(async move {
        let provider=novu_postgres_queue::discord::Webhook::test_endpoint(&sender_endpoint).unwrap();
        novu_postgres_queue::discord_queue::process_one(&mut sender,"a","dest",&provider).await
    });
    let mut waiting=false;
    for _ in 0..150 {
        waiting=db.query_one("SELECT count(*)=1 FROM pg_stat_activity WHERE application_name='discord_preference_fence' AND wait_event_type='Lock'",&[]).await.unwrap().get(0);
        if waiting {break;}
        assert!(!task.is_finished(),"sender finished before preference commit");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(waiting,"sender did not wait for the preference writer");
    tx.commit().await.unwrap();
    assert!(tokio::time::timeout(Duration::from_secs(10),task).await.unwrap().unwrap().unwrap());
    assert_eq!(db.query_one("SELECT count(*) FROM public.novu_queue_jobs WHERE kind=$1 AND status='cancelled' AND last_error='chat preference disabled'",&[&novu_postgres_queue::discord_queue::WORKFLOW_KIND]).await.unwrap().get::<_,i64>(0),1);
    assert_eq!(db.query_one("SELECT count(*) FROM public.novu_rust_discord_attempts",&[]).await.unwrap().get::<_,i64>(0),1);
    db.batch_execute("DELETE FROM public.novu_rust_workflow_preferences WHERE environment_id='a' AND workflow_identifier='discord-workflow'").await.unwrap();
    // Reservation first holds the workflow lock until the dispatch attempt is
    // committed; a later preference change waits but cannot retract that send.
    db.batch_execute("INSERT INTO public.novu_rust_workflow_preferences(environment_id,workflow_identifier,chat_enabled) VALUES('a','discord-workflow',true); CREATE FUNCTION public.test_discord_reservation_pause() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock(766481); RETURN NEW; END $$; CREATE TRIGGER test_discord_reservation_pause BEFORE INSERT ON public.novu_rust_discord_attempts FOR EACH ROW EXECUTE FUNCTION public.test_discord_reservation_pause()").await.unwrap();
    produce(&mut db,app.clone(),"reservation-first").await;
    let (mut blocker,driver)=tokio_postgres::connect(&url,NoTls).await.unwrap();tokio::spawn(driver);let blocker_tx=blocker.transaction().await.unwrap();blocker_tx.query_one("SELECT pg_advisory_xact_lock(766481)",&[]).await.unwrap();
    let listener=TcpListener::bind("127.0.0.1:0").await.unwrap();let endpoint2=format!("http://{}/api/webhooks/1/token",listener.local_addr().unwrap());
    let server2=tokio::spawn(async move {let (mut socket,_)=listener.accept().await.unwrap();let mut bytes=[0u8;4096];assert!(socket.read(&mut bytes).await.unwrap()>0);let body=r#"{"id":"2","timestamp":"2026-01-01Z"}"#;socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();});
    let (mut sender,driver)=tokio_postgres::connect(&url,NoTls).await.unwrap();tokio::spawn(driver);sender.batch_execute("SET application_name='discord_reservation_first'").await.unwrap();
    let send=tokio::spawn(async move {let provider=novu_postgres_queue::discord::Webhook::test_endpoint(&endpoint2).unwrap();novu_postgres_queue::discord_queue::process_one(&mut sender,"a","dest",&provider).await});
    let mut paused=false;for _ in 0..150 {paused=db.query_one("SELECT count(*)=1 FROM pg_stat_activity WHERE application_name='discord_reservation_first' AND wait_event='advisory'",&[]).await.unwrap().get(0);if paused {break;}assert!(!send.is_finished());tokio::time::sleep(Duration::from_millis(100)).await;}assert!(paused,"sender did not reach attempt reservation");
    let (writer,driver)=tokio_postgres::connect(&url,NoTls).await.unwrap();tokio::spawn(driver);writer.batch_execute("SET application_name='discord_preference_writer'").await.unwrap();
    let change=tokio::spawn(async move {writer.execute("UPDATE public.novu_rust_workflow_preferences SET chat_enabled=false WHERE environment_id='a' AND workflow_identifier='discord-workflow'",&[]).await});
    let mut writer_waiting=false;for _ in 0..150 {writer_waiting=db.query_one("SELECT count(*)=1 FROM pg_stat_activity WHERE application_name='discord_preference_writer' AND wait_event_type='Lock'",&[]).await.unwrap().get(0);if writer_waiting {break;}assert!(!change.is_finished());tokio::time::sleep(Duration::from_millis(100)).await;}assert!(writer_waiting,"preference writer did not wait for reservation");
    blocker_tx.commit().await.unwrap();assert!(tokio::time::timeout(Duration::from_secs(10),send).await.unwrap().unwrap().unwrap());tokio::time::timeout(Duration::from_secs(10),change).await.unwrap().unwrap().unwrap();tokio::time::timeout(Duration::from_secs(10),server2).await.unwrap().unwrap();
    assert_eq!(db.query_one("SELECT count(*) FROM public.novu_rust_discord_attempts WHERE state='accepted'",&[]).await.unwrap().get::<_,i64>(0),2);
    db.batch_execute("DROP TRIGGER test_discord_reservation_pause ON public.novu_rust_discord_attempts; DROP FUNCTION public.test_discord_reservation_pause(); DELETE FROM public.novu_rust_workflow_preferences WHERE environment_id='a' AND workflow_identifier='discord-workflow'").await.unwrap();
    produce(&mut db, app.clone(), "revision").await;
    db.batch_execute("UPDATE public.novu_rust_discord_workflow_endpoints SET context_keys=ARRAY['x:1'] WHERE identifier='ep'; UPDATE public.novu_rust_discord_workflow_endpoints SET context_keys=ARRAY[]::text[] WHERE identifier='ep'").await.unwrap();
    assert!(
        novu_postgres_queue::discord_queue::process_one(&mut db, "a", "dest", &provider)
            .await
            .unwrap()
    );
    assert_eq!(db.query_one("SELECT count(*) FROM public.novu_queue_jobs WHERE kind=$1 AND last_error='Discord endpoint changed or deleted'",&[&novu_postgres_queue::discord_queue::WORKFLOW_KIND]).await.unwrap().get::<_,i64>(0),1);
    produce(&mut db, app.clone(), "preference").await;
    db.batch_execute("INSERT INTO public.novu_rust_subscriber_preferences(environment_id,subscriber_id,channels) VALUES('a','alice','{\"chat\":false}')").await.unwrap();
    assert!(
        novu_postgres_queue::discord_queue::process_one(&mut db, "a", "dest", &provider)
            .await
            .unwrap()
    );
    assert_eq!(db.query_one("SELECT count(*) FROM public.novu_queue_jobs WHERE kind=$1 AND status='cancelled' AND last_error='chat preference disabled'",&[&novu_postgres_queue::discord_queue::WORKFLOW_KIND]).await.unwrap().get::<_,i64>(0),2);
    db.batch_execute("DELETE FROM public.novu_rust_subscriber_preferences WHERE environment_id='a' AND subscriber_id='alice'").await.unwrap();
    produce(&mut db, app.clone(), "workflow-change").await;
    db.batch_execute("UPDATE public.novu_rust_workflows SET chat_content='different' WHERE environment_id='a' AND identifier='discord-workflow'").await.unwrap();
    assert!(
        novu_postgres_queue::discord_queue::process_one(&mut db, "a", "dest", &provider)
            .await
            .unwrap()
    );
    assert_eq!(db.query_one("SELECT count(*) FROM public.novu_queue_jobs WHERE kind=$1 AND last_error='Discord workflow step changed or disabled'", &[&novu_postgres_queue::discord_queue::WORKFLOW_KIND]).await.unwrap().get::<_,i64>(0),1);
    db.batch_execute("UPDATE public.novu_rust_workflows SET chat_content='{{name}} {{subscriber.firstName}}' WHERE environment_id='a' AND identifier='discord-workflow'").await.unwrap();
    produce(&mut db, app, "identity").await;
    db.batch_execute("DELETE FROM public.novu_rust_subscribers WHERE environment_id='a' AND subscriber_id='alice'; INSERT INTO public.novu_rust_subscribers(environment_id,subscriber_id,internal_id,profile) VALUES ('a','alice','new','{}')").await.unwrap();
    assert!(
        novu_postgres_queue::discord_queue::process_one(&mut db, "a", "dest", &provider)
            .await
            .unwrap()
    );
    assert_eq!(db.query_one("SELECT count(*) FROM public.novu_queue_jobs WHERE kind=$1 AND last_error='subscriber identity changed'",&[&novu_postgres_queue::discord_queue::WORKFLOW_KIND]).await.unwrap().get::<_,i64>(0),1);
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM public.novu_rust_discord_attempts",
            &[]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        2
    );
}
