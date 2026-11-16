//! API trigger through workflow worker and Telegram queue, with no external sends.
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
    let response=app.oneshot(Request::builder().method("POST").uri("/v1/events/trigger").header("authorization","ApiKey telegram-key").header("content-type","application/json").body(Body::from(json!({"name":"telegram-workflow","to":"alice","transactionId":id,"payload":{"name":"Hello"}}).to_string())).unwrap()).await.unwrap();
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
        novu_postgres_queue::in_app::process_one_with_channel_options(
            db,
            Duration::from_secs(60),
            false,
            false,
            true
        )
        .await
        .unwrap()
    );
}
#[tokio::test]
#[ignore = "requires a disposable PostgreSQL database"]
async fn scoped_worker_renders_and_guards_subscriber_endpoint_and_preferences() {
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
    db.batch_execute("INSERT INTO public.novu_rust_users VALUES ('u'); INSERT INTO public.novu_rust_environments VALUES ('a','org'); INSERT INTO public.novu_rust_api_keys(key_hash,environment_id,user_id) VALUES (encode(sha256(convert_to('telegram-key','UTF8')),'hex'),'a','u'); INSERT INTO public.novu_rust_workflows(environment_id,identifier,active,has_steps,in_app_step_active,chat_content,inbox_metadata) VALUES ('a','telegram-workflow',true,true,false,'{{name}} {{subscriber.firstName}}','{\"id\":\"wf-id\"}'); INSERT INTO public.novu_rust_subscribers(environment_id,subscriber_id,internal_id,profile) VALUES ('a','alice','old','{\"firstName\":\"Jane\"}'); INSERT INTO public.novu_rust_telegram_destinations(environment_id,destination_id,token_hash) VALUES('a','dest','fixture'); INSERT INTO public.novu_rust_telegram_endpoint_permissions(key_hash,permission) VALUES (encode(sha256(convert_to('telegram-key','UTF8')),'hex'),'write')").await.unwrap();
    let app = novu_rust_api::router_with_subscriber_secret(Arc::new(db), None);
    let registration = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/experimental/telegram-endpoints")
                .header("authorization", "ApiKey telegram-key")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"identifier":"ep","subscriberId":"alice","destinationId":"dest","chatId":"-123"})
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
    db.batch_execute("CREATE ROLE telegram_workflow_worker LOGIN; GRANT USAGE ON SCHEMA public TO telegram_workflow_worker; GRANT EXECUTE ON FUNCTION public.novu_rust_telegram_workflow_claim(text,text,uuid),public.novu_rust_telegram_workflow_snapshot(text,text,uuid,uuid,boolean),public.novu_rust_telegram_workflow_prepare(text,text,uuid,uuid,text,boolean),public.novu_rust_telegram_worker_prepare(text,text,uuid,uuid,boolean),public.novu_rust_telegram_worker_finish(text,text,uuid,uuid,text,integer,text,text),public.novu_rust_telegram_workflow_recover(text,text) TO telegram_workflow_worker; INSERT INTO public.novu_rust_telegram_worker_grants SELECT 'a','dest',rolname,oid FROM pg_roles WHERE rolname='telegram_workflow_worker'").await.unwrap();
    let (mut scoped, driver) = tokio_postgres::connect(
        &url.replace(
            "postgres://postgres@",
            "postgres://telegram_workflow_worker@",
        ),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(driver);
    assert!(
        scoped
            .query("SELECT * FROM public.novu_queue_jobs", &[])
            .await
            .is_err()
    );
    assert!(
        scoped
            .query("SELECT * FROM public.novu_rust_subscribers", &[])
            .await
            .is_err()
    );
    assert!(
        scoped
            .query(
                "SELECT * FROM public.novu_rust_telegram_workflow_claim($1,$2,$3)",
                &[&"a", &"other", &uuid::Uuid::new_v4()]
            )
            .await
            .is_err()
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/sendMessage", listener.local_addr().unwrap());
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
        let body = r#"{"ok":true,"result":{"message_id":1,"date":1700000000}}"#;
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
    let provider = novu_postgres_queue::telegram::Telegram::test_endpoint(&endpoint).unwrap();
    trigger(app.clone(), "disabled").await;
    assert!(
        novu_postgres_queue::in_app::process_one_with_channel_options(
            &mut db,
            Duration::from_secs(60),
            false,
            false,
            true
        )
        .await
        .unwrap()
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM public.novu_queue_jobs WHERE kind=$1",
            &[&novu_postgres_queue::telegram_queue::WORKFLOW_KIND]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    db.batch_execute(
        "UPDATE public.novu_rust_telegram_workflow_policy SET enabled=true WHERE environment_id='a'",
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
        novu_postgres_queue::in_app::process_one_with_channel_options(
            &mut db,
            Duration::from_secs(60),
            false,
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
    // commit the parent or its domain writes while Telegram is enabled.
    assert!(
        novu_postgres_queue::in_app::process_one_with_channel_options(
            &mut db,
            Duration::from_secs(60),
            false,
            false,
            false
        )
        .await
        .is_err()
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM public.novu_queue_jobs WHERE kind=$1",
            &[&novu_postgres_queue::telegram_queue::WORKFLOW_KIND]
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
        novu_postgres_queue::in_app::process_one_with_channel_options(
            &mut db,
            Duration::from_secs(60),
            false,
            false,
            true
        )
        .await
        .unwrap()
    );

    assert!(
        novu_postgres_queue::telegram_queue::process_one_with_workflow(
            &mut scoped,
            "a",
            "dest",
            &provider,
            true,
            true
        )
        .await
        .unwrap()
    );
    let sent = server.await.unwrap();
    assert_eq!(sent["text"], "Hello Jane");
    assert_eq!(sent["chat_id"], "-123");
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM public.novu_rust_telegram_attempts WHERE state='accepted'",
            &[]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        1
    );
    // Preparing directly without first calling snapshot must still validate the endpoint.
    produce(&mut db, app.clone(), "prepare-check").await;
    let lease = uuid::Uuid::new_v4();
    let claimed = scoped
        .query_one(
            "SELECT id FROM public.novu_rust_telegram_workflow_claim($1,$2,$3)",
            &[&"a", &"dest", &lease],
        )
        .await
        .unwrap();
    let claimed_id: uuid::Uuid = claimed.get(0);
    db.batch_execute("UPDATE public.novu_rust_telegram_workflow_endpoints SET chat_id='-124' WHERE identifier='ep'").await.unwrap();
    let prepared: bool = scoped
        .query_one(
            "SELECT public.novu_rust_telegram_workflow_prepare($1,$2,$3,$4,$5,$6)",
            &[
                &"a",
                &"dest",
                &claimed_id,
                &lease,
                &Option::<String>::None,
                &false,
            ],
        )
        .await
        .unwrap()
        .get(0);
    assert!(!prepared);
    assert_eq!(
        db.query_one(
            "SELECT last_error FROM public.novu_queue_jobs WHERE id=$1",
            &[&claimed_id]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "Telegram endpoint changed or deleted"
    );
    produce(&mut db, app.clone(), "revision").await;
    db.batch_execute("UPDATE public.novu_rust_telegram_workflow_endpoints SET context_keys=ARRAY['x:1'] WHERE identifier='ep'; UPDATE public.novu_rust_telegram_workflow_endpoints SET context_keys=ARRAY[]::text[] WHERE identifier='ep'").await.unwrap();
    assert!(
        novu_postgres_queue::telegram_queue::process_one_with_workflow(
            &mut scoped,
            "a",
            "dest",
            &provider,
            true,
            true
        )
        .await
        .unwrap()
    );
    assert_eq!(db.query_one("SELECT count(*) FROM public.novu_queue_jobs WHERE kind=$1 AND last_error='Telegram endpoint changed or deleted'",&[&novu_postgres_queue::telegram_queue::WORKFLOW_KIND]).await.unwrap().get::<_,i64>(0),2);
    produce(&mut db, app.clone(), "preference").await;
    db.batch_execute("INSERT INTO public.novu_rust_subscriber_preferences(environment_id,subscriber_id,channels) VALUES('a','alice','{\"chat\":false}')").await.unwrap();
    assert!(
        novu_postgres_queue::telegram_queue::process_one_with_workflow(
            &mut scoped,
            "a",
            "dest",
            &provider,
            true,
            true
        )
        .await
        .unwrap()
    );
    assert_eq!(db.query_one("SELECT count(*) FROM public.novu_queue_jobs WHERE kind=$1 AND status='cancelled' AND last_error='chat preference disabled'",&[&novu_postgres_queue::telegram_queue::WORKFLOW_KIND]).await.unwrap().get::<_,i64>(0),1);
    db.batch_execute("DELETE FROM public.novu_rust_subscriber_preferences WHERE environment_id='a' AND subscriber_id='alice'").await.unwrap();
    // A default preference insertion holds the workflow lock while the sender
    // has REPEATABLE READ configured as its connection default.
    produce(&mut db, app.clone(), "default-preference-race").await;
    let (mut changer,driver)=tokio_postgres::connect(&url,NoTls).await.unwrap();tokio::spawn(driver);
    let pref_tx=changer.transaction().await.unwrap();
    pref_tx.execute("INSERT INTO public.novu_rust_workflow_preferences(environment_id,workflow_identifier,chat_enabled) VALUES('a','telegram-workflow',false)",&[]).await.unwrap();
    let writer_pid:i32=pref_tx.query_one("SELECT pg_backend_pid()",&[]).await.unwrap().get(0);
    let listener=TcpListener::bind("127.0.0.1:0").await.unwrap();
    let test_endpoint=format!("http://{}/sendMessage",listener.local_addr().unwrap());
    let worker_url=url.replace("postgres://postgres@","postgres://telegram_workflow_worker@");
    let (mut sender,driver)=tokio_postgres::connect(&worker_url,NoTls).await.unwrap();tokio::spawn(driver);
    sender.batch_execute("SET default_transaction_isolation='repeatable read'").await.unwrap();
    let sender_pid:i32=sender.query_one("SELECT pg_backend_pid()",&[]).await.unwrap().get(0);
    let attempts:i64=db.query_one("SELECT count(*) FROM public.novu_rust_telegram_attempts",&[]).await.unwrap().get(0);
    let task=tokio::spawn(async move {let provider=novu_postgres_queue::telegram::Telegram::test_endpoint(&test_endpoint).unwrap();novu_postgres_queue::telegram_queue::process_one_with_workflow(&mut sender,"a","dest",&provider,true,true).await});
    let mut waiting=false;for _ in 0..150 {waiting=pref_tx.query_one("SELECT $1=ANY(pg_blocking_pids($2))",&[&writer_pid,&sender_pid]).await.unwrap().get(0);if waiting {break;}assert!(!task.is_finished());tokio::time::sleep(Duration::from_millis(100)).await;}assert!(waiting,"Telegram sender did not wait for preference writer");
    pref_tx.commit().await.unwrap();
    assert!(tokio::time::timeout(Duration::from_secs(10),task).await.unwrap().unwrap().unwrap());
    assert_eq!(db.query_one("SELECT count(*) FROM public.novu_queue_jobs WHERE kind=$1 AND status='cancelled' AND last_error='chat preference disabled'",&[&novu_postgres_queue::telegram_queue::WORKFLOW_KIND]).await.unwrap().get::<_,i64>(0),2);
    assert_eq!(db.query_one("SELECT count(*) FROM public.novu_rust_telegram_attempts",&[]).await.unwrap().get::<_,i64>(0),attempts);
    assert!(tokio::time::timeout(Duration::from_millis(200),listener.accept()).await.is_err());
    db.batch_execute("DELETE FROM public.novu_rust_workflow_preferences WHERE environment_id='a' AND workflow_identifier='telegram-workflow'").await.unwrap();
    produce(&mut db, app.clone(), "workflow-change").await;
    db.batch_execute("UPDATE public.novu_rust_workflows SET chat_content='different' WHERE environment_id='a' AND identifier='telegram-workflow'").await.unwrap();
    assert!(
        novu_postgres_queue::telegram_queue::process_one_with_workflow(
            &mut scoped,
            "a",
            "dest",
            &provider,
            true,
            true
        )
        .await
        .unwrap()
    );
    assert_eq!(db.query_one("SELECT count(*) FROM public.novu_queue_jobs WHERE kind=$1 AND last_error='Telegram workflow step changed or disabled'", &[&novu_postgres_queue::telegram_queue::WORKFLOW_KIND]).await.unwrap().get::<_,i64>(0),1);
    db.batch_execute("UPDATE public.novu_rust_workflows SET chat_content='{{name}} {{subscriber.firstName}}' WHERE environment_id='a' AND identifier='telegram-workflow'").await.unwrap();
    // A real ambiguous HTTP response is visible to a separately granted operator.
    produce(&mut db, app.clone(), "operator-retry").await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/sendMessage", listener.local_addr().unwrap());
    let ambiguous = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buf = [0; 4096];
        assert!(socket.read(&mut buf).await.unwrap() > 0);
        socket
            .write_all(b"HTTP/1.1 409 Conflict\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
    });
    let failure_provider =
        novu_postgres_queue::telegram::Telegram::test_endpoint(&endpoint).unwrap();
    assert!(
        novu_postgres_queue::telegram_queue::process_one_with_workflow(
            &mut db,
            "a",
            "dest",
            &failure_provider,
            false,
            true
        )
        .await
        .unwrap()
    );
    ambiguous.await.unwrap();
    let row = db.query_one("SELECT j.id,a.attempt_id FROM public.novu_queue_jobs j JOIN public.novu_rust_telegram_attempts a ON a.job_id=j.id WHERE j.kind='experimental_telegram_workflow' AND a.state='unknown'", &[]).await.unwrap();
    let job: uuid::Uuid = row.get(0);
    let attempt: uuid::Uuid = row.get(1);
    db.batch_execute("CREATE ROLE telegram_workflow_ops LOGIN; CREATE ROLE telegram_workflow_denied LOGIN; GRANT USAGE ON SCHEMA public TO telegram_workflow_ops,telegram_workflow_denied; GRANT EXECUTE ON FUNCTION public.novu_rust_telegram_attention(text,uuid),public.novu_rust_telegram_resolve(text,uuid,uuid,uuid,text,text) TO telegram_workflow_ops,telegram_workflow_denied; INSERT INTO public.novu_rust_telegram_operator_grants SELECT 'a',rolname,oid,true,true,true FROM pg_roles WHERE rolname='telegram_workflow_ops'").await.unwrap();
    let (ops, driver) = tokio_postgres::connect(
        &url.replace("postgres://postgres@", "postgres://telegram_workflow_ops@"),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(driver);
    let (denied, driver) = tokio_postgres::connect(
        &url.replace(
            "postgres://postgres@",
            "postgres://telegram_workflow_denied@",
        ),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(driver);
    let list: Value = ops
        .query_one(
            "SELECT public.novu_rust_telegram_attention($1,$2)",
            &[&"a", &Option::<uuid::Uuid>::None],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(list["data"][0]["jobId"], job.to_string());
    assert_eq!(list["data"][0]["jobKind"], "experimental_telegram_workflow");
    assert!(!list.to_string().contains("-123"));
    assert!(
        denied
            .query_one(
                "SELECT public.novu_rust_telegram_attention($1,$2)",
                &[&"a", &Option::<uuid::Uuid>::None]
            )
            .await
            .is_err()
    );
    assert!(
        ops.query_one(
            "SELECT public.novu_rust_telegram_attention($1,$2)",
            &[&"other", &Option::<uuid::Uuid>::None]
        )
        .await
        .is_err()
    );
    let request = uuid::Uuid::new_v4();
    let query = "SELECT public.novu_rust_telegram_resolve($1,$2,$3,$4,$5,$6)";
    assert!(
        denied
            .query_one(
                query,
                &[&"a", &request, &job, &attempt, &"retry", &"verified"]
            )
            .await
            .is_err()
    );
    db.batch_execute("CREATE FUNCTION public.telegram_workflow_audit_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'test'; END $$; CREATE TRIGGER telegram_workflow_audit_failure BEFORE INSERT ON public.novu_rust_telegram_resolutions FOR EACH ROW EXECUTE FUNCTION public.telegram_workflow_audit_failure()").await.unwrap();
    assert!(
        ops.query_one(
            query,
            &[&"a", &request, &job, &attempt, &"retry", &"verified"]
        )
        .await
        .is_err()
    );
    assert_eq!(
        db.query_one(
            "SELECT state FROM public.novu_rust_telegram_attempts WHERE job_id=$1",
            &[&job]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "unknown"
    );
    db.batch_execute(
        "DROP TRIGGER telegram_workflow_audit_failure ON public.novu_rust_telegram_resolutions",
    )
    .await
    .unwrap();
    let params: &[&(dyn tokio_postgres::types::ToSql + Sync)] =
        &[&"a", &request, &job, &attempt, &"retry", &"verified"];
    let (first, second) = tokio::join!(ops.query_one(query, params), ops.query_one(query, params));
    let mut replay = [
        first.unwrap().get::<_, Value>(0)["replayed"]
            .as_bool()
            .unwrap(),
        second.unwrap().get::<_, Value>(0)["replayed"]
            .as_bool()
            .unwrap(),
    ];
    replay.sort();
    assert_eq!(replay, [false, true]);
    assert!(
        ops.query_one(
            query,
            &[&"a", &request, &job, &attempt, &"retry", &"changed"]
        )
        .await
        .is_err()
    );
    assert_eq!(db.query_one("SELECT count(*) FROM public.novu_rust_telegram_resolutions WHERE job_id=$1 AND attempt_id=$2 AND previous_state='unknown'", &[&job,&attempt]).await.unwrap().get::<_,i64>(0), 1);
    db.batch_execute("UPDATE public.novu_rust_telegram_workflow_endpoints SET chat_id='-999' WHERE identifier='ep'").await.unwrap();
    assert!(
        novu_postgres_queue::telegram_queue::process_one_with_workflow(
            &mut scoped,
            "a",
            "dest",
            &provider,
            true,
            true
        )
        .await
        .unwrap()
    );
    let result = db
        .query_one(
            "SELECT status,last_error FROM public.novu_queue_jobs WHERE id=$1",
            &[&job],
        )
        .await
        .unwrap();
    assert_eq!(result.get::<_, String>(0), "failed");
    assert_eq!(
        result.get::<_, String>(1),
        "Telegram endpoint changed or deleted"
    );
    assert!(
        db.query_opt(
            "SELECT 1 FROM public.novu_rust_telegram_attempts WHERE job_id=$1",
            &[&job]
        )
        .await
        .unwrap()
        .is_none()
    );
    produce(&mut db, app, "identity").await;
    db.batch_execute("DELETE FROM public.novu_rust_subscribers WHERE environment_id='a' AND subscriber_id='alice'; INSERT INTO public.novu_rust_subscribers(environment_id,subscriber_id,internal_id,profile) VALUES ('a','alice','new','{}')").await.unwrap();
    assert!(
        novu_postgres_queue::telegram_queue::process_one_with_workflow(
            &mut scoped,
            "a",
            "dest",
            &provider,
            true,
            true
        )
        .await
        .unwrap()
    );
    assert_eq!(db.query_one("SELECT count(*) FROM public.novu_queue_jobs WHERE kind=$1 AND last_error='subscriber identity changed'",&[&novu_postgres_queue::telegram_queue::WORKFLOW_KIND]).await.unwrap().get::<_,i64>(0),1);
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM public.novu_rust_telegram_attempts",
            &[]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        1
    );
}
