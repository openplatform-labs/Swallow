//! Trigger-to-email-queue attachment handling against a migrated disposable database.
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tokio_postgres::NoTls;
use tower::ServiceExt;

static QUEUE_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[tokio::test]
#[ignore = "requires fully migrated disposable PostgreSQL"]
async fn trigger_lookup_is_environment_scoped_and_distinguishes_missing_jobs() {
    let _guard=QUEUE_TEST_LOCK.lock().await;let url=std::env::var("TEST_DATABASE_URL").unwrap();
    let (mut db,driver)=tokio_postgres::connect(&url,NoTls).await.unwrap();tokio::spawn(async move{driver.await.unwrap()});
    db.batch_execute("INSERT INTO public.novu_rust_users VALUES ('lookup-user'); INSERT INTO public.novu_rust_environments VALUES ('lookup-a','org'),('lookup-b','org'); INSERT INTO public.novu_rust_trigger_transactions(environment_id,transaction_id) VALUES ('lookup-a','fixture-txn'); INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload) VALUES ('11111111-1111-4111-8111-111111111111','lookup-a','experimental_static_in_app','{\"transactionId\":\"fixture-txn\",\"secret\":\"hidden\"}')").await.unwrap();
    for (env,key) in [("lookup-a","key-a"),("lookup-b","key-b")] {let hash=format!("{:x}",Sha256::digest(key.as_bytes()));db.execute("INSERT INTO public.novu_rust_api_keys(key_hash,environment_id,user_id) VALUES($1,$2,'lookup-user')",&[&hash,&env]).await.unwrap();}
    db.execute("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload) VALUES ('33333333-3333-4333-8333-333333333333','lookup-a','experimental_static_in_app',jsonb_build_object('transactionId',repeat('x',10000)))",&[]).await.unwrap();
    let (api,driver)=tokio_postgres::connect(&url,NoTls).await.unwrap();tokio::spawn(async move{driver.await.unwrap()});let router=novu_rust_api::router(Arc::new(api));
    let request=|key:&str,query:&str| Request::builder().uri(format!("/v1/experimental/workflow-trigger-result?{query}")).header("authorization",format!("ApiKey {key}")).body(Body::empty()).unwrap();
    let role=format!("lookup_api_{}",uuid::Uuid::new_v4().simple());
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT SELECT ON public.novu_rust_api_keys,public.novu_rust_users TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_api_trigger_lookup(text,text,text) TO {role}")).await.unwrap();
    let restricted_url=url.replacen("postgres://postgres@",&format!("postgres://{role}@"),1);let (restricted,driver)=tokio_postgres::connect(&restricted_url,NoTls).await.unwrap();tokio::spawn(async move{driver.await.unwrap()});
    assert!(restricted.query("SELECT * FROM public.novu_queue_jobs",&[]).await.is_err());assert!(restricted.execute("UPDATE public.novu_rust_api_keys SET enabled=false",&[]).await.is_err());let restricted_router=novu_rust_api::router(Arc::new(restricted));
    assert_eq!(restricted_router.clone().oneshot(request("key-a","transactionId=fixture-txn")).await.unwrap().status(),StatusCode::OK);
    let response=router.clone().oneshot(request("key-a","transactionId=fixture-txn")).await.unwrap();assert_eq!(response.status(),StatusCode::OK);assert_eq!(response.headers()["cache-control"],"no-store");let body:Value=serde_json::from_slice(&axum::body::to_bytes(response.into_body(),1024*16).await.unwrap()).unwrap();assert_eq!(body,json!({"data":{"transactionId":"fixture-txn","recorded":true,"jobCount":1,"job":{"jobId":"11111111-1111-4111-8111-111111111111","jobStatus":"pending"}}}));
    let response=router.clone().oneshot(request("key-b","transactionId=fixture-txn")).await.unwrap();let body:Value=serde_json::from_slice(&axum::body::to_bytes(response.into_body(),1024*16).await.unwrap()).unwrap();assert_eq!(body["data"],json!({"transactionId":"fixture-txn","recorded":false,"jobCount":0,"job":null}));
    db.execute("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload) VALUES ('22222222-2222-4222-8222-222222222222','lookup-a','experimental_static_in_app','{\"transactionId\":\"fixture-txn\"}')",&[]).await.unwrap();
    let response=router.clone().oneshot(request("key-a","transactionId=fixture-txn")).await.unwrap();let body:Value=serde_json::from_slice(&axum::body::to_bytes(response.into_body(),1024*16).await.unwrap()).unwrap();assert_eq!(body["data"],json!({"transactionId":"fixture-txn","recorded":true,"jobCount":2,"job":null}));
    assert_eq!(router.clone().oneshot(request("bad-key","transactionId=fixture-txn")).await.unwrap().status(),StatusCode::UNAUTHORIZED);
    assert_eq!(router.clone().oneshot(request("key-a","transactionId=fixture-txn&transactionId=fixture-txn")).await.unwrap().status(),StatusCode::BAD_REQUEST);
    let (observer,driver)=tokio_postgres::connect(&url,NoTls).await.unwrap();tokio::spawn(async move{driver.await.unwrap()});
    for commit in [false,true] {
        let tx=db.transaction().await.unwrap();tx.execute("UPDATE public.novu_rust_api_keys SET enabled=false WHERE environment_id='lookup-a'",&[]).await.unwrap();
        let pending=tokio::spawn(restricted_router.clone().oneshot(request("key-a","transactionId=fixture-txn")));
        let mut waiting=false;for _ in 0..100 {waiting=observer.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE '%novu_rust_api_trigger_lookup%')",&[]).await.unwrap().get(0);if waiting {break}tokio::time::sleep(std::time::Duration::from_millis(20)).await;}assert!(waiting);
        if commit {tx.commit().await.unwrap();assert_eq!(pending.await.unwrap().unwrap().status(),StatusCode::UNAUTHORIZED);} else {tx.rollback().await.unwrap();assert_eq!(pending.await.unwrap().unwrap().status(),StatusCode::OK);}
    }
    db.execute("UPDATE public.novu_rust_api_keys SET enabled=true WHERE environment_id='lookup-a'",&[]).await.unwrap();
    let tx=db.transaction().await.unwrap();tx.execute("DELETE FROM public.novu_rust_users WHERE id='lookup-user'",&[]).await.unwrap();
    let pending=tokio::spawn(restricted_router.clone().oneshot(request("key-a","transactionId=fixture-txn")));
    let mut waiting=false;for _ in 0..100 {waiting=observer.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE '%novu_rust_api_trigger_lookup%')",&[]).await.unwrap().get(0);if waiting {break}tokio::time::sleep(std::time::Duration::from_millis(20)).await;}assert!(waiting);tx.commit().await.unwrap();assert_eq!(pending.await.unwrap().unwrap().status(),StatusCode::UNAUTHORIZED);
    db.execute("INSERT INTO public.novu_rust_users VALUES('lookup-user')",&[]).await.unwrap();
    db.execute("DELETE FROM public.novu_queue_jobs WHERE environment_id='lookup-a'",&[]).await.unwrap();
    let response=router.clone().oneshot(request("key-a","transactionId=fixture-txn")).await.unwrap();let body:Value=serde_json::from_slice(&axum::body::to_bytes(response.into_body(),1024*16).await.unwrap()).unwrap();assert_eq!(body["data"],json!({"transactionId":"fixture-txn","recorded":true,"jobCount":0,"job":null}));
    db.execute("DELETE FROM public.novu_rust_trigger_transactions WHERE environment_id='lookup-a'",&[]).await.unwrap();
    let response=router.oneshot(request("key-a","transactionId=fixture-txn")).await.unwrap();let body:Value=serde_json::from_slice(&axum::body::to_bytes(response.into_body(),1024*16).await.unwrap()).unwrap();assert_eq!(body["data"],json!({"transactionId":"fixture-txn","recorded":false,"jobCount":0,"job":null}));
}

#[tokio::test]
#[ignore = "requires fully migrated disposable PostgreSQL"]
async fn trigger_preserves_bounded_attachments_for_both_email_providers() {
    let _queue_guard = QUEUE_TEST_LOCK.lock().await;
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (mut db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    db.batch_execute("INSERT INTO public.novu_rust_users VALUES ('attachment-user'); INSERT INTO public.novu_rust_environments VALUES ('attachment-env','org'); INSERT INTO public.novu_rust_workflows(environment_id,identifier,active,has_steps,email_subject,email_html,email_provider,inbox_metadata) VALUES ('attachment-env','resend-attachment',true,true,'Hello','<p>hello</p>','resend','{\"id\":\"workflow-r\"}'),('attachment-env','sendgrid-attachment',true,true,'Hello','<p>hello</p>','sendgrid','{\"id\":\"workflow-s\"}')").await.unwrap();
    db.batch_execute("UPDATE public.novu_rust_workflows SET in_app_content='Hello' WHERE environment_id='attachment-env'").await.unwrap();
    let hash = format!("{:x}", Sha256::digest(b"attachment-key"));
    db.execute("INSERT INTO public.novu_rust_api_keys(key_hash,environment_id,user_id) VALUES($1,'attachment-env','attachment-user')",&[&hash]).await.unwrap();
    let (api, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    let router = novu_rust_api::router(Arc::new(api));
    let payload = json!({"name":"Alice","attachments":[{"file":"aGVsbG8=","name":"hello.txt","mime":"text/plain","disposition":"attachment"},{"file":{"type":"Buffer","data":[0,1,255]},"name":"logo.png","mime":"image/png","cid":"logo","disposition":"inline","channels":["email"]}]});
    for provider in ["resend", "sendgrid"] {
        let body = json!({"name":format!("{provider}-attachment"),"to":{"subscriberId":format!("{provider}-person"),"email":"recipient@example.test"},"payload":payload,"transactionId":format!("attachment-{provider}")});
        let req = Request::builder()
            .method("POST")
            .uri("/v1/events/trigger")
            .header("authorization", "ApiKey attachment-key")
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap();
        assert_eq!(
            router.clone().oneshot(req).await.unwrap().status(),
            StatusCode::CREATED
        );
        let row=db.query_one("SELECT kind FROM public.novu_queue_jobs WHERE environment_id='attachment-env' AND payload->>'transactionId'=$1", &[&format!("attachment-{provider}")]).await.unwrap();
        assert_eq!(
            row.get::<_, String>(0),
            novu_postgres_queue::in_app::ATTACHMENT_JOB_KIND
        );
        assert!(
            novu_postgres_queue::claim(
                &db,
                novu_postgres_queue::in_app::JOB_KIND,
                std::time::Duration::from_secs(60)
            )
            .await
            .unwrap()
            .is_none()
        );
        assert!(
            novu_postgres_queue::in_app::process_one(&mut db)
                .await
                .unwrap()
        );
        let row=db.query_one("SELECT payload FROM public.novu_queue_jobs WHERE environment_id='attachment-env' AND payload->>'transaction_id'=$1", &[&format!("attachment-{provider}")]).await.unwrap();
        let queue: Value = row.get(0);
        assert!(queue.get("ip_pool_name").is_none());
        assert_eq!(
            queue["attachments"][0]["file"],
            json!([104, 101, 108, 108, 111])
        );
        assert_eq!(queue["attachments"][1]["file"], json!([0, 1, 255]));
        assert!(
            queue["workflow_context"]["payload"]["attachments"][0]
                .get("file")
                .is_none()
        );
        let row=db.query_one("SELECT payload FROM public.novu_rust_notifications WHERE environment_id='attachment-env' AND transaction_id=$1", &[&format!("attachment-{provider}")]).await.unwrap();
        let notification: Value = row.get(0);
        assert!(notification["attachments"][0].get("file").is_none());
        let row=db.query_one("SELECT payload FROM public.novu_rust_in_app_messages WHERE environment_id='attachment-env' AND transaction_id=$1", &[&format!("attachment-{provider}")]).await.unwrap();
        let message: Value = row.get(0);
        assert!(message.get("attachments").is_none());
    }
    let invalid = json!({"name":"resend-attachment","to":{"subscriberId":"bad"},"transactionId":"bad-attachment","payload":{"attachments":[{"file":"invalid?","name":"bad","mime":"text/plain"}]}});
    let req = Request::builder()
        .method("POST")
        .uri("/v1/events/trigger")
        .header("authorization", "ApiKey attachment-key")
        .header("content-type", "application/json")
        .body(Body::from(invalid.to_string()))
        .unwrap();
    assert_eq!(
        router.oneshot(req).await.unwrap().status(),
        StatusCode::NOT_IMPLEMENTED
    );
    assert_eq!(db.query_one("SELECT count(*) FROM public.novu_rust_trigger_transactions WHERE transaction_id='bad-attachment'",&[]).await.unwrap().get::<_,i64>(0),0);
}

#[tokio::test]
#[ignore = "requires fully migrated disposable PostgreSQL"]
async fn sendgrid_ip_pool_override_reaches_provider_body() {
    let _queue_guard = QUEUE_TEST_LOCK.lock().await;
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (mut db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    db.batch_execute("INSERT INTO public.novu_rust_users VALUES ('pool-user'); INSERT INTO public.novu_rust_environments VALUES ('pool-env','org'); INSERT INTO public.novu_rust_workflows(environment_id,identifier,active,has_steps,in_app_content,email_subject,email_html,email_provider,email_step_id,inbox_metadata) VALUES ('pool-env','pool-workflow',true,true,'Hello','Hello','<p>hello</p>','sendgrid','mail-step','{\"id\":\"pool-workflow-id\"}')").await.unwrap();
    let hash = format!("{:x}", Sha256::digest(b"pool-key"));
    db.execute("INSERT INTO public.novu_rust_api_keys(key_hash,environment_id,user_id) VALUES($1,'pool-env','pool-user')", &[&hash]).await.unwrap();
    let (api, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    let router = novu_rust_api::router(Arc::new(api));
    let cases = [
        (
            "channel",
            json!({"email":{"ipPoolName":"channel-pool"}}),
            "channel-pool",
        ),
        (
            "flat",
            json!({"email":{"ipPoolName":"channel-pool"},"sendgrid":{"ipPoolName":"flat-pool"}}),
            "flat-pool",
        ),
        (
            "provider",
            json!({"sendgrid":{"ipPoolName":"flat-pool"},"providers":{"sendgrid":{"ipPoolName":"provider-pool"}}}),
            "provider-pool",
        ),
        (
            "step",
            json!({"providers":{"sendgrid":{"ipPoolName":"provider-pool"}},"steps":{"mail-step":{"providers":{"sendgrid":{"ipPoolName":"step-pool"}}}}}),
            "step-pool",
        ),
        (
            "empty-provider",
            json!({"email":{"ipPoolName":"channel-pool"},"providers":{"sendgrid":{"ipPoolName":""}}}),
            "",
        ),
    ];
    for (case, overrides, expected) in cases {
        let body = json!({"name":"pool-workflow","to":{"subscriberId":format!("person-{case}"),"email":"recipient@example.test"},"payload":{},"transactionId":format!("pool-{case}"),"overrides":overrides});
        let req = Request::builder()
            .method("POST")
            .uri("/v1/events/trigger")
            .header("authorization", "ApiKey pool-key")
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap();
        assert_eq!(
            router.clone().oneshot(req).await.unwrap().status(),
            StatusCode::CREATED
        );
        assert!(
            novu_postgres_queue::in_app::process_one(&mut db)
                .await
                .unwrap()
        );
        let row = db.query_one("SELECT payload FROM public.novu_queue_jobs WHERE environment_id='pool-env' AND payload->>'transaction_id'=$1", &[&format!("pool-{case}")]).await.unwrap();
        let payload: Value = row.get(0);
        assert_eq!(payload["ip_pool_name"], expected);
        let mut email: novu_postgres_queue::sendgrid::Email =
            serde_json::from_value(payload).unwrap();
        email.to = vec!["recipient@example.test".into()];
        let config = novu_postgres_queue::sendgrid::Config {
            api_key: "SG.fixture".into(),
            from: "sender@example.test".into(),
            sender_name: "Sender".into(),
            region: novu_postgres_queue::sendgrid::Region::Global,
        };
        let body = novu_postgres_queue::sendgrid::request_body(&config, &email).unwrap();
        assert_eq!(body["ip_pool_name"], expected);
    }
}

#[tokio::test]
#[ignore = "requires fully migrated disposable PostgreSQL"]
async fn resend_alternatives_policy_checks_effective_override_without_partial_trigger() {
    use axum::Extension;
    let _queue_guard=QUEUE_TEST_LOCK.lock().await;
    let url=std::env::var("TEST_DATABASE_URL").unwrap();
    let (db,driver)=tokio_postgres::connect(&url,NoTls).await.unwrap();tokio::spawn(driver);
    db.batch_execute("INSERT INTO public.novu_rust_users VALUES ('alternative-policy-user'); INSERT INTO public.novu_rust_environments VALUES ('alternative-policy-env','org'); INSERT INTO public.novu_rust_workflows(environment_id,identifier,active,has_steps,in_app_content,email_subject,email_html,email_provider,email_step_id,inbox_metadata) VALUES ('alternative-policy-env','resend-workflow',true,true,'Hello','Hello','<p>Hello</p>','resend','mail-step','{\"id\":\"policy-workflow\"}'),('alternative-policy-env','sendgrid-workflow',true,true,'Hello','Hello','<p>Hello</p>','sendgrid','mail-step','{\"id\":\"policy-workflow-2\"}');").await.unwrap();
    let hash=format!("{:x}",Sha256::digest(b"alternative-policy-key"));db.execute("INSERT INTO public.novu_rust_api_keys(key_hash,environment_id,user_id) VALUES($1,'alternative-policy-env','alternative-policy-user')",&[&hash]).await.unwrap();
    let (api,driver)=tokio_postgres::connect(&url,NoTls).await.unwrap();tokio::spawn(driver);
    let default=novu_rust_api::router_with_subscriber_secret(Arc::new(api),None);let strict=default.clone().layer(Extension(novu_rust_api::RejectResendAlternatives(true)));
    let part=json!([{"contentType":"text/calendar","content":"meeting"}]);
    let cases=[
      ("reject-channel","resend-workflow",json!({"email":{"alternatives":part}}),StatusCode::UNPROCESSABLE_ENTITY),
      ("reject-provider","resend-workflow",json!({"providers":{"resend":{"alternatives":part}}}),StatusCode::UNPROCESSABLE_ENTITY),
      ("reject-step","resend-workflow",json!({"email":{"alternatives":[]},"steps":{"mail-step":{"providers":{"resend":{"alternatives":part}}}}}),StatusCode::UNPROCESSABLE_ENTITY),
      ("allow-provider-clear","resend-workflow",json!({"email":{"alternatives":part},"providers":{"resend":{"alternatives":[]}}}),StatusCode::CREATED),
      ("allow-step-clear","resend-workflow",json!({"providers":{"resend":{"alternatives":part}},"steps":{"mail-step":{"providers":{"resend":{"alternatives":[]}}}}}),StatusCode::CREATED),
      ("allow-other-step","resend-workflow",json!({"steps":{"other":{"providers":{"resend":{"alternatives":part}}}}}),StatusCode::CREATED),
      ("allow-other-provider","resend-workflow",json!({"providers":{"sendgrid":{"alternatives":part}}}),StatusCode::CREATED),
      ("allow-sendgrid","sendgrid-workflow",json!({"email":{"alternatives":part}}),StatusCode::CREATED),
    ];
    let request=|workflow:&str,transaction:&str,overrides:Value| Request::builder().method("POST").uri("/v1/events/trigger").header("authorization","ApiKey alternative-policy-key").header("content-type","application/json").body(Body::from(json!({"name":workflow,"to":{"subscriberId":transaction,"email":"recipient@example.test"},"transactionId":transaction,"payload":{},"overrides":overrides}).to_string())).unwrap();
    for (id,workflow,overrides,status) in cases {assert_eq!(strict.clone().oneshot(request(workflow,id,overrides)).await.unwrap().status(),status,"{id}");}
    for id in ["reject-channel","reject-provider","reject-step"] {assert_eq!(db.query_one("SELECT count(*) FROM public.novu_rust_trigger_transactions WHERE transaction_id=$1",&[&id]).await.unwrap().get::<_,i64>(0),0);}
    assert_eq!(default.clone().layer(Extension(novu_rust_api::RejectResendAlternatives(false))).oneshot(request("resend-workflow","defer",json!({"email":{"alternatives":part}}))).await.unwrap().status(),StatusCode::CREATED);
    db.execute("UPDATE public.novu_rust_workflows SET email_step_active=false WHERE environment_id='alternative-policy-env' AND identifier='resend-workflow'",&[]).await.unwrap();
    assert_eq!(strict.oneshot(request("resend-workflow","inactive-email",json!({"email":{"alternatives":part}}))).await.unwrap().status(),StatusCode::CREATED);
}

#[tokio::test]
#[ignore = "requires fully migrated disposable PostgreSQL and loopback"]
async fn sendgrid_tracking_reaches_mock_provider_with_layer_precedence() {
    use tokio::io::{AsyncReadExt,AsyncWriteExt};
    let _guard=QUEUE_TEST_LOCK.lock().await;let url=std::env::var("TEST_DATABASE_URL").unwrap();
    let (mut db,driver)=tokio_postgres::connect(&url,NoTls).await.unwrap();tokio::spawn(async move{driver.await.unwrap()});
    db.batch_execute("INSERT INTO public.novu_rust_users VALUES ('tracking-user'); INSERT INTO public.novu_rust_environments VALUES ('tracking-env','org'); INSERT INTO public.novu_rust_workflows(environment_id,identifier,active,has_steps,in_app_content,email_subject,email_html,email_provider,email_step_id,inbox_metadata) VALUES ('tracking-env','tracking-workflow',true,true,'Hi','Hi','<p>Hi</p>','sendgrid','mail-step','{\"id\":\"tracking-workflow-id\"}')").await.unwrap();
    let hash=format!("{:x}",Sha256::digest(b"tracking-key"));db.execute("INSERT INTO public.novu_rust_api_keys(key_hash,environment_id,user_id) VALUES($1,'tracking-env','tracking-user')",&[&hash]).await.unwrap();
    let (api,driver)=tokio_postgres::connect(&url,NoTls).await.unwrap();tokio::spawn(async move{driver.await.unwrap()});let router=novu_rust_api::router(Arc::new(api));
    let request=|txn:&str,overrides:Value| Request::builder().method("POST").uri("/v1/events/trigger").header("authorization","ApiKey tracking-key").header("content-type","application/json").body(Body::from(json!({"name":"tracking-workflow","to":{"subscriberId":txn,"email":"recipient@example.test"},"payload":{},"transactionId":txn,"overrides":overrides}).to_string())).unwrap();
    for (i,invalid) in [json!({"providers":{"sendgrid":{"trackingSettings":{"clickTracking":{"enable":"yes"}}}}}),json!({"providers":{"sendgrid":{"_passthrough":{"body":{"trackingSettings":{"clickTracking":{"unknown":true}}}}}}}),json!({"providers":{"resend":{"trackingSettings":{"clickTracking":{"enable":true}}}}})].into_iter().enumerate() {
        assert_eq!(router.clone().oneshot(request(&format!("bad-tracking-{i}"),invalid)).await.unwrap().status(),StatusCode::NOT_IMPLEMENTED);
    }
    let overrides=json!({"providers":{"sendgrid":{"trackingSettings":{"clickTracking":{"enable":true,"enableText":true},"openTracking":{"enable":true}},"_passthrough":{"body":{"trackingSettings":{"clickTracking":{"enable":false}}}}}},"steps":{"mail-step":{"providers":{"sendgrid":{"trackingSettings":{"openTracking":{"substitutionTag":"%open%"}},"_passthrough":{"body":{"trackingSettings":{"subscriptionTracking":{"enable":true,"text":"bye"}}}}}}}}});
    assert_eq!(router.oneshot(request("tracking-valid",overrides)).await.unwrap().status(),StatusCode::CREATED);
    assert!(novu_postgres_queue::in_app::process_one(&mut db).await.unwrap());
    let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();let addr=listener.local_addr().unwrap();
    let server=tokio::spawn(async move{let (mut socket,_)=listener.accept().await.unwrap();let mut raw=Vec::new();let mut chunk=[0;4096];loop{let n=socket.read(&mut chunk).await.unwrap();assert!(n>0);raw.extend_from_slice(&chunk[..n]);if let Some(pos)=raw.windows(4).position(|v|v==b"\r\n\r\n"){let header=String::from_utf8_lossy(&raw[..pos]);let len=header.lines().find_map(|l|l.to_ascii_lowercase().strip_prefix("content-length: ").and_then(|v|v.parse::<usize>().ok())).unwrap();if raw.len()>=pos+4+len{let body:Value=serde_json::from_slice(&raw[pos+4..pos+4+len]).unwrap();socket.write_all(b"HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.unwrap();return body;}}}});
    let provider=novu_postgres_queue::sendgrid::Sendgrid::test_endpoint(novu_postgres_queue::sendgrid::Config{api_key:"fixture".into(),from:"sender@example.test".into(),sender_name:"Sender".into(),region:novu_postgres_queue::sendgrid::Region::Global},&format!("http://{addr}/send")).unwrap();
    assert!(novu_postgres_queue::sendgrid_queue::process_one(&mut db,"tracking-env",&provider).await.unwrap());
    let body=server.await.unwrap();assert_eq!(body["tracking_settings"],json!({"click_tracking":{"enable":false,"enable_text":true},"open_tracking":{"enable":true,"substitution_tag":"%open%"},"subscription_tracking":{"enable":true,"text":"bye"}}));
    let row=db.query_one("SELECT j.status,a.state FROM novu_queue_jobs j JOIN novu_rust_sendgrid_attempts a ON a.job_id=j.id WHERE j.environment_id='tracking-env'",&[]).await.unwrap();assert_eq!(row.get::<_,String>(0),"completed");assert_eq!(row.get::<_,String>(1),"accepted");
}


#[tokio::test]
#[ignore = "requires fully migrated disposable PostgreSQL and loopback"]
async fn sendgrid_sandbox_workflow_records_validation_without_delivery() {
    use tokio::io::{AsyncReadExt,AsyncWriteExt};
    let _guard=QUEUE_TEST_LOCK.lock().await;let url=std::env::var("TEST_DATABASE_URL").unwrap();
    let (mut db,driver)=tokio_postgres::connect(&url,NoTls).await.unwrap();tokio::spawn(async move{driver.await.unwrap()});
    db.batch_execute("INSERT INTO public.novu_rust_users VALUES ('sandbox-user'); INSERT INTO public.novu_rust_environments VALUES ('sandbox-env','org'); INSERT INTO public.novu_rust_workflows(environment_id,identifier,active,has_steps,in_app_content,email_subject,email_html,email_provider,email_step_id,inbox_metadata) VALUES ('sandbox-env','sandbox-workflow',true,true,'Hi','Hi','<p>Hi</p>','sendgrid','mail-step','{\"id\":\"sandbox-workflow-id\"}')").await.unwrap();
    let hash=format!("{:x}",Sha256::digest(b"sandbox-key"));db.execute("INSERT INTO public.novu_rust_api_keys(key_hash,environment_id,user_id) VALUES($1,'sandbox-env','sandbox-user')",&[&hash]).await.unwrap();
    let (api,driver)=tokio_postgres::connect(&url,NoTls).await.unwrap();tokio::spawn(async move{driver.await.unwrap()});let router=novu_rust_api::router(Arc::new(api));
    let request=|txn:&str,overrides:Value| Request::builder().method("POST").uri("/v1/events/trigger").header("authorization","ApiKey sandbox-key").header("content-type","application/json").body(Body::from(json!({"name":"sandbox-workflow","to":{"subscriberId":txn,"email":"recipient@example.test"},"payload":{},"transactionId":txn,"overrides":overrides}).to_string())).unwrap();
    for (i,invalid) in [json!({"providers":{"sendgrid":{"mailSettings":{"sandboxMode":{"enable":"yes"}}}}}),json!({"providers":{"resend":{"mailSettings":{"sandboxMode":{"enable":true}}}}})].into_iter().enumerate() {
        assert_eq!(router.clone().oneshot(request(&format!("invalid-sandbox-{i}"),invalid)).await.unwrap().status(),StatusCode::NOT_IMPLEMENTED);
    }
    let overrides=json!({"providers":{"sendgrid":{"mailSettings":{"sandboxMode":{"enable":false}},"_passthrough":{"body":{"mailSettings":{"sandboxMode":{"enable":true}}}}}},"steps":{"mail-step":{"providers":{"sendgrid":{"mailSettings":{"sandboxMode":{"enable":false}}}}}}});
    assert_eq!(router.clone().oneshot(request("sandbox-valid",overrides)).await.unwrap().status(),StatusCode::CREATED);
    assert!(novu_postgres_queue::in_app::process_one(&mut db).await.unwrap());
    let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();let addr=listener.local_addr().unwrap();
    let server=tokio::spawn(async move{let (mut socket,_)=listener.accept().await.unwrap();let mut raw=Vec::new();let mut chunk=[0;4096];loop{let n=socket.read(&mut chunk).await.unwrap();assert!(n>0);raw.extend_from_slice(&chunk[..n]);if let Some(pos)=raw.windows(4).position(|v|v==b"\r\n\r\n"){let header=String::from_utf8_lossy(&raw[..pos]);let len=header.lines().find_map(|l|l.to_ascii_lowercase().strip_prefix("content-length: ").and_then(|v|v.parse::<usize>().ok())).unwrap();if raw.len()>=pos+4+len{let body:Value=serde_json::from_slice(&raw[pos+4..pos+4+len]).unwrap();socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.unwrap();return body;}}}});
    let provider=novu_postgres_queue::sendgrid::Sendgrid::test_endpoint(novu_postgres_queue::sendgrid::Config{api_key:"fixture".into(),from:"sender@example.test".into(),sender_name:"Sender".into(),region:novu_postgres_queue::sendgrid::Region::Global},&format!("http://{addr}/send")).unwrap();
    assert!(novu_postgres_queue::sendgrid_queue::process_one(&mut db,"sandbox-env",&provider).await.unwrap());
    let body=server.await.unwrap();assert_eq!(body["mail_settings"],json!({"sandbox_mode":{"enable":true}}));
    let row=db.query_one("SELECT j.id,j.status,a.state,a.dispatch_id,m.status,m.delivery_state FROM novu_queue_jobs j JOIN novu_rust_sendgrid_attempts a ON a.job_id=j.id JOIN novu_rust_email_messages m ON m.email_job_id=j.id WHERE j.environment_id='sandbox-env'",&[]).await.unwrap();
    assert_eq!(row.get::<_,String>(1),"completed");assert_eq!(row.get::<_,String>(2),"validated");assert_eq!(row.get::<_,Option<uuid::Uuid>>(3),None);assert_eq!(row.get::<_,String>(4),"warning");assert_eq!(row.get::<_,String>(5),"validated");
    let job:uuid::Uuid=row.get(0);
    assert!(db.execute("UPDATE novu_rust_sendgrid_attempts SET state='accepted' WHERE job_id=$1",&[&job]).await.is_err());
    assert!(db.execute("UPDATE novu_queue_jobs SET payload=jsonb_set(payload,'{sandbox_mode}','false'::jsonb) WHERE id=$1",&[&job]).await.is_err());
    assert!(novu_postgres_queue::sendgrid_projection::project(&mut db,"sandbox-env",job).await.unwrap());
    let result=db.query_one("SELECT sendgrid_delivery_result FROM novu_queue_jobs WHERE id=$1",&[&job]).await.unwrap().get::<_,Option<String>>(0);assert_eq!(result.as_deref(),Some("unknown"));
    db.execute("UPDATE novu_queue_jobs SET available_at=clock_timestamp()-interval '1 hour' WHERE id=$1",&[&job]).await.unwrap();
    let health=novu_postgres_queue::reconciliation::sendgrid_health(&db,"sandbox-env",10,10).await.unwrap();assert_eq!(health["eligibleJobs"],0);
    for (name,response,expected) in [("unexpected-202",Some(202),"unknown"),("rejected-400",Some(400),"rejected"),("transport",None,"unknown")] {
        let options=json!({"providers":{"sendgrid":{"mailSettings":{"sandboxMode":{"enable":true}}}}});
        assert_eq!(router.clone().oneshot(request(name,options)).await.unwrap().status(),StatusCode::CREATED);
        assert!(novu_postgres_queue::in_app::process_one(&mut db).await.unwrap());
        let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();let addr=listener.local_addr().unwrap();
        let server=tokio::spawn(async move {let (mut socket,_)=listener.accept().await.unwrap();let mut raw=Vec::new();let mut chunk=[0;4096];loop {let n=socket.read(&mut chunk).await.unwrap();assert!(n>0);raw.extend_from_slice(&chunk[..n]);if let Some(pos)=raw.windows(4).position(|v|v==b"\r\n\r\n") {let header=String::from_utf8_lossy(&raw[..pos]);let len=header.lines().find_map(|l|l.to_ascii_lowercase().strip_prefix("content-length: ").and_then(|v|v.parse::<usize>().ok())).unwrap();if raw.len()>=pos+4+len {let body:Value=serde_json::from_slice(&raw[pos+4..pos+4+len]).unwrap();assert_eq!(body["mail_settings"]["sandbox_mode"]["enable"],true);break;}}}if let Some(code)=response {socket.write_all(format!("HTTP/1.1 {code} Fixture\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();}});
        let provider=novu_postgres_queue::sendgrid::Sendgrid::test_endpoint(novu_postgres_queue::sendgrid::Config{api_key:"fixture".into(),from:"sender@example.test".into(),sender_name:"Sender".into(),region:novu_postgres_queue::sendgrid::Region::Global},&format!("http://{addr}/send")).unwrap().with_dispatch_tracking();
        assert!(novu_postgres_queue::sendgrid_queue::process_one(&mut db,"sandbox-env",&provider).await.unwrap());server.await.unwrap();
        let row=db.query_one("SELECT j.id,j.status,a.state,a.dispatch_id,a.http_status,m.status,m.delivery_state FROM novu_queue_jobs j JOIN novu_rust_sendgrid_attempts a ON a.job_id=j.id JOIN novu_rust_email_messages m ON m.email_job_id=j.id WHERE j.environment_id='sandbox-env' AND j.payload->>'transaction_id'=$1",&[&name]).await.unwrap();
        assert_eq!(row.get::<_,String>(1),"failed");assert_eq!(row.get::<_,String>(2),expected);assert_eq!(row.get::<_,Option<uuid::Uuid>>(3),None);assert_eq!(row.get::<_,Option<i32>>(4),response);assert_eq!(row.get::<_,String>(5),"error");assert_eq!(row.get::<_,String>(6),expected);
        let failed:uuid::Uuid=row.get(0);assert!(db.execute("UPDATE novu_rust_sendgrid_attempts SET state='accepted' WHERE job_id=$1",&[&failed]).await.is_err());
        db.execute("UPDATE novu_queue_jobs SET available_at=clock_timestamp()-interval '1 hour' WHERE id=$1",&[&failed]).await.unwrap();
    }
    let health=novu_postgres_queue::reconciliation::sendgrid_health(&db,"sandbox-env",10,10).await.unwrap();assert_eq!(health["eligibleJobs"],3);assert_eq!(health["overdueUndelivered"],3);
    db.batch_execute("INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'sandbox-env',rolname,'alert_view',oid FROM pg_roles WHERE rolname=session_user").await.unwrap();
    let scoped=novu_postgres_queue::reconciliation::scoped_sendgrid_health(&db,"sandbox-env",10,10).await.unwrap();assert_eq!(scoped["eligibleJobs"],3);assert_eq!(scoped["overdueUndelivered"],3);
}
