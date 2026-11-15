//! Shared Slack cooldown on a fully migrated disposable database.
#![cfg(feature = "test-mocks")]
use novu_postgres_queue as queue;
use serde_json::json;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use tokio_postgres::NoTls;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires fully migrated disposable PostgreSQL and role creation"]
async fn shared_slack_429_defers_all_three_workers_and_is_scoped() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (mut db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let env = format!("slack-cooldown-{}", Uuid::new_v4());
    let other = format!("slack-other-{}", Uuid::new_v4());
    db.execute(
        "INSERT INTO public.novu_rust_environments VALUES($1,'org'),($2,'org')",
        &[&env, &other],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO public.novu_rust_slack_cooldown_policies(environment_id,fallback_seconds) VALUES($1,7)",&[&env]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_slack_cooldown_workers SELECT $1,rolname,oid FROM pg_roles WHERE rolname=current_user",&[&env]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_workflows(environment_id,identifier,active,has_steps) VALUES($1,'flow',true,true)",&[&env]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_subscribers(environment_id,subscriber_id,internal_id,profile) VALUES($1,'alice','alice-internal','{}')",&[&env]).await.unwrap();
    db.execute(
        "INSERT INTO public.novu_rust_slack_endpoint_integrations VALUES($1,'main')",
        &[&env],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO public.novu_rust_slack_channel_endpoints(environment_id,identifier,subscriber_id,subscriber_internal_id,integration_identifier,endpoint_type,endpoint) VALUES($1,'one','alice','alice-internal','main','slack_channel','{\"channelId\":\"C1\"}')",&[&env]).await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        for response in [
            "HTTP/1.1 429 Too Many Requests\r\nRetry-After: 120\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            "HTTP/1.1 429 Too Many Requests\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            "HTTP/1.1 200 OK\r\nContent-Length: 35\r\nConnection: close\r\n\r\n{\"ok\":true,\"channel\":\"C1\",\"ts\":\"1\"}",
        ] {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut b = [0; 4096];
            assert!(socket.read(&mut b).await.unwrap() > 0);
            socket.write_all(response.as_bytes()).await.unwrap();
        }
    });
    let app = queue::slack::Slack::test_endpoint("test-token".into(), &endpoint).unwrap();
    let hook = queue::slack_webhook::Webhook::test_endpoint(&endpoint).unwrap();
    let first = Uuid::new_v4();
    queue::enqueue(
        &db,
        first,
        &env,
        queue::slack_queue::JOB_KIND,
        &json!({"channel":"C1","text":"hello"}),
    )
    .await
    .unwrap();
    assert!(
        queue::slack_queue::process_one_with_cooldown(&mut db, &env, &app, true)
            .await
            .unwrap()
    );
    assert!(db.query_one("SELECT not_before>clock_timestamp()+interval '100 seconds' FROM public.novu_rust_slack_cooldown_policies WHERE environment_id=$1",&[&env]).await.unwrap().get::<_,bool>(0));
    let hook_job = Uuid::new_v4();
    queue::enqueue(
        &db,
        hook_job,
        &env,
        queue::slack_webhook_queue::JOB_KIND,
        &json!({"destination":"ops","text":"hello"}),
    )
    .await
    .unwrap();
    assert!(
        queue::slack_webhook_queue::process_one_with_cooldown(&mut db, &env, "ops", &hook, true)
            .await
            .unwrap()
    );
    let ctx = queue::slack_workflow::Context {
        subscriber_id: "alice".into(),
        subscriber_internal_id: "alice-internal".into(),
        workflow_identifier: "flow".into(),
        workflow_internal_id: None,
        content_template: "hello".into(),
        payload: json!({}),
        context_keys: vec![],
        actor: None,
        actor_internal_id: None,
        endpoint_snapshot: None,
    };
    queue::enqueue(
        &db,
        Uuid::new_v4(),
        &env,
        queue::slack_workflow::FANOUT_KIND,
        &serde_json::to_value(ctx).unwrap(),
    )
    .await
    .unwrap();
    assert!(
        queue::slack_workflow::fanout_one(&mut db, &env)
            .await
            .unwrap()
    );
    let mut providers = queue::slack_workflow::Providers::new();
    providers.insert(
        ("main".into(), String::new()),
        queue::slack::Slack::test_endpoint("test-token".into(), &endpoint).unwrap(),
    );
    assert!(
        queue::slack_workflow::delivery_one_with_cooldown(
            &mut db,
            &env,
            &providers,
            queue::slack_workflow::StepPolicy::Snapshot,
            queue::slack_workflow::EndpointPolicy::Current,
            true
        )
        .await
        .unwrap()
    );
    let rows=db.query("SELECT j.status,j.available_at>clock_timestamp()+interval '100 seconds',j.last_error,(SELECT count(*) FROM public.novu_rust_slack_attempts a WHERE a.job_id=j.id) FROM public.novu_queue_jobs j WHERE j.environment_id=$1 AND j.kind=ANY($2) ORDER BY j.kind",&[&env,&vec![queue::slack_webhook_queue::JOB_KIND,queue::slack_workflow::DELIVERY_KIND]]).await.unwrap();
    assert_eq!(rows.len(), 2);
    for row in rows {
        assert_eq!(row.get::<_, String>(0), "pending");
        assert!(row.get::<_, bool>(1));
        assert_eq!(row.get::<_, String>(2), "Slack shared cooldown active");
        assert_eq!(row.get::<_, i64>(3), 0);
    }
    // Missing Retry-After uses the configured fallback and never shortens a longer existing cooldown.
    db.execute("UPDATE public.novu_rust_slack_cooldown_policies SET not_before='-infinity' WHERE environment_id=$1",&[&env]).await.unwrap();
    let second = Uuid::new_v4();
    queue::enqueue(
        &db,
        second,
        &env,
        queue::slack_queue::JOB_KIND,
        &json!({"channel":"C1","text":"hello"}),
    )
    .await
    .unwrap();
    assert!(
        queue::slack_queue::process_one_with_cooldown(&mut db, &env, &app, true)
            .await
            .unwrap()
    );
    assert!(db.query_one("SELECT not_before>clock_timestamp()+interval '5 seconds' FROM public.novu_rust_slack_cooldown_policies WHERE environment_id=$1",&[&env]).await.unwrap().get::<_,bool>(0));
    db.query_one(
        "SELECT public.novu_rust_slack_cooldown_record($1,120)",
        &[&env],
    )
    .await
    .unwrap();
    db.query_one(
        "SELECT public.novu_rust_slack_cooldown_record($1,1)",
        &[&env],
    )
    .await
    .unwrap();
    assert!(db.query_one("SELECT not_before>clock_timestamp()+interval '100 seconds' FROM public.novu_rust_slack_cooldown_policies WHERE environment_id=$1",&[&env]).await.unwrap().get::<_,bool>(0));
    // A dispatch reservation holds a shared policy lock; a concurrent 429 update waits.
    let (mut locker, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let tx = locker.transaction().await.unwrap();
    tx.query_one("SELECT public.novu_rust_slack_cooldown_check($1)", &[&env])
        .await
        .unwrap();
    let (writer, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let writer_env = env.clone();
    let pending = tokio::spawn(async move {
        writer
            .query_one(
                "SELECT public.novu_rust_slack_cooldown_record($1,30)",
                &[&writer_env],
            )
            .await
    });
    let mut waiting = false;
    for _ in 0..100 {
        if db.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE 'SELECT public.novu_rust_slack_cooldown_record%')",&[]).await.unwrap().get::<_,bool>(0){waiting=true;break;}
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(waiting && !pending.is_finished());
    tx.commit().await.unwrap();
    pending.await.unwrap().unwrap();
    // Default path remains available without a policy, and the selected path fails closed.
    let third = Uuid::new_v4();
    queue::enqueue(
        &db,
        third,
        &other,
        queue::slack_queue::JOB_KIND,
        &json!({"channel":"C1","text":"hello"}),
    )
    .await
    .unwrap();
    assert!(
        queue::slack_queue::process_one_with_cooldown(&mut db, &other, &app, true)
            .await
            .is_err()
    );
    db.execute("UPDATE public.novu_queue_jobs SET status='pending',lease_token=NULL,lease_until=NULL WHERE id=$1",&[&third]).await.unwrap();
    assert!(
        queue::slack_queue::process_one(&mut db, &other, &app)
            .await
            .unwrap()
    );
    server.await.unwrap();
    assert_eq!(
        db.query_one(
            "SELECT status FROM public.novu_queue_jobs WHERE id=$1",
            &[&third]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "completed"
    );
    let role = format!("cooldown_{}", Uuid::new_v4().simple());
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_slack_cooldown_check(text),public.novu_rust_slack_cooldown_record(text,integer) TO {role}")).await.unwrap();
    let (restricted, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    restricted
        .batch_execute(&format!("SET SESSION AUTHORIZATION {role}"))
        .await
        .unwrap();
    assert!(
        restricted
            .query_one("SELECT public.novu_rust_slack_cooldown_check($1)", &[&env])
            .await
            .is_err()
    );
    db.execute("INSERT INTO public.novu_rust_slack_cooldown_workers SELECT $1,rolname,oid FROM pg_roles WHERE rolname::text=$2",&[&env,&role]).await.unwrap();
    assert!(
        restricted
            .query_one("SELECT public.novu_rust_slack_cooldown_check($1)", &[&env])
            .await
            .is_ok()
    );
    assert!(
        restricted
            .query_one(
                "SELECT public.novu_rust_slack_cooldown_check($1)",
                &[&other]
            )
            .await
            .is_err()
    );
    db.execute("DELETE FROM public.novu_rust_slack_cooldown_workers WHERE environment_id=$1 AND database_principal=$2",&[&env,&role]).await.unwrap();
    assert!(
        restricted
            .query_one("SELECT public.novu_rust_slack_cooldown_check($1)", &[&env])
            .await
            .is_err()
    );
}
