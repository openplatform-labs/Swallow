//! Resend automatic retry health and alert selection against a disposable database.
use novu_postgres_queue as queue;
use serde_json::{Value, json};
use std::process::Command;
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;
async fn connect(url: &str) -> Client {
    let (db, driver) = tokio_postgres::connect(url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    db
}
async fn job(db: &Client, env: &str) -> Uuid {
    let id = Uuid::new_v4();
    queue::enqueue(db, id, env, queue::resend_queue::JOB_KIND, &json!({}))
        .await
        .unwrap();
    id
}
fn cli(url: &str, env: &str) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
        .env("DATABASE_URL", url)
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .args(["check-resend-auto", env, "60"])
        .output()
        .unwrap()
}
fn alert(url: &str, env: &str) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_send-delivery-alert"))
        .env_clear()
        .env("DATABASE_URL", url)
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .env("SWALLOW_ALERT_RESEND_ENABLED", "false")
        .env("SWALLOW_ALERT_RESEND_AUTO_GRACE_SECONDS", "60")
        .env("SWALLOW_ALERT_SCOPED_DELIVERY", "true")
        .env(
            "SWALLOW_ALERT_SLACK_WEBHOOK",
            "https://hooks.slack.com/services/A/B/C",
        )
        .env("SWALLOW_ALERT_SLACK_TARGET_ID", "blocked")
        .args([env, "300", "120", "60"])
        .output()
        .unwrap()
}
#[tokio::test]
#[ignore = "requires migrated disposable PostgreSQL with role creation"]
async fn scoped_health_counts_attention_and_reaches_alert_reservation() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let db = connect(&url).await;
    let env = format!("auto-health-{}", Uuid::new_v4());
    let other = format!("auto-other-{}", Uuid::new_v4());
    let role = format!("health_{}", Uuid::new_v4().simple());
    db.execute(
        "INSERT INTO public.novu_rust_environments VALUES ($1,'org'),($2,'org')",
        &[&env, &other],
    )
    .await
    .unwrap();
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN PASSWORD 'test-only'; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_resend_auto_retry_health(text,integer),public.novu_rust_reserve_alert(text,text,text,integer,integer) TO {role}")).await.unwrap();
    let mut role_url = reqwest::Url::parse(&url).unwrap();
    role_url.set_username(&role).unwrap();
    role_url.set_password(Some("test-only")).unwrap();
    let actor = connect(role_url.as_str()).await;
    assert!(
        actor
            .query("SELECT * FROM public.novu_queue_jobs", &[])
            .await
            .is_err()
    );
    assert_eq!(cli(role_url.as_str(), &env).status.code(), Some(1));
    db.execute("INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT $1,$2,p,oid FROM pg_roles CROSS JOIN unnest(ARRAY['alert_view','alert_send']) p WHERE rolname::text=$2", &[&env,&role]).await.unwrap();
    let healthy = cli(role_url.as_str(), &env);
    assert_eq!(healthy.status.code(), Some(0));
    assert_eq!(
        serde_json::from_slice::<Value>(&healthy.stdout).unwrap(),
        json!({"rateLimited":0,"exhausted":0,"guardDeferred":0,"overdueAuto":0})
    );
    let healthy_alert = alert(role_url.as_str(), &env);
    assert_eq!(healthy_alert.status.code(), Some(0));
    assert!(String::from_utf8_lossy(&healthy_alert.stdout).contains("healthy"));
    db.execute("INSERT INTO public.novu_rust_resend_auto_retry_policy(environment_id,enabled,max_retries,not_before) VALUES ($1,true,1,clock_timestamp()-interval '1 day')", &[&env]).await.unwrap();
    let rate = job(&db, &env).await;
    db.execute(
        "UPDATE public.novu_queue_jobs SET status='failed' WHERE id=$1",
        &[&rate],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO public.novu_rust_resend_attempts(job_id,environment_id,state,http_status,retry_after_seconds,retry_not_before,created_at,updated_at) VALUES ($1,$2,'rejected',429,5,clock_timestamp(),clock_timestamp()-interval '1 hour',clock_timestamp()-interval '1 hour')", &[&rate,&env]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_delivery_reconciliations(id,environment_id,job_id,decision,operator_name,reason,previous_http_status,resend_auto_retry) VALUES ($1,$2,$3,'retry','auto','fixture',429,true)", &[&Uuid::new_v4(),&env,&rate]).await.unwrap();
    let deferred = job(&db, &env).await;
    db.execute("UPDATE public.novu_queue_jobs SET status='pending',last_error='Resend automatic retry requires evidence guards',available_at=clock_timestamp()+interval '5 seconds' WHERE id=$1", &[&deferred]).await.unwrap();
    let overdue = job(&db, &env).await;
    db.execute("UPDATE public.novu_queue_jobs SET status='pending',available_at=clock_timestamp()-interval '2 minutes' WHERE id=$1", &[&overdue]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_delivery_reconciliations(id,environment_id,job_id,decision,operator_name,reason,previous_http_status,resend_auto_retry) VALUES ($1,$2,$3,'retry','auto','fixture',429,true)", &[&Uuid::new_v4(),&env,&overdue]).await.unwrap();
    let fresh = job(&db, &env).await;
    db.execute(
        "UPDATE public.novu_queue_jobs SET status='failed' WHERE id=$1",
        &[&fresh],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO public.novu_rust_resend_attempts(job_id,environment_id,state,http_status) VALUES ($1,$2,'rejected',429)", &[&fresh,&env]).await.unwrap();
    let ignored = job(&db, &other).await;
    db.execute(
        "UPDATE public.novu_queue_jobs SET status='failed' WHERE id=$1",
        &[&ignored],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO public.novu_rust_resend_attempts(job_id,environment_id,state,http_status,updated_at) VALUES ($1,$2,'rejected',429,clock_timestamp()-interval '1 hour')", &[&ignored,&other]).await.unwrap();
    let result = cli(role_url.as_str(), &env);
    assert_eq!(result.status.code(), Some(2));
    let health: Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(
        health,
        json!({"rateLimited":1,"exhausted":1,"guardDeferred":1,"overdueAuto":1})
    );
    assert_eq!(cli(role_url.as_str(), &other).status.code(), Some(1));
    assert!(
        actor
            .query_one(
                "SELECT * FROM public.novu_rust_resend_auto_retry_health($1,0)",
                &[&env]
            )
            .await
            .is_err()
    );
    db.execute("INSERT INTO public.novu_rust_alert_delivery_slots(environment_id,channel,target_id,state) VALUES ($1,'slack','blocked','rejected')", &[&env]).await.unwrap();
    let output = alert(role_url.as_str(), &env);
    assert_eq!(
        output.status.code(),
        Some(2),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stderr).contains("requires operator review"));
    db.execute("DELETE FROM public.novu_rust_delivery_operator_grants WHERE environment_id=$1 AND database_principal=$2 AND decision='alert_view'", &[&env,&role]).await.unwrap();
    assert_eq!(cli(role_url.as_str(), &env).status.code(), Some(1));
    let output = alert(role_url.as_str(), &env);
    assert_eq!(output.status.code(), Some(1));
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("automatic retry health check failed")
    );
}
