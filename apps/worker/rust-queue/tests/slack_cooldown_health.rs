//! Slack shared cooldown monitoring and alert selection.
use novu_postgres_queue as queue;
use serde_json::{Value, json};
use std::process::Command;
use tokio_postgres::NoTls;
use uuid::Uuid;

fn cli(url: &str, env: &str) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
        .env("DATABASE_URL", url)
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .args(["check-slack-cooldown", env, "60"])
        .output()
        .unwrap()
}
fn alert(url: &str, env: &str) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_send-delivery-alert"))
        .env_clear()
        .env("DATABASE_URL", url)
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .env("SWALLOW_ALERT_RESEND_ENABLED", "false")
        .env("SWALLOW_ALERT_SLACK_COOLDOWN_GRACE_SECONDS", "60")
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
#[ignore = "requires migrated disposable PostgreSQL and role creation"]
async fn scoped_slack_cooldown_health_reaches_alerts() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let env = format!("cool-health-{}", Uuid::new_v4());
    let other = format!("cool-other-{}", Uuid::new_v4());
    let role = format!("cool_health_{}", Uuid::new_v4().simple());
    db.execute(
        "INSERT INTO public.novu_rust_environments VALUES ($1,'org'),($2,'org')",
        &[&env, &other],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO public.novu_rust_slack_cooldown_policies(environment_id,fallback_seconds) VALUES ($1,30),($2,30)",&[&env,&other]).await.unwrap();
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN PASSWORD 'test-only'; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_slack_cooldown_health(text,integer),public.novu_rust_reserve_alert(text,text,text,integer,integer) TO {role}")).await.unwrap();
    let mut role_url = reqwest::Url::parse(&url).unwrap();
    role_url.set_username(&role).unwrap();
    role_url.set_password(Some("test-only")).unwrap();
    assert_eq!(cli(role_url.as_str(), &env).status.code(), Some(1));
    db.execute("INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT $1,$2,p,oid FROM pg_roles CROSS JOIN unnest(ARRAY['alert_view','alert_send']) p WHERE rolname::text=$2",&[&env,&role]).await.unwrap();
    let healthy = cli(role_url.as_str(), &env);
    assert_eq!(healthy.status.code(), Some(0));
    assert_eq!(
        serde_json::from_slice::<Value>(&healthy.stdout).unwrap(),
        json!({"active":false,"notBefore":null,"fallbackSeconds":30,"deferred":0,"overdue":0})
    );
    assert_eq!(alert(role_url.as_str(), &env).status.code(), Some(0));
    assert_eq!(cli(role_url.as_str(), &other).status.code(), Some(1));
    db.execute("UPDATE public.novu_rust_slack_cooldown_policies SET not_before=clock_timestamp()+interval '2 minutes' WHERE environment_id=$1",&[&env]).await.unwrap();
    let waiting = Uuid::new_v4();
    let overdue = Uuid::new_v4();
    let unrelated = Uuid::new_v4();
    for (id, e) in [(waiting, &env), (overdue, &env), (unrelated, &other)] {
        queue::enqueue(
            &db,
            id,
            e,
            queue::slack_queue::JOB_KIND,
            &json!({"secret":"fixture"}),
        )
        .await
        .unwrap();
    }
    db.execute("UPDATE public.novu_queue_jobs SET last_error='Slack shared cooldown active',available_at=clock_timestamp()+interval '2 minutes' WHERE id=$1",&[&waiting]).await.unwrap();
    db.execute("UPDATE public.novu_queue_jobs SET last_error='Slack shared cooldown active',available_at=clock_timestamp()-interval '2 minutes' WHERE id=ANY($1)",&[&vec![overdue,unrelated]]).await.unwrap();
    let result = cli(role_url.as_str(), &env);
    assert_eq!(result.status.code(), Some(2));
    let value: Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(value["active"], true);
    assert_eq!(value["deferred"], 2);
    assert_eq!(value["overdue"], 1);
    assert!(!value.to_string().contains("fixture"));
    db.execute("INSERT INTO public.novu_rust_alert_delivery_slots(environment_id,channel,target_id,state) VALUES ($1,'slack','blocked','rejected')",&[&env]).await.unwrap();
    let result = alert(role_url.as_str(), &env);
    assert_eq!(
        result.status.code(),
        Some(2),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert!(String::from_utf8_lossy(&result.stderr).contains("requires operator review"));
    db.execute("DELETE FROM public.novu_rust_delivery_operator_grants WHERE environment_id=$1 AND database_principal=$2 AND decision='alert_view'",&[&env,&role]).await.unwrap();
    assert_eq!(cli(role_url.as_str(), &env).status.code(), Some(1));
    let result = alert(role_url.as_str(), &env);
    assert_eq!(result.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&result.stderr).contains("Slack shared cooldown check failed"));
}
