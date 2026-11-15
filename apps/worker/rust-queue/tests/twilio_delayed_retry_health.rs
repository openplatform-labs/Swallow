//! Scoped delayed-retry health and CLI alerts on disposable PostgreSQL.
use std::process::Command;
use tokio_postgres::NoTls;
use uuid::Uuid;
fn check(url: &str, env: &str, grace: &str) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
        .env_clear()
        .env("DATABASE_URL", url)
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .args(["check-twilio-delayed", env, grace])
        .output()
        .unwrap()
}
fn alert(url: &str, env: &str, v2: bool) -> std::process::Output {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_send-delivery-alert"));
    if v2 {
        cmd.env("SWALLOW_ALERT_COOLDOWN_POLICY", "new-condition")
            .env("SWALLOW_ALERT_CONDITIONS_V2", "true");
    }
    cmd.env_clear()
        .env("DATABASE_URL", url)
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .env("SWALLOW_ALERT_RESEND_ENABLED", "false")
        .env("SWALLOW_ALERT_TWILIO_DELAYED_GRACE_SECONDS", "60")
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
#[ignore = "requires fully migrated disposable PostgreSQL and role creation"]
async fn scopes_counts_and_reports_blocked_alerts() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let env = format!("twilio-health-{}", Uuid::new_v4());
    let other = format!("twilio-other-{}", Uuid::new_v4());
    let role = format!("twilio_health_{}", Uuid::new_v4().simple());
    db.execute(
        "INSERT INTO public.novu_rust_environments VALUES($1,'org'),($2,'org')",
        &[&env, &other],
    )
    .await
    .unwrap();
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN PASSWORD 'test-only'; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_twilio_delayed_retry_health(text,integer),public.novu_rust_reserve_alert(text,text,text,integer,integer),public.novu_rust_reserve_alert_v2(text,text,text,integer,bigint),public.novu_rust_observe_alert_conditions_v2(text,text,text,bigint,bigint) TO {role}")).await.unwrap();
    let mut u = reqwest::Url::parse(&url).unwrap();
    u.set_username(&role).unwrap();
    u.set_password(Some("test-only")).unwrap();
    let role_url = u.as_str();
    assert_eq!(check(role_url, &env, "60").status.code(), Some(1));
    db.execute("INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT $1,$2,p,oid FROM pg_roles CROSS JOIN unnest(ARRAY['alert_view','alert_send']) p WHERE rolname::text=$2",&[&env,&role]).await.unwrap();
    let healthy = check(role_url, &env, "60");
    assert_eq!(healthy.status.code(), Some(0));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&healthy.stdout).unwrap(),
        serde_json::json!({"pendingOverdue":0,"expiredLease":0,"callbackBlocked":0})
    );
    assert_eq!(check(role_url, &other, "60").status.code(), Some(1));
    assert_eq!(check(role_url, &env, "0").status.code(), Some(1));
    db.execute("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status,twilio_delayed_retry,available_at,lease_until,lease_token,last_error) VALUES(gen_random_uuid(),$1,'experimental_twilio_sms','{}','pending',true,clock_timestamp()-interval '2 minutes',NULL,NULL,NULL),(gen_random_uuid(),$1,'experimental_twilio_sms','{}','running',true,clock_timestamp(),clock_timestamp()-interval '2 minutes',gen_random_uuid(),NULL),(gen_random_uuid(),$1,'experimental_twilio_sms','{}','failed',true,clock_timestamp(),NULL,NULL,'sms retry blocked by provider callback'),(gen_random_uuid(),$2,'experimental_twilio_sms','{}','pending',true,clock_timestamp()-interval '2 minutes',NULL,NULL,NULL)",&[&env,&other]).await.unwrap();
    let unhealthy = check(role_url, &env, "60");
    assert_eq!(unhealthy.status.code(), Some(2));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&unhealthy.stdout).unwrap(),
        serde_json::json!({"pendingOverdue":1,"expiredLease":1,"callbackBlocked":1})
    );
    db.execute("INSERT INTO public.novu_rust_alert_delivery_slots(environment_id,channel,target_id,state) VALUES($1,'slack','blocked','rejected')",&[&env]).await.unwrap();
    assert_eq!(alert(role_url, &env, false).status.code(), Some(2));
    assert_eq!(alert(role_url, &env, true).status.code(), Some(2));
    db.execute("DELETE FROM public.novu_rust_delivery_operator_grants WHERE environment_id=$1 AND database_principal=$2 AND decision='alert_view'",&[&env,&role]).await.unwrap();
    assert_eq!(check(role_url, &env, "60").status.code(), Some(1));
    assert_eq!(alert(role_url, &env, false).status.code(), Some(1));
}
