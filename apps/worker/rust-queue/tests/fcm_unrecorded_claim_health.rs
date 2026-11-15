//! Scoped diagnostic for repeatedly claimed FCM work.
use novu_postgres_queue as queue;
use serde_json::{Value, json};
use std::process::Command;
use tokio_postgres::NoTls;
use uuid::Uuid;
fn cli(url: &str, env: &str) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
        .env("DATABASE_URL", url)
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .args(["check-fcm-unrecorded-claims", env, "2", "60"])
        .output()
        .unwrap()
}
fn alert(url: &str, env: &str) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_send-delivery-alert"))
        .env_clear()
        .env("DATABASE_URL", url)
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .env("SWALLOW_ALERT_RESEND_ENABLED", "false")
        .env("SWALLOW_ALERT_FCM_UNRECORDED_CLAIM_MIN_GAP", "2")
        .env("SWALLOW_ALERT_FCM_UNRECORDED_CLAIM_GRACE_SECONDS", "60")
        .env("SWALLOW_ALERT_SCOPED_DELIVERY", "true")
        .env("SWALLOW_ALERT_CONDITIONS_V2", "true")
        .env("SWALLOW_ALERT_COOLDOWN_POLICY", "new-condition")
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
async fn diagnostic_is_scoped_and_reaches_alert_runner() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let env = format!("fcm-claims-{}", Uuid::new_v4());
    let other = format!("fcm-other-{}", Uuid::new_v4());
    let role = format!("fcm_claim_{}", Uuid::new_v4().simple());
    db.execute(
        "INSERT INTO public.novu_rust_environments VALUES($1,'org'),($2,'org')",
        &[&env, &other],
    )
    .await
    .unwrap();
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN PASSWORD 'test-only'; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_fcm_unrecorded_claim_health(text,integer,integer),public.novu_rust_reserve_alert_v2(text,text,text,integer,bigint),public.novu_rust_observe_alert_conditions_v2(text,text,text,bigint,bigint) TO {role}")).await.unwrap();
    let mut role_url = reqwest::Url::parse(&url).unwrap();
    role_url.set_username(&role).unwrap();
    role_url.set_password(Some("test-only")).unwrap();
    assert_eq!(cli(role_url.as_str(), &env).status.code(), Some(1));
    db.execute("INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT $1,$2,p,oid FROM pg_roles CROSS JOIN unnest(ARRAY['alert_view','alert_send']) p WHERE rolname::text=$2", &[&env,&role]).await.unwrap();
    assert_eq!(cli(role_url.as_str(), &other).status.code(), Some(1));
    assert!(
        db.query_one(
            "SELECT public.novu_rust_fcm_unrecorded_claim_health($1,1,60)",
            &[&env]
        )
        .await
        .is_err()
    );
    assert_eq!(cli(role_url.as_str(), &env).status.code(), Some(0));
    assert_eq!(alert(role_url.as_str(), &env).status.code(), Some(0));
    let suspect = Uuid::new_v4();
    let normal = Uuid::new_v4();
    let future = Uuid::new_v4();
    let foreign = Uuid::new_v4();
    for (id, e) in [
        (suspect, &env),
        (normal, &env),
        (future, &env),
        (foreign, &other),
    ] {
        queue::enqueue(
            &db,
            id,
            e,
            queue::fcm_queue::JOB_KIND,
            &json!({"secret":"fixture"}),
        )
        .await
        .unwrap();
    }
    db.execute("UPDATE public.novu_queue_jobs SET status='running',attempts=3,lease_token=gen_random_uuid(),lease_until=clock_timestamp()+interval '2 minutes',available_at=clock_timestamp()-interval '2 minutes' WHERE id=ANY($1)", &[&vec![suspect,normal,foreign]]).await.unwrap();
    db.execute("UPDATE public.novu_queue_jobs SET attempts=3,available_at=clock_timestamp()+interval '2 minutes' WHERE id=$1", &[&future]).await.unwrap();
    for index in 0..2 {
        db.execute("INSERT INTO public.novu_rust_fcm_attempts(job_id,environment_id,target_index,state) VALUES($1,$2,$3,'unknown')", &[&normal,&env,&index]).await.unwrap();
    }
    let output = cli(role_url.as_str(), &env);
    assert_eq!(output.status.code(), Some(2));
    let v: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(v["unrecordedClaimJobs"], 1);
    assert!(!v.to_string().contains("fixture"));
    db.execute("INSERT INTO public.novu_rust_alert_delivery_slots(environment_id,channel,target_id,state) VALUES($1,'slack','blocked','rejected')", &[&env]).await.unwrap();
    let output = alert(role_url.as_str(), &env);
    assert_eq!(
        output.status.code(),
        Some(2),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    db.execute("DELETE FROM public.novu_rust_delivery_operator_grants WHERE environment_id=$1 AND database_principal=$2 AND decision='alert_view'", &[&env,&role]).await.unwrap();
    assert_eq!(cli(role_url.as_str(), &env).status.code(), Some(1));
    let output = alert(role_url.as_str(), &env);
    assert_eq!(output.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&output.stderr).contains("FCM unrecorded claims check failed"));
}
