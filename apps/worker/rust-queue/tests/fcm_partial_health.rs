//! Scoped FCM partial-failure monitoring on a fully migrated disposable database.
use std::process::Command;
use tokio_postgres::NoTls;
use uuid::Uuid;
fn check(url: &str, environment: &str, window: &str) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
        .env_clear()
        .env("DATABASE_URL", url)
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .args(["check-fcm-partial", environment, window])
        .output()
        .unwrap()
}
fn alert(url: &str, environment: &str) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_send-delivery-alert"))
        .env_clear()
        .env("DATABASE_URL", url)
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .env("SWALLOW_ALERT_RESEND_ENABLED", "false")
        .env("SWALLOW_ALERT_FCM_PARTIAL_WINDOW_SECONDS", "60")
        .env("SWALLOW_ALERT_SCOPED_DELIVERY", "true")
        .env("SWALLOW_ALERT_COOLDOWN_POLICY", "new-condition")
        .env("SWALLOW_ALERT_CONDITIONS_V2", "true")
        .env(
            "SWALLOW_ALERT_SLACK_WEBHOOK",
            "https://hooks.slack.com/services/A/B/C",
        )
        .env("SWALLOW_ALERT_SLACK_TARGET_ID", "blocked")
        .args([environment, "300", "120", "60"])
        .output()
        .unwrap()
}
#[tokio::test]
#[ignore = "requires fully migrated disposable PostgreSQL and role creation"]
async fn scoped_recent_partial_jobs_are_counted_once() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let env = format!("fcm-partial-{}", Uuid::new_v4());
    let other = format!("fcm-other-{}", Uuid::new_v4());
    let role = format!("fcm_partial_{}", Uuid::new_v4().simple());
    db.execute(
        "INSERT INTO public.novu_rust_environments VALUES($1,'org'),($2,'org')",
        &[&env, &other],
    )
    .await
    .unwrap();
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN PASSWORD 'test-only'; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_fcm_partial_health(text,integer),public.novu_rust_reserve_alert_v2(text,text,text,integer,bigint),public.novu_rust_observe_alert_conditions_v2(text,text,text,bigint,bigint) TO {role}")).await.unwrap();
    let mut u = reqwest::Url::parse(&url).unwrap();
    u.set_username(&role).unwrap();
    u.set_password(Some("test-only")).unwrap();
    let role_url = u.as_str();
    assert_eq!(check(role_url, &env, "60").status.code(), Some(1));
    db.execute("INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT $1,rolname,p,oid FROM pg_roles CROSS JOIN unnest(ARRAY['alert_view','alert_send']) p WHERE rolname::text=$2",&[&env,&role]).await.unwrap();
    assert_eq!(check(role_url, &env, "60").status.code(), Some(0));
    assert_eq!(check(role_url, &other, "60").status.code(), Some(1));
    assert_eq!(check(role_url, &env, "0").status.code(), Some(1));
    let partial = Uuid::new_v4();
    let old = Uuid::new_v4();
    let failed = Uuid::new_v4();
    let foreign = Uuid::new_v4();
    db.execute("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status) VALUES($1,$5,'experimental_fcm_push','{}','completed'),($2,$5,'experimental_fcm_push','{}','completed'),($3,$5,'experimental_fcm_push','{}','failed'),($4,$6,'experimental_fcm_push','{}','completed')",&[&partial,&old,&failed,&foreign,&env,&other]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_fcm_attempts(job_id,environment_id,target_index,state,updated_at) VALUES($1,$5,0,'accepted',now()),($1,$5,1,'rejected',now()),($1,$5,2,'rejected',now()),($2,$5,0,'accepted',now()),($2,$5,1,'rejected',now()-interval '2 minutes'),($3,$5,0,'accepted',now()),($3,$5,1,'rejected',now()),($4,$6,0,'accepted',now()),($4,$6,1,'rejected',now())",&[&partial,&old,&failed,&foreign,&env,&other]).await.unwrap();
    let result = check(role_url, &env, "60");
    assert_eq!(result.status.code(), Some(2));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&result.stdout).unwrap(),
        serde_json::json!({"partialJobs":1})
    );
    let longer = check(role_url, &env, "180");
    assert_eq!(longer.status.code(), Some(2));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&longer.stdout).unwrap(),
        serde_json::json!({"partialJobs":2})
    );
    db.execute("INSERT INTO public.novu_rust_alert_delivery_slots(environment_id,channel,target_id,state) VALUES($1,'slack','blocked','rejected')",&[&env]).await.unwrap();
    assert_eq!(alert(role_url, &env).status.code(), Some(2));
    db.execute("DELETE FROM public.novu_rust_delivery_operator_grants WHERE environment_id=$1 AND database_principal=$2 AND decision='alert_view'",&[&env,&role]).await.unwrap();
    assert_eq!(check(role_url, &env, "60").status.code(), Some(1));
    assert_eq!(alert(role_url, &env).status.code(), Some(1));
}
