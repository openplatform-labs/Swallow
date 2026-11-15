//! Disposable PostgreSQL and real CLI shared-condition destination binding.
use std::process::{Command, Output};
use tokio_postgres::NoTls;
use uuid::Uuid;

fn run(url: &str, env: &str, check: &str) -> Output {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_send-delivery-alert"));
    cmd.env_clear()
        .env("DATABASE_URL", url)
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .env("SWALLOW_ALERT_COOLDOWN_POLICY", "new-condition")
        .env("SWALLOW_ALERT_REQUIRE_DISTINCT_SHARED_CHECKS", "true")
        .env("SWALLOW_ALERT_BIND_SHARED_CHECKS", "true")
        .env("SWALLOW_ALERT_RESEND_ENABLED", "false")
        .env(
            "SWALLOW_ALERT_SLACK_WEBHOOK",
            "https://hooks.slack.com/services/A/B/C",
        )
        .env("SWALLOW_ALERT_SLACK_TARGET_ID", "shared")
        .args([env, "300", "120", "60"]);
    match check {
        "slack" => {
            cmd.env("SWALLOW_ALERT_SLACK_COOLDOWN_GRACE_SECONDS", "60");
        }
        "invalid" => {
            cmd.env("SWALLOW_ALERT_SLACK_COOLDOWN_GRACE_SECONDS", "0");
        }
        "failed" => {
            cmd.env("SWALLOW_ALERT_FAILED_JOBS_ENABLED", "true");
        }
        _ => {}
    }
    cmd.output().unwrap()
}
#[tokio::test]
#[ignore = "requires fully migrated disposable PostgreSQL with role creation"]
async fn target_binding_denies_other_checks_and_enforces_role_scope() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let env = format!("shared-bind-{}", Uuid::new_v4());
    let other = format!("shared-other-{}", Uuid::new_v4());
    let role = format!("alert_bind_{}", Uuid::new_v4().simple());
    db.execute(
        "INSERT INTO public.novu_rust_environments VALUES($1,'org'),($2,'org')",
        &[&env, &other],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO public.novu_rust_slack_cooldown_policies(environment_id,fallback_seconds) VALUES($1,30)",&[&env]).await.unwrap();
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN PASSWORD 'test-only'; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_bind_alert_shared_check(text,text,text,text),public.novu_rust_slack_cooldown_health(text,integer),public.novu_rust_observe_alert_conditions(text,text,text,integer,integer) TO {role}")).await.unwrap();
    let mut role_url = reqwest::Url::parse(&url).unwrap();
    role_url.set_username(&role).unwrap();
    role_url.set_password(Some("test-only")).unwrap();
    assert!(!run(role_url.as_str(), &env, "slack").status.success());
    db.execute("INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT $1,$2,p,oid FROM pg_roles CROSS JOIN unnest(ARRAY['alert_view','alert_send']) p WHERE rolname::text=$2",&[&env,&role]).await.unwrap();
    let invalid = run(role_url.as_str(), &env, "invalid");
    assert!(!invalid.status.success());
    let before: i64 = db.query_one("SELECT count(*) FROM public.novu_rust_alert_shared_check_bindings WHERE environment_id=$1", &[&env]).await.unwrap().get(0);
    assert_eq!(before, 0, "invalid thresholds must not persist a binding");
    let ok = run(role_url.as_str(), &env, "slack");
    assert!(
        ok.status.success(),
        "{}",
        String::from_utf8_lossy(&ok.stderr)
    );
    let again = run(role_url.as_str(), &env, "slack");
    assert!(again.status.success());
    let conflict = run(role_url.as_str(), &env, "failed");
    assert!(!conflict.status.success());
    assert!(
        String::from_utf8_lossy(&conflict.stderr).contains("shared alert check binding failed")
    );
    assert!(!run(role_url.as_str(), &other, "slack").status.success());
    let row=db.query_one("SELECT check_name FROM public.novu_rust_alert_shared_check_bindings WHERE environment_id=$1 AND channel='slack' AND target_id='shared'",&[&env]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "slack-cooldown");
    db.execute("DELETE FROM public.novu_rust_delivery_operator_grants WHERE environment_id=$1 AND database_principal=$2 AND decision='alert_send'",&[&env,&role]).await.unwrap();
    assert!(!run(role_url.as_str(), &env, "slack").status.success());
}
