use novu_postgres_queue::{self as queue, discord_queue};
use serde_json::{Value, json};
use std::path::Path;
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;

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
#[tokio::test]
#[ignore = "requires fresh disposable PostgreSQL and role creation"]
async fn cooldown_health_is_scoped_and_detects_overdue_work() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(driver);
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    migrate(&db, &root.join("../../api/rust-api/migrations"), 1..=17).await;
    migrate(&db, &root.join("migrations"), 1..=u32::MAX).await;
    let env = format!("health-{}", Uuid::new_v4());
    let other = format!("other-{}", Uuid::new_v4());
    let role = format!("cooldown_health_{}", Uuid::new_v4().simple());
    db.execute(
        "INSERT INTO public.novu_rust_environments VALUES($1,'org'),($2,'org')",
        &[&env, &other],
    )
    .await
    .unwrap();
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN PASSWORD 'fixture'; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_discord_cooldown_health(text,integer) TO {role}")).await.unwrap();
    let mut role_url = reqwest::Url::parse(&url).unwrap();
    role_url.set_username(&role).unwrap();
    role_url.set_password(Some("fixture")).unwrap();
    let (reader, driver) = tokio_postgres::connect(role_url.as_str(), NoTls)
        .await
        .unwrap();
    tokio::spawn(driver);
    assert!(
        reader
            .query_one(
                "SELECT * FROM public.novu_rust_discord_cooldown_health($1,60)",
                &[&env]
            )
            .await
            .is_err()
    );
    db.execute("INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT $1,rolname,'alert_view',oid FROM pg_roles WHERE rolname::text=$2",&[&env,&role]).await.unwrap();
    assert!(
        reader
            .query_one(
                "SELECT * FROM public.novu_rust_discord_cooldown_health($1,60)",
                &[&other]
            )
            .await
            .is_err()
    );
    assert!(
        reader
            .query_one(
                "SELECT * FROM public.novu_rust_discord_cooldown_health($1,0)",
                &[&env]
            )
            .await
            .is_err()
    );
    db.execute("INSERT INTO public.novu_rust_discord_shared_cooldown(environment_id,destination_id,not_before) VALUES($1,'a',clock_timestamp()+interval '1 hour'),($1,'b',clock_timestamp()-interval '1 hour'),($2,'a',clock_timestamp()+interval '1 hour')",&[&env,&other]).await.unwrap();
    let pending = Uuid::new_v4();
    let overdue = Uuid::new_v4();
    let foreign = Uuid::new_v4();
    let unrelated = Uuid::new_v4();
    for (id, which, kind) in [
        (pending, &env, discord_queue::JOB_KIND),
        (overdue, &env, discord_queue::WORKFLOW_KIND),
        (foreign, &other, discord_queue::JOB_KIND),
        (unrelated, &env, "other"),
    ] {
        queue::enqueue(
            &db,
            id,
            which,
            kind,
            &json!({"destination":"a","content":"fixture"}),
        )
        .await
        .unwrap();
    }
    db.execute("UPDATE public.novu_queue_jobs SET last_error='Discord shared cooldown active',available_at=CASE WHEN id=$1 THEN clock_timestamp()-interval '2 minutes' ELSE clock_timestamp()+interval '1 hour' END WHERE id=ANY($2::uuid[])",&[&overdue,&vec![pending,overdue,foreign,unrelated]]).await.unwrap();
    let row = reader
        .query_one(
            "SELECT * FROM public.novu_rust_discord_cooldown_health($1,60)",
            &[&env],
        )
        .await
        .unwrap();
    assert_eq!(
        (
            row.get::<_, i64>(0),
            row.get::<_, i64>(1),
            row.get::<_, i64>(2)
        ),
        (1, 2, 1)
    );
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
        .env_clear()
        .env("DATABASE_URL", role_url.as_str())
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .args(["check-discord-cooldown", &env, "60"])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    let value: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        value,
        json!({"activeDestinations":1,"deferred":2,"overdue":1})
    );
    db.batch_execute(&format!("GRANT EXECUTE ON FUNCTION public.novu_rust_reserve_alert_v2(text,text,text,integer,bigint),public.novu_rust_observe_alert_conditions_v2(text,text,text,bigint,bigint),public.novu_rust_finish_alert(text,text,text,uuid,text,integer) TO {role}")).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_delivery_operator_grants SELECT $1,rolname,'alert_send',oid FROM pg_roles WHERE rolname::text=$2", &[&env,&role]).await.unwrap();
    // PostgreSQL bigint bit 63 is negative; test the real reservation and recovery protocol.
    let first=reader.query_one("SELECT result,attempt_id FROM public.novu_rust_reserve_alert_v2($1,'slack','signed',60,$2)",&[&env,&(1_i64<<62)]).await.unwrap();
    assert_eq!(first.get::<_, String>(0), "claimed");
    let attempt: Uuid = first.get(1);
    assert!(
        reader
            .query_one(
                "SELECT public.novu_rust_finish_alert($1,'slack','signed',$2,'accepted',NULL)",
                &[&env, &attempt]
            )
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    let failure=reader.query_one("SELECT result,attempt_id FROM public.novu_rust_reserve_alert_v2($1,'slack','signed',60,$2)",&[&env,&i64::MIN]).await.unwrap();
    assert_eq!(failure.get::<_, String>(0), "claimed");
    let attempt: Uuid = failure.get(1);
    assert!(
        reader
            .query_one(
                "SELECT public.novu_rust_finish_alert($1,'slack','signed',$2,'accepted',NULL)",
                &[&env, &attempt]
            )
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    reader
        .query_one(
            "SELECT public.novu_rust_observe_alert_conditions_v2($1,'slack','signed',$2,$3)",
            &[&env, &0_i64, &i64::MIN],
        )
        .await
        .unwrap();
    let repeat = reader
        .query_one(
            "SELECT result FROM public.novu_rust_reserve_alert_v2($1,'slack','signed',60,$2)",
            &[&env, &i64::MIN],
        )
        .await
        .unwrap();
    assert_eq!(repeat.get::<_, String>(0), "claimed");
    db.execute("INSERT INTO public.novu_rust_alert_delivery_slots(environment_id,channel,target_id,state) VALUES($1,'slack','blocked','rejected')", &[&env]).await.unwrap();
    let alert = std::process::Command::new(env!("CARGO_BIN_EXE_send-delivery-alert"))
        .env_clear()
        .env("DATABASE_URL", role_url.as_str())
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .env("SWALLOW_ALERT_RESEND_ENABLED", "false")
        .env("SWALLOW_ALERT_DISCORD_COOLDOWN_GRACE_SECONDS", "60")
        .env("SWALLOW_ALERT_SCOPED_DELIVERY", "true")
        .env("SWALLOW_ALERT_COOLDOWN_POLICY", "new-condition")
        .env("SWALLOW_ALERT_CONDITIONS_V2", "true")
        .env(
            "SWALLOW_ALERT_SLACK_WEBHOOK",
            "https://hooks.slack.com/services/A/B/C",
        )
        .env("SWALLOW_ALERT_SLACK_TARGET_ID", "blocked")
        .args([&env, "300", "120", "60"])
        .output()
        .unwrap();
    assert_eq!(
        alert.status.code(),
        Some(2),
        "{}",
        String::from_utf8_lossy(&alert.stderr)
    );
    assert!(String::from_utf8_lossy(&alert.stderr).contains("requires operator review"));
    db.execute(
        "UPDATE public.novu_queue_jobs SET status='completed' WHERE id=ANY($1::uuid[])",
        &[&vec![pending, overdue]],
    )
    .await
    .unwrap();
    db.execute("UPDATE public.novu_rust_discord_shared_cooldown SET not_before=clock_timestamp()-interval '1 second' WHERE environment_id=$1",&[&env]).await.unwrap();
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
        .env_clear()
        .env("DATABASE_URL", role_url.as_str())
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .args(["check-discord-cooldown", &env, "60"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let healthy = std::process::Command::new(env!("CARGO_BIN_EXE_send-delivery-alert"))
        .env_clear()
        .env("DATABASE_URL", role_url.as_str())
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .env("SWALLOW_ALERT_RESEND_ENABLED", "false")
        .env("SWALLOW_ALERT_DISCORD_COOLDOWN_GRACE_SECONDS", "60")
        .env("SWALLOW_ALERT_SCOPED_DELIVERY", "true")
        .env("SWALLOW_ALERT_COOLDOWN_POLICY", "new-condition")
        .env("SWALLOW_ALERT_CONDITIONS_V2", "true")
        .env(
            "SWALLOW_ALERT_SLACK_WEBHOOK",
            "https://hooks.slack.com/services/A/B/C",
        )
        .env("SWALLOW_ALERT_SLACK_TARGET_ID", "blocked")
        .args([&env, "300", "120", "60"])
        .output()
        .unwrap();
    assert!(
        healthy.status.success(),
        "{}",
        String::from_utf8_lossy(&healthy.stderr)
    );
    db.execute("DELETE FROM public.novu_rust_delivery_operator_grants WHERE environment_id=$1 AND database_principal::text=$2 AND decision='alert_view'",&[&env,&role]).await.unwrap();
    assert!(
        reader
            .query_one(
                "SELECT * FROM public.novu_rust_discord_cooldown_health($1,60)",
                &[&env]
            )
            .await
            .is_err()
    );
    let denied = std::process::Command::new(env!("CARGO_BIN_EXE_send-delivery-alert"))
        .env_clear()
        .env("DATABASE_URL", role_url.as_str())
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .env("SWALLOW_ALERT_RESEND_ENABLED", "false")
        .env("SWALLOW_ALERT_DISCORD_COOLDOWN_GRACE_SECONDS", "60")
        .env("SWALLOW_ALERT_SCOPED_DELIVERY", "true")
        .env("SWALLOW_ALERT_COOLDOWN_POLICY", "new-condition")
        .env("SWALLOW_ALERT_CONDITIONS_V2", "true")
        .env(
            "SWALLOW_ALERT_SLACK_WEBHOOK",
            "https://hooks.slack.com/services/A/B/C",
        )
        .env("SWALLOW_ALERT_SLACK_TARGET_ID", "blocked")
        .args([&env, "300", "120", "60"])
        .output()
        .unwrap();
    assert_eq!(
        denied.status.code(),
        Some(1),
        "{}",
        String::from_utf8_lossy(&denied.stderr)
    );
    assert!(
        String::from_utf8_lossy(&denied.stderr).contains("Discord cooldown health check failed")
    );
}
