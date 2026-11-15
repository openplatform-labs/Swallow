use serde_json::{Value, json};
use tokio_postgres::NoTls;
use uuid::Uuid;
#[tokio::test]
#[ignore = "requires fully migrated disposable PostgreSQL and role creation"]
async fn scoped_credential_health_prevents_spoofing_and_detects_failure() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(driver);
    let env = format!("smtp-cred-{}", Uuid::new_v4());
    let other = format!("smtp-other-{}", Uuid::new_v4());
    let worker = format!("smtp_cw_{}", Uuid::new_v4().simple());
    let monitor = format!("smtp_cm_{}", Uuid::new_v4().simple());
    db.execute(
        "INSERT INTO public.novu_rust_environments VALUES($1,'org'),($2,'org')",
        &[&env, &other],
    )
    .await
    .unwrap();
    db.batch_execute(&format!("CREATE ROLE {worker} LOGIN; CREATE ROLE {monitor} LOGIN; GRANT USAGE ON SCHEMA public TO {worker},{monitor}; GRANT EXECUTE ON FUNCTION public.novu_rust_smtp_credential_heartbeat(text,uuid,text) TO {worker}; GRANT EXECUTE ON FUNCTION public.novu_rust_smtp_credential_health_check(text,integer) TO {monitor}")).await.unwrap();
    let login = |role: &str| {
        let mut u = reqwest::Url::parse(&url).unwrap();
        u.set_username(role).unwrap();
        u.to_string()
    };
    let (writer, driver) = tokio_postgres::connect(&login(&worker), NoTls)
        .await
        .unwrap();
    tokio::spawn(driver);
    let (reader, driver) = tokio_postgres::connect(&login(&monitor), NoTls)
        .await
        .unwrap();
    tokio::spawn(driver);
    let instance = Uuid::new_v4();
    assert!(
        writer
            .query_one(
                "SELECT public.novu_rust_smtp_credential_heartbeat($1,$2,'healthy')",
                &[&env, &instance]
            )
            .await
            .is_err()
    );
    db.execute("INSERT INTO public.novu_rust_smtp_credential_workers SELECT $1,rolname,oid FROM pg_roles WHERE rolname::text=$2",&[&env,&worker]).await.unwrap();
    assert!(
        writer
            .query_one(
                "SELECT public.novu_rust_smtp_credential_heartbeat($1,$2,'healthy')",
                &[&other, &instance]
            )
            .await
            .is_err()
    );
    assert!(
        writer
            .query_one(
                "SELECT public.novu_rust_smtp_credential_heartbeat($1,$2,'healthy')",
                &[&env, &instance]
            )
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    assert!(
        reader
            .query_one(
                "SELECT * FROM public.novu_rust_smtp_credential_health_check($1,60)",
                &[&env]
            )
            .await
            .is_err()
    );
    db.execute("INSERT INTO public.novu_rust_delivery_operator_grants SELECT $1,rolname,'alert_view',oid FROM pg_roles WHERE rolname::text=$2",&[&env,&monitor]).await.unwrap();
    assert!(
        reader
            .query_one(
                "SELECT * FROM public.novu_rust_smtp_credential_health_check($1,60)",
                &[&other]
            )
            .await
            .is_err()
    );
    let row = reader
        .query_one(
            "SELECT * FROM public.novu_rust_smtp_credential_health_check($1,60)",
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
        (1, 0, 0)
    );
    let (spoof, driver) = tokio_postgres::connect(&login(&worker), NoTls)
        .await
        .unwrap();
    tokio::spawn(driver);
    assert!(
        !spoof
            .query_one(
                "SELECT public.novu_rust_smtp_credential_heartbeat($1,$2,'healthy')",
                &[&env, &instance]
            )
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    writer
        .query_one(
            "SELECT public.novu_rust_smtp_credential_heartbeat($1,$2,'unhealthy')",
            &[&env, &instance],
        )
        .await
        .unwrap();
    let run = || {
        std::process::Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
            .env_clear()
            .env("DATABASE_URL", login(&monitor))
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .args(["check-smtp-credentials", &env, "60", "1"])
            .output()
            .unwrap()
    };
    let out = run();
    assert_eq!(out.status.code(), Some(2));
    let value: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(
        value,
        json!({"healthy":0,"unhealthy":1,"stale":0,"minimumHealthy":1})
    );
    db.batch_execute(&format!("GRANT EXECUTE ON FUNCTION public.novu_rust_reserve_alert(text,text,text,integer,integer) TO {monitor}")).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_delivery_operator_grants SELECT $1,rolname,'alert_send',oid FROM pg_roles WHERE rolname::text=$2",&[&env,&monitor]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_alert_delivery_slots(environment_id,channel,target_id,state) VALUES($1,'slack','blocked','rejected')",&[&env]).await.unwrap();
    let alert = |policy: &str| {
        std::process::Command::new(env!("CARGO_BIN_EXE_send-delivery-alert"))
            .env_clear()
            .env("DATABASE_URL", login(&monitor))
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("SWALLOW_ALERT_RESEND_ENABLED", "false")
            .env("SWALLOW_ALERT_SMTP_CREDENTIAL_MAX_AGE_SECONDS", "60")
            .env("SWALLOW_ALERT_SMTP_CREDENTIAL_MIN_HEALTHY", "1")
            .env("SWALLOW_ALERT_SCOPED_DELIVERY", "true")
            .env("SWALLOW_ALERT_COOLDOWN_POLICY", policy)
            .env(
                "SWALLOW_ALERT_SLACK_WEBHOOK",
                "https://hooks.slack.com/services/A/B/C",
            )
            .env("SWALLOW_ALERT_SLACK_TARGET_ID", "blocked")
            .args([&env, "300", "120", "60"])
            .output()
            .unwrap()
    };
    let blocked = alert("destination");
    assert_eq!(
        blocked.status.code(),
        Some(2),
        "{}",
        String::from_utf8_lossy(&blocked.stderr)
    );
    assert!(String::from_utf8_lossy(&blocked.stderr).contains("requires operator review"));
    let invalid = alert("new-condition");
    assert_eq!(invalid.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&invalid.stderr).contains("requires destination cooldown"));
    writer
        .query_one(
            "SELECT public.novu_rust_smtp_credential_heartbeat($1,$2,'healthy')",
            &[&env, &instance],
        )
        .await
        .unwrap();
    assert!(run().status.success());
    assert!(alert("destination").status.success());
    drop(writer);
    let mut disconnected = false;
    for _ in 0..100 {
        disconnected = reader
            .query_one(
                "SELECT healthy=0 FROM public.novu_rust_smtp_credential_health_check($1,60)",
                &[&env],
            )
            .await
            .unwrap()
            .get(0);
        if disconnected {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(disconnected);
    assert_eq!(run().status.code(), Some(2));
    db.execute("UPDATE public.novu_rust_smtp_credential_health SET observed_at=clock_timestamp()-interval '2 minutes' WHERE instance_id=$1",&[&instance]).await.unwrap();
    let out = run();
    assert_eq!(out.status.code(), Some(2));
    assert_eq!(
        serde_json::from_slice::<Value>(&out.stdout).unwrap()["stale"],
        1
    );
    db.execute("DELETE FROM public.novu_rust_delivery_operator_grants WHERE environment_id=$1 AND database_principal=$2 AND decision='alert_view'",&[&env,&monitor]).await.unwrap();
    assert!(
        reader
            .query_one(
                "SELECT * FROM public.novu_rust_smtp_credential_health_check($1,60)",
                &[&env]
            )
            .await
            .is_err()
    );
}
