//! Versioned 64-bit alert conditions using a fully migrated disposable database.
use tokio_postgres::NoTls;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires fully migrated disposable PostgreSQL and role creation"]
async fn independent_bits_bypass_and_recover_without_access_to_other_environments() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (admin, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let environment = format!("alert-v2-{}", Uuid::new_v4());
    let other = format!("alert-v2-other-{}", Uuid::new_v4());
    let role = format!("alert_v2_{}", Uuid::new_v4().simple());
    admin
        .execute(
            "INSERT INTO public.novu_rust_environments VALUES($1,'org'),($2,'org')",
            &[&environment, &other],
        )
        .await
        .unwrap();
    admin.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_reserve_alert_v2(text,text,text,integer,bigint),public.novu_rust_observe_alert_conditions_v2(text,text,text,bigint,bigint),public.novu_rust_finish_alert(text,text,text,uuid,text,integer),public.novu_rust_reserve_alert(text,text,text,integer,integer) TO {role}")).await.unwrap();
    let (user, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    user.batch_execute(&format!("SET SESSION AUTHORIZATION {role}"))
        .await
        .unwrap();
    let reserve =
        "SELECT result,attempt_id FROM public.novu_rust_reserve_alert_v2($1,'slack','ops',60,$2)";
    let failed_jobs: i64 = 1 << 18;
    let resend: i64 = 1 << 32;
    let slack: i64 = 1 << 34;
    assert!(
        user.query_one(reserve, &[&environment, &failed_jobs])
            .await
            .is_err()
    );
    admin.execute("INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT $1,$2,'alert_send',oid FROM pg_roles WHERE rolname::text=$2", &[&environment,&role]).await.unwrap();
    assert!(
        user.query_one(reserve, &[&other, &failed_jobs])
            .await
            .is_err()
    );
    let old = user
        .query_one(
            "SELECT result FROM public.novu_rust_reserve_alert($1,'slack','legacy',60,1::integer)",
            &[&environment],
        )
        .await
        .unwrap();
    assert_eq!(old.get::<_, String>(0), "claimed");
    let mut mask = failed_jobs;
    for bit in [failed_jobs, resend, slack] {
        mask |= bit;
        let row = user
            .query_one(reserve, &[&environment, &mask])
            .await
            .unwrap();
        assert_eq!(row.get::<_, String>(0), "claimed");
        let id: Uuid = row.get(1);
        assert!(
            user.query_one(
                "SELECT public.novu_rust_finish_alert($1,'slack','ops',$2,'accepted',NULL)",
                &[&environment, &id]
            )
            .await
            .unwrap()
            .get::<_, bool>(0)
        );
        assert_eq!(
            user.query_one(reserve, &[&environment, &mask])
                .await
                .unwrap()
                .get::<_, String>(0),
            "cooldown"
        );
    }
    user.query_one(
        "SELECT public.novu_rust_observe_alert_conditions_v2($1,'slack','ops',$2,$3)",
        &[&environment, &failed_jobs, &(failed_jobs | resend)],
    )
    .await
    .unwrap();
    assert_eq!(
        user.query_one(reserve, &[&environment, &(failed_jobs | slack)])
            .await
            .unwrap()
            .get::<_, String>(0),
        "cooldown"
    );
    assert_eq!(
        user.query_one(reserve, &[&environment, &mask])
            .await
            .unwrap()
            .get::<_, String>(0),
        "claimed"
    );
    admin.execute("DELETE FROM public.novu_rust_delivery_operator_grants WHERE environment_id=$1 AND database_principal=$2 AND decision='alert_send'", &[&environment,&role]).await.unwrap();
    assert!(
        user.query_one(reserve, &[&environment, &mask])
            .await
            .is_err()
    );
}

#[tokio::test]
#[ignore = "requires fully migrated disposable PostgreSQL and role creation"]
async fn v2_cli_uses_restricted_observe_and_reserve_paths() {
    use std::process::Command;
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (admin, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let environment = format!("alert-v2-cli-{}", Uuid::new_v4());
    let role = format!("alert_v2_cli_{}", Uuid::new_v4().simple());
    admin
        .execute(
            "INSERT INTO public.novu_rust_environments VALUES($1,'org')",
            &[&environment],
        )
        .await
        .unwrap();
    admin.execute("INSERT INTO public.novu_rust_slack_cooldown_policies(environment_id,fallback_seconds,not_before) VALUES($1,30,clock_timestamp()+interval '2 minutes')",&[&environment]).await.unwrap();
    admin.execute("INSERT INTO public.novu_rust_alert_delivery_slots(environment_id,channel,target_id,state) VALUES($1,'slack','blocked','rejected')",&[&environment]).await.unwrap();
    admin.batch_execute(&format!("CREATE ROLE {role} LOGIN PASSWORD 'test-only'; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_reserve_alert_v2(text,text,text,integer,bigint),public.novu_rust_observe_alert_conditions_v2(text,text,text,bigint,bigint),public.novu_rust_slack_cooldown_health(text,integer) TO {role}")).await.unwrap();
    admin.execute("INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT $1,$2,p,oid FROM pg_roles CROSS JOIN unnest(ARRAY['alert_view','alert_send']) p WHERE rolname::text=$2",&[&environment,&role]).await.unwrap();
    let mut role_url = reqwest::Url::parse(&url).unwrap();
    role_url.set_username(&role).unwrap();
    role_url.set_password(Some("test-only")).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_send-delivery-alert"))
        .env_clear()
        .env("DATABASE_URL", role_url.as_str())
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .env("SWALLOW_ALERT_COOLDOWN_POLICY", "new-condition")
        .env("SWALLOW_ALERT_CONDITIONS_V2", "true")
        .env("SWALLOW_ALERT_SCOPED_DELIVERY", "true")
        .env("SWALLOW_ALERT_RESEND_ENABLED", "false")
        .env("SWALLOW_ALERT_SLACK_COOLDOWN_GRACE_SECONDS", "60")
        .env(
            "SWALLOW_ALERT_SLACK_WEBHOOK",
            "https://hooks.slack.com/services/A/B/C",
        )
        .env("SWALLOW_ALERT_SLACK_TARGET_ID", "blocked")
        .args([&environment, "300", "120", "60"])
        .output()
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(2),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stderr).contains("requires operator review"));
}
