use novu_postgres_queue as queue;
use serde_json::json;
use std::{path::Path, process::Command};
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;

async fn migrate(db: &Client, dir: &Path, range: std::ops::RangeInclusive<u32>) {
    let mut files: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
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
fn alert(url: &str, env: &str, v2: bool) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_send-delivery-alert"))
        .env_clear()
        .env("DATABASE_URL", url)
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .env("SWALLOW_ALERT_RESEND_ENABLED", "false")
        .env("SWALLOW_ALERT_DISCORD_ENABLED", "true")
        .env("SWALLOW_ALERT_SCOPED_DELIVERY", "true")
        .env(
            "SWALLOW_ALERT_COOLDOWN_POLICY",
            if v2 { "new-condition" } else { "destination" },
        )
        .env(
            "SWALLOW_ALERT_CONDITIONS_V2",
            if v2 { "true" } else { "false" },
        )
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
#[ignore = "requires fresh disposable PostgreSQL and role creation"]
async fn scoped_health_and_alert_selection() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(driver);
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    migrate(&db, &root.join("../../api/rust-api/migrations"), 1..=17).await;
    migrate(&db, &root.join("migrations"), 1..=u32::MAX).await;
    let env = format!("discord-health-{}", Uuid::new_v4());
    let other = format!("discord-other-{}", Uuid::new_v4());
    let role = format!("discord_health_{}", Uuid::new_v4().simple());
    db.execute(
        "INSERT INTO public.novu_rust_environments VALUES($1,'org'),($2,'org')",
        &[&env, &other],
    )
    .await
    .unwrap();
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN PASSWORD 'test-only'; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_discord_health(text),public.novu_rust_reserve_alert(text,text,text,integer,integer),public.novu_rust_reserve_alert_v2(text,text,text,integer,bigint),public.novu_rust_observe_alert_conditions_v2(text,text,text,bigint,bigint) TO {role}")).await.unwrap();
    let mut role_url = reqwest::Url::parse(&url).unwrap();
    role_url.set_username(&role).unwrap();
    role_url.set_password(Some("test-only")).unwrap();
    let (reader, driver) = tokio_postgres::connect(role_url.as_str(), NoTls)
        .await
        .unwrap();
    tokio::spawn(driver);
    assert!(
        reader
            .query_one("SELECT * FROM public.novu_rust_discord_health($1)", &[&env])
            .await
            .is_err()
    );
    db.execute("INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT $1,$2,p,oid FROM pg_roles CROSS JOIN unnest(ARRAY['alert_view','alert_send']) p WHERE rolname::text=$2", &[&env,&role]).await.unwrap();
    assert!(
        reader
            .query_one(
                "SELECT * FROM public.novu_rust_discord_health($1)",
                &[&other]
            )
            .await
            .is_err()
    );
    assert!(
        reader
            .query_one(
                "SELECT * FROM public.novu_rust_discord_attempts LIMIT 1",
                &[]
            )
            .await
            .is_err()
    );
    assert_eq!(alert(role_url.as_str(), &env, false).status.code(), Some(0));
    let check = |url: &str, env: &str| {
        Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
            .env_clear()
            .env("DATABASE_URL", url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .args(["check-discord", env])
            .output()
            .unwrap()
    };
    assert_eq!(check(role_url.as_str(), &env).status.code(), Some(0));
    assert_eq!(check(role_url.as_str(), &other).status.code(), Some(1));

    let ids: Vec<Uuid> = (0..5).map(|_| Uuid::new_v4()).collect();
    for (id, e) in [
        (ids[0], &env),
        (ids[1], &env),
        (ids[2], &env),
        (ids[3], &env),
        (ids[4], &other),
    ] {
        queue::enqueue(
            &db,
            id,
            e,
            queue::discord_queue::WORKFLOW_KIND,
            &json!({"destination":"x","secret":"fixture"}),
        )
        .await
        .unwrap();
    }
    db.execute(
        "UPDATE public.novu_queue_jobs SET status='failed' WHERE id=ANY($1)",
        &[&vec![ids[0], ids[1], ids[3], ids[4]]],
    )
    .await
    .unwrap();
    db.execute("UPDATE public.novu_queue_jobs SET status='running',lease_token=$2,lease_until=clock_timestamp()-interval '1 second' WHERE id=$1", &[&ids[2],&Uuid::new_v4()]).await.unwrap();
    for (id, e, state) in [
        (ids[0], &env, "unknown"),
        (ids[1], &env, "rejected"),
        (ids[2], &env, "dispatching"),
        (ids[4], &other, "unknown"),
    ] {
        db.execute("INSERT INTO public.novu_rust_discord_attempts(job_id,environment_id,state) VALUES($1,$2,$3)",&[&id,e,&state]).await.unwrap();
    }
    let row = reader
        .query_one("SELECT * FROM public.novu_rust_discord_health($1)", &[&env])
        .await
        .unwrap();
    assert_eq!(
        (
            row.get::<_, i64>(0),
            row.get::<_, i64>(1),
            row.get::<_, i64>(2),
            row.get::<_, i64>(3)
        ),
        (1, 1, 1, 1)
    );
    let report = check(role_url.as_str(), &env);
    assert_eq!(report.status.code(), Some(2));
    let json: serde_json::Value = serde_json::from_slice(&report.stdout).unwrap();
    assert_eq!(
        json,
        json!({"unknownAttempts":1,"rejectedAttempts":1,"strandedDispatches":1,"preDispatchFailures":1})
    );
    assert!(!String::from_utf8_lossy(&report.stdout).contains("fixture"));
    db.execute("INSERT INTO public.novu_rust_alert_delivery_slots(environment_id,channel,target_id,state) VALUES($1,'slack','blocked','rejected')", &[&env]).await.unwrap();
    for v2 in [false, true] {
        let result = alert(role_url.as_str(), &env, v2);
        assert_eq!(
            result.status.code(),
            Some(2),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        assert!(String::from_utf8_lossy(&result.stderr).contains("requires operator review"));
    }
    db.batch_execute(&format!("GRANT EXECUTE ON FUNCTION public.novu_rust_discord_attention(text,uuid),public.novu_rust_ack_discord(text,uuid,uuid,bigint,text,text) TO {role}")).await.unwrap();
    let attention = reader
        .query(
            "SELECT * FROM public.novu_rust_discord_attention($1,NULL)",
            &[&env],
        )
        .await
        .unwrap();
    assert_eq!(attention.len(), 4);
    let listed = Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
        .env_clear()
        .env("DATABASE_URL", role_url.as_str())
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .args(["list-discord", &env])
        .output()
        .unwrap();
    assert_eq!(listed.status.code(), Some(0));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&listed.stdout).unwrap()["data"]
            .as_array()
            .unwrap()
            .len(),
        4
    );
    assert!(!String::from_utf8_lossy(&listed.stdout).contains("fixture"));

    let unknown = attention
        .iter()
        .find(|r| r.get::<_, Uuid>(0) == ids[0])
        .unwrap();
    let revision: Uuid = unknown.get::<_, Option<Uuid>>(4).unwrap();
    let failure: i64 = unknown.get(5);
    let attempt = Some(revision);
    assert!(
        reader
            .query_one(
                "SELECT public.novu_rust_ack_discord($1,$2,$3,$4,'operator','checked')",
                &[&env, &ids[0], &attempt, &failure]
            )
            .await
            .is_err()
    );
    db.execute("INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT $1,$2,'queue_ack',oid FROM pg_roles WHERE rolname::text=$2", &[&env,&role]).await.unwrap();
    let stranded = attention
        .iter()
        .find(|r| r.get::<_, Uuid>(0) == ids[2])
        .unwrap();
    let stranded_revision: Option<Uuid> = stranded.get(4);
    let stranded_failure: i64 = stranded.get(5);
    assert!(
        reader
            .query_one(
                "SELECT public.novu_rust_ack_discord($1,$2,$3,$4,'operator','checked')",
                &[&env, &ids[2], &stranded_revision, &stranded_failure]
            )
            .await
            .is_err()
    );
    db.batch_execute("CREATE FUNCTION public.discord_review_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'test audit failure'; END $$; CREATE TRIGGER discord_review_fail BEFORE INSERT ON public.novu_rust_discord_reviews FOR EACH ROW EXECUTE FUNCTION public.discord_review_fail()").await.unwrap();
    assert!(
        reader
            .query_one(
                "SELECT public.novu_rust_ack_discord($1,$2,$3,$4,'operator','checked')",
                &[&env, &ids[0], &attempt, &failure]
            )
            .await
            .is_err()
    );
    assert_eq!(
        db.query_one("SELECT count(*) FROM public.novu_rust_discord_reviews", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );
    db.batch_execute("DROP TRIGGER discord_review_fail ON public.novu_rust_discord_reviews; DROP FUNCTION public.discord_review_fail()").await.unwrap();
    let cli = Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
        .env_clear()
        .env("DATABASE_URL", role_url.as_str())
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .args([
            "ack-discord",
            &env,
            &ids[0].to_string(),
            &revision.to_string(),
            &failure.to_string(),
            "operator",
            "checked",
        ])
        .output()
        .unwrap();
    assert_eq!(
        cli.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&cli.stderr)
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&cli.stdout).unwrap()["recorded"],
        true
    );
    assert!(
        !reader
            .query_one(
                "SELECT public.novu_rust_ack_discord($1,$2,$3,$4,'operator','checked')",
                &[&env, &ids[0], &attempt, &failure]
            )
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    let row = reader
        .query_one("SELECT * FROM public.novu_rust_discord_health($1)", &[&env])
        .await
        .unwrap();
    assert_eq!(row.get::<_, i64>(0), 0);
    db.execute(
        "UPDATE public.novu_rust_discord_attempts SET state='rejected' WHERE job_id=$1",
        &[&ids[0]],
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE public.novu_rust_discord_attempts SET state='unknown' WHERE job_id=$1",
        &[&ids[0]],
    )
    .await
    .unwrap();
    assert!(
        reader
            .query_one(
                "SELECT public.novu_rust_ack_discord($1,$2,$3,$4,'operator','checked')",
                &[&env, &ids[0], &attempt, &failure]
            )
            .await
            .is_err()
    );
    let row = reader
        .query_one("SELECT * FROM public.novu_rust_discord_health($1)", &[&env])
        .await
        .unwrap();
    assert_eq!(row.get::<_, i64>(0), 1);
    let attention = reader
        .query(
            "SELECT * FROM public.novu_rust_discord_attention($1,NULL)",
            &[&env],
        )
        .await
        .unwrap();
    let current = attention
        .iter()
        .find(|r| r.get::<_, Uuid>(0) == ids[0])
        .unwrap();
    let current_revision: Option<Uuid> = current.get(4);
    let current_failure: i64 = current.get(5);
    let (reader2, driver) = tokio_postgres::connect(role_url.as_str(), NoTls)
        .await
        .unwrap();
    tokio::spawn(driver);
    let params: &[&(dyn tokio_postgres::types::ToSql + Sync)] =
        &[&env, &ids[0], &current_revision, &current_failure];
    let sql = "SELECT public.novu_rust_ack_discord($1,$2,$3,$4,'operator','checked')";
    let (one, two) = tokio::join!(
        reader.query_one(sql, params),
        reader2.query_one(sql, params)
    );
    assert_ne!(
        one.unwrap().get::<_, bool>(0),
        two.unwrap().get::<_, bool>(0)
    );
    let pre = attention
        .iter()
        .find(|r| r.get::<_, Uuid>(0) == ids[3])
        .unwrap();
    let no_attempt: Option<Uuid> = None;
    let pre_rev: i64 = pre.get(5);
    assert!(
        reader
            .query_one(sql, &[&env, &ids[3], &no_attempt, &pre_rev])
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    let row = reader
        .query_one("SELECT * FROM public.novu_rust_discord_health($1)", &[&env])
        .await
        .unwrap();
    assert_eq!(
        (
            row.get::<_, i64>(0),
            row.get::<_, i64>(1),
            row.get::<_, i64>(2),
            row.get::<_, i64>(3)
        ),
        (0, 1, 1, 0)
    );
    assert_eq!(
        db.query_one("SELECT count(*) FROM public.novu_rust_discord_reviews", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        3
    );
    db.execute("DELETE FROM public.novu_rust_delivery_operator_grants WHERE environment_id=$1 AND database_principal=$2 AND decision='alert_view'", &[&env,&role]).await.unwrap();
    let result = alert(role_url.as_str(), &env, false);
    assert_eq!(result.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&result.stderr).contains("Discord health check failed"));
}
