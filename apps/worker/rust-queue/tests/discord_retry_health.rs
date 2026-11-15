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
#[tokio::test]
#[ignore = "requires fresh disposable PostgreSQL and role creation"]
async fn retry_health_is_scoped_and_alerts_only_overdue_jobs() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(driver);
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    migrate(&db, &root.join("../../api/rust-api/migrations"), 1..=17).await;
    migrate(&db, &root.join("migrations"), 1..=u32::MAX).await;
    let env = format!("retry-health-{}", Uuid::new_v4());
    let other = format!("other-{}", Uuid::new_v4());
    let role = format!("retry_health_{}", Uuid::new_v4().simple());
    db.execute(
        "INSERT INTO public.novu_rust_environments VALUES($1,'org'),($2,'org')",
        &[&env, &other],
    )
    .await
    .unwrap();
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN PASSWORD 'test-only'; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_discord_retry_health(text,integer),public.novu_rust_discord_retry_list(text,integer,boolean,uuid),public.novu_rust_reserve_alert(text,text,text,integer,integer),public.novu_rust_reserve_alert_v2(text,text,text,integer,bigint),public.novu_rust_observe_alert_conditions_v2(text,text,text,bigint,bigint) TO {role}")).await.unwrap();
    let mut role_url = reqwest::Url::parse(&url).unwrap();
    role_url.set_username(&role).unwrap();
    role_url.set_password(Some("test-only")).unwrap();
    let (reader, driver) = tokio_postgres::connect(role_url.as_str(), NoTls)
        .await
        .unwrap();
    tokio::spawn(driver);
    assert!(
        reader
            .query_one(
                "SELECT * FROM public.novu_rust_discord_retry_health($1,60)",
                &[&env]
            )
            .await
            .is_err()
    );
    db.execute("INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT $1,$2,p,oid FROM pg_roles CROSS JOIN unnest(ARRAY['alert_view','alert_send']) p WHERE rolname::text=$2",&[&env,&role]).await.unwrap();
    assert!(
        reader
            .query_one(
                "SELECT * FROM public.novu_rust_discord_retry_health($1,60)",
                &[&other]
            )
            .await
            .is_err()
    );
    assert!(
        reader
            .query_one(
                "SELECT * FROM public.novu_rust_discord_retry_health($1,0)",
                &[&env]
            )
            .await
            .is_err()
    );
    assert!(
        reader
            .query("SELECT * FROM public.novu_rust_discord_retry_history", &[])
            .await
            .is_err()
    );
    let ids: Vec<_> = (0..4).map(|_| Uuid::new_v4()).collect();
    for (id, e) in [
        (ids[0], &env),
        (ids[1], &env),
        (ids[2], &env),
        (ids[3], &other),
    ] {
        queue::enqueue(
            &db,
            id,
            e,
            queue::discord_queue::JOB_KIND,
            &json!({"destination":"private","content":"secret"}),
        )
        .await
        .unwrap();
        db.execute("INSERT INTO public.novu_rust_discord_retry_history(job_id,environment_id,destination_id,scheduled_delay_seconds) VALUES($1,$2,'private',10)",&[&id,e]).await.unwrap();
    }
    db.execute("UPDATE public.novu_queue_jobs SET available_at=clock_timestamp()+interval '5 minutes' WHERE id=$1",&[&ids[0]]).await.unwrap();
    db.execute("UPDATE public.novu_queue_jobs SET available_at=clock_timestamp()-interval '2 minutes' WHERE id=ANY($1)",&[&vec![ids[1],ids[3]]]).await.unwrap();
    db.execute("UPDATE public.novu_queue_jobs SET status='running',lease_token=$2,lease_until=clock_timestamp()+interval '1 minute' WHERE id=$1",&[&ids[2],&Uuid::new_v4()]).await.unwrap();
    let row = reader
        .query_one(
            "SELECT * FROM public.novu_rust_discord_retry_health($1,60)",
            &[&env],
        )
        .await
        .unwrap();
    assert_eq!((row.get::<_, i64>(0), row.get::<_, i64>(1)), (2, 1));
    let overdue = reader
        .query(
            "SELECT * FROM public.novu_rust_discord_retry_list($1,60,true,NULL)",
            &[&env],
        )
        .await
        .unwrap();
    assert_eq!(overdue.len(), 1);
    assert_eq!(overdue[0].get::<_, Uuid>(0), ids[1]);
    assert_eq!(overdue[0].get::<_, i64>(5), 1);
    assert!(overdue[0].get::<_, bool>(8));
    assert!(
        reader
            .query(
                "SELECT * FROM public.novu_rust_discord_retry_list($1,60,false,NULL)",
                &[&other]
            )
            .await
            .is_err()
    );

    let report = Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
        .env_clear()
        .env("DATABASE_URL", role_url.as_str())
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .args(["check-discord-retry", &env, "60"])
        .output()
        .unwrap();
    assert_eq!(report.status.code(), Some(2));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&report.stdout).unwrap(),
        json!({"pending":2,"overdue":1})
    );
    assert!(!String::from_utf8_lossy(&report.stdout).contains("private"));
    db.execute("INSERT INTO public.novu_rust_alert_delivery_slots(environment_id,channel,target_id,state) VALUES($1,'slack','blocked','rejected')",&[&env]).await.unwrap();
    for v2 in [false, true] {
        let report = Command::new(env!("CARGO_BIN_EXE_send-delivery-alert"))
            .env_clear()
            .env("DATABASE_URL", role_url.as_str())
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("SWALLOW_ALERT_RESEND_ENABLED", "false")
            .env("SWALLOW_ALERT_DISCORD_RETRY_GRACE_SECONDS", "60")
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
            .args([&env, "300", "120", "60"])
            .output()
            .unwrap();
        assert_eq!(
            report.status.code(),
            Some(2),
            "{}",
            String::from_utf8_lossy(&report.stderr)
        );
        assert!(String::from_utf8_lossy(&report.stderr).contains("requires operator review"));
    }
    db.execute("UPDATE public.novu_queue_jobs SET available_at=clock_timestamp()+interval '5 minutes' WHERE id=$1",&[&ids[1]]).await.unwrap();
    let row = reader
        .query_one(
            "SELECT * FROM public.novu_rust_discord_retry_health($1,60)",
            &[&env],
        )
        .await
        .unwrap();
    assert_eq!((row.get::<_, i64>(0), row.get::<_, i64>(1)), (2, 0));
    let healthy = Command::new(env!("CARGO_BIN_EXE_send-delivery-alert"))
        .env_clear()
        .env("DATABASE_URL", role_url.as_str())
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .env("SWALLOW_ALERT_RESEND_ENABLED", "false")
        .env("SWALLOW_ALERT_DISCORD_RETRY_GRACE_SECONDS", "60")
        .env("SWALLOW_ALERT_SCOPED_DELIVERY", "true")
        .env(
            "SWALLOW_ALERT_SLACK_WEBHOOK",
            "https://hooks.slack.com/services/A/B/C",
        )
        .env("SWALLOW_ALERT_SLACK_TARGET_ID", "blocked")
        .args([&env, "300", "120", "60"])
        .output()
        .unwrap();
    assert_eq!(healthy.status.code(), Some(0));
    assert!(String::from_utf8_lossy(&healthy.stdout).contains("healthy"));
    db.execute("UPDATE public.novu_queue_jobs SET payload=jsonb_build_object('destination','payload-secret') WHERE id=$1",&[&ids[0]]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_discord_retry_history(job_id,environment_id,destination_id,retry_after_seconds,scheduled_delay_seconds) VALUES($1,$2,'private',9,11)",&[&ids[0],&env]).await.unwrap();
    let row = reader
        .query_one(
            "SELECT * FROM public.novu_rust_discord_retry_list($1,60,false,NULL) WHERE job_id=$2",
            &[&env, &ids[0]],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>(2), "private");
    assert_eq!(row.get::<_, i64>(5), 2);
    assert_eq!(row.get::<_, i32>(6), 11);
    assert_eq!(row.get::<_, Option<i32>>(7), Some(9));
    for _ in 0..103 {
        let id = Uuid::new_v4();
        queue::enqueue(
            &db,
            id,
            &env,
            queue::discord_queue::JOB_KIND,
            &json!({"destination":"safe-id","content":"secret-payload"}),
        )
        .await
        .unwrap();
        db.execute("INSERT INTO public.novu_rust_discord_retry_history(job_id,environment_id,destination_id,retry_after_seconds,scheduled_delay_seconds) VALUES($1,$2,'safe-id',5,5)",&[&id,&env]).await.unwrap();
    }
    let list = |after: Option<&str>| {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"));
        cmd.env_clear()
            .env("DATABASE_URL", role_url.as_str())
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .args(["list-discord-retry", &env, "60", "all"]);
        if let Some(after) = after {
            cmd.arg(after);
        }
        cmd.output().unwrap()
    };
    let first = list(None);
    assert_eq!(first.status.code(), Some(0));
    let first: serde_json::Value = serde_json::from_slice(&first.stdout).unwrap();
    assert_eq!(first["data"].as_array().unwrap().len(), 100);
    let cursor = first["next"].as_str().unwrap();
    assert!(!first.to_string().contains("secret-payload"));
    assert!(!first.to_string().contains("payload-secret"));
    let second = list(Some(cursor));
    assert_eq!(second.status.code(), Some(0));
    let second: serde_json::Value = serde_json::from_slice(&second.stdout).unwrap();
    assert_eq!(second["data"].as_array().unwrap().len(), 5);
    assert!(second["next"].is_null());
    // Manual 429 decisions join diagnostics, without consuming automatic retry budget.
    let manual_env = format!("manual-{}", Uuid::new_v4());
    let manual_job = Uuid::new_v4();
    let manual_id = Uuid::new_v4();
    db.execute(
        "INSERT INTO public.novu_rust_environments VALUES($1,'org')",
        &[&manual_env],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT $1,$2,p,oid FROM pg_roles CROSS JOIN unnest(ARRAY['alert_view','alert_send']) p WHERE rolname::text=$2",&[&manual_env,&role]).await.unwrap();
    db.batch_execute(&format!("GRANT EXECUTE ON FUNCTION public.novu_rust_resolve_discord(text,uuid,uuid,uuid,bigint,text,text) TO {role}")).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_discord_resolvers SELECT $1,rolname,oid,false,true FROM pg_roles WHERE rolname::text=$2",&[&manual_env,&role]).await.unwrap();
    db.execute("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status,attempts) VALUES($1,$2,'experimental_discord_webhook',jsonb_build_object('destination','manual-dest','content','secret-payload'),'failed',1)",&[&manual_job,&manual_env]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_discord_attempts(job_id,environment_id,state,http_status,retry_after_seconds) VALUES($1,$2,'rejected',429,60)",&[&manual_job,&manual_env]).await.unwrap();
    let row=db.query_one("SELECT a.revision,j.failure_revision FROM public.novu_rust_discord_attempts a JOIN public.novu_queue_jobs j ON j.id=a.job_id WHERE j.id=$1",&[&manual_job]).await.unwrap();
    let rev: Uuid = row.get(0);
    let failure: i64 = row.get(1);
    assert!(
        reader
            .query_one(
                "SELECT public.novu_rust_resolve_discord($1,$2,$3,$4,$5,'retry_after','reviewed')",
                &[&manual_env, &manual_id, &manual_job, &rev, &failure]
            )
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    let row = reader
        .query_one(
            "SELECT * FROM public.novu_rust_discord_retry_health($1,60)",
            &[&manual_env],
        )
        .await
        .unwrap();
    assert_eq!((row.get::<_, i64>(0), row.get::<_, i64>(1)), (1, 0));
    let row = reader
        .query_one(
            "SELECT * FROM public.novu_rust_discord_retry_list($1,60,false,NULL)",
            &[&manual_env],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>(2), "manual-dest");
    assert_eq!(row.get::<_, i64>(5), 1);
    assert_eq!(row.get::<_, Option<i32>>(7), Some(60));
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM public.novu_rust_discord_retry_history WHERE job_id=$1",
            &[&manual_job]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    db.execute("UPDATE public.novu_queue_jobs SET available_at=clock_timestamp()-interval '2 minutes' WHERE id=$1",&[&manual_job]).await.unwrap();
    let row = reader
        .query_one(
            "SELECT * FROM public.novu_rust_discord_retry_health($1,60)",
            &[&manual_env],
        )
        .await
        .unwrap();
    assert_eq!((row.get::<_, i64>(0), row.get::<_, i64>(1)), (1, 1));
    db.execute("INSERT INTO public.novu_rust_alert_delivery_slots(environment_id,channel,target_id,state) VALUES($1,'slack','blocked','rejected')",&[&manual_env]).await.unwrap();
    let alert = Command::new(env!("CARGO_BIN_EXE_send-delivery-alert"))
        .env_clear()
        .env("DATABASE_URL", role_url.as_str())
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .env("SWALLOW_ALERT_RESEND_ENABLED", "false")
        .env("SWALLOW_ALERT_DISCORD_RETRY_GRACE_SECONDS", "60")
        .env("SWALLOW_ALERT_SCOPED_DELIVERY", "true")
        .env(
            "SWALLOW_ALERT_SLACK_WEBHOOK",
            "https://hooks.slack.com/services/A/B/C",
        )
        .env("SWALLOW_ALERT_SLACK_TARGET_ID", "blocked")
        .args([&manual_env, "300", "120", "60"])
        .output()
        .unwrap();
    assert_eq!(alert.status.code(), Some(2));
    db.execute("DELETE FROM public.novu_rust_delivery_operator_grants WHERE environment_id=$1 AND database_principal::text=$2 AND decision='alert_view'",&[&env,&role]).await.unwrap();
    assert!(
        reader
            .query(
                "SELECT * FROM public.novu_rust_discord_retry_list($1,60,false,NULL)",
                &[&env]
            )
            .await
            .is_err()
    );
    assert!(
        reader
            .query_one(
                "SELECT * FROM public.novu_rust_discord_retry_health($1,60)",
                &[&env]
            )
            .await
            .is_err()
    );
}
