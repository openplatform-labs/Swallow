//! Version-fenced review against a fully migrated disposable PostgreSQL.
use std::process::Command;
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;
async fn connect(url: &str) -> Client {
    let (c, d) = tokio_postgres::connect(url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = d.await;
    });
    c
}
fn cli(url: &str, args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
        .env_clear()
        .env("DATABASE_URL", url)
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .args(args)
        .output()
        .unwrap()
}
fn alert(url: &str, env: &str) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_send-delivery-alert"))
        .env_clear()
        .env("DATABASE_URL", url)
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .env("SWALLOW_ALERT_RESEND_ENABLED", "false")
        .env("SWALLOW_ALERT_FCM_UNREVIEWED_ENABLED", "true")
        .env("SWALLOW_ALERT_SCOPED_DELIVERY", "true")
        .env("SWALLOW_ALERT_COOLDOWN_POLICY", "new-condition")
        .env("SWALLOW_ALERT_CONDITIONS_V2", "true")
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
async fn reviews_are_scoped_persistent_and_revision_fenced() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let mut db = connect(&url).await;
    let env = format!("partial-review-{}", Uuid::new_v4());
    let other = format!("partial-review-other-{}", Uuid::new_v4());
    let role = format!("partial_review_{}", Uuid::new_v4().simple());
    db.execute(
        "INSERT INTO public.novu_rust_environments VALUES($1,'org'),($2,'org')",
        &[&env, &other],
    )
    .await
    .unwrap();
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN PASSWORD 'test-only'; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_fcm_partial_unreviewed(text,uuid),public.novu_rust_fcm_partial_ack(text,uuid,text,text,text),public.novu_rust_fcm_unreviewed_health(text),public.novu_rust_reserve_alert_v2(text,text,text,integer,bigint),public.novu_rust_observe_alert_conditions_v2(text,text,text,bigint,bigint) TO {role}")).await.unwrap();
    let mut role_url = reqwest::Url::parse(&url).unwrap();
    role_url.set_username(&role).unwrap();
    role_url.set_password(Some("test-only")).unwrap();
    let role_url = role_url.as_str();
    assert_eq!(
        cli(role_url, &["list-unreviewed-fcm", &env]).status.code(),
        Some(1)
    );
    db.execute("INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT $1,rolname,p,oid FROM pg_roles CROSS JOIN unnest(ARRAY['alert_view','queue_ack','alert_send']) p WHERE rolname::text=$2",&[&env,&role]).await.unwrap();
    let job = Uuid::new_v4();
    let other_job = Uuid::new_v4();
    db.execute("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status) VALUES($1,$3,'experimental_fcm_push','{}','completed'),($2,$4,'experimental_fcm_push','{}','completed')",&[&job,&other_job,&env,&other]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_fcm_attempts(job_id,environment_id,target_index,state,updated_at) VALUES($1,$3,0,'accepted',now()-interval '2 days'),($1,$3,1,'rejected',now()-interval '2 days'),($2,$4,0,'accepted',now()),($2,$4,1,'rejected',now())",&[&job,&other_job,&env,&other]).await.unwrap();
    let health = cli(role_url, &["check-unreviewed-fcm", &env]);
    assert_eq!(health.status.code(), Some(2));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&health.stdout).unwrap(),
        serde_json::json!({"unreviewedJobs":1})
    );
    assert_eq!(
        cli(role_url, &["check-unreviewed-fcm", &other])
            .status
            .code(),
        Some(1)
    );
    db.execute("INSERT INTO public.novu_rust_alert_delivery_slots(environment_id,channel,target_id,state) VALUES($1,'slack','blocked','rejected')",&[&env]).await.unwrap();
    assert_eq!(alert(role_url, &env).status.code(), Some(2));
    let list = cli(role_url, &["list-unreviewed-fcm", &env]);
    assert_eq!(list.status.code(), Some(0));
    let v: serde_json::Value = serde_json::from_slice(&list.stdout).unwrap();
    assert_eq!(v["data"].as_array().unwrap().len(), 1);
    assert_eq!(v["data"][0]["jobId"], job.to_string());
    let rev = v["data"][0]["revision"].as_str().unwrap().to_owned();
    assert_eq!(
        cli(role_url, &["list-unreviewed-fcm", &other])
            .status
            .code(),
        Some(1)
    );
    db.execute("UPDATE public.novu_rust_fcm_attempts SET http_status=429,updated_at=clock_timestamp() WHERE job_id=$1 AND target_index=1",&[&job]).await.unwrap();
    assert_eq!(
        cli(
            role_url,
            &[
                "ack-fcm-partial",
                &env,
                &job.to_string(),
                &rev,
                "operator",
                "reviewed"
            ]
        )
        .status
        .code(),
        Some(1)
    );
    let list = cli(role_url, &["list-unreviewed-fcm", &env]);
    let v: serde_json::Value = serde_json::from_slice(&list.stdout).unwrap();
    let new_rev = v["data"][0]["revision"].as_str().unwrap().to_owned();
    assert_ne!(rev, new_rev);
    // A worker-like transaction holds the job lock, changes the evidence, then
    // the waiting review must reject its now-stale revision.
    let tx = db.transaction().await.unwrap();
    tx.query_one(
        "SELECT id FROM public.novu_queue_jobs WHERE id=$1 FOR UPDATE",
        &[&job],
    )
    .await
    .unwrap();
    let cli_url = role_url.to_owned();
    let cli_env = env.clone();
    let cli_revision = new_rev.clone();
    let cli_job = job.to_string();
    let pending = std::thread::spawn(move || {
        cli(
            &cli_url,
            &[
                "ack-fcm-partial",
                &cli_env,
                &cli_job,
                &cli_revision,
                "operator",
                "reviewed",
            ],
        )
    });
    let mut waiting = false;
    for _ in 0..100 {
        tx.query_one("SELECT pg_stat_clear_snapshot()", &[])
            .await
            .unwrap();
        waiting=tx.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE '%novu_rust_fcm_partial_ack%')",&[]).await.unwrap().get(0);
        if waiting {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(waiting, "review should wait for the job lock");
    tx.execute("UPDATE public.novu_rust_fcm_attempts SET http_status=503,updated_at=clock_timestamp()+interval '1 second' WHERE job_id=$1 AND target_index=1",&[&job]).await.unwrap();
    tx.commit().await.unwrap();
    assert_eq!(pending.join().unwrap().status.code(), Some(1));
    let list = cli(role_url, &["list-unreviewed-fcm", &env]);
    let value: serde_json::Value = serde_json::from_slice(&list.stdout).unwrap();
    let new_rev = value["data"][0]["revision"].as_str().unwrap().to_owned();
    let ack = cli(
        role_url,
        &[
            "ack-fcm-partial",
            &env,
            &job.to_string(),
            &new_rev,
            "operator",
            "reviewed",
        ],
    );
    assert_eq!(ack.status.code(), Some(0));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&ack.stdout).unwrap(),
        serde_json::json!({"recorded":true})
    );
    let repeat = cli(
        role_url,
        &[
            "ack-fcm-partial",
            &env,
            &job.to_string(),
            &new_rev,
            "operator",
            "reviewed",
        ],
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&repeat.stdout).unwrap(),
        serde_json::json!({"recorded":false})
    );
    let health = cli(role_url, &["check-unreviewed-fcm", &env]);
    assert_eq!(health.status.code(), Some(0));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&health.stdout).unwrap(),
        serde_json::json!({"unreviewedJobs":0})
    );
    assert_eq!(alert(role_url, &env).status.code(), Some(0));
    let empty = cli(role_url, &["list-unreviewed-fcm", &env]);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&empty.stdout).unwrap(),
        serde_json::json!({"data":[]})
    );
    db.execute("UPDATE public.novu_rust_fcm_attempts SET http_status=400,updated_at=clock_timestamp()+interval '2 seconds' WHERE job_id=$1 AND target_index=1",&[&job]).await.unwrap();
    let changed = cli(role_url, &["list-unreviewed-fcm", &env]);
    let v: serde_json::Value = serde_json::from_slice(&changed.stdout).unwrap();
    assert_eq!(v["data"].as_array().unwrap().len(), 1);
    assert_ne!(v["data"][0]["revision"], new_rev);
    let health = cli(role_url, &["check-unreviewed-fcm", &env]);
    assert_eq!(health.status.code(), Some(2));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&health.stdout).unwrap(),
        serde_json::json!({"unreviewedJobs":1})
    );
    db.execute("WITH added AS (INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status) SELECT gen_random_uuid(),$1,'experimental_fcm_push','{}','completed' FROM generate_series(1,101) RETURNING id,environment_id) INSERT INTO public.novu_rust_fcm_attempts(job_id,environment_id,target_index,state) SELECT id,environment_id,n,CASE WHEN n=0 THEN 'accepted' ELSE 'rejected' END FROM added CROSS JOIN generate_series(0,1) n",&[&env]).await.unwrap();
    let health = cli(role_url, &["check-unreviewed-fcm", &env]);
    assert_eq!(health.status.code(), Some(2));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&health.stdout).unwrap(),
        serde_json::json!({"unreviewedJobs":102})
    );
    let page = cli(role_url, &["list-unreviewed-fcm", &env]);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&page.stdout).unwrap()["data"]
            .as_array()
            .unwrap()
            .len(),
        100
    );
    db.execute("DELETE FROM public.novu_rust_delivery_operator_grants WHERE environment_id=$1 AND database_principal=$2 AND decision='queue_ack'",&[&env,&role]).await.unwrap();
    let next = v["data"][0]["revision"].as_str().unwrap();
    assert_eq!(
        cli(
            role_url,
            &[
                "ack-fcm-partial",
                &env,
                &job.to_string(),
                next,
                "operator",
                "reviewed"
            ]
        )
        .status
        .code(),
        Some(1)
    );
    let audit=db.query_one("SELECT operator_name,reason,database_principal FROM public.novu_rust_fcm_partial_reviews WHERE job_id=$1",&[&job]).await.unwrap();
    assert_eq!(audit.get::<_, String>(0), "operator");
    assert_eq!(audit.get::<_, String>(1), "reviewed");
    assert_eq!(audit.get::<_, String>(2), role);
}
