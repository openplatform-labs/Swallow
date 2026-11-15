//! Reviewed delayed Twilio 429 retry on a migrated disposable database.
use std::process::{Command, Stdio};
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;

async fn rejected(db: &Client, env: &str, delay: Option<i32>) -> (Uuid, Uuid) {
    let job = Uuid::new_v4();
    let dispatch = Uuid::new_v4();
    db.execute("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status) VALUES($1,$2,'experimental_twilio_sms','{}','failed')",&[&job,&env]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_twilio_dispatch_ids(id,job_id,environment_id) VALUES($1,$2,$3)",&[&dispatch,&job,&env]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_twilio_attempts(job_id,environment_id,state,http_status,dispatch_id,retry_after_seconds,retry_not_before) VALUES($1,$2,'rejected',429,$3,$4,CASE WHEN $4::int IS NULL THEN NULL ELSE clock_timestamp()+make_interval(secs=>$4::int::float8) END)",&[&job,&env,&dispatch,&delay]).await.unwrap();
    (job, dispatch)
}
fn cli(url: &str, env: &str, job: Uuid, dispatch: Uuid) -> Command {
    let mut c = Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"));
    c.env("DATABASE_URL", url)
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .args([
            "twilio-retry-after",
            env,
            &job.to_string(),
            &dispatch.to_string(),
            "fixture-operator",
            "ticket-123",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    c
}
#[tokio::test]
#[ignore = "requires fully migrated disposable PostgreSQL and role creation"]
async fn delayed_retry_is_fenced_audited_and_blocks_callback_evidence() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let env = format!("twilio-delay-{}", Uuid::new_v4());
    let role = format!("twilio_delay_{}", Uuid::new_v4().simple());
    db.execute(
        "INSERT INTO public.novu_rust_environments VALUES($1,'org')",
        &[&env],
    )
    .await
    .unwrap();
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN PASSWORD 'test-only'; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_resolve_twilio_after(text,uuid,uuid,text,text),public.novu_rust_twilio_retry_guidance(text,uuid) TO {role}")).await.unwrap();
    let mut role_url = reqwest::Url::parse(&url).unwrap();
    role_url.set_username(&role).unwrap();
    role_url.set_password(Some("test-only")).unwrap();
    let role_url = role_url.as_str();
    let (job, dispatch) = rejected(&db, &env, Some(120)).await;
    assert!(
        !cli(role_url, &env, job, dispatch)
            .status()
            .unwrap()
            .success()
    );
    db.execute("INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT $1,$2,'retry',oid FROM pg_roles WHERE rolname::text=$2",&[&env,&role]).await.unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
        .env("DATABASE_URL", role_url)
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .args(["twilio-retry-guidance", &env, &job.to_string()])
        .output()
        .unwrap();
    assert!(output.status.success());
    let guidance: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(guidance["dispatchId"], dispatch.to_string());
    assert_eq!(guidance["retryAfterSeconds"], 120);
    let (missing, id) = rejected(&db, &env, None).await;
    assert!(!cli(role_url, &env, missing, id).status().unwrap().success());
    assert!(
        !cli(role_url, &env, job, Uuid::new_v4())
            .status()
            .unwrap()
            .success()
    );
    assert!(
        !cli(role_url, "other-env", job, dispatch)
            .status()
            .unwrap()
            .success()
    );
    let (evidence, evidence_id) = rejected(&db, &env, Some(120)).await;
    db.execute("INSERT INTO public.novu_rust_twilio_delivery_events(environment_id,account_sid,message_sid,status,job_id,dispatch_id) VALUES($1,'ACfixture','SMfixture','delivered',$2,$3)",&[&env,&evidence,&evidence_id]).await.unwrap();
    assert!(
        !cli(role_url, &env, evidence, evidence_id)
            .status()
            .unwrap()
            .success()
    );
    let (rollback, rollback_id) = rejected(&db, &env, Some(120)).await;
    db.batch_execute("ALTER TABLE public.novu_rust_delivery_reconciliations ADD CONSTRAINT fixture_twilio_no_audit CHECK (twilio_retry_not_before IS NULL) NOT VALID").await.unwrap();
    assert!(
        !cli(role_url, &env, rollback, rollback_id)
            .status()
            .unwrap()
            .success()
    );
    db.batch_execute("ALTER TABLE public.novu_rust_delivery_reconciliations DROP CONSTRAINT fixture_twilio_no_audit").await.unwrap();
    let unchanged=db.query_one("SELECT status,(SELECT count(*) FROM public.novu_rust_twilio_attempts WHERE job_id=$1),(SELECT count(*) FROM public.novu_rust_delivery_reconciliations WHERE job_id=$1) FROM public.novu_queue_jobs WHERE id=$1",&[&rollback]).await.unwrap();
    assert_eq!(unchanged.get::<_, String>(0), "failed");
    assert_eq!(unchanged.get::<_, i64>(1), 1);
    assert_eq!(unchanged.get::<_, i64>(2), 0);
    // Simulate the legacy worker's dispatch insert after claiming a scheduled job.
    let (legacy_job, legacy_dispatch) = rejected(&db, &env, Some(0)).await;
    assert!(
        cli(role_url, &env, legacy_job, legacy_dispatch)
            .status()
            .unwrap()
            .success()
    );
    let lease = Uuid::new_v4();
    db.execute("UPDATE public.novu_queue_jobs SET status='running',lease_token=$2,lease_until=clock_timestamp()+interval '1 minute' WHERE id=$1", &[&legacy_job,&lease]).await.unwrap();
    assert!(db.execute("INSERT INTO public.novu_rust_twilio_attempts(job_id,environment_id,state,dispatch_id) VALUES($1,$2,'dispatching',$3)", &[&legacy_job,&env,&Uuid::new_v4()]).await.is_err());
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM public.novu_rust_twilio_attempts WHERE job_id=$1",
            &[&legacy_job]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    let mut first = cli(role_url, &env, job, dispatch).spawn().unwrap();
    let mut second = cli(role_url, &env, job, dispatch).spawn().unwrap();
    let outcomes = [
        first.wait().unwrap().success(),
        second.wait().unwrap().success(),
    ];
    assert_eq!(outcomes.into_iter().filter(|x| *x).count(), 1);
    let row=db.query_one("SELECT status,available_at>clock_timestamp()+interval '100 seconds',(SELECT count(*) FROM public.novu_rust_twilio_attempts WHERE job_id=$1),(SELECT count(*) FROM public.novu_rust_delivery_reconciliations WHERE job_id=$1 AND retry_dispatch_id=$2 AND twilio_retry_not_before IS NOT NULL) FROM public.novu_queue_jobs WHERE id=$1",&[&job,&dispatch]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "pending");
    assert!(row.get::<_, bool>(1));
    assert_eq!(row.get::<_, i64>(2), 0);
    assert_eq!(row.get::<_, i64>(3), 1);
    assert!(
        db.query_one(
            "SELECT twilio_delayed_retry FROM public.novu_queue_jobs WHERE id=$1",
            &[&job]
        )
        .await
        .unwrap()
        .get::<_, bool>(0)
    );
    db.execute("DELETE FROM public.novu_rust_delivery_operator_grants WHERE environment_id=$1 AND database_principal=$2 AND decision='retry'",&[&env,&role]).await.unwrap();
    let (revoked, id) = rejected(&db, &env, Some(120)).await;
    assert!(!cli(role_url, &env, revoked, id).status().unwrap().success());
}
