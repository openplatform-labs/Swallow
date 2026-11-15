//! Operator retry committed before synchronous PostgreSQL promotion.
use novu_postgres_queue as queue;
use serde_json::json;
use std::{
    io::{BufRead, BufReader},
    process::{Child, Command, Stdio},
    time::Duration,
};
use tokio_postgres::NoTls;
use uuid::Uuid;
struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
#[tokio::test]
#[ignore = "requires disposable synchronous local PostgreSQL pair; stops primary"]
async fn operator_retry_survives_synchronous_promotion() {
    let url = std::env::var("TEST_PRIMARY_URL").unwrap();
    let standby_url = std::env::var("TEST_STANDBY_URL").unwrap();
    let cluster_url = std::env::var("TEST_FAILOVER_URL").unwrap();
    let pgctl = std::env::var("TEST_LOCAL_PG_CTL").unwrap();
    let primary = std::env::var("TEST_LOCAL_PRIMARY_DATA").unwrap();
    let standby = std::env::var("TEST_LOCAL_STANDBY_DATA").unwrap();
    let (mut db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(driver);
    let env = format!("smtp-crash-{}", Uuid::new_v4().simple());
    db.execute(
        "INSERT INTO public.novu_rust_environments VALUES($1,'org')",
        &[&env],
    )
    .await
    .unwrap();
    let dir = std::env::temp_dir().join(format!("swallow-smtp-crash-{}", Uuid::new_v4()));
    std::fs::create_dir(&dir).unwrap();
    let cert = dir.join("cert");
    let key = dir.join("key");
    assert!(
        Command::new("openssl")
            .args([
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-days",
                "1",
                "-subj",
                "/CN=localhost",
                "-addext",
                "subjectAltName=DNS:localhost",
                "-addext",
                "basicConstraints=critical,CA:FALSE",
                "-keyout"
            ])
            .arg(&key)
            .arg("-out")
            .arg(&cert)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap()
            .success()
    );
    let mut server = Process(
        Command::new("python3")
            .arg(
                std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("tests/fixtures/smtp_crash_fixture.py"),
            )
            .arg(&cert)
            .arg(&key)
            .arg(&dir)
            .stdout(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let mut port = String::new();
    BufReader::new(server.0.stdout.take().unwrap())
        .read_line(&mut port)
        .unwrap();
    let worker = |worker_url: &str| {
        Process(
            Command::new(env!("CARGO_BIN_EXE_smtp-worker"))
                .env_clear()
                .env("DATABASE_URL", worker_url)
                .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
                .env("NOVU_SMTP_ENVIRONMENT_ID", &env)
                .env("SWALLOW_SMTP_HOST", "localhost")
                .env("SWALLOW_SMTP_PORT", port.trim())
                .env("SWALLOW_SMTP_FROM", "sender@example.test")
                .env("SWALLOW_SMTP_CA_FILE", &cert)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap(),
        )
    };
    let first = Uuid::new_v4();
    queue::enqueue(
        &db,
        first,
        &env,
        queue::smtp_queue::JOB_KIND,
        &json!({"to":["first@example.test"],"subject":"first","html":"first"}),
    )
    .await
    .unwrap();
    db.execute("INSERT INTO public.novu_rust_email_messages(internal_id,environment_id,subscriber_id,subscriber_internal_id,notification_internal_id,workflow_identifier,transaction_id,subject,status,email_job_id,delivery_state) VALUES($1,$2,'sub','sub-internal','notification','wf','tx','first','warning',$3,'not_attempted')",&[&first.to_string(),&env,&first]).await.unwrap();
    let mut first_worker = worker(&url);
    for _ in 0..200 {
        if dir.join("message-1").exists() {
            break;
        }
        assert!(first_worker.0.try_wait().unwrap().is_none());
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(dir.join("message-1").exists());
    let row=db.query_one("SELECT j.status,a.state FROM public.novu_queue_jobs j JOIN public.novu_rust_smtp_attempts a ON a.job_id=j.id WHERE j.id=$1",&[&first]).await.unwrap();
    assert_eq!(
        (row.get::<_, String>(0), row.get::<_, String>(1)),
        ("running".into(), "dispatching".into())
    );
    first_worker.0.kill().unwrap();
    first_worker.0.wait().unwrap();
    db.execute("UPDATE public.novu_queue_jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1",&[&first]).await.unwrap();
    assert_eq!(
        queue::smtp_queue::recover_expired(&mut db, &env)
            .await
            .unwrap(),
        1
    );
    assert!(!dir.join("message-2").exists());
    let snapshot=db.query_one("SELECT a.revision,j.failure_revision FROM public.novu_rust_smtp_attempts a JOIN public.novu_queue_jobs j ON j.id=a.job_id WHERE j.id=$1",&[&first]).await.unwrap();
    let revision: Uuid = snapshot.get(0);
    let failure: i64 = snapshot.get(1);
    let role = format!("smtp_retry_{}", Uuid::new_v4().simple());
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN PASSWORD 'test-only'; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_resolve_smtp(text,uuid,uuid,uuid,bigint,text,text) TO {role}")).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_smtp_resolvers SELECT $1,rolname,oid,false,true FROM pg_roles WHERE rolname=$2",&[&env,&role]).await.unwrap();
    let mut role_url = reqwest::Url::parse(&url).unwrap();
    role_url.set_username(&role).unwrap();
    role_url.set_password(Some("test-only")).unwrap();
    let request = Uuid::new_v4();
    let result = Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
        .env_clear()
        .env("DATABASE_URL", role_url.as_str())
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .args([
            "resolve-smtp",
            &env,
            &request.to_string(),
            &first.to_string(),
            &revision.to_string(),
            &failure.to_string(),
            "retry",
            "fixture operator decision",
        ])
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let (replica, driver) = tokio_postgres::connect(&standby_url, NoTls).await.unwrap();
    tokio::spawn(driver);
    let mut replicated = false;
    for _ in 0..200 {
        if replica.query_one("SELECT EXISTS(SELECT 1 FROM public.novu_queue_jobs j JOIN public.novu_rust_smtp_resolutions r ON r.job_id=j.id WHERE j.id=$1 AND j.status='pending' AND r.request_id=$2 AND r.decision='retry') AND NOT EXISTS(SELECT 1 FROM public.novu_rust_smtp_attempts WHERE job_id=$1)",&[&first,&request]).await.unwrap().get::<_,bool>(0) {replicated=true;break;}
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(replicated, "decision did not reach standby");
    assert!(
        Command::new(&pgctl)
            .args(["-D", &primary, "-m", "immediate", "stop", "-w"])
            .stdout(Stdio::null())
            .status()
            .unwrap()
            .success()
    );
    assert!(
        Command::new(&pgctl)
            .args(["-D", &standby, "promote", "-w"])
            .stdout(Stdio::null())
            .status()
            .unwrap()
            .success()
    );
    let (db, driver) = tokio_postgres::connect(&standby_url, NoTls).await.unwrap();
    tokio::spawn(driver);
    assert!(
        !db.query_one("SELECT pg_is_in_recovery()", &[])
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    let mut second_worker = worker(&cluster_url);
    let mut done = false;
    for _ in 0..200 {
        let row = db
            .query_one(
                "SELECT status FROM public.novu_queue_jobs WHERE id=$1",
                &[&first],
            )
            .await
            .unwrap();
        if row.get::<_, String>(0) == "completed" {
            done = true;
            break;
        }
        assert!(second_worker.0.try_wait().unwrap().is_none());
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(done);
    assert!(server.0.wait().unwrap().success());
    assert!(!dir.join("extra-connection").exists());
    let first_body = std::fs::read_to_string(dir.join("message-1")).unwrap();
    let retry_body = std::fs::read_to_string(dir.join("message-2")).unwrap();
    assert!(first_body.contains("first@example.test"));
    assert!(retry_body.contains("first@example.test"));
    let row=db.query_one("SELECT j.status,a.state,a.revision,m.status,m.delivery_state,(SELECT count(*) FROM public.novu_rust_smtp_resolutions WHERE job_id=j.id AND request_id=$2 AND decision='retry' AND attempt_revision=$3) FROM public.novu_queue_jobs j JOIN public.novu_rust_smtp_attempts a ON a.job_id=j.id JOIN public.novu_rust_email_messages m ON m.email_job_id=j.id WHERE j.id=$1",&[&first,&request,&revision]).await.unwrap();
    assert_eq!(
        (
            row.get::<_, String>(0),
            row.get::<_, String>(1),
            row.get::<_, String>(3),
            row.get::<_, String>(4),
            row.get::<_, i64>(5)
        ),
        (
            "completed".into(),
            "accepted".into(),
            "sent".into(),
            "accepted".into(),
            1
        )
    );
    assert_ne!(row.get::<_, Uuid>(2), revision);
    let retry_url = cluster_url.replacen("postgres://postgres@", &format!("postgres://{role}@"), 1);
    let replay = Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
        .env_clear()
        .env("DATABASE_URL", retry_url)
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .args([
            "resolve-smtp",
            &env,
            &request.to_string(),
            &first.to_string(),
            &revision.to_string(),
            &failure.to_string(),
            "retry",
            "fixture operator decision",
        ])
        .output()
        .unwrap();
    assert!(
        replay.status.success(),
        "{}",
        String::from_utf8_lossy(&replay.stderr)
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&replay.stdout).unwrap()["recorded"],
        false
    );
    std::fs::remove_dir_all(dir).unwrap();
}
