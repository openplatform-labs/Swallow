//! Pre-dispatch failure retry through an actual SMTP worker.
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
#[ignore = "requires fully migrated disposable PostgreSQL, openssl and python3"]
async fn operator_retry_dispatches_after_preflight_failure() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
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
                    .join("tests/fixtures/smtp_starttls_fixture.py"),
            )
            .arg(&cert)
            .arg(&key)
            .arg(dir.join("message"))
            .stdout(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let mut port = String::new();
    BufReader::new(server.0.stdout.take().unwrap())
        .read_line(&mut port)
        .unwrap();
    let worker = || {
        Process(
            Command::new(env!("CARGO_BIN_EXE_smtp-worker"))
                .env_clear()
                .env("DATABASE_URL", &url)
                .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
                .env("NOVU_SMTP_ENVIRONMENT_ID", &env)
                .env("SWALLOW_SMTP_HOST", "localhost")
                .env("SWALLOW_SMTP_PORT", port.trim())
                .env("SWALLOW_SMTP_FROM", "sender@example.test")
                .env("SWALLOW_SMTP_CA_FILE", &cert)
                .env("SWALLOW_SMTP_USERNAME", "fixture-user")
                .env("SWALLOW_SMTP_PASSWORD", "fixture-password")
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
    db.execute("UPDATE public.novu_queue_jobs SET status='failed',last_error='configuration unavailable' WHERE id=$1",&[&first]).await.unwrap();
    let failure: i64 = db
        .query_one(
            "SELECT failure_revision FROM public.novu_queue_jobs WHERE id=$1",
            &[&first],
        )
        .await
        .unwrap()
        .get(0);
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
            "-",
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
    let mut second_worker = worker();
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
    let body = std::fs::read_to_string(dir.join("message")).unwrap();
    assert!(body.contains("first@example.test"));
    let row=db.query_one("SELECT j.status,a.state,m.status,m.delivery_state,(SELECT count(*) FROM public.novu_rust_smtp_resolutions WHERE job_id=j.id AND request_id=$2 AND decision='retry' AND attempt_revision IS NULL) FROM public.novu_queue_jobs j JOIN public.novu_rust_smtp_attempts a ON a.job_id=j.id JOIN public.novu_rust_email_messages m ON m.email_job_id=j.id WHERE j.id=$1",&[&first,&request]).await.unwrap();
    assert_eq!(
        (
            row.get::<_, String>(0),
            row.get::<_, String>(1),
            row.get::<_, String>(2),
            row.get::<_, String>(3),
            row.get::<_, i64>(4)
        ),
        (
            "completed".into(),
            "accepted".into(),
            "sent".into(),
            "accepted".into(),
            1
        )
    );
    std::fs::remove_dir_all(dir).unwrap();
}
