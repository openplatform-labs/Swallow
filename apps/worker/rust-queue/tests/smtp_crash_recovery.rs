//! Actual worker crash after SMTP DATA on disposable PostgreSQL.
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
async fn crash_after_smtp_data_never_automatically_resends() {
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
    let mut first_worker = worker();
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
    let mut second_worker = worker();
    let mut recovered = false;
    for _ in 0..200 {
        let row=db.query_one("SELECT j.status,a.state FROM public.novu_queue_jobs j JOIN public.novu_rust_smtp_attempts a ON a.job_id=j.id WHERE j.id=$1",&[&first]).await.unwrap();
        if row.get::<_, String>(0) == "failed" && row.get::<_, String>(1) == "unknown" {
            recovered = true;
            break;
        }
        assert!(second_worker.0.try_wait().unwrap().is_none());
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(recovered);
    let second = Uuid::new_v4();
    queue::enqueue(
        &db,
        second,
        &env,
        queue::smtp_queue::JOB_KIND,
        &json!({"to":["second@example.test"],"subject":"second","html":"second"}),
    )
    .await
    .unwrap();
    let mut done = false;
    for _ in 0..200 {
        let row = db
            .query_one(
                "SELECT status FROM public.novu_queue_jobs WHERE id=$1",
                &[&second],
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
    let second_body = std::fs::read_to_string(dir.join("message-2")).unwrap();
    assert!(first_body.contains("first@example.test"));
    assert!(second_body.contains("second@example.test"));
    let row=db.query_one("SELECT status,(SELECT state FROM public.novu_rust_smtp_attempts WHERE job_id=$1),(SELECT count(*) FROM public.novu_rust_smtp_attempts WHERE job_id=$1) FROM public.novu_queue_jobs WHERE id=$1",&[&first]).await.unwrap();
    assert_eq!(
        (
            row.get::<_, String>(0),
            row.get::<_, String>(1),
            row.get::<_, i64>(2)
        ),
        ("failed".into(), "unknown".into(), 1)
    );
    std::fs::remove_dir_all(dir).unwrap();
}
