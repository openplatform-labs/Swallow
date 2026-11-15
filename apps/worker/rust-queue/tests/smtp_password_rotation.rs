//! Password rotation with an actual SMTP worker and disposable PostgreSQL.
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
async fn worker_pauses_for_invalid_password_and_uses_atomic_rotation() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(driver);
    let env = format!("smtp-rotation-{}", Uuid::new_v4().simple());
    db.execute(
        "INSERT INTO public.novu_rust_environments VALUES($1,'org')",
        &[&env],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO public.novu_rust_smtp_credential_workers SELECT $1,rolname,oid FROM pg_roles WHERE rolname=current_user",&[&env]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_smtp_ca_workers SELECT $1,rolname,oid FROM pg_roles WHERE rolname=current_user",&[&env]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_delivery_operator_grants SELECT $1,rolname,'alert_view',oid FROM pg_roles WHERE rolname=current_user",&[&env]).await.unwrap();
    let dir = std::env::temp_dir().join(format!("swallow-smtp-rotation-{}", Uuid::new_v4()));
    std::fs::create_dir(&dir).unwrap();
    let cert = dir.join("cert");
    let ca = dir.join("ca");
    let key = dir.join("key");
    let password = dir.join("password");
    std::fs::write(&password, "old-fixture").unwrap();
    let conflict = Command::new(env!("CARGO_BIN_EXE_smtp-worker"))
        .env_clear()
        .env("NOVU_SMTP_ENVIRONMENT_ID", &env)
        .env("SWALLOW_SMTP_HOST", "localhost")
        .env("SWALLOW_SMTP_FROM", "sender@example.test")
        .env("SWALLOW_SMTP_USERNAME", "fixture-user")
        .env("SWALLOW_SMTP_PASSWORD_FILE", &password)
        .env("SWALLOW_SMTP_PASSWORD", "inline-fixture")
        .env("NOVU_SMTP_RELOAD_PASSWORD", "true")
        .output()
        .unwrap();
    assert!(!conflict.status.success());
    assert!(!String::from_utf8_lossy(&conflict.stderr).contains("inline-fixture"));
    let missing_ca = Command::new(env!("CARGO_BIN_EXE_smtp-worker"))
        .env_clear()
        .env("NOVU_SMTP_ENVIRONMENT_ID", &env)
        .env("NOVU_SMTP_RELOAD_CA", "true")
        .output()
        .unwrap();
    assert!(!missing_ca.status.success());
    assert!(String::from_utf8_lossy(&missing_ca.stderr).contains("SMTP CA reload requires a file"));
    assert!(Command::new("openssl")
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
        .success());
    std::fs::copy(&cert, &ca).unwrap();
    let mut server = Process(
        Command::new("python3")
            .arg(
                std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("tests/fixtures/smtp_rotation_fixture.py"),
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
    let mut worker = Process(
        Command::new(env!("CARGO_BIN_EXE_smtp-worker"))
            .env_clear()
            .env("DATABASE_URL", &url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("NOVU_SMTP_ENVIRONMENT_ID", &env)
            .env("SWALLOW_SMTP_HOST", "localhost")
            .env("SWALLOW_SMTP_PORT", port.trim())
            .env("SWALLOW_SMTP_FROM", "sender@example.test")
            .env("SWALLOW_SMTP_USERNAME", "fixture-user")
            .env("SWALLOW_SMTP_PASSWORD_FILE", &password)
            .env("SWALLOW_SMTP_CA_FILE", &ca)
            .env("NOVU_SMTP_RELOAD_PASSWORD", "true")
            .env("NOVU_SMTP_RELOAD_CA", "true")
            .env("NOVU_SMTP_CA_HEALTH_ENABLED", "true")
            .env("NOVU_SMTP_CREDENTIAL_HEALTH_ENABLED", "true")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
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
    for _ in 0..200 {
        if dir.join("accepted-1").exists() {
            break;
        }
        assert!(worker.0.try_wait().unwrap().is_none());
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(dir.join("accepted-1").exists());
    let row = db
        .query_one(
            "SELECT * FROM public.novu_rust_smtp_credential_health_check($1,60)",
            &[&env],
        )
        .await
        .unwrap();
    assert_eq!((row.get::<_, i64>(0), row.get::<_, i64>(1)), (1, 0));
    std::fs::write(&password, "invalid\npassword").unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;
    let row = db
        .query_one(
            "SELECT * FROM public.novu_rust_smtp_credential_health_check($1,60)",
            &[&env],
        )
        .await
        .unwrap();
    assert_eq!((row.get::<_, i64>(0), row.get::<_, i64>(1)), (0, 1));
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
    tokio::time::sleep(Duration::from_secs(3)).await;
    let row=db.query_one("SELECT status,attempts,(SELECT count(*) FROM public.novu_rust_smtp_attempts WHERE job_id=$1) FROM public.novu_queue_jobs WHERE id=$1",&[&second]).await.unwrap();
    assert_eq!(
        (
            row.get::<_, String>(0),
            row.get::<_, i32>(1),
            row.get::<_, i64>(2)
        ),
        ("pending".into(), 0, 0)
    );
    std::fs::write(dir.join("new"), "new-fixture").unwrap();
    std::fs::write(dir.join("bad-ca"), "invalid certificate").unwrap();
    std::fs::rename(dir.join("bad-ca"), &ca).unwrap();
    std::fs::rename(dir.join("new"), &password).unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;
    let row=db.query_one("SELECT status,attempts,(SELECT count(*) FROM public.novu_rust_smtp_attempts WHERE job_id=$1) FROM public.novu_queue_jobs WHERE id=$1",&[&second]).await.unwrap();
    assert_eq!(
        (
            row.get::<_, String>(0),
            row.get::<_, i32>(1),
            row.get::<_, i64>(2)
        ),
        ("pending".into(), 0, 0)
    );
    let row = db
        .query_one(
            "SELECT * FROM public.novu_rust_smtp_credential_health_check($1,60)",
            &[&env],
        )
        .await
        .unwrap();
    assert_eq!((row.get::<_, i64>(0), row.get::<_, i64>(1)), (1, 0));
    let ca_row = db
        .query_one(
            "SELECT * FROM public.novu_rust_smtp_ca_health_check($1,60)",
            &[&env],
        )
        .await
        .unwrap();
    assert_eq!((ca_row.get::<_, i64>(0), ca_row.get::<_, i64>(1)), (0, 1));
    let replacement = format!("{}\n", std::fs::read_to_string(&cert).unwrap());
    std::fs::write(dir.join("new-ca"), replacement).unwrap();
    std::fs::rename(dir.join("new-ca"), &ca).unwrap();
    for _ in 0..200 {
        if dir.join("accepted-2").exists() {
            break;
        }
        assert!(worker.0.try_wait().unwrap().is_none());
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(dir.join("accepted-2").exists());
    let ca_row = db
        .query_one(
            "SELECT * FROM public.novu_rust_smtp_ca_health_check($1,60)",
            &[&env],
        )
        .await
        .unwrap();
    assert_eq!((ca_row.get::<_, i64>(0), ca_row.get::<_, i64>(1)), (1, 0));
    let row = db
        .query_one(
            "SELECT * FROM public.novu_rust_smtp_credential_health_check($1,60)",
            &[&env],
        )
        .await
        .unwrap();
    assert_eq!((row.get::<_, i64>(0), row.get::<_, i64>(1)), (1, 0));
    let mut done = false;
    for _ in 0..100 {
        done=db.query_opt("SELECT 1 FROM public.novu_queue_jobs j JOIN public.novu_rust_smtp_attempts a ON a.job_id=j.id WHERE j.id=$1 AND j.status='completed' AND a.state='accepted'",&[&second]).await.unwrap().is_some();
        if done {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(done);
    assert!(server.0.wait().unwrap().success());
    drop(worker);
    drop(server);
    let tagged_url = format!("{url}?application_name=smtp_rotation_exit");
    let mut exiting = Process(
        Command::new(env!("CARGO_BIN_EXE_smtp-worker"))
            .env_clear()
            .env("DATABASE_URL", tagged_url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("NOVU_SMTP_ENVIRONMENT_ID", &env)
            .env("SWALLOW_SMTP_HOST", "localhost")
            .env("SWALLOW_SMTP_PORT", port.trim())
            .env("SWALLOW_SMTP_FROM", "sender@example.test")
            .env("SWALLOW_SMTP_USERNAME", "fixture-user")
            .env("SWALLOW_SMTP_PASSWORD_FILE", &password)
            .env("SWALLOW_SMTP_CA_FILE", &cert)
            .env("NOVU_SMTP_RELOAD_PASSWORD", "true")
            .env("NOVU_SMTP_CREDENTIAL_HEALTH_ENABLED", "true")
            .env("NOVU_SMTP_RELOAD_FAILURE_POLICY", "exit")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let mut connected = false;
    for _ in 0..100 {
        connected=db.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='smtp_rotation_exit')",&[]).await.unwrap().get(0);
        if connected {
            break;
        }
        assert!(exiting.0.try_wait().unwrap().is_none());
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(connected);
    std::fs::write(&password, "invalid\nsecret").unwrap();
    let mut exit = None;
    for _ in 0..200 {
        exit = exiting.0.try_wait().unwrap();
        if exit.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(exit.is_some_and(|status| !status.success()));
    drop(exiting);
    let tagged_url = format!("{url}?application_name=smtp_ca_exit");
    let mut ca_exiting = Process(
        Command::new(env!("CARGO_BIN_EXE_smtp-worker"))
            .env_clear()
            .env("DATABASE_URL", tagged_url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("NOVU_SMTP_ENVIRONMENT_ID", &env)
            .env("SWALLOW_SMTP_HOST", "localhost")
            .env("SWALLOW_SMTP_PORT", port.trim())
            .env("SWALLOW_SMTP_FROM", "sender@example.test")
            .env("SWALLOW_SMTP_CA_FILE", &ca)
            .env("NOVU_SMTP_RELOAD_CA", "true")
            .env("NOVU_SMTP_RELOAD_FAILURE_POLICY", "exit")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let mut connected = false;
    for _ in 0..100 {
        connected=db.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='smtp_ca_exit')",&[]).await.unwrap().get(0);
        if connected {
            break;
        }
        assert!(ca_exiting.0.try_wait().unwrap().is_none());
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(connected);
    std::fs::write(&ca, "invalid certificate").unwrap();
    let mut exit = None;
    for _ in 0..200 {
        exit = ca_exiting.0.try_wait().unwrap();
        if exit.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(exit.is_some_and(|status| !status.success()));
    drop(ca_exiting);
    std::fs::remove_dir_all(dir).unwrap();
}
