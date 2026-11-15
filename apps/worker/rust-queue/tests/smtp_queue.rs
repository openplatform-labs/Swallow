//! Actual SMTP worker and disposable PostgreSQL; no external provider is contacted.
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
async fn worker_sends_once_and_recovers_expired_attempts() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (mut db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    let env = format!("smtp-worker-{}", Uuid::new_v4().simple());
    db.execute(
        "INSERT INTO public.novu_rust_environments VALUES($1,'org')",
        &[&env],
    )
    .await
    .unwrap();
    let dir = std::env::temp_dir().join(format!("swallow-smtp-worker-{}", Uuid::new_v4()));
    std::fs::create_dir(&dir).unwrap();
    let cert = dir.join("cert");
    let key = dir.join("key");
    let output = dir.join("message");
    let password = dir.join("password");
    std::fs::write(&password, "fixture-password").unwrap();
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
            .arg(&output)
            .stdout(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let mut port = String::new();
    BufReader::new(server.0.stdout.take().unwrap())
        .read_line(&mut port)
        .unwrap();
    db.execute("INSERT INTO public.novu_rust_workflows(environment_id,identifier,active,has_steps,email_subject,email_html,email_provider,inbox_metadata) VALUES($1,'smtp-fixture',true,true,'workflow fixture','<p>Hello SMTP</p>','smtp','{\"id\":\"64a000000000000000000051\",\"name\":\"Fixture\",\"critical\":false,\"tags\":[]}'::jsonb)",&[&env]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_subscribers(environment_id,subscriber_id,internal_id,profile) VALUES($1,'alice','64a000000000000000000054','{\"email\":\"ops@example.test\"}'::jsonb)",&[&env]).await.unwrap();
    let trigger = Uuid::new_v4();
    queue::enqueue(&db,trigger,&env,queue::in_app::JOB_KIND,&json!({"workflow":"smtp-fixture","transactionId":"smtp-fixture-tx","content":"","payload":{},"recipients":[{"subscriberId":"alice"}],"recipientIdentitySnapshot":{"alice":"64a000000000000000000054"},"emailAttachments":[{"file":[104,101,108,108,111],"name":"report.txt","mime":"text/plain"},{"file":[0,1,255],"name":"logo.png","mime":"image/png","cid":"logo"}],"severityOverride":null})).await.unwrap();
    assert!(queue::in_app::process_one(&mut db).await.unwrap());
    let job:Uuid=db.query_one("SELECT email_job_id FROM public.novu_rust_email_step_outcomes WHERE environment_id=$1 AND transaction_id='smtp-fixture-tx'",&[&env]).await.unwrap().get(0);
    assert_eq!(
        db.query_one(
            "SELECT kind FROM public.novu_queue_jobs WHERE id=$1",
            &[&job]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        queue::smtp_queue::JOB_KIND
    );
    let legacy = json!({"workflow_identifier":"smtp-fixture","workflow_context":{}});
    assert!(db.execute("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload) VALUES($1,$2,'experimental_sendgrid_email',$3)",&[&Uuid::new_v4(),&env,&legacy]).await.is_err());
    db.execute("INSERT INTO public.novu_rust_subscribers(environment_id,subscriber_id,internal_id,profile) VALUES($1,'bob','64a000000000000000000055','{\"email\":\"old@example.test\"}'::jsonb)",&[&env]).await.unwrap();
    let stale_trigger = Uuid::new_v4();
    queue::enqueue(&db,stale_trigger,&env,queue::in_app::JOB_KIND,&json!({"workflow":"smtp-fixture","transactionId":"smtp-stale-tx","content":"","payload":{},"recipients":[{"subscriberId":"bob"}],"recipientIdentitySnapshot":{"bob":"64a000000000000000000055"},"severityOverride":null})).await.unwrap();
    assert!(queue::in_app::process_one(&mut db).await.unwrap());
    let stale:Uuid=db.query_one("SELECT email_job_id FROM public.novu_rust_email_step_outcomes WHERE environment_id=$1 AND transaction_id='smtp-stale-tx'",&[&env]).await.unwrap().get(0);
    db.execute(
        "DELETE FROM public.novu_rust_subscribers WHERE environment_id=$1 AND subscriber_id='bob'",
        &[&env],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO public.novu_rust_subscribers(environment_id,subscriber_id,internal_id,profile) VALUES($1,'bob','64a000000000000000000056','{\"email\":\"new@example.test\"}'::jsonb)",&[&env]).await.unwrap();
    let invalid = Uuid::new_v4();
    queue::enqueue(&db,invalid,&env,queue::smtp_queue::JOB_KIND,&json!({"to":["ops@example.test"],"subject":"unsupported","html":"test","headers":{"Bcc":"not supported"}})).await.unwrap();
    let expired = Uuid::new_v4();
    queue::enqueue(&db, expired, &env, queue::smtp_queue::JOB_KIND, &json!({}))
        .await
        .unwrap();
    db.execute("UPDATE public.novu_queue_jobs SET status='running',lease_token=$2,lease_until=clock_timestamp()-interval '1 second' WHERE id=$1",&[&expired,&Uuid::new_v4()]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_smtp_attempts(job_id,environment_id,state) VALUES ($1,$2,'dispatching')",&[&expired,&env]).await.unwrap();
    let mut worker = Process(
        Command::new(env!("CARGO_BIN_EXE_smtp-worker"))
            .env_clear()
            .env("DATABASE_URL", &url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("NOVU_SMTP_ENVIRONMENT_ID", &env)
            .env("SWALLOW_SMTP_HOST", "localhost")
            .env("SWALLOW_SMTP_PORT", port.trim())
            .env("SWALLOW_SMTP_TLS", "starttls")
            .env("SWALLOW_SMTP_FROM", "sender@example.test")
            .env("SWALLOW_SMTP_USERNAME", "fixture-user")
            .env("SWALLOW_SMTP_PASSWORD_FILE", &password)
            .env("SWALLOW_SMTP_CA_FILE", &cert)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let mut done = false;
    for _ in 0..200 {
        let row=db.query_one("SELECT (SELECT status='completed' FROM public.novu_queue_jobs WHERE id=$1),(SELECT state='accepted' FROM public.novu_rust_smtp_attempts WHERE job_id=$1),(SELECT status='failed' AND last_error='unsupported SMTP email' FROM public.novu_queue_jobs WHERE id=$2),(SELECT status='failed' FROM public.novu_queue_jobs WHERE id=$3),(SELECT state='unknown' FROM public.novu_rust_smtp_attempts WHERE job_id=$3)",&[&job,&invalid,&expired]).await.unwrap();
        done = (0..5).all(|i| row.get::<_, Option<bool>>(i) == Some(true));
        if done {
            break;
        }
        assert!(worker.0.try_wait().unwrap().is_none(), "SMTP worker exited");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(done, "SMTP jobs did not complete");
    assert!(server.0.wait().unwrap().success());
    let body = std::fs::read_to_string(&output).unwrap();
    assert!(body.contains("workflow fixture"));
    assert!(body.contains("Hello SMTP"));
    assert!(!body.contains("fixture-password"));
    let parsed = Command::new("python3").arg("-c").arg(r#"import sys,email; m=email.message_from_binary_file(open(sys.argv[1],"rb")); p=[x for x in m.walk() if x.get_filename()]; assert len(p)==2; assert p[0].get("Content-ID")=="<logo>" and p[0].get_payload(decode=True)==bytes([0,1,255]); assert p[1].get_filename()=="report.txt" and p[1].get_payload(decode=True)==b"hello""#).arg(&output).status().unwrap();
    assert!(parsed.success());
    let mut stale_failed = false;
    for _ in 0..100 {
        let row=db.query_one("SELECT status,last_error,(SELECT count(*) FROM public.novu_rust_smtp_attempts WHERE job_id=$1) FROM public.novu_queue_jobs WHERE id=$1",&[&stale]).await.unwrap();
        if row.get::<_, String>(0) == "failed" {
            assert_eq!(
                row.get::<_, Option<String>>(1).as_deref(),
                Some("subscriber identity changed")
            );
            assert_eq!(row.get::<_, i64>(2), 0);
            stale_failed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(stale_failed, "recreated subscriber job was not rejected");
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM public.novu_rust_smtp_attempts WHERE environment_id=$1",
            &[&env]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        2
    );
    std::fs::remove_dir_all(dir).unwrap();
}
