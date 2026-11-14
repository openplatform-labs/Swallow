//! Resend queue with shared workflow email preparation.
use crate::{
    ClaimedJob,
    resend::{Resend, SendError},
    sendgrid::Email,
};
use std::time::Duration;
use tokio_postgres::{Client, Error};
use uuid::Uuid;

pub const JOB_KIND: &str = "experimental_resend_email";
pub const MIGRATION: &str = include_str!("../migrations/0060_resend_attempts.sql");

async fn claim(client: &Client, environment: &str) -> Result<Option<ClaimedJob>, Error> {
    let token = Uuid::new_v4();
    let row=client.query_opt("WITH candidate AS (SELECT id FROM novu_queue_jobs WHERE environment_id=$1 AND kind=$2 AND status='pending' AND available_at<=now() ORDER BY available_at,created_at,id FOR UPDATE SKIP LOCKED LIMIT 1) UPDATE novu_queue_jobs j SET status='running',attempts=j.attempts+1,lease_token=$3,lease_until=clock_timestamp()+interval '60 seconds' FROM candidate WHERE j.id=candidate.id RETURNING j.id,j.environment_id,j.kind,j.payload,j.attempts", &[&environment,&JOB_KIND,&token]).await?;
    Ok(row.map(|r| ClaimedJob {
        id: r.get(0),
        environment_id: r.get(1),
        kind: r.get(2),
        payload: r.get(3),
        attempts: r.get(4),
        lease_token: token,
    }))
}

async fn live(tx: &tokio_postgres::Transaction<'_>, job: &ClaimedJob) -> Result<bool, Error> {
    let row = tx.query_opt("SELECT lease_until > clock_timestamp() FROM novu_queue_jobs WHERE id=$1 AND environment_id=$2 AND kind=$3 AND status='running' AND lease_token=$4 FOR UPDATE", &[&job.id,&job.environment_id,&JOB_KIND,&job.lease_token]).await?;
    Ok(row.is_some_and(|r| r.get(0)))
}

pub async fn process_one(
    client: &mut Client,
    environment: &str,
    provider: &Resend,
) -> Result<bool, Error> {
    let Some(job) = claim(client, environment).await? else {
        return Ok(false);
    };
    let mut email: Email = match serde_json::from_value(job.payload.clone()) {
        Ok(value) => value,
        Err(_) => {
            crate::fail(client, job.id, job.lease_token, "invalid Resend job").await?;
            return Ok(true);
        }
    };
    let tx = client.transaction().await?;
    let subscriber =
        crate::sendgrid_queue::lock_workflow_subscriber(&tx, environment, &email).await?;
    if !live(&tx, &job).await? {
        tx.rollback().await?;
        return Ok(true);
    }
    if !crate::sendgrid_queue::prepare_workflow_email(
        &tx,
        environment,
        &job,
        &mut email,
        subscriber,
    )
    .await?
    {
        tx.commit().await?;
        return Ok(true);
    }
    let inserted=tx.execute("INSERT INTO novu_rust_resend_attempts(job_id,environment_id,state) VALUES ($1,$2,'dispatching') ON CONFLICT(job_id) DO NOTHING", &[&job.id,&environment]).await?;
    if inserted == 0 {
        tx.execute("UPDATE novu_queue_jobs SET status='failed',last_error='Resend attempt already exists',lease_token=NULL,lease_until=NULL WHERE id=$1 AND environment_id=$2", &[&job.id,&environment]).await?;
        tx.commit().await?;
        return Ok(true);
    }
    if email.message_id.is_some() {
        tx.execute("UPDATE novu_rust_email_messages SET delivery_state='dispatching' WHERE environment_id=$1 AND email_job_id=$2", &[&environment,&job.id]).await?;
    }
    tx.commit().await?;
    let key = format!("swallow/{}", job.id);
    let (state, code, id, diagnostic) = match provider.send(&email, Some(&key)).await {
        Ok(id) => ("accepted", None, Some(id), None),
        Err(SendError::Provider(code)) if code >= 500 || code == 409 => (
            "unknown",
            Some(i32::from(code)),
            None,
            Some("Resend outcome unknown"),
        ),
        Err(SendError::Provider(code)) => (
            "rejected",
            Some(i32::from(code)),
            None,
            Some("Resend rejected request"),
        ),
        Err(SendError::Transport | SendError::InvalidResponse) => {
            ("unknown", None, None, Some("Resend outcome unknown"))
        }
        Err(_) => ("rejected", None, None, Some("invalid Resend request")),
    };
    let tx = client.transaction().await?;
    if !live(&tx, &job).await? {
        tx.rollback().await?;
        return Ok(true);
    }
    let updated=tx.execute("UPDATE novu_rust_resend_attempts SET state=$3,http_status=$4,provider_message_id=$5,updated_at=clock_timestamp() WHERE job_id=$1 AND environment_id=$2 AND state='dispatching'", &[&job.id,&environment,&state,&code,&id]).await?;
    if updated == 1 {
        if email.message_id.is_some() {
            tx.execute("UPDATE novu_rust_email_messages SET delivery_state=$3,identifier=CASE WHEN $3='accepted' THEN $4 ELSE identifier END,status=CASE WHEN $3='accepted' THEN 'sent' ELSE 'error' END,error_id=CASE WHEN $3='accepted' THEN NULL ELSE 'mail_unexpected_error' END,error_text=CASE WHEN $3='accepted' THEN NULL ELSE $5 END WHERE environment_id=$1 AND email_job_id=$2", &[&environment,&job.id,&state,&id,&diagnostic]).await?;
        }
        let status = if state == "accepted" {
            "completed"
        } else {
            "failed"
        };
        tx.execute("UPDATE novu_queue_jobs SET status=$3,last_error=$4,lease_token=NULL,lease_until=NULL WHERE id=$1 AND environment_id=$2", &[&job.id,&environment,&status,&diagnostic]).await?;
    }
    tx.commit().await?;
    Ok(true)
}

pub async fn recover_expired(client: &mut Client, environment: &str) -> Result<usize, Error> {
    let tx = client.transaction().await?;
    let rows=tx.query("SELECT id FROM novu_queue_jobs WHERE environment_id=$1 AND kind=$2 AND status='running' AND lease_until<=clock_timestamp() ORDER BY lease_until,id FOR UPDATE SKIP LOCKED LIMIT 100", &[&environment,&JOB_KIND]).await?;
    for row in &rows {
        let id: Uuid = row.get(0);
        let attempt=tx.execute("UPDATE novu_rust_resend_attempts SET state='unknown',updated_at=clock_timestamp() WHERE job_id=$1 AND environment_id=$2 AND state='dispatching'", &[&id,&environment]).await?;
        if attempt == 1 {
            tx.execute("UPDATE novu_rust_email_messages SET delivery_state='unknown' WHERE environment_id=$1 AND email_job_id=$2", &[&environment,&id]).await?;
            tx.execute("UPDATE novu_queue_jobs SET status='failed',last_error='Resend outcome unknown',lease_token=NULL,lease_until=NULL WHERE id=$1 AND environment_id=$2", &[&id,&environment]).await?;
        } else {
            let exists:bool=tx.query_one("SELECT EXISTS(SELECT 1 FROM novu_rust_resend_attempts WHERE job_id=$1 AND environment_id=$2)", &[&id,&environment]).await?.get(0);
            let (status, error) = if exists {
                ("failed", Some("Resend attempt already exists"))
            } else {
                ("pending", None)
            };
            tx.execute("UPDATE novu_queue_jobs SET status=$3,last_error=$4,lease_token=NULL,lease_until=NULL WHERE id=$1 AND environment_id=$2", &[&id,&environment,&status,&error]).await?;
        }
    }
    tx.commit().await?;
    Ok(rows.len())
}

pub const POLL_INTERVAL: Duration = Duration::from_secs(1);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{enqueue, resend::Config};
    use serde_json::json;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };
    use tokio_postgres::NoTls;

    #[tokio::test]
    #[ignore = "sends real email and requires a fresh disposable PostgreSQL database"]
    async fn live_workflow_send() {
        let url = std::env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL required");
        let recipient = std::env::var("NOVU_RESEND_TEST_TO").expect("NOVU_RESEND_TEST_TO required");
        let (mut db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
        tokio::spawn(async move { driver.await.unwrap() });
        db.batch_execute("CREATE TABLE novu_rust_workflows (environment_id text, identifier text, in_app_content text, has_steps boolean NOT NULL DEFAULT true, PRIMARY KEY (environment_id, identifier))").await.unwrap();
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
        let mut paths: Vec<_> = std::fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|p| p.extension().is_some_and(|v| v == "sql"))
            .collect();
        paths.sort();
        for path in paths {
            db.batch_execute(&std::fs::read_to_string(path).unwrap())
                .await
                .unwrap();
        }
        db.execute("INSERT INTO novu_rust_workflows(environment_id,identifier,email_subject,email_html,email_provider,in_app_step_active,inbox_metadata) VALUES ('env-a','live-test','Swallow Resend workflow test','<p>Swallow Resend workflow test message.</p>','resend',false,'{\"id\":\"workflow-id\"}')",&[]).await.unwrap();
        db.execute("INSERT INTO novu_rust_subscribers(environment_id,subscriber_id,internal_id,profile) VALUES ('env-a','live-recipient','live-id',jsonb_build_object('email',$1::text))", &[&recipient]).await.unwrap();
        let trigger = Uuid::new_v4();
        let payload = json!({"workflow":"live-test","transactionId":trigger.to_string(),"content":"","payload":{},"recipients":[{"subscriberId":"live-recipient"}],"recipientIdentitySnapshot":{"live-recipient":"live-id"},"severityOverride":null});
        enqueue(&db, trigger, "env-a", crate::in_app::JOB_KIND, &payload)
            .await
            .unwrap();
        assert!(crate::in_app::process_one(&mut db).await.unwrap());
        let job: Uuid = db
            .query_one(
                "SELECT id FROM novu_queue_jobs WHERE kind=$1 AND payload->>'transaction_id'=$2",
                &[&JOB_KIND, &trigger.to_string()],
            )
            .await
            .unwrap()
            .get(0);
        let provider = Resend::new(Config {
            api_key: std::env::var("NOVU_RESEND_API_KEY").expect("NOVU_RESEND_API_KEY required"),
            from: std::env::var("NOVU_RESEND_FROM").expect("NOVU_RESEND_FROM required"),
            sender_name: String::new(),
        })
        .unwrap();
        assert!(process_one(&mut db, "env-a", &provider).await.unwrap());
        let row=db.query_one("SELECT j.status,a.state,m.delivery_state,a.provider_message_id FROM novu_queue_jobs j JOIN novu_rust_resend_attempts a ON a.job_id=j.id JOIN novu_rust_email_messages m ON m.email_job_id=j.id WHERE j.id=$1", &[&job]).await.unwrap();
        assert_eq!(row.get::<_, String>(0), "completed");
        assert_eq!(row.get::<_, String>(1), "accepted");
        assert_eq!(row.get::<_, String>(2), "accepted");
        assert!(!row.get::<_, String>(3).is_empty());
    }

    #[tokio::test]
    #[ignore = "requires fresh disposable PostgreSQL and loopback"]
    async fn workflow_delivery_revalidates_subscriber() {
        let url = std::env::var("TEST_DATABASE_URL").unwrap();
        let (mut db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
        tokio::spawn(async move { driver.await.unwrap() });
        db.batch_execute("CREATE TABLE novu_rust_workflows (environment_id text, identifier text, in_app_content text, has_steps boolean NOT NULL DEFAULT true, PRIMARY KEY (environment_id, identifier))").await.unwrap();
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
        let mut paths: Vec<_> = std::fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.extension().is_some_and(|v| v == "sql"))
            .collect();
        paths.sort();
        for path in paths {
            db.batch_execute(&std::fs::read_to_string(path).unwrap())
                .await
                .unwrap();
        }
        db.batch_execute("INSERT INTO novu_rust_workflows(environment_id,identifier,email_subject,email_html,email_provider,in_app_step_active,inbox_metadata) VALUES ('env-a','hello','Hi {{subscriber.firstName}}','<p>Hello {{subscriber.firstName}}</p>','resend',false,'{\"id\":\"workflow-id\"}'); INSERT INTO novu_rust_subscribers(environment_id,subscriber_id,internal_id,profile) VALUES ('env-a','recipient','old-id','{\"email\":\"to@example.test\",\"firstName\":\"Alice\"}')").await.unwrap();
        let trigger = Uuid::new_v4();
        let payload = json!({"workflow":"hello","transactionId":trigger.to_string(),"content":"","payload":{},"recipients":[{"subscriberId":"recipient"}],"recipientIdentitySnapshot":{"recipient":"old-id"},"severityOverride":null});
        enqueue(&db, trigger, "env-a", crate::in_app::JOB_KIND, &payload)
            .await
            .unwrap();
        assert!(crate::in_app::process_one(&mut db).await.unwrap());
        let row = db
            .query_one(
                "SELECT id FROM novu_queue_jobs WHERE kind=$1 AND payload->>'transaction_id'=$2",
                &[&JOB_KIND, &trigger.to_string()],
            )
            .await
            .unwrap();
        let email_job: Uuid = row.get(0);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/emails", listener.local_addr().unwrap());
        let mock = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            let mut chunk = [0; 4096];
            loop {
                let n = socket.read(&mut chunk).await.unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&chunk[..n]);
                if let Some(pos) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
                    let h = String::from_utf8_lossy(&bytes[..pos]);
                    let len: usize = h
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length: ")
                                .and_then(|v| v.parse().ok())
                        })
                        .unwrap();
                    if bytes.len() >= pos + 4 + len {
                        break;
                    }
                }
            }
            let req = String::from_utf8(bytes).unwrap();
            let body: serde_json::Value =
                serde_json::from_str(req.split_once("\r\n\r\n").unwrap().1).unwrap();
            assert_eq!(body["to"], json!(["to@example.test"]));
            assert_eq!(body["subject"], "Hi Alice");
            assert!(body["html"].as_str().unwrap().contains("Hello Alice"));
            let response = "{\"id\":\"resend-workflow-id\"}";
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}",response.len()).as_bytes()).await.unwrap();
        });
        let provider = Resend::test_endpoint(
            Config {
                api_key: "re_test".into(),
                from: "from@example.test".into(),
                sender_name: String::new(),
            },
            &endpoint,
        )
        .unwrap();
        let second_provider = Resend::test_endpoint(
            Config {
                api_key: "re_test".into(),
                from: "from@example.test".into(),
                sender_name: String::new(),
            },
            &endpoint,
        )
        .unwrap();
        let (mut second_db, second_driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
        tokio::spawn(async move { second_driver.await.unwrap() });
        let (first, second) = tokio::join!(
            process_one(&mut db, "env-a", &provider),
            process_one(&mut second_db, "env-a", &second_provider)
        );
        assert_eq!(
            usize::from(first.unwrap()) + usize::from(second.unwrap()),
            1
        );
        mock.await.unwrap();
        let row=db.query_one("SELECT j.status,a.state,m.delivery_state,m.identifier FROM novu_queue_jobs j JOIN novu_rust_resend_attempts a ON a.job_id=j.id JOIN novu_rust_email_messages m ON m.email_job_id=j.id WHERE j.id=$1", &[&email_job]).await.unwrap();
        assert_eq!(row.get::<_, String>(0), "completed");
        assert_eq!(row.get::<_, String>(1), "accepted");
        assert_eq!(row.get::<_, String>(2), "accepted");
        assert_eq!(row.get::<_, String>(3), "resend-workflow-id");
        // A stale worker must not overwrite the unknown result after another
        // worker recovers an expired lease during an outstanding HTTP call.
        let race = Uuid::new_v4();
        enqueue(
            &db,
            race,
            "env-race",
            JOB_KIND,
            &json!({"to":["to@example.test"],"subject":"Race","html":"Hi"}),
        )
        .await
        .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/emails", listener.local_addr().unwrap());
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (finish_tx, finish_rx) = tokio::sync::oneshot::channel();
        let mock = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut data = [0; 4096];
            assert!(socket.read(&mut data).await.unwrap() > 0);
            started_tx.send(()).unwrap();
            finish_rx.await.unwrap();
            let body = "{\"id\":\"late-provider-id\"}";
            socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
        });
        let provider_race = Resend::test_endpoint(
            Config {
                api_key: "re_test".into(),
                from: "from@example.test".into(),
                sender_name: String::new(),
            },
            &endpoint,
        )
        .unwrap();
        let (mut dispatch_db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
        tokio::spawn(async move { driver.await.unwrap() });
        let dispatch =
            tokio::spawn(
                async move { process_one(&mut dispatch_db, "env-race", &provider_race).await },
            );
        started_rx.await.unwrap();
        db.execute("UPDATE novu_queue_jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1", &[&race]).await.unwrap();
        assert_eq!(recover_expired(&mut db, "env-race").await.unwrap(), 1);
        finish_tx.send(()).unwrap();
        assert!(dispatch.await.unwrap().unwrap());
        mock.await.unwrap();
        let row=db.query_one("SELECT j.status,a.state,a.provider_message_id FROM novu_queue_jobs j JOIN novu_rust_resend_attempts a ON a.job_id=j.id WHERE j.id=$1", &[&race]).await.unwrap();
        assert_eq!(row.get::<_, String>(0), "failed");
        assert_eq!(row.get::<_, String>(1), "unknown");
        assert!(row.get::<_, Option<String>>(2).is_none());
        let trigger2 = Uuid::new_v4();
        let mut second = payload.clone();
        second["transactionId"] = json!(trigger2.to_string());
        enqueue(&db, trigger2, "env-a", crate::in_app::JOB_KIND, &second)
            .await
            .unwrap();
        assert!(crate::in_app::process_one(&mut db).await.unwrap());
        let job2: Uuid = db
            .query_one(
                "SELECT id FROM novu_queue_jobs WHERE kind=$1 AND payload->>'transaction_id'=$2",
                &[&JOB_KIND, &trigger2.to_string()],
            )
            .await
            .unwrap()
            .get(0);
        db.execute("UPDATE novu_rust_subscribers SET internal_id='new-id' WHERE environment_id='env-a' AND subscriber_id='recipient'",&[]).await.unwrap();
        assert!(process_one(&mut db, "env-a", &provider).await.unwrap());
        let row=db.query_one("SELECT status,last_error,(SELECT count(*) FROM novu_rust_resend_attempts WHERE job_id=$1) FROM novu_queue_jobs WHERE id=$1", &[&job2]).await.unwrap();
        assert_eq!(row.get::<_, String>(0), "failed");
        assert_eq!(row.get::<_, String>(1), "subscriber identity changed");
        assert_eq!(row.get::<_, i64>(2), 0);
    }

    #[tokio::test]
    #[ignore = "requires a disposable PostgreSQL database"]
    async fn standalone_dispatch_and_recovery() {
        let url = std::env::var("TEST_DATABASE_URL").unwrap();
        let (mut db, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
        tokio::spawn(async move { connection.await.unwrap() });
        db.batch_execute("DROP TABLE IF EXISTS novu_rust_resend_attempts,novu_rust_email_messages,novu_queue_jobs CASCADE")
            .await
            .unwrap();
        db.batch_execute(crate::MIGRATION).await.unwrap();
        db.batch_execute(MIGRATION).await.unwrap();
        db.batch_execute(MIGRATION).await.unwrap();
        db.batch_execute(crate::in_app::EMAIL_MESSAGES_MIGRATION)
            .await
            .unwrap();
        db.batch_execute(crate::in_app::EMAIL_PROVIDER_RESULT_MIGRATION)
            .await
            .unwrap();
        db.batch_execute("ALTER TABLE novu_rust_email_messages ADD COLUMN delivery_state text NOT NULL DEFAULT 'not_attempted'").await.unwrap();
        let id = Uuid::new_v4();
        let other = Uuid::new_v4();
        let payload = json!({"to":["to@example.test"],"subject":"Hello","html":"Hi"});
        enqueue(&db, id, "env-a", JOB_KIND, &payload).await.unwrap();
        enqueue(&db, other, "env-b", JOB_KIND, &payload)
            .await
            .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/emails", listener.local_addr().unwrap());
        let mock = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            let mut buf = [0; 4096];
            loop {
                let n = socket.read(&mut buf).await.unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&buf[..n]);
                if let Some(pos) = bytes.windows(4).position(|x| x == b"\r\n\r\n") {
                    let h = String::from_utf8_lossy(&bytes[..pos]);
                    let n: usize = h
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length: ")
                                .and_then(|v| v.parse().ok())
                        })
                        .unwrap();
                    if bytes.len() >= pos + 4 + n {
                        break;
                    }
                }
            }
            let req = String::from_utf8(bytes).unwrap().to_ascii_lowercase();
            assert!(req.contains(&format!("idempotency-key: swallow/{id}")));
            assert!(req.contains("authorization: bearer re_test"));
            let response = "{\"id\":\"resend-id\"}";
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}",response.len()).as_bytes()).await.unwrap();
        });
        let provider = Resend::test_endpoint(
            Config {
                api_key: "re_test".into(),
                from: "from@example.test".into(),
                sender_name: String::new(),
            },
            &endpoint,
        )
        .unwrap();
        assert!(process_one(&mut db, "env-a", &provider).await.unwrap());
        mock.await.unwrap();
        let row=db.query_one("SELECT j.status,a.state,a.provider_message_id FROM novu_queue_jobs j JOIN novu_rust_resend_attempts a ON a.job_id=j.id WHERE j.id=$1", &[&id]).await.unwrap();
        assert_eq!(row.get::<_, String>(0), "completed");
        assert_eq!(row.get::<_, String>(1), "accepted");
        assert_eq!(row.get::<_, String>(2), "resend-id");
        assert_eq!(
            db.query_one("SELECT status FROM novu_queue_jobs WHERE id=$1", &[&other])
                .await
                .unwrap()
                .get::<_, String>(0),
            "pending"
        );
        let pre = Uuid::new_v4();
        let post = Uuid::new_v4();
        for job in [pre, post] {
            enqueue(&db, job, "env-a", JOB_KIND, &payload)
                .await
                .unwrap();
            db.execute("UPDATE novu_queue_jobs SET status='running',lease_token=$2,lease_until=clock_timestamp()-interval '1 second' WHERE id=$1", &[&job,&Uuid::new_v4()]).await.unwrap();
        }
        db.execute("INSERT INTO novu_rust_resend_attempts(job_id,environment_id,state) VALUES ($1,'env-a','dispatching')", &[&post]).await.unwrap();
        assert_eq!(recover_expired(&mut db, "env-b").await.unwrap(), 0);
        assert_eq!(recover_expired(&mut db, "env-a").await.unwrap(), 2);
        assert_eq!(
            db.query_one("SELECT status FROM novu_queue_jobs WHERE id=$1", &[&pre])
                .await
                .unwrap()
                .get::<_, String>(0),
            "pending"
        );
        let row=db.query_one("SELECT j.status,a.state FROM novu_queue_jobs j JOIN novu_rust_resend_attempts a ON a.job_id=j.id WHERE j.id=$1", &[&post]).await.unwrap();
        assert_eq!(row.get::<_, String>(0), "failed");
        assert_eq!(row.get::<_, String>(1), "unknown");

        // Each test job has an isolated environment so it cannot consume the
        // pre-dispatch job requeued above.
        for (status, body, expected) in [
            (500, "{}", "unknown"),
            (409, "{}", "unknown"),
            (429, "{}", "rejected"),
            (200, "{}", "unknown"),
        ] {
            let job = Uuid::new_v4();
            let environment = format!("env-{job}");
            enqueue(&db, job, &environment, JOB_KIND, &payload)
                .await
                .unwrap();
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("http://{}/emails", listener.local_addr().unwrap());
            let mock = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut buf = [0; 4096];
                assert!(socket.read(&mut buf).await.unwrap() > 0);
                socket.write_all(format!("HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
            });
            let provider = Resend::test_endpoint(
                Config {
                    api_key: "re_test".into(),
                    from: "from@example.test".into(),
                    sender_name: String::new(),
                },
                &endpoint,
            )
            .unwrap();
            assert!(process_one(&mut db, &environment, &provider).await.unwrap());
            mock.await.unwrap();
            let row = db.query_one("SELECT j.status,a.state,a.http_status FROM novu_queue_jobs j JOIN novu_rust_resend_attempts a ON a.job_id=j.id WHERE j.id=$1", &[&job]).await.unwrap();
            assert_eq!(row.get::<_, String>(0), "failed");
            assert_eq!(row.get::<_, String>(1), expected);
            assert_eq!(
                row.get::<_, Option<i32>>(2),
                if status == 200 { None } else { Some(status) }
            );
            assert!(!process_one(&mut db, &environment, &provider).await.unwrap());
        }
        let job = Uuid::new_v4();
        let environment = format!("env-{job}");
        let unsupported = json!({"to":["to@example.test"],"subject":"Hello","html":"Hi","unrecognized_option":true});
        enqueue(&db, job, &environment, JOB_KIND, &unsupported)
            .await
            .unwrap();
        let row = db
            .query_one(
                "SELECT count(*) FROM novu_rust_resend_attempts WHERE job_id=$1",
                &[&job],
            )
            .await
            .unwrap();
        assert_eq!(row.get::<_, i64>(0), 0);
        // No listener is running; if the unsupported job is sent, the result
        // would be unknown rather than the explicit pre-dispatch failure.
        assert!(process_one(&mut db, &environment, &provider).await.unwrap());
        let row = db.query_one("SELECT status,last_error,(SELECT count(*) FROM novu_rust_resend_attempts WHERE job_id=$1) FROM novu_queue_jobs WHERE id=$1", &[&job]).await.unwrap();
        assert_eq!(row.get::<_, String>(0), "failed");
        assert_eq!(row.get::<_, String>(1), "invalid Resend job");
        assert_eq!(row.get::<_, i64>(2), 0);
    }
}
