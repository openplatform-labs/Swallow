//! Standalone Resend queue. Workflow and subscriber identity handling are not wired.
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
    let email: Email = match serde_json::from_value(job.payload.clone()) {
        Ok(value) => value,
        Err(_) => {
            crate::fail(client, job.id, job.lease_token, "invalid Resend job").await?;
            return Ok(true);
        }
    };
    // This path accepts only already-prepared standalone messages. It cannot
    // safely evaluate a workflow, subscriber identity, or preferences.
    if email.workflow_context.is_some() {
        crate::fail(
            client,
            job.id,
            job.lease_token,
            "Resend workflow jobs are unsupported",
        )
        .await?;
        return Ok(true);
    }
    let tx = client.transaction().await?;
    if !live(&tx, &job).await? {
        tx.rollback().await?;
        return Ok(true);
    }
    let inserted=tx.execute("INSERT INTO novu_rust_resend_attempts(job_id,environment_id,state) VALUES ($1,$2,'dispatching') ON CONFLICT(job_id) DO NOTHING", &[&job.id,&environment]).await?;
    if inserted == 0 {
        tx.execute("UPDATE novu_queue_jobs SET status='failed',last_error='Resend attempt already exists',lease_token=NULL,lease_until=NULL WHERE id=$1 AND environment_id=$2", &[&job.id,&environment]).await?;
        tx.commit().await?;
        return Ok(true);
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
    #[ignore = "requires a disposable PostgreSQL database"]
    async fn standalone_dispatch_and_recovery() {
        let url = std::env::var("TEST_DATABASE_URL").unwrap();
        let (mut db, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
        tokio::spawn(async move { connection.await.unwrap() });
        db.batch_execute("DROP TABLE IF EXISTS novu_rust_resend_attempts,novu_queue_jobs CASCADE")
            .await
            .unwrap();
        db.batch_execute(crate::MIGRATION).await.unwrap();
        db.batch_execute(MIGRATION).await.unwrap();
        db.batch_execute(MIGRATION).await.unwrap();
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
    }
}
