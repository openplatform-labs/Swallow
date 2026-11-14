//! Environment- and destination-scoped standalone Slack webhook jobs.
use crate::{
    ClaimedJob,
    slack::SendError,
    slack_webhook::{Message, Webhook},
};
use tokio_postgres::{Client, Error, Transaction};
use uuid::Uuid;
pub const JOB_KIND: &str = "experimental_slack_webhook";
pub const MIGRATION: &str = include_str!("../migrations/0074_slack_webhook_queue.sql");
async fn claim(
    client: &Client,
    environment: &str,
    destination: &str,
) -> Result<Option<ClaimedJob>, Error> {
    let token = Uuid::new_v4();
    let row=client.query_opt("WITH candidate AS (SELECT id FROM novu_queue_jobs WHERE environment_id=$1 AND kind=$2 AND status='pending' AND payload->>'destination'=$4 AND available_at<=now() ORDER BY available_at,created_at,id FOR UPDATE SKIP LOCKED LIMIT 1) UPDATE novu_queue_jobs j SET status='running',attempts=j.attempts+1,lease_token=$3,lease_until=clock_timestamp()+interval '60 seconds' FROM candidate WHERE j.id=candidate.id RETURNING j.id,j.environment_id,j.kind,j.payload,j.attempts", &[&environment,&JOB_KIND,&token,&destination]).await?;
    Ok(row.map(|r| ClaimedJob {
        id: r.get(0),
        environment_id: r.get(1),
        kind: r.get(2),
        payload: r.get(3),
        attempts: r.get(4),
        lease_token: token,
    }))
}
async fn live(tx: &Transaction<'_>, job: &ClaimedJob) -> Result<bool, Error> {
    let row=tx.query_opt("SELECT 1 FROM novu_queue_jobs WHERE id=$1 AND environment_id=$2 AND kind=$3 AND status='running' AND lease_token=$4 FOR UPDATE", &[&job.id,&job.environment_id,&JOB_KIND,&job.lease_token]).await?;
    if row.is_none() {
        return Ok(false);
    }
    Ok(tx
        .query_one(
            "SELECT lease_until>clock_timestamp() FROM novu_queue_jobs WHERE id=$1",
            &[&job.id],
        )
        .await?
        .get(0))
}

pub async fn process_one(
    db: &mut Client,
    environment: &str,
    destination: &str,
    provider: &Webhook,
) -> Result<bool, Error> {
    let Some(job) = claim(db, environment, destination).await? else {
        return Ok(false);
    };
    let message: Message = match serde_json::from_value(job.payload.clone()) {
        Ok(m) => m,
        Err(_) => {
            crate::fail(db, job.id, job.lease_token, "invalid Slack job").await?;
            return Ok(true);
        }
    };
    if crate::slack_webhook::request_body(&message).is_err() {
        crate::fail(db, job.id, job.lease_token, "invalid Slack job").await?;
        return Ok(true);
    }
    let tx = db.transaction().await?;
    if !live(&tx, &job).await? {
        tx.rollback().await?;
        return Ok(true);
    }
    let inserted=tx.execute("INSERT INTO novu_rust_slack_attempts(job_id,environment_id,state) VALUES ($1,$2,'dispatching') ON CONFLICT DO NOTHING", &[&job.id,&environment]).await?;
    if inserted != 1 {
        tx.execute("UPDATE novu_queue_jobs SET status='failed',last_error='Slack attempt requires review',lease_token=NULL,lease_until=NULL WHERE id=$1", &[&job.id]).await?;
        tx.commit().await?;
        return Ok(true);
    }
    tx.commit().await?;
    let (state, code, id, diagnostic) = match provider.send(&message).await {
        Ok(id) => ("accepted", None, Some(id), None),
        Err(SendError::Rejected(code)) => (
            "rejected",
            code.map(i32::from),
            None,
            Some("Slack rejected request"),
        ),
        Err(SendError::Invalid) => ("rejected", None, None, Some("invalid Slack request")),
        Err(SendError::Unknown) => ("unknown", None, None, Some("Slack outcome unknown")),
    };
    let tx = db.transaction().await?;
    if !live(&tx, &job).await? {
        tx.rollback().await?;
        return Ok(true);
    }
    let updated=tx.execute("UPDATE novu_rust_slack_attempts SET state=$3,http_status=$4,provider_message_id=$5,updated_at=clock_timestamp() WHERE job_id=$1 AND environment_id=$2 AND state='dispatching'", &[&job.id,&environment,&state,&code,&id]).await?;
    if updated == 1 {
        let job_status = if state == "accepted" {
            "completed"
        } else {
            "failed"
        };
        tx.execute("UPDATE novu_queue_jobs SET status=$2,last_error=$3,lease_token=NULL,lease_until=NULL WHERE id=$1", &[&job.id,&job_status,&diagnostic]).await?;
    }
    tx.commit().await?;
    Ok(true)
}

pub async fn recover_expired(
    db: &mut Client,
    environment: &str,
    destination: &str,
) -> Result<usize, Error> {
    let tx = db.transaction().await?;
    let rows=tx.query("SELECT id FROM novu_queue_jobs WHERE environment_id=$1 AND kind=$2 AND payload->>'destination'=$3 AND status='running' AND lease_until<=clock_timestamp() ORDER BY lease_until,id FOR UPDATE SKIP LOCKED LIMIT 100", &[&environment,&JOB_KIND,&destination]).await?;
    for row in &rows {
        let id: Uuid = row.get(0);
        let attempted = tx
            .query_opt(
                "SELECT state FROM novu_rust_slack_attempts WHERE job_id=$1 AND environment_id=$2",
                &[&id, &environment],
            )
            .await?
            .is_some();
        if attempted {
            tx.execute("UPDATE novu_rust_slack_attempts SET state='unknown',updated_at=clock_timestamp() WHERE job_id=$1 AND environment_id=$2 AND state='dispatching'", &[&id,&environment]).await?;
            tx.execute("UPDATE novu_queue_jobs SET status='failed',last_error='Slack outcome unknown',lease_token=NULL,lease_until=NULL WHERE id=$1", &[&id]).await?;
        } else {
            tx.execute("UPDATE novu_queue_jobs SET status='pending',lease_token=NULL,lease_until=NULL WHERE id=$1", &[&id]).await?;
        }
    }
    tx.commit().await?;
    Ok(rows.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };
    use tokio_postgres::NoTls;
    #[tokio::test]
    #[ignore = "requires disposable PostgreSQL and loopback HTTP"]
    async fn destinations_are_isolated_and_uncertain_attempts_are_not_retried() {
        let (mut db, driver) =
            tokio_postgres::connect(&std::env::var("TEST_DATABASE_URL").unwrap(), NoTls)
                .await
                .unwrap();
        tokio::spawn(async move {
            let _ = driver.await;
        });
        db.batch_execute("CREATE TABLE novu_rust_environments(id text PRIMARY KEY,organization_id text NOT NULL); INSERT INTO novu_rust_environments VALUES ('env','org'),('other','org')").await.unwrap();
        for migration in [
            crate::MIGRATION,
            crate::slack_queue::MIGRATION,
            MIGRATION,
            crate::reconciliation::MIGRATION,
            crate::reconciliation::OPERATOR_GRANTS_MIGRATION,
            crate::alerts::MIGRATION,
            crate::alerts::RESOLUTION_MIGRATION,
            crate::reconciliation::OPERATOR_GRANT_LOCK_MIGRATION,
            crate::slack_queue::OPERATOR_MIGRATION,
            crate::slack_queue::WORKFLOW_ATTENTION_MIGRATION,
        ] {
            db.batch_execute(migration).await.unwrap();
        }
        let first = Uuid::new_v4();
        let second = Uuid::new_v4();
        let other = Uuid::new_v4();
        for (id, env, dest) in [
            (first, "env", "a"),
            (second, "env", "b"),
            (other, "other", "a"),
        ] {
            crate::enqueue(
                &db,
                id,
                env,
                JOB_KIND,
                &json!({"destination":dest,"text":"hello"}),
            )
            .await
            .unwrap();
        }
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = [0; 4096];
            assert!(socket.read(&mut buf).await.unwrap() > 0);
            socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nX-Slack-Req-Id: req1\r\nConnection: close\r\n\r\nok").await.unwrap();
        });
        let provider = Webhook::test_endpoint(&url).unwrap();
        assert!(process_one(&mut db, "env", "a", &provider).await.unwrap());
        server.await.unwrap();
        assert!(!process_one(&mut db, "env", "a", &provider).await.unwrap());
        assert_eq!(
            db.query_one(
                "SELECT state FROM novu_rust_slack_attempts WHERE job_id=$1",
                &[&first]
            )
            .await
            .unwrap()
            .get::<_, String>(0),
            "accepted"
        );
        for id in [second, other] {
            assert_eq!(
                db.query_one("SELECT status FROM novu_queue_jobs WHERE id=$1", &[&id])
                    .await
                    .unwrap()
                    .get::<_, String>(0),
                "pending"
            );
        }
        db.execute("UPDATE novu_queue_jobs SET status='running',lease_token=$2,lease_until=clock_timestamp()-interval '1 second' WHERE id=$1", &[&second,&Uuid::new_v4()]).await.unwrap();
        db.execute("INSERT INTO novu_rust_slack_attempts(job_id,environment_id,state) VALUES ($1,'env','dispatching')", &[&second]).await.unwrap();
        assert_eq!(recover_expired(&mut db, "env", "a").await.unwrap(), 0);
        assert_eq!(recover_expired(&mut db, "env", "b").await.unwrap(), 1);
        assert_eq!(
            db.query_one(
                "SELECT state FROM novu_rust_slack_attempts WHERE job_id=$1",
                &[&second]
            )
            .await
            .unwrap()
            .get::<_, String>(0),
            "unknown"
        );
        assert_eq!(
            crate::slack_queue::health(&db, "env").await.unwrap()["unknownAttempts"],
            1
        );
        assert!(!process_one(&mut db, "env", "b", &provider).await.unwrap());
        assert!(
            crate::slack_queue::resolve(&mut db, "env", second, "retry", "operator", "verified")
                .await
                .is_err()
        );
        db.execute("INSERT INTO novu_rust_delivery_operator_grants VALUES ('env',session_user,'slack_retry')",&[]).await.unwrap();
        crate::slack_queue::resolve(&mut db, "env", second, "retry", "operator", "verified")
            .await
            .unwrap();
        assert_eq!(
            db.query_one("SELECT status FROM novu_queue_jobs WHERE id=$1", &[&second])
                .await
                .unwrap()
                .get::<_, String>(0),
            "pending"
        );
    }
}
