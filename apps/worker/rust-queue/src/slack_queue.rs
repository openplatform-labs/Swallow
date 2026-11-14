//! Environment-scoped Slack app jobs and workflow delivery reconciliation.
use crate::{
    ClaimedJob,
    slack::{Message, SendError, Slack},
};
use tokio_postgres::{Client, Error, Transaction};
use uuid::Uuid;

pub const JOB_KIND: &str = "experimental_slack_chat";
pub const MIGRATION: &str = include_str!("../migrations/0070_slack_attempts.sql");
pub const OPERATOR_MIGRATION: &str =
    include_str!("../migrations/0071_slack_operator_permissions.sql");
pub const WORKFLOW_ATTENTION_MIGRATION: &str =
    include_str!("../migrations/0079_slack_workflow_attention.sql");
pub const ATTENTION_REVISION_MIGRATION: &str =
    include_str!("../migrations/0092_slack_attention_revision.sql");
pub const RESOLVE_FUNCTION_MIGRATION: &str =
    include_str!("../migrations/0083_slack_resolve_function.sql");

/// Acknowledge one pre-dispatch failure or missing-endpoint result without changing the job.
pub async fn acknowledge_workflow(
    client: &mut Client,
    environment: &str,
    job: Uuid,
    operator: &str,
    reason: &str,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    let row = client
        .query_one(
            "SELECT public.novu_rust_ack_slack_workflow($1,$2,$3,$4)",
            &[&environment, &job, &operator, &reason],
        )
        .await?;
    Ok(row.get(0))
}

/// Operator assertion after independently checking the provider. Retry can duplicate a message.
pub async fn resolve(
    client: &mut Client,
    environment: &str,
    job: Uuid,
    decision: &str,
    operator: &str,
    reason: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    client
        .query_one(
            "SELECT public.novu_rust_resolve_slack($1,$2,$3,$4,$5)",
            &[&environment, &job, &decision, &operator, &reason],
        )
        .await?;
    Ok(())
}

/// Non-secret snapshot of attempts requiring review in one environment.
pub async fn health(
    client: &Client,
    environment: &str,
) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
    if environment.trim().is_empty() {
        return Err("environment required".into());
    }
    let row=client.query_one("SELECT count(*) FILTER (WHERE a.state='unknown'), count(*) FILTER (WHERE a.state='rejected'), count(*) FILTER (WHERE a.state='dispatching' AND (j.status<>'running' OR j.lease_until<=statement_timestamp())), EXISTS(SELECT 1 FROM novu_rust_environments WHERE id=$1), (SELECT count(*) FROM novu_queue_jobs x WHERE x.environment_id=$1 AND x.kind=ANY($3) AND x.status='failed' AND x.last_error IS NOT NULL AND NOT EXISTS(SELECT 1 FROM novu_rust_slack_attempts a2 WHERE a2.job_id=x.id) AND NOT EXISTS(SELECT 1 FROM novu_rust_slack_workflow_acknowledgements z WHERE z.environment_id=x.environment_id AND z.job_id=x.id AND z.job_attempts=x.attempts AND z.job_status=x.status AND z.job_error=x.last_error AND z.job_attention_revision=x.slack_attention_revision)), (SELECT count(*) FROM novu_queue_jobs x WHERE x.environment_id=$1 AND x.kind=$4 AND x.status='cancelled' AND x.last_error='no Slack endpoints' AND NOT EXISTS(SELECT 1 FROM novu_rust_slack_workflow_acknowledgements z WHERE z.environment_id=x.environment_id AND z.job_id=x.id AND z.job_attempts=x.attempts AND z.job_status=x.status AND z.job_error=x.last_error AND z.job_attention_revision=x.slack_attention_revision)) FROM novu_rust_slack_attempts a JOIN novu_queue_jobs j ON j.id=a.job_id AND j.environment_id=a.environment_id WHERE a.environment_id=$1 AND j.kind=ANY($2)", &[&environment,&vec![JOB_KIND,crate::slack_webhook_queue::JOB_KIND,crate::slack_workflow::DELIVERY_KIND],&vec![crate::slack_workflow::FANOUT_KIND,crate::slack_workflow::DELIVERY_KIND],&crate::slack_workflow::FANOUT_KIND]).await?;
    if !row.get::<_, bool>(3) {
        return Err("environment not found".into());
    }
    Ok(
        serde_json::json!({"unknownAttempts":row.get::<_,i64>(0),"rejectedAttempts":row.get::<_,i64>(1),"strandedDispatches":row.get::<_,i64>(2),"preDispatchFailures":row.get::<_,i64>(4),"missingEndpoints":row.get::<_,i64>(5)}),
    )
}
/// Environment-authorized aggregate health without direct table privileges.
pub async fn scoped_health(
    client: &Client,
    environment: &str,
) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
    let row = client
        .query_one(
            "SELECT * FROM public.novu_rust_slack_health($1)",
            &[&environment],
        )
        .await?;
    Ok(
        serde_json::json!({"unknownAttempts":row.get::<_,i64>(0),"rejectedAttempts":row.get::<_,i64>(1),"strandedDispatches":row.get::<_,i64>(2),"preDispatchFailures":row.get::<_,i64>(3),"missingEndpoints":row.get::<_,i64>(4)}),
    )
}

/// Optional count of unacknowledged workflow-policy cancellations.
pub async fn policy_cancellations(
    client: &Client,
    environment: &str,
    scoped: bool,
) -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
    if environment.trim().is_empty() {
        return Err("environment required".into());
    }
    if scoped {
        return Ok(client
            .query_one(
                "SELECT public.novu_rust_slack_policy_cancelled($1)",
                &[&environment],
            )
            .await?
            .get(0));
    }
    let row = client.query_one("SELECT EXISTS(SELECT 1 FROM public.novu_rust_environments WHERE id=$1),(SELECT count(*) FROM public.novu_queue_jobs j WHERE j.environment_id=$1 AND j.kind IN ('experimental_slack_workflow_fanout','experimental_slack_workflow_delivery') AND j.status='cancelled' AND j.last_error IN ('Slack workflow step disabled','Slack workflow content changed') AND NOT EXISTS(SELECT 1 FROM public.novu_rust_slack_attempts a WHERE a.job_id=j.id) AND NOT EXISTS(SELECT 1 FROM public.novu_rust_slack_workflow_acknowledgements z WHERE z.environment_id=j.environment_id AND z.job_id=j.id AND z.job_attempts=j.attempts AND z.job_status=j.status AND z.job_error=j.last_error AND z.job_attention_revision=j.slack_attention_revision))", &[&environment]).await?;
    if !row.get::<_, bool>(0) {
        return Err("environment not found".into());
    }
    Ok(row.get(1))
}

/// List the same Slack attention categories as health, without payloads or error text.
/// The UUID cursor is exclusive; each page contains at most 100 jobs.
pub async fn list_attention(
    client: &Client,
    environment: &str,
    after: Option<Uuid>,
) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error + Send + Sync>> {
    let rows = client
        .query(
            "SELECT * FROM public.novu_rust_list_slack_attention($1,$2)",
            &[&environment, &after],
        )
        .await?;
    Ok(rows
        .into_iter()
        .map(|row| {
            serde_json::json!({
                "jobId":row.get::<_,Uuid>(0).to_string(), "kind":row.get::<_,String>(1),
                "jobStatus":row.get::<_,String>(2), "attempts":row.get::<_,i32>(3),
                "httpStatus":row.get::<_,Option<i32>>(4), "category":row.get::<_,String>(5)
            })
        })
        .collect())
}

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
    provider: &Slack,
) -> Result<bool, Error> {
    let Some(job) = claim(db, environment).await? else {
        return Ok(false);
    };
    let message: Message = match serde_json::from_value(job.payload.clone()) {
        Ok(m) => m,
        Err(_) => {
            crate::fail(db, job.id, job.lease_token, "invalid Slack job").await?;
            return Ok(true);
        }
    };
    if crate::slack::request_body(&message).is_err() {
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

pub async fn recover_expired(db: &mut Client, environment: &str) -> Result<usize, Error> {
    let tx = db.transaction().await?;
    let rows=tx.query("SELECT id FROM novu_queue_jobs WHERE environment_id=$1 AND kind=$2 AND status='running' AND lease_until<=clock_timestamp() ORDER BY lease_until,id FOR UPDATE SKIP LOCKED LIMIT 100", &[&environment,&JOB_KIND]).await?;
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
        sync::oneshot,
    };
    use tokio_postgres::NoTls;

    async fn mock() -> (
        String,
        oneshot::Receiver<String>,
        oneshot::Sender<()>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        let (request_tx, request_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            let mut chunk = [0; 4096];
            loop {
                let n = stream.read(&mut chunk).await.unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&chunk[..n]);
                if let Some(p) = bytes.windows(4).position(|x| x == b"\r\n\r\n") {
                    let h = String::from_utf8_lossy(&bytes[..p]);
                    let len: usize = h
                        .lines()
                        .find_map(|l| {
                            l.to_ascii_lowercase()
                                .strip_prefix("content-length: ")
                                .and_then(|x| x.parse().ok())
                        })
                        .unwrap();
                    if bytes.len() >= p + 4 + len {
                        break;
                    }
                }
            }
            request_tx.send(String::from_utf8(bytes).unwrap()).unwrap();
            release_rx.await.unwrap();
            let body = r#"{"ok":true,"channel":"C123","ts":"1.2"}"#;
            stream
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
        (url, request_rx, release_tx, task)
    }
    #[tokio::test]
    #[ignore = "requires disposable PostgreSQL and loopback HTTP"]
    async fn queue_fences_concurrency_and_recovers_uncertain_attempts() {
        let url = std::env::var("TEST_DATABASE_URL").unwrap();
        let (mut a, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
        tokio::spawn(async move {
            let _ = driver.await;
        });
        let (mut b, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
        tokio::spawn(async move {
            let _ = driver.await;
        });
        a.batch_execute("CREATE TABLE novu_rust_environments(id text PRIMARY KEY,organization_id text NOT NULL); INSERT INTO novu_rust_environments VALUES ('a','org'),('b','org')").await.unwrap();
        a.batch_execute(crate::MIGRATION).await.unwrap();
        a.batch_execute(MIGRATION).await.unwrap();
        a.batch_execute(MIGRATION).await.unwrap();
        a.batch_execute(WORKFLOW_ATTENTION_MIGRATION).await.unwrap();
        a.batch_execute(ATTENTION_REVISION_MIGRATION).await.unwrap();
        let id = Uuid::new_v4();
        crate::enqueue(
            &a,
            id,
            "a",
            JOB_KIND,
            &json!({"channel":"C123","text":"hello","customData":{"channel":"C999","thread_ts":"1.2"}}),
        )
        .await
        .unwrap();
        let (endpoint, received, release, server) = mock().await;
        let slack = Slack::test_endpoint("fixture-token".into(), &endpoint).unwrap();
        let run = tokio::spawn(async move {
            let result = process_one(&mut a, "a", &slack).await;
            (a, result)
        });
        let request = received.await.unwrap();
        assert!(request.contains("fixture-token"));
        let (_, body) = request.split_once("\r\n\r\n").unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(body).unwrap(),
            json!({"channel":"C999","text":"hello","thread_ts":"1.2"})
        );
        let (unused, _, _, unused_server) = mock().await;
        let other = Slack::test_endpoint("fixture-other".into(), &unused).unwrap();
        assert!(!process_one(&mut b, "a", &other).await.unwrap());
        unused_server.abort();
        assert!(!process_one(&mut b, "b", &other).await.unwrap());
        release.send(()).unwrap();
        server.await.unwrap();
        let (mut a, result) = run.await.unwrap();
        assert!(result.unwrap());
        let row=a.query_one("SELECT j.status,a.state,a.provider_message_id FROM novu_queue_jobs j JOIN novu_rust_slack_attempts a ON a.job_id=j.id WHERE j.id=$1", &[&id]).await.unwrap();
        assert_eq!(row.get::<_, String>(0), "completed");
        assert_eq!(row.get::<_, String>(1), "accepted");
        assert_eq!(row.get::<_, String>(2), "C123:1.2");
        let state=a.query_one("SELECT payload::text || coalesce(provider_message_id,'') FROM novu_queue_jobs j JOIN novu_rust_slack_attempts a ON a.job_id=j.id WHERE j.id=$1", &[&id]).await.unwrap().get::<_,String>(0);
        assert!(!state.contains("fixture-token"));
        let before = Uuid::new_v4();
        let after = Uuid::new_v4();
        for id in [before, after] {
            crate::enqueue(
                &a,
                id,
                "a",
                JOB_KIND,
                &json!({"channel":"C123","text":"hello"}),
            )
            .await
            .unwrap();
            a.execute("UPDATE novu_queue_jobs SET status='running',lease_token=$2,lease_until=clock_timestamp()-interval '1 second' WHERE id=$1", &[&id,&Uuid::new_v4()]).await.unwrap();
        }
        a.execute("INSERT INTO novu_rust_slack_attempts(job_id,environment_id,state) VALUES ($1,'a','dispatching')", &[&after]).await.unwrap();
        assert_eq!(recover_expired(&mut b, "b").await.unwrap(), 0);
        assert_eq!(recover_expired(&mut b, "a").await.unwrap(), 2);
        assert_eq!(
            a.query_one("SELECT status FROM novu_queue_jobs WHERE id=$1", &[&before])
                .await
                .unwrap()
                .get::<_, String>(0),
            "pending"
        );
        let row=a.query_one("SELECT j.status,a.state FROM novu_queue_jobs j JOIN novu_rust_slack_attempts a ON a.job_id=j.id WHERE j.id=$1", &[&after]).await.unwrap();
        assert_eq!(row.get::<_, String>(0), "failed");
        assert_eq!(row.get::<_, String>(1), "unknown");
        assert_eq!(health(&a, "a").await.unwrap()["unknownAttempts"], 1);
        assert!(health(&a, "missing").await.is_err());
        a.execute(
            "UPDATE novu_queue_jobs SET status='cancelled' WHERE id=$1",
            &[&before],
        )
        .await
        .unwrap();
        let late = Uuid::new_v4();
        crate::enqueue(
            &a,
            late,
            "a",
            JOB_KIND,
            &json!({"channel":"C123","text":"late"}),
        )
        .await
        .unwrap();
        let (endpoint, received, release, server) = mock().await;
        let provider = Slack::test_endpoint("fixture-token".into(), &endpoint).unwrap();
        let run = tokio::spawn(async move {
            let result = process_one(&mut a, "a", &provider).await;
            (a, result)
        });
        received.await.unwrap();
        b.execute("UPDATE novu_queue_jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1", &[&late]).await.unwrap();
        assert_eq!(recover_expired(&mut b, "a").await.unwrap(), 1);
        release.send(()).unwrap();
        server.await.unwrap();
        let (a, result) = run.await.unwrap();
        assert!(result.unwrap());
        let row=a.query_one("SELECT j.status,a.state FROM novu_queue_jobs j JOIN novu_rust_slack_attempts a ON a.job_id=j.id WHERE j.id=$1", &[&late]).await.unwrap();
        assert_eq!(row.get::<_, String>(0), "failed");
        assert_eq!(row.get::<_, String>(1), "unknown");
    }
}
