//! Standalone Discord jobs; this is not the Novu workflow integration.
use crate::{
    ClaimedJob,
    discord::{Webhook, request_body},
    slack::SendError,
};
use serde::Deserialize;
use serde_json::{Map, Value};
use tokio_postgres::{Client, Error, Transaction};
use uuid::Uuid;

pub const JOB_KIND: &str = "experimental_discord_webhook";
pub const MIGRATION: &str = include_str!("../migrations/0202_discord_webhook_queue.sql");

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Message {
    destination: String,
    content: String,
    #[serde(default, rename = "customData")]
    custom_data: Map<String, Value>,
    #[serde(default, rename = "providerData")]
    provider_data: Map<String, Value>,
    #[serde(default)]
    passthrough: Map<String, Value>,
}

async fn claim(
    db: &Client,
    environment: &str,
    destination: &str,
) -> Result<Option<ClaimedJob>, Error> {
    let token = Uuid::new_v4();
    let row = db.query_opt("WITH candidate AS (SELECT id FROM novu_queue_jobs WHERE environment_id=$1 AND kind=$2 AND status='pending' AND payload->>'destination'=$4 AND available_at<=now() ORDER BY available_at,created_at,id FOR UPDATE SKIP LOCKED LIMIT 1) UPDATE novu_queue_jobs j SET status='running',attempts=j.attempts+1,lease_token=$3,lease_until=clock_timestamp()+interval '60 seconds' FROM candidate WHERE j.id=candidate.id RETURNING j.id,j.environment_id,j.kind,j.payload,j.attempts", &[&environment,&JOB_KIND,&token,&destination]).await?;
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
    let row = tx.query_opt("SELECT 1 FROM novu_queue_jobs WHERE id=$1 AND environment_id=$2 AND kind=$3 AND status='running' AND lease_token=$4 FOR UPDATE", &[&job.id,&job.environment_id,&JOB_KIND,&job.lease_token]).await?;
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
    let body = serde_json::from_value::<Message>(job.payload.clone())
        .ok()
        .and_then(|m| {
            if m.destination != destination {
                return None;
            }
            request_body(&m.content, &m.custom_data, &m.provider_data, &m.passthrough).ok()
        });
    let Some(body) = body else {
        crate::fail(db, job.id, job.lease_token, "invalid Discord job").await?;
        return Ok(true);
    };
    let tx = db.transaction().await?;
    if !live(&tx, &job).await? {
        tx.rollback().await?;
        return Ok(true);
    }
    let inserted = tx.execute("INSERT INTO novu_rust_discord_attempts(job_id,environment_id,state) VALUES ($1,$2,'dispatching') ON CONFLICT DO NOTHING", &[&job.id,&environment]).await?;
    if inserted != 1 {
        tx.execute("UPDATE novu_queue_jobs SET status='failed',last_error='Discord attempt requires review',lease_token=NULL,lease_until=NULL WHERE id=$1", &[&job.id]).await?;
        tx.commit().await?;
        return Ok(true);
    }
    tx.commit().await?;
    let (state, code, id, timestamp, diagnostic, retry_after) = match provider.send(&body).await {
        Ok(result) => (
            "accepted",
            None,
            Some(result.id),
            Some(result.timestamp),
            None,
            None,
        ),
        Err(SendError::Rejected(code)) => (
            "rejected",
            code.map(i32::from),
            None,
            None,
            Some("Discord rejected request"),
            None,
        ),
        Err(SendError::RateLimited(seconds)) => (
            "rejected",
            Some(429),
            None,
            None,
            Some("Discord rate limited request"),
            seconds.map(|s| s as i32),
        ),
        Err(SendError::Invalid) => (
            "rejected",
            None,
            None,
            None,
            Some("invalid Discord request"),
            None,
        ),
        Err(SendError::Unknown) => (
            "unknown",
            None,
            None,
            None,
            Some("Discord outcome unknown"),
            None,
        ),
    };
    let tx = db.transaction().await?;
    if !live(&tx, &job).await? {
        tx.rollback().await?;
        return Ok(true);
    }
    let updated = tx.execute("UPDATE novu_rust_discord_attempts SET state=$3,http_status=$4,provider_message_id=$5,provider_timestamp=$6,retry_after_seconds=$7,updated_at=clock_timestamp() WHERE job_id=$1 AND environment_id=$2 AND state='dispatching'", &[&job.id,&environment,&state,&code,&id,&timestamp,&retry_after]).await?;
    if updated == 1 {
        let status = if state == "accepted" {
            "completed"
        } else {
            "failed"
        };
        tx.execute("UPDATE novu_queue_jobs SET status=$2,last_error=$3,lease_token=NULL,lease_until=NULL WHERE id=$1", &[&job.id,&status,&diagnostic]).await?;
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
    let rows = tx.query("SELECT id FROM novu_queue_jobs WHERE environment_id=$1 AND kind=$2 AND payload->>'destination'=$3 AND status='running' AND lease_until<=clock_timestamp() ORDER BY lease_until,id FOR UPDATE SKIP LOCKED LIMIT 100", &[&environment,&JOB_KIND,&destination]).await?;
    for row in &rows {
        let id: Uuid = row.get(0);
        let attempted = tx
            .query_opt(
                "SELECT 1 FROM novu_rust_discord_attempts WHERE job_id=$1 AND environment_id=$2",
                &[&id, &environment],
            )
            .await?
            .is_some();
        if attempted {
            tx.execute("UPDATE novu_rust_discord_attempts SET state='unknown',updated_at=clock_timestamp() WHERE job_id=$1 AND environment_id=$2 AND state='dispatching'", &[&id,&environment]).await?;
            tx.execute("UPDATE novu_queue_jobs SET status='failed',last_error='Discord outcome unknown',lease_token=NULL,lease_until=NULL WHERE id=$1", &[&id]).await?;
        } else {
            tx.execute("UPDATE novu_queue_jobs SET status='pending',lease_token=NULL,lease_until=NULL WHERE id=$1", &[&id]).await?;
        }
    }
    tx.commit().await?;
    Ok(rows.len())
}
