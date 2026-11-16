//! Standalone Telegram queue. Workflow and Novu management API are not connected.
use crate::{
    ClaimedJob,
    telegram::{Message, SendError, Telegram, request_body},
};
use tokio_postgres::{Client, Error, Transaction};
use uuid::Uuid;
pub const JOB_KIND: &str = "experimental_telegram";
async fn claim(db: &Client, env: &str, destination: &str) -> Result<Option<ClaimedJob>, Error> {
    let token = Uuid::new_v4();
    let row=db.query_opt("WITH c AS (SELECT id FROM public.novu_queue_jobs WHERE environment_id=$1 AND kind=$2 AND status='pending' AND payload->>'destination'=$4 AND available_at<=now() ORDER BY available_at,created_at,id FOR UPDATE SKIP LOCKED LIMIT 1) UPDATE public.novu_queue_jobs j SET status='running',attempts=j.attempts+1,lease_token=$3,lease_until=clock_timestamp()+interval '60 seconds' FROM c WHERE j.id=c.id RETURNING j.id,j.environment_id,j.kind,j.payload,j.attempts",&[&env,&JOB_KIND,&token,&destination]).await?;
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
    let row=tx.query_opt("SELECT lease_until>clock_timestamp() FROM public.novu_queue_jobs WHERE id=$1 AND environment_id=$2 AND kind=$3 AND status='running' AND lease_token=$4 FOR UPDATE",&[&job.id,&job.environment_id,&JOB_KIND,&job.lease_token]).await?;
    Ok(row.is_some_and(|r| r.get(0)))
}
pub async fn process_one(
    db: &mut Client,
    env: &str,
    destination: &str,
    provider: &Telegram,
) -> Result<bool, Error> {
    let Some(job) = claim(db, env, destination).await? else {
        return Ok(false);
    };
    let body = match serde_json::from_value::<Message>(job.payload.clone())
        .ok()
        .and_then(|m| request_body(&m).ok())
    {
        Some(v) => v,
        None => {
            crate::fail(db, job.id, job.lease_token, "invalid Telegram job").await?;
            return Ok(true);
        }
    };
    let tx = db.transaction().await?;
    if !live(&tx, &job).await? {
        tx.rollback().await?;
        return Ok(true);
    }
    let inserted=tx.execute("INSERT INTO public.novu_rust_telegram_attempts(job_id,environment_id,state) VALUES($1,$2,'dispatching') ON CONFLICT DO NOTHING",&[&job.id,&env]).await?;
    if inserted != 1 {
        tx.execute("UPDATE public.novu_queue_jobs SET status='failed',last_error='Telegram attempt requires review',lease_token=NULL,lease_until=NULL WHERE id=$1",&[&job.id]).await?;
        tx.commit().await?;
        return Ok(true);
    }
    if !live(&tx, &job).await? {
        tx.rollback().await?;
        return Ok(true);
    }
    tx.commit().await?;
    let (state, code, id, timestamp, diagnostic) = match provider.send(&body).await {
        Ok(v) => ("accepted", None, Some(v.id), Some(v.timestamp), None),
        Err(SendError::Rejected(code)) => (
            "rejected",
            Some(i32::from(code)),
            None,
            None,
            Some("Telegram rejected request"),
        ),
        Err(SendError::Invalid) => (
            "unknown",
            None,
            None,
            None,
            Some("Telegram outcome unknown"),
        ),
        Err(SendError::Unknown) => (
            "unknown",
            None,
            None,
            None,
            Some("Telegram outcome unknown"),
        ),
    };
    let tx = db.transaction().await?;
    if !live(&tx, &job).await? {
        tx.rollback().await?;
        return Ok(true);
    }
    let updated=tx.execute("UPDATE public.novu_rust_telegram_attempts SET state=$3,http_status=$4,provider_message_id=$5,provider_timestamp=$6,updated_at=clock_timestamp() WHERE job_id=$1 AND environment_id=$2 AND state='dispatching'",&[&job.id,&env,&state,&code,&id,&timestamp]).await?;
    if updated == 1 {
        let job_status = if state == "accepted" {
            "completed"
        } else {
            "failed"
        };
        tx.execute("UPDATE public.novu_queue_jobs SET status=$2,last_error=$3,lease_token=NULL,lease_until=NULL WHERE id=$1",&[&job.id,&job_status,&diagnostic]).await?;
    }
    tx.commit().await?;
    Ok(true)
}
pub async fn recover_expired(
    db: &mut Client,
    env: &str,
    destination: &str,
) -> Result<usize, Error> {
    let tx = db.transaction().await?;
    let rows=tx.query("SELECT id FROM public.novu_queue_jobs WHERE environment_id=$1 AND kind=$2 AND payload->>'destination'=$3 AND status='running' AND lease_until<=clock_timestamp() ORDER BY lease_until,id FOR UPDATE SKIP LOCKED LIMIT 100",&[&env,&JOB_KIND,&destination]).await?;
    for row in &rows {
        let id: Uuid = row.get(0);
        let attempted=tx.query_opt("SELECT 1 FROM public.novu_rust_telegram_attempts WHERE job_id=$1 AND environment_id=$2",&[&id,&env]).await?.is_some();
        if attempted {
            tx.execute("UPDATE public.novu_rust_telegram_attempts SET state='unknown',updated_at=clock_timestamp() WHERE job_id=$1 AND environment_id=$2 AND state='dispatching'",&[&id,&env]).await?;
            tx.execute("UPDATE public.novu_queue_jobs SET status='failed',last_error='Telegram outcome unknown',lease_token=NULL,lease_until=NULL WHERE id=$1",&[&id]).await?;
        } else {
            tx.execute("UPDATE public.novu_queue_jobs SET status='pending',lease_token=NULL,lease_until=NULL WHERE id=$1",&[&id]).await?;
        }
    }
    tx.commit().await?;
    Ok(rows.len())
}
