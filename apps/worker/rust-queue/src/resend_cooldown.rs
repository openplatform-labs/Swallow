//! Optional environment-wide Resend 429 dispatch guard.
use crate::ClaimedJob;
use tokio_postgres::{Error, Transaction};

pub fn enabled_from_env() -> Result<bool, &'static str> {
    match std::env::var("NOVU_RESEND_SHARED_COOLDOWN").as_deref() {
        Ok("true") => Ok(true),
        Ok("false") | Err(std::env::VarError::NotPresent) => Ok(false),
        _ => Err("invalid Resend shared cooldown setting"),
    }
}

/// The caller has already locked and checked its current lease. The policy's
/// shared lock is held until the dispatch reservation commits.
pub async fn defer(tx: &Transaction<'_>, job: &ClaimedJob) -> Result<bool, Error> {
    let until: Option<String> = tx
        .query_one(
            "SELECT public.novu_rust_resend_cooldown_check($1)",
            &[&job.environment_id],
        )
        .await?
        .get(0);
    if let Some(until) = until {
        tx.execute("UPDATE public.novu_queue_jobs SET status='pending',available_at=$3::text::timestamptz,last_error='Resend shared cooldown active',lease_token=NULL,lease_until=NULL WHERE id=$1 AND lease_token=$2", &[&job.id,&job.lease_token,&until]).await?;
        return Ok(true);
    }
    Ok(false)
}

/// Update only after the 429 attempt was successfully fenced by its lease.
pub async fn record(
    tx: &Transaction<'_>,
    environment: &str,
    seconds: Option<i32>,
) -> Result<(), Error> {
    tx.query_one(
        "SELECT public.novu_rust_resend_cooldown_record($1,$2)",
        &[&environment, &seconds],
    )
    .await?;
    Ok(())
}

