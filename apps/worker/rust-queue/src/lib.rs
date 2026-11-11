//! PostgreSQL queue primitives for the Rust rewrite. This is not yet a Novu-compatible worker.
pub mod in_app;

use serde_json::Value;
use std::time::Duration;
use tokio_postgres::{Client, Error};
use uuid::Uuid;

pub const MIGRATION: &str = include_str!("../migrations/0001_jobs.sql");

#[derive(Debug)]
pub struct ClaimedJob {
    pub id: Uuid,
    pub environment_id: String,
    pub kind: String,
    pub payload: Value,
    pub attempts: i32,
    pub lease_token: Uuid,
}

/// Enqueue in the caller's transaction when atomicity with domain state is needed.
pub async fn enqueue<C: tokio_postgres::GenericClient + Sync>(
    client: &C,
    id: Uuid,
    environment_id: &str,
    kind: &str,
    payload: &Value,
) -> Result<(), Error> {
    enqueue_delayed(client, id, environment_id, kind, payload, Duration::ZERO).await
}

/// Insert a job with a database-relative delay in the caller's transaction.
pub async fn enqueue_delayed<C: tokio_postgres::GenericClient + Sync>(
    client: &C,
    id: Uuid,
    environment_id: &str,
    kind: &str,
    payload: &Value,
    delay: Duration,
) -> Result<(), Error> {
    let seconds = delay.as_secs_f64();
    client
        .execute(
            "INSERT INTO novu_queue_jobs (id, environment_id, kind, payload, available_at) \
         VALUES ($1, $2, $3, $4, now() + $5 * interval '1 second')",
            &[&id, &environment_id, &kind, payload, &seconds],
        )
        .await?;
    Ok(())
}

/// Return an owned job to the queue after a caller-selected delay.
/// This operation does not determine whether it is safe to retry a side effect.
pub async fn retry(
    client: &Client,
    id: Uuid,
    token: Uuid,
    delay: Duration,
    error: &str,
) -> Result<bool, Error> {
    let seconds = delay.as_secs_f64();
    Ok(client.execute(
        "UPDATE novu_queue_jobs SET status = 'pending', available_at = now() + $3 * interval '1 second', \
         last_error = $4, lease_token = NULL, lease_until = NULL \
         WHERE id = $1 AND status = 'running' AND lease_token = $2 AND lease_until > now()",
        &[&id, &token, &seconds, &error],
    ).await? == 1)
}

/// Cancel only a pending job in the given environment. Running jobs are not interrupted.
pub async fn cancel_pending(
    client: &Client,
    environment_id: &str,
    id: Uuid,
) -> Result<bool, Error> {
    Ok(client
        .execute(
            "UPDATE novu_queue_jobs SET status = 'cancelled' \
         WHERE id = $1 AND environment_id = $2 AND status = 'pending'",
            &[&id, &environment_id],
        )
        .await?
        == 1)
}

/// Claim one due job. Expired jobs are recovered separately according to the caller's policy.
/// A fresh token fences stale workers from updating a re-claimed job.
pub async fn claim(
    client: &Client,
    kind: &str,
    lease: Duration,
) -> Result<Option<ClaimedJob>, Error> {
    let token = Uuid::new_v4();
    let seconds = lease.as_secs_f64();
    let row = client
        .query_opt(
            "WITH candidate AS (\
         SELECT id FROM novu_queue_jobs WHERE status = 'pending' AND kind = $3 AND available_at <= now() \
         ORDER BY available_at, created_at, id FOR UPDATE SKIP LOCKED LIMIT 1) \
         UPDATE novu_queue_jobs j SET status = 'running', attempts = j.attempts + 1, \
         lease_token = $1, lease_until = now() + $2 * interval '1 second' \
         FROM candidate WHERE j.id = candidate.id \
         RETURNING j.id, j.environment_id, j.kind, j.payload, j.attempts",
        &[&token, &seconds, &kind],
        )
        .await?;
    Ok(row.map(|r| ClaimedJob {
        id: r.get(0),
        environment_id: r.get(1),
        kind: r.get(2),
        payload: r.get(3),
        attempts: r.get(4),
        lease_token: token,
    }))
}

/// Completion is valid only while this exact lease is still live.
pub async fn complete(client: &Client, id: Uuid, token: Uuid) -> Result<bool, Error> {
    Ok(client.execute(
        "UPDATE novu_queue_jobs SET status = 'completed', lease_token = NULL, lease_until = NULL \
         WHERE id = $1 AND status = 'running' AND lease_token = $2 AND lease_until > now()",
        &[&id, &token],
    ).await? == 1)
}

/// Record a permanent failure. Callers decide which failures are non-retryable.
pub async fn fail(client: &Client, id: Uuid, token: Uuid, error: &str) -> Result<bool, Error> {
    Ok(client.execute(
        "UPDATE novu_queue_jobs SET status = 'failed', last_error = $3, lease_token = NULL, lease_until = NULL \
         WHERE id = $1 AND status = 'running' AND lease_token = $2 AND lease_until > now()",
        &[&id, &token, &error],
    ).await? == 1)
}

/// Renew only a lease that has not already expired.
pub async fn renew(client: &Client, id: Uuid, token: Uuid, lease: Duration) -> Result<bool, Error> {
    let seconds = lease.as_secs_f64();
    Ok(client
        .execute(
            "UPDATE novu_queue_jobs SET lease_until = now() + $3 * interval '1 second' \
         WHERE id = $1 AND status = 'running' AND lease_token = $2 AND lease_until > now()",
            &[&id, &token, &seconds],
        )
        .await?
        == 1)
}

/// Explicit recovery: requeue expired jobs. Not appropriate for at-most-once handlers
/// or external side effects unless the caller has resolved their duplicate-delivery policy.
pub async fn requeue_expired(client: &Client, kind: &str) -> Result<u64, Error> {
    client
        .execute(
            "UPDATE novu_queue_jobs SET status = 'pending', lease_token = NULL, lease_until = NULL \
         WHERE kind = $1 AND status = 'running' AND lease_until <= now()",
            &[&kind],
        )
        .await
}
