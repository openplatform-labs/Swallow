//! Minimal experimental single-step in-app processor. Not a Novu Inbox implementation.
use std::{collections::HashSet, error::Error, time::Duration};
use tokio_postgres::Client;
use uuid::Uuid;

pub const MIGRATION: &str = include_str!("../migrations/0002_experimental_in_app.sql");
pub const JOB_KIND: &str = novu_rust_contracts::EXPERIMENTAL_IN_APP_JOB_KIND;
type WorkerResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

/// Preserve the first insertion order of subscriber IDs, as the reference Map does.
pub fn unique_recipient_ids(recipients: &[String]) -> Vec<&str> {
    let mut seen = HashSet::new();
    recipients
        .iter()
        .map(String::as_str)
        .filter(|s| seen.insert(*s))
        .collect()
}

/// Atomically create subscribers/messages and complete the claimed job.
/// Other Novu step types, complex templates, preferences and integrations are not supported.
pub async fn process_one(client: &mut Client) -> WorkerResult<bool> {
    let Some(job) = crate::claim(client, JOB_KIND, Duration::from_secs(60)).await? else {
        return Ok(false);
    };
    let data = &job.payload;
    let id = |key| {
        data.get(key)
            .and_then(|v| v.as_str())
            .map(str::to_owned)
            .ok_or_else(|| format!("missing {key}"))
    };
    let workflow = id("workflow")?;
    let transaction_id = id("transactionId")?;
    let content = id("content")?;
    let payload = data
        .get("payload")
        .filter(|v| v.is_object())
        .ok_or("missing payload")?;
    let recipients = data
        .get("recipients")
        .and_then(|v| v.as_array())
        .ok_or("missing recipients")?;
    let recipients: Vec<String> = recipients
        .iter()
        .map(|recipient| {
            recipient
                .as_str()
                .filter(|v| !v.is_empty())
                .map(str::to_owned)
                .ok_or("invalid recipient")
        })
        .collect::<Result<_, _>>()?;
    let unique = unique_recipient_ids(&recipients);
    let tx = client.transaction().await?;
    // Hold the job row lock until domain writes and completion commit together.
    let owner = tx.query_opt("SELECT id FROM novu_queue_jobs WHERE id = $1 AND status = 'running' AND lease_token = $2 AND lease_until > now() FOR UPDATE", &[&job.id, &job.lease_token]).await?;
    if owner.is_none() {
        tx.rollback().await?;
        return Err("job lease expired".into());
    }
    for subscriber in unique {
        let rendered_content =
            novu_rust_contracts::template::render_in_app(&content, payload, subscriber)?;
        tx.execute("INSERT INTO novu_rust_subscribers (environment_id, subscriber_id) VALUES ($1, $2) ON CONFLICT DO NOTHING", &[&job.environment_id, &subscriber]).await?;
        tx.execute("INSERT INTO novu_rust_in_app_messages (id, environment_id, subscriber_id, workflow_identifier, transaction_id, content, payload) VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (environment_id, subscriber_id, workflow_identifier, transaction_id) DO NOTHING", &[&Uuid::new_v4(), &job.environment_id, &subscriber, &workflow, &transaction_id, &rendered_content, &payload]).await?;
    }
    let completed = tx.execute("UPDATE novu_queue_jobs SET status = 'completed', lease_token = NULL, lease_until = NULL WHERE id = $1 AND lease_token = $2 AND lease_until > clock_timestamp()", &[&job.id, &job.lease_token]).await?;
    if completed != 1 {
        tx.rollback().await?;
        return Err("job lease expired before completion".into());
    }
    tx.commit().await?;
    Ok(true)
}
