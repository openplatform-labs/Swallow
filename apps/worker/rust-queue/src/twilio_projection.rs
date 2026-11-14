//! Conservative, eventually consistent Twilio callback projection.
use crate::{reconciliation::sms_delivery, twilio_queue};
use serde_json::Value;
use tokio_postgres::{Client, IsolationLevel};
use uuid::Uuid;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
pub const MIGRATION: &str = include_str!("../migrations/0069_twilio_delivery_projection.sql");

fn classify(evidence: &Value) -> &'static str {
    if evidence["attemptState"] != "accepted" || evidence["providerIdKnown"] != true {
        return "unknown";
    }
    match evidence["providerState"].as_str() {
        Some("delivered") => "delivered",
        Some("failed") => "failed",
        Some("pending") => "pending",
        Some("conflict") => "conflicting",
        _ => "unknown",
    }
}

pub async fn project(client: &mut Client, environment: &str, job: Uuid) -> Result<bool> {
    let tx = client
        .build_transaction()
        .isolation_level(IsolationLevel::RepeatableRead)
        .start()
        .await?;
    let found = tx.query_opt("SELECT id FROM novu_queue_jobs WHERE id=$1 AND environment_id=$2 AND kind=$3 FOR UPDATE SKIP LOCKED", &[&job,&environment,&twilio_queue::JOB_KIND]).await?;
    if found.is_none() {
        tx.rollback().await?;
        return Ok(false);
    }
    // A concurrent retry can remove the attempt between paging and this lock.
    let exists: bool = tx.query_one("SELECT EXISTS(SELECT 1 FROM novu_rust_twilio_attempts WHERE job_id=$1 AND environment_id=$2)", &[&job,&environment]).await?.get(0);
    if !exists {
        tx.rollback().await?;
        return Ok(false);
    }
    let evidence = sms_delivery(&tx, environment, job).await?;
    let result = classify(&evidence);
    tx.execute("UPDATE novu_queue_jobs SET twilio_delivery_result=$3 WHERE id=$1 AND environment_id=$2 AND twilio_delivery_result IS DISTINCT FROM $3", &[&job,&environment,&result]).await?;
    tx.execute("UPDATE novu_rust_sms_messages SET twilio_delivery_result=$3 WHERE sms_job_id=$1 AND environment_id=$2 AND twilio_delivery_result IS DISTINCT FROM $3", &[&job,&environment,&result]).await?;
    tx.commit().await?;
    Ok(true)
}

pub async fn page(client: &Client, environment: &str, after: Option<Uuid>) -> Result<Vec<Uuid>> {
    let rows=client.query("SELECT j.id FROM novu_queue_jobs j JOIN novu_rust_twilio_attempts a ON a.job_id=j.id AND a.environment_id=j.environment_id WHERE j.environment_id=$1 AND j.kind=$2 AND ($3::uuid IS NULL OR j.id>$3) ORDER BY j.id LIMIT 100", &[&environment,&twilio_queue::JOB_KIND,&after]).await?;
    Ok(rows.into_iter().map(|r| r.get(0)).collect())
}
