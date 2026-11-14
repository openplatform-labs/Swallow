//! Eventually consistent, conservative Resend provider delivery projection.
use crate::{reconciliation::resend_evidence, resend_queue};
use serde_json::Value;
use tokio_postgres::{Client, IsolationLevel};
use uuid::Uuid;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
pub const MIGRATION: &str = include_str!("../migrations/0063_resend_delivery_projection.sql");
pub const HEARTBEAT_MIGRATION: &str =
    include_str!("../migrations/0065_resend_projection_heartbeat.sql");

pub(crate) fn classify(evidence: &Value) -> &'static str {
    let Some(recipients) = evidence["recipients"].as_array() else {
        return "unknown";
    };
    if evidence["attemptState"] != "accepted"
        || evidence["providerIdKnown"] != true
        || evidence["unmatchedEvents"] != 0
        || recipients.is_empty()
    {
        return "unknown";
    }
    if recipients
        .iter()
        .any(|r| matches!(r["result"].as_str(), Some("unlinked" | "ambiguous") | None))
    {
        return "unknown";
    }
    if recipients.iter().any(|r| r["result"] == "conflicting") {
        return "conflicting";
    }
    if recipients.iter().any(|r| r["complained"] != false) {
        return "complained";
    }
    if recipients.iter().any(|r| r["result"] == "pending") {
        return "pending";
    }
    let delivered = recipients
        .iter()
        .filter(|r| r["result"] == "delivered")
        .count();
    let failed = recipients
        .iter()
        .filter(|r| {
            matches!(
                r["result"].as_str(),
                Some("bounced" | "failed" | "suppressed")
            )
        })
        .count();
    if delivered + failed != recipients.len() {
        "unknown"
    } else if delivered == recipients.len() {
        "delivered"
    } else if failed == recipients.len() {
        "failed"
    } else {
        "partial"
    }
}

/// Lock the queue job and evaluate evidence in a repeatable-read snapshot.
pub async fn project(client: &mut Client, environment: &str, job: Uuid) -> Result<bool> {
    let tx = client
        .build_transaction()
        .isolation_level(IsolationLevel::RepeatableRead)
        .start()
        .await?;
    let found=tx.query_opt("SELECT id FROM novu_queue_jobs WHERE id=$1 AND environment_id=$2 AND kind=$3 FOR UPDATE SKIP LOCKED", &[&job,&environment,&resend_queue::JOB_KIND]).await?;
    if found.is_none() {
        tx.rollback().await?;
        return Ok(false);
    }
    let evidence = resend_evidence(&tx, environment, job).await?;
    let result = classify(&evidence);
    tx.execute("UPDATE novu_queue_jobs SET resend_delivery_result=$3 WHERE id=$1 AND environment_id=$2 AND resend_delivery_result IS DISTINCT FROM $3", &[&job,&environment,&result]).await?;
    tx.execute("UPDATE novu_rust_email_messages SET resend_delivery_result=$3 WHERE email_job_id=$1 AND environment_id=$2 AND resend_delivery_result IS DISTINCT FROM $3", &[&job,&environment,&result]).await?;
    tx.commit().await?;
    Ok(true)
}

pub async fn page(client: &Client, environment: &str, after: Option<Uuid>) -> Result<Vec<Uuid>> {
    let rows=client.query("SELECT j.id FROM novu_queue_jobs j JOIN novu_rust_resend_attempts a ON a.job_id=j.id AND a.environment_id=j.environment_id WHERE j.environment_id=$1 AND j.kind=$2 AND ($3::uuid IS NULL OR j.id>$3) ORDER BY j.id LIMIT 100", &[&environment,&resend_queue::JOB_KIND,&after]).await?;
    Ok(rows.into_iter().map(|r| r.get(0)).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn results_are_conservative_for_mixed_and_conflicting_evidence() {
        let mut evidence = json!({"attemptState":"accepted","providerIdKnown":true,"unmatchedEvents":0,"recipients":[{"result":"delivered","complained":false},{"result":"bounced","complained":false}]});
        assert_eq!(classify(&evidence), "partial");
        evidence["recipients"][1]["result"] = json!("pending");
        assert_eq!(classify(&evidence), "pending");
        evidence["recipients"][1]["result"] = json!("conflicting");
        assert_eq!(classify(&evidence), "conflicting");
        evidence["unmatchedEvents"] = json!(1);
        assert_eq!(classify(&evidence), "unknown");
        evidence["unmatchedEvents"] = json!(0);
        evidence["recipients"][1]["result"] = json!("delivered");
        evidence["recipients"][1]["complained"] = json!(true);
        assert_eq!(classify(&evidence), "complained");
    }
}
