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
        || evidence["autoCorrelationConflict"] == true
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
    project_with_auto_correlation(client, environment, job, false).await
}

/// Opt-in recovery uses only a uniquely linked authenticated delivery event.
pub async fn project_with_auto_correlation(
    client: &mut Client,
    environment: &str,
    job: Uuid,
    auto: bool,
) -> Result<bool> {
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
    if auto {
        // Do not auto-resolve retries: an older attempt may also have reached
        // the provider. Require every tagged event to identify a recorded
        // recipient and a single provider ID, with no competing evidence.
        let candidate = tx.query_opt("SELECT a.correlation_id,min(e.email_id),count(*) FROM novu_rust_resend_attempts a JOIN novu_queue_jobs j ON j.id=a.job_id AND j.environment_id=a.environment_id JOIN novu_rust_resend_delivery_events e ON e.environment_id=a.environment_id AND e.correlation_id=a.correlation_id WHERE a.environment_id=$1 AND a.job_id=$2 AND a.state='unknown' AND a.provider_message_id IS NULL AND a.correlation_id IS NOT NULL AND j.status='failed' AND j.last_error='Resend outcome unknown' AND NOT EXISTS(SELECT 1 FROM novu_rust_delivery_reconciliations r WHERE r.environment_id=a.environment_id AND r.job_id=a.job_id AND r.decision='retry' AND r.target_index IS NULL) GROUP BY a.correlation_id HAVING count(DISTINCT e.email_id)=1 AND bool_and(e.recipient_hash IS NOT NULL AND EXISTS(SELECT 1 FROM novu_rust_resend_recipients r WHERE r.environment_id=a.environment_id AND r.job_id=a.job_id AND r.recipient_hash=e.recipient_hash)) AND bool_or(e.event_type IN ('email.sent','email.delivered','email.bounced','email.failed','email.suppressed','email.complained'))", &[&environment,&job]).await?;
        if let Some(row) = candidate {
            let tag: Uuid = row.get(0);
            let provider_id: String = row.get(1);
            let count: i64 = row.get(2);
            let conflict: bool = tx.query_one("SELECT EXISTS(SELECT 1 FROM novu_rust_resend_attempts a WHERE a.environment_id=$1 AND a.provider_message_id=$3 AND a.job_id<>$2) OR EXISTS(SELECT 1 FROM novu_rust_delivery_reconciliations r WHERE r.environment_id=$1 AND r.retry_provider_message_id=$3) OR EXISTS(SELECT 1 FROM novu_rust_resend_delivery_events e WHERE e.environment_id=$1 AND e.email_id=$3 AND e.correlation_id IS DISTINCT FROM $4)", &[&environment,&job,&provider_id,&tag]).await?.get(0);
            if !conflict {
                tx.execute("INSERT INTO novu_rust_resend_auto_correlations(job_id,environment_id,correlation_id,provider_message_id,evidence_count) VALUES ($1,$2,$3,$4,$5)", &[&job,&environment,&tag,&provider_id,&count]).await?;
                tx.execute("UPDATE novu_rust_resend_attempts SET state='accepted',provider_message_id=$3,updated_at=clock_timestamp() WHERE environment_id=$1 AND job_id=$2 AND state='unknown' AND provider_message_id IS NULL", &[&environment,&job,&provider_id]).await?;
                tx.execute("UPDATE novu_queue_jobs SET status='completed',last_error=NULL WHERE environment_id=$1 AND id=$2", &[&environment,&job]).await?;
                tx.execute("UPDATE novu_rust_email_messages SET delivery_state='accepted',status='sent',error_id=NULL,error_text=NULL,identifier=$3 WHERE environment_id=$1 AND email_job_id=$2", &[&environment,&job,&provider_id]).await?;
            }
        }
    }
    let evidence = if auto {
        crate::reconciliation::resend_evidence_with_auto(&tx, environment, job).await?
    } else {
        resend_evidence(&tx, environment, job).await?
    };
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
