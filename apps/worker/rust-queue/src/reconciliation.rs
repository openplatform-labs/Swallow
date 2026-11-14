//! Explicit, audited operator decisions for uncertain external requests.
use serde_json::{Value, json};
use tokio_postgres::Client;
use uuid::Uuid;

pub const MIGRATION: &str = include_str!("../migrations/0054_delivery_reconciliation.sql");
pub const RETRY_DISPATCH_MIGRATION: &str =
    include_str!("../migrations/0056_retry_dispatch_identity.sql");
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Count unresolved and stranded attempts for one environment in one snapshot.
pub async fn health(client: &Client, environment: &str) -> Result<Value> {
    if environment.trim().is_empty() {
        return Err("environment is required".into());
    }
    let row = client.query_one(
        "SELECT count(*) FILTER (WHERE a.state='unknown'), \
         count(*) FILTER (WHERE a.state='dispatching' AND (j.status <> 'running' OR j.lease_until <= statement_timestamp())), \
         min(a.updated_at) FILTER (WHERE a.state='unknown')::text, \
         min(a.updated_at) FILTER (WHERE a.state='dispatching' AND (j.status <> 'running' OR j.lease_until <= statement_timestamp()))::text \
         FROM novu_queue_jobs j JOIN ( \
         SELECT job_id,environment_id,state,updated_at FROM novu_rust_sendgrid_attempts \
         UNION ALL SELECT job_id,environment_id,state,updated_at FROM novu_rust_resend_attempts \
         UNION ALL SELECT job_id,environment_id,state,updated_at FROM novu_rust_twilio_attempts \
         UNION ALL SELECT job_id,environment_id,state,updated_at FROM novu_rust_fcm_attempts \
         ) a ON a.job_id=j.id AND a.environment_id=j.environment_id WHERE j.environment_id=$1",
        &[&environment],
    ).await?;
    Ok(json!({
        "unknownAttempts": row.get::<_, i64>(0),
        "strandedDispatches": row.get::<_, i64>(1),
        "oldestUnknownUpdatedAt": row.get::<_, Option<String>>(2),
        "oldestStrandedUpdatedAt": row.get::<_, Option<String>>(3),
    }))
}

/// Detect overdue runnable jobs and expired leases, including pre-dispatch crashes.
pub async fn queue_health(
    client: &Client,
    environment: &str,
    max_wait_seconds: i32,
) -> Result<Value> {
    if environment.trim().is_empty() || !(1..=86_400).contains(&max_wait_seconds) {
        return Err("invalid environment or wait threshold (1-86400 seconds)".into());
    }
    let row = client.query_one(
        "SELECT count(*) FILTER (WHERE status='pending' AND available_at <= statement_timestamp() - $2::integer * interval '1 second'), \
         count(*) FILTER (WHERE status='running' AND lease_until <= statement_timestamp()), \
         min(available_at) FILTER (WHERE status='pending' AND available_at <= statement_timestamp() - $2::integer * interval '1 second')::text, \
         min(lease_until) FILTER (WHERE status='running' AND lease_until <= statement_timestamp())::text \
         FROM novu_queue_jobs WHERE environment_id=$1",
        &[&environment, &max_wait_seconds],
    ).await?;
    Ok(json!({
        "overdueJobs": row.get::<_, i64>(0),
        "expiredLeases": row.get::<_, i64>(1),
        "oldestOverdueAvailableAt": row.get::<_, Option<String>>(2),
        "oldestExpiredLeaseAt": row.get::<_, Option<String>>(3),
        "maxWaitSeconds": max_wait_seconds,
    }))
}

/// List bounded unknown attempts without returning recipient data or payloads.
pub async fn list(client: &Client, environment: &str) -> Result<Vec<Value>> {
    list_after(client, environment, None).await
}

/// Return the next page after the last job ID and target index of a prior page.
pub async fn list_after(
    client: &Client,
    environment: &str,
    after: Option<(Uuid, Option<i32>)>,
) -> Result<Vec<Value>> {
    if environment.is_empty() {
        return Err("environment is required".into());
    }
    if after
        .as_ref()
        .is_some_and(|(_, index)| index.is_some_and(|index| index < 0))
    {
        return Err("invalid target index".into());
    }
    let after_job = after.as_ref().map(|(job, _)| *job);
    let after_index = after.and_then(|(_, index)| index).unwrap_or(-1);
    let rows=client.query("SELECT j.id,j.kind,j.status,a.target_index,a.http_status,a.updated_at::text FROM novu_queue_jobs j JOIN (SELECT job_id,environment_id,NULL::integer AS target_index,http_status,updated_at FROM novu_rust_sendgrid_attempts WHERE state='unknown' UNION ALL SELECT job_id,environment_id,NULL::integer,http_status,updated_at FROM novu_rust_resend_attempts WHERE state='unknown' UNION ALL SELECT job_id,environment_id,NULL::integer,http_status,updated_at FROM novu_rust_twilio_attempts WHERE state='unknown' UNION ALL SELECT job_id,environment_id,target_index,http_status,updated_at FROM novu_rust_fcm_attempts WHERE state='unknown') a ON a.job_id=j.id AND a.environment_id=j.environment_id WHERE j.environment_id=$1 AND ($2::uuid IS NULL OR (j.id,COALESCE(a.target_index,-1))>($2,$3)) ORDER BY j.id,COALESCE(a.target_index,-1) LIMIT 100", &[&environment,&after_job,&after_index]).await?;
    Ok(rows.into_iter().map(|r| json!({"jobId":r.get::<_,Uuid>(0).to_string(),"kind":r.get::<_,String>(1),"jobStatus":r.get::<_,String>(2),"targetIndex":r.get::<_,Option<i32>>(3),"httpStatus":r.get::<_,Option<i32>>(4),"updatedAt":r.get::<_,String>(5)})).collect())
}

/// Summarize authenticated SendGrid evidence without exposing recipient hashes.
pub async fn email_evidence(client: &Client, environment: &str, job: Uuid) -> Result<Vec<Value>> {
    if environment.is_empty() {
        return Err("environment is required".into());
    }
    let exists = client
        .query_opt(
            "SELECT 1 FROM novu_queue_jobs WHERE environment_id=$1 AND id=$2 AND kind=$3",
            &[&environment, &job, &crate::sendgrid_queue::JOB_KIND],
        )
        .await?;
    if exists.is_none() {
        return Err("email job not found".into());
    }
    let rows=client.query("SELECT e.event_type,count(*),count(DISTINCT e.recipient_hash),max(e.event_timestamp) FROM novu_rust_sendgrid_delivery_events e JOIN novu_rust_email_messages m ON m.environment_id=e.environment_id AND m.internal_id=e.message_internal_id WHERE m.environment_id=$1 AND m.email_job_id=$2 GROUP BY e.event_type ORDER BY e.event_type", &[&environment,&job]).await?;
    Ok(rows.into_iter().map(|r|json!({"event":r.get::<_,String>(0),"eventCount":r.get::<_,i64>(1),"recipientCount":r.get::<_,i64>(2),"latestEventTimestamp":r.get::<_,Option<i64>>(3)})).collect())
}

/// Summarize SendGrid evidence by dispatch without exposing recipient hashes.
pub async fn email_dispatch_evidence(
    client: &Client,
    environment: &str,
    job: Uuid,
) -> Result<Vec<Value>> {
    if environment.is_empty() {
        return Err("environment is required".into());
    }
    if client
        .query_opt(
            "SELECT 1 FROM novu_queue_jobs WHERE environment_id=$1 AND id=$2 AND kind=$3",
            &[&environment, &job, &crate::sendgrid_queue::JOB_KIND],
        )
        .await?
        .is_none()
    {
        return Err("email job not found".into());
    }
    let rows = client.query("SELECT CASE WHEN e.dispatch_proof_verified THEN e.dispatch_id ELSE NULL END AS verified_dispatch,e.event_type,count(*),count(DISTINCT e.recipient_hash),max(e.event_timestamp) FROM novu_rust_sendgrid_delivery_events e JOIN novu_rust_email_messages m ON m.environment_id=e.environment_id AND m.internal_id=e.message_internal_id WHERE m.environment_id=$1 AND m.email_job_id=$2 GROUP BY verified_dispatch,e.event_type ORDER BY verified_dispatch NULLS FIRST,e.event_type LIMIT 101", &[&environment,&job]).await?;
    if rows.len() > 100 {
        return Err(
            "more than 100 email evidence groups; query the evidence table directly".into(),
        );
    }
    Ok(rows.into_iter().map(|r|json!({"dispatchId":r.get::<_,Option<Uuid>>(0).map(|id|id.to_string()),"event":r.get::<_,String>(1),"eventCount":r.get::<_,i64>(2),"recipientCount":r.get::<_,i64>(3),"latestEventTimestamp":r.get::<_,Option<i64>>(4)})).collect())
}

/// Summarize signed Resend events for the provider ID of a recorded attempt.
pub async fn resend_evidence(client: &Client, environment: &str, job: Uuid) -> Result<Value> {
    if environment.trim().is_empty() {
        return Err("environment is required".into());
    }
    let row = client.query_opt("SELECT a.state,a.provider_message_id,COALESCE((SELECT jsonb_object_agg(event_type,n) FROM (SELECT e.event_type,count(*) n FROM novu_rust_resend_delivery_events e WHERE e.environment_id=j.environment_id AND e.email_id=a.provider_message_id GROUP BY e.event_type) counts),'{}'::jsonb) FROM novu_queue_jobs j JOIN novu_rust_resend_attempts a ON a.job_id=j.id AND a.environment_id=j.environment_id WHERE j.environment_id=$1 AND j.id=$2 AND j.kind=$3", &[&environment,&job,&crate::resend_queue::JOB_KIND]).await?.ok_or("current Resend attempt not found")?;
    let id: Option<String> = row.get(1);
    Ok(
        json!({"attemptState":row.get::<_,String>(0),"providerIdKnown":id.is_some(),"events":row.get::<_,Value>(2)}),
    )
}

/// Show signed Twilio evidence explicitly bound to this job, without phone numbers.
pub async fn sms_evidence(client: &Client, environment: &str, job: Uuid) -> Result<Vec<Value>> {
    if environment.is_empty() {
        return Err("environment is required".into());
    }
    if client
        .query_opt(
            "SELECT 1 FROM novu_queue_jobs WHERE environment_id=$1 AND id=$2 AND kind=$3",
            &[&environment, &job, &crate::twilio_queue::JOB_KIND],
        )
        .await?
        .is_none()
    {
        return Err("SMS job not found".into());
    }
    let rows = client.query("SELECT message_sid,status,error_code,received_at::text,dispatch_id FROM novu_rust_twilio_delivery_events WHERE environment_id=$1 AND job_id=$2 ORDER BY received_at,message_sid,status,error_code LIMIT 101", &[&environment,&job]).await?;
    if rows.len() > 100 {
        return Err("more than 100 SMS events; query the evidence table directly".into());
    }
    Ok(rows.into_iter().map(|r| json!({"providerMessageId":r.get::<_,String>(0),"status":r.get::<_,String>(1),"errorCode":r.get::<_,String>(2),"receivedAt":r.get::<_,String>(3),"dispatchId":r.get::<_,Option<Uuid>>(4).map(|id|id.to_string())})).collect())
}

/// Classify authenticated callbacks for the current identified SMS attempt.
/// Conflicting or unfamiliar evidence is intentionally never resolved.
pub async fn sms_delivery(client: &Client, environment: &str, job: Uuid) -> Result<Value> {
    if environment.trim().is_empty() {
        return Err("environment is required".into());
    }
    let row = client.query_opt(
        "SELECT a.state,a.dispatch_id,count(e.message_sid),count(DISTINCT e.message_sid), \
         COALESCE(bool_or(e.status='delivered'),false), \
         COALESCE(bool_or(e.status IN ('failed','undelivered','canceled')),false), \
         COALESCE(bool_or(e.status NOT IN ('accepted','scheduled','queued','sending','sent','delivered','failed','undelivered','canceled')),false) \
         FROM novu_queue_jobs j JOIN novu_rust_twilio_attempts a ON a.job_id=j.id AND a.environment_id=j.environment_id \
         LEFT JOIN novu_rust_twilio_delivery_events e ON e.job_id=j.id AND e.environment_id=j.environment_id AND e.dispatch_id=a.dispatch_id \
         WHERE j.environment_id=$1 AND j.id=$2 AND j.kind=$3 GROUP BY a.state,a.dispatch_id",
        &[&environment, &job, &crate::twilio_queue::JOB_KIND],
    ).await?.ok_or("current SMS attempt not found")?;
    let attempt: String = row.get(0);
    let dispatch: Option<Uuid> = row.get(1);
    let event_count: i64 = row.get(2);
    let message_count: i64 = row.get(3);
    let delivered: bool = row.get(4);
    let failed: bool = row.get(5);
    let unrecognized: bool = row.get(6);
    let state = if dispatch.is_none() || event_count == 0 {
        "unavailable"
    } else if message_count != 1 || (delivered && failed) {
        "conflict"
    } else if unrecognized {
        "unrecognized"
    } else if delivered {
        "delivered"
    } else if failed {
        "failed"
    } else {
        "pending"
    };
    Ok(
        json!({"attemptState":attempt,"providerState":state,"eventCount":event_count,"messageCount":message_count}),
    )
}

/// Resolve an unknown attempt after an operator independently verifies its outcome.
/// `retry` asserts non-acceptance and can cause duplicates if that assertion is wrong.
pub async fn resolve(
    client: &mut Client,
    environment: &str,
    job: Uuid,
    target: Option<i32>,
    decision: &str,
    operator: &str,
    reason: &str,
) -> Result<()> {
    if environment.is_empty()
        || operator.trim().is_empty()
        || operator.len() > 128
        || reason.trim().is_empty()
        || reason.len() > 512
        || !["retry", "accepted"].contains(&decision)
        || target.is_some_and(|v| v < 0)
    {
        return Err("invalid reconciliation parameters".into());
    }
    let tx = client.transaction().await?;
    let row=tx.query_opt("SELECT kind,status,payload,last_error FROM novu_queue_jobs WHERE environment_id=$1 AND id=$2 FOR UPDATE", &[&environment,&job]).await?.ok_or("job not found")?;
    let kind: String = row.get(0);
    let status: String = row.get(1);
    let payload: Value = row.get(2);
    let last_error: Option<String> = row.get(3);
    if status != "failed" {
        return Err("job must be failed and not running".into());
    }
    if kind == crate::fcm_queue::JOB_KIND
        && decision == "retry"
        && last_error.as_deref() != Some("push delivery outcome unknown")
    {
        return Err("cannot retry push job with a different failure reason".into());
    }
    let attempt=match (kind.as_str(),target) {
        (crate::sendgrid_queue::JOB_KIND,None)=>tx.query_opt("SELECT http_status,dispatch_id FROM novu_rust_sendgrid_attempts WHERE environment_id=$1 AND job_id=$2 AND state='unknown' FOR UPDATE", &[&environment,&job]).await?,
        (crate::resend_queue::JOB_KIND,None)=>tx.query_opt("SELECT http_status,NULL::uuid FROM novu_rust_resend_attempts WHERE environment_id=$1 AND job_id=$2 AND state='unknown' FOR UPDATE", &[&environment,&job]).await?,
        (crate::twilio_queue::JOB_KIND,None)=>tx.query_opt("SELECT http_status,dispatch_id FROM novu_rust_twilio_attempts WHERE environment_id=$1 AND job_id=$2 AND state='unknown' FOR UPDATE", &[&environment,&job]).await?,
        (crate::fcm_queue::JOB_KIND,Some(index))=>tx.query_opt("SELECT http_status,NULL::uuid FROM novu_rust_fcm_attempts WHERE environment_id=$1 AND job_id=$2 AND target_index=$3 AND state='unknown' FOR UPDATE", &[&environment,&job,&index]).await?,
        _=>return Err("job kind and target do not match".into()),
    }.ok_or("unknown attempt not found")?;
    let http_status: Option<i32> = attempt.get(0);
    let retry_dispatch: Option<Uuid> = if decision == "retry" {
        attempt.get(1)
    } else {
        None
    };
    if kind == crate::sendgrid_queue::JOB_KIND && decision == "retry" {
        // Any recipient event means that SendGrid accepted at least part of
        // this request. Retrying the full request could duplicate delivery.
        let evidence: bool = tx.query_one("SELECT EXISTS(SELECT 1 FROM novu_rust_sendgrid_delivery_events e JOIN novu_rust_email_messages m ON m.environment_id=e.environment_id AND m.internal_id=e.message_internal_id WHERE m.environment_id=$1 AND m.email_job_id=$2 AND ($3::uuid IS NULL OR NOT e.dispatch_proof_verified OR e.dispatch_id IS NULL OR e.dispatch_id=$3))", &[&environment,&job,&retry_dispatch]).await?.get(0);
        if evidence {
            tx.rollback().await?;
            return Err("email retry refused: authenticated provider callback already exists; inspect evidence".into());
        }
    }
    if kind == crate::twilio_queue::JOB_KIND && decision == "retry" {
        // Webhook inserts serialize on the same job row. A callback already
        // committed at this point proves Twilio accepted some request; never
        // assert non-acceptance for the matching or ambiguous dispatch.
        let evidence: bool = tx.query_one("SELECT EXISTS(SELECT 1 FROM novu_rust_twilio_delivery_events WHERE environment_id=$1 AND job_id=$2 AND ($3::uuid IS NULL OR dispatch_id IS NULL OR dispatch_id=$3))", &[&environment,&job,&retry_dispatch]).await?.get(0);
        if evidence {
            tx.rollback().await?;
            return Err("SMS retry refused: authenticated provider callback already exists; inspect sms-evidence".into());
        }
    }
    tx.execute("INSERT INTO novu_rust_delivery_reconciliations(id,environment_id,job_id,target_index,decision,operator_name,reason,previous_http_status,retry_dispatch_id) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)",&[&Uuid::new_v4(),&environment,&job,&target,&decision,&operator,&reason,&http_status,&retry_dispatch]).await?;
    match (kind.as_str(), target, decision) {
        (crate::resend_queue::JOB_KIND, None, "retry") => {
            tx.execute(
                "DELETE FROM novu_rust_resend_attempts WHERE environment_id=$1 AND job_id=$2",
                &[&environment, &job],
            )
            .await?;
            tx.execute("UPDATE novu_rust_email_messages SET delivery_state='not_attempted',status='sent',error_id=NULL,error_text=NULL,identifier=NULL WHERE environment_id=$1 AND email_job_id=$2", &[&environment,&job]).await?;
        }
        (crate::resend_queue::JOB_KIND, None, "accepted") => {
            tx.execute("UPDATE novu_rust_resend_attempts SET state='accepted',updated_at=clock_timestamp() WHERE environment_id=$1 AND job_id=$2", &[&environment, &job]).await?;
        }
        (crate::sendgrid_queue::JOB_KIND, None, "retry") => {
            tx.execute(
                "DELETE FROM novu_rust_sendgrid_attempts WHERE environment_id=$1 AND job_id=$2",
                &[&environment, &job],
            )
            .await?;
            tx.execute("UPDATE novu_rust_email_messages SET delivery_state='accepted',status='sent',error_id=NULL,error_text=NULL WHERE environment_id=$1 AND email_job_id=$2", &[&environment,&job]).await?;
            tx.execute("UPDATE novu_rust_email_messages SET delivery_state='not_attempted',status='sent',error_id=NULL,error_text=NULL,identifier=NULL WHERE environment_id=$1 AND email_job_id=$2", &[&environment,&job]).await?;
        }
        (crate::twilio_queue::JOB_KIND, None, "retry") => {
            tx.execute(
                "DELETE FROM novu_rust_twilio_attempts WHERE environment_id=$1 AND job_id=$2",
                &[&environment, &job],
            )
            .await?;
            tx.execute("UPDATE novu_rust_sms_messages SET delivery_state='not_attempted',status='sent',error_id=NULL,error_text=NULL,identifier=NULL WHERE environment_id=$1 AND sms_job_id=$2", &[&environment,&job]).await?;
        }
        (crate::sendgrid_queue::JOB_KIND, None, "accepted") => {
            tx.execute("UPDATE novu_rust_sendgrid_attempts SET state='accepted',updated_at=clock_timestamp() WHERE environment_id=$1 AND job_id=$2", &[&environment,&job]).await?;
            tx.execute("UPDATE novu_rust_email_messages SET delivery_state='accepted',status='sent',error_id=NULL,error_text=NULL WHERE environment_id=$1 AND email_job_id=$2", &[&environment,&job]).await?;
        }
        (crate::twilio_queue::JOB_KIND, None, "accepted") => {
            tx.execute("UPDATE novu_rust_twilio_attempts SET state='accepted',updated_at=clock_timestamp() WHERE environment_id=$1 AND job_id=$2", &[&environment,&job]).await?;
            tx.execute("UPDATE novu_rust_sms_messages SET delivery_state='accepted',status='sent',error_id=NULL,error_text=NULL WHERE environment_id=$1 AND sms_job_id=$2", &[&environment,&job]).await?;
        }
        (crate::fcm_queue::JOB_KIND, Some(index), _) => {
            let metadata = payload.pointer(&format!("/prepared_context/target_metadata/{index}"));
            let history_index = metadata
                .and_then(|v| v.get("history_index"))
                .and_then(Value::as_i64)
                .and_then(|v| i32::try_from(v).ok())
                .unwrap_or(index);
            if decision == "retry" {
                tx.execute("DELETE FROM novu_rust_fcm_attempts WHERE environment_id=$1 AND job_id=$2 AND target_index=$3", &[&environment,&job,&index]).await?;
                let group_size = metadata
                    .and_then(|v| v.get("group_size"))
                    .and_then(Value::as_i64)
                    .and_then(|v| i32::try_from(v).ok())
                    .filter(|v| *v > 0)
                    .unwrap_or(1);
                let end = history_index.saturating_add(group_size);
                tx.execute("UPDATE novu_rust_push_messages SET delivery_state=CASE WHEN EXISTS(SELECT 1 FROM novu_rust_fcm_attempts WHERE environment_id=$1 AND job_id=$2 AND target_index >= $3 AND target_index < $4 AND state='unknown') THEN 'unknown' ELSE 'in_progress' END,status='sent',error_id=NULL,error_text=NULL WHERE environment_id=$1 AND push_job_id=$2 AND target_index=$3", &[&environment,&job,&history_index,&end]).await?;
            } else {
                tx.execute("UPDATE novu_rust_fcm_attempts SET state='accepted',updated_at=clock_timestamp() WHERE environment_id=$1 AND job_id=$2 AND target_index=$3", &[&environment,&job,&index]).await?;
                let group_size = metadata
                    .and_then(|v| v.get("group_size"))
                    .and_then(Value::as_i64)
                    .and_then(|v| i32::try_from(v).ok())
                    .filter(|v| *v > 0)
                    .unwrap_or(1);
                let end = history_index.saturating_add(group_size);
                let row=tx.query_one("SELECT count(*),count(*) FILTER (WHERE state IN ('unknown','dispatching')) FROM novu_rust_fcm_attempts WHERE environment_id=$1 AND job_id=$2 AND target_index >= $3 AND target_index < $4", &[&environment,&job,&history_index,&end]).await?;
                let count = row.get::<_, i64>(0);
                let uncertain = row.get::<_, i64>(1);
                if uncertain > 0 {
                    tx.execute("UPDATE novu_rust_push_messages SET delivery_state='unknown' WHERE environment_id=$1 AND push_job_id=$2 AND target_index=$3", &[&environment,&job,&history_index]).await?;
                } else if count == i64::from(group_size) {
                    tx.execute("UPDATE novu_rust_push_messages SET delivery_state='accepted',status='sent',error_id=NULL,error_text=NULL,identifier=internal_id WHERE environment_id=$1 AND push_job_id=$2 AND target_index=$3", &[&environment,&job,&history_index]).await?;
                } else if count < i64::from(group_size) {
                    tx.execute("UPDATE novu_rust_push_messages SET delivery_state='in_progress' WHERE environment_id=$1 AND push_job_id=$2 AND target_index=$3", &[&environment,&job,&history_index]).await?;
                }
            }
        }
        _ => return Err("unsupported reconciliation".into()),
    }
    if decision == "retry" {
        tx.execute("UPDATE novu_queue_jobs SET status='pending',available_at=now(),last_error=NULL WHERE environment_id=$1 AND id=$2", &[&environment,&job]).await?;
    } else if kind != crate::fcm_queue::JOB_KIND {
        tx.execute("UPDATE novu_queue_jobs SET status='completed',last_error=NULL WHERE environment_id=$1 AND id=$2", &[&environment,&job]).await?;
    } else {
        let expected = payload
            .get("prepared_messages")
            .and_then(Value::as_array)
            .map(|v| v.len());
        let counts=tx.query_one("SELECT count(*),count(*) FILTER (WHERE state IN ('unknown','dispatching')) FROM novu_rust_fcm_attempts WHERE environment_id=$1 AND job_id=$2", &[&environment,&job]).await?;
        let count = counts.get::<_, i64>(0);
        let unknown = counts.get::<_, i64>(1);
        // Resolving an attempt cannot override a different failure, such as
        // a replaced subscriber or invalid prepared payload.
        if unknown == 0
            && last_error.as_deref() == Some("push delivery outcome unknown")
            && let Some(expected) = expected
                .and_then(|n| i64::try_from(n).ok())
                .filter(|n| *n > 0)
        {
            if count == expected {
                tx.execute("UPDATE novu_queue_jobs SET status='completed',last_error=NULL WHERE environment_id=$1 AND id=$2", &[&environment,&job]).await?;
            } else if count < expected {
                tx.execute("UPDATE novu_queue_jobs SET status='pending',available_at=now(),last_error=NULL WHERE environment_id=$1 AND id=$2", &[&environment,&job]).await?;
            }
        }
    }
    tx.commit().await?;
    Ok(())
}
