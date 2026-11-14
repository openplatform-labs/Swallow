//! Explicit, audited operator decisions for uncertain external requests.
use serde_json::{Value, json};
use tokio_postgres::Client;
use uuid::Uuid;

pub const MIGRATION: &str = include_str!("../migrations/0054_delivery_reconciliation.sql");
pub const RETRY_DISPATCH_MIGRATION: &str =
    include_str!("../migrations/0056_retry_dispatch_identity.sql");
pub const OPERATOR_GRANTS_MIGRATION: &str =
    include_str!("../migrations/0064_delivery_operator_grants.sql");
pub const OPERATOR_GRANT_LOCK_MIGRATION: &str =
    include_str!("../migrations/0068_lock_operator_grants.sql");
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
         min(a.updated_at) FILTER (WHERE a.state='dispatching' AND (j.status <> 'running' OR j.lease_until <= statement_timestamp()))::text, \
         EXISTS(SELECT 1 FROM novu_rust_environments WHERE id=$1) \
         FROM novu_queue_jobs j JOIN ( \
         SELECT job_id,environment_id,state,updated_at FROM novu_rust_sendgrid_attempts \
         UNION ALL SELECT job_id,environment_id,state,updated_at FROM novu_rust_resend_attempts \
         UNION ALL SELECT job_id,environment_id,state,updated_at FROM novu_rust_twilio_attempts \
         UNION ALL SELECT job_id,environment_id,state,updated_at FROM novu_rust_fcm_attempts \
         ) a ON a.job_id=j.id AND a.environment_id=j.environment_id WHERE j.environment_id=$1",
        &[&environment],
    ).await?;
    if !row.get::<_, bool>(4) {
        return Err("environment not found".into());
    }
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
         min(lease_until) FILTER (WHERE status='running' AND lease_until <= statement_timestamp())::text, \
         EXISTS(SELECT 1 FROM novu_rust_environments WHERE id=$1) \
         FROM novu_queue_jobs WHERE environment_id=$1",
        &[&environment, &max_wait_seconds],
    ).await?;
    if !row.get::<_, bool>(4) {
        return Err("environment not found".into());
    }
    Ok(json!({
        "overdueJobs": row.get::<_, i64>(0),
        "expiredLeases": row.get::<_, i64>(1),
        "oldestOverdueAvailableAt": row.get::<_, Option<String>>(2),
        "oldestExpiredLeaseAt": row.get::<_, Option<String>>(3),
        "maxWaitSeconds": max_wait_seconds,
    }))
}

/// Environment-authorized attempt aggregates without direct table access.
pub async fn scoped_health(client: &Client, environment: &str) -> Result<Value> {
    let r = client
        .query_one(
            "SELECT * FROM public.novu_rust_delivery_health($1)",
            &[&environment],
        )
        .await?;
    Ok(
        json!({"unknownAttempts":r.get::<_,i64>(0),"strandedDispatches":r.get::<_,i64>(1),"oldestUnknownUpdatedAt":r.get::<_,Option<String>>(2),"oldestStrandedUpdatedAt":r.get::<_,Option<String>>(3)}),
    )
}
pub async fn scoped_queue_health(client: &Client, environment: &str, wait: i32) -> Result<Value> {
    let r = client
        .query_one(
            "SELECT * FROM public.novu_rust_queue_health($1,$2)",
            &[&environment, &wait],
        )
        .await?;
    Ok(
        json!({"overdueJobs":r.get::<_,i64>(0),"expiredLeases":r.get::<_,i64>(1),"oldestOverdueAvailableAt":r.get::<_,Option<String>>(2),"oldestExpiredLeaseAt":r.get::<_,Option<String>>(3),"maxWaitSeconds":wait}),
    )
}
pub async fn scoped_provider_health(
    client: &Client,
    environment: &str,
    provider: &str,
    grace: i32,
    heartbeat: i32,
) -> Result<Value> {
    let r = client
        .query_one(
            "SELECT * FROM public.novu_rust_provider_health($1,$2,$3,$4)",
            &[&environment, &provider, &grace, &heartbeat],
        )
        .await?;
    Ok(
        json!({"overdueUndelivered":r.get::<_,i64>(0),"eligibleJobs":r.get::<_,i64>(1),"lastProjectionAt":r.get::<_,Option<String>>(2),"projectorStale":r.get::<_,bool>(3),"graceSeconds":grace,"heartbeatMaxSeconds":heartbeat}),
    )
}

/// Count unacknowledged guarded channel cancellations under alert_view authorization.
pub async fn channel_policy_count(
    client: &Client,
    environment: &str,
    channel: &str,
) -> Result<i64> {
    Ok(client
        .query_one(
            "SELECT public.novu_rust_count_channel_policy_attention($1,$2)",
            &[&environment, &channel],
        )
        .await?
        .get(0))
}
pub async fn channel_policy_list(
    client: &Client,
    environment: &str,
    channel: &str,
    after: Option<Uuid>,
) -> Result<Vec<Value>> {
    let rows = client
        .query(
            "SELECT * FROM public.novu_rust_list_channel_policy_attention($1,$2,$3)",
            &[&environment, &channel, &after],
        )
        .await?;
    Ok(rows.into_iter().map(|r| json!({"jobId":r.get::<_,Uuid>(0).to_string(),"kind":r.get::<_,String>(1),"reasonCode":r.get::<_,String>(2),"attempts":r.get::<_,i32>(3),"createdAt":r.get::<_,String>(4),"revision":r.get::<_,String>(5)})).collect())
}
pub async fn acknowledge_channel_policy(
    client: &Client,
    environment: &str,
    job: Uuid,
    expected_revision: i64,
    operator: &str,
    reason: &str,
) -> Result<bool> {
    Ok(client
        .query_one(
            "SELECT public.novu_rust_ack_channel_policy($1,$2,$3,$4,$5)",
            &[&environment, &job, &expected_revision, &operator, &reason],
        )
        .await?
        .get(0))
}

/// Aggregate attempt counts for the reviewed push cancellation revision.
pub async fn push_policy_evidence(
    client: &Client,
    environment: &str,
    job: Uuid,
    revision: i64,
) -> Result<Value> {
    let r = client
        .query_one(
            "SELECT * FROM public.novu_rust_push_policy_evidence_v2($1,$2,$3)",
            &[&environment, &job, &revision],
        )
        .await?;
    Ok(
        json!({"accepted":r.get::<_,i64>(0),"rejected":r.get::<_,i64>(1),"unknown":r.get::<_,i64>(2),"dispatching":r.get::<_,i64>(3),"unattempted":r.get::<_,Option<i64>>(4),"skipped":r.get::<_,i64>(5)}),
    )
}

/// Resolve one unknown attempt of a policy-cancelled push without resuming it.
#[allow(clippy::too_many_arguments)] // Mirrors the explicit, audited database operation.
pub async fn resolve_cancelled_push(
    client: &Client,
    environment: &str,
    job: Uuid,
    revision: i64,
    target: i32,
    decision: &str,
    operator: &str,
    reason: &str,
) -> Result<()> {
    client
        .query_one(
            "SELECT public.novu_rust_resolve_cancelled_push($1,$2,$3,$4,$5,$6,$7)",
            &[
                &environment,
                &job,
                &revision,
                &target,
                &decision,
                &operator,
                &reason,
            ],
        )
        .await?;
    Ok(())
}
pub async fn list_cancelled_push_resolutions(
    client: &Client,
    environment: &str,
    job: Uuid,
    revision: i64,
    after: i32,
) -> Result<Vec<Value>> {
    let rows = client
        .query(
            "SELECT * FROM public.novu_rust_list_cancelled_push_resolutions($1,$2,$3,$4)",
            &[&environment, &job, &revision, &after],
        )
        .await?;
    Ok(rows.into_iter().map(|r|json!({"targetIndex":r.get::<_,i32>(0),"decision":r.get::<_,String>(1),"createdAt":r.get::<_,String>(2),"revision":r.get::<_,String>(3)})).collect())
}

/// List policy-skipped FCM targets without exposing token values.
pub async fn list_skipped_fcm(
    client: &Client,
    environment: &str,
    job: Uuid,
    after: i32,
) -> Result<Vec<Value>> {
    let rows = client
        .query(
            "SELECT * FROM public.novu_rust_list_skipped_fcm($1,$2,$3)",
            &[&environment, &job, &after],
        )
        .await?;
    Ok(rows.into_iter().map(|r|json!({"targetIndex":r.get::<_,i32>(0),"reason":r.get::<_,String>(1),"createdAt":r.get::<_,String>(2)})).collect())
}

/// Count failed queue jobs not acknowledged for their current snapshot.
pub async fn failed_queue_count(client: &Client, environment: &str) -> Result<i64> {
    Ok(client
        .query_one(
            "SELECT public.novu_rust_failed_queue_count($1)",
            &[&environment],
        )
        .await?
        .get(0))
}
/// List bounded failure metadata without payload or diagnostic text.
pub async fn failed_queue_list(
    client: &Client,
    environment: &str,
    after: Option<Uuid>,
) -> Result<Vec<Value>> {
    let rows = client
        .query(
            "SELECT * FROM public.novu_rust_failed_queue_list($1,$2)",
            &[&environment, &after],
        )
        .await?;
    Ok(rows.into_iter().map(|r|json!({"jobId":r.get::<_,Uuid>(0).to_string(),"kind":r.get::<_,String>(1),"attempts":r.get::<_,i32>(2),"createdAt":r.get::<_,String>(3)})).collect())
}
/// List failure revisions for a fenced operator review.
pub async fn failed_queue_list_v2(
    client: &Client,
    environment: &str,
    after: Option<Uuid>,
) -> Result<Vec<Value>> {
    let rows = client
        .query(
            "SELECT * FROM public.novu_rust_failed_queue_list_v2($1,$2)",
            &[&environment, &after],
        )
        .await?;
    Ok(rows.into_iter().map(|r| json!({"jobId":r.get::<_,Uuid>(0).to_string(),"kind":r.get::<_,String>(1),"attempts":r.get::<_,i32>(2),"createdAt":r.get::<_,String>(3),"revision":r.get::<_,String>(4)})).collect())
}
/// Acknowledge only the failure revision the operator reviewed.
pub async fn acknowledge_failed_queue_v2(
    client: &Client,
    environment: &str,
    job: Uuid,
    revision: i64,
    operator: &str,
    reason: &str,
) -> Result<bool> {
    Ok(client
        .query_one(
            "SELECT public.novu_rust_ack_failed_queue_v2($1,$2,$3,$4,$5)",
            &[&environment, &job, &revision, &operator, &reason],
        )
        .await?
        .get(0))
}
pub async fn acknowledge_failed_queue(
    client: &Client,
    environment: &str,
    job: Uuid,
    operator: &str,
    reason: &str,
) -> Result<bool> {
    Ok(client
        .query_one(
            "SELECT public.novu_rust_ack_failed_queue($1,$2,$3,$4)",
            &[&environment, &job, &operator, &reason],
        )
        .await?
        .get(0))
}

/// Query the least-privilege SendGrid health function.
pub async fn scoped_sendgrid_health(
    client: &Client,
    environment: &str,
    grace: i32,
    heartbeat: i32,
) -> Result<Value> {
    let r = client
        .query_one(
            "SELECT * FROM public.novu_rust_sendgrid_health($1,$2,$3)",
            &[&environment, &grace, &heartbeat],
        )
        .await?;
    Ok(
        json!({"overdueUndelivered":r.get::<_,i64>(0),"eligibleJobs":r.get::<_,i64>(1),"lastProjectionAt":r.get::<_,Option<String>>(2),"projectorStale":r.get::<_,bool>(3),"graceSeconds":grace,"heartbeatMaxSeconds":heartbeat}),
    )
}

/// Count overdue SendGrid jobs and detect an absent or stale projector.
pub async fn sendgrid_health(
    client: &Client,
    environment: &str,
    grace: i32,
    heartbeat_max_age: i32,
) -> Result<Value> {
    if environment.trim().is_empty()
        || !(1..=86_400).contains(&grace)
        || !(1..=86_400).contains(&heartbeat_max_age)
    {
        return Err("invalid environment or threshold (1-86400 seconds)".into());
    }
    let row=client.query_one("SELECT count(*) FILTER (WHERE j.sendgrid_delivery_result IS DISTINCT FROM 'delivered'), count(*), EXISTS(SELECT 1 FROM novu_rust_environments e WHERE e.id=$1), (SELECT h.last_completed_at::text FROM novu_rust_sendgrid_projection_heartbeat h WHERE h.environment_id=$1), COALESCE((SELECT h.last_completed_at < statement_timestamp() - $3::integer * interval '1 second' FROM novu_rust_sendgrid_projection_heartbeat h WHERE h.environment_id=$1),true) FROM novu_queue_jobs j WHERE j.environment_id=$1 AND j.kind=$4 AND j.status<>'cancelled' AND j.available_at<=statement_timestamp()-$2::integer*interval '1 second'", &[&environment,&grace,&heartbeat_max_age,&crate::sendgrid_queue::JOB_KIND]).await?;
    if !row.get::<_, bool>(2) {
        return Err("environment not found".into());
    }
    Ok(
        json!({"overdueUndelivered":row.get::<_,i64>(0),"eligibleJobs":row.get::<_,i64>(1),"lastProjectionAt":row.get::<_,Option<String>>(3),"projectorStale":row.get::<_,bool>(4),"graceSeconds":grace,"heartbeatMaxSeconds":heartbeat_max_age}),
    )
}

/// Alert on overdue non-delivered Resend jobs and an absent or stale projector.
pub async fn resend_health(
    client: &Client,
    environment: &str,
    grace: i32,
    heartbeat_max_age: i32,
) -> Result<Value> {
    if environment.trim().is_empty()
        || !(1..=86_400).contains(&grace)
        || !(1..=86_400).contains(&heartbeat_max_age)
    {
        return Err("invalid environment or threshold (1-86400 seconds)".into());
    }
    let row=client.query_one("SELECT count(*) FILTER (WHERE (j.resend_delivery_result IS DISTINCT FROM 'delivered' OR EXISTS(SELECT 1 FROM public.novu_rust_resend_attempts a JOIN public.novu_rust_resend_attempts b ON b.environment_id=a.environment_id AND b.provider_message_id=a.provider_message_id AND b.job_id<>a.job_id WHERE a.environment_id=j.environment_id AND a.job_id=j.id AND a.provider_message_id IS NOT NULL AND a.provider_message_id<>''))), count(*), EXISTS(SELECT 1 FROM novu_rust_environments e WHERE e.id=$1), (SELECT h.last_completed_at::text FROM novu_rust_resend_projection_heartbeat h WHERE h.environment_id=$1), COALESCE((SELECT h.last_completed_at < statement_timestamp() - $3::integer * interval '1 second' FROM novu_rust_resend_projection_heartbeat h WHERE h.environment_id=$1),true) FROM novu_queue_jobs j WHERE j.environment_id=$1 AND j.kind=$4 AND j.status<>'cancelled' AND j.available_at<=statement_timestamp()-$2::integer*interval '1 second'", &[&environment,&grace,&heartbeat_max_age,&crate::resend_queue::JOB_KIND]).await?;
    if !row.get::<_, bool>(2) {
        return Err("environment not found".into());
    }
    Ok(
        json!({"overdueUndelivered":row.get::<_,i64>(0),"eligibleJobs":row.get::<_,i64>(1),"lastProjectionAt":row.get::<_,Option<String>>(3),"projectorStale":row.get::<_,bool>(4),"graceSeconds":grace,"heartbeatMaxSeconds":heartbeat_max_age}),
    )
}

/// Count old SMS jobs without a confirmed delivery and detect a stale projector.
pub async fn twilio_health(
    client: &Client,
    environment: &str,
    grace: i32,
    heartbeat_max_age: i32,
) -> Result<Value> {
    if environment.trim().is_empty()
        || !(1..=86_400).contains(&grace)
        || !(1..=86_400).contains(&heartbeat_max_age)
    {
        return Err("invalid environment or threshold (1-86400 seconds)".into());
    }
    let row=client.query_one("SELECT count(*) FILTER (WHERE (j.twilio_delivery_result IS DISTINCT FROM 'delivered' OR EXISTS(SELECT 1 FROM public.novu_rust_twilio_attempts a JOIN public.novu_rust_twilio_attempts b ON b.environment_id=a.environment_id AND b.provider_message_id=a.provider_message_id AND b.job_id<>a.job_id WHERE a.environment_id=j.environment_id AND a.job_id=j.id AND a.provider_message_id IS NOT NULL AND a.provider_message_id<>''))), count(*), EXISTS(SELECT 1 FROM novu_rust_environments e WHERE e.id=$1), (SELECT h.last_completed_at::text FROM novu_rust_twilio_projection_heartbeat h WHERE h.environment_id=$1), COALESCE((SELECT h.last_completed_at < statement_timestamp() - $3::integer * interval '1 second' FROM novu_rust_twilio_projection_heartbeat h WHERE h.environment_id=$1),true) FROM novu_queue_jobs j WHERE j.environment_id=$1 AND j.kind=$4 AND j.status<>'cancelled' AND j.available_at<=statement_timestamp()-$2::integer*interval '1 second'", &[&environment,&grace,&heartbeat_max_age,&crate::twilio_queue::JOB_KIND]).await?;
    if !row.get::<_, bool>(2) {
        return Err("environment not found".into());
    }
    Ok(
        json!({"overdueUndelivered":row.get::<_,i64>(0),"eligibleJobs":row.get::<_,i64>(1),"lastProjectionAt":row.get::<_,Option<String>>(3),"projectorStale":row.get::<_,bool>(4),"graceSeconds":grace,"heartbeatMaxSeconds":heartbeat_max_age}),
    )
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

/// List unknown attempts using an environment-scoped operator login.
pub async fn scoped_list_after(
    client: &Client,
    environment: &str,
    after: Option<(Uuid, Option<i32>)>,
) -> Result<Vec<Value>> {
    if after
        .as_ref()
        .is_some_and(|(_, index)| index.is_some_and(|i| i < 0))
    {
        return Err("invalid target index".into());
    }
    let job = after.as_ref().map(|(id, _)| *id);
    let index = after.and_then(|(_, index)| index).unwrap_or(-1);
    let rows = client
        .query(
            "SELECT * FROM public.novu_rust_list_unknown_attempts($1,$2,$3)",
            &[&environment, &job, &index],
        )
        .await?;
    Ok(rows.into_iter().map(|r| json!({"jobId":r.get::<_,Uuid>(0).to_string(),"kind":r.get::<_,String>(1),"jobStatus":r.get::<_,String>(2),"targetIndex":r.get::<_,Option<i32>>(3),"httpStatus":r.get::<_,Option<i32>>(4),"updatedAt":r.get::<_,String>(5)})).collect())
}

/// Read a current SendGrid projection through the environment-scoped DB role.
pub async fn scoped_sendgrid_result(
    client: &Client,
    environment: &str,
    job: Uuid,
) -> Result<Value> {
    Ok(client
        .query_one(
            "SELECT public.novu_rust_sendgrid_result($1,$2)",
            &[&environment, &job],
        )
        .await?
        .get(0))
}

/// Read stored channel projection without direct access to provider data.
pub async fn scoped_channel_result(
    client: &Client,
    environment: &str,
    job: Uuid,
    provider: &str,
) -> Result<Value> {
    Ok(client
        .query_one(
            "SELECT public.novu_rust_channel_result($1,$2,$3)",
            &[&environment, &job, &provider],
        )
        .await?
        .get(0))
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
pub async fn resend_evidence<C: tokio_postgres::GenericClient + Sync>(
    client: &C,
    environment: &str,
    job: Uuid,
) -> Result<Value> {
    if environment.trim().is_empty() {
        return Err("environment is required".into());
    }
    let row = client.query_opt("SELECT a.state,a.provider_message_id,COALESCE((SELECT jsonb_object_agg(event_type,n) FROM (SELECT e.event_type,count(*) n FROM novu_rust_resend_delivery_events e WHERE e.environment_id=j.environment_id AND e.email_id=a.provider_message_id GROUP BY e.event_type) counts),'{}'::jsonb) FROM novu_queue_jobs j JOIN novu_rust_resend_attempts a ON a.job_id=j.id AND a.environment_id=j.environment_id WHERE j.environment_id=$1 AND j.id=$2 AND j.kind=$3", &[&environment,&job,&crate::resend_queue::JOB_KIND]).await?.ok_or("current Resend attempt not found")?;
    let id: Option<String> = row.get(1);
    let rows=client.query("SELECT r.recipient_kind,r.recipient_index, count(DISTINCT a2.job_id), COALESCE(bool_or(e.event_type='email.delivered'),false), COALESCE(bool_or(e.event_type='email.bounced'),false), COALESCE(bool_or(e.event_type='email.failed'),false), COALESCE(bool_or(e.event_type='email.suppressed'),false), COALESCE(bool_or(e.event_type='email.complained'),false) FROM novu_rust_resend_recipients r JOIN novu_rust_resend_attempts a ON a.job_id=r.job_id AND a.environment_id=r.environment_id LEFT JOIN novu_rust_resend_attempts a2 ON a2.environment_id=a.environment_id AND a2.provider_message_id=a.provider_message_id LEFT JOIN novu_rust_resend_delivery_events e ON e.environment_id=a.environment_id AND e.email_id=a.provider_message_id AND e.recipient_hash=r.recipient_hash WHERE r.environment_id=$1 AND r.job_id=$2 GROUP BY r.recipient_kind,r.recipient_index ORDER BY r.recipient_kind,r.recipient_index LIMIT 1001", &[&environment,&job]).await?;
    if rows.len() > 1000 {
        return Err("more than 1000 recipients; query the database directly".into());
    }
    let recipients:Vec<Value>=rows.into_iter().map(|r| {
        let delivered:bool=r.get(3);
        let bounced:bool=r.get(4);
        let failed:bool=r.get(5);
        let suppressed:bool=r.get(6);
        let terminal_count=[delivered,bounced,failed,suppressed].into_iter().filter(|v|*v).count();
        let result=if id.is_none() {"unlinked"} else if r.get::<_,i64>(2)!=1 {"ambiguous"} else if terminal_count>1 {"conflicting"} else if delivered {"delivered"} else if bounced {"bounced"} else if failed {"failed"} else if suppressed {"suppressed"} else {"pending"};
        let complained=if id.is_some() && r.get::<_,i64>(2)==1 { Some(r.get::<_,bool>(7)) } else { None };
        json!({"kind":r.get::<_,String>(0),"index":r.get::<_,i32>(1),"result":result,"complained":complained})
    }).collect();
    let unmatched: i64 = client.query_one("SELECT count(*) FROM novu_rust_resend_delivery_events e WHERE e.environment_id=$1 AND e.email_id=$3 AND NOT EXISTS (SELECT 1 FROM novu_rust_resend_recipients r WHERE r.environment_id=$1 AND r.job_id=$2 AND r.recipient_hash=e.recipient_hash)", &[&environment,&job,&id]).await?.get(0);
    Ok(
        json!({"attemptState":row.get::<_,String>(0),"providerIdKnown":id.is_some(),"events":row.get::<_,Value>(2),"unmatchedEvents":unmatched,"recipients":recipients}),
    )
}

/// Obtain all statements of a single-job evidence read from one database snapshot.
pub async fn resend_evidence_snapshot(
    client: &mut Client,
    environment: &str,
    job: Uuid,
) -> Result<Value> {
    resend_evidence_snapshot_with_auto(client, environment, job, false).await
}

pub async fn resend_evidence_with_auto<C: tokio_postgres::GenericClient + Sync>(
    client: &C,
    environment: &str,
    job: Uuid,
) -> Result<Value> {
    let mut evidence = resend_evidence(client, environment, job).await?;
    let conflict: bool = client.query_one("SELECT EXISTS(SELECT 1 FROM novu_rust_resend_auto_correlations c WHERE c.environment_id=$1 AND c.job_id=$2 AND (NOT EXISTS(SELECT 1 FROM novu_rust_resend_delivery_events e JOIN novu_rust_resend_recipients r ON r.environment_id=e.environment_id AND r.job_id=c.job_id AND r.recipient_hash=e.recipient_hash WHERE e.environment_id=c.environment_id AND e.correlation_id=c.correlation_id AND e.email_id=c.provider_message_id) OR EXISTS(SELECT 1 FROM novu_rust_resend_delivery_events e WHERE e.environment_id=c.environment_id AND ((e.correlation_id=c.correlation_id AND e.email_id<>c.provider_message_id) OR (e.email_id=c.provider_message_id AND e.correlation_id IS DISTINCT FROM c.correlation_id))) OR EXISTS(SELECT 1 FROM novu_rust_resend_attempts a WHERE a.environment_id=c.environment_id AND a.provider_message_id=c.provider_message_id AND a.job_id<>c.job_id) OR EXISTS(SELECT 1 FROM novu_rust_delivery_reconciliations r WHERE r.environment_id=c.environment_id AND r.retry_provider_message_id=c.provider_message_id)))", &[&environment,&job]).await?.get(0);
    evidence["autoCorrelationConflict"] = json!(conflict);
    Ok(evidence)
}

pub async fn resend_evidence_snapshot_with_auto(
    client: &mut Client,
    environment: &str,
    job: Uuid,
    auto: bool,
) -> Result<Value> {
    let tx = client
        .build_transaction()
        .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
        .read_only(true)
        .start()
        .await?;
    let evidence = if auto {
        resend_evidence_with_auto(&tx, environment, job).await?
    } else {
        resend_evidence(&tx, environment, job).await?
    };
    tx.commit().await?;
    Ok(evidence)
}

/// A conservative, per-job alert: only a complete set of delivered recipients is healthy.
pub fn resend_needs_attention(evidence: &Value) -> Result<bool> {
    evidence["recipients"]
        .as_array()
        .ok_or("invalid Resend evidence")?;
    Ok(crate::resend_projection::classify(evidence) != "delivered")
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
pub async fn sms_delivery<C: tokio_postgres::GenericClient + Sync>(
    client: &C,
    environment: &str,
    job: Uuid,
) -> Result<Value> {
    if environment.trim().is_empty() {
        return Err("environment is required".into());
    }
    let row = client.query_opt(
        "SELECT a.state,a.dispatch_id,count(e.message_sid),count(DISTINCT e.message_sid), \
         COALESCE(bool_or(e.status='delivered'),false), \
         COALESCE(bool_or(e.status IN ('failed','undelivered','canceled')),false), \
         COALESCE(bool_or(e.status NOT IN ('accepted','scheduled','queued','sending','sent','delivered','failed','undelivered','canceled')),false), \
         COALESCE(bool_or(e.message_sid <> a.provider_message_id),false), count(DISTINCT e.account_sid), a.provider_message_id \
         FROM novu_queue_jobs j JOIN novu_rust_twilio_attempts a ON a.job_id=j.id AND a.environment_id=j.environment_id \
         LEFT JOIN novu_rust_twilio_delivery_events e ON e.job_id=j.id AND e.environment_id=j.environment_id AND e.dispatch_id=a.dispatch_id \
         WHERE j.environment_id=$1 AND j.id=$2 AND j.kind=$3 GROUP BY a.state,a.dispatch_id,a.provider_message_id",
        &[&environment, &job, &crate::twilio_queue::JOB_KIND],
    ).await?.ok_or("current SMS attempt not found")?;
    let attempt: String = row.get(0);
    let dispatch: Option<Uuid> = row.get(1);
    let event_count: i64 = row.get(2);
    let message_count: i64 = row.get(3);
    let delivered: bool = row.get(4);
    let failed: bool = row.get(5);
    let unrecognized: bool = row.get(6);
    let mismatched: bool = row.get(7);
    let accounts: i64 = row.get(8);
    let provider_id: Option<String> = row.get(9);
    let duplicate = if let Some(id) = provider_id.as_ref().filter(|id| !id.is_empty()) {
        client.query_one("SELECT EXISTS(SELECT 1 FROM novu_rust_twilio_attempts WHERE environment_id=$1 AND provider_message_id=$2 AND job_id<>$3)", &[&environment,&id,&job]).await?.get::<_, bool>(0)
    } else {
        false
    };
    let state = if dispatch.is_none() || event_count == 0 {
        "unavailable"
    } else if message_count != 1
        || accounts != 1
        || mismatched
        || duplicate
        || (delivered && failed)
    {
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
        json!({"attemptState":attempt,"providerState":state,"eventCount":event_count,"messageCount":message_count,"providerIdKnown":provider_id.is_some_and(|id| !id.is_empty())}),
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
    let allowed: bool = tx
        .query_one(
            "SELECT public.novu_rust_lock_operator_grant($1,$2)",
            &[&environment, &decision],
        )
        .await?
        .get(0);
    if !allowed {
        return Err("reconciliation permission denied".into());
    }
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
        (crate::resend_queue::JOB_KIND,None)=>tx.query_opt("SELECT http_status,NULL::uuid,provider_message_id FROM novu_rust_resend_attempts WHERE environment_id=$1 AND job_id=$2 AND state='unknown' FOR UPDATE", &[&environment,&job]).await?,
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
    if kind == crate::resend_queue::JOB_KIND && decision == "retry" {
        let provider_id: Option<String> = attempt.get(2);
        if let Some(provider_id) = provider_id.as_ref().filter(|id| !id.is_empty()) {
            let evidence: bool = tx.query_one("SELECT EXISTS(SELECT 1 FROM novu_rust_resend_delivery_events WHERE environment_id=$1 AND email_id=$2)", &[&environment,provider_id]).await?.get(0);
            if evidence {
                tx.rollback().await?;
                return Err("Resend retry refused: authenticated provider callback already exists; inspect evidence".into());
            }
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
    if kind == crate::resend_queue::JOB_KIND && decision == "retry" {
        let provider_id: Option<String> = attempt.get(2);
        tx.execute("INSERT INTO novu_rust_delivery_reconciliations(id,environment_id,job_id,target_index,decision,operator_name,reason,previous_http_status,retry_dispatch_id,retry_provider_message_id,database_session_user) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,session_user)",&[&Uuid::new_v4(),&environment,&job,&target,&decision,&operator,&reason,&http_status,&retry_dispatch,&provider_id]).await?;
    } else {
        tx.execute("INSERT INTO novu_rust_delivery_reconciliations(id,environment_id,job_id,target_index,decision,operator_name,reason,previous_http_status,retry_dispatch_id,database_session_user) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,session_user)",&[&Uuid::new_v4(),&environment,&job,&target,&decision,&operator,&reason,&http_status,&retry_dispatch]).await?;
    }
    match (kind.as_str(), target, decision) {
        (crate::resend_queue::JOB_KIND, None, "retry") => {
            tx.execute(
                "DELETE FROM novu_rust_resend_attempts WHERE environment_id=$1 AND job_id=$2",
                &[&environment, &job],
            )
            .await?;
            tx.execute("UPDATE novu_queue_jobs SET resend_delivery_result=NULL WHERE environment_id=$1 AND id=$2", &[&environment,&job]).await?;
            tx.execute("UPDATE novu_rust_email_messages SET delivery_state='not_attempted',status='sent',error_id=NULL,error_text=NULL,identifier=NULL,resend_delivery_result=NULL WHERE environment_id=$1 AND email_job_id=$2", &[&environment,&job]).await?;
        }
        (crate::resend_queue::JOB_KIND, None, "accepted") => {
            tx.execute("UPDATE novu_rust_resend_attempts SET state='accepted',updated_at=clock_timestamp() WHERE environment_id=$1 AND job_id=$2", &[&environment, &job]).await?;
            // Acceptance is an operator assertion, not evidence of recipient delivery.
            tx.execute("UPDATE novu_queue_jobs SET resend_delivery_result=NULL WHERE environment_id=$1 AND id=$2", &[&environment,&job]).await?;
            tx.execute("UPDATE novu_rust_email_messages SET delivery_state='accepted',status='sent',error_id=NULL,error_text=NULL,identifier=COALESCE((SELECT provider_message_id FROM novu_rust_resend_attempts WHERE environment_id=$1 AND job_id=$2),identifier),resend_delivery_result=NULL WHERE environment_id=$1 AND email_job_id=$2", &[&environment,&job]).await?;
        }
        (crate::sendgrid_queue::JOB_KIND, None, "retry") => {
            tx.execute(
                "DELETE FROM novu_rust_sendgrid_attempts WHERE environment_id=$1 AND job_id=$2",
                &[&environment, &job],
            )
            .await?;
            // The result belongs to the old dispatch and must never describe a retry.
            tx.execute("UPDATE novu_queue_jobs SET sendgrid_delivery_result='unknown' WHERE environment_id=$1 AND id=$2", &[&environment,&job]).await?;
            tx.execute("UPDATE novu_rust_email_messages SET delivery_state='not_attempted',status='sent',error_id=NULL,error_text=NULL,identifier=NULL,sendgrid_delivery_result='unknown' WHERE environment_id=$1 AND email_job_id=$2", &[&environment,&job]).await?;
        }
        (crate::twilio_queue::JOB_KIND, None, "retry") => {
            tx.execute(
                "DELETE FROM novu_rust_twilio_attempts WHERE environment_id=$1 AND job_id=$2",
                &[&environment, &job],
            )
            .await?;
            tx.execute("UPDATE novu_queue_jobs SET twilio_delivery_result=NULL WHERE environment_id=$1 AND id=$2", &[&environment,&job]).await?;
            tx.execute("UPDATE novu_rust_sms_messages SET twilio_delivery_result=NULL,delivery_state='not_attempted',status='sent',error_id=NULL,error_text=NULL,identifier=NULL WHERE environment_id=$1 AND sms_job_id=$2", &[&environment,&job]).await?;
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
                tx.execute("UPDATE novu_rust_push_messages SET delivery_state=CASE WHEN EXISTS(SELECT 1 FROM novu_rust_fcm_attempts WHERE environment_id=$1 AND job_id=$2 AND target_index >= $3 AND target_index < $4 AND state IN ('unknown','skipped')) THEN 'unknown' ELSE 'in_progress' END,status=CASE WHEN EXISTS(SELECT 1 FROM novu_rust_fcm_attempts WHERE environment_id=$1 AND job_id=$2 AND target_index >= $3 AND target_index < $4 AND state='skipped') THEN 'error' ELSE 'sent' END,error_id=CASE WHEN EXISTS(SELECT 1 FROM novu_rust_fcm_attempts WHERE environment_id=$1 AND job_id=$2 AND target_index >= $3 AND target_index < $4 AND state='skipped') THEN 'unexpected_push_error' ELSE NULL END,error_text=CASE WHEN EXISTS(SELECT 1 FROM novu_rust_fcm_attempts WHERE environment_id=$1 AND job_id=$2 AND target_index >= $3 AND target_index < $4 AND state='skipped') THEN 'push target skipped by policy' ELSE NULL END WHERE environment_id=$1 AND push_job_id=$2 AND target_index=$3", &[&environment,&job,&history_index,&end]).await?;
            } else {
                tx.execute("UPDATE novu_rust_fcm_attempts SET state='accepted',updated_at=clock_timestamp() WHERE environment_id=$1 AND job_id=$2 AND target_index=$3", &[&environment,&job,&index]).await?;
                let group_size = metadata
                    .and_then(|v| v.get("group_size"))
                    .and_then(Value::as_i64)
                    .and_then(|v| i32::try_from(v).ok())
                    .filter(|v| *v > 0)
                    .unwrap_or(1);
                let end = history_index.saturating_add(group_size);
                let row=tx.query_one("SELECT count(*),count(*) FILTER (WHERE state IN ('unknown','dispatching')),count(*) FILTER (WHERE state='skipped') FROM novu_rust_fcm_attempts WHERE environment_id=$1 AND job_id=$2 AND target_index >= $3 AND target_index < $4", &[&environment,&job,&history_index,&end]).await?;
                let count = row.get::<_, i64>(0);
                let uncertain = row.get::<_, i64>(1);
                let skipped = row.get::<_, i64>(2);
                if skipped > 0 {
                    tx.execute("UPDATE novu_rust_push_messages SET status='error',error_id='unexpected_push_error',error_text='push target skipped by policy',delivery_state='unknown' WHERE environment_id=$1 AND push_job_id=$2 AND target_index=$3", &[&environment,&job,&history_index]).await?;
                } else if uncertain > 0 {
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
        let counts=tx.query_one("SELECT count(*),count(*) FILTER (WHERE state IN ('unknown','dispatching')),count(*) FILTER (WHERE state='skipped') FROM novu_rust_fcm_attempts WHERE environment_id=$1 AND job_id=$2", &[&environment,&job]).await?;
        let count = counts.get::<_, i64>(0);
        let unknown = counts.get::<_, i64>(1);
        let skipped = counts.get::<_, i64>(2);
        // Resolving an attempt cannot override a different failure, such as
        // a replaced subscriber or invalid prepared payload.
        if unknown == 0
            && last_error.as_deref() == Some("push delivery outcome unknown")
            && let Some(expected) = expected
                .and_then(|n| i64::try_from(n).ok())
                .filter(|n| *n > 0)
        {
            if count == expected && skipped > 0 {
                tx.execute("UPDATE novu_queue_jobs SET status='failed',last_error='push targets skipped by policy' WHERE environment_id=$1 AND id=$2", &[&environment,&job]).await?;
            } else if count == expected {
                tx.execute("UPDATE novu_queue_jobs SET status='completed',last_error=NULL WHERE environment_id=$1 AND id=$2", &[&environment,&job]).await?;
            } else if count < expected {
                tx.execute("UPDATE novu_queue_jobs SET status='pending',available_at=now(),last_error=NULL WHERE environment_id=$1 AND id=$2", &[&environment,&job]).await?;
            }
        }
    }
    tx.commit().await?;
    Ok(())
}

#[cfg(test)]
mod resend_alert_tests {
    use super::*;

    #[test]
    fn only_complete_unambiguous_delivery_is_healthy() {
        let healthy = json!({"attemptState":"accepted","providerIdKnown":true,"unmatchedEvents":0,"recipients":[{"result":"delivered","complained":false}]});
        assert!(!resend_needs_attention(&healthy).unwrap());
        for state in [
            json!("unknown"),
            json!("dispatching"),
            json!("rejected"),
            json!(null),
        ] {
            let mut evidence = healthy.clone();
            evidence["attemptState"] = state;
            assert!(resend_needs_attention(&evidence).unwrap());
        }
        for mut evidence in [
            json!({"providerIdKnown":false,"unmatchedEvents":0,"recipients":[{"result":"delivered","complained":false}]}),
            json!({"providerIdKnown":true,"unmatchedEvents":1,"recipients":[{"result":"delivered","complained":false}]}),
            json!({"providerIdKnown":true,"unmatchedEvents":0,"recipients":[]}),
            json!({"providerIdKnown":true,"unmatchedEvents":0,"recipients":[{"result":"pending","complained":false}]}),
            json!({"providerIdKnown":true,"unmatchedEvents":0,"recipients":[{"result":"conflicting","complained":false}]}),
            json!({"providerIdKnown":true,"unmatchedEvents":0,"recipients":[{"result":"delivered","complained":true}]}),
            json!({"providerIdKnown":true,"unmatchedEvents":0,"recipients":[{"result":"delivered","complained":null}]}),
        ] {
            evidence["attemptState"] = json!("accepted");
            assert!(resend_needs_attention(&evidence).unwrap());
        }
        assert!(resend_needs_attention(&json!({})).is_err());
    }
}
