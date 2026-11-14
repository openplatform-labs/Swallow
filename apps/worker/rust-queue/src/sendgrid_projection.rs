//! Conservative, dispatch- and recipient-scoped SendGrid event projection.
use crate::sendgrid_queue;
use serde_json::{Value, json};
use tokio_postgres::{Client, IsolationLevel};
use uuid::Uuid;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
pub const MIGRATION: &str = include_str!("../migrations/0097_sendgrid_delivery_projection.sql");

/// Locks one job. The webhook takes the same job lock before inserting events.
/// An older or unverified callback cannot be treated as evidence for this dispatch.
pub async fn project(client: &mut Client, environment: &str, job: Uuid) -> Result<bool> {
    let tx = client
        .build_transaction()
        .isolation_level(IsolationLevel::RepeatableRead)
        .start()
        .await?;
    let row=tx.query_opt("SELECT a.state,a.dispatch_id,(SELECT count(*) FROM novu_rust_email_messages m WHERE m.environment_id=j.environment_id AND m.email_job_id=j.id) FROM novu_queue_jobs j JOIN novu_rust_sendgrid_attempts a ON a.job_id=j.id AND a.environment_id=j.environment_id WHERE j.id=$1 AND j.environment_id=$2 AND j.kind=$3 FOR UPDATE OF j SKIP LOCKED", &[&job,&environment,&sendgrid_queue::JOB_KIND]).await?;
    let Some(row) = row else {
        tx.rollback().await?;
        return Ok(false);
    };
    let state: String = row.get(0);
    let dispatch: Option<Uuid> = row.get(1);
    let message_count: i64 = row.get(2);
    let mut updates = Vec::<Value>::new();
    let mut results = Vec::<&str>::new();
    let mut unmatched = 0i64;
    if let Some(dispatch) = dispatch {
        let rows=tx.query("SELECT r.recipient_kind,r.recipient_index,(SELECT count(*) FROM novu_rust_sendgrid_recipients d WHERE d.environment_id=r.environment_id AND d.dispatch_id=r.dispatch_id AND d.recipient_hash=r.recipient_hash),COALESCE(bool_or(e.event_type='delivered'),false),COALESCE(bool_or(e.event_type='dropped' OR (e.event_type='bounce' AND e.bounce_type='bounce')),false),COALESCE(bool_or(e.event_type='bounce' AND e.bounce_type IS NULL),false),COALESCE(bool_or(e.event_type IN ('processed','deferred','blocked') OR (e.event_type='bounce' AND e.bounce_type='blocked')),false) FROM novu_rust_sendgrid_recipients r LEFT JOIN novu_rust_sendgrid_delivery_events e ON e.environment_id=r.environment_id AND e.dispatch_id=r.dispatch_id AND e.dispatch_proof_verified AND e.recipient_hash=r.recipient_hash AND EXISTS (SELECT 1 FROM novu_rust_email_messages m WHERE m.environment_id=e.environment_id AND m.email_job_id=$2 AND m.internal_id=e.message_internal_id) WHERE r.environment_id=$1 AND r.dispatch_id=$3 GROUP BY r.environment_id,r.dispatch_id,r.recipient_kind,r.recipient_index,r.recipient_hash ORDER BY r.recipient_kind,r.recipient_index", &[&environment,&job,&dispatch]).await?;
        unmatched=tx.query_one("SELECT count(*) FROM novu_rust_sendgrid_delivery_events e WHERE e.environment_id=$1 AND e.dispatch_id=$3 AND e.dispatch_proof_verified AND EXISTS(SELECT 1 FROM novu_rust_email_messages m WHERE m.environment_id=e.environment_id AND m.email_job_id=$2 AND m.internal_id=e.message_internal_id) AND NOT EXISTS(SELECT 1 FROM novu_rust_sendgrid_recipients r WHERE r.environment_id=e.environment_id AND r.dispatch_id=e.dispatch_id AND r.recipient_hash=e.recipient_hash)", &[&environment,&job,&dispatch]).await?.get(0);
        for r in rows {
            let duplicate: i64 = r.get(2);
            let delivered: bool = r.get(3);
            let failed: bool = r.get(4);
            let unknown_bounce: bool = r.get(5);
            let intermediate: bool = r.get(6);
            let value =
                if message_count != 1 || state == "rejected" || duplicate != 1 || unknown_bounce {
                    "unknown"
                } else if delivered && failed {
                    "conflicting"
                } else if delivered {
                    "delivered"
                } else if failed {
                    "failed"
                } else if state == "accepted" || intermediate {
                    "pending"
                } else {
                    "unknown"
                };
            results.push(value);
            updates
                .push(json!({"kind":r.get::<_,String>(0),"idx":r.get::<_,i32>(1),"result":value}));
        }
        tx.execute("UPDATE novu_rust_sendgrid_recipients r SET delivery_result=x.result FROM jsonb_to_recordset($3::jsonb) AS x(kind text,idx integer,result text) WHERE r.environment_id=$1 AND r.dispatch_id=$2 AND r.recipient_kind=x.kind AND r.recipient_index=x.idx AND r.delivery_result IS DISTINCT FROM x.result", &[&environment,&dispatch,&json!(updates)]).await?;
    }
    let result = if results.is_empty() || unmatched > 0 || results.contains(&"unknown") {
        "unknown"
    } else if results.contains(&"conflicting") {
        "conflicting"
    } else if results.contains(&"pending") {
        "pending"
    } else if results.iter().all(|v| *v == "delivered") {
        "delivered"
    } else if results.iter().all(|v| *v == "failed") {
        "failed"
    } else {
        "partial"
    };
    tx.execute("UPDATE novu_queue_jobs SET sendgrid_delivery_result=$3 WHERE id=$1 AND environment_id=$2 AND sendgrid_delivery_result IS DISTINCT FROM $3", &[&job,&environment,&result]).await?;
    tx.execute("UPDATE novu_rust_email_messages SET sendgrid_delivery_result=$3 WHERE email_job_id=$1 AND environment_id=$2 AND sendgrid_delivery_result IS DISTINCT FROM $3", &[&job,&environment,&result]).await?;
    tx.commit().await?;
    Ok(true)
}

pub async fn page(client: &Client, environment: &str, after: Option<Uuid>) -> Result<Vec<Uuid>> {
    let rows=client.query("SELECT j.id FROM novu_queue_jobs j JOIN novu_rust_sendgrid_attempts a ON a.job_id=j.id AND a.environment_id=j.environment_id WHERE j.environment_id=$1 AND j.kind=$2 AND ($3::uuid IS NULL OR j.id>$3) ORDER BY j.id LIMIT 100", &[&environment,&sendgrid_queue::JOB_KIND,&after]).await?;
    Ok(rows.into_iter().map(|r| r.get(0)).collect())
}
