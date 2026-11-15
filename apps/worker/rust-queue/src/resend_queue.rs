//! Resend queue with shared workflow email preparation.
use crate::{
    ClaimedJob,
    resend::{Resend, SendError},
    sendgrid::Email,
};
use std::time::Duration;
use tokio_postgres::{Client, Error};
use uuid::Uuid;

pub const JOB_KIND: &str = "experimental_resend_email";
pub const MIGRATION: &str = include_str!("../migrations/0060_resend_attempts.sql");
pub const RECIPIENT_MIGRATION: &str = include_str!("../migrations/0062_resend_recipients.sql");

/// Exact byte matching is conservative: provider normalization may prevent correlation.
pub fn recipient_hash(address: &str) -> String {
    use base64::{Engine, engine::general_purpose::STANDARD};
    STANDARD.encode(ring::digest::digest(&ring::digest::SHA256, address.as_bytes()).as_ref())
}

/// Register a shared policy before a new worker processes jobs. Old workers do not participate.
pub async fn register_idempotency_scope(
    client: &Client,
    environment: &str,
    retry_key: bool,
) -> Result<bool, Error> {
    let scope = if retry_key { "retry" } else { "job" };
    Ok(client.query_opt("INSERT INTO public.novu_rust_resend_idempotency_policy(environment_id,scope) VALUES ($1,$2) ON CONFLICT(environment_id) DO UPDATE SET scope=EXCLUDED.scope WHERE novu_rust_resend_idempotency_policy.scope=EXCLUDED.scope RETURNING 1", &[&environment,&scope]).await?.is_some())
}

/// Atomically bind idempotency and cooldown settings among upgraded workers.
pub async fn register_worker_policy(client: &Client, environment: &str, retry_key: bool, cooldown: bool) -> Result<bool, Error> {
    let scope = if retry_key { "retry" } else { "job" };
    Ok(client.query_opt("INSERT INTO public.novu_rust_resend_idempotency_policy(environment_id,scope,shared_cooldown) VALUES($1,$2,$3) ON CONFLICT(environment_id) DO UPDATE SET shared_cooldown=EXCLUDED.shared_cooldown WHERE novu_rust_resend_idempotency_policy.scope=EXCLUDED.scope AND (novu_rust_resend_idempotency_policy.shared_cooldown IS NULL OR novu_rust_resend_idempotency_policy.shared_cooldown=EXCLUDED.shared_cooldown) RETURNING 1", &[&environment,&scope,&cooldown]).await?.is_some())
}

async fn claim(client: &Client, environment: &str) -> Result<Option<ClaimedJob>, Error> {
    let token = Uuid::new_v4();
    let row=client.query_opt("WITH candidate AS (SELECT id FROM novu_queue_jobs WHERE environment_id=$1 AND kind=$2 AND status='pending' AND available_at<=now() ORDER BY available_at,created_at,id FOR UPDATE SKIP LOCKED LIMIT 1) UPDATE novu_queue_jobs j SET status='running',attempts=j.attempts+1,lease_token=$3,lease_until=clock_timestamp()+interval '60 seconds' FROM candidate WHERE j.id=candidate.id RETURNING j.id,j.environment_id,j.kind,j.payload,j.attempts", &[&environment,&JOB_KIND,&token]).await?;
    Ok(row.map(|r| ClaimedJob {
        id: r.get(0),
        environment_id: r.get(1),
        kind: r.get(2),
        payload: r.get(3),
        attempts: r.get(4),
        lease_token: token,
    }))
}

async fn live(tx: &tokio_postgres::Transaction<'_>, job: &ClaimedJob) -> Result<bool, Error> {
    let row = tx.query_opt("SELECT lease_until > clock_timestamp() FROM novu_queue_jobs WHERE id=$1 AND environment_id=$2 AND kind=$3 AND status='running' AND lease_token=$4 FOR UPDATE", &[&job.id,&job.environment_id,&JOB_KIND,&job.lease_token]).await?;
    Ok(row.is_some_and(|r| r.get(0)))
}

pub async fn process_one(
    client: &mut Client,
    environment: &str,
    provider: &Resend,
) -> Result<bool, Error> {
    process_one_with_retry_guard(client, environment, provider, false).await
}

/// Optionally recheck known prior Resend callback evidence before retry dispatch.
pub async fn process_one_with_retry_guard(
    client: &mut Client,
    environment: &str,
    provider: &Resend,
    retry_guard: bool,
) -> Result<bool, Error> {
    process_one_with_policies(
        client,
        environment,
        provider,
        retry_guard,
        crate::email_step_policy::Policy::Snapshot,
    )
    .await
}

pub async fn process_one_with_policies(
    client: &mut Client,
    environment: &str,
    provider: &Resend,
    retry_guard: bool,
    policy: crate::email_step_policy::Policy,
) -> Result<bool, Error> {
    process_one_with_correlation(client, environment, provider, retry_guard, policy, false).await
}

pub async fn process_one_with_correlation(
    client: &mut Client,
    environment: &str,
    provider: &Resend,
    retry_guard: bool,
    policy: crate::email_step_policy::Policy,
    correlation_enabled: bool,
) -> Result<bool, Error> {
    process_one_with_correlation_guard(
        client,
        environment,
        provider,
        retry_guard,
        policy,
        correlation_enabled,
        false,
    )
    .await
}

pub async fn process_one_with_correlation_guard(
    client: &mut Client,
    environment: &str,
    provider: &Resend,
    retry_guard: bool,
    policy: crate::email_step_policy::Policy,
    correlation_enabled: bool,
    correlation_guard: bool,
) -> Result<bool, Error> {
    process_one_with_retry_key(
        client,
        environment,
        provider,
        retry_guard,
        policy,
        correlation_enabled,
        correlation_guard,
        false,
    )
    .await
}

/// A retry-specific key is an explicit opt-in: a mistaken retry can resend a previously accepted email.
pub async fn process_one_with_retry_key(
    client: &mut Client,
    environment: &str,
    provider: &Resend,
    retry_guard: bool,
    policy: crate::email_step_policy::Policy,
    correlation_enabled: bool,
    correlation_guard: bool,
    retry_key: bool,
) -> Result<bool, Error> {
    process_one_inner(
        client,
        environment,
        provider,
        retry_guard,
        policy,
        correlation_enabled,
        correlation_guard,
        retry_key,
        false,
        false,
        false,
        false,
    )
    .await
}

/// Worker entry point: fence the registered policy through attempt creation.
pub async fn process_one_with_registered_policy(
    client: &mut Client,
    environment: &str,
    provider: &Resend,
    retry_guard: bool,
    policy: crate::email_step_policy::Policy,
    correlation_enabled: bool,
    correlation_guard: bool,
    retry_key: bool,
) -> Result<bool, Error> {
    process_one_inner(
        client,
        environment,
        provider,
        retry_guard,
        policy,
        correlation_enabled,
        correlation_guard,
        retry_key,
        true,
        false,
        false,
        false,
    )
    .await
}

/// Worker path with optional environment-scoped 429 cooldown.
pub async fn process_one_with_registered_policy_and_cooldown(
    client: &mut Client, environment: &str, provider: &Resend, retry_guard: bool,
    policy: crate::email_step_policy::Policy, correlation_enabled: bool,
    correlation_guard: bool, retry_key: bool, cooldown: bool,
) -> Result<bool, Error> {
    process_one_inner(client, environment, provider, retry_guard, policy, correlation_enabled,
        correlation_guard, retry_key, true, cooldown, true, false).await
}

/// Optional one-time replay of an uncertain first dispatch using identical request and key.
pub async fn process_one_with_immediate_replay(
    client: &mut Client, environment: &str, provider: &Resend, retry_guard: bool,
    policy: crate::email_step_policy::Policy, correlation_enabled: bool,
    correlation_guard: bool, retry_key: bool, cooldown: bool, replay_uncertain_once: bool,
) -> Result<bool, Error> {
    process_one_inner(client, environment, provider, retry_guard, policy, correlation_enabled,
        correlation_guard, retry_key, true, cooldown, true, replay_uncertain_once).await
}

async fn process_one_inner(
    client: &mut Client,
    environment: &str,
    provider: &Resend,
    retry_guard: bool,
    policy: crate::email_step_policy::Policy,
    correlation_enabled: bool,
    correlation_guard: bool,
    retry_key: bool,
    registered: bool,
    cooldown: bool,
    enforce_cooldown_policy: bool,
    replay_uncertain_once: bool,
) -> Result<bool, Error> {
    let Some(job) = claim(client, environment).await? else {
        return Ok(false);
    };
    let mut email: Email = match serde_json::from_value(job.payload.clone()) {
        Ok(value) => value,
        Err(_) => {
            crate::fail(client, job.id, job.lease_token, "invalid Resend job").await?;
            return Ok(true);
        }
    };
    let tx = client.transaction().await?;
    if registered {
        let scope = if retry_key { "retry" } else { "job" };
        tx.query_one(
            "SELECT public.novu_rust_check_resend_idempotency_scope($1,$2)",
            &[&environment, &scope],
        )
        .await?;
    }
    if enforce_cooldown_policy {
        tx.query_one("SELECT public.novu_rust_check_resend_shared_cooldown($1,$2)", &[&environment,&cooldown]).await?;
    }
    let subscriber =
        crate::sendgrid_queue::lock_workflow_subscriber(&tx, environment, &email).await?;
    if !live(&tx, &job).await? {
        tx.rollback().await?;
        return Ok(true);
    }
    if cooldown && crate::resend_cooldown::defer(&tx, &job).await? {
        tx.commit().await?;
        return Ok(true);
    }
    if registered {
        let allowed: bool = tx
            .query_one(
                "SELECT public.novu_rust_resend_auto_dispatch_allowed($1,$2,$3,$4,$5,$6)",
                &[
                    &environment,
                    &job.id,
                    &retry_guard,
                    &correlation_enabled,
                    &correlation_guard,
                    &retry_key,
                ],
            )
            .await?
            .get(0);
        if !allowed {
            tx.execute("UPDATE public.novu_queue_jobs SET status='pending',available_at=clock_timestamp()+interval '5 seconds',last_error='Resend automatic retry requires evidence guards',lease_token=NULL,lease_until=NULL WHERE id=$1 AND environment_id=$2 AND status='running' AND lease_token=$3", &[&job.id,&environment,&job.lease_token]).await?;
            tx.commit().await?;
            eprintln!("Resend automatic retry requires evidence guards; job deferred");
            return Ok(true);
        }
    }
    if correlation_guard {
        let evidence = tx.query_opt("SELECT r.retry_correlation_id,r.retry_provider_message_id FROM novu_rust_delivery_reconciliations r WHERE r.environment_id=$1 AND r.job_id=$2 AND r.target_index IS NULL AND r.decision='retry' AND r.retry_correlation_id IS NOT NULL AND EXISTS(SELECT 1 FROM novu_rust_resend_delivery_events e WHERE e.environment_id=$1 AND e.correlation_id=r.retry_correlation_id) ORDER BY r.decision_order DESC LIMIT 1", &[&environment,&job.id]).await?;
        if let Some(evidence) = evidence {
            let tag: Uuid = evidence.get(0);
            let provider_id: Option<String> = evidence.get(1);
            let inserted=tx.execute("INSERT INTO novu_rust_resend_attempts(job_id,environment_id,state,provider_message_id,correlation_id) VALUES ($1,$2,'unknown',$3,$4) ON CONFLICT(job_id) DO NOTHING", &[&job.id,&environment,&provider_id,&tag]).await?;
            if inserted == 1 {
                tx.execute("UPDATE novu_rust_email_messages SET delivery_state='unknown',status='error',error_id='mail_unexpected_error',error_text='Resend retry blocked by tagged callback',resend_delivery_result=NULL WHERE environment_id=$1 AND email_job_id=$2", &[&environment,&job.id]).await?;
            }
            tx.execute("UPDATE novu_queue_jobs SET status='failed',last_error='Resend retry blocked by tagged callback',lease_token=NULL,lease_until=NULL,resend_delivery_result=NULL WHERE id=$1 AND environment_id=$2", &[&job.id,&environment]).await?;
            tx.commit().await?;
            return Ok(true);
        }
    }
    if retry_guard {
        let evidence = tx.query_opt("SELECT r.retry_provider_message_id FROM novu_rust_delivery_reconciliations r WHERE r.environment_id=$1 AND r.job_id=$2 AND r.target_index IS NULL AND r.decision='retry' AND r.retry_provider_message_id IS NOT NULL AND r.retry_provider_message_id<>'' AND EXISTS(SELECT 1 FROM novu_rust_resend_delivery_events e WHERE e.environment_id=$1 AND e.email_id=r.retry_provider_message_id) ORDER BY r.decision_order DESC LIMIT 1", &[&environment,&job.id]).await?;
        if let Some(evidence) = evidence {
            let provider_id: String = evidence.get(0);
            let inserted = tx.execute("INSERT INTO novu_rust_resend_attempts(job_id,environment_id,state,provider_message_id) VALUES ($1,$2,'unknown',$3) ON CONFLICT(job_id) DO NOTHING", &[&job.id,&environment,&provider_id]).await?;
            if inserted == 1 {
                tx.execute("UPDATE novu_rust_email_messages SET delivery_state='unknown',status='error',error_id='mail_unexpected_error',error_text='Resend retry blocked by provider callback',resend_delivery_result=NULL WHERE environment_id=$1 AND email_job_id=$2", &[&environment,&job.id]).await?;
            }
            tx.execute("UPDATE novu_queue_jobs SET status='failed',last_error='Resend retry blocked by provider callback',lease_token=NULL,lease_until=NULL,resend_delivery_result=NULL WHERE id=$1 AND environment_id=$2", &[&job.id,&environment]).await?;
            tx.commit().await?;
            return Ok(true);
        }
    }
    match crate::email_step_policy::check(&tx, environment, &job, &email, policy).await? {
        crate::email_step_policy::Check::Continue => {}
        crate::email_step_policy::Check::Cancelled => {
            tx.commit().await?;
            return Ok(true);
        }
        crate::email_step_policy::Check::LeaseLost => {
            tx.rollback().await?;
            return Ok(true);
        }
    }
    if !crate::sendgrid_queue::prepare_workflow_email(
        &tx,
        environment,
        &job,
        &mut email,
        subscriber,
    )
    .await?
    {
        tx.commit().await?;
        return Ok(true);
    }
    let prepared = provider.prepare(&email);
    // Preparation can be expensive (e.g. attachments). Do not reserve after
    // the lease has expired, regardless of the workflow policy.
    if !live(&tx, &job).await? {
        tx.rollback().await?;
        return Ok(true);
    }
    let correlation = correlation_enabled.then(Uuid::new_v4);
    let inserted = if correlation_enabled {
        tx.execute("INSERT INTO novu_rust_resend_attempts(job_id,environment_id,state,correlation_id) VALUES ($1,$2,'dispatching',$3) ON CONFLICT(job_id) DO NOTHING", &[&job.id,&environment,&correlation]).await?
    } else {
        tx.execute("INSERT INTO novu_rust_resend_attempts(job_id,environment_id,state) VALUES ($1,$2,'dispatching') ON CONFLICT(job_id) DO NOTHING", &[&job.id,&environment]).await?
    };
    if inserted == 0 {
        tx.execute("UPDATE novu_queue_jobs SET status='failed',last_error='Resend attempt already exists',lease_token=NULL,lease_until=NULL WHERE id=$1 AND environment_id=$2", &[&job.id,&environment]).await?;
        tx.commit().await?;
        return Ok(true);
    }
    let mut recipients = Vec::new();
    if email
        .to
        .len()
        .saturating_add(email.cc.len())
        .saturating_add(email.bcc.len())
        <= 1000
    {
        for (kind, addresses) in [("to", &email.to), ("cc", &email.cc), ("bcc", &email.bcc)] {
            for (index, address) in addresses.iter().enumerate() {
                recipients.push(
                    serde_json::json!({"kind":kind,"idx":index,"hash":recipient_hash(address)}),
                );
            }
        }
    }
    tx.execute("INSERT INTO novu_rust_resend_recipients(job_id,environment_id,recipient_kind,recipient_index,recipient_hash) SELECT $1,$2,r.kind,r.idx,r.hash FROM jsonb_to_recordset($3::jsonb) AS r(kind text,idx integer,hash text)", &[&job.id,&environment,&serde_json::json!(recipients)]).await?;
    let prepared = match prepared {
        Ok(body) => body,
        Err(_) => {
            if !live(&tx, &job).await? {
                tx.rollback().await?;
                return Ok(true);
            }
            tx.execute("UPDATE novu_rust_resend_attempts SET state='rejected',updated_at=clock_timestamp() WHERE job_id=$1 AND environment_id=$2 AND state='dispatching'", &[&job.id,&environment]).await?;
            if email.message_id.is_some() {
                tx.execute("UPDATE novu_rust_email_messages SET delivery_state='rejected',status='error',error_id='mail_unexpected_error',error_text='invalid Resend request' WHERE environment_id=$1 AND email_job_id=$2", &[&environment,&job.id]).await?;
            }
            tx.execute("UPDATE novu_queue_jobs SET status='failed',last_error='invalid Resend request',lease_token=NULL,lease_until=NULL WHERE id=$1 AND environment_id=$2", &[&job.id,&environment]).await?;
            tx.commit().await?;
            return Ok(true);
        }
    };
    tx.execute("UPDATE public.novu_queue_jobs SET resend_scheduled_at=$3::text::timestamptz WHERE environment_id=$1 AND id=$2", &[&environment,&job.id,&email.resend_scheduled_at]).await?;
    if email.message_id.is_some() {
        tx.execute("UPDATE novu_rust_email_messages SET delivery_state='dispatching' WHERE environment_id=$1 AND email_job_id=$2", &[&environment,&job.id]).await?;
    }
    let retry_id: Option<Uuid> = if retry_key {
        tx.query_opt("SELECT id FROM novu_rust_delivery_reconciliations WHERE environment_id=$1 AND job_id=$2 AND target_index IS NULL AND decision='retry' ORDER BY decision_order DESC LIMIT 1", &[&environment, &job.id]).await?.map(|row| row.get(0))
    } else {
        None
    };
    if !live(&tx, &job).await? {
        tx.rollback().await?;
        return Ok(true);
    }
    tx.commit().await?;
    let key = match retry_id {
        Some(id) => format!("swallow/{}/retry/{id}", job.id),
        None => format!("swallow/{}", job.id),
    };
    let result = if replay_uncertain_once && retry_id.is_none() {
        let mut result = provider.send_prepared_with_correlation(prepared.clone(), Some(&key), correlation).await;
        if matches!(&result, Err(SendError::Transport | SendError::InvalidResponse) | Err(SendError::Provider(408 | 500..=599))) {
            tokio::time::sleep(Duration::from_secs(1)).await;
            if recheck_immediate_replay(client, environment, &job, provider, policy, &prepared, true).await? {
                #[cfg(feature = "test-mocks")]
                let proceed = test_replay_gate().await;
                #[cfg(not(feature = "test-mocks"))]
                let proceed = true;
                if proceed && recheck_immediate_replay(client, environment, &job, provider, policy, &prepared, false).await? {
                    // A rejection or ambiguous replay cannot disprove acceptance of the first request.
                    let replay = provider.send_prepared_with_correlation(prepared, Some(&key), correlation).await;
                    if replay.is_ok() { result = replay; }
                }
            }
        }
        result
    } else {
        provider.send_prepared_with_correlation(prepared, Some(&key), correlation).await
    };
    let (state, code, id, diagnostic, retry_after) = match result {
        Ok(id) => ("accepted", None, Some(id), None, None),
        Err(SendError::RateLimited(delay)) => (
            "rejected",
            Some(429),
            None,
            Some("Resend rejected request"),
            delay,
        ),
        Err(SendError::Provider(code))
            if !(400..500).contains(&code) || [408, 409].contains(&code) =>
        {
            (
                "unknown",
                Some(i32::from(code)),
                None,
                Some("Resend outcome unknown"),
                None,
            )
        }
        Err(SendError::Provider(code)) => (
            "rejected",
            Some(i32::from(code)),
            None,
            Some("Resend rejected request"),
            None,
        ),
        Err(SendError::Transport | SendError::InvalidResponse) => {
            ("unknown", None, None, Some("Resend outcome unknown"), None)
        }
        Err(_) => ("rejected", None, None, Some("invalid Resend request"), None),
    };
    let tx = client.transaction().await?;
    if !live(&tx, &job).await? {
        tx.rollback().await?;
        return Ok(true);
    }
    let updated=tx.execute("UPDATE novu_rust_resend_attempts SET state=$3,http_status=$4,provider_message_id=$5,updated_at=clock_timestamp() WHERE job_id=$1 AND environment_id=$2 AND state='dispatching'", &[&job.id,&environment,&state,&code,&id]).await?;
    if updated == 1 {
        if cooldown && code == Some(429) {
            let seconds = retry_after.and_then(|value| i32::try_from(value).ok());
            crate::resend_cooldown::record(&tx, environment, seconds).await?;
        }
        if let Some(seconds) = retry_after {
            let seconds = i32::try_from(seconds).expect("bounded retry-after");
            tx.execute("UPDATE public.novu_rust_resend_attempts SET retry_after_seconds=$3, retry_not_before=clock_timestamp()+make_interval(secs => $3::integer::double precision) WHERE job_id=$1 AND environment_id=$2", &[&job.id,&environment,&seconds]).await?;
        }
        if email.message_id.is_some() {
            tx.execute("UPDATE novu_rust_email_messages SET delivery_state=$3,identifier=CASE WHEN $3='accepted' THEN $4 ELSE identifier END,status=CASE WHEN $3='accepted' THEN 'sent' ELSE 'error' END,error_id=CASE WHEN $3='accepted' THEN NULL ELSE 'mail_unexpected_error' END,error_text=CASE WHEN $3='accepted' THEN NULL ELSE $5 END WHERE environment_id=$1 AND email_job_id=$2", &[&environment,&job.id,&state,&id,&diagnostic]).await?;
        }
        let status = if state == "accepted" {
            "completed"
        } else {
            "failed"
        };
        tx.execute("UPDATE novu_queue_jobs SET status=$3,last_error=$4,lease_token=NULL,lease_until=NULL WHERE id=$1 AND environment_id=$2", &[&job.id,&environment,&status,&diagnostic]).await?;
    }
    tx.commit().await?;
    Ok(true)
}

// Re-render from the original job under current subscriber and step checks. A
// failed recheck rolls back any preparation side effects and keeps the first
// attempt unknown; it must not mark a possibly delivered message cancelled.
async fn recheck_immediate_replay(
    client: &mut Client, environment: &str, job: &ClaimedJob, provider: &Resend,
    policy: crate::email_step_policy::Policy, original: &serde_json::Value, reserve: bool,
) -> Result<bool, Error> {
    let Ok(mut email) = serde_json::from_value::<Email>(job.payload.clone()) else { return Ok(false); };
    let tx=client.transaction().await?;
    let subscriber=crate::sendgrid_queue::lock_workflow_subscriber(&tx,environment,&email).await?;
    if !live(&tx,job).await? { tx.rollback().await?; return Ok(false); }
    if !matches!(crate::email_step_policy::check(&tx,environment,job,&email,policy).await?,crate::email_step_policy::Check::Continue) { tx.rollback().await?; return Ok(false); }
    // Do not replay a job that has ever undergone an operator/automatic retry.
    let previous:bool=tx.query_one("SELECT EXISTS(SELECT 1 FROM public.novu_rust_delivery_reconciliations WHERE environment_id=$1 AND job_id=$2 AND decision='retry')",&[&environment,&job.id]).await?.get(0);
    if previous {tx.rollback().await?;return Ok(false);}
    tx.batch_execute("SAVEPOINT resend_replay_render").await?;
    let valid=crate::sendgrid_queue::prepare_workflow_email(&tx,environment,job,&mut email,subscriber).await?;
    let same=valid && provider.prepare(&email).is_ok_and(|body| body==*original);
    tx.batch_execute("ROLLBACK TO SAVEPOINT resend_replay_render").await?;
    if !same {tx.rollback().await?;return Ok(false);}
    let eligible=if reserve {
        tx.execute("UPDATE public.novu_rust_resend_attempts a SET same_request_replays=1 WHERE a.job_id=$1 AND a.environment_id=$2 AND a.state='dispatching' AND a.same_request_replays=0 AND EXISTS(SELECT 1 FROM public.novu_queue_jobs j WHERE j.id=a.job_id AND j.environment_id=$2 AND j.status='running' AND j.lease_token=$3 AND j.lease_until>clock_timestamp()+interval '20 seconds')",&[&job.id,&environment,&job.lease_token]).await? == 1
    } else {
        tx.query_one("SELECT EXISTS(SELECT 1 FROM public.novu_rust_resend_attempts a JOIN public.novu_queue_jobs j ON j.id=a.job_id AND j.environment_id=a.environment_id WHERE a.job_id=$1 AND a.environment_id=$2 AND a.state='dispatching' AND a.same_request_replays=1 AND j.status='running' AND j.lease_token=$3 AND j.lease_until>clock_timestamp()+interval '20 seconds')",&[&job.id,&environment,&job.lease_token]).await?.get(0)
    };
    if !eligible {tx.rollback().await?;return Ok(false);}
    tx.commit().await?;
    Ok(true)
}

// Test-feature-only barrier for exercising the post-reservation crash window.
#[cfg(feature = "test-mocks")]
async fn test_replay_gate() -> bool {
    let Some(path) = std::env::var_os("SWALLOW_TEST_RESEND_REPLAY_GATE") else { return true; };
    let path=std::path::PathBuf::from(path);
    if std::fs::write(path.join("ready"),std::process::id().to_string()).is_err() {return false;}
    while !path.join("release").exists() {tokio::time::sleep(Duration::from_millis(50)).await;}
    true
}

pub async fn recover_expired(client: &mut Client, environment: &str) -> Result<usize, Error> {
    let tx = client.transaction().await?;
    let rows=tx.query("SELECT id FROM novu_queue_jobs WHERE environment_id=$1 AND kind=$2 AND status='running' AND lease_until<=clock_timestamp() ORDER BY lease_until,id FOR UPDATE SKIP LOCKED LIMIT 100", &[&environment,&JOB_KIND]).await?;
    for row in &rows {
        let id: Uuid = row.get(0);
        let attempt=tx.execute("UPDATE novu_rust_resend_attempts SET state='unknown',updated_at=clock_timestamp() WHERE job_id=$1 AND environment_id=$2 AND state='dispatching'", &[&id,&environment]).await?;
        if attempt == 1 {
            tx.execute("UPDATE novu_rust_email_messages SET delivery_state='unknown' WHERE environment_id=$1 AND email_job_id=$2", &[&environment,&id]).await?;
            tx.execute("UPDATE novu_queue_jobs SET status='failed',last_error='Resend outcome unknown',lease_token=NULL,lease_until=NULL WHERE id=$1 AND environment_id=$2", &[&id,&environment]).await?;
        } else {
            let exists:bool=tx.query_one("SELECT EXISTS(SELECT 1 FROM novu_rust_resend_attempts WHERE job_id=$1 AND environment_id=$2)", &[&id,&environment]).await?.get(0);
            let (status, error) = if exists {
                ("failed", Some("Resend attempt already exists"))
            } else {
                ("pending", None)
            };
            tx.execute("UPDATE novu_queue_jobs SET status=$3,last_error=$4,lease_token=NULL,lease_until=NULL WHERE id=$1 AND environment_id=$2", &[&id,&environment,&status,&error]).await?;
        }
    }
    tx.commit().await?;
    Ok(rows.len())
}

pub const POLL_INTERVAL: Duration = Duration::from_secs(1);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{enqueue, resend::Config};
    use serde_json::json;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };
    use tokio_postgres::NoTls;

    #[tokio::test]
    #[ignore = "requires fully migrated disposable PostgreSQL and loopback"]
    async fn immediate_replay_keeps_body_and_key_and_preserves_uncertainty() {
        use tokio::time::timeout;
        let url=std::env::var("TEST_DATABASE_URL").unwrap();
        let (mut db,driver)=tokio_postgres::connect(&url,NoTls).await.unwrap();tokio::spawn(driver);
        let env=format!("resend-replay-{}",Uuid::new_v4().simple());db.execute("INSERT INTO public.novu_rust_environments VALUES($1,'org')",&[&env]).await.unwrap();assert!(register_worker_policy(&db,&env,false,false).await.unwrap());
        for scenario in ["success","lost-response","invalid-success","http-408","rejected-replay","conflict-replay","first-conflict","disabled","changed-address","disabled-preference","short-lease","rate-limit","prior-retry"] {
            let job=if ["changed-address","disabled-preference"].contains(&scenario) {
                db.execute("INSERT INTO public.novu_rust_workflows(environment_id,identifier,active,has_steps,email_subject,email_html,email_provider,inbox_metadata) VALUES($1,'fixture',true,true,'Hello','<p>Hello</p>','resend','{\"id\":\"64a000000000000000000051\"}') ON CONFLICT(environment_id,identifier) DO NOTHING",&[&env]).await.unwrap();
                db.execute("INSERT INTO public.novu_rust_subscribers(environment_id,subscriber_id,internal_id,profile) VALUES($1,'alice','64a000000000000000000054','{\"email\":\"alice@example.test\"}') ON CONFLICT(environment_id,subscriber_id) DO UPDATE SET profile=EXCLUDED.profile",&[&env]).await.unwrap();
                let transaction=format!("replay-{scenario}");
                enqueue(&db,Uuid::new_v4(),&env,crate::in_app::JOB_KIND,&json!({"workflow":"fixture","transactionId":transaction,"content":"","payload":{},"recipients":[{"subscriberId":"alice"}],"recipientIdentitySnapshot":{"alice":"64a000000000000000000054"},"severityOverride":null})).await.unwrap();assert!(crate::in_app::process_one(&mut db).await.unwrap());
                db.query_one("SELECT email_job_id FROM public.novu_rust_email_step_outcomes WHERE environment_id=$1 AND transaction_id=$2",&[&env,&transaction]).await.unwrap().get::<_,Uuid>(0)
            } else {let id=Uuid::new_v4();enqueue(&db,id,&env,JOB_KIND,&json!({"to":["to@example.test"],"subject":"fixture","html":"hello"})).await.unwrap();id};
            if scenario=="prior-retry" {db.execute("INSERT INTO public.novu_rust_delivery_reconciliations(id,environment_id,job_id,decision,operator_name,reason) VALUES($1,$2,$3,'retry','fixture','fixture')",&[&Uuid::new_v4(),&env,&job]).await.unwrap();}
            let listener=TcpListener::bind("127.0.0.1:0").await.unwrap();let provider=Resend::test_endpoint(Config{api_key:"fixture".into(),from:"sender@example.test".into(),sender_name:String::new()},&format!("http://{}/emails",listener.local_addr().unwrap())).unwrap();let env2=env.clone();let scenario2=scenario.to_owned();
            let (mutator,driver)=tokio_postgres::connect(&url,NoTls).await.unwrap();tokio::spawn(driver);
            let server=tokio::spawn(async move {
                async fn read_request(socket:&mut tokio::net::TcpStream)->(String,Vec<u8>) {let mut data=Vec::new();let mut buf=[0u8;4096];loop {let n=socket.read(&mut buf).await.unwrap();assert!(n>0);data.extend_from_slice(&buf[..n]);if let Some(pos)=data.windows(4).position(|x|x==b"\r\n\r\n"){let headers=String::from_utf8(data[..pos].to_vec()).unwrap();let length:usize=headers.lines().find_map(|line|line.to_ascii_lowercase().strip_prefix("content-length: ").and_then(|v|v.parse().ok())).unwrap();if data.len()>=pos+4+length{return (headers,data[pos+4..pos+4+length].to_vec());}}}}
                let (mut first,_)=listener.accept().await.unwrap();let (headers,body)=read_request(&mut first).await;
                if scenario2=="changed-address" {mutator.execute("UPDATE public.novu_rust_subscribers SET profile='{\"email\":\"new@example.test\"}' WHERE environment_id=$1 AND subscriber_id='alice'",&[&env2]).await.unwrap();}
                if scenario2=="disabled-preference" {mutator.execute("INSERT INTO public.novu_rust_subscriber_preferences(environment_id,subscriber_id,channels) VALUES($1,'alice','{\"email\":false}') ON CONFLICT(environment_id,subscriber_id) DO UPDATE SET channels=EXCLUDED.channels",&[&env2]).await.unwrap();}
                if scenario2=="short-lease" {mutator.execute("UPDATE public.novu_queue_jobs SET lease_until=clock_timestamp()+interval '5 seconds' WHERE id=$1",&[&job]).await.unwrap();}
                if scenario2=="lost-response" {drop(first);} else if scenario2=="invalid-success" {first.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.unwrap();} else {let status=match scenario2.as_str(){"rate-limit"=>"429 Too Many Requests","first-conflict"=>"409 Conflict","http-408"=>"408 Request Timeout",_=>"500 Internal Server Error"};first.write_all(format!("HTTP/1.1 {status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();}
                if ["success","lost-response","invalid-success","http-408","rejected-replay","conflict-replay"].contains(&scenario2.as_str()) {let (mut second,_)=timeout(Duration::from_secs(3),listener.accept()).await.unwrap().unwrap();let (headers2,body2)=read_request(&mut second).await;let key=|h:&str| h.lines().find(|line|line.to_ascii_lowercase().starts_with("idempotency-key:")).unwrap().to_owned();assert_eq!(key(&headers),key(&headers2));assert_eq!(body,body2);let response=match scenario2.as_str() {"rejected-replay"=>"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n","conflict-replay"=>"HTTP/1.1 409 Conflict\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",_=>"HTTP/1.1 200 OK\r\nContent-Length: 17\r\nConnection: close\r\n\r\n{\"id\":\"accepted\"}"};second.write_all(response.as_bytes()).await.unwrap();} else {assert!(timeout(Duration::from_millis(400),listener.accept()).await.is_err());}
            });
            assert!(process_one_with_immediate_replay(&mut db,&env,&provider,false,crate::email_step_policy::Policy::Snapshot,true,false,false,false,scenario!="disabled").await.unwrap());
            server.await.unwrap();let row=db.query_one("SELECT j.status,a.state,a.same_request_replays FROM public.novu_queue_jobs j JOIN public.novu_rust_resend_attempts a ON a.job_id=j.id WHERE j.id=$1",&[&job]).await.unwrap();
            if ["success","lost-response","invalid-success","http-408"].contains(&scenario) {assert_eq!(row.get::<_,String>(0),"completed");assert_eq!(row.get::<_,String>(1),"accepted");} else if scenario=="rate-limit" {assert_eq!(row.get::<_,String>(1),"rejected");} else {assert_eq!(row.get::<_,String>(1),"unknown");}
            assert_eq!(row.get::<_,i32>(2),if ["success","lost-response","invalid-success","http-408","rejected-replay","conflict-replay"].contains(&scenario){1}else{0});
            if ["changed-address","disabled-preference"].contains(&scenario) {let row=db.query_one("SELECT status,delivery_state FROM public.novu_rust_email_messages WHERE environment_id=$1 AND email_job_id=$2",&[&env,&job]).await.unwrap();assert_eq!(row.get::<_,String>(1),"unknown");}
        }
    }

    #[tokio::test]
    #[ignore = "requires fully migrated disposable PostgreSQL and loopback"]
    async fn invalid_alternatives_are_rejected_before_dispatch_reservation() {
        let (mut db, driver) = tokio_postgres::connect(&std::env::var("TEST_DATABASE_URL").unwrap(), NoTls).await.unwrap();
        tokio::spawn(driver);
        let env = format!("resend-invalid-{}", Uuid::new_v4());
        db.execute("INSERT INTO public.novu_rust_environments VALUES($1,'org')", &[&env]).await.unwrap();
        db.execute("INSERT INTO public.novu_rust_workflows(environment_id,identifier,active,has_steps,email_subject,email_html,email_provider,inbox_metadata) VALUES($1,'fixture',true,true,'hello','<p>hello</p>','resend','{\"id\":\"64a000000000000000000051\"}')", &[&env]).await.unwrap();
        db.execute("INSERT INTO public.novu_rust_subscribers(environment_id,subscriber_id,internal_id,profile) VALUES($1,'alice','64a000000000000000000054','{\"email\":\"alice@example.test\"}')", &[&env]).await.unwrap();
        let trigger=Uuid::new_v4();
        enqueue(&db,trigger,&env,crate::in_app::JOB_KIND,&json!({"workflow":"fixture","transactionId":"invalid-alt","content":"","payload":{},"recipients":[{"subscriberId":"alice"}],"recipientIdentitySnapshot":{"alice":"64a000000000000000000054"},"emailOverride":{"alternatives":[{"contentType":"text/calendar","content":"meeting"}]},"severityOverride":null})).await.unwrap();
        assert!(crate::in_app::process_one(&mut db).await.unwrap());
        let job:Uuid=db.query_one("SELECT email_job_id FROM public.novu_rust_email_step_outcomes WHERE environment_id=$1 AND transaction_id='invalid-alt'",&[&env]).await.unwrap().get(0);
        let listener=TcpListener::bind("127.0.0.1:0").await.unwrap();
        let provider=Resend::test_endpoint(Config{api_key:"fixture".into(),from:"sender@example.test".into(),sender_name:String::new()},&format!("http://{}/emails",listener.local_addr().unwrap())).unwrap();
        db.batch_execute("CREATE FUNCTION public.test_resend_invalid_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture'; END $$; CREATE TRIGGER test_resend_invalid_fail BEFORE UPDATE OF delivery_state ON public.novu_rust_email_messages FOR EACH ROW WHEN (NEW.delivery_state='rejected') EXECUTE FUNCTION public.test_resend_invalid_fail()").await.unwrap();
        assert!(process_one(&mut db,&env,&provider).await.is_err());
        assert_eq!(db.query_one("SELECT count(*) FROM public.novu_rust_resend_attempts WHERE job_id=$1",&[&job]).await.unwrap().get::<_,i64>(0),0);
        db.batch_execute("DROP TRIGGER test_resend_invalid_fail ON public.novu_rust_email_messages; DROP FUNCTION public.test_resend_invalid_fail()").await.unwrap();
        db.execute("UPDATE public.novu_queue_jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1",&[&job]).await.unwrap();
        assert_eq!(recover_expired(&mut db,&env).await.unwrap(),1);
        assert!(process_one(&mut db,&env,&provider).await.unwrap());
        assert!(tokio::time::timeout(Duration::from_millis(300),listener.accept()).await.is_err());
        let row=db.query_one("SELECT j.status,j.last_error,a.state,a.http_status,m.status,m.delivery_state,m.error_text FROM public.novu_queue_jobs j JOIN public.novu_rust_resend_attempts a ON a.job_id=j.id JOIN public.novu_rust_email_messages m ON m.email_job_id=j.id AND m.environment_id=j.environment_id WHERE j.id=$1",&[&job]).await.unwrap();
        assert_eq!(row.get::<_,String>(0),"failed");assert_eq!(row.get::<_,String>(1),"invalid Resend request");assert_eq!(row.get::<_,String>(2),"rejected");assert_eq!(row.get::<_,Option<i32>>(3),None);assert_eq!(row.get::<_,String>(4),"error");assert_eq!(row.get::<_,String>(5),"rejected");assert_eq!(row.get::<_,String>(6),"invalid Resend request");
        assert!(!process_one(&mut db,&env,&provider).await.unwrap());
        let valid=Uuid::new_v4();
        enqueue(&db,valid,&env,JOB_KIND,&json!({"to":["alice@example.test"],"subject":"valid","html":"<p>ok</p>"})).await.unwrap();
        let server=tokio::spawn(async move {
            let (mut socket,_)=listener.accept().await.unwrap();let mut request=[0u8;4096];assert!(socket.read(&mut request).await.unwrap()>0);
            socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 18\r\nConnection: close\r\n\r\n{\"id\":\"mock-id-1\"}").await.unwrap();
        });
        assert!(process_one(&mut db,&env,&provider).await.unwrap());server.await.unwrap();
        let accepted=db.query_one("SELECT j.status,a.state,a.provider_message_id FROM public.novu_queue_jobs j JOIN public.novu_rust_resend_attempts a ON a.job_id=j.id WHERE j.id=$1",&[&valid]).await.unwrap();
        assert_eq!(accepted.get::<_,String>(0),"completed");assert_eq!(accepted.get::<_,String>(1),"accepted");assert_eq!(accepted.get::<_,Option<String>>(2).as_deref(),Some("mock-id-1"));
    }

    #[tokio::test]
    #[ignore = "requires migrated disposable PostgreSQL and loopback"]
    async fn shared_resend_cooldown_defers_other_jobs_and_fences_policy() {
        let url=std::env::var("TEST_DATABASE_URL").unwrap();
        let (mut db,driver)=tokio_postgres::connect(&url,NoTls).await.unwrap();
        tokio::spawn(async move { driver.await.unwrap() });
        let env=format!("resend-cooldown-{}",Uuid::new_v4());
        db.execute("INSERT INTO public.novu_rust_environments VALUES($1,'org')", &[&env]).await.unwrap();
        assert!(register_worker_policy(&db,&env,false,true).await.unwrap());
        assert!(register_worker_policy(&db,&env,false,true).await.unwrap());
        assert!(!register_worker_policy(&db,&env,false,false).await.unwrap());
        assert!(!register_worker_policy(&db,&env,true,true).await.unwrap());
        assert!(db.query_one("SELECT public.novu_rust_check_resend_shared_cooldown($1,false)", &[&env]).await.is_err());
        db.execute("INSERT INTO public.novu_rust_resend_cooldown_policies(environment_id,fallback_seconds) VALUES($1,45)", &[&env]).await.unwrap();
        db.execute("INSERT INTO public.novu_rust_resend_cooldown_workers SELECT $1,rolname,oid FROM pg_roles WHERE rolname=session_user", &[&env]).await.unwrap();
        let first=Uuid::new_v4();
        enqueue(&db,first,&env,JOB_KIND,&json!({"to":["to@example.test"],"subject":"first","html":"test"})).await.unwrap();
        let listener=TcpListener::bind("127.0.0.1:0").await.unwrap();
        let provider=Resend::test_endpoint(Config {api_key:"fixture".into(),from:"from@example.test".into(),sender_name:String::new()}, &format!("http://{}/emails",listener.local_addr().unwrap())).unwrap();
        let server=tokio::spawn(async move {
            let (mut socket,_)=listener.accept().await.unwrap();let mut data=[0u8;4096]; assert!(socket.read(&mut data).await.unwrap()>0);
            socket.write_all(b"HTTP/1.1 429 Too Many Requests\r\nDate: Wed, 21 Oct 2015 07:28:00 GMT\r\nRetry-After: Wed, 21 Oct 2015 07:28:30 GMT\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.unwrap();
        });
        assert!(process_one_with_registered_policy_and_cooldown(&mut db,&env,&provider,false,crate::email_step_policy::Policy::Snapshot,false,false,false,true).await.unwrap());
        server.await.unwrap();
        let row=db.query_one("SELECT state,http_status FROM public.novu_rust_resend_attempts WHERE job_id=$1", &[&first]).await.unwrap();
        assert_eq!(row.get::<_,String>(0),"rejected"); assert_eq!(row.get::<_,Option<i32>>(1),Some(429));
        assert!(db.query_one("SELECT not_before>clock_timestamp()+interval '20 seconds' AND not_before<clock_timestamp()+interval '40 seconds' FROM public.novu_rust_resend_cooldown_policies WHERE environment_id=$1", &[&env]).await.unwrap().get::<_,bool>(0));
        let second=Uuid::new_v4();
        enqueue(&db,second,&env,JOB_KIND,&json!({"to":["to@example.test"],"subject":"second","html":"test"})).await.unwrap();
        let (mut second_db,second_driver)=tokio_postgres::connect(&url,NoTls).await.unwrap();
        tokio::spawn(async move {second_driver.await.unwrap()});
        assert!(process_one_with_registered_policy_and_cooldown(&mut second_db,&env,&provider,false,crate::email_step_policy::Policy::Snapshot,false,false,false,true).await.unwrap());
        let row=db.query_one("SELECT status,last_error,available_at>clock_timestamp(),EXISTS(SELECT 1 FROM public.novu_rust_resend_attempts a WHERE a.job_id=j.id) FROM public.novu_queue_jobs j WHERE id=$1", &[&second]).await.unwrap();
        assert_eq!(row.get::<_,String>(0),"pending");assert_eq!(row.get::<_,String>(1),"Resend shared cooldown active");assert!(row.get::<_,bool>(2));assert!(!row.get::<_,bool>(3));
        db.execute("UPDATE public.novu_rust_resend_cooldown_policies SET not_before='-infinity' WHERE environment_id=$1", &[&env]).await.unwrap();
        db.query_one("SELECT public.novu_rust_resend_cooldown_record($1,NULL)", &[&env]).await.unwrap();
        assert!(db.query_one("SELECT not_before>clock_timestamp()+interval '35 seconds' FROM public.novu_rust_resend_cooldown_policies WHERE environment_id=$1", &[&env]).await.unwrap().get::<_,bool>(0));
        let role=format!("resend_guard_{}",Uuid::new_v4().simple());
        db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_resend_cooldown_check(text),public.novu_rust_resend_cooldown_record(text,integer) TO {role}")).await.unwrap();
        let (other,driver)=tokio_postgres::connect(&url,NoTls).await.unwrap();tokio::spawn(async move {driver.await.unwrap()});
        other.batch_execute(&format!("SET SESSION AUTHORIZATION {role}")).await.unwrap();
        assert!(other.query_one("SELECT public.novu_rust_resend_cooldown_check($1)", &[&env]).await.is_err());
        db.execute("INSERT INTO public.novu_rust_resend_cooldown_workers SELECT $1,rolname,oid FROM pg_roles WHERE rolname::text=$2", &[&env,&role]).await.unwrap();
        assert!(other.query_one("SELECT public.novu_rust_resend_cooldown_check($1)", &[&env]).await.is_ok());
        assert!(other.query_one("SELECT public.novu_rust_resend_cooldown_check($1)", &[&"other-environment"]).await.is_err());
        let tx=db.transaction().await.unwrap();
        tx.query_one("SELECT public.novu_rust_resend_cooldown_check($1)", &[&env]).await.unwrap();
        let env2=env.clone();
        let mut update=tokio::spawn(async move { other.query_one("SELECT public.novu_rust_resend_cooldown_record($1,30)", &[&env2]).await });
        assert!(tokio::time::timeout(Duration::from_millis(200),&mut update).await.is_err());
        tx.commit().await.unwrap();
        assert!(tokio::time::timeout(Duration::from_secs(5),update).await.unwrap().unwrap().is_ok());
    }

    #[tokio::test]
    #[ignore = "requires migrated disposable PostgreSQL and loopback"]
    async fn rate_limit_header_is_recorded_on_the_attempt() {
        let (mut db, driver) =
            tokio_postgres::connect(&std::env::var("TEST_DATABASE_URL").unwrap(), NoTls)
                .await
                .unwrap();
        tokio::spawn(async move { driver.await.unwrap() });
        let env = format!("rate-limit-{}", Uuid::new_v4());
        db.execute(
            "INSERT INTO public.novu_rust_environments VALUES ($1,'org')",
            &[&env],
        )
        .await
        .unwrap();
        let job = Uuid::new_v4();
        enqueue(
            &db,
            job,
            &env,
            JOB_KIND,
            &json!({"to":["to@example.test"],"subject":"rate","html":"test"}),
        )
        .await
        .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let provider = Resend::test_endpoint(
            Config {
                api_key: "re_test".into(),
                from: "from@example.test".into(),
                sender_name: String::new(),
            },
            &format!("http://{}/emails", listener.local_addr().unwrap()),
        )
        .unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = [0; 4096];
            assert!(socket.read(&mut buf).await.unwrap() > 0);
            socket.write_all(b"HTTP/1.1 429 Too Many Requests\r\nDate: Wed, 21 Oct 2015 07:28:00 GMT\r\nRetry-After: Wed, 21 Oct 2015 07:28:30 GMT\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.unwrap();
        });
        assert!(process_one(&mut db, &env, &provider).await.unwrap());
        server.await.unwrap();
        let row=db.query_one("SELECT j.status,a.state,a.http_status,a.retry_after_seconds,a.retry_not_before > a.updated_at FROM public.novu_queue_jobs j JOIN public.novu_rust_resend_attempts a ON a.job_id=j.id WHERE j.id=$1 AND j.environment_id=$2", &[&job,&env]).await.unwrap();
        assert_eq!(row.get::<_, String>(0), "failed");
        assert_eq!(row.get::<_, String>(1), "rejected");
        assert_eq!(row.get::<_, Option<i32>>(2), Some(429));
        assert_eq!(row.get::<_, Option<i32>>(3), Some(30));
        assert!(row.get::<_, bool>(4));
        assert!(!process_one(&mut db, &env, &provider).await.unwrap());
    }

    #[tokio::test]
    #[ignore = "requires the workflow_email_attachments API fixture and loopback"]
    async fn workflow_email_attachments_reach_both_mock_providers() {
        let (mut db, driver) =
            tokio_postgres::connect(&std::env::var("TEST_DATABASE_URL").unwrap(), NoTls)
                .await
                .unwrap();
        tokio::spawn(async move { driver.await.unwrap() });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/send", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            for provider in ["resend", "sendgrid"] {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                let mut buf = [0u8; 4096];
                let body_start = loop {
                    let n = socket.read(&mut buf).await.unwrap();
                    assert!(n > 0);
                    bytes.extend_from_slice(&buf[..n]);
                    if let Some(pos) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
                        let headers = String::from_utf8_lossy(&bytes[..pos]);
                        let length: usize = headers
                            .lines()
                            .find_map(|line| {
                                line.to_ascii_lowercase()
                                    .strip_prefix("content-length: ")
                                    .and_then(|v| v.parse().ok())
                            })
                            .unwrap();
                        if bytes.len() >= pos + 4 + length {
                            break pos + 4;
                        }
                    }
                };
                let body: serde_json::Value = serde_json::from_slice(&bytes[body_start..]).unwrap();
                assert_eq!(body["attachments"][0]["content"], "aGVsbG8=");
                assert_eq!(body["attachments"][1]["content"], "AAH/");
                if provider == "resend" {
                    assert_eq!(body["attachments"][1]["content_id"], "logo");
                    assert!(body["attachments"][0].get("content_id").is_none());
                    assert!(body["attachments"][0].get("disposition").is_none());
                    assert!(body["attachments"][1].get("disposition").is_none());
                } else {
                    assert_eq!(body["attachments"][0]["disposition"], "attachment");
                    assert_eq!(body["attachments"][1]["disposition"], "inline");
                }
                let response = if provider == "resend" {
                    "HTTP/1.1 200 OK\r\nContent-Length: 20\r\nConnection: close\r\n\r\n{\"id\":\"mock-resend\"}"
                } else {
                    "HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\nX-Message-Id: mock-sendgrid\r\nConnection: close\r\n\r\n"
                };
                socket.write_all(response.as_bytes()).await.unwrap();
            }
        });
        let resend = Resend::test_endpoint(
            Config {
                api_key: "fixture".into(),
                from: "sender@example.test".into(),
                sender_name: "Sender".into(),
            },
            &endpoint,
        )
        .unwrap();
        assert!(
            process_one(&mut db, "attachment-env", &resend)
                .await
                .unwrap()
        );
        let sendgrid = crate::sendgrid::Sendgrid::test_endpoint(
            crate::sendgrid::Config {
                api_key: "fixture".into(),
                from: "sender@example.test".into(),
                sender_name: "Sender".into(),
                region: crate::sendgrid::Region::Global,
            },
            &endpoint,
        )
        .unwrap();
        assert!(
            crate::sendgrid_queue::process_one(&mut db, "attachment-env", &sendgrid)
                .await
                .unwrap()
        );
        server.await.unwrap();
        let statuses=db.query("SELECT status FROM public.novu_queue_jobs WHERE environment_id='attachment-env' AND kind IN ('experimental_resend_email','experimental_sendgrid_email') ORDER BY kind",&[]).await.unwrap();
        assert_eq!(statuses.len(), 2);
        assert!(
            statuses
                .iter()
                .all(|row| row.get::<_, String>(0) == "completed")
        );
    }

    #[tokio::test]
    #[ignore = "requires differential PostgreSQL and loopback access"]
    async fn capture_reference_resend_workflow() {
        let (mut db, driver) =
            tokio_postgres::connect(&std::env::var("TEST_DATABASE_URL").unwrap(), NoTls)
                .await
                .unwrap();
        tokio::spawn(async move { driver.await.unwrap() });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/emails", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            let mut buf = [0; 4096];
            loop {
                let n = socket.read(&mut buf).await.unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&buf[..n]);
                if let Some(pos) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
                    let h = String::from_utf8_lossy(&bytes[..pos]);
                    let len: usize = h
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length: ")
                                .and_then(|v| v.parse().ok())
                        })
                        .unwrap();
                    if bytes.len() >= pos + 4 + len {
                        std::fs::write(
                            std::env::var("TEST_EMAIL_CAPTURE_PATH").unwrap(),
                            &bytes[pos + 4..pos + 4 + len],
                        )
                        .unwrap();
                        break;
                    }
                }
            }
            let body = "{\"id\":\"mock-resend-id\"}";
            socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
        });
        let provider = Resend::test_endpoint(
            Config {
                api_key: "re_fixture".into(),
                from: "sender@example.test".into(),
                sender_name: "Sender".into(),
            },
            &endpoint,
        )
        .unwrap();
        assert!(
            process_one(&mut db, "64a000000000000000000002", &provider)
                .await
                .unwrap()
        );
        server.await.unwrap();
    }

    #[tokio::test]
    #[ignore = "sends real email and requires a fresh disposable PostgreSQL database"]
    async fn live_workflow_send() {
        let url = std::env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL required");
        let recipient = std::env::var("NOVU_RESEND_TEST_TO").expect("NOVU_RESEND_TEST_TO required");
        let (mut db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
        tokio::spawn(async move { driver.await.unwrap() });
        db.batch_execute("CREATE TABLE novu_rust_environments (id text PRIMARY KEY, organization_id text NOT NULL); INSERT INTO novu_rust_environments VALUES ('env-a','org-a'); CREATE TABLE novu_rust_workflows (environment_id text, identifier text, in_app_content text, has_steps boolean NOT NULL DEFAULT true, PRIMARY KEY (environment_id, identifier))").await.unwrap();
        // Workers 0100, 0102 and 0103 install triggers on API-owned webhook event tables.
        for sql in [
            include_str!("../../../api/rust-api/migrations/0004_sendgrid_webhooks.sql"),
            include_str!("../../../api/rust-api/migrations/0008_sendgrid_dispatch_identity.sql"),
            include_str!("../../../api/rust-api/migrations/0009_sendgrid_dispatch_proof.sql"),
            include_str!("../../../api/rust-api/migrations/0010_resend_webhooks.sql"),
            include_str!("../../../api/rust-api/migrations/0047_resend_event_correlation.sql"),
            include_str!("../../../api/rust-api/migrations/0005_twilio_webhooks.sql"),
            include_str!("../../../api/rust-api/migrations/0006_twilio_webhook_job_link.sql"),
            include_str!("../../../api/rust-api/migrations/0007_twilio_dispatch_identity.sql"),
        ] {
            db.batch_execute(sql).await.unwrap();
        }
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
        let mut paths: Vec<_> = std::fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|p| p.extension().is_some_and(|v| v == "sql"))
            .collect();
        paths.sort();
        for path in paths {
            db.batch_execute(&std::fs::read_to_string(path).unwrap())
                .await
                .unwrap();
        }
        db.execute("INSERT INTO novu_rust_workflows(environment_id,identifier,email_subject,email_html,email_provider,in_app_step_active,inbox_metadata) VALUES ('env-a','live-test','Swallow Resend workflow test','<p>Swallow Resend workflow test message.</p>','resend',false,'{\"id\":\"workflow-id\"}')",&[]).await.unwrap();
        db.execute("INSERT INTO novu_rust_subscribers(environment_id,subscriber_id,internal_id,profile) VALUES ('env-a','live-recipient','live-id',jsonb_build_object('email',$1::text))", &[&recipient]).await.unwrap();
        let trigger = Uuid::new_v4();
        let payload = json!({"workflow":"live-test","transactionId":trigger.to_string(),"content":"","payload":{},"recipients":[{"subscriberId":"live-recipient"}],"recipientIdentitySnapshot":{"live-recipient":"live-id"},"severityOverride":null});
        enqueue(&db, trigger, "env-a", crate::in_app::JOB_KIND, &payload)
            .await
            .unwrap();
        assert!(crate::in_app::process_one(&mut db).await.unwrap());
        let job: Uuid = db
            .query_one(
                "SELECT id FROM novu_queue_jobs WHERE kind=$1 AND payload->>'transaction_id'=$2",
                &[&JOB_KIND, &trigger.to_string()],
            )
            .await
            .unwrap()
            .get(0);
        let provider = Resend::new(Config {
            api_key: std::env::var("NOVU_RESEND_API_KEY").expect("NOVU_RESEND_API_KEY required"),
            from: std::env::var("NOVU_RESEND_FROM").expect("NOVU_RESEND_FROM required"),
            sender_name: String::new(),
        })
        .unwrap();
        assert!(process_one(&mut db, "env-a", &provider).await.unwrap());
        let row=db.query_one("SELECT j.status,a.state,m.delivery_state,a.provider_message_id FROM novu_queue_jobs j JOIN novu_rust_resend_attempts a ON a.job_id=j.id JOIN novu_rust_email_messages m ON m.email_job_id=j.id WHERE j.id=$1", &[&job]).await.unwrap();
        assert_eq!(row.get::<_, String>(0), "completed");
        assert_eq!(row.get::<_, String>(1), "accepted");
        assert_eq!(row.get::<_, String>(2), "accepted");
        assert!(!row.get::<_, String>(3).is_empty());
    }

    #[tokio::test]
    #[ignore = "requires fresh disposable PostgreSQL and loopback"]
    async fn workflow_delivery_revalidates_subscriber() {
        let url = std::env::var("TEST_DATABASE_URL").unwrap();
        let (mut db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
        tokio::spawn(async move { driver.await.unwrap() });
        db.batch_execute("CREATE TABLE novu_rust_environments (id text PRIMARY KEY, organization_id text NOT NULL); INSERT INTO novu_rust_environments VALUES ('env-a','org-a'); CREATE TABLE novu_rust_workflows (environment_id text, identifier text, in_app_content text, has_steps boolean NOT NULL DEFAULT true, PRIMARY KEY (environment_id, identifier))").await.unwrap();
        // Workers 0100, 0102 and 0103 install triggers on API-owned webhook event tables.
        for sql in [
            include_str!("../../../api/rust-api/migrations/0004_sendgrid_webhooks.sql"),
            include_str!("../../../api/rust-api/migrations/0008_sendgrid_dispatch_identity.sql"),
            include_str!("../../../api/rust-api/migrations/0009_sendgrid_dispatch_proof.sql"),
            include_str!("../../../api/rust-api/migrations/0010_resend_webhooks.sql"),
            include_str!("../../../api/rust-api/migrations/0013_resend_event_recipient.sql"),
            include_str!("../../../api/rust-api/migrations/0047_resend_event_correlation.sql"),
            include_str!("../../../api/rust-api/migrations/0005_twilio_webhooks.sql"),
            include_str!("../../../api/rust-api/migrations/0006_twilio_webhook_job_link.sql"),
            include_str!("../../../api/rust-api/migrations/0007_twilio_dispatch_identity.sql"),
        ] {
            db.batch_execute(sql).await.unwrap();
        }
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
        let mut paths: Vec<_> = std::fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.extension().is_some_and(|v| v == "sql"))
            .collect();
        paths.sort();
        for path in paths {
            db.batch_execute(&std::fs::read_to_string(path).unwrap())
                .await
                .unwrap();
        }
        db.batch_execute("INSERT INTO novu_rust_workflows(environment_id,identifier,email_subject,email_html,email_provider,in_app_step_active,inbox_metadata) VALUES ('env-a','hello','Hi {{subscriber.firstName}}','<p>Hello {{subscriber.firstName}}</p>','resend',false,'{\"id\":\"workflow-id\"}'); INSERT INTO novu_rust_subscribers(environment_id,subscriber_id,internal_id,profile) VALUES ('env-a','recipient','old-id','{\"email\":\"to@example.test\",\"firstName\":\"Alice\"}')").await.unwrap();
        let trigger = Uuid::new_v4();
        let payload = json!({"workflow":"hello","transactionId":trigger.to_string(),"content":"","payload":{},"recipients":[{"subscriberId":"recipient"}],"recipientIdentitySnapshot":{"recipient":"old-id"},"severityOverride":null});
        enqueue(&db, trigger, "env-a", crate::in_app::JOB_KIND, &payload)
            .await
            .unwrap();
        assert!(crate::in_app::process_one(&mut db).await.unwrap());
        let row = db
            .query_one(
                "SELECT id FROM novu_queue_jobs WHERE kind=$1 AND payload->>'transaction_id'=$2",
                &[&JOB_KIND, &trigger.to_string()],
            )
            .await
            .unwrap();
        let email_job: Uuid = row.get(0);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/emails", listener.local_addr().unwrap());
        let mock = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            let mut chunk = [0; 4096];
            loop {
                let n = socket.read(&mut chunk).await.unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&chunk[..n]);
                if let Some(pos) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
                    let h = String::from_utf8_lossy(&bytes[..pos]);
                    let len: usize = h
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length: ")
                                .and_then(|v| v.parse().ok())
                        })
                        .unwrap();
                    if bytes.len() >= pos + 4 + len {
                        break;
                    }
                }
            }
            let req = String::from_utf8(bytes).unwrap();
            let body: serde_json::Value =
                serde_json::from_str(req.split_once("\r\n\r\n").unwrap().1).unwrap();
            assert_eq!(body["to"], json!(["to@example.test"]));
            assert_eq!(body["subject"], "Hi Alice");
            assert!(body["html"].as_str().unwrap().contains("Hello Alice"));
            let response = "{\"id\":\"resend-workflow-id\"}";
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}",response.len()).as_bytes()).await.unwrap();
        });
        let provider = Resend::test_endpoint(
            Config {
                api_key: "re_test".into(),
                from: "from@example.test".into(),
                sender_name: String::new(),
            },
            &endpoint,
        )
        .unwrap();
        let second_provider = Resend::test_endpoint(
            Config {
                api_key: "re_test".into(),
                from: "from@example.test".into(),
                sender_name: String::new(),
            },
            &endpoint,
        )
        .unwrap();
        let (mut second_db, second_driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
        tokio::spawn(async move { second_driver.await.unwrap() });
        let (first, second) = tokio::join!(
            process_one(&mut db, "env-a", &provider),
            process_one(&mut second_db, "env-a", &second_provider)
        );
        assert_eq!(
            usize::from(first.unwrap()) + usize::from(second.unwrap()),
            1
        );
        mock.await.unwrap();
        let row=db.query_one("SELECT j.status,a.state,m.delivery_state,m.identifier FROM novu_queue_jobs j JOIN novu_rust_resend_attempts a ON a.job_id=j.id JOIN novu_rust_email_messages m ON m.email_job_id=j.id WHERE j.id=$1", &[&email_job]).await.unwrap();
        assert_eq!(row.get::<_, String>(0), "completed");
        assert_eq!(row.get::<_, String>(1), "accepted");
        assert_eq!(row.get::<_, String>(2), "accepted");
        assert_eq!(row.get::<_, String>(3), "resend-workflow-id");
        // A stale worker must not overwrite the unknown result after another
        // worker recovers an expired lease during an outstanding HTTP call.
        let race = Uuid::new_v4();
        enqueue(
            &db,
            race,
            "env-race",
            JOB_KIND,
            &json!({"to":["to@example.test"],"subject":"Race","html":"Hi"}),
        )
        .await
        .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/emails", listener.local_addr().unwrap());
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (finish_tx, finish_rx) = tokio::sync::oneshot::channel();
        let mock = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut data = [0; 4096];
            assert!(socket.read(&mut data).await.unwrap() > 0);
            started_tx.send(()).unwrap();
            finish_rx.await.unwrap();
            let body = "{\"id\":\"late-provider-id\"}";
            socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
        });
        let provider_race = Resend::test_endpoint(
            Config {
                api_key: "re_test".into(),
                from: "from@example.test".into(),
                sender_name: String::new(),
            },
            &endpoint,
        )
        .unwrap();
        let (mut dispatch_db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
        tokio::spawn(async move { driver.await.unwrap() });
        let dispatch =
            tokio::spawn(
                async move { process_one(&mut dispatch_db, "env-race", &provider_race).await },
            );
        started_rx.await.unwrap();
        db.execute("UPDATE novu_queue_jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1", &[&race]).await.unwrap();
        assert_eq!(recover_expired(&mut db, "env-race").await.unwrap(), 1);
        finish_tx.send(()).unwrap();
        assert!(dispatch.await.unwrap().unwrap());
        mock.await.unwrap();
        let row=db.query_one("SELECT j.status,a.state,a.provider_message_id FROM novu_queue_jobs j JOIN novu_rust_resend_attempts a ON a.job_id=j.id WHERE j.id=$1", &[&race]).await.unwrap();
        assert_eq!(row.get::<_, String>(0), "failed");
        assert_eq!(row.get::<_, String>(1), "unknown");
        assert!(row.get::<_, Option<String>>(2).is_none());
        let trigger2 = Uuid::new_v4();
        let mut second = payload.clone();
        second["transactionId"] = json!(trigger2.to_string());
        enqueue(&db, trigger2, "env-a", crate::in_app::JOB_KIND, &second)
            .await
            .unwrap();
        assert!(crate::in_app::process_one(&mut db).await.unwrap());
        let job2: Uuid = db
            .query_one(
                "SELECT id FROM novu_queue_jobs WHERE kind=$1 AND payload->>'transaction_id'=$2",
                &[&JOB_KIND, &trigger2.to_string()],
            )
            .await
            .unwrap()
            .get(0);
        db.execute("UPDATE novu_rust_subscribers SET internal_id='new-id' WHERE environment_id='env-a' AND subscriber_id='recipient'",&[]).await.unwrap();
        assert!(process_one(&mut db, "env-a", &provider).await.unwrap());
        let row=db.query_one("SELECT status,last_error,(SELECT count(*) FROM novu_rust_resend_attempts WHERE job_id=$1) FROM novu_queue_jobs WHERE id=$1", &[&job2]).await.unwrap();
        assert_eq!(row.get::<_, String>(0), "failed");
        assert_eq!(row.get::<_, String>(1), "subscriber identity changed");
        assert_eq!(row.get::<_, i64>(2), 0);

        // Callback arriving after operator retry must be checked again before send.
        let guarded = Uuid::new_v4();
        enqueue(
            &db,
            guarded,
            "env-a",
            JOB_KIND,
            &json!({"to":["to@example.test"],"subject":"Guard","html":"Hi"}),
        )
        .await
        .unwrap();
        db.execute(
            "UPDATE novu_queue_jobs SET status='failed' WHERE id=$1",
            &[&guarded],
        )
        .await
        .unwrap();
        db.execute("INSERT INTO novu_rust_resend_attempts(job_id,environment_id,state,provider_message_id) VALUES ($1,'env-a','unknown','prior-provider-id')", &[&guarded]).await.unwrap();
        db.execute("INSERT INTO novu_rust_email_messages(internal_id,environment_id,subscriber_id,subscriber_internal_id,notification_internal_id,workflow_identifier,transaction_id,subject,status,email_job_id,delivery_state) VALUES ($1,'env-a','s','s','n','w','t','subject','error',$2,'unknown')", &[&guarded.to_string(),&guarded]).await.unwrap();
        db.execute("INSERT INTO novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'env-a',session_user,'retry',oid FROM pg_roles WHERE rolname=session_user", &[]).await.unwrap();
        crate::reconciliation::resolve(
            &mut db, "env-a", guarded, None, "retry", "operator", "ticket",
        )
        .await
        .unwrap();
        db.execute("INSERT INTO novu_rust_resend_delivery_events(environment_id,event_id,email_id,event_type) VALUES ('env-a','late-event','prior-provider-id','email.delivered')", &[]).await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let provider = Resend::test_endpoint(
            Config {
                api_key: "re_test".into(),
                from: "from@example.test".into(),
                sender_name: String::new(),
            },
            &format!("http://{}/emails", listener.local_addr().unwrap()),
        )
        .unwrap();
        assert!(
            process_one_with_retry_guard(&mut db, "env-a", &provider, true)
                .await
                .unwrap()
        );
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), listener.accept())
                .await
                .is_err()
        );
        let result=db.query_one("SELECT j.status,j.last_error,a.state,a.provider_message_id,m.delivery_state FROM novu_queue_jobs j JOIN novu_rust_resend_attempts a ON a.job_id=j.id JOIN novu_rust_email_messages m ON m.email_job_id=j.id WHERE j.id=$1", &[&guarded]).await.unwrap();
        assert_eq!(result.get::<_, String>(0), "failed");
        assert_eq!(
            result.get::<_, String>(1),
            "Resend retry blocked by provider callback"
        );
        assert_eq!(result.get::<_, String>(2), "unknown");
        assert_eq!(result.get::<_, String>(3), "prior-provider-id");
        assert_eq!(result.get::<_, String>(4), "unknown");

        // A callback after the retry decision can be correlated without any
        // provider response ID; the guard must prevent a new HTTP request.
        let tagged = Uuid::new_v4();
        let tag = Uuid::new_v4();
        enqueue(
            &db,
            tagged,
            "env-a",
            JOB_KIND,
            &json!({"to":["to@example.test"],"subject":"Tag guard","html":"Hi"}),
        )
        .await
        .unwrap();
        db.execute(
            "UPDATE novu_queue_jobs SET status='failed' WHERE id=$1",
            &[&tagged],
        )
        .await
        .unwrap();
        db.execute("INSERT INTO novu_rust_resend_attempts(job_id,environment_id,state,correlation_id) VALUES ($1,'env-a','unknown',$2)", &[&tagged,&tag]).await.unwrap();
        crate::reconciliation::resolve(
            &mut db, "env-a", tagged, None, "retry", "operator", "ticket",
        )
        .await
        .unwrap();
        db.execute("INSERT INTO novu_rust_resend_delivery_events(environment_id,event_id,email_id,event_type,correlation_id) VALUES ('env-a','tag-late','unknown-id','email.delivered',$1)", &[&tag]).await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let provider = Resend::test_endpoint(
            Config {
                api_key: "re_test".into(),
                from: "from@example.test".into(),
                sender_name: String::new(),
            },
            &format!("http://{}/emails", listener.local_addr().unwrap()),
        )
        .unwrap();
        assert!(
            process_one_with_correlation_guard(
                &mut db,
                "env-a",
                &provider,
                true,
                crate::email_step_policy::Policy::Snapshot,
                true,
                true
            )
            .await
            .unwrap()
        );
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), listener.accept())
                .await
                .is_err()
        );
        let row=db.query_one("SELECT j.status,j.last_error,a.state,a.correlation_id,a.provider_message_id FROM novu_queue_jobs j JOIN novu_rust_resend_attempts a ON a.job_id=j.id WHERE j.id=$1", &[&tagged]).await.unwrap();
        assert_eq!(row.get::<_, String>(0), "failed");
        assert_eq!(
            row.get::<_, String>(1),
            "Resend retry blocked by tagged callback"
        );
        assert_eq!(row.get::<_, String>(2), "unknown");
        assert_eq!(row.get::<_, Uuid>(3), tag);
        assert!(row.get::<_, Option<String>>(4).is_none());

        // A disabled guard permits the prior behavior; an unrelated event never blocks.
        for (enabled, prior_id, retry_key) in [
            (false, "prior-provider-id", false),
            (true, "unrelated-provider-id", true),
        ] {
            let unguarded = Uuid::new_v4();
            enqueue(
                &db,
                unguarded,
                "env-a",
                JOB_KIND,
                &json!({"to":["to@example.test"],"subject":"Unguarded","html":"Hi"}),
            )
            .await
            .unwrap();
            if retry_key {
                db.execute(
                    "UPDATE novu_queue_jobs SET status='failed' WHERE id=$1",
                    &[&unguarded],
                )
                .await
                .unwrap();
                db.execute("INSERT INTO novu_rust_resend_attempts(job_id,environment_id,state,provider_message_id) VALUES ($1,'env-a','unknown',$2)", &[&unguarded,&prior_id]).await.unwrap();
                crate::reconciliation::resolve(
                    &mut db,
                    "env-a",
                    unguarded,
                    None,
                    "retry",
                    "operator",
                    "verified not accepted",
                )
                .await
                .unwrap();
            } else {
                db.execute("INSERT INTO novu_rust_delivery_reconciliations(id,environment_id,job_id,decision,operator_name,reason,retry_provider_message_id) VALUES ($1,'env-a',$2,'retry','operator','test',$3)", &[&Uuid::new_v4(),&unguarded,&prior_id]).await.unwrap();
            }
            let decision: Uuid = db.query_one("SELECT id FROM novu_rust_delivery_reconciliations WHERE environment_id='env-a' AND job_id=$1 AND decision='retry'", &[&unguarded]).await.unwrap().get(0);
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let provider = Resend::test_endpoint(
                Config {
                    api_key: "re_test".into(),
                    from: "from@example.test".into(),
                    sender_name: String::new(),
                },
                &format!("http://{}/emails", listener.local_addr().unwrap()),
            )
            .unwrap();
            let mock = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                let mut buf = [0; 4096];
                loop {
                    let n = socket.read(&mut buf).await.unwrap();
                    assert!(n > 0);
                    bytes.extend_from_slice(&buf[..n]);
                    if let Some(pos) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
                        let headers = String::from_utf8_lossy(&bytes[..pos]);
                        let length = headers
                            .lines()
                            .find_map(|line| {
                                line.to_ascii_lowercase()
                                    .strip_prefix("content-length: ")
                                    .and_then(|v| v.parse::<usize>().ok())
                            })
                            .unwrap();
                        if bytes.len() >= pos + 4 + length {
                            break;
                        }
                    }
                }
                let headers = String::from_utf8_lossy(&bytes).to_ascii_lowercase();
                let expected = if retry_key {
                    format!("swallow/{unguarded}/retry/{decision}")
                } else {
                    format!("swallow/{unguarded}")
                };
                assert!(headers.contains(&format!("idempotency-key: {expected}\r\n")));
                let body = "{\"id\":\"mock-accepted\"}";
                socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            });
            assert!(
                process_one_with_retry_key(
                    &mut db,
                    "env-a",
                    &provider,
                    enabled,
                    crate::email_step_policy::Policy::Snapshot,
                    false,
                    false,
                    retry_key
                )
                .await
                .unwrap()
            );
            tokio::time::timeout(std::time::Duration::from_secs(2), mock)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                db.query_one(
                    "SELECT status FROM novu_queue_jobs WHERE id=$1",
                    &[&unguarded]
                )
                .await
                .unwrap()
                .get::<_, String>(0),
                "completed"
            );
        }

        // Provider rate-limits a real queue dispatch; an authorized retry then sends again.
        let job = Uuid::new_v4();
        enqueue(
            &db,
            job,
            "env-a",
            JOB_KIND,
            &json!({"to":["to@example.test"],"subject":"rate","html":"test"}),
        )
        .await
        .unwrap();
        for response in [429, 200] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let provider = Resend::test_endpoint(
                Config {
                    api_key: "re_test".into(),
                    from: "from@example.test".into(),
                    sender_name: String::new(),
                },
                &format!("http://{}/emails", listener.local_addr().unwrap()),
            )
            .unwrap();
            let mock = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                let mut buffer = [0; 4096];
                loop {
                    let n = socket.read(&mut buffer).await.unwrap();
                    assert!(n > 0);
                    bytes.extend_from_slice(&buffer[..n]);
                    if let Some(pos) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
                        let header = String::from_utf8_lossy(&bytes[..pos]).to_ascii_lowercase();
                        let len: usize = header
                            .lines()
                            .find_map(|v| {
                                v.strip_prefix("content-length: ")
                                    .and_then(|v| v.parse().ok())
                            })
                            .unwrap();
                        if bytes.len() >= pos + 4 + len {
                            break;
                        }
                    }
                }
                let body = if response == 200 {
                    "{\"id\":\"retry-success\"}"
                } else {
                    "{}"
                };
                socket.write_all(format!("HTTP/1.1 {response} Test\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
            });
            assert!(
                process_one_with_retry_key(
                    &mut db,
                    "env-a",
                    &provider,
                    true,
                    crate::email_step_policy::Policy::Snapshot,
                    true,
                    true,
                    true
                )
                .await
                .unwrap()
            );
            mock.await.unwrap();
            if response == 429 {
                let row = db
                    .query_one(
                        "SELECT state,http_status FROM novu_rust_resend_attempts WHERE job_id=$1",
                        &[&job],
                    )
                    .await
                    .unwrap();
                assert_eq!(row.get::<_, String>(0), "rejected");
                assert_eq!(row.get::<_, i32>(1), 429);
                crate::reconciliation::resolve(
                    &mut db,
                    "env-a",
                    job,
                    None,
                    "retry",
                    "operator",
                    "provider rate limit",
                )
                .await
                .unwrap();
            } else {
                let row = db.query_one("SELECT j.status,a.state,a.provider_message_id,r.previous_http_status,r.retry_correlation_id FROM novu_queue_jobs j JOIN novu_rust_resend_attempts a ON a.job_id=j.id JOIN novu_rust_delivery_reconciliations r ON r.job_id=j.id WHERE j.id=$1", &[&job]).await.unwrap();
                assert_eq!(row.get::<_, String>(0), "completed");
                assert_eq!(row.get::<_, String>(1), "accepted");
                assert_eq!(row.get::<_, String>(2), "retry-success");
                assert_eq!(row.get::<_, i32>(3), 429);
                assert!(row.get::<_, Option<Uuid>>(4).is_some());
            }
        }
    }

    #[tokio::test]
    #[ignore = "requires a disposable PostgreSQL database"]
    async fn standalone_dispatch_and_recovery() {
        let url = std::env::var("TEST_DATABASE_URL").unwrap();
        let (mut db, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
        tokio::spawn(async move { connection.await.unwrap() });
        db.batch_execute("DROP TABLE IF EXISTS novu_rust_resend_attempts,novu_rust_email_messages,novu_queue_jobs CASCADE")
            .await
            .unwrap();
        db.batch_execute(crate::MIGRATION).await.unwrap();
        db.batch_execute(MIGRATION).await.unwrap();
        db.batch_execute(MIGRATION).await.unwrap();
        db.batch_execute(RECIPIENT_MIGRATION).await.unwrap();
        db.batch_execute(RECIPIENT_MIGRATION).await.unwrap();
        db.batch_execute(include_str!("../migrations/0134_resend_correlation.sql"))
            .await
            .unwrap();
        db.batch_execute(crate::in_app::EMAIL_MESSAGES_MIGRATION)
            .await
            .unwrap();
        db.batch_execute(crate::in_app::EMAIL_PROVIDER_RESULT_MIGRATION)
            .await
            .unwrap();
        db.batch_execute("ALTER TABLE novu_rust_email_messages ADD COLUMN delivery_state text NOT NULL DEFAULT 'not_attempted'").await.unwrap();
        let id = Uuid::new_v4();
        let other = Uuid::new_v4();
        let payload = json!({"to":["to@example.test"],"cc":["cc@example.test"],"bcc":["bcc@example.test"],"subject":"Hello","html":"Hi"});
        enqueue(&db, id, "env-a", JOB_KIND, &payload).await.unwrap();
        enqueue(&db, other, "env-b", JOB_KIND, &payload)
            .await
            .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/emails", listener.local_addr().unwrap());
        let mock = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            let mut buf = [0; 4096];
            loop {
                let n = socket.read(&mut buf).await.unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&buf[..n]);
                if let Some(pos) = bytes.windows(4).position(|x| x == b"\r\n\r\n") {
                    let h = String::from_utf8_lossy(&bytes[..pos]);
                    let n: usize = h
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length: ")
                                .and_then(|v| v.parse().ok())
                        })
                        .unwrap();
                    if bytes.len() >= pos + 4 + n {
                        break;
                    }
                }
            }
            let req = String::from_utf8(bytes).unwrap().to_ascii_lowercase();
            assert!(req.contains(&format!("idempotency-key: swallow/{id}")));
            assert!(req.contains("authorization: bearer re_test"));
            let value: serde_json::Value =
                serde_json::from_str(req.split_once("\r\n\r\n").unwrap().1).unwrap();
            assert_eq!(value["tags"][0]["name"], "swallow_dispatch");
            let correlation = Uuid::parse_str(value["tags"][0]["value"].as_str().unwrap()).unwrap();
            let response = "{\"id\":\"resend-id\"}";
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}",response.len()).as_bytes()).await.unwrap();
            correlation
        });
        let provider = Resend::test_endpoint(
            Config {
                api_key: "re_test".into(),
                from: "from@example.test".into(),
                sender_name: String::new(),
            },
            &endpoint,
        )
        .unwrap();
        assert!(
            process_one_with_correlation(
                &mut db,
                "env-a",
                &provider,
                false,
                crate::email_step_policy::Policy::Snapshot,
                true
            )
            .await
            .unwrap()
        );
        let correlation = mock.await.unwrap();
        assert_eq!(
            db.query_one(
                "SELECT correlation_id FROM novu_rust_resend_attempts WHERE job_id=$1",
                &[&id]
            )
            .await
            .unwrap()
            .get::<_, Uuid>(0),
            correlation
        );
        let row=db.query_one("SELECT j.status,a.state,a.provider_message_id FROM novu_queue_jobs j JOIN novu_rust_resend_attempts a ON a.job_id=j.id WHERE j.id=$1", &[&id]).await.unwrap();
        assert_eq!(row.get::<_, String>(0), "completed");
        assert_eq!(row.get::<_, String>(1), "accepted");
        assert_eq!(row.get::<_, String>(2), "resend-id");
        let recipients=db.query("SELECT recipient_kind,recipient_index,recipient_hash FROM novu_rust_resend_recipients WHERE job_id=$1 ORDER BY recipient_kind,recipient_index", &[&id]).await.unwrap();
        assert_eq!(recipients.len(), 3);
        for (row, kind, address) in recipients
            .iter()
            .zip(["bcc", "cc", "to"])
            .zip(["bcc@example.test", "cc@example.test", "to@example.test"])
            .map(|((row, kind), address)| (row, kind, address))
        {
            assert_eq!(row.get::<_, String>(0), kind);
            assert_eq!(row.get::<_, i32>(1), 0);
            assert_eq!(row.get::<_, String>(2), recipient_hash(address));
        }
        assert_eq!(
            db.query_one("SELECT status FROM novu_queue_jobs WHERE id=$1", &[&other])
                .await
                .unwrap()
                .get::<_, String>(0),
            "pending"
        );
        let pre = Uuid::new_v4();
        let post = Uuid::new_v4();
        for job in [pre, post] {
            enqueue(&db, job, "env-a", JOB_KIND, &payload)
                .await
                .unwrap();
            db.execute("UPDATE novu_queue_jobs SET status='running',lease_token=$2,lease_until=clock_timestamp()-interval '1 second' WHERE id=$1", &[&job,&Uuid::new_v4()]).await.unwrap();
        }
        db.execute("INSERT INTO novu_rust_resend_attempts(job_id,environment_id,state) VALUES ($1,'env-a','dispatching')", &[&post]).await.unwrap();
        assert_eq!(recover_expired(&mut db, "env-b").await.unwrap(), 0);
        assert_eq!(recover_expired(&mut db, "env-a").await.unwrap(), 2);
        assert_eq!(
            db.query_one("SELECT status FROM novu_queue_jobs WHERE id=$1", &[&pre])
                .await
                .unwrap()
                .get::<_, String>(0),
            "pending"
        );
        let row=db.query_one("SELECT j.status,a.state FROM novu_queue_jobs j JOIN novu_rust_resend_attempts a ON a.job_id=j.id WHERE j.id=$1", &[&post]).await.unwrap();
        assert_eq!(row.get::<_, String>(0), "failed");
        assert_eq!(row.get::<_, String>(1), "unknown");

        // Each test job has an isolated environment so it cannot consume the
        // pre-dispatch job requeued above.
        for (status, body, expected) in [
            (500, "{}", "unknown"),
            (302, "{}", "unknown"),
            (408, "{}", "unknown"),
            (409, "{}", "unknown"),
            (429, "{}", "rejected"),
            (200, "{}", "unknown"),
        ] {
            let job = Uuid::new_v4();
            let environment = format!("env-{job}");
            enqueue(&db, job, &environment, JOB_KIND, &payload)
                .await
                .unwrap();
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("http://{}/emails", listener.local_addr().unwrap());
            let mock = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut buf = [0; 4096];
                assert!(socket.read(&mut buf).await.unwrap() > 0);
                socket.write_all(format!("HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
            });
            let provider = Resend::test_endpoint(
                Config {
                    api_key: "re_test".into(),
                    from: "from@example.test".into(),
                    sender_name: String::new(),
                },
                &endpoint,
            )
            .unwrap();
            assert!(process_one(&mut db, &environment, &provider).await.unwrap());
            mock.await.unwrap();
            let row = db.query_one("SELECT j.status,a.state,a.http_status FROM novu_queue_jobs j JOIN novu_rust_resend_attempts a ON a.job_id=j.id WHERE j.id=$1", &[&job]).await.unwrap();
            assert_eq!(row.get::<_, String>(0), "failed");
            assert_eq!(row.get::<_, String>(1), expected);
            assert_eq!(
                row.get::<_, Option<i32>>(2),
                if status == 200 { None } else { Some(status) }
            );
            assert!(!process_one(&mut db, &environment, &provider).await.unwrap());
        }
        let job = Uuid::new_v4();
        let environment = format!("env-{job}");
        let unsupported = json!({"to":["to@example.test"],"subject":"Hello","html":"Hi","unrecognized_option":true});
        enqueue(&db, job, &environment, JOB_KIND, &unsupported)
            .await
            .unwrap();
        let row = db
            .query_one(
                "SELECT count(*) FROM novu_rust_resend_attempts WHERE job_id=$1",
                &[&job],
            )
            .await
            .unwrap();
        assert_eq!(row.get::<_, i64>(0), 0);
        // No listener is running; if the unsupported job is sent, the result
        // would be unknown rather than the explicit pre-dispatch failure.
        assert!(process_one(&mut db, &environment, &provider).await.unwrap());
        let row = db.query_one("SELECT status,last_error,(SELECT count(*) FROM novu_rust_resend_attempts WHERE job_id=$1) FROM novu_queue_jobs WHERE id=$1", &[&job]).await.unwrap();
        assert_eq!(row.get::<_, String>(0), "failed");
        assert_eq!(row.get::<_, String>(1), "invalid Resend job");
        assert_eq!(row.get::<_, i64>(2), 0);
    }
}
