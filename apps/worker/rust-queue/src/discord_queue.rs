//! Standalone Discord jobs; this is not the Novu workflow integration.
use crate::{
    ClaimedJob,
    discord::{Webhook, request_body},
    slack::SendError,
};
use serde::Deserialize;
use serde_json::{Map, Value};
use tokio_postgres::{Client, Error, Transaction};
use uuid::Uuid;

pub const JOB_KIND: &str = "experimental_discord_webhook";
pub const WORKFLOW_KIND: &str = "experimental_discord_workflow";
pub const MIGRATION: &str = include_str!("../migrations/0202_discord_webhook_queue.sql");
pub const RETRY_POLICY_MIGRATION: &str =
    include_str!("../migrations/0208_discord_retry_policy.sql");
pub const SHARED_COOLDOWN_MIGRATION: &str =
    include_str!("../migrations/0230_discord_shared_cooldown.sql");

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Message {
    destination: String,
    content: String,
    #[serde(default, rename = "workflowContext")]
    workflow_context: Option<WorkflowContext>,
    #[serde(default, rename = "customData")]
    custom_data: Map<String, Value>,
    #[serde(default, rename = "providerData")]
    provider_data: Map<String, Value>,
    #[serde(default)]
    passthrough: Map<String, Value>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkflowContext {
    subscriber_id: String,
    subscriber_internal_id: String,
    workflow_identifier: String,
    workflow_internal_id: Option<String>,
    content_template: String,
    payload: Value,
    context_keys: Vec<String>,
    actor: Option<Value>,
    actor_internal_id: Option<String>,
    endpoint_id: Uuid,
    endpoint_revision: Uuid,
}

async fn workflow_body(
    tx: &Transaction<'_>,
    job: &ClaimedJob,
    m: &Message,
    ctx: &WorkflowContext,
) -> Result<Result<Value, &'static str>, Error> {
    let Some(row) = tx.query_opt("SELECT profile,internal_id FROM public.novu_rust_subscribers WHERE environment_id=$1 AND subscriber_id=$2 FOR UPDATE", &[&job.environment_id,&ctx.subscriber_id]).await? else { return Ok(Err("subscriber not found")); };
    if row.get::<_, String>(1) != ctx.subscriber_internal_id {
        return Ok(Err("subscriber identity changed"));
    }
    let pref = tx.query_one("SELECT w.read_only,w.all_enabled,w.chat_enabled,(p.channels->>'chat')::boolean,(sp.channels->>'chat')::boolean FROM (SELECT 1) singleton LEFT JOIN public.novu_rust_workflow_preferences w ON w.environment_id=$1 AND w.workflow_identifier=$3 LEFT JOIN public.novu_rust_subscriber_preferences p ON p.environment_id=$1 AND p.subscriber_id=$2 LEFT JOIN public.novu_rust_subscriber_workflow_preferences sp ON sp.environment_id=$1 AND sp.subscriber_id=$2 AND sp.workflow_identifier=$3", &[&job.environment_id,&ctx.subscriber_id,&ctx.workflow_identifier]).await?;
    if !novu_rust_contracts::preference::in_app_enabled(
        pref.get(0),
        pref.get(1),
        pref.get(2),
        pref.get(3),
        pref.get(4),
    ) {
        return Ok(Err("chat preference disabled"));
    }
    let workflow = tx.query_opt("SELECT active,has_steps,chat_step_active,chat_content FROM public.novu_rust_workflows WHERE environment_id=$1 AND identifier=$2 AND ($3::text IS NULL OR inbox_metadata->>'id'=$3) FOR SHARE", &[&job.environment_id,&ctx.workflow_identifier,&ctx.workflow_internal_id]).await?;
    if !workflow.is_some_and(|w| {
        w.get::<_, bool>(0)
            && w.get::<_, bool>(1)
            && w.get::<_, bool>(2)
            && w.get::<_, Option<String>>(3).as_deref() == Some(ctx.content_template.as_str())
    }) {
        return Ok(Err("Discord workflow step changed or disabled"));
    }
    let endpoint = tx.query_opt("SELECT 1 FROM public.novu_rust_discord_workflow_endpoints WHERE environment_id=$1 AND internal_id=$2 AND revision=$3 AND subscriber_internal_id=$4 AND destination_id=$5 AND context_keys @> $6 AND context_keys <@ $6 AND cardinality(context_keys)=cardinality($6) FOR SHARE", &[&job.environment_id,&ctx.endpoint_id,&ctx.endpoint_revision,&ctx.subscriber_internal_id,&m.destination,&ctx.context_keys]).await?;
    if endpoint.is_none() {
        return Ok(Err("Discord endpoint changed or deleted"));
    }
    let mut profile: Value = row.get(0);
    let Some(fields) = profile.as_object_mut() else {
        return Ok(Err("invalid subscriber profile"));
    };
    fields.insert("subscriberId".into(), ctx.subscriber_id.clone().into());
    let actor = match crate::resolve_actor(
        tx,
        &job.environment_id,
        ctx.actor.as_ref(),
        ctx.actor_internal_id.as_deref(),
        &ctx.subscriber_id,
        &profile,
    )
    .await?
    {
        Ok(actor) => actor,
        Err(()) => return Ok(Err("actor identity changed or missing")),
    };
    let mut contexts = Map::new();
    if !ctx.context_keys.is_empty() {
        for row in tx.query("SELECT context_key,data FROM public.novu_rust_contexts WHERE environment_id=$1 AND context_key=ANY($2)",&[&job.environment_id,&ctx.context_keys]).await? {
            let key:String=row.get(0); let data:Value=row.get(1);
            if let Some((kind,id))=key.split_once(':') { contexts.insert(kind.to_owned(),serde_json::json!({"id":id,"data":data})); }
        }
    }
    let Ok(content) = novu_rust_contracts::template::render_in_app_with_actor(
        &ctx.content_template,
        &ctx.payload,
        &profile,
        &Value::Object(contexts),
        actor.as_ref(),
    ) else {
        return Ok(Err("unsupported Discord template or input"));
    };
    Ok(
        request_body(&content, &m.custom_data, &m.provider_data, &m.passthrough)
            .map_err(|_| "invalid Discord request"),
    )
}

async fn claim(
    db: &Client,
    environment: &str,
    destination: &str,
) -> Result<Option<ClaimedJob>, Error> {
    let token = Uuid::new_v4();
    let row = db.query_opt("WITH candidate AS (SELECT id FROM novu_queue_jobs WHERE environment_id=$1 AND kind=ANY($2::text[]) AND status='pending' AND payload->>'destination'=$4 AND available_at<=now() ORDER BY available_at,created_at,id FOR UPDATE SKIP LOCKED LIMIT 1) UPDATE novu_queue_jobs j SET status='running',attempts=j.attempts+1,lease_token=$3,lease_until=clock_timestamp()+interval '60 seconds' FROM candidate WHERE j.id=candidate.id RETURNING j.id,j.environment_id,j.kind,j.payload,j.attempts", &[&environment,&vec![JOB_KIND,WORKFLOW_KIND],&token,&destination]).await?;
    Ok(row.map(|r| ClaimedJob {
        id: r.get(0),
        environment_id: r.get(1),
        kind: r.get(2),
        payload: r.get(3),
        attempts: r.get(4),
        lease_token: token,
    }))
}
async fn live(tx: &Transaction<'_>, job: &ClaimedJob) -> Result<bool, Error> {
    let row = tx.query_opt("SELECT 1 FROM novu_queue_jobs WHERE id=$1 AND environment_id=$2 AND kind=$3 AND status='running' AND lease_token=$4 FOR UPDATE", &[&job.id,&job.environment_id,&job.kind,&job.lease_token]).await?;
    if row.is_none() {
        return Ok(false);
    }
    Ok(tx
        .query_one(
            "SELECT lease_until>clock_timestamp() FROM novu_queue_jobs WHERE id=$1",
            &[&job.id],
        )
        .await?
        .get(0))
}

pub async fn process_one(
    db: &mut Client,
    environment: &str,
    destination: &str,
    provider: &Webhook,
) -> Result<bool, Error> {
    let Some(job) = claim(db, environment, destination).await? else {
        return Ok(false);
    };
    let message = serde_json::from_value::<Message>(job.payload.clone());
    let Ok(message) = message else {
        crate::fail(db, job.id, job.lease_token, "invalid Discord job").await?;
        return Ok(true);
    };
    if message.destination != destination
        || (job.kind == WORKFLOW_KIND) != message.workflow_context.is_some()
    {
        crate::fail(db, job.id, job.lease_token, "invalid Discord job").await?;
        return Ok(true);
    }
    let tx = db.transaction().await?;
    let result = if let Some(ctx) = message.workflow_context.as_ref() {
        workflow_body(&tx, &job, &message, ctx).await?
    } else {
        request_body(
            &message.content,
            &message.custom_data,
            &message.provider_data,
            &message.passthrough,
        )
        .map_err(|_| "invalid Discord request")
    };
    if !live(&tx, &job).await? {
        tx.rollback().await?;
        return Ok(true);
    }
    let body = match result {
        Ok(body) => body,
        Err(reason) => {
            let status = if reason == "chat preference disabled" {
                "cancelled"
            } else {
                "failed"
            };
            tx.execute("UPDATE novu_queue_jobs SET status=$2,last_error=$3,lease_token=NULL,lease_until=NULL WHERE id=$1", &[&job.id,&status,&reason]).await?;
            tx.commit().await?;
            return Ok(true);
        }
    };
    // Serialize policy changes with reservation and fence old worker versions.
    tx.query_opt("SELECT mode FROM public.novu_rust_discord_retry_policy WHERE environment_id=$1 AND destination_id=$2 FOR SHARE", &[&environment,&destination]).await?;
    let cooldown = tx.query_opt("SELECT not_before>clock_timestamp() FROM public.novu_rust_discord_shared_cooldown WHERE environment_id=$1 AND destination_id=$2 FOR UPDATE", &[&environment,&destination]).await?;
    if cooldown.is_some_and(|row| row.get::<_, bool>(0)) {
        tx.execute("UPDATE public.novu_queue_jobs SET status='pending',attempts=attempts-1,available_at=GREATEST(available_at,(SELECT not_before FROM public.novu_rust_discord_shared_cooldown WHERE environment_id=$2 AND destination_id=$3)),lease_token=NULL,lease_until=NULL WHERE id=$1", &[&job.id,&environment,&destination]).await?;
        tx.commit().await?;
        return Ok(true);
    }
    let marker = format!("{}:{}", job.id, job.lease_token);
    tx.query_one(
        "SELECT set_config('swallow.discord_retry_fence',$1,true)",
        &[&marker],
    )
    .await?;
    tx.query_one("SELECT set_config('swallow.discord_cooldown_fence',$1,true)", &[&marker]).await?;
    let inserted = tx.execute("INSERT INTO novu_rust_discord_attempts(job_id,environment_id,state) VALUES ($1,$2,'dispatching') ON CONFLICT DO NOTHING", &[&job.id,&environment]).await?;
    if inserted != 1 {
        tx.execute("UPDATE novu_queue_jobs SET status='failed',last_error='Discord attempt requires review',lease_token=NULL,lease_until=NULL WHERE id=$1", &[&job.id]).await?;
        tx.commit().await?;
        return Ok(true);
    }
    tx.commit().await?;
    let (state, code, id, timestamp, diagnostic, retry_after) = match provider.send(&body).await {
        Ok(result) => (
            "accepted",
            None,
            Some(result.id),
            Some(result.timestamp),
            None,
            None,
        ),
        Err(SendError::Rejected(code)) => (
            "rejected",
            code.map(i32::from),
            None,
            None,
            Some("Discord rejected request"),
            None,
        ),
        Err(SendError::RateLimited(seconds)) => (
            "rejected",
            Some(429),
            None,
            None,
            Some("Discord rate limited request"),
            seconds.map(|s| s as i32),
        ),
        Err(SendError::Invalid) => (
            "rejected",
            None,
            None,
            None,
            Some("invalid Discord request"),
            None,
        ),
        Err(SendError::Unknown) => (
            "unknown",
            None,
            None,
            None,
            Some("Discord outcome unknown"),
            None,
        ),
    };
    let tx = db.transaction().await?;
    if !live(&tx, &job).await? {
        tx.rollback().await?;
        return Ok(true);
    }
    let updated = tx.execute("UPDATE novu_rust_discord_attempts SET state=$3,http_status=$4,provider_message_id=$5,provider_timestamp=$6,retry_after_seconds=$7,updated_at=clock_timestamp() WHERE job_id=$1 AND environment_id=$2 AND state='dispatching'", &[&job.id,&environment,&state,&code,&id,&timestamp,&retry_after]).await?;
    if updated == 1 {
        let retry_policy = tx.query_opt("SELECT mode,max_retries,fallback_seconds FROM public.novu_rust_discord_retry_policy WHERE environment_id=$1 AND destination_id=$2 FOR SHARE", &[&environment,&destination]).await?;
        if let (Some(429), Some(seconds)) = (code, retry_after) {
            let delay = seconds.max(1) as f64;
            tx.execute("UPDATE public.novu_rust_discord_shared_cooldown SET not_before=GREATEST(not_before,clock_timestamp()+make_interval(secs=>$3::double precision)) WHERE environment_id=$1 AND destination_id=$2", &[&environment,&destination,&delay]).await?;
        }
        let mut retry = None;
        if code == Some(429) {
            if let Some(policy) = retry_policy {
                let mode: String = policy.get(0);
                let max: i32 = policy.get(1);
                let fallback: i32 = policy.get(2);
                let count: i64 = tx.query_one("SELECT count(*) FROM public.novu_rust_discord_retry_history WHERE job_id=$1", &[&job.id]).await?.get(0);
                if count < i64::from(max) {
                    retry = match (mode.as_str(), retry_after) {
                        ("known", Some(seconds)) | ("fallback", Some(seconds)) => {
                            Some(seconds.max(1))
                        }
                        ("fallback", None) => Some(fallback),
                        _ => None,
                    };
                }
            }
        }
        if let Some(delay) = retry {
            tx.execute("INSERT INTO public.novu_rust_discord_retry_history(job_id,environment_id,destination_id,retry_after_seconds,scheduled_delay_seconds) VALUES($1,$2,$3,$4,$5)",&[&job.id,&environment,&destination,&retry_after,&delay]).await?;
            tx.execute("DELETE FROM public.novu_rust_discord_attempts WHERE job_id=$1 AND environment_id=$2 AND state='rejected' AND http_status=429",&[&job.id,&environment]).await?;
            tx.execute("UPDATE public.novu_queue_jobs SET status='pending',available_at=clock_timestamp()+make_interval(secs=>$2::double precision),last_error='Discord rate limited; retry scheduled',lease_token=NULL,lease_until=NULL WHERE id=$1", &[&job.id,&(delay as f64)]).await?;
        } else {
            let status = if state == "accepted" {
                "completed"
            } else {
                "failed"
            };
            tx.execute("UPDATE novu_queue_jobs SET status=$2,last_error=$3,lease_token=NULL,lease_until=NULL WHERE id=$1", &[&job.id,&status,&diagnostic]).await?;
        }
    }
    tx.commit().await?;
    Ok(true)
}

pub async fn recover_expired(
    db: &mut Client,
    environment: &str,
    destination: &str,
) -> Result<usize, Error> {
    let tx = db.transaction().await?;
    let rows = tx.query("SELECT id FROM novu_queue_jobs WHERE environment_id=$1 AND kind=ANY($2::text[]) AND payload->>'destination'=$3 AND status='running' AND lease_until<=clock_timestamp() ORDER BY lease_until,id FOR UPDATE SKIP LOCKED LIMIT 100", &[&environment,&vec![JOB_KIND,WORKFLOW_KIND],&destination]).await?;
    for row in &rows {
        let id: Uuid = row.get(0);
        let attempted = tx
            .query_opt(
                "SELECT 1 FROM novu_rust_discord_attempts WHERE job_id=$1 AND environment_id=$2",
                &[&id, &environment],
            )
            .await?
            .is_some();
        if attempted {
            tx.execute("UPDATE novu_rust_discord_attempts SET state='unknown',updated_at=clock_timestamp() WHERE job_id=$1 AND environment_id=$2 AND state='dispatching'", &[&id,&environment]).await?;
            tx.execute("UPDATE novu_queue_jobs SET status='failed',last_error='Discord outcome unknown',lease_token=NULL,lease_until=NULL WHERE id=$1", &[&id]).await?;
        } else {
            tx.execute("UPDATE novu_queue_jobs SET status='pending',lease_token=NULL,lease_until=NULL WHERE id=$1", &[&id]).await?;
        }
    }
    tx.commit().await?;
    Ok(rows.len())
}
