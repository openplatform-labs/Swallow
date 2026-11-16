//! Experimental Telegram queue and opt-in workflow delivery.
use crate::{
    ClaimedJob,
    telegram::{Message, SendError, Telegram, request_body},
};
use serde::Deserialize;
use serde_json::{Map, Value};
use tokio_postgres::{Client, Error, Transaction};
use uuid::Uuid;
pub const JOB_KIND: &str = "experimental_telegram";
pub const WORKFLOW_KIND: &str = "experimental_telegram_workflow";
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkflowMessage {
    destination: String,
    #[serde(rename = "workflowContext")]
    workflow_context: WorkflowContext,
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
    m: &WorkflowMessage,
    ctx: &WorkflowContext,
) -> Result<Result<Value, &'static str>, Error> {
    let Some(row) = tx.query_opt("SELECT profile,internal_id FROM public.novu_rust_subscribers WHERE environment_id=$1 AND subscriber_id=$2 FOR UPDATE", &[&job.environment_id,&ctx.subscriber_id]).await? else { return Ok(Err("subscriber not found")); };
    if row.get::<_, String>(1) != ctx.subscriber_internal_id {
        return Ok(Err("subscriber identity changed"));
    }
    let workflow = tx.query_opt("SELECT active,has_steps,chat_step_active,chat_content FROM public.novu_rust_workflows WHERE environment_id=$1 AND identifier=$2 AND ($3::text IS NULL OR inbox_metadata->>'id'=$3) FOR SHARE", &[&job.environment_id,&ctx.workflow_identifier,&ctx.workflow_internal_id]).await?;
    if !workflow.is_some_and(|w| {
        w.get::<_, bool>(0)
            && w.get::<_, bool>(1)
            && w.get::<_, bool>(2)
            && w.get::<_, Option<String>>(3).as_deref() == Some(ctx.content_template.as_str())
    }) {
        return Ok(Err("Telegram workflow step changed or disabled"));
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
    let endpoint = tx.query_opt("SELECT chat_id FROM public.novu_rust_telegram_workflow_endpoints WHERE environment_id=$1 AND internal_id=$2 AND revision=$3 AND subscriber_internal_id=$4 AND destination_id=$5 AND context_keys @> $6 AND context_keys <@ $6 AND cardinality(context_keys)=cardinality($6) FOR SHARE", &[&job.environment_id,&ctx.endpoint_id,&ctx.endpoint_revision,&ctx.subscriber_internal_id,&m.destination,&ctx.context_keys]).await?;
    let Some(endpoint) = endpoint else {
        return Ok(Err("Telegram endpoint changed or deleted"));
    };
    let chat_id: String = endpoint.get(0);
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
        return Ok(Err("unsupported Telegram template or input"));
    };
    Ok(request_body(&Message {
        destination: m.destination.clone(),
        chat_id,
        text: content,
        passthrough: Map::new(),
    })
    .map_err(|_| "invalid Telegram request"))
}

async fn claim(
    db: &Client,
    env: &str,
    destination: &str,
    workflow: bool,
) -> Result<Option<ClaimedJob>, Error> {
    let token = Uuid::new_v4();
    let row=db.query_opt("WITH c AS (SELECT id FROM public.novu_queue_jobs WHERE environment_id=$1 AND kind=ANY($2::text[]) AND status='pending' AND payload->>'destination'=$4 AND available_at<=now() ORDER BY available_at,created_at,id FOR UPDATE SKIP LOCKED LIMIT 1) UPDATE public.novu_queue_jobs j SET status='running',attempts=j.attempts+1,lease_token=$3,lease_until=clock_timestamp()+interval '60 seconds' FROM c WHERE j.id=c.id RETURNING j.id,j.environment_id,j.kind,j.payload,j.attempts",&[&env,&if workflow {vec![JOB_KIND, WORKFLOW_KIND]} else {vec![JOB_KIND]},&token,&destination]).await?;
    Ok(row.map(|r| ClaimedJob {
        id: r.get(0),
        environment_id: r.get(1),
        kind: r.get(2),
        payload: r.get(3),
        attempts: r.get(4),
        lease_token: token,
    }))
}
async fn claim_scoped(
    db: &Client,
    env: &str,
    destination: &str,
    workflow: bool,
) -> Result<Option<ClaimedJob>, Error> {
    let token = Uuid::new_v4();
    if workflow {
        let row = db.query_opt("SELECT id,kind,payload,attempts FROM public.novu_rust_telegram_workflow_claim($1,$2,$3)", &[&env,&destination,&token]).await?;
        return Ok(row.map(|r| ClaimedJob {
            id: r.get(0),
            environment_id: env.to_owned(),
            kind: r.get(1),
            payload: r.get(2),
            attempts: r.get(3),
            lease_token: token,
        }));
    }
    let row = db
        .query_opt(
            "SELECT id,payload,attempts FROM public.novu_rust_telegram_worker_claim($1,$2,$3)",
            &[&env, &destination, &token],
        )
        .await?;
    Ok(row.map(|r| ClaimedJob {
        id: r.get(0),
        environment_id: env.to_owned(),
        kind: JOB_KIND.to_owned(),
        payload: r.get(1),
        attempts: r.get(2),
        lease_token: token,
    }))
}
async fn live(tx: &Transaction<'_>, job: &ClaimedJob) -> Result<bool, Error> {
    let row=tx.query_opt("SELECT lease_until>clock_timestamp() FROM public.novu_queue_jobs WHERE id=$1 AND environment_id=$2 AND kind=$3 AND status='running' AND lease_token=$4 FOR UPDATE",&[&job.id,&job.environment_id,&job.kind,&job.lease_token]).await?;
    Ok(row.is_some_and(|r| r.get(0)))
}
pub async fn process_one(
    db: &mut Client,
    env: &str,
    destination: &str,
    provider: &Telegram,
) -> Result<bool, Error> {
    process_one_with_scope(db, env, destination, provider, false).await
}
pub async fn process_one_with_scope(
    db: &mut Client,
    env: &str,
    destination: &str,
    provider: &Telegram,
    scoped: bool,
) -> Result<bool, Error> {
    process_one_with_workflow(db, env, destination, provider, scoped, false).await
}
pub async fn process_one_with_workflow(
    db: &mut Client,
    env: &str,
    destination: &str,
    provider: &Telegram,
    scoped: bool,
    workflow: bool,
) -> Result<bool, Error> {
    let claimed = if scoped {
        claim_scoped(db, env, destination, workflow).await?
    } else {
        claim(db, env, destination, workflow).await?
    };
    let Some(job) = claimed else { return Ok(false) };
    let body = if job.kind == WORKFLOW_KIND && scoped {
        let message = serde_json::from_value::<WorkflowMessage>(job.payload.clone());
        let tx = db.transaction().await?;
        let result: Result<Value, &str> = match message {
            Ok(ref m) if m.destination == destination => {
                let snapshot: Option<Value> = tx
                    .query_one(
                        "SELECT public.novu_rust_telegram_workflow_snapshot($1,$2,$3,$4,$5)",
                        &[
                            &env,
                            &destination,
                            &job.id,
                            &job.lease_token,
                            &crate::allow_recreated_subscriber(),
                        ],
                    )
                    .await?
                    .get(0);
                let Some(snapshot) = snapshot else {
                    tx.rollback().await?;
                    return Ok(true);
                };
                if let Some(reason) = snapshot.get("error").and_then(Value::as_str) {
                    // Only fixed diagnostics from the database are forwarded below.
                    let reason: &'static str = match reason {
                        "subscriber not found" => "subscriber not found",
                        "subscriber identity changed" => "subscriber identity changed",
                        "invalid subscriber profile" => "invalid subscriber profile",
                        "chat preference disabled" => "chat preference disabled",
                        "Telegram workflow step changed or disabled" => {
                            "Telegram workflow step changed or disabled"
                        }
                        "Telegram endpoint changed or deleted" => {
                            "Telegram endpoint changed or deleted"
                        }
                        "actor identity changed or missing" => "actor identity changed or missing",
                        _ => "invalid Telegram workflow job",
                    };
                    Err(reason)
                } else {
                    let actor = snapshot.get("actor").filter(|v| !v.is_null());
                    match novu_rust_contracts::template::render_in_app_with_actor(
                        &m.workflow_context.content_template,
                        &m.workflow_context.payload,
                        &snapshot["profile"],
                        &snapshot["contexts"],
                        actor,
                    ) {
                        Ok(text) => request_body(&Message {
                            destination: m.destination.clone(),
                            chat_id: snapshot["chatId"].as_str().unwrap_or("").to_owned(),
                            text,
                            passthrough: Map::new(),
                        })
                        .map_err(|_| "invalid Telegram request"),
                        Err(_) => Err("unsupported Telegram template or input"),
                    }
                }
            }
            _ => Err("invalid Telegram workflow job"),
        };
        let reason = result.as_ref().err().copied();
        let prepared: bool = tx
            .query_one(
                "SELECT public.novu_rust_telegram_workflow_prepare($1,$2,$3,$4,$5,$6)",
                &[
                    &env,
                    &destination,
                    &job.id,
                    &job.lease_token,
                    &reason,
                    &crate::allow_recreated_subscriber(),
                ],
            )
            .await?
            .get(0);
        tx.commit().await?;
        if !prepared {
            return Ok(true);
        }
        match result {
            Ok(body) => body,
            Err(_) => return Ok(true),
        }
    } else if job.kind == WORKFLOW_KIND {
        let message = serde_json::from_value::<WorkflowMessage>(job.payload.clone());
        let tx = db.transaction().await?;
        let result = match message {
            Ok(m) if m.destination == destination => {
                workflow_body(&tx, &job, &m, &m.workflow_context).await?
            }
            _ => Err("invalid Telegram workflow job"),
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
                tx.execute("UPDATE public.novu_queue_jobs SET status=$2,last_error=$3,lease_token=NULL,lease_until=NULL WHERE id=$1", &[&job.id,&status,&reason]).await?;
                tx.commit().await?;
                return Ok(true);
            }
        };
        let inserted=tx.execute("INSERT INTO public.novu_rust_telegram_attempts(job_id,environment_id,state) VALUES($1,$2,'dispatching') ON CONFLICT DO NOTHING",&[&job.id,&env]).await?;
        if inserted != 1 {
            tx.execute("UPDATE public.novu_queue_jobs SET status='failed',last_error='Telegram attempt requires review',lease_token=NULL,lease_until=NULL WHERE id=$1",&[&job.id]).await?;
            tx.commit().await?;
            return Ok(true);
        }
        if !live(&tx, &job).await? {
            tx.rollback().await?;
            return Ok(true);
        }
        tx.commit().await?;
        body
    } else {
        let body = match serde_json::from_value::<Message>(job.payload.clone())
            .ok()
            .and_then(|m| request_body(&m).ok())
        {
            Some(v) => v,
            None => {
                if scoped {
                    db.query_one(
                        "SELECT public.novu_rust_telegram_worker_prepare($1,$2,$3,$4,false)",
                        &[&env, &destination, &job.id, &job.lease_token],
                    )
                    .await?;
                } else {
                    crate::fail(db, job.id, job.lease_token, "invalid Telegram job").await?;
                }
                return Ok(true);
            }
        };
        if scoped {
            let prepared: bool = db
                .query_one(
                    "SELECT public.novu_rust_telegram_worker_prepare($1,$2,$3,$4,true)",
                    &[&env, &destination, &job.id, &job.lease_token],
                )
                .await?
                .get(0);
            if !prepared {
                return Ok(true);
            }
        } else {
            let tx = db.transaction().await?;
            if !live(&tx, &job).await? {
                tx.rollback().await?;
                return Ok(true);
            }
            let inserted=tx.execute("INSERT INTO public.novu_rust_telegram_attempts(job_id,environment_id,state) VALUES($1,$2,'dispatching') ON CONFLICT DO NOTHING",&[&job.id,&env]).await?;
            if inserted != 1 {
                tx.execute("UPDATE public.novu_queue_jobs SET status='failed',last_error='Telegram attempt requires review',lease_token=NULL,lease_until=NULL WHERE id=$1",&[&job.id]).await?;
                tx.commit().await?;
                return Ok(true);
            }
            if !live(&tx, &job).await? {
                tx.rollback().await?;
                return Ok(true);
            }
            tx.commit().await?;
        }
        body
    };
    let (state, code, id, timestamp, diagnostic) = match provider.send(&body).await {
        Ok(v) => ("accepted", None, Some(v.id), Some(v.timestamp), None),
        Err(SendError::Rejected(code)) => (
            "rejected",
            Some(i32::from(code)),
            None,
            None,
            Some("Telegram rejected request"),
        ),
        Err(SendError::Invalid) => (
            "unknown",
            None,
            None,
            None,
            Some("Telegram outcome unknown"),
        ),
        Err(SendError::Unknown) => (
            "unknown",
            None,
            None,
            None,
            Some("Telegram outcome unknown"),
        ),
    };
    if scoped {
        db.query_one(
            "SELECT public.novu_rust_telegram_worker_finish($1,$2,$3,$4,$5,$6,$7,$8)",
            &[
                &env,
                &destination,
                &job.id,
                &job.lease_token,
                &state,
                &code,
                &id,
                &timestamp,
            ],
        )
        .await?;
        return Ok(true);
    }
    let tx = db.transaction().await?;
    if !live(&tx, &job).await? {
        tx.rollback().await?;
        return Ok(true);
    }
    let updated=tx.execute("UPDATE public.novu_rust_telegram_attempts SET state=$3,http_status=$4,provider_message_id=$5,provider_timestamp=$6,updated_at=clock_timestamp() WHERE job_id=$1 AND environment_id=$2 AND state='dispatching'",&[&job.id,&env,&state,&code,&id,&timestamp]).await?;
    if updated == 1 {
        let job_status = if state == "accepted" {
            "completed"
        } else {
            "failed"
        };
        tx.execute("UPDATE public.novu_queue_jobs SET status=$2,last_error=$3,lease_token=NULL,lease_until=NULL WHERE id=$1",&[&job.id,&job_status,&diagnostic]).await?;
    }
    tx.commit().await?;
    Ok(true)
}
pub async fn recover_expired(
    db: &mut Client,
    env: &str,
    destination: &str,
) -> Result<usize, Error> {
    recover_expired_with_scope(db, env, destination, false).await
}
pub async fn recover_expired_with_scope(
    db: &mut Client,
    env: &str,
    destination: &str,
    scoped: bool,
) -> Result<usize, Error> {
    recover_expired_with_workflow(db, env, destination, scoped, false).await
}
pub async fn recover_expired_with_workflow(
    db: &mut Client,
    env: &str,
    destination: &str,
    scoped: bool,
    workflow: bool,
) -> Result<usize, Error> {
    if scoped {
        let n: i32 = db
            .query_one(
                if workflow {
                    "SELECT public.novu_rust_telegram_workflow_recover($1,$2)"
                } else {
                    "SELECT public.novu_rust_telegram_worker_recover($1,$2)"
                },
                &[&env, &destination],
            )
            .await?
            .get(0);
        return Ok(n as usize);
    }
    let tx = db.transaction().await?;
    let rows=tx.query("SELECT id FROM public.novu_queue_jobs WHERE environment_id=$1 AND kind=ANY($2::text[]) AND payload->>'destination'=$3 AND status='running' AND lease_until<=clock_timestamp() ORDER BY lease_until,id FOR UPDATE SKIP LOCKED LIMIT 100",&[&env,&if workflow {vec![JOB_KIND, WORKFLOW_KIND]} else {vec![JOB_KIND]},&destination]).await?;
    for row in &rows {
        let id: Uuid = row.get(0);
        let attempted=tx.query_opt("SELECT 1 FROM public.novu_rust_telegram_attempts WHERE job_id=$1 AND environment_id=$2",&[&id,&env]).await?.is_some();
        if attempted {
            tx.execute("UPDATE public.novu_rust_telegram_attempts SET state='unknown',updated_at=clock_timestamp() WHERE job_id=$1 AND environment_id=$2 AND state='dispatching'",&[&id,&env]).await?;
            tx.execute("UPDATE public.novu_queue_jobs SET status='failed',last_error='Telegram outcome unknown',lease_token=NULL,lease_until=NULL WHERE id=$1",&[&id]).await?;
        } else {
            tx.execute("UPDATE public.novu_queue_jobs SET status='pending',lease_token=NULL,lease_until=NULL WHERE id=$1",&[&id]).await?;
        }
    }
    tx.commit().await?;
    Ok(rows.len())
}
