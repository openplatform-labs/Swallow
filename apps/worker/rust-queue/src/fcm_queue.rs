//! Experimental environment-scoped FCM queue, with durable per-target attempts.
use crate::{
    ClaimedJob,
    fcm::{self, Fcm, Push, SendError},
};
use std::collections::HashMap;
use std::time::Duration;
use tokio_postgres::{Client, Error, Transaction};
use uuid::Uuid;

pub const JOB_KIND: &str = "experimental_fcm_push";
pub const MIGRATION: &str = include_str!("../migrations/0045_experimental_fcm_attempts.sql");

async fn claim(client: &Client, environment: &str) -> Result<Option<ClaimedJob>, Error> {
    let token = Uuid::new_v4();
    let lease = Duration::from_secs(60).as_secs_f64();
    let row=client.query_opt("WITH candidate AS (SELECT id FROM novu_queue_jobs WHERE environment_id=$1 AND kind=$2 AND status='pending' AND available_at<=now() ORDER BY available_at,created_at,id FOR UPDATE SKIP LOCKED LIMIT 1) UPDATE novu_queue_jobs j SET status='running',attempts=j.attempts+1,lease_token=$3,lease_until=clock_timestamp()+$4*interval '1 second' FROM candidate WHERE j.id=candidate.id RETURNING j.id,j.environment_id,j.kind,j.payload,j.attempts",&[&environment,&JOB_KIND,&token,&lease]).await?;
    Ok(row.map(|r| ClaimedJob {
        id: r.get(0),
        environment_id: r.get(1),
        kind: r.get(2),
        payload: r.get(3),
        attempts: r.get(4),
        lease_token: token,
    }))
}
async fn lock_live(tx: &Transaction<'_>, job: &ClaimedJob) -> Result<bool, Error> {
    let row=tx.query_opt("SELECT id FROM novu_queue_jobs WHERE id=$1 AND environment_id=$2 AND kind=$3 AND status='running' AND lease_token=$4 FOR UPDATE",&[&job.id,&job.environment_id,&JOB_KIND,&job.lease_token]).await?;
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

/// Complete only if at least one target was accepted and none is uncertain.
async fn finish(tx: &Transaction<'_>, job: &ClaimedJob) -> Result<(), Error> {
    let row=tx.query_one("SELECT count(*) FILTER(WHERE state='accepted'),count(*) FILTER(WHERE state IN ('unknown','dispatching')) FROM novu_rust_fcm_attempts WHERE job_id=$1 AND environment_id=$2",&[&job.id,&job.environment_id]).await?;
    let accepted: i64 = row.get(0);
    let unknown: i64 = row.get(1);
    let (status, diagnostic) = if unknown > 0 {
        ("failed", Some("push delivery outcome unknown"))
    } else if accepted > 0 {
        ("completed", None)
    } else {
        ("failed", Some("all push targets rejected"))
    };
    tx.execute("UPDATE novu_queue_jobs SET status=$2,last_error=$3,lease_token=NULL,lease_until=NULL WHERE id=$1 AND lease_token=$4",&[&job.id,&status,&diagnostic,&job.lease_token]).await?;
    Ok(())
}

/// Process at most one target. Other targets are requeued without repeating attempts.
pub async fn process_one(
    client: &mut Client,
    environment: &str,
    provider: &Fcm,
) -> Result<bool, Error> {
    process_one_inner(client, environment, Some(provider), None).await
}

/// Process workflow targets using an explicit integration-to-provider map.
/// Missing credentials fail the job before any target is sent.
pub async fn process_one_with_integrations(
    client: &mut Client,
    environment: &str,
    providers: &HashMap<String, Fcm>,
) -> Result<bool, Error> {
    process_one_inner(client, environment, None, Some(providers)).await
}

async fn process_one_inner(
    client: &mut Client,
    environment: &str,
    default_provider: Option<&Fcm>,
    providers: Option<&HashMap<String, Fcm>>,
) -> Result<bool, Error> {
    let Some(job) = claim(client, environment).await? else {
        return Ok(false);
    };
    let mut push: Push = match serde_json::from_value(job.payload.clone()) {
        Ok(push) => push,
        Err(_) => {
            crate::fail(client, job.id, job.lease_token, "invalid push job").await?;
            return Ok(true);
        }
    };
    let mut prepared_context = push.prepared_context.take();
    let tx = client.transaction().await?;
    // Match subscriber mutation lock ordering before locking the job.
    let subscriber = if let Some(context) = push.workflow_context.as_ref() {
        tx.query_opt("SELECT s.profile,w.read_only,w.all_enabled,w.push_enabled,(p.channels->>'push')::boolean,(sp.channels->>'push')::boolean,EXISTS(SELECT 1 FROM novu_rust_workflows wf WHERE wf.environment_id=s.environment_id AND wf.identifier=$3 AND ($4::text IS NULL OR wf.inbox_metadata->>'id'=$4)),d.device_tokens,d.integration_internal_id IS NULL OR EXISTS(SELECT 1 FROM novu_rust_fcm_integrations i WHERE i.environment_id=s.environment_id AND i.internal_id=d.integration_internal_id AND i.active),d.integration_internal_id,EXISTS(SELECT 1 FROM novu_rust_fcm_devices x WHERE x.environment_id=s.environment_id AND x.subscriber_id=s.subscriber_id OFFSET 1),s.internal_id FROM novu_rust_subscribers s LEFT JOIN novu_rust_workflow_preferences w ON w.environment_id=s.environment_id AND w.workflow_identifier=$3 LEFT JOIN novu_rust_subscriber_preferences p ON p.environment_id=s.environment_id AND p.subscriber_id=s.subscriber_id LEFT JOIN novu_rust_subscriber_workflow_preferences sp ON sp.environment_id=s.environment_id AND sp.subscriber_id=s.subscriber_id AND sp.workflow_identifier=$3 LEFT JOIN LATERAL (SELECT * FROM novu_rust_fcm_devices d WHERE d.environment_id=s.environment_id AND d.subscriber_id=s.subscriber_id ORDER BY d.channel_order LIMIT 1) d ON true WHERE s.environment_id=$1 AND s.subscriber_id=$2 FOR UPDATE OF s",&[&environment,&context.subscriber_id,&context.workflow_identifier,&context.workflow_internal_id]).await?
    } else {
        None
    };
    let prepared_identity_valid = if !crate::allow_recreated_subscriber()
        && push.workflow_context.is_none()
    {
        if let Some(context) = prepared_context.as_ref() {
            let id = context
                .get("subscriber_id")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("");
            let expected = context
                .get("subscriber_internal_id")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("");
            tx.query_opt("SELECT internal_id FROM novu_rust_subscribers WHERE environment_id=$1 AND subscriber_id=$2 FOR UPDATE", &[&environment,&id]).await?.is_some_and(|row| row.get::<_, String>(0)==expected)
        } else {
            true
        }
    } else {
        true
    };
    if !lock_live(&tx, &job).await? {
        tx.rollback().await?;
        return Ok(true);
    }
    if !prepared_identity_valid {
        tx.execute("UPDATE novu_rust_push_messages SET delivery_state='unknown' WHERE environment_id=$1 AND push_job_id=$2 AND delivery_state IN ('dispatching','in_progress')", &[&environment,&job.id]).await?;
        tx.execute("UPDATE novu_rust_push_step_outcomes SET status='failed',push_job_id=NULL WHERE environment_id=$1 AND push_job_id=$2", &[&environment,&job.id]).await?;
        tx.execute("UPDATE novu_queue_jobs SET status='failed',last_error='subscriber identity changed',lease_token=NULL,lease_until=NULL WHERE id=$1", &[&job.id]).await?;
        tx.commit().await?;
        return Ok(true);
    }
    let messages = if let Some(context) = push.workflow_context.take() {
        let route = fcm::routing(&context.provider_data);
        let skip = match subscriber.as_ref() {
            None => Some(("failed", "failed", "subscriber not found")),
            Some(row)
                if !crate::allow_recreated_subscriber()
                    && context.subscriber_internal_id.as_deref()
                        != Some(row.get::<_, String>(11).as_str()) =>
            {
                Some(("failed", "failed", "subscriber identity changed"))
            }
            Some(row) if !row.get::<_, bool>(6) => Some(("failed", "failed", "workflow not found")),
            Some(row)
                if !novu_rust_contracts::preference::in_app_enabled(
                    row.get(1),
                    row.get(2),
                    row.get(3),
                    row.get(4),
                    row.get(5),
                ) =>
            {
                Some(("canceled", "cancelled", "push preference disabled"))
            }
            Some(row) if route.is_none() && row.get::<_, bool>(10) && providers.is_none() => {
                Some((
                    "failed",
                    "failed",
                    "multiple push integrations not configured for delivery",
                ))
            }
            Some(row)
                if route.is_none()
                    && !row.get::<_, bool>(10)
                    && row
                        .get::<_, Option<Vec<String>>>(7)
                        .is_none_or(|tokens| tokens.is_empty()) =>
            {
                Some((
                    "missing_device",
                    "cancelled",
                    "subscriber missing push device",
                ))
            }
            Some(row) if route.is_none() && !row.get::<_, bool>(10) && !row.get::<_, bool>(8) => {
                Some((
                    "failed",
                    "failed",
                    "subscriber has no active push integration",
                ))
            }
            _ => None,
        };
        if let Some((outcome, status, diagnostic)) = skip {
            tx.execute("UPDATE novu_rust_push_step_outcomes SET status=$3,push_job_id=NULL WHERE environment_id=$1 AND push_job_id=$2",&[&environment,&job.id,&outcome]).await?;
            tx.execute("UPDATE novu_queue_jobs SET status=$2,last_error=$3,lease_token=NULL,lease_until=NULL WHERE id=$1",&[&job.id,&status,&diagnostic]).await?;
            tx.commit().await?;
            return Ok(true);
        }
        let row = subscriber.as_ref().unwrap();
        let groups: Vec<(Option<String>, Vec<String>, bool)> = if let Some((key, _)) =
            route.as_ref()
        {
            // The reference keeps one send per populated subscriber channel for
            // token routes. Broadcasts and token-less subscribers instead use
            // the newest active provider selected by Novu's default policy.
            let mut channels = if *key == "token" || *key == "tokens" {
                tx.query("SELECT d.integration_internal_id FROM novu_rust_fcm_devices d JOIN novu_rust_fcm_integrations i ON i.environment_id=d.environment_id AND i.internal_id=d.integration_internal_id AND i.active WHERE d.environment_id=$1 AND d.subscriber_id=$2 AND cardinality(d.device_tokens)>0 ORDER BY d.channel_order", &[&environment,&context.subscriber_id]).await?.into_iter().map(|row| (Some(row.get::<_, String>(0)),vec![String::new()],true)).collect::<Vec<_>>()
            } else {
                Vec::new()
            };
            if channels.is_empty() {
                let active = tx.query_opt("SELECT internal_id,identifier FROM novu_rust_fcm_integrations WHERE environment_id=$1 AND active ORDER BY created_at DESC,internal_id DESC LIMIT 1", &[&environment]).await?;
                if let Some(active) = active.filter(|row| {
                    row.get::<_, Option<String>>(1)
                        .is_some_and(|value| !value.is_empty())
                }) {
                    channels.push((Some(active.get(0)), vec![String::new()], true));
                }
            }
            if channels.is_empty() || (channels.len() > 1 && providers.is_none()) {
                tx.execute("UPDATE novu_rust_push_step_outcomes SET status='failed',push_job_id=NULL WHERE environment_id=$1 AND push_job_id=$2",&[&environment,&job.id]).await?;
                tx.execute("UPDATE novu_queue_jobs SET status='failed',last_error='FCM routing integration unavailable',lease_token=NULL,lease_until=NULL WHERE id=$1",&[&job.id]).await?;
                tx.commit().await?;
                return Ok(true);
            }
            channels
        } else if row.get::<_, bool>(10) {
            tx.query("SELECT d.integration_internal_id,d.device_tokens,d.integration_internal_id IS NULL OR EXISTS(SELECT 1 FROM novu_rust_fcm_integrations i WHERE i.environment_id=d.environment_id AND i.internal_id=d.integration_internal_id AND i.active) FROM novu_rust_fcm_devices d WHERE d.environment_id=$1 AND d.subscriber_id=$2 ORDER BY d.channel_order", &[&environment,&context.subscriber_id]).await?
                .into_iter().map(|r| (r.get(0),r.get(1),r.get(2))).collect()
        } else {
            vec![(
                row.get(9),
                row.get::<_, Option<Vec<String>>>(7).unwrap_or_default(),
                row.get(8),
            )]
        };
        let eligible: Vec<_> = groups
            .iter()
            .filter(|(_, tokens, active)| *active && !tokens.is_empty())
            .collect();
        let unavailable = providers.is_some_and(|providers| {
            eligible
                .iter()
                .any(|(id, _, _)| id.as_ref().is_none_or(|id| !providers.contains_key(id)))
        });
        if eligible.is_empty() || unavailable {
            let missing =
                eligible.is_empty() && groups.iter().any(|(_, tokens, _)| tokens.is_empty());
            let (outcome, status, diagnostic) = if missing {
                (
                    "missing_device",
                    "cancelled",
                    "subscriber missing push device",
                )
            } else {
                (
                    "failed",
                    "failed",
                    if unavailable {
                        "push integration credentials not configured"
                    } else {
                        "subscriber has no active push integration"
                    },
                )
            };
            tx.execute("UPDATE novu_rust_push_step_outcomes SET status=$3,push_job_id=NULL WHERE environment_id=$1 AND push_job_id=$2",&[&environment,&job.id,&outcome]).await?;
            tx.execute("UPDATE novu_queue_jobs SET status=$2,last_error=$3,lease_token=NULL,lease_until=NULL WHERE id=$1",&[&job.id,&status,&diagnostic]).await?;
            tx.commit().await?;
            return Ok(true);
        }
        let mut profile: serde_json::Value = row.get(0);
        if let Some(fields) = profile.as_object_mut() {
            fields.insert("subscriberId".into(), context.subscriber_id.clone().into());
        }
        let actor = match crate::resolve_actor(
            &tx,
            environment,
            context.actor.as_ref(),
            context.actor_internal_id.as_deref(),
            &context.subscriber_id,
            &profile,
        )
        .await?
        {
            Ok(actor) => actor,
            Err(()) => {
                tx.execute("UPDATE novu_rust_push_step_outcomes SET status='failed',push_job_id=NULL WHERE environment_id=$1 AND push_job_id=$2", &[&environment,&job.id]).await?;
                tx.execute("UPDATE novu_queue_jobs SET status='failed',last_error='actor identity changed or missing',lease_token=NULL,lease_until=NULL WHERE id=$1", &[&job.id]).await?;
                tx.commit().await?;
                return Ok(true);
            }
        };
        let mut resolved_context = serde_json::Map::new();
        if !context.context_keys.is_empty() {
            for row in tx.query("SELECT context_key,data FROM novu_rust_contexts WHERE environment_id=$1 AND context_key=ANY($2)",&[&environment,&context.context_keys]).await? {
                let key:String=row.get(0);let data:serde_json::Value=row.get(1);
                if let Some((kind,id))=key.split_once(':'){resolved_context.insert(kind.to_owned(),serde_json::json!({"id":id,"data":data}));}
            }
        }
        let resolved = serde_json::Value::Object(resolved_context);
        let render = |template: &str| {
            novu_rust_contracts::template::render_in_app_with_actor(
                template,
                &context.payload,
                &profile,
                &resolved,
                actor.as_ref(),
            )
        };
        let rendered = render(&context.title_template)
            .and_then(|title| render(&context.content_template).map(|content| (title, content)));
        let (title, content) = match rendered {
            Ok(value) => value,
            Err(_) => {
                tx.execute("UPDATE novu_rust_push_step_outcomes SET status='failed',push_job_id=NULL WHERE environment_id=$1 AND push_job_id=$2",&[&environment,&job.id]).await?;
                tx.execute("UPDATE novu_queue_jobs SET status='failed',last_error='unsupported push template or input',lease_token=NULL,lease_until=NULL WHERE id=$1",&[&job.id]).await?;
                tx.commit().await?;
                return Ok(true);
            }
        };
        let mut messages = Vec::new();
        let mut target_metadata = Vec::new();
        for (integration, tokens, _) in eligible {
            let override_tokens: Option<Vec<String>> = context
                .overrides
                .get("deviceTokens")
                .and_then(|v| v.as_array())
                .map(|items| {
                    items
                        .iter()
                        .filter_map(|v| v.as_str().map(str::to_owned))
                        .collect()
                });
            let targets = if route.is_some() {
                tokens
            } else {
                override_tokens.as_ref().unwrap_or(tokens)
            };
            for token in targets {
                let message_id: String = tx
                    .query_one("SELECT novu_rust_new_objectid()", &[])
                    .await?
                    .get(0);
                let mut payload = context.payload.as_object().cloned().unwrap_or_default();
                payload.insert("__nvMessageId".into(), message_id.clone().into());
                let candidate = Push {
                    title: title.clone(),
                    content: content.clone(),
                    targets: vec![token.clone()],
                    payload,
                    overrides: context.overrides.clone(),
                    provider_data: context.provider_data.clone(),
                    ..Push::default()
                };
                match fcm::wire_messages(&fcm::plan(&candidate)) {
                    Ok(mut bodies) => {
                        let history_index = messages.len();
                        let group_size = bodies.len();
                        let recorded_tokens = match route.as_ref() {
                            Some(("tokens", value)) => value.clone(),
                            Some(_) => serde_json::json!([]),
                            None => serde_json::json!(targets),
                        };
                        for _ in 0..group_size {
                            target_metadata.push(serde_json::json!({"device_tokens":recorded_tokens,"integration_internal_id":integration,"message_id":message_id,"routed":route.is_some(),"history_index":history_index,"group_size":group_size}));
                        }
                        messages.append(&mut bodies);
                    }
                    Err(_) => {
                        tx.execute("UPDATE novu_rust_push_step_outcomes SET status='failed',push_job_id=NULL WHERE environment_id=$1 AND push_job_id=$2",&[&environment,&job.id]).await?;
                        tx.execute("UPDATE novu_queue_jobs SET status='failed',last_error='unsupported push message',lease_token=NULL,lease_until=NULL WHERE id=$1",&[&job.id]).await?;
                        tx.commit().await?;
                        return Ok(true);
                    }
                }
            }
        }
        if let (
            Some(subscriber_internal_id),
            Some(notification_internal_id),
            Some(transaction_id),
        ) = (
            &context.subscriber_internal_id,
            &context.notification_internal_id,
            &context.transaction_id,
        ) {
            prepared_context = Some(
                serde_json::json!({"subscriber_id":context.subscriber_id,"subscriber_internal_id":subscriber_internal_id,"notification_internal_id":notification_internal_id,"transaction_id":transaction_id,"workflow_identifier":context.workflow_identifier,"target_metadata":target_metadata,"title":title,"content":content}),
            );
        }
        let updated =
            serde_json::json!({"prepared_messages":messages,"prepared_context":prepared_context});
        tx.execute(
            "UPDATE novu_queue_jobs SET payload=$2 WHERE id=$1 AND lease_token=$3",
            &[&job.id, &updated, &job.lease_token],
        )
        .await?;
        messages
    } else if let Some(prepared) = push.prepared_messages.take() {
        // Validate persisted bodies again before using them, including after recovery.
        let mut checked = Vec::new();
        for body in prepared {
            let Some(message) = body.get("message") else {
                tx.rollback().await?;
                crate::fail(client, job.id, job.lease_token, "invalid prepared push").await?;
                return Ok(true);
            };
            let plan = fcm::Plan {
                multicast: false,
                message: message.clone(),
            };
            match fcm::wire_messages(&plan) {
                Ok(mut value) if value.len() == 1 => checked.append(&mut value),
                _ => {
                    tx.rollback().await?;
                    crate::fail(client, job.id, job.lease_token, "invalid prepared push").await?;
                    return Ok(true);
                }
            }
        }
        checked
    } else {
        match fcm::wire_messages(&fcm::plan(&push)) {
            Ok(messages) => messages,
            Err(_) => {
                tx.rollback().await?;
                crate::fail(client, job.id, job.lease_token, "unsupported push message").await?;
                return Ok(true);
            }
        }
    };
    let rows = tx
        .query(
            "SELECT target_index FROM novu_rust_fcm_attempts WHERE job_id=$1 AND environment_id=$2",
            &[&job.id, &environment],
        )
        .await?;
    let attempted: std::collections::HashSet<i32> = rows.iter().map(|r| r.get(0)).collect();
    let next = (0..messages.len()).find(|i| !attempted.contains(&(*i as i32)));
    let Some(index) = next else {
        finish(&tx, &job).await?;
        tx.commit().await?;
        return Ok(true);
    };
    let index_i32 = index as i32;
    let target_metadata = prepared_context.as_ref().map(|context| {
        context
            .get("target_metadata")
            .and_then(serde_json::Value::as_array)
            .and_then(|targets| targets.get(index))
            .unwrap_or(context)
    });
    let integration_id = target_metadata
        .and_then(|meta| meta.get("integration_internal_id"))
        .and_then(serde_json::Value::as_str);
    let history_index = target_metadata
        .and_then(|meta| meta.get("history_index"))
        .and_then(serde_json::Value::as_i64)
        .and_then(|value| i32::try_from(value).ok())
        .unwrap_or(index_i32);
    let group_size = target_metadata
        .and_then(|meta| meta.get("group_size"))
        .and_then(serde_json::Value::as_i64)
        .and_then(|value| i32::try_from(value).ok())
        .filter(|value| *value > 0)
        .unwrap_or(1);
    let selected_provider = if let Some(providers) = providers {
        integration_id.and_then(|id| providers.get(id))
    } else {
        default_provider
    };
    let Some(selected_provider) = selected_provider else {
        tx.rollback().await?;
        crate::fail(
            client,
            job.id,
            job.lease_token,
            "push integration credentials not configured",
        )
        .await?;
        return Ok(true);
    };
    if let Some(context) = prepared_context.as_ref() {
        let string = |key: &str| context.get(key).and_then(serde_json::Value::as_str);
        let tokens = target_metadata
            .unwrap_or(context)
            .get("device_tokens")
            .and_then(serde_json::Value::as_array)
            .and_then(|a| {
                a.iter()
                    .map(|v| v.as_str().map(str::to_owned))
                    .collect::<Option<Vec<_>>>()
            });
        let message_id = target_metadata
            .and_then(|v| v.get("message_id"))
            .and_then(serde_json::Value::as_str)
            .or_else(|| {
                messages[index]
                    .get("message")
                    .and_then(|v| v.get("data"))
                    .and_then(|v| v.get("__nvMessageId"))
                    .and_then(serde_json::Value::as_str)
            });
        if let (
            Some(message_id),
            Some(subscriber_id),
            Some(subscriber_internal_id),
            Some(notification_internal_id),
            Some(workflow),
            Some(transaction),
            Some(tokens),
            Some(title),
            Some(content),
        ) = (
            message_id,
            string("subscriber_id"),
            string("subscriber_internal_id"),
            string("notification_internal_id"),
            string("workflow_identifier"),
            string("transaction_id"),
            tokens,
            string("title"),
            string("content"),
        ) {
            let stored_content = (std::env::var("STORE_NOTIFICATION_CONTENT").as_deref()
                == Ok("true"))
            .then_some(content);
            tx.execute("INSERT INTO novu_rust_push_messages(internal_id,environment_id,subscriber_id,subscriber_internal_id,notification_internal_id,workflow_identifier,transaction_id,device_tokens,title,content,status,push_job_id,target_index) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'sent',$11,$12) ON CONFLICT(push_job_id,target_index) DO NOTHING",&[&message_id,&environment,&subscriber_id,&subscriber_internal_id,&notification_internal_id,&workflow,&transaction,&tokens,&title,&stored_content,&job.id,&history_index]).await?;
        } else {
            tx.rollback().await?;
            crate::fail(
                client,
                job.id,
                job.lease_token,
                "invalid prepared push context",
            )
            .await?;
            return Ok(true);
        }
    }
    tx.execute("INSERT INTO novu_rust_fcm_attempts(job_id,environment_id,target_index,state) VALUES ($1,$2,$3,'dispatching')",&[&job.id,&environment,&index_i32]).await?;
    if prepared_context.is_some() {
        tx.execute("UPDATE novu_rust_push_messages SET delivery_state=CASE WHEN delivery_state='unknown' THEN 'unknown' ELSE 'dispatching' END WHERE environment_id=$1 AND push_job_id=$2 AND target_index=$3", &[&environment,&job.id,&history_index]).await?;
    }
    tx.commit().await?;
    let result = selected_provider.send_wire(&messages[index]).await;
    let invalid_token = matches!(result, Err(SendError::InvalidDeviceToken(_)));
    let (state, status, id) = match result {
        Ok(id) => (
            "accepted",
            None,
            Some(id.chars().take(1024).collect::<String>()),
        ),
        Err(SendError::Provider(code) | SendError::InvalidDeviceToken(code)) if code >= 500 => {
            ("unknown", Some(i32::from(code)), None)
        }
        Err(SendError::Provider(code) | SendError::InvalidDeviceToken(code)) => {
            ("rejected", Some(i32::from(code)), None)
        }
        Err(SendError::Transport | SendError::InvalidResponse) => ("unknown", None, None),
        Err(_) => ("rejected", None, None),
    };
    let tx = client.transaction().await?;
    if !lock_live(&tx, &job).await? {
        tx.rollback().await?;
        return Ok(true);
    }
    let updated=tx.execute("UPDATE novu_rust_fcm_attempts SET state=$4,http_status=$5,provider_message_id=$6,updated_at=clock_timestamp() WHERE job_id=$1 AND environment_id=$2 AND target_index=$3 AND state='dispatching'",&[&job.id,&environment,&index_i32,&state,&status,&id]).await?;
    if updated == 1 {
        if invalid_token
            && std::env::var("IS_EXPIRED_TOKENS_REMOVAL_ENABLED").as_deref() == Ok("true")
            && !target_metadata
                .and_then(|v| v.get("routed"))
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false)
            && let (Some(context), Some(token)) = (
                prepared_context.as_ref(),
                messages[index]
                    .pointer("/message/token")
                    .and_then(serde_json::Value::as_str)
                    .filter(|v| !v.is_empty()),
            )
            && let (Some(subscriber_id), Some(internal_id), Some(integration_id)) = (
                context
                    .get("subscriber_id")
                    .and_then(serde_json::Value::as_str),
                context
                    .get("subscriber_internal_id")
                    .and_then(serde_json::Value::as_str),
                target_metadata
                    .unwrap_or(context)
                    .get("integration_internal_id")
                    .and_then(serde_json::Value::as_str),
            )
        {
            tx.execute("WITH target AS MATERIALIZED (SELECT environment_id,subscriber_id FROM novu_rust_subscribers WHERE environment_id=$1 AND subscriber_id=$2 AND internal_id=$3 FOR UPDATE), removed AS (UPDATE novu_rust_fcm_devices d SET device_tokens=array_remove(d.device_tokens,$5) FROM target WHERE d.environment_id=target.environment_id AND d.subscriber_id=target.subscriber_id AND d.integration_internal_id=$4 RETURNING 1) UPDATE novu_rust_subscribers s SET updated_at=now() FROM target WHERE s.environment_id=target.environment_id AND s.subscriber_id=target.subscriber_id AND EXISTS(SELECT 1 FROM removed)",&[&environment,&subscriber_id,&internal_id,&integration_id,&token]).await?;
        }
        if prepared_context.is_some() {
            let end = history_index.saturating_add(group_size);
            let counts = tx.query_one("SELECT count(*), count(*) FILTER (WHERE state='accepted'), count(*) FILTER (WHERE state IN ('unknown','dispatching')) FROM novu_rust_fcm_attempts WHERE environment_id=$1 AND job_id=$2 AND target_index >= $3 AND target_index < $4", &[&environment,&job.id,&history_index,&end]).await?;
            let count: i64 = counts.get(0);
            if count == i64::from(group_size) {
                let accepted: i64 = counts.get(1);
                let unknown: i64 = counts.get(2);
                let message_status = if accepted > 0 && unknown == 0 {
                    "sent"
                } else {
                    "error"
                };
                let diagnostic = if unknown > 0 {
                    Some("push delivery outcome unknown")
                } else if accepted == 0 {
                    Some("push provider rejected request")
                } else {
                    None
                };
                let error_id = (message_status == "error").then_some("unexpected_push_error");
                let delivery_state = if unknown > 0 {
                    "unknown"
                } else if accepted > 0 {
                    "accepted"
                } else {
                    "rejected"
                };
                tx.execute("UPDATE novu_rust_push_messages SET status=$4,error_id=$5,error_text=$6,identifier=internal_id,delivery_state=$7 WHERE environment_id=$1 AND push_job_id=$2 AND target_index=$3",&[&environment,&job.id,&history_index,&message_status,&error_id,&diagnostic,&delivery_state]).await?;
            } else {
                let unknown: i64 = counts.get(2);
                let delivery_state = if unknown > 0 {
                    "unknown"
                } else {
                    "in_progress"
                };
                tx.execute("UPDATE novu_rust_push_messages SET delivery_state=$4 WHERE environment_id=$1 AND push_job_id=$2 AND target_index=$3", &[&environment,&job.id,&history_index,&delivery_state]).await?;
            }
        }
        if attempted.len() + 1 < messages.len() {
            tx.execute("UPDATE novu_queue_jobs SET status='pending',available_at=clock_timestamp(),lease_token=NULL,lease_until=NULL WHERE id=$1 AND lease_token=$2",&[&job.id,&job.lease_token]).await?;
        } else {
            finish(&tx, &job).await?;
        }
    }
    tx.commit().await?;
    Ok(true)
}

/// Expired attempts are unknown; remaining targets may still run once.
pub async fn recover_expired(client: &mut Client, environment: &str) -> Result<usize, Error> {
    let tx = client.transaction().await?;
    let rows=tx.query("SELECT id FROM novu_queue_jobs WHERE environment_id=$1 AND kind=$2 AND status='running' AND lease_until<=clock_timestamp() ORDER BY lease_until,id FOR UPDATE SKIP LOCKED LIMIT 100",&[&environment,&JOB_KIND]).await?;
    for row in &rows {
        let id: Uuid = row.get(0);
        tx.execute("UPDATE novu_rust_push_messages m SET delivery_state='unknown' FROM novu_rust_fcm_attempts a JOIN novu_queue_jobs j ON j.id=a.job_id WHERE m.environment_id=$1 AND m.push_job_id=$2 AND a.environment_id=m.environment_id AND a.job_id=m.push_job_id AND a.state='dispatching' AND m.target_index=COALESCE((j.payload->'prepared_context'->'target_metadata'->a.target_index->>'history_index')::integer,a.target_index)",&[&environment,&id]).await?;
        tx.execute("UPDATE novu_rust_fcm_attempts SET state='unknown',updated_at=clock_timestamp() WHERE job_id=$1 AND environment_id=$2 AND state='dispatching'",&[&id,&environment]).await?;
        tx.execute("UPDATE novu_queue_jobs SET status='pending',lease_token=NULL,lease_until=NULL WHERE id=$1",&[&id]).await?;
    }
    tx.commit().await?;
    Ok(rows.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };
    use tokio_postgres::NoTls;

    #[tokio::test]
    #[ignore = "requires differential harness PostgreSQL and loopback access"]
    async fn capture_reference_workflow_push() {
        let count: usize = std::env::var("TEST_PUSH_EXPECTED_COUNT")
            .unwrap()
            .parse()
            .unwrap();
        let (mut db, connection) =
            tokio_postgres::connect(&std::env::var("TEST_DATABASE_URL").unwrap(), NoTls)
                .await
                .unwrap();
        tokio::spawn(async move { connection.await.unwrap() });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let mut captures = Vec::new();
            let mut authorizations = Vec::new();
            if count == 0 {
                assert!(
                    tokio::time::timeout(Duration::from_secs(1), listener.accept())
                        .await
                        .is_err(),
                    "unexpected push request"
                );
            }
            for _ in 0..count {
                let (mut socket, _) =
                    tokio::time::timeout(Duration::from_secs(5), listener.accept())
                        .await
                        .unwrap()
                        .unwrap();
                let mut buf = Vec::new();
                let mut tmp = [0; 4096];
                loop {
                    let n = socket.read(&mut tmp).await.unwrap();
                    assert!(n > 0);
                    buf.extend_from_slice(&tmp[..n]);
                    if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                        let headers = String::from_utf8_lossy(&buf[..pos]);
                        let len = headers
                            .lines()
                            .find_map(|l| {
                                l.to_ascii_lowercase()
                                    .strip_prefix("content-length: ")
                                    .and_then(|v| v.parse::<usize>().ok())
                            })
                            .unwrap();
                        if buf.len() >= pos + 4 + len {
                            authorizations.push(
                                headers
                                    .lines()
                                    .find_map(|line| {
                                        line.split_once(':')
                                            .filter(|(name, _)| {
                                                name.eq_ignore_ascii_case("authorization")
                                            })
                                            .map(|(_, value)| value.trim().to_owned())
                                    })
                                    .unwrap(),
                            );
                            captures.push(
                                serde_json::from_slice::<serde_json::Value>(
                                    &buf[pos + 4..pos + 4 + len],
                                )
                                .unwrap(),
                            );
                            break;
                        }
                    }
                }
                let status = if let Ok(invalid_target) = std::env::var("TEST_PUSH_INVALID_TARGET") {
                    if captures
                        .last()
                        .and_then(|v| v.pointer("/message/token"))
                        .and_then(serde_json::Value::as_str)
                        == Some(invalid_target.as_str())
                    {
                        "404".into()
                    } else {
                        "200".into()
                    }
                } else {
                    std::env::var("TEST_PUSH_HTTP_STATUS").unwrap_or_else(|_| "200".into())
                };
                assert!(status == "200" || status == "429" || status == "404");
                let body = std::env::var("TEST_PUSH_ERROR_BODY").unwrap_or_else(|_| {
                    if status == "404" {
                        r#"{"error":{"message":"Requested entity was not found."}}"#.into()
                    } else {
                        r#"{"name":"projects/demo/messages/123"}"#.into()
                    }
                });
                if let Ok(directory) = std::env::var("TEST_PUSH_BARRIER_DIR") {
                    let directory = std::path::Path::new(&directory);
                    std::fs::write(directory.join("ready"), b"ready").unwrap();
                    tokio::time::timeout(Duration::from_secs(12), async {
                        while !directory.join("release").exists() {
                            tokio::time::sleep(Duration::from_millis(20)).await;
                        }
                    })
                    .await
                    .expect("push test barrier timed out");
                }
                socket.write_all(format!("HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
            }
            std::fs::write(
                std::env::var("TEST_PUSH_CAPTURE_PATH").unwrap(),
                serde_json::to_vec(&captures).unwrap(),
            )
            .unwrap();
            if let Ok(path) = std::env::var("TEST_PUSH_AUTH_CAPTURE_PATH") {
                std::fs::write(path, serde_json::to_vec(&authorizations).unwrap()).unwrap();
            }
        });
        let provider = Fcm::with_origin("demo", "fixture".into(), &origin).unwrap();
        let integrations: Option<HashMap<String, Fcm>> =
            std::env::var("TEST_PUSH_INTEGRATIONS").ok().map(|raw| {
                let tokens: HashMap<String, String> = serde_json::from_str(&raw).unwrap();
                tokens
                    .into_iter()
                    .map(|(id, token)| (id, Fcm::with_origin("demo", token, &origin).unwrap()))
                    .collect()
            });
        for _ in 0..count.max(1) {
            let processed = if let Some(integrations) = integrations.as_ref() {
                process_one_with_integrations(&mut db, "64a000000000000000000002", integrations)
                    .await
            } else {
                process_one(&mut db, "64a000000000000000000002", &provider).await
            };
            assert!(processed.unwrap());
        }
        server.await.unwrap();
    }

    async fn mock(statuses: Vec<u16>) -> (Fcm, tokio::task::JoinHandle<Vec<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let mut targets = Vec::new();
            for status in statuses {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut buf = Vec::new();
                let mut tmp = [0; 4096];
                loop {
                    let n = socket.read(&mut tmp).await.unwrap();
                    assert!(n > 0);
                    buf.extend_from_slice(&tmp[..n]);
                    if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                        let headers = String::from_utf8_lossy(&buf[..pos]);
                        let len = headers
                            .lines()
                            .find_map(|l| {
                                l.to_ascii_lowercase()
                                    .strip_prefix("content-length: ")
                                    .and_then(|v| v.parse::<usize>().ok())
                            })
                            .unwrap();
                        if buf.len() >= pos + 4 + len {
                            break;
                        }
                    }
                }
                let raw = String::from_utf8(buf).unwrap();
                assert!(raw.contains("authorization: Bearer fixture\r\n"));
                let body: serde_json::Value =
                    serde_json::from_str(raw.split_once("\r\n\r\n").unwrap().1).unwrap();
                targets.push(body["message"]["token"].as_str().unwrap().to_owned());
                if targets.len() == 1 {
                    tokio::time::sleep(Duration::from_millis(150)).await;
                }
                let response = r#"{"name":"projects/demo/messages/123"}"#;
                socket.write_all(format!("HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}",response.len()).as_bytes()).await.unwrap();
            }
            targets
        });
        (
            Fcm::with_origin("demo", "fixture".into(), &origin).unwrap(),
            server,
        )
    }
    #[tokio::test]
    #[ignore = "requires disposable PostgreSQL and loopback access"]
    async fn multicast_attempts_recovery_and_tenant_isolation() {
        let url = std::env::var("TEST_DATABASE_URL").unwrap();
        let (mut db, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
        tokio::spawn(async move { connection.await.unwrap() });
        let (mut other_db, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
        tokio::spawn(async move { connection.await.unwrap() });
        db.batch_execute("DROP TABLE IF EXISTS novu_rust_fcm_attempts,novu_queue_jobs CASCADE")
            .await
            .unwrap();
        db.batch_execute(crate::MIGRATION).await.unwrap();
        db.batch_execute(MIGRATION).await.unwrap();
        db.batch_execute(crate::in_app::PUSH_MESSAGES_MIGRATION)
            .await
            .unwrap();
        db.batch_execute("ALTER TABLE novu_rust_push_messages ADD COLUMN IF NOT EXISTS delivery_state text NOT NULL DEFAULT 'not_attempted'").await.unwrap();
        db.batch_execute(crate::reconciliation::MIGRATION)
            .await
            .unwrap();
        db.batch_execute(crate::reconciliation::RETRY_DISPATCH_MIGRATION)
            .await
            .unwrap();
        db.batch_execute(crate::reconciliation::OPERATOR_GRANTS_MIGRATION)
            .await
            .unwrap();
        db.execute("INSERT INTO novu_rust_delivery_operator_grants(environment_id,database_principal,decision) VALUES ('test',session_user,'retry'),('test',session_user,'accepted') ON CONFLICT DO NOTHING", &[]).await.unwrap();
        let payload = json!({"title":"Hi","content":"Hello","targets":["a","b","c"]});
        let other = Uuid::new_v4();
        let id = Uuid::new_v4();
        crate::enqueue(&db, other, "other", JOB_KIND, &payload)
            .await
            .unwrap();
        crate::enqueue(&db, id, "test", JOB_KIND, &payload)
            .await
            .unwrap();
        let (provider, server) = mock(vec![200, 429, 500]).await;
        let (first, second) = tokio::join!(
            process_one(&mut db, "test", &provider),
            process_one(&mut other_db, "test", &provider)
        );
        assert_ne!(first.unwrap(), second.unwrap());
        assert!(process_one(&mut db, "test", &provider).await.unwrap());
        assert!(process_one(&mut db, "test", &provider).await.unwrap());
        assert_eq!(server.await.unwrap(), ["a", "b", "c"]);
        let rows=db.query("SELECT state,http_status FROM novu_rust_fcm_attempts WHERE job_id=$1 ORDER BY target_index",&[&id]).await.unwrap();
        assert_eq!(
            rows.iter()
                .map(|r| r.get::<_, String>(0))
                .collect::<Vec<_>>(),
            ["accepted", "rejected", "unknown"]
        );
        assert_eq!(
            rows.iter()
                .map(|r| r.get::<_, Option<i32>>(1))
                .collect::<Vec<_>>(),
            [None, Some(429), Some(500)]
        );
        assert_eq!(
            db.query_one("SELECT status FROM novu_queue_jobs WHERE id=$1", &[&id])
                .await
                .unwrap()
                .get::<_, String>(0),
            "failed"
        );
        assert_eq!(
            db.query_one("SELECT status FROM novu_queue_jobs WHERE id=$1", &[&other])
                .await
                .unwrap()
                .get::<_, String>(0),
            "pending"
        );
        for (statuses, expected) in [(vec![200, 429], "completed"), (vec![429, 429], "failed")] {
            let id = Uuid::new_v4();
            let payload = json!({"title":"Hi","content":"Hello","targets":["x","y"]});
            crate::enqueue(&db, id, "test", JOB_KIND, &payload)
                .await
                .unwrap();
            let (provider, server) = mock(statuses).await;
            assert!(process_one(&mut db, "test", &provider).await.unwrap());
            assert!(process_one(&mut db, "test", &provider).await.unwrap());
            assert_eq!(server.await.unwrap(), ["x", "y"]);
            assert_eq!(
                db.query_one("SELECT status FROM novu_queue_jobs WHERE id=$1", &[&id])
                    .await
                    .unwrap()
                    .get::<_, String>(0),
                expected
            );
        }
        let before = Uuid::new_v4();
        crate::enqueue(
            &db,
            before,
            "test",
            JOB_KIND,
            &json!({"title":"Hi","content":"Hello","targets":["before"]}),
        )
        .await
        .unwrap();
        let expired = claim(&db, "test").await.unwrap().unwrap();
        assert_eq!(expired.id, before);
        db.execute("UPDATE novu_queue_jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1",&[&before]).await.unwrap();
        assert_eq!(recover_expired(&mut db, "test").await.unwrap(), 1);
        let (provider, server) = mock(vec![200]).await;
        assert!(process_one(&mut db, "test", &provider).await.unwrap());
        assert_eq!(server.await.unwrap(), ["before"]);
        assert_eq!(
            db.query_one("SELECT status FROM novu_queue_jobs WHERE id=$1", &[&before])
                .await
                .unwrap()
                .get::<_, String>(0),
            "completed"
        );
        let recovery = Uuid::new_v4();
        crate::enqueue(&db, recovery, "test", JOB_KIND, &payload)
            .await
            .unwrap();
        let job = claim(&db, "test").await.unwrap().unwrap();
        assert_eq!(job.id, recovery);
        db.execute("INSERT INTO novu_rust_push_messages(internal_id,environment_id,subscriber_id,subscriber_internal_id,notification_internal_id,workflow_identifier,transaction_id,device_tokens,title,status,push_job_id,target_index,delivery_state) VALUES ($1,'test','alice','subscriber','notification','workflow','transaction',ARRAY['a'],'Hi','sent',$2,0,'dispatching')", &[&recovery.to_string(),&recovery]).await.unwrap();
        db.execute("INSERT INTO novu_rust_fcm_attempts(job_id,environment_id,target_index,state) VALUES ($1,'test',0,'dispatching')",&[&recovery]).await.unwrap();
        db.execute("UPDATE novu_queue_jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1",&[&recovery]).await.unwrap();
        assert_eq!(recover_expired(&mut db, "test").await.unwrap(), 1);
        assert_eq!(
            db.query_one(
                "SELECT delivery_state FROM novu_rust_push_messages WHERE push_job_id=$1",
                &[&recovery]
            )
            .await
            .unwrap()
            .get::<_, String>(0),
            "unknown"
        );
        let (provider, server) = mock(vec![200, 200]).await;
        assert!(process_one(&mut db, "test", &provider).await.unwrap());
        assert!(process_one(&mut db, "test", &provider).await.unwrap());
        assert_eq!(server.await.unwrap(), ["b", "c"]);
        let rows = db
            .query(
                "SELECT state FROM novu_rust_fcm_attempts WHERE job_id=$1 ORDER BY target_index",
                &[&recovery],
            )
            .await
            .unwrap();
        assert_eq!(
            rows.iter()
                .map(|r| r.get::<_, String>(0))
                .collect::<Vec<_>>(),
            ["unknown", "accepted", "accepted"]
        );
        assert_eq!(
            db.query_one(
                "SELECT status FROM novu_queue_jobs WHERE id=$1",
                &[&recovery]
            )
            .await
            .unwrap()
            .get::<_, String>(0),
            "failed"
        );
        assert!(!process_one(&mut db, "test", &provider).await.unwrap());
        crate::reconciliation::resolve(
            &mut db,
            "test",
            recovery,
            Some(0),
            "retry",
            "fixture-operator",
            "provider confirmed no request",
        )
        .await
        .unwrap();
        let (provider, server) = mock(vec![200]).await;
        assert!(process_one(&mut db, "test", &provider).await.unwrap());
        assert_eq!(server.await.unwrap(), ["a"]);
        let rows = db
            .query(
                "SELECT state FROM novu_rust_fcm_attempts WHERE job_id=$1 ORDER BY target_index",
                &[&recovery],
            )
            .await
            .unwrap();
        assert_eq!(
            rows.iter()
                .map(|row| row.get::<_, String>(0))
                .collect::<Vec<_>>(),
            ["accepted", "accepted", "accepted"]
        );

        // Confirming an uncertain first target resumes only untouched targets.
        let resume = Uuid::new_v4();
        let plan = fcm::plan(&serde_json::from_value(payload.clone()).unwrap());
        let messages = fcm::wire_messages(&plan).unwrap();
        crate::enqueue(
            &db,
            resume,
            "test",
            JOB_KIND,
            &json!({"prepared_messages":messages}),
        )
        .await
        .unwrap();
        db.execute("UPDATE novu_queue_jobs SET status='failed',last_error='push delivery outcome unknown' WHERE id=$1", &[&resume]).await.unwrap();
        db.execute("INSERT INTO novu_rust_fcm_attempts(job_id,environment_id,target_index,state) VALUES ($1,'test',0,'unknown')", &[&resume]).await.unwrap();
        crate::reconciliation::resolve(
            &mut db,
            "test",
            resume,
            Some(0),
            "accepted",
            "fixture-operator",
            "provider confirmed acceptance",
        )
        .await
        .unwrap();
        let (provider, server) = mock(vec![200, 200]).await;
        assert!(process_one(&mut db, "test", &provider).await.unwrap());
        assert!(process_one(&mut db, "test", &provider).await.unwrap());
        assert_eq!(server.await.unwrap(), ["b", "c"]);
        let row=db.query_one("SELECT status,(SELECT count(*) FROM novu_rust_fcm_attempts WHERE job_id=$1 AND state='accepted') FROM novu_queue_jobs WHERE id=$1", &[&resume]).await.unwrap();
        assert_eq!(row.get::<_, String>(0), "completed");
        assert_eq!(row.get::<_, i64>(1), 3);
    }
}
