//! Minimal experimental single-step in-app processor. Not a Novu Inbox implementation.
use std::{
    collections::{HashMap, HashSet},
    error::Error,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio_postgres::Client;
use uuid::Uuid;

#[derive(Debug)]
struct InvalidJob(&'static str);

impl std::fmt::Display for InvalidJob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

impl Error for InvalidJob {}

#[derive(Debug)]
struct LeaseLost;

// Restricted ASCII header-key conversion matching the reference provider's
// camelCase transform for the accepted header characters.
fn provider_header_key(input: &str) -> String {
    let chars: Vec<char> = input.chars().collect();
    let mut words = Vec::new();
    let mut word = String::new();
    for (index, c) in chars.iter().copied().enumerate() {
        if c == '-' {
            if !word.is_empty() {
                words.push(std::mem::take(&mut word));
            }
            continue;
        }
        let boundary = index > 0
            && c.is_ascii_uppercase()
            && (chars[index - 1].is_ascii_lowercase()
                || (chars[index - 1].is_ascii_uppercase()
                    && chars.get(index + 1).is_some_and(char::is_ascii_lowercase)));
        if boundary && !word.is_empty() {
            words.push(std::mem::take(&mut word));
        }
        word.push(c);
    }
    if !word.is_empty() {
        words.push(word);
    }
    let mut result = String::new();
    for (index, word) in words.iter().enumerate() {
        let lower = word.to_ascii_lowercase();
        if index == 0 {
            result.push_str(&lower);
        } else {
            let mut chars = lower.chars();
            if let Some(first) = chars.next() {
                result.push(first.to_ascii_uppercase());
            }
            result.extend(chars);
        }
    }
    result
}

impl std::fmt::Display for LeaseLost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("job lease expired or was replaced")
    }
}

impl Error for LeaseLost {}

pub const MIGRATION: &str = include_str!("../migrations/0002_experimental_in_app.sql");
pub const ACTION_CTA_MIGRATION: &str = include_str!("../migrations/0059_in_app_action_cta.sql");
pub const STATE_MIGRATION: &str = include_str!("../migrations/0003_experimental_message_state.sql");
pub const STATE_DATES_MIGRATION: &str =
    include_str!("../migrations/0004_experimental_message_state_dates.sql");
pub const IDENTIFIER_MIGRATION: &str =
    include_str!("../migrations/0005_experimental_message_identifier.sql");
pub const INBOX_METADATA_MIGRATION: &str =
    include_str!("../migrations/0006_experimental_inbox_metadata.sql");
pub const INBOX_DATE_PRESENCE_MIGRATION: &str =
    include_str!("../migrations/0007_experimental_inbox_date_presence.sql");
pub const INBOX_DATA_MIGRATION: &str =
    include_str!("../migrations/0008_experimental_message_data.sql");
pub const INBOX_DATA_ARRAY_MIGRATION: &str =
    include_str!("../migrations/0009_experimental_data_array_paths.sql");
pub const CONTEXT_MIGRATION: &str = include_str!("../migrations/0010_experimental_context.sql");
pub const CONTEXT_DATA_MIGRATION: &str =
    include_str!("../migrations/0011_experimental_context_data.sql");
pub const CONTEXT_TIMESTAMPS_MIGRATION: &str =
    include_str!("../migrations/0012_experimental_context_timestamps.sql");
pub const CONTEXT_CURSOR_MIGRATION: &str =
    include_str!("../migrations/0013_experimental_context_cursor.sql");
pub const SUBSCRIBER_OBJECTID_MIGRATION: &str =
    include_str!("../migrations/0014_experimental_subscriber_objectid.sql");
pub const GLOBAL_PREFERENCES_MIGRATION: &str =
    include_str!("../migrations/0015_experimental_global_preferences.sql");
pub const WORKFLOW_PREFERENCES_MIGRATION: &str =
    include_str!("../migrations/0016_experimental_workflow_preferences.sql");
pub const SUBSCRIBER_WORKFLOW_PREFERENCES_MIGRATION: &str =
    include_str!("../migrations/0017_experimental_subscriber_workflow_preferences.sql");
pub const STEP_OUTCOMES_MIGRATION: &str =
    include_str!("../migrations/0018_experimental_in_app_outcomes.sql");
pub const SUBSCRIBER_ONLINE_MIGRATION: &str =
    include_str!("../migrations/0019_experimental_subscriber_online.sql");
pub const TOPICS_MIGRATION: &str = include_str!("../migrations/0020_experimental_topics.sql");
pub const TOPIC_DELETE_RESTRICT_MIGRATION: &str =
    include_str!("../migrations/0021_topic_delete_restrict.sql");
pub const TOPIC_DATA_MIGRATION: &str =
    include_str!("../migrations/0022_experimental_topic_data.sql");
pub const TOPIC_SUBSCRIPTIONS_MIGRATION: &str =
    include_str!("../migrations/0023_experimental_topic_subscriptions.sql");
pub const TOPIC_SUBSCRIPTIONS_CURSOR_MIGRATION: &str =
    include_str!("../migrations/0024_experimental_topic_subscription_cursor.sql");
pub const TOPIC_SUBSCRIPTION_NAME_MIGRATION: &str =
    include_str!("../migrations/0025_experimental_subscription_name_presence.sql");
pub const TOPIC_SUBSCRIPTION_PREFERENCES_MIGRATION: &str =
    include_str!("../migrations/0026_experimental_subscription_preferences.sql");
pub const SUBSCRIBER_SUBSCRIPTION_CURSOR_MIGRATION: &str =
    include_str!("../migrations/0027_experimental_subscriber_subscription_cursor.sql");
pub const SNOOZED_UNTIL_MIGRATION: &str =
    include_str!("../migrations/0028_experimental_snoozed_until.sql");
pub const DELIVERY_HISTORY_MIGRATION: &str =
    include_str!("../migrations/0029_experimental_delivery_history.sql");
pub const WS_EVENTS_MIGRATION: &str = include_str!("../migrations/0030_experimental_ws_events.sql");
pub const WS_BEST_EFFORT_MIGRATION: &str =
    include_str!("../migrations/0031_experimental_ws_best_effort.sql");
pub const MESSAGE_SEVERITY_MIGRATION: &str =
    include_str!("../migrations/0032_experimental_message_severity.sql");
pub const IN_APP_STEP_ACTIVE_MIGRATION: &str =
    include_str!("../migrations/0033_experimental_in_app_step_active.sql");
pub const NOTIFICATIONS_MIGRATION: &str =
    include_str!("../migrations/0034_experimental_notifications.sql");
pub const EMAIL_STEP_MIGRATION: &str =
    include_str!("../migrations/0036_experimental_email_step.sql");
pub const EMAIL_MESSAGES_MIGRATION: &str =
    include_str!("../migrations/0037_experimental_email_messages.sql");
pub const EMAIL_PROVIDER_RESULT_MIGRATION: &str =
    include_str!("../migrations/0038_experimental_email_provider_result.sql");
pub const EMAIL_STEP_ID_MIGRATION: &str =
    include_str!("../migrations/0039_experimental_email_step_id.sql");
pub const EMAIL_REVALIDATION_MIGRATION: &str =
    include_str!("../migrations/0040_experimental_email_revalidation.sql");
pub const SMS_STEP_MIGRATION: &str = include_str!("../migrations/0042_experimental_sms_step.sql");
pub const SMS_MESSAGES_MIGRATION: &str =
    include_str!("../migrations/0043_experimental_sms_messages.sql");
pub const SMS_STEP_ID_MIGRATION: &str =
    include_str!("../migrations/0044_experimental_sms_step_id.sql");
pub const PUSH_STEP_MIGRATION: &str = include_str!("../migrations/0046_experimental_push_step.sql");
pub const FCM_CREDENTIALS_MIGRATION: &str =
    include_str!("../migrations/0047_experimental_fcm_credentials.sql");
pub const PUSH_MESSAGES_MIGRATION: &str =
    include_str!("../migrations/0048_experimental_push_messages.sql");
pub const MESSAGE_SUBSCRIBER_CLEANUP_INDEXES_MIGRATION: &str =
    include_str!("../migrations/0049_message_subscriber_cleanup_indexes.sql");
pub const IN_APP_SUBSCRIBER_IDENTITY_MIGRATION: &str =
    include_str!("../migrations/0050_in_app_subscriber_identity.sql");
pub const MULTIPLE_FCM_CHANNELS_MIGRATION: &str =
    include_str!("../migrations/0051_multiple_fcm_channels.sql");
pub const PUSH_STEP_ID_MIGRATION: &str =
    include_str!("../migrations/0052_experimental_push_step_id.sql");
pub const DELIVERY_STATE_MIGRATION: &str = include_str!("../migrations/0053_delivery_state.sql");
pub const JOB_KIND: &str = novu_rust_contracts::EXPERIMENTAL_IN_APP_JOB_KIND;
pub const UNSNOOZE_JOB_KIND: &str = "experimental-in-app-unsnooze";
type WorkerResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

// Trigger provider layers use lodash merge with replacement for arrays.
fn merge_provider_layer(target: &mut serde_json::Value, source: &serde_json::Value) {
    match (target, source) {
        (serde_json::Value::Object(target), serde_json::Value::Object(source)) => {
            for (key, value) in source {
                if matches!(key.as_str(), "__proto__" | "constructor" | "prototype") {
                    continue;
                }
                if let Some(existing) = target.get_mut(key) {
                    merge_provider_layer(existing, value);
                } else {
                    target.insert(key.clone(), value.clone());
                }
            }
        }
        (target, source) => *target = source.clone(),
    }
}

/// Atomically consume a due unsnooze job and update its message. Subscriber
/// locking follows the same order as API mutations and subscriber deletion.
pub async fn process_one_unsnooze(client: &Client) -> Result<bool, tokio_postgres::Error> {
    let row = client.query_one(
        "WITH candidate AS MATERIALIZED (\
           SELECT j.id FROM novu_queue_jobs j JOIN novu_rust_subscribers s ON s.environment_id=j.environment_id AND s.subscriber_id=j.payload->>'subscriberId' \
           WHERE j.kind=$1 AND j.status='pending' AND j.available_at<=now() \
           ORDER BY j.available_at,j.created_at,j.id FOR UPDATE OF s SKIP LOCKED LIMIT 1\
         ), job AS MATERIALIZED (\
           SELECT j.* FROM novu_queue_jobs j JOIN candidate c ON c.id=j.id \
           WHERE j.status='pending' AND j.available_at<=now() FOR UPDATE OF j SKIP LOCKED\
         ), updated AS (\
           UPDATE novu_rust_in_app_messages m SET snoozed_until=NULL,created_at=now(),is_read=false,last_read_at=NULL,read_at_present=true, \
             delivered_history=COALESCE(m.delivered_history,ARRAY[COALESCE(m.delivered_at,m.created_at)]) || ARRAY[now()] \
           FROM job j WHERE m.environment_id=j.environment_id AND m.subscriber_id=j.payload->>'subscriberId' \
             AND m.subscriber_internal_id=(SELECT s.internal_id FROM novu_rust_subscribers s WHERE s.environment_id=m.environment_id AND s.subscriber_id=m.subscriber_id) AND m.internal_id=j.payload->>'notificationId' AND m.snoozed_until IS NOT NULL RETURNING m.id,m.internal_id,m.environment_id,m.subscriber_id,m.subscriber_internal_id,m.context_keys\
         ), emitted AS (\
           SELECT novu_rust_emit_ws_event(m.environment_id,s.internal_id,m.context_keys,'notification_received',jsonb_build_object('messageId',m.internal_id)) \
           FROM updated m JOIN novu_rust_subscribers s ON s.environment_id=m.environment_id AND s.subscriber_id=m.subscriber_id AND s.internal_id=m.subscriber_internal_id\
         ), completed AS (\
           UPDATE novu_queue_jobs q SET status=CASE WHEN EXISTS(SELECT 1 FROM updated) THEN 'completed' ELSE 'failed' END, \
             attempts=q.attempts+1,last_error=CASE WHEN EXISTS(SELECT 1 FROM updated) THEN NULL ELSE 'snoozed notification not found' END \
           FROM job j WHERE q.id=j.id AND (SELECT count(*) FROM emitted)>=0 RETURNING q.id\
         ) SELECT EXISTS(SELECT 1 FROM completed)",
        &[&UNSNOOZE_JOB_KIND],
    ).await?;
    if row.get(0) {
        return Ok(true);
    }
    // Subscriber deletion can leave queued work without a subscriber. Do not
    // leave that work permanently pending or touch a recreated identity.
    let orphan = client.query_one(
        "WITH orphan AS MATERIALIZED (SELECT j.id FROM novu_queue_jobs j \
         WHERE j.kind=$1 AND j.status='pending' AND j.available_at<=now() \
         AND NOT EXISTS (SELECT 1 FROM novu_rust_subscribers s WHERE s.environment_id=j.environment_id AND s.subscriber_id=j.payload->>'subscriberId') \
         ORDER BY j.available_at,j.created_at,j.id FOR UPDATE OF j SKIP LOCKED LIMIT 1), \
         failed AS (UPDATE novu_queue_jobs j SET status='failed',attempts=j.attempts+1,last_error='subscriber not found' FROM orphan o WHERE j.id=o.id RETURNING j.id) \
         SELECT EXISTS(SELECT 1 FROM failed)",
        &[&UNSNOOZE_JOB_KIND],
    ).await?;
    Ok(orphan.get(0))
}

// Novu requires a boolean JSON Logic result, and treats evaluation errors as
// false. A condition takes precedence over the enabled flag.
#[doc(hidden)]
pub fn topic_preference_passes(
    preferences: Option<&serde_json::Value>,
    variables: &serde_json::Value,
) -> bool {
    let Some(preferences) = preferences else {
        return true;
    };
    if let Some(condition) = preferences
        .pointer("/all/condition")
        .filter(|v| !v.is_null())
    {
        return std::panic::catch_unwind(|| jsonlogic::apply(condition, variables))
            .ok()
            .and_then(Result::ok)
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
    }
    preferences
        .pointer("/all/enabled")
        .and_then(|v| v.as_bool())
        .unwrap_or(true)
}

/// Preserve the first insertion order of subscriber IDs, as the reference Map does.
pub fn unique_recipient_ids(recipients: &[String]) -> Vec<&str> {
    let mut seen = HashSet::new();
    recipients
        .iter()
        .map(String::as_str)
        .filter(|s| seen.insert(*s))
        .collect()
}

/// Atomically create subscribers/messages and complete the claimed job.
/// Other Novu step types, complex templates, preferences and integrations are not supported.
pub async fn process_one(client: &mut Client) -> WorkerResult<bool> {
    Box::pin(process_one_with_lease(client, Duration::from_secs(60))).await
}

/// Process at most one job with a caller-selected lease. Expired jobs remain
/// eligible for the queue's explicit recovery policy; stale writes roll back.
pub async fn process_one_with_lease(client: &mut Client, lease: Duration) -> WorkerResult<bool> {
    let Some(job) = crate::claim(client, JOB_KIND, lease).await? else {
        return Ok(false);
    };
    match Box::pin(process_claimed(client, &job)).await {
        Ok(()) => Ok(true),
        Err(error) if error.downcast_ref::<LeaseLost>().is_some() => Ok(true),
        Err(error) if error.downcast_ref::<InvalidJob>().is_some() => {
            // Store only a fixed diagnostic, never the job payload or rendered content.
            // A false return means ownership was already lost. Do not overwrite
            // a newer lease; the recovery loop will handle an expired job.
            crate::fail(client, job.id, job.lease_token, &error.to_string()).await?;
            Ok(true)
        }
        Err(error) => Err(error),
    }
}

async fn process_claimed(client: &mut Client, job: &crate::ClaimedJob) -> WorkerResult<()> {
    let data = &job.payload;
    if !crate::allow_recreated_subscriber()
        && data
            .get("recipientIdentitySnapshot")
            .and_then(serde_json::Value::as_object)
            .is_none()
    {
        return Err(InvalidJob("subscriber identity snapshot missing").into());
    }
    let id = |key| {
        data.get(key)
            .and_then(|v| v.as_str())
            .map(str::to_owned)
            .ok_or(InvalidJob("invalid job fields"))
    };
    let workflow = id("workflow")?;
    let transaction_id = id("transactionId")?;
    let queued_content = id("content")?;
    let payload = data
        .get("payload")
        .filter(|v| v.is_object())
        .ok_or(InvalidJob("invalid job payload"))?;
    let message_payload = novu_rust_contracts::message::in_app_payload(payload);
    let email_override = match data.get("emailOverride") {
        None | Some(serde_json::Value::Null) => None,
        Some(serde_json::Value::Object(value)) => Some(value.clone()),
        _ => return Err(InvalidJob("invalid email override").into()),
    };
    let job_severity = match data.get("severity") {
        None | Some(serde_json::Value::Null) => "none",
        Some(serde_json::Value::String(value))
            if matches!(value.as_str(), "none" | "low" | "medium" | "high") =>
        {
            value.as_str()
        }
        _ => return Err(InvalidJob("invalid job severity").into()),
    };
    let severity_override = match data.get("severityOverride") {
        None => Some(job_severity), // jobs queued by the previous implementation
        Some(serde_json::Value::Null) => None,
        Some(serde_json::Value::String(value))
            if matches!(value.as_str(), "none" | "low" | "medium" | "high") =>
        {
            Some(value.as_str())
        }
        _ => return Err(InvalidJob("invalid severity override").into()),
    };
    let queued_tags = data
        .get("tags")
        .filter(|value| value.is_array())
        .cloned()
        .unwrap_or_else(|| serde_json::json!([]));
    let raw_recipients = data
        .get("recipients")
        .ok_or(InvalidJob("invalid recipients"))?;
    let mut recipients = if raw_recipients.as_array().is_some_and(Vec::is_empty) {
        Vec::new()
    } else {
        novu_rust_contracts::recipients::normalize(raw_recipients)
            .map_err(|_| InvalidJob("invalid recipients"))?
    };
    let string_list = |key: &str| -> Result<Vec<String>, InvalidJob> {
        match data.get(key) {
            None => Ok(Vec::new()),
            Some(serde_json::Value::Array(values)) => values
                .iter()
                .map(|v| {
                    v.as_str()
                        .map(str::to_owned)
                        .ok_or(InvalidJob("invalid topic recipients"))
                })
                .collect(),
            _ => Err(InvalidJob("invalid topic recipients")),
        }
    };
    let topic_keys = string_list("topicKeys")?;
    let topic_exclusions = string_list("topicExclusions")?;
    let actor = match data.get("actor") {
        None | Some(serde_json::Value::Null) => None,
        Some(value) if !value.is_array() => {
            let mut actors = novu_rust_contracts::recipients::normalize(value)
                .map_err(|_| InvalidJob("invalid actor"))?;
            actors.pop()
        }
        _ => return Err(InvalidJob("invalid actor").into()),
    };
    let actor_id = actor
        .as_ref()
        .and_then(|value| value.get("subscriberId"))
        .and_then(|value| value.as_str());
    if recipients.is_empty() && topic_keys.is_empty() {
        return Err(InvalidJob("invalid recipients").into());
    }
    let context_keys: Vec<String> = match data.get("contextKeys") {
        None => Vec::new(),
        Some(serde_json::Value::Array(keys)) => keys
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .map(str::to_owned)
                    .ok_or(InvalidJob("invalid context"))
            })
            .collect::<Result<_, _>>()?,
        _ => return Err(InvalidJob("invalid context").into()),
    };
    let tx = client.transaction().await?;
    // Hold the job row lock until domain writes and completion commit together.
    let owner = tx.query_opt("SELECT id FROM novu_queue_jobs WHERE id = $1 AND status = 'running' AND lease_token = $2 AND lease_until > clock_timestamp() FOR UPDATE", &[&job.id, &job.lease_token]).await?;
    if owner.is_none() {
        tx.rollback().await?;
        return Err(LeaseLost.into());
    }
    // The reference's first workflow worker resolves the trigger identifier
    // after enqueue. Older queued jobs retain their previous semantics.
    let (
        workflow_metadata,
        content,
        tags,
        step_active,
        workflow_internal_id,
        email_step,
        email_step_id,
        sms_step,
        sms_step_id,
        push_step,
        push_step_id,
    ) = if data.get("severityOverride").is_some() {
        let row = tx.query_opt("SELECT inbox_metadata->>'severity',in_app_content,inbox_metadata->'tags',has_steps,in_app_step_active,inbox_metadata->>'id',email_subject,email_html,email_step_active,email_step_id,sms_content,sms_step_active,sms_step_id,push_title,push_content,push_step_active,push_step_id FROM novu_rust_workflows WHERE environment_id=$1 AND identifier=$2", &[&job.environment_id, &workflow]).await?
            .ok_or(InvalidJob("workflow no longer exists"))?;
        let has_steps = row.get::<_, bool>(3);
        let email_subject: Option<String> = row.get(6);
        let email_html: Option<String> = row.get(7);
        let email_configured = email_subject.is_some() && email_html.is_some();
        let sms_content: Option<String> = row.get(10);
        let sms_configured = sms_content.is_some();
        let sms_step = sms_content.filter(|_| has_steps && row.get::<_, bool>(11));
        let push_title: Option<String> = row.get(13);
        let push_content: Option<String> = row.get(14);
        let push_configured = push_title.is_some() && push_content.is_some();
        let push_step = match (push_title, push_content) {
            (Some(title), Some(content)) if has_steps && row.get::<_, bool>(15) => {
                Some((title, content))
            }
            (None, None) | (Some(_), Some(_)) => None,
            _ => return Err(InvalidJob("invalid push step").into()),
        };
        let email_step = match (email_subject, email_html) {
            (Some(subject), Some(html)) if has_steps && row.get::<_, bool>(8) => {
                Some((subject, html))
            }
            (None, None) | (Some(_), Some(_)) => None,
            _ => return Err(InvalidJob("invalid email step").into()),
        };
        let step_active = has_steps
            && row.get::<_, bool>(4)
            && (row.get::<_, Option<String>>(1).is_some()
                || (!email_configured && !sms_configured && !push_configured));
        let current_content = match row.get::<_, Option<String>>(1) {
            Some(content) => content,
            None if !step_active => String::new(),
            None => return Err(InvalidJob("workflow content unavailable").into()),
        };
        let current_tags = row
            .get::<_, Option<serde_json::Value>>(2)
            .filter(serde_json::Value::is_array)
            .unwrap_or_else(|| serde_json::json!([]));
        (
            row.get::<_, Option<String>>(0),
            current_content,
            current_tags,
            step_active,
            row.get::<_, Option<String>>(5),
            email_step,
            row.get::<_, Option<String>>(9),
            sms_step,
            row.get::<_, Option<String>>(12),
            push_step,
            row.get::<_, Option<String>>(16),
        )
    } else {
        (
            None,
            queued_content,
            queued_tags,
            true,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
    };
    let push_provider = data
        .get("emailProviderOverrides")
        .and_then(|v| v.get("fcm"));
    let push_step_provider = push_step_id
        .as_ref()
        .and_then(|step| data.get("emailStepOverrides").and_then(|v| v.get(step)))
        .and_then(|v| v.get("providers"))
        .and_then(|v| v.get("fcm"));
    let mut push_provider_data = serde_json::Value::Object(serde_json::Map::new());
    for layer in [push_provider, push_step_provider].into_iter().flatten() {
        if layer.as_object().is_some_and(|v| {
            ["token", "topic", "condition", "tokens"].iter().any(|key| {
                v.get(*key).is_some_and(|value| {
                    value.as_str().is_some_and(|s| !s.is_empty())
                        || value.as_array().is_some_and(|items| !items.is_empty())
                })
            })
        }) && let Some(fields) = push_provider_data.as_object_mut()
        {
            for key in ["token", "topic", "condition", "tokens"] {
                fields.remove(key);
            }
        }
        merge_provider_layer(&mut push_provider_data, layer);
    }
    let mut merged_sms_override = data
        .get("smsOverride")
        .and_then(serde_json::Value::as_object)
        .cloned()
        .unwrap_or_default();
    let sms_provider = data
        .get("emailProviderOverrides")
        .and_then(|v| v.get("twilio"));
    let sms_step_provider = sms_step_id
        .as_deref()
        .and_then(|step| data.get("emailStepOverrides").and_then(|v| v.get(step)))
        .and_then(|v| v.get("providers"))
        .and_then(|v| v.get("twilio"));
    for layer in [
        data.get("smsFlatProviderOverride"),
        sms_provider,
        sms_step_provider,
    ]
    .into_iter()
    .flatten()
    {
        if !layer.is_null() {
            merged_sms_override.extend(
                layer
                    .as_object()
                    .ok_or(InvalidJob("invalid sms provider override"))?
                    .clone(),
            );
        }
    }
    let mut provider_data = serde_json::Map::new();
    let mut passthrough_body = serde_json::Map::new();
    for layer in [sms_provider, sms_step_provider]
        .into_iter()
        .flatten()
        .filter_map(serde_json::Value::as_object)
    {
        for (key, value) in layer {
            if key != "_passthrough" {
                provider_data.insert(key.clone(), value.clone());
            }
        }
        if let Some(body) = layer
            .get("_passthrough")
            .and_then(|v| v.get("body"))
            .and_then(serde_json::Value::as_object)
        {
            passthrough_body.extend(body.clone());
        }
    }
    if !passthrough_body.is_empty() {
        provider_data.insert(
            "_passthrough".into(),
            serde_json::json!({"body":passthrough_body}),
        );
    }
    let mut merged_email_override = email_override.unwrap_or_default();
    let provider_layer = data
        .get("emailProviderOverrides")
        .and_then(|v| v.get("sendgrid"));
    let step_layer = email_step_id
        .as_deref()
        .and_then(|step| data.get("emailStepOverrides").and_then(|v| v.get(step)))
        .and_then(|v| v.get("providers"))
        .and_then(|v| v.get("sendgrid"));
    let mut raw_provider_headers = serde_json::Map::new();
    let mut provider_from_without_name = false;
    for layer in [provider_layer, step_layer].into_iter().flatten() {
        if let Some(fields) = layer.as_object() {
            if fields.contains_key("from") {
                provider_from_without_name = true;
            }
            if let Some(headers) = fields.get("headers").and_then(serde_json::Value::as_object) {
                raw_provider_headers.extend(headers.clone());
            }
        }
    }
    for layer in [
        data.get("emailFlatProviderOverride"),
        provider_layer,
        step_layer,
    ]
    .into_iter()
    .flatten()
    {
        if !layer.is_null() {
            let fields = layer
                .as_object()
                .ok_or(InvalidJob("invalid provider override"))?;
            merged_email_override.extend(fields.clone());
        }
    }
    let email_override = Some(&merged_email_override);
    let severity = severity_override
        .map(str::to_owned)
        .or(workflow_metadata)
        .unwrap_or_else(|| "none".to_owned());
    if !matches!(severity.as_str(), "none" | "low" | "medium" | "high") {
        return Err(InvalidJob("invalid workflow severity").into());
    }
    // The original expands topics during multicast processing, not when the
    // trigger HTTP request is accepted. Direct recipients take precedence.
    let mut topic_preferences: HashMap<String, Vec<Option<serde_json::Value>>> = HashMap::new();
    let mut topic_identities: HashMap<String, String> = HashMap::new();
    let mut identity_changed = false;
    if !topic_keys.is_empty() {
        let topic_snapshot = if crate::allow_recreated_subscriber() {
            None
        } else {
            Some(
                data.get("topicIdentitySnapshot")
                    .and_then(serde_json::Value::as_object)
                    .ok_or(InvalidJob("topic identity snapshot missing"))?,
            )
        };
        let direct: Vec<String> = recipients
            .iter()
            .filter_map(|v| {
                v.get("subscriberId")
                    .and_then(|v| v.as_str())
                    .map(str::to_owned)
            })
            .collect();
        let rows = tx.query("SELECT t.subscriber_id,p.preferences,(w.inbox_metadata->>'id' IS NULL AND p.preferences IS NOT NULL),s.internal_id FROM novu_rust_topic_subscribers t JOIN novu_rust_subscribers s ON s.environment_id=t.environment_id AND s.subscriber_id=t.subscriber_id LEFT JOIN novu_rust_workflows w ON w.environment_id=t.environment_id AND w.identifier=$6 LEFT JOIN LATERAL (SELECT preferences FROM novu_rust_subscription_preferences p WHERE p.environment_id=t.environment_id AND p.subscription_internal_id=t.internal_id AND p.subscriber_internal_id=s.internal_id AND (w.inbox_metadata->>'id' IS NULL OR p.workflow_internal_id=w.inbox_metadata->>'id') ORDER BY p.created_at,p.context_keys_hash LIMIT 1) p ON true WHERE t.environment_id=$1 AND t.topic_key=ANY($2) AND NOT (t.subscriber_id=ANY($3)) AND NOT (t.subscriber_id=ANY($4)) AND ($5::text IS NULL OR t.subscriber_id<>$5) ORDER BY t.subscriber_id,t.internal_id", &[&job.environment_id,&topic_keys,&topic_exclusions,&direct,&actor_id,&workflow]).await?;
        for row in rows {
            if row.get::<_, bool>(2) {
                return Err(InvalidJob("workflow identity required for topic preferences").into());
            }
            let subscriber: String = row.get(0);
            let identity = if let Some(snapshot) = topic_snapshot {
                match snapshot.get(&subscriber) {
                    Some(serde_json::Value::String(id)) => id.clone(),
                    None => {
                        identity_changed = true;
                        continue;
                    }
                    _ => return Err(InvalidJob("invalid topic identity snapshot").into()),
                }
            } else {
                row.get(3)
            };
            topic_identities.insert(subscriber.clone(), identity);
            if !topic_preferences.contains_key(&subscriber) {
                recipients.push(serde_json::json!({"subscriberId":subscriber}));
            }
            topic_preferences
                .entry(subscriber)
                .or_default()
                .push(row.get(1));
        }
    }
    // Serialize overlapping batches before acquiring subscriber row locks. Sort
    // the actual lock keys so hash collisions cannot reverse the lock order.
    // Preserve recipient order for profile updates, generated IDs and messages.
    let mut subscriber_ids: Vec<&str> = recipients
        .iter()
        .map(|recipient| {
            recipient
                .get("subscriberId")
                .and_then(|value| value.as_str())
                .ok_or(InvalidJob("invalid subscriber id"))
        })
        .collect::<Result<_, _>>()?;
    if let Some(id) = actor_id {
        subscriber_ids.push(id);
    }
    tx.query("SELECT pg_advisory_xact_lock(hashtext($1), lock_key) FROM (SELECT DISTINCT hashtext(subscriber_id) AS lock_key FROM unnest($2::text[]) AS subscriber_id ORDER BY lock_key OFFSET 0) AS ordered_locks", &[&job.environment_id, &subscriber_ids]).await?;
    let snapshots = data
        .get("recipientIdentitySnapshot")
        .and_then(serde_json::Value::as_object);
    let mut created_actor_identity = None;
    let mut actor_internal_id: Option<String> = None;
    let mut actor_profile = if let (Some(actor), Some(actor_id)) = (actor.as_ref(), actor_id) {
        let mut fields = actor
            .as_object()
            .ok_or(InvalidJob("invalid actor"))?
            .clone();
        fields.remove("subscriberId");
        let fields = serde_json::Value::Object(fields);
        // Fence actor updates against the trigger-time identity. A direct
        // recipient can share the actor identity snapshot from older jobs.
        let expected = if crate::allow_recreated_subscriber() {
            None
        } else {
            Some(
                snapshots
                    .and_then(|map| map.get(actor_id))
                    .or_else(|| data.get("actorIdentitySnapshot"))
                    .ok_or(InvalidJob("actor identity snapshot missing"))?,
            )
        };
        let row = match expected {
            Some(serde_json::Value::String(id)) => tx.query_opt("UPDATE novu_rust_subscribers SET updated_at=CASE WHEN profile IS DISTINCT FROM profile || $3::jsonb THEN now() ELSE updated_at END,profile=profile || $3::jsonb WHERE environment_id=$1 AND subscriber_id=$2 AND internal_id=$4 RETURNING profile,internal_id", &[&job.environment_id,&actor_id,&fields,&id]).await?,
            Some(serde_json::Value::Null) => tx.query_opt("INSERT INTO novu_rust_subscribers (environment_id,subscriber_id,profile,internal_id) VALUES ($1,$2,$3,novu_rust_new_objectid()) ON CONFLICT (environment_id,subscriber_id) DO NOTHING RETURNING profile,internal_id", &[&job.environment_id,&actor_id,&fields]).await?,
            Some(_) => return Err(InvalidJob("invalid subscriber identity snapshot").into()),
            None => Some(tx.query_one("INSERT INTO novu_rust_subscribers (environment_id,subscriber_id,profile,internal_id) VALUES ($1,$2,$3,novu_rust_new_objectid()) ON CONFLICT (environment_id,subscriber_id) DO UPDATE SET updated_at=CASE WHEN novu_rust_subscribers.profile IS DISTINCT FROM novu_rust_subscribers.profile || EXCLUDED.profile THEN now() ELSE novu_rust_subscribers.updated_at END,profile=novu_rust_subscribers.profile || EXCLUDED.profile RETURNING profile,internal_id", &[&job.environment_id,&actor_id,&fields]).await?),
        };
        let row = row.ok_or(InvalidJob("subscriber identity changed"))?;
        actor_internal_id = Some(row.get(1));
        if matches!(expected, Some(serde_json::Value::Null)) {
            created_actor_identity = Some(serde_json::Value::String(row.get(1)));
        }
        let mut profile: serde_json::Value = row.get(0);
        profile
            .as_object_mut()
            .ok_or("invalid actor profile")?
            .insert("subscriberId".into(), actor_id.into());
        Some(profile)
    } else {
        None
    };
    // Novu resolves the latest stored context at send time, once per job.
    let mut resolved_context = serde_json::Map::new();
    if !context_keys.is_empty() {
        let rows = tx.query("SELECT context_key,data FROM novu_rust_contexts WHERE environment_id=$1 AND context_key=ANY($2)", &[&job.environment_id,&context_keys]).await?;
        for row in rows {
            let key: String = row.get(0);
            let data: serde_json::Value = row.get(1);
            let (kind, id) = key
                .split_once(':')
                .ok_or(InvalidJob("invalid context key"))?;
            resolved_context.insert(kind.to_owned(), serde_json::json!({"id":id,"data":data}));
        }
    }
    let resolved_context = serde_json::Value::Object(resolved_context);
    for recipient in recipients {
        let mut fields = recipient
            .as_object()
            .ok_or(InvalidJob("invalid recipient"))?
            .clone();
        let subscriber = fields
            .remove("subscriberId")
            .and_then(|v| v.as_str().map(str::to_owned))
            .ok_or(InvalidJob("invalid subscriber id"))?;
        let fields = serde_json::Value::Object(fields);
        let seconds = u32::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs())?;
        let topic_identity = topic_identities
            .get(&subscriber)
            .cloned()
            .map(serde_json::Value::String);
        let expected = if crate::allow_recreated_subscriber() {
            None
        } else {
            let snapshot = snapshots
                .and_then(|map| map.get(&subscriber))
                .or(topic_identity.as_ref());
            if actor_id == Some(subscriber.as_str())
                && matches!(snapshot, Some(serde_json::Value::Null))
            {
                created_actor_identity.as_ref().or(snapshot)
            } else {
                snapshot
            }
        };
        let row = match expected {
            Some(serde_json::Value::String(id)) => tx.query_opt("UPDATE novu_rust_subscribers SET updated_at=CASE WHEN profile IS DISTINCT FROM profile || $3::jsonb THEN now() ELSE updated_at END,profile=profile || $3::jsonb WHERE environment_id=$1 AND subscriber_id=$2 AND internal_id=$4 RETURNING profile,internal_id", &[&job.environment_id,&subscriber,&fields,&id]).await?,
            Some(serde_json::Value::Null) => tx.query_opt("INSERT INTO novu_rust_subscribers (environment_id,subscriber_id,profile,internal_id) VALUES ($1,$2,$3,novu_rust_new_objectid()) ON CONFLICT (environment_id,subscriber_id) DO NOTHING RETURNING profile,internal_id", &[&job.environment_id,&subscriber,&fields]).await?,
            Some(_) => return Err(InvalidJob("invalid subscriber identity snapshot").into()),
            None => Some(tx.query_one("INSERT INTO novu_rust_subscribers (environment_id, subscriber_id, profile, internal_id) VALUES ($1, $2, $3, novu_rust_new_objectid()) ON CONFLICT (environment_id, subscriber_id) DO UPDATE SET updated_at = CASE WHEN novu_rust_subscribers.profile IS DISTINCT FROM novu_rust_subscribers.profile || EXCLUDED.profile THEN now() ELSE novu_rust_subscribers.updated_at END, profile = novu_rust_subscribers.profile || EXCLUDED.profile RETURNING profile, internal_id", &[&job.environment_id, &subscriber, &fields]).await?),
        };
        let Some(row) = row else {
            identity_changed = true;
            continue;
        };
        let subscriber_internal_id: String = row.get(1);
        let mut profile: serde_json::Value = row.get(0);
        profile
            .as_object_mut()
            .ok_or("invalid stored profile")?
            .insert("subscriberId".into(), subscriber.clone().into());
        if actor_id == Some(subscriber.as_str()) {
            actor_profile = Some(profile.clone());
        }
        if let Some(preferences) = topic_preferences.get(&subscriber) {
            let mut variables = serde_json::json!({"payload":payload,"subscriber":profile,"context":resolved_context});
            if let Some(actor) = actor_profile.as_ref() {
                variables["actor"] = actor.clone();
            }
            if !preferences
                .iter()
                .any(|preference| topic_preference_passes(preference.as_ref(), &variables))
            {
                continue;
            }
        }
        // The subscriber upsert holds its row lock. Preference updates acquire
        // the same lock, so the channel decision is serialized with updates.
        let preference = tx.query_one("SELECT w.read_only,w.all_enabled,w.in_app_enabled,(p.channels->>'in_app')::boolean,(sp.channels->>'in_app')::boolean,w.email_enabled,(p.channels->>'email')::boolean,(sp.channels->>'email')::boolean FROM (SELECT 1) singleton LEFT JOIN novu_rust_workflow_preferences w ON w.environment_id=$1 AND w.workflow_identifier=$3 LEFT JOIN novu_rust_subscriber_preferences p ON p.environment_id=$1 AND p.subscriber_id=$2 LEFT JOIN novu_rust_subscriber_workflow_preferences sp ON sp.environment_id=$1 AND sp.subscriber_id=$2 AND sp.workflow_identifier=$3", &[&job.environment_id,&subscriber,&workflow]).await?;
        let critical = preference.get::<_, Option<bool>>(0).unwrap_or(false);
        let mut channels: Vec<&str> = Vec::new();
        if step_active {
            channels.push("in_app");
        }
        if email_step.is_some() {
            channels.push("email");
        }
        if sms_step.is_some() {
            channels.push("sms");
        }
        if push_step.is_some() {
            channels.push("push");
        }
        let organization_id = data
            .get("organizationId")
            .and_then(serde_json::Value::as_str);
        let notification = tx.query_one("INSERT INTO novu_rust_notifications (job_id,environment_id,organization_id,subscriber_id,subscriber_internal_id,workflow_identifier,workflow_internal_id,transaction_id,recipient,payload,channels,tags,severity,critical,context_keys) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15) ON CONFLICT (job_id,subscriber_internal_id) DO UPDATE SET job_id=EXCLUDED.job_id RETURNING internal_id", &[&job.id,&job.environment_id,&organization_id,&subscriber,&subscriber_internal_id,&workflow,&workflow_internal_id,&transaction_id,&recipient,&payload,&channels,&tags,&severity,&critical,&context_keys]).await?;
        let notification_id: String = notification.get(0);
        if let Some((push_title, push_content)) = push_step.as_ref() {
            let push_job_id = Uuid::new_v4();
            let push_payload = serde_json::json!({"workflow_context":{"subscriber_id":subscriber,"subscriber_internal_id":subscriber_internal_id,"notification_internal_id":notification_id,"transaction_id":transaction_id,"workflow_identifier":workflow,"workflow_internal_id":workflow_internal_id,"title_template":push_title,"content_template":push_content,"payload":payload,"context_keys":context_keys,"actor":actor_profile,"actor_internal_id":actor_internal_id,"overrides":data.get("pushFlatProviderOverride").and_then(|v| v.as_object()).cloned().unwrap_or_default(),"provider_data":push_provider_data}});
            tx.execute(
                "INSERT INTO novu_queue_jobs(id,environment_id,kind,payload) VALUES ($1,$2,$3,$4)",
                &[
                    &push_job_id,
                    &job.environment_id,
                    &crate::fcm_queue::JOB_KIND,
                    &push_payload,
                ],
            )
            .await?;
            tx.execute("INSERT INTO novu_rust_push_step_outcomes(job_id,environment_id,subscriber_id,subscriber_internal_id,workflow_identifier,transaction_id,status,push_job_id) VALUES ($1,$2,$3,$4,$5,$6,'queued',$7)",&[&job.id,&job.environment_id,&subscriber,&subscriber_internal_id,&workflow,&transaction_id,&push_job_id]).await?;
        }
        if let Some(sms_content) = sms_step.as_ref() {
            let sms_job_id = Uuid::new_v4();
            let message_id: String = tx
                .query_one("SELECT novu_rust_new_objectid()", &[])
                .await?
                .get(0);
            let sms_payload = serde_json::json!({"to":"", "content":"", "from":null,"provider_data":provider_data,
                "workflow_context":{"subscriber_id":subscriber,"subscriber_internal_id":subscriber_internal_id,
                    "workflow_identifier":workflow,"workflow_internal_id":workflow_internal_id,
                    "notification_internal_id":notification_id,"message_id":message_id,"transaction_id":transaction_id,
                    "content_template":sms_content,"payload":payload,"context":resolved_context,
                    "context_keys":context_keys,"actor":actor_profile,"actor_internal_id":actor_internal_id,"overrides":merged_sms_override}});
            tx.execute(
                "INSERT INTO novu_queue_jobs(id,environment_id,kind,payload) VALUES ($1,$2,$3,$4)",
                &[
                    &sms_job_id,
                    &job.environment_id,
                    &crate::twilio_queue::JOB_KIND,
                    &sms_payload,
                ],
            )
            .await?;
            tx.execute("INSERT INTO novu_rust_sms_step_outcomes(job_id,environment_id,subscriber_id,subscriber_internal_id,workflow_identifier,transaction_id,status,sms_job_id) VALUES ($1,$2,$3,$4,$5,$6,'queued',$7)", &[&job.id,&job.environment_id,&subscriber,&subscriber_internal_id,&workflow,&transaction_id,&sms_job_id]).await?;
        }
        if let Some((subject, html)) = email_step.as_ref() {
            // The original evaluates preferences and recipient data in the
            // channel worker, after the workflow job has been queued.
            let option = |key: &str| {
                email_override
                    .and_then(|v| v.get(key))
                    .and_then(serde_json::Value::as_str)
                    .filter(|v| !v.is_empty())
            };
            let override_list = |key: &str| -> Vec<String> {
                email_override
                    .and_then(|v| v.get(key))
                    .and_then(serde_json::Value::as_array)
                    .map(|items| {
                        items
                            .iter()
                            .filter_map(serde_json::Value::as_str)
                            .map(str::to_owned)
                            .collect()
                    })
                    .unwrap_or_default()
            };
            let message_id: String = tx
                .query_one("SELECT novu_rust_new_objectid()", &[])
                .await?
                .get(0);
            let email_job_id = Uuid::new_v4();
            let mut headers = email_override
                .and_then(|v| v.get("headers"))
                .and_then(serde_json::Value::as_object)
                .cloned()
                .unwrap_or_default();
            for (key, value) in &raw_provider_headers {
                headers.insert(provider_header_key(key), value.clone());
            }
            let email_payload = serde_json::json!({"to":[],"cc":override_list("cc"),"bcc":override_list("bcc"),"from":option("from"),"sender_name":option("senderName"),"provider_from_without_name":provider_from_without_name,"reply_to":option("replyTo"),"text":option("text"),"headers":headers,"custom_data":email_override.and_then(|v| v.get("customData")).cloned().unwrap_or_else(||serde_json::json!({})),"subject":"","html":"","message_id":message_id,"transaction_id":transaction_id,"workflow_identifier":workflow,"subscriber_id":subscriber,"workflow_context":{"subscriber_internal_id":subscriber_internal_id,"check_workflow":true,"workflow_internal_id":workflow_internal_id,"notification_internal_id":notification_id,"subject_template":subject,"html_template":html,"payload":payload,"context":resolved_context,"context_keys":context_keys,"actor":actor_profile,"actor_internal_id":actor_internal_id,"overrides":merged_email_override}});
            tx.execute(
                "INSERT INTO novu_queue_jobs(id,environment_id,kind,payload) VALUES ($1,$2,$3,$4)",
                &[
                    &email_job_id,
                    &job.environment_id,
                    &crate::sendgrid_queue::JOB_KIND,
                    &email_payload,
                ],
            )
            .await?;
            tx.execute("INSERT INTO novu_rust_email_step_outcomes(job_id,environment_id,subscriber_id,subscriber_internal_id,workflow_identifier,transaction_id,status,email_job_id) VALUES ($1,$2,$3,$4,$5,$6,'queued',$7)", &[&job.id,&job.environment_id,&subscriber,&subscriber_internal_id,&workflow,&transaction_id,&email_job_id]).await?;
        }
        if !step_active {
            continue;
        }
        let channel_enabled = novu_rust_contracts::preference::in_app_enabled(
            preference.get(0),
            preference.get(1),
            preference.get(2),
            preference.get(3),
            preference.get(4),
        );
        if !channel_enabled {
            tx.execute("INSERT INTO novu_rust_in_app_step_outcomes (job_id,environment_id,subscriber_id,subscriber_internal_id,workflow_identifier,transaction_id,status,detail) VALUES ($1,$2,$3,$4,$5,$6,'canceled','preference')", &[&job.id,&job.environment_id,&subscriber,&subscriber_internal_id,&workflow,&transaction_id]).await?;
            continue;
        }
        let rendered_content = novu_rust_contracts::template::render_in_app_with_actor(
            &content,
            payload,
            &profile,
            &resolved_context,
            actor_profile.as_ref(),
        )
        .map_err(|_| InvalidJob("unsupported template or input"))?;
        let message_random = Uuid::new_v4().simple().to_string();
        let message_internal_id = format!("{seconds:08x}{}", &message_random[..16]);
        let inserted = tx.query_opt("INSERT INTO novu_rust_in_app_messages (id, internal_id, environment_id, subscriber_id, workflow_identifier, transaction_id, content, payload, delivered_at, tags, context_keys, severity, severity_present, notification_internal_id, subscriber_internal_id) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now(), $9, $10, $11, true, $12, $13) ON CONFLICT (environment_id, subscriber_internal_id, workflow_identifier, transaction_id) DO NOTHING RETURNING internal_id", &[&Uuid::new_v4(), &message_internal_id, &job.environment_id, &subscriber, &workflow, &transaction_id, &rendered_content, &message_payload, &tags, &context_keys, &severity, &notification_id, &subscriber_internal_id]).await?;
        if inserted.is_some() {
            tx.query_one("SELECT novu_rust_emit_ws_event($1,$2,$3,'notification_received',jsonb_build_object('messageId',$4::text))", &[&job.environment_id,&subscriber_internal_id,&context_keys,&message_internal_id]).await?;
        }
        tx.execute("INSERT INTO novu_rust_in_app_step_outcomes (job_id,environment_id,subscriber_id,subscriber_internal_id,workflow_identifier,transaction_id,status) VALUES ($1,$2,$3,$4,$5,$6,'completed')", &[&job.id,&job.environment_id,&subscriber,&subscriber_internal_id,&workflow,&transaction_id]).await?;
    }
    let (status, diagnostic) = if identity_changed {
        ("failed", Some("subscriber identity changed"))
    } else {
        ("completed", None)
    };
    let completed = tx.execute("UPDATE novu_queue_jobs SET status = $3, last_error=$4, lease_token = NULL, lease_until = NULL WHERE id = $1 AND lease_token = $2 AND lease_until > clock_timestamp()", &[&job.id, &job.lease_token, &status, &diagnostic]).await?;
    if completed != 1 {
        tx.rollback().await?;
        return Err(LeaseLost.into());
    }
    tx.commit().await?;
    Ok(())
}
