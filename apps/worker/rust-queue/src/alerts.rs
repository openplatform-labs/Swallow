//! Alert transports. The caller owns scheduling, deduplication and recovery.
use crate::{resend::Resend, sendgrid::Email};
use reqwest::{Client, Url};
use serde_json::json;
use std::time::Duration;
use tokio_postgres::Client as PgClient;
use uuid::Uuid;

pub const MIGRATION: &str = include_str!("../migrations/0066_alert_delivery.sql");
pub const RESOLUTION_MIGRATION: &str =
    include_str!("../migrations/0067_alert_delivery_resolution.sql");
pub const RESOLVE_FUNCTION_MIGRATION: &str =
    include_str!("../migrations/0086_alert_resolve_function.sql");
type DbResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Debug, PartialEq, Eq)]
pub enum Reservation {
    Claimed(Uuid),
    CoolingDown,
    InFlight,
    Blocked,
}

/// Reserve one destination under a row lock. Expired in-flight attempts are
/// marked unknown, not automatically resent, because Slack has no dedup key.
pub async fn reserve(
    db: &mut PgClient,
    environment: &str,
    channel: &str,
    target: &str,
    cooldown: i32,
) -> DbResult<Reservation> {
    reserve_with_conditions(db, environment, channel, target, cooldown, None).await
}

/// Optional mask permits new problem categories to bypass a successful cooldown.
pub async fn reserve_with_conditions(
    db: &mut PgClient,
    environment: &str,
    channel: &str,
    target: &str,
    cooldown: i32,
    condition_mask: Option<i32>,
) -> DbResult<Reservation> {
    if condition_mask.is_some_and(|mask| mask <= 0) {
        return Err("invalid alert condition mask".into());
    }
    if !["slack", "email"].contains(&channel)
        || !(60..=86_400).contains(&cooldown)
        || target.is_empty()
        || target.len() > 64
        || !target
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"._-".contains(&c))
    {
        return Err("invalid alert destination or cooldown".into());
    }
    let tx = db.transaction().await?;
    if tx
        .query_opt(
            "SELECT 1 FROM novu_rust_environments WHERE id=$1",
            &[&environment],
        )
        .await?
        .is_none()
    {
        return Err("environment not found".into());
    }
    let allowed: bool = tx
        .query_one(
            "SELECT public.novu_rust_lock_operator_grant($1,'alert_send')",
            &[&environment],
        )
        .await?
        .get(0);
    if !allowed {
        return Err("alert send permission denied".into());
    }
    tx.execute("INSERT INTO novu_rust_alert_delivery_slots(environment_id,channel,target_id,state) VALUES ($1,$2,$3,'idle') ON CONFLICT DO NOTHING", &[&environment,&channel,&target]).await?;
    let row=tx.query_one("SELECT state,attempt_id,lease_until<=clock_timestamp(),last_started_at>clock_timestamp()-$4::integer*interval '1 second' FROM novu_rust_alert_delivery_slots WHERE environment_id=$1 AND channel=$2 AND target_id=$3 FOR UPDATE", &[&environment,&channel,&target,&cooldown]).await?;
    let state: String = row.get(0);
    if state == "dispatching" && row.get::<_, Option<bool>>(2) == Some(true) {
        let id: Uuid = row.get(1);
        tx.execute("UPDATE novu_rust_alert_delivery_attempts SET state='unknown',finished_at=clock_timestamp() WHERE id=$1 AND state='dispatching'", &[&id]).await?;
        tx.execute("UPDATE novu_rust_alert_delivery_slots SET state='unknown',lease_until=NULL WHERE environment_id=$1 AND channel=$2 AND target_id=$3", &[&environment,&channel,&target]).await?;
        tx.commit().await?;
        return Ok(Reservation::Blocked);
    }
    if state == "dispatching" {
        tx.commit().await?;
        return Ok(Reservation::InFlight);
    }
    if ["unknown", "rejected"].contains(&state.as_str()) {
        tx.commit().await?;
        return Ok(Reservation::Blocked);
    }
    if state == "accepted" && row.get::<_, Option<bool>>(3) == Some(true) {
        let new_condition = if let Some(mask) = condition_mask {
            let previous: Option<i32> = tx.query_one("SELECT last_condition_mask FROM novu_rust_alert_delivery_slots WHERE environment_id=$1 AND channel=$2 AND target_id=$3", &[&environment,&channel,&target]).await?.get(0);
            mask & !previous.unwrap_or(0) != 0
        } else {
            false
        };
        if !new_condition {
            tx.commit().await?;
            return Ok(Reservation::CoolingDown);
        }
    }
    let id = Uuid::new_v4();
    if let Some(mask) = condition_mask {
        tx.execute("UPDATE novu_rust_alert_delivery_slots SET last_condition_mask=$4 WHERE environment_id=$1 AND channel=$2 AND target_id=$3", &[&environment,&channel,&target,&mask]).await?;
    }
    tx.execute("UPDATE novu_rust_alert_delivery_slots SET state='dispatching',attempt_id=$4,last_started_at=clock_timestamp(),lease_until=clock_timestamp()+interval '60 seconds' WHERE environment_id=$1 AND channel=$2 AND target_id=$3", &[&environment,&channel,&target,&id]).await?;
    tx.execute("INSERT INTO novu_rust_alert_delivery_attempts(id,environment_id,channel,target_id,state) VALUES ($1,$2,$3,$4,'dispatching')", &[&id,&environment,&channel,&target]).await?;
    tx.commit().await?;
    Ok(Reservation::Claimed(id))
}

/// Reserve through the environment-scoped security-definer function.
pub async fn reserve_scoped(
    db: &PgClient,
    environment: &str,
    channel: &str,
    target: &str,
    cooldown: i32,
    condition_mask: Option<i32>,
) -> DbResult<Reservation> {
    let row = db
        .query_one(
            "SELECT * FROM public.novu_rust_reserve_alert($1,$2,$3,$4,$5)",
            &[&environment, &channel, &target, &cooldown, &condition_mask],
        )
        .await?;
    let result: String = row.get(0);
    let id: Option<Uuid> = row.get(1);
    match (result.as_str(), id) {
        ("claimed", Some(id)) => Ok(Reservation::Claimed(id)),
        ("cooldown", None) => Ok(Reservation::CoolingDown),
        ("inflight", None) => Ok(Reservation::InFlight),
        ("blocked", None) => Ok(Reservation::Blocked),
        _ => Err("invalid scoped reservation result".into()),
    }
}
/// Record only the calling login's own still-valid in-flight attempt.
pub async fn finish_scoped(
    db: &PgClient,
    environment: &str,
    channel: &str,
    target: &str,
    id: Uuid,
    result: &Result<(), AlertError>,
) -> DbResult<bool> {
    let (state, status) = match result {
        Ok(()) => ("accepted", None),
        Err(AlertError::Rejected(code)) => ("rejected", Some(i32::from(*code))),
        Err(AlertError::InvalidConfiguration) => ("rejected", None),
        Err(AlertError::Unknown) => ("unknown", None),
    };
    Ok(db
        .query_one(
            "SELECT public.novu_rust_finish_alert($1,$2,$3,$4,$5,$6)",
            &[&environment, &channel, &target, &id, &state, &status],
        )
        .await?
        .get(0))
}

/// Record which successfully observed conditions have recovered for one destination.
pub async fn observe_conditions(
    db: &PgClient,
    environment: &str,
    channel: &str,
    target: &str,
    active: i32,
    known: i32,
) -> DbResult<()> {
    db.query_one(
        "SELECT public.novu_rust_observe_alert_conditions($1,$2,$3,$4,$5)",
        &[&environment, &channel, &target, &active, &known],
    )
    .await?;
    Ok(())
}

/// The lease fences late responses after another process has recovered a slot.
pub async fn finish(
    db: &mut PgClient,
    environment: &str,
    channel: &str,
    target: &str,
    id: Uuid,
    result: &Result<(), AlertError>,
) -> DbResult<bool> {
    let (state, status) = match result {
        Ok(()) => ("accepted", None),
        Err(AlertError::Rejected(code)) => ("rejected", Some(i32::from(*code))),
        Err(AlertError::InvalidConfiguration) => ("rejected", None),
        Err(AlertError::Unknown) => ("unknown", None),
    };
    let tx = db.transaction().await?;
    let changed=tx.execute("UPDATE novu_rust_alert_delivery_slots SET state=$5,lease_until=NULL WHERE environment_id=$1 AND channel=$2 AND target_id=$3 AND attempt_id=$4 AND state='dispatching' AND lease_until>clock_timestamp()", &[&environment,&channel,&target,&id,&state]).await?;
    if changed == 1 {
        tx.execute("UPDATE novu_rust_alert_delivery_attempts SET state=$2,http_status=$3,finished_at=clock_timestamp() WHERE id=$1 AND state='dispatching'", &[&id,&state,&status]).await?;
    }
    tx.commit().await?;
    Ok(changed == 1)
}

/// This is an operator assertion, not provider verification. A mistaken retry
/// may duplicate an alert; direct database writes can bypass this guard.
pub async fn resolve(
    db: &mut PgClient,
    environment: &str,
    channel: &str,
    target: &str,
    decision: &str,
    operator: &str,
    reason: &str,
) -> DbResult<()> {
    db.query_one(
        "SELECT public.novu_rust_resolve_alert($1,$2,$3,$4,$5,$6)",
        &[
            &environment,
            &channel,
            &target,
            &decision,
            &operator,
            &reason,
        ],
    )
    .await?;
    Ok(())
}

pub async fn status(db: &PgClient, environment: &str) -> DbResult<Vec<serde_json::Value>> {
    if environment.trim().is_empty() {
        return Err("environment is required".into());
    }
    if db
        .query_opt(
            "SELECT 1 FROM novu_rust_environments WHERE id=$1",
            &[&environment],
        )
        .await?
        .is_none()
    {
        return Err("environment not found".into());
    }
    let rows=db.query("SELECT channel,target_id,CASE WHEN state='dispatching' AND lease_until<=clock_timestamp() THEN 'expired' ELSE state END,attempt_id,last_started_at::text FROM novu_rust_alert_delivery_slots WHERE environment_id=$1 ORDER BY channel,target_id LIMIT 101", &[&environment]).await?;
    if rows.len() > 100 {
        return Err("more than 100 alert destinations".into());
    }
    Ok(rows.into_iter().map(|r|json!({"channel":r.get::<_,String>(0),"targetId":r.get::<_,String>(1),"state":r.get::<_,String>(2),"attemptId":r.get::<_,Option<Uuid>>(3).map(|v|v.to_string()),"lastStartedAt":r.get::<_,Option<String>>(4)})).collect())
}

pub async fn health(db: &PgClient, environment: &str) -> DbResult<serde_json::Value> {
    if environment.trim().is_empty() {
        return Err("environment is required".into());
    }
    let row=db.query_one("SELECT count(*) FILTER (WHERE state='unknown'),count(*) FILTER (WHERE state='rejected'),count(*) FILTER (WHERE state='dispatching' AND lease_until<=clock_timestamp()),EXISTS(SELECT 1 FROM novu_rust_environments WHERE id=$1) FROM novu_rust_alert_delivery_slots WHERE environment_id=$1", &[&environment]).await?;
    if !row.get::<_, bool>(3) {
        return Err("environment not found".into());
    }
    Ok(
        json!({"unknown":row.get::<_,i64>(0),"rejected":row.get::<_,i64>(1),"expired":row.get::<_,i64>(2)}),
    )
}

/// Authorized environment-scoped aggregate without direct table access.
pub async fn scoped_health(db: &PgClient, environment: &str) -> DbResult<serde_json::Value> {
    let row = db
        .query_one(
            "SELECT * FROM public.novu_rust_alert_health($1)",
            &[&environment],
        )
        .await?;
    Ok(
        json!({"unknown":row.get::<_,i64>(0),"rejected":row.get::<_,i64>(1),"expired":row.get::<_,i64>(2)}),
    )
}
/// Authorized destination metadata; retains the existing 100-destination limit.
pub async fn scoped_status(db: &PgClient, environment: &str) -> DbResult<Vec<serde_json::Value>> {
    let rows = db
        .query(
            "SELECT * FROM public.novu_rust_alert_status($1)",
            &[&environment],
        )
        .await?;
    if rows.len() > 100 {
        return Err("more than 100 alert destinations".into());
    }
    Ok(rows.into_iter().map(|r|json!({"channel":r.get::<_,String>(0),"targetId":r.get::<_,String>(1),"state":r.get::<_,String>(2),"attemptId":r.get::<_,Option<Uuid>>(3).map(|v|v.to_string()),"lastStartedAt":r.get::<_,Option<String>>(4)})).collect())
}

#[derive(Debug, PartialEq, Eq)]
pub enum AlertError {
    InvalidConfiguration,
    Rejected(u16),
    Unknown,
}

/// Only aggregate counts are included; no subscriber, payload or credentials.
pub struct Alert {
    pub environment: String,
    pub overdue_undelivered: u64,
    pub projector_stale: bool,
    pub resend_enabled: bool,
    pub queue: Option<(u64, u64)>,
    pub attempts: Option<(u64, u64)>,
    pub twilio: Option<(u64, bool)>,
    pub sendgrid: Option<(u64, bool)>,
    pub sendgrid_check_failed: bool,
    pub slack_attempts: Option<(u64, u64, u64, u64, Option<u64>)>,
    pub failed_checks: [bool; 5], // resend, queue, attempts, twilio, Slack chat
    pub failed_jobs: Option<u64>,
    pub failed_jobs_check_failed: bool,
    pub database_probe: Option<(String, bool)>,
    pub alert_store_unavailable: bool,
}
impl Alert {
    /// Bit flags describe problem categories, not counts or sensitive content.
    pub fn condition_mask(&self) -> i32 {
        let mut mask = 0;
        let checks = [
            self.resend_enabled && self.overdue_undelivered > 0,
            self.resend_enabled && self.projector_stale,
            self.queue.is_some_and(|v| v.0 > 0),
            self.queue.is_some_and(|v| v.1 > 0),
            self.attempts.is_some_and(|v| v.0 > 0),
            self.attempts.is_some_and(|v| v.1 > 0),
            self.twilio.is_some_and(|v| v.0 > 0),
            self.twilio.is_some_and(|v| v.1),
            self.slack_attempts.is_some_and(|v| v.0 > 0),
            self.slack_attempts.is_some_and(|v| v.1 > 0),
            self.slack_attempts.is_some_and(|v| v.2 > 0),
            self.slack_attempts.is_some_and(|v| v.3 > 0),
            self.slack_attempts
                .is_some_and(|v| v.4.is_some_and(|n| n > 0)),
        ];
        for (bit, enabled) in checks.into_iter().chain(self.failed_checks).enumerate() {
            if enabled {
                mask |= 1 << bit;
            }
        }
        // Preserve existing bit numbers used by persisted cooldown masks.
        if self.failed_jobs.is_some_and(|count| count > 0) {
            mask |= 1 << 18;
        }
        if self.failed_jobs_check_failed {
            mask |= 1 << 19;
        }
        if self
            .database_probe
            .as_ref()
            .is_some_and(|(_, unhealthy)| *unhealthy)
        {
            mask |= 1 << 20;
        }
        if self.alert_store_unavailable {
            mask |= 1 << 21;
        }
        if self.sendgrid.is_some_and(|v| v.0 > 0) {
            mask |= 1 << 22;
        }
        if self.sendgrid.is_some_and(|v| v.1) {
            mask |= 1 << 23;
        }
        if self.sendgrid_check_failed {
            mask |= 1 << 24;
        }
        mask
    }
    /// Bits whose current state was actually observed, excluding unavailable or disabled checks.
    pub fn observed_condition_mask(&self) -> i32 {
        let mut known = 0;
        if self.resend_enabled {
            known |= (1 << 0) | (1 << 1);
        }
        if self.queue.is_some() {
            known |= (1 << 2) | (1 << 3);
        }
        if self.attempts.is_some() {
            known |= (1 << 4) | (1 << 5);
        }
        if self.twilio.is_some() {
            known |= (1 << 6) | (1 << 7);
        }
        if let Some(slack) = self.slack_attempts {
            known |= (1 << 8) | (1 << 9) | (1 << 10) | (1 << 11);
            if slack.4.is_some() {
                known |= 1 << 12;
            }
        }
        for (i, enabled) in [
            self.resend_enabled,
            self.queue.is_some(),
            self.attempts.is_some(),
            self.twilio.is_some(),
            self.slack_attempts.is_some(),
        ]
        .into_iter()
        .zip(self.failed_checks)
        .enumerate()
        {
            if enabled.0 || enabled.1 {
                known |= 1 << (13 + i);
            }
        }
        if self.failed_jobs.is_some() {
            known |= 1 << 18;
        }
        if self.failed_jobs.is_some() || self.failed_jobs_check_failed {
            known |= 1 << 19;
        }
        if self.database_probe.is_some() {
            known |= 1 << 20;
        }
        if self.alert_store_unavailable {
            known |= 1 << 21;
        }
        if self.sendgrid.is_some() {
            known |= (1 << 22) | (1 << 23);
        }
        if self.sendgrid.is_some() || self.sendgrid_check_failed {
            known |= 1 << 24;
        }
        known
    }
    pub fn text(&self) -> Result<String, AlertError> {
        if self.environment.is_empty()
            || self.environment.len() > 128
            || !self
                .environment
                .bytes()
                .all(|v| v.is_ascii_alphanumeric() || b"._-".contains(&v))
        {
            return Err(AlertError::InvalidConfiguration);
        }
        let mut details = Vec::new();
        if self.resend_enabled {
            details.push(format!(
                "Resend overdue undelivered jobs: {}; projector stale: {}",
                self.overdue_undelivered, self.projector_stale
            ));
        }
        if let Some((overdue, expired)) = self.queue {
            details.push(format!(
                "queue overdue jobs: {overdue}; expired leases: {expired}"
            ));
        }
        if let Some((unknown, stranded)) = self.attempts {
            details.push(format!(
                "unknown delivery attempts: {unknown}; stranded dispatches: {stranded}"
            ));
        }
        if let Some((overdue, stale)) = self.sendgrid {
            details.push(format!(
                "SendGrid overdue undelivered jobs: {overdue}; projector stale: {stale}"
            ));
        }
        if self.sendgrid_check_failed {
            details.push("SendGrid check failed".into());
        }
        if let Some((overdue, stale)) = self.twilio {
            details.push(format!(
                "Twilio overdue undelivered jobs: {overdue}; projector stale: {stale}"
            ));
        }
        if let Some((unknown, rejected, stranded, pre_dispatch, missing)) = self.slack_attempts {
            let missing = missing
                .map(|n| n.to_string())
                .unwrap_or_else(|| "disabled".into());
            details.push(format!("Slack chat unknown attempts: {unknown}; rejected attempts: {rejected}; stranded dispatches: {stranded}; pre-dispatch failures: {pre_dispatch}; missing endpoint alerts: {missing}"));
        }
        for (failed, name) in self.failed_checks.iter().zip([
            "Resend",
            "queue",
            "delivery attempts",
            "Twilio",
            "Slack chat",
        ]) {
            if *failed {
                details.push(format!("{name} check failed"));
            }
        }
        if let Some(count) = self.failed_jobs {
            details.push(format!("unacknowledged failed jobs: {count}"));
        }
        if self.failed_jobs_check_failed {
            details.push("failed jobs check failed".into());
        }
        if let Some((name, unhealthy)) = &self.database_probe {
            if name.is_empty()
                || name.len() > 48
                || !name
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || b"._-".contains(&c))
            {
                return Err(AlertError::InvalidConfiguration);
            }
            details.push(format!("database {name} probe failed: {unhealthy}"));
        }
        if self.alert_store_unavailable {
            details.push("alert state database unavailable".into());
        }
        if details.is_empty() {
            return Err(AlertError::InvalidConfiguration);
        }
        Ok(format!(
            "Swallow delivery alert: environment {}; {}",
            self.environment,
            details.join("; ")
        ))
    }
}

pub struct SlackWebhook {
    client: Client,
    endpoint: Url,
}
impl SlackWebhook {
    /// Webhook URL is a credential and must never be printed or persisted here.
    pub fn new(url: &str) -> Result<Self, AlertError> {
        let endpoint = Url::parse(url).map_err(|_| AlertError::InvalidConfiguration)?;
        let segments: Vec<_> = endpoint
            .path_segments()
            .ok_or(AlertError::InvalidConfiguration)?
            .collect();
        if endpoint.scheme() != "https"
            || !matches!(
                endpoint.host_str(),
                Some("hooks.slack.com" | "hooks.slack-gov.com")
            )
            || endpoint.port().is_some_and(|p| p != 443)
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
            || segments.len() != 4
            || segments[0] != "services"
            || segments[1..].iter().any(|s| {
                s.is_empty() || s.len() > 128 || !s.bytes().all(|b| b.is_ascii_alphanumeric())
            })
        {
            return Err(AlertError::InvalidConfiguration);
        }
        Self::with_endpoint(endpoint)
    }
    fn with_endpoint(endpoint: Url) -> Result<Self, AlertError> {
        let client = Client::builder()
            .timeout(Duration::from_secs(15))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| AlertError::InvalidConfiguration)?;
        Ok(Self { client, endpoint })
    }
    /// A transport error or unexpected success body leaves the outcome unknown.
    pub async fn send(&self, alert: &Alert) -> Result<(), AlertError> {
        let text = alert.text()?;
        let mut response = self
            .client
            .post(self.endpoint.clone())
            .json(&json!({"text":text,"mrkdwn":false,"unfurl_links":false}))
            .send()
            .await
            .map_err(|_| AlertError::Unknown)?;
        let status = response.status().as_u16();
        if status != 200 {
            return Err(if (400..500).contains(&status) && status != 408 {
                AlertError::Rejected(status)
            } else {
                AlertError::Unknown
            });
        }
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| AlertError::Unknown)? {
            if body.len() + chunk.len() > 256 {
                return Err(AlertError::Unknown);
            }
            body.extend_from_slice(&chunk);
        }
        if body == b"ok" {
            Ok(())
        } else {
            Err(AlertError::Unknown)
        }
    }
}

/// Resend acceptance is not evidence of inbox delivery. Callers supply a stable
/// idempotency key and handle ambiguous outcomes without blindly retrying.
pub async fn send_email(
    provider: &Resend,
    recipient: &str,
    alert: &Alert,
    idempotency_key: &str,
) -> Result<(), AlertError> {
    if recipient.is_empty()
        || recipient.len() > 320
        || recipient.contains(['\r', '\n', '<', '>'])
        || !recipient.contains('@')
    {
        return Err(AlertError::InvalidConfiguration);
    }
    let text = alert.text()?;
    let email = Email {
        to: vec![recipient.to_owned()],
        subject: "Swallow delivery alert".into(),
        html: format!("<p>{text}</p>"),
        text: Some(text),
        ..Default::default()
    };
    provider
        .send(&email, Some(idempotency_key))
        .await
        .map(|_| ())
        .map_err(|error| match error {
            crate::resend::SendError::InvalidConfiguration
            | crate::resend::SendError::InvalidMessage => AlertError::InvalidConfiguration,
            crate::resend::SendError::Provider(code)
                if (400..500).contains(&code) && code != 408 && code != 409 =>
            {
                AlertError::Rejected(code)
            }
            _ => AlertError::Unknown,
        })
}

/// Best-effort fallback, deliberately without durable coordination or automatic retry.
pub async fn send_uncoordinated(
    alert: &Alert,
    slack: Option<&SlackWebhook>,
    email: Option<(&Resend, &str)>,
    timeout: Duration,
) -> (
    Option<Result<(), AlertError>>,
    Option<Result<(), AlertError>>,
) {
    let key = format!("swallow-db-fallback/{}", Uuid::new_v4());
    let slack_result = if let Some(provider) = slack {
        Some(
            tokio::time::timeout(timeout, provider.send(alert))
                .await
                .unwrap_or(Err(AlertError::Unknown)),
        )
    } else {
        None
    };
    let email_result = if let Some((provider, to)) = email {
        Some(
            tokio::time::timeout(timeout, send_email(provider, to, alert, &key))
                .await
                .unwrap_or(Err(AlertError::Unknown)),
        )
    } else {
        None
    };
    (slack_result, email_result)
}

#[derive(Debug, PartialEq, Eq)]
pub enum Dispatch {
    Accepted,
    CoolingDown,
    InFlight,
    NeedsReview,
}

async fn dispatch<F, Fut>(
    db: &mut PgClient,
    environment: &str,
    channel: &str,
    target: &str,
    cooldown: i32,
    policy: (Option<i32>, bool),
    send: F,
) -> DbResult<Dispatch>
where
    F: FnOnce(Uuid) -> Fut,
    Fut: std::future::Future<Output = Result<(), AlertError>>,
{
    let (condition_mask, scoped) = policy;
    let reservation = if scoped {
        reserve_scoped(db, environment, channel, target, cooldown, condition_mask).await?
    } else {
        reserve_with_conditions(db, environment, channel, target, cooldown, condition_mask).await?
    };
    match reservation {
        Reservation::CoolingDown => Ok(Dispatch::CoolingDown),
        Reservation::InFlight => Ok(Dispatch::InFlight),
        Reservation::Blocked => Ok(Dispatch::NeedsReview),
        Reservation::Claimed(id) => {
            let result = send(id).await;
            let recorded = if scoped {
                finish_scoped(db, environment, channel, target, id, &result).await?
            } else {
                finish(db, environment, channel, target, id, &result).await?
            };
            if result.is_ok() && recorded {
                Ok(Dispatch::Accepted)
            } else {
                Ok(Dispatch::NeedsReview)
            }
        }
    }
}

pub async fn dispatch_slack(
    db: &mut PgClient,
    target: &str,
    cooldown: i32,
    alert: &Alert,
    provider: &SlackWebhook,
) -> DbResult<Dispatch> {
    dispatch_slack_with_conditions(db, target, cooldown, alert, provider, false).await
}

pub async fn dispatch_slack_with_conditions(
    db: &mut PgClient,
    target: &str,
    cooldown: i32,
    alert: &Alert,
    provider: &SlackWebhook,
    on_new_condition: bool,
) -> DbResult<Dispatch> {
    dispatch_slack_with_options(
        db,
        target,
        cooldown,
        alert,
        provider,
        on_new_condition,
        false,
    )
    .await
}

pub async fn dispatch_slack_with_options(
    db: &mut PgClient,
    target: &str,
    cooldown: i32,
    alert: &Alert,
    provider: &SlackWebhook,
    on_new_condition: bool,
    scoped: bool,
) -> DbResult<Dispatch> {
    alert.text().map_err(|_| "invalid alert environment")?;
    dispatch(
        db,
        &alert.environment,
        "slack",
        target,
        cooldown,
        (on_new_condition.then(|| alert.condition_mask()), scoped),
        |_| provider.send(alert),
    )
    .await
}

pub async fn dispatch_email(
    db: &mut PgClient,
    target: &str,
    cooldown: i32,
    alert: &Alert,
    provider: &Resend,
    recipient: &str,
) -> DbResult<Dispatch> {
    dispatch_email_with_conditions(db, target, cooldown, alert, provider, recipient, false).await
}

pub async fn dispatch_email_with_conditions(
    db: &mut PgClient,
    target: &str,
    cooldown: i32,
    alert: &Alert,
    provider: &Resend,
    recipient: &str,
    on_new_condition: bool,
) -> DbResult<Dispatch> {
    dispatch_email_with_options(
        db,
        target,
        cooldown,
        alert,
        provider,
        recipient,
        (on_new_condition, false),
    )
    .await
}

pub async fn dispatch_email_with_options(
    db: &mut PgClient,
    target: &str,
    cooldown: i32,
    alert: &Alert,
    provider: &Resend,
    recipient: &str,
    policy: (bool, bool),
) -> DbResult<Dispatch> {
    let (on_new_condition, scoped) = policy;
    alert.text().map_err(|_| "invalid alert environment")?;
    if recipient.is_empty()
        || recipient.len() > 320
        || recipient.contains(['\r', '\n', '<', '>'])
        || !recipient.contains('@')
    {
        return Err("invalid alert recipient".into());
    }
    dispatch(db,&alert.environment,"email",target,cooldown,(on_new_condition.then(|| alert.condition_mask()),scoped),|id|async move {send_email(provider,recipient,alert,&format!("swallow-alert/{id}")).await}).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    fn alert() -> Alert {
        Alert {
            environment: "env-a".into(),
            overdue_undelivered: 2,
            projector_stale: true,
            resend_enabled: true,
            queue: None,
            attempts: None,
            twilio: None,
            sendgrid: None,
            sendgrid_check_failed: false,
            slack_attempts: None,
            failed_checks: [false; 5],
            failed_jobs: None,
            failed_jobs_check_failed: false,
            database_probe: None,
            alert_store_unavailable: false,
        }
    }

    #[test]
    fn sendgrid_alert_conditions_are_independent_and_observed() {
        let mut a = alert();
        assert_eq!(a.observed_condition_mask() & (7 << 22), 0);
        a.sendgrid = Some((2, true));
        assert_eq!(a.condition_mask() & (7 << 22), 3 << 22);
        assert_eq!(a.observed_condition_mask() & (7 << 22), 7 << 22);
        assert!(
            a.text()
                .unwrap()
                .contains("SendGrid overdue undelivered jobs: 2; projector stale: true")
        );
        a.sendgrid = None;
        a.sendgrid_check_failed = true;
        assert_eq!(a.condition_mask() & (7 << 22), 1 << 24);
        assert_eq!(a.observed_condition_mask() & (7 << 22), 1 << 24);
        assert!(a.text().unwrap().contains("SendGrid check failed"));
    }

    #[test]
    fn condition_mask_tracks_types_not_counts() {
        let mut a = alert();
        let base = a.condition_mask();
        a.overdue_undelivered = 42;
        assert_eq!(a.condition_mask(), base);
        a.queue = Some((1, 0));
        assert_ne!(a.condition_mask(), base);
        let with_queue = a.condition_mask();
        a.failed_checks[2] = true;
        assert_eq!(a.condition_mask() ^ with_queue, 1 << 15);
        let prior = a.condition_mask();
        a.failed_jobs = Some(1);
        assert_eq!(a.condition_mask() ^ prior, 1 << 18);
        a.failed_jobs_check_failed = true;
        assert_eq!(a.condition_mask() ^ prior, (1 << 18) | (1 << 19));
    }

    #[test]
    fn observation_does_not_clear_disabled_or_failed_checks() {
        let mut a = alert();
        a.queue = Some((0, 0));
        a.failed_checks[2] = true;
        a.slack_attempts = Some((0, 0, 0, 0, None));
        let mask = a.observed_condition_mask();
        assert_ne!(mask & ((1 << 0) | (1 << 1)), 0);
        assert_eq!(mask & ((1 << 4) | (1 << 5)), 0);
        assert_ne!(mask & (1 << 15), 0);
        assert_eq!(mask & (1 << 12), 0);
        assert_eq!(a.condition_mask() & !mask, 0);
    }

    async fn mock_server(status: &str, body: &str) -> (Url, tokio::task::JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint =
            Url::parse(&format!("http://{}/test", listener.local_addr().unwrap())).unwrap();
        let status = status.to_owned();
        let body = body.to_owned();
        let task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buffer = Vec::new();
            let mut chunk = [0; 4096];
            loop {
                let n = stream.read(&mut chunk).await.unwrap();
                assert!(n > 0);
                buffer.extend_from_slice(&chunk[..n]);
                if let Some(pos) = buffer.windows(4).position(|w| w == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&buffer[..pos]);
                    let length: usize = headers
                        .lines()
                        .find_map(|l| {
                            l.to_ascii_lowercase()
                                .strip_prefix("content-length: ")
                                .and_then(|v| v.parse().ok())
                        })
                        .unwrap();
                    if buffer.len() >= pos + 4 + length {
                        break;
                    }
                }
            }
            stream.write_all(format!("HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
            String::from_utf8(buffer).unwrap()
        });
        (endpoint, task)
    }

    #[test]
    fn rejects_untrusted_webhook_destinations_and_injection() {
        for url in [
            "http://hooks.slack.com/services/A/B/C",
            "https://hooks.slack.com.evil.test/services/A/B/C",
            "https://hooks.slack.com/services/A/B/C?secret=1",
            "https://user@hooks.slack.com/services/A/B/C",
            "https://hooks.slack.com/services/A/B/C/extra",
            "https://127.0.0.1/services/A/B/C",
        ] {
            assert!(SlackWebhook::new(url).is_err());
        }
        assert!(SlackWebhook::new("https://hooks.slack.com/services/ABC/DEF/GHI").is_ok());
        assert!(
            Alert {
                environment: "<!channel>".into(),
                ..alert()
            }
            .text()
            .is_err()
        );
    }

    #[test]
    fn combined_alerts_only_report_enabled_aggregate_checks() {
        let mut a = alert();
        a.resend_enabled = false;
        a.queue = Some((3, 1));
        a.attempts = Some((2, 4));
        a.twilio = Some((5, true));
        a.slack_attempts = Some((1, 2, 3, 4, Some(5)));
        let text = a.text().unwrap();
        assert!(text.contains("queue overdue jobs: 3; expired leases: 1"));
        assert!(text.contains("unknown delivery attempts: 2; stranded dispatches: 4"));
        assert!(text.contains("Twilio overdue undelivered jobs: 5; projector stale: true"));
        assert!(text.contains(
            "Slack chat unknown attempts: 1; rejected attempts: 2; stranded dispatches: 3"
        ));
        assert!(!text.contains("Resend"));
        a.queue = None;
        a.attempts = None;
        a.twilio = None;
        a.slack_attempts = None;
        assert!(a.text().is_err());
        a.failed_checks = [true, false, true, true, true];
        let text = a.text().unwrap();
        assert!(text.contains("Resend check failed"));
        assert!(text.contains("delivery attempts check failed"));
        assert!(text.contains("Twilio check failed"));
        assert!(text.contains("Slack chat check failed"));
        assert!(!text.contains("projector stale"));
    }

    #[tokio::test]
    async fn slack_response_and_message_are_bounded() {
        let (url, server) = mock_server("200 OK", "ok").await;
        assert_eq!(
            SlackWebhook::with_endpoint(url)
                .unwrap()
                .send(&alert())
                .await,
            Ok(())
        );
        let request = server.await.unwrap();
        let body: serde_json::Value =
            serde_json::from_str(request.split_once("\r\n\r\n").unwrap().1).unwrap();
        assert_eq!(body["mrkdwn"], false);
        assert!(
            body["text"]
                .as_str()
                .unwrap()
                .contains("overdue undelivered jobs: 2")
        );
        let (url, server) = mock_server("429 Too Many Requests", "rate_limited").await;
        assert_eq!(
            SlackWebhook::with_endpoint(url)
                .unwrap()
                .send(&alert())
                .await,
            Err(AlertError::Rejected(429))
        );
        server.await.unwrap();
        let (url, server) = mock_server("200 OK", "unexpected").await;
        assert_eq!(
            SlackWebhook::with_endpoint(url)
                .unwrap()
                .send(&alert())
                .await,
            Err(AlertError::Unknown)
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn database_probe_alert_uses_both_mock_transports() {
        let mut a = alert();
        a.resend_enabled = false;
        a.database_probe = Some(("primary".into(), true));
        assert_eq!(a.condition_mask(), 1 << 20);
        assert_ne!(a.observed_condition_mask() & (1 << 20), 0);
        let (url, server) = mock_server("200 OK", "ok").await;
        assert_eq!(
            SlackWebhook::with_endpoint(url).unwrap().send(&a).await,
            Ok(())
        );
        assert!(
            server
                .await
                .unwrap()
                .contains("database primary probe failed: true")
        );
        let (url, server) = mock_server("200 OK", r#"{"id":"fixture"}"#).await;
        let provider = Resend::test_endpoint(
            crate::resend::Config {
                api_key: "fixture-key".into(),
                from: "from@example.test".into(),
                sender_name: String::new(),
            },
            url.as_str(),
        )
        .unwrap();
        assert_eq!(
            send_email(&provider, "ops@example.test", &a, "db-alert-fixture").await,
            Ok(())
        );
        assert!(
            server
                .await
                .unwrap()
                .contains("database primary probe failed: true")
        );
        a.database_probe = Some(("bad\nlabel".into(), true));
        assert!(a.text().is_err());
    }

    #[tokio::test]
    async fn uncoordinated_fallback_tries_both_transports_independently() {
        let mut a = alert();
        a.resend_enabled = false;
        a.database_probe = Some(("primary".into(), true));
        a.alert_store_unavailable = true;
        let (url, slack_server) = mock_server("500 Internal Server Error", "failed").await;
        let slack = SlackWebhook::with_endpoint(url).unwrap();
        let (url, email_server) = mock_server("200 OK", r#"{"id":"fallback"}"#).await;
        let email = Resend::test_endpoint(
            crate::resend::Config {
                api_key: "fixture-key".into(),
                from: "from@example.test".into(),
                sender_name: String::new(),
            },
            url.as_str(),
        )
        .unwrap();
        let result = send_uncoordinated(
            &a,
            Some(&slack),
            Some((&email, "ops@example.test")),
            Duration::from_secs(2),
        )
        .await;
        assert_eq!(result.0, Some(Err(AlertError::Unknown)));
        assert_eq!(result.1, Some(Ok(())));
        let slack_request = slack_server.await.unwrap();
        let email_request = email_server.await.unwrap();
        for request in [&slack_request, &email_request] {
            assert!(request.contains("alert state database unavailable"));
            assert!(request.contains("database primary probe failed: true"));
        }
        assert!(
            email_request
                .to_ascii_lowercase()
                .contains("idempotency-key: swallow-db-fallback/")
        );
        assert!(!a.text().unwrap().contains("fixture-key"));
    }

    #[tokio::test]
    async fn resend_alert_is_separate_from_slack_and_has_idempotency_key() {
        let (url, server) = mock_server("200 OK", r#"{"id":"mock-id"}"#).await;
        let provider = Resend::test_endpoint(
            crate::resend::Config {
                api_key: "fixture-key".into(),
                from: "from@example.test".into(),
                sender_name: String::new(),
            },
            url.as_str(),
        )
        .unwrap();
        assert_eq!(
            send_email(&provider, "ops@example.test", &alert(), "alert/test-1").await,
            Ok(())
        );
        let request = server.await.unwrap();
        assert!(
            request
                .to_ascii_lowercase()
                .contains("idempotency-key: alert/test-1")
        );
        let body: serde_json::Value =
            serde_json::from_str(request.split_once("\r\n\r\n").unwrap().1).unwrap();
        assert_eq!(body["to"], json!(["ops@example.test"]));
        assert_eq!(body["subject"], "Swallow delivery alert");
        assert!(
            send_email(&provider, "bad\n@example.test", &alert(), "alert/test-2")
                .await
                .is_err()
        );
    }

    #[tokio::test]
    #[ignore = "requires disposable PostgreSQL and mock loopback servers"]
    async fn unhealthy_environment_dispatches_both_destinations_with_cooldown() {
        let (mut db, driver) = tokio_postgres::connect(
            &std::env::var("TEST_DATABASE_URL").unwrap(),
            tokio_postgres::NoTls,
        )
        .await
        .unwrap();
        tokio::spawn(async move {
            let _ = driver.await;
        });
        db.batch_execute("CREATE TABLE novu_rust_environments(id text PRIMARY KEY,organization_id text NOT NULL); INSERT INTO novu_rust_environments VALUES ('env-a','org')").await.unwrap();
        for sql in [
            crate::MIGRATION,
            crate::in_app::EMAIL_MESSAGES_MIGRATION,
            crate::resend_queue::MIGRATION,
            crate::resend_projection::MIGRATION,
            crate::resend_projection::HEARTBEAT_MIGRATION,
            crate::reconciliation::MIGRATION,
            crate::reconciliation::OPERATOR_GRANTS_MIGRATION,
            crate::reconciliation::OPERATOR_GRANT_LOCK_MIGRATION,
            MIGRATION,
            RESOLUTION_MIGRATION,
        ] {
            db.batch_execute(sql).await.unwrap();
        }
        db.execute("INSERT INTO novu_rust_delivery_operator_grants VALUES ('env-a',session_user,'alert_send')", &[]).await.unwrap();
        let health = crate::reconciliation::resend_health(&db, "env-a", 300, 120)
            .await
            .unwrap();
        let alert = Alert {
            environment: "env-a".into(),
            overdue_undelivered: health["overdueUndelivered"].as_u64().unwrap(),
            projector_stale: health["projectorStale"].as_bool().unwrap(),
            resend_enabled: true,
            queue: None,
            attempts: None,
            twilio: None,
            sendgrid: Some((1, true)),
            sendgrid_check_failed: false,
            slack_attempts: None,
            failed_checks: [false; 5],
            failed_jobs: None,
            failed_jobs_check_failed: false,
            database_probe: None,
            alert_store_unavailable: false,
        };
        assert!(alert.projector_stale);
        let (url, slack_server) = mock_server("200 OK", "ok").await;
        let slack = SlackWebhook::with_endpoint(url).unwrap();
        assert_eq!(
            dispatch_slack(&mut db, "ops", 60, &alert, &slack)
                .await
                .unwrap(),
            Dispatch::Accepted
        );
        let slack_request = slack_server.await.unwrap();
        assert!(slack_request.contains("projector stale: true"));
        assert!(slack_request.contains("SendGrid overdue undelivered jobs: 1"));
        assert_eq!(
            dispatch_slack(&mut db, "ops", 60, &alert, &slack)
                .await
                .unwrap(),
            Dispatch::CoolingDown
        );
        let (url, email_server) = mock_server("200 OK", r#"{"id":"mock-id"}"#).await;
        let email = Resend::test_endpoint(
            crate::resend::Config {
                api_key: "fixture-key".into(),
                from: "from@example.test".into(),
                sender_name: String::new(),
            },
            url.as_str(),
        )
        .unwrap();
        assert_eq!(
            dispatch_email(&mut db, "oncall", 60, &alert, &email, "ops@example.test")
                .await
                .unwrap(),
            Dispatch::Accepted
        );
        let email_request = email_server.await.unwrap();
        assert!(
            email_request
                .to_ascii_lowercase()
                .contains("idempotency-key: swallow-alert/")
        );
        let rows = db
            .query(
                "SELECT channel,state FROM novu_rust_alert_delivery_attempts ORDER BY channel",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|r| r.get::<_, String>(1) == "accepted"));
        db.batch_execute(include_str!(
            "../migrations/0085_alert_condition_cooldown.sql"
        ))
        .await
        .unwrap();
        let mut alert = alert;
        let (url, server) = mock_server("200 OK", "ok").await;
        let slack = SlackWebhook::with_endpoint(url).unwrap();
        assert_eq!(
            dispatch_slack_with_conditions(&mut db, "ops", 60, &alert, &slack, true)
                .await
                .unwrap(),
            Dispatch::Accepted
        );
        server.await.unwrap();
        assert_eq!(
            dispatch_slack_with_conditions(&mut db, "ops", 60, &alert, &slack, true)
                .await
                .unwrap(),
            Dispatch::CoolingDown
        );
        alert.queue = Some((1, 0));
        let (url, server) = mock_server("200 OK", "ok").await;
        let slack = SlackWebhook::with_endpoint(url).unwrap();
        assert_eq!(
            dispatch_slack_with_conditions(&mut db, "ops", 60, &alert, &slack, true)
                .await
                .unwrap(),
            Dispatch::Accepted
        );
        assert!(server.await.unwrap().contains("queue overdue jobs: 1"));
        let (url, server) = mock_server("200 OK", r#"{"id":"mock-new"}"#).await;
        let email = Resend::test_endpoint(
            crate::resend::Config {
                api_key: "fixture-key".into(),
                from: "from@example.test".into(),
                sender_name: String::new(),
            },
            url.as_str(),
        )
        .unwrap();
        assert_eq!(
            dispatch_email_with_conditions(
                &mut db,
                "oncall",
                60,
                &alert,
                &email,
                "ops@example.test",
                true
            )
            .await
            .unwrap(),
            Dispatch::Accepted
        );
        assert!(server.await.unwrap().contains("queue overdue jobs: 1"));
        db.batch_execute(include_str!("../migrations/0088_scoped_alert_dispatch.sql"))
            .await
            .unwrap();
        let (url, server) = mock_server("200 OK", "ok").await;
        let scoped_slack = SlackWebhook::with_endpoint(url).unwrap();
        assert_eq!(
            dispatch_slack_with_options(
                &mut db,
                "scoped-ops",
                60,
                &alert,
                &scoped_slack,
                true,
                true
            )
            .await
            .unwrap(),
            Dispatch::Accepted
        );
        assert!(server.await.unwrap().contains("queue overdue jobs: 1"));
        let (url, server) = mock_server("200 OK", r#"{"id":"scoped-test"}"#).await;
        let scoped_email = Resend::test_endpoint(
            crate::resend::Config {
                api_key: "fixture-key".into(),
                from: "from@example.test".into(),
                sender_name: String::new(),
            },
            url.as_str(),
        )
        .unwrap();
        assert_eq!(
            dispatch_email_with_options(
                &mut db,
                "scoped-email",
                60,
                &alert,
                &scoped_email,
                "ops@example.test",
                (true, true)
            )
            .await
            .unwrap(),
            Dispatch::Accepted
        );
        assert!(server.await.unwrap().contains("queue overdue jobs: 1"));
        assert_eq!(db.query_one("SELECT count(*) FROM novu_rust_alert_delivery_attempts WHERE target_id IN ('scoped-ops','scoped-email') AND state='accepted' AND sender_session_user=session_user AND sender_oid IS NOT NULL",&[]).await.unwrap().get::<_,i64>(0),2);
        let role = format!("mock_sender_{}", Uuid::new_v4().simple());
        db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_reserve_alert(text,text,text,integer,integer),public.novu_rust_finish_alert(text,text,text,uuid,text,integer) TO {role}")).await.unwrap();
        db.execute("INSERT INTO novu_rust_delivery_operator_grants(environment_id,database_principal,decision) VALUES ('env-a',$1,'alert_send')",&[&role]).await.unwrap();
        let (mut restricted, driver) = tokio_postgres::connect(
            &std::env::var("TEST_DATABASE_URL").unwrap(),
            tokio_postgres::NoTls,
        )
        .await
        .unwrap();
        tokio::spawn(async move {
            let _ = driver.await;
        });
        restricted
            .batch_execute(&format!("SET SESSION AUTHORIZATION {role}"))
            .await
            .unwrap();
        let (url, server) = mock_server("200 OK", "ok").await;
        let slack = SlackWebhook::with_endpoint(url).unwrap();
        assert_eq!(
            dispatch_slack_with_options(
                &mut restricted,
                "limited-slack",
                60,
                &alert,
                &slack,
                true,
                true
            )
            .await
            .unwrap(),
            Dispatch::Accepted
        );
        assert!(server.await.unwrap().contains("queue overdue jobs: 1"));
        let (url, server) = mock_server("200 OK", r#"{"id":"limited"}"#).await;
        let email = Resend::test_endpoint(
            crate::resend::Config {
                api_key: "fixture-key".into(),
                from: "from@example.test".into(),
                sender_name: String::new(),
            },
            url.as_str(),
        )
        .unwrap();
        assert_eq!(
            dispatch_email_with_options(
                &mut restricted,
                "limited-email",
                60,
                &alert,
                &email,
                "ops@example.test",
                (true, true)
            )
            .await
            .unwrap(),
            Dispatch::Accepted
        );
        assert!(server.await.unwrap().contains("queue overdue jobs: 1"));
        assert!(
            restricted
                .query("SELECT * FROM public.novu_rust_alert_delivery_slots", &[])
                .await
                .is_err()
        );
        assert_eq!(db.query_one("SELECT count(*) FROM novu_rust_alert_delivery_attempts WHERE sender_session_user=$1 AND state='accepted'",&[&role]).await.unwrap().get::<_,i64>(0),2);
        // An operation timeout after the HTTP request starts must not cause an automatic retry.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
        let (seen_tx, seen_rx) = tokio::sync::oneshot::channel();
        let blocked_server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 4096];
            let n = stream.read(&mut buf).await.unwrap();
            assert!(n > 0);
            let _ = seen_tx.send(());
            std::future::pending::<()>().await;
        });
        let stalled = SlackWebhook::with_endpoint(endpoint).unwrap();
        assert!(
            tokio::time::timeout(
                Duration::from_secs(1),
                dispatch_slack_with_options(
                    &mut restricted,
                    "stalled",
                    60,
                    &alert,
                    &stalled,
                    true,
                    true
                )
            )
            .await
            .is_err()
        );
        tokio::time::timeout(Duration::from_secs(2), seen_rx)
            .await
            .unwrap()
            .unwrap();
        let (url, second_server) = mock_server("200 OK", r#"{"id":"after-timeout"}"#).await;
        let second_email = Resend::test_endpoint(
            crate::resend::Config {
                api_key: "fixture-key".into(),
                from: "from@example.test".into(),
                sender_name: String::new(),
            },
            url.as_str(),
        )
        .unwrap();
        assert_eq!(
            tokio::time::timeout(
                Duration::from_secs(2),
                dispatch_email_with_options(
                    &mut restricted,
                    "after-slack-timeout",
                    60,
                    &alert,
                    &second_email,
                    "ops@example.test",
                    (true, true),
                ),
            )
            .await
            .unwrap()
            .unwrap(),
            Dispatch::Accepted
        );
        second_server.await.unwrap();
        assert_eq!(db.query_one("SELECT state FROM novu_rust_alert_delivery_slots WHERE environment_id='env-a' AND target_id='stalled'",&[]).await.unwrap().get::<_,String>(0),"dispatching");
        db.execute("UPDATE novu_rust_alert_delivery_slots SET lease_until=clock_timestamp()-interval '1 second' WHERE environment_id='env-a' AND target_id='stalled'",&[]).await.unwrap();
        assert_eq!(
            reserve_scoped(
                &restricted,
                "env-a",
                "slack",
                "stalled",
                60,
                Some(alert.condition_mask())
            )
            .await
            .unwrap(),
            Reservation::Blocked
        );
        assert_eq!(db.query_one("SELECT state FROM novu_rust_alert_delivery_slots WHERE environment_id='env-a' AND target_id='stalled'",&[]).await.unwrap().get::<_,String>(0),"unknown");
        blocked_server.abort();
        restricted
            .batch_execute("RESET SESSION AUTHORIZATION")
            .await
            .unwrap();
        db.execute(
            "DELETE FROM novu_rust_delivery_operator_grants WHERE database_principal=$1",
            &[&role],
        )
        .await
        .unwrap();
        db.batch_execute(&format!("DROP OWNED BY {role}; DROP ROLE {role}"))
            .await
            .unwrap();
    }
}
