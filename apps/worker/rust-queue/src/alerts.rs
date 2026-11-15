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
    if condition_mask == Some(0) {
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
    pub sendgrid_webhook_keys: Option<Result<u64, ()>>,
    pub fcm_credentials: Option<Result<(u64, u64, u64, u64), ()>>,
    pub slack_attempts: Option<(u64, u64, u64, u64, Option<u64>)>,
    pub slack_policy_cancelled: Option<u64>,
    pub discord_health: Option<Result<(u64,u64,u64,u64), ()>>,
    pub discord_retry_health: Option<Result<(u64,u64), ()>>,
    pub email_policy_cancelled: Option<u64>,
    pub sms_policy_cancelled: Option<u64>,
    pub push_policy_cancelled: Option<u64>,
    pub channel_policy_check_failed: bool,
    pub failed_checks: [bool; 5], // resend, queue, attempts, twilio, Slack chat
    pub resend_auto: Option<Result<(u64, u64, u64, u64), ()>>,
    pub slack_cooldown: Option<Result<(bool, u64, u64), ()>>,
    pub resend_cooldown: Option<Result<(bool, u64, u64), ()>>,
    pub twilio_delayed: Option<Result<(u64, u64, u64), ()>>,
    pub fcm_partial: Option<Result<u64, ()>>,
    pub fcm_unreviewed: Option<Result<u64, ()>>,
    pub fcm_unrecorded_claims: Option<Result<u64, ()>>,
    pub fcm_cooldown: Option<Result<(bool,u64,u64), ()>>,
    pub fcm_cooldown_source: Option<Option<u16>>,
    pub fcm_cooldown_distinct_source: bool,
    pub failed_jobs: Option<u64>,
    pub failed_jobs_check_failed: bool,
    pub database_probe: Option<(String, bool)>,
    pub operator_key_probe: Option<(String, bool)>,
    pub resend_webhook_key_probe: Option<(String, bool)>,
    pub twilio_webhook_key_probe: Option<(String, bool)>,
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
        if self.failed_jobs.is_some_and(|count| count > 0)
            || self
                .resend_auto
                .is_some_and(|value| matches!(value,Ok((a,b,c,d)) if a>0||b>0||c>0||d>0))
            || self.slack_cooldown.is_some_and(|value| matches!(value,Ok((active,deferred,overdue)) if active||deferred>0||overdue>0))
            || self.resend_cooldown.is_some_and(|value| matches!(value,Ok((active,deferred,overdue)) if active||deferred>0||overdue>0))
            || self.twilio_delayed.is_some_and(|value| matches!(value,Ok((a,b,c)) if a>0||b>0||c>0))
            || self.fcm_partial.is_some_and(|value| matches!(value,Ok(n) if n>0))
            || self.fcm_unreviewed.is_some_and(|value| matches!(value,Ok(n) if n>0))
            || self.fcm_unrecorded_claims.is_some_and(|value| matches!(value,Ok(n) if n>0))
            || self.discord_health.is_some_and(|v| matches!(v,Ok((a,b,c,d)) if a>0||b>0||c>0||d>0))
            || self.discord_retry_health.is_some_and(|v| matches!(v,Ok((_,overdue)) if overdue>0))
            || self.fcm_cooldown.is_some_and(|value| matches!(value,Ok((active,deferred,overdue)) if active||deferred>0||overdue>0))
        {
            mask |= 1 << 18;
        }
        if self.discord_retry_health.is_some_and(|value| value.is_err()) || self.discord_health.is_some_and(|value| value.is_err()) || self.fcm_unrecorded_claims.is_some_and(|value| value.is_err()) || self.failed_jobs_check_failed || self.resend_auto.is_some_and(|value| value.is_err()) || self.slack_cooldown.is_some_and(|value| value.is_err()) || self.resend_cooldown.is_some_and(|value| value.is_err()) || self.twilio_delayed.is_some_and(|value| value.is_err()) || self.fcm_partial.is_some_and(|value| value.is_err()) || self.fcm_unreviewed.is_some_and(|value| value.is_err()) || self.fcm_cooldown.is_some_and(|value| value.is_err()) {
            mask |= 1 << 19;
        }
        if self
            .database_probe
            .as_ref()
            .is_some_and(|(_, unhealthy)| *unhealthy)
            || self.operator_key_probe.as_ref().is_some_and(|(_, unhealthy)| *unhealthy)
            || self.resend_webhook_key_probe.as_ref().is_some_and(|(_, unhealthy)| *unhealthy)
            || self.twilio_webhook_key_probe.as_ref().is_some_and(|(_, unhealthy)| *unhealthy)
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
        if self.slack_policy_cancelled.is_some_and(|n| n > 0) {
            mask |= 1 << 25;
        }
        if self.email_policy_cancelled.is_some_and(|n| n > 0) {
            mask |= 1 << 26;
        }
        if self.sms_policy_cancelled.is_some_and(|n| n > 0) {
            mask |= 1 << 27;
        }
        if self.channel_policy_check_failed {
            mask |= 1 << 28;
        }
        if self.push_policy_cancelled.is_some_and(|n| n > 0) {
            mask |= 1 << 29;
        }
        if self
            .sendgrid_webhook_keys
            .is_some_and(|v| !matches!(v, Ok(1..=10)))
        {
            mask |= 1 << 30;
        }
        if self.fcm_credentials.is_some_and(|value| match value {
            Ok((healthy, unhealthy, _, minimum)) => healthy < minimum || unhealthy > 0,
            Err(()) => true,
        }) {
            mask |= i32::MIN;
        }
        mask
    }
    /// V2 assigns separate condition and failure bits to the three shared checks.
    pub fn condition_mask_v2(&self) -> i64 {
        let mut mask = i64::from(self.condition_mask() as u32) & !((1_i64 << 18) | (1_i64 << 19));
        if self.failed_jobs.is_some_and(|n| n > 0) { mask |= 1_i64 << 18; }
        if self.failed_jobs_check_failed { mask |= 1_i64 << 19; }
        if self.resend_auto.is_some_and(|v| matches!(v, Ok((a,b,c,d)) if a>0 || b>0 || c>0 || d>0)) { mask |= 1_i64 << 32; }
        if self.resend_auto.is_some_and(|v| v.is_err()) { mask |= 1_i64 << 33; }
        if self.slack_cooldown.is_some_and(|v| matches!(v, Ok((active,deferred,overdue)) if active || deferred>0 || overdue>0)) { mask |= 1_i64 << 34; }
        if self.slack_cooldown.is_some_and(|v| v.is_err()) { mask |= 1_i64 << 35; }
        if self.twilio_delayed.is_some_and(|v| matches!(v, Ok((a,b,c)) if a>0||b>0||c>0)) { mask |= 1_i64 << 36; }
        if self.twilio_delayed.is_some_and(|v| v.is_err()) { mask |= 1_i64 << 37; }
        if self.fcm_partial.is_some_and(|v| matches!(v, Ok(n) if n>0)) { mask |= 1_i64 << 38; }
        if self.fcm_partial.is_some_and(|v| v.is_err()) { mask |= 1_i64 << 39; }
        if self.fcm_unreviewed.is_some_and(|v| matches!(v, Ok(n) if n>0)) { mask |= 1_i64 << 40; }
        if self.fcm_unreviewed.is_some_and(|v| v.is_err()) { mask |= 1_i64 << 41; }
        if self.fcm_cooldown.is_some_and(|v| matches!(v, Ok((active,deferred,overdue)) if active||deferred>0||overdue>0)) { mask |= 1_i64 << 42; }
        if self.fcm_cooldown.is_some_and(|v| v.is_err()) { mask |= 1_i64 << 43; }
        if self.fcm_unrecorded_claims.is_some_and(|v| matches!(v,Ok(n) if n>0)) { mask |= 1_i64 << 49; }
        if self.fcm_unrecorded_claims.is_some_and(|v| v.is_err()) { mask |= 1_i64 << 50; }
        if self.resend_cooldown.is_some_and(|v| matches!(v,Ok((active,deferred,overdue)) if active||deferred>0||overdue>0)) { mask |= 1_i64 << 47; }
        if self.resend_cooldown.is_some_and(|v| v.is_err()) { mask |= 1_i64 << 48; }
        if self.fcm_cooldown_distinct_source && self.fcm_cooldown.is_some_and(|v| matches!(v,Ok((true,_,_)))) {
            match self.fcm_cooldown_source { Some(Some(429)) => mask |= 1_i64 << 44, Some(Some(503)) => mask |= 1_i64 << 45, Some(None) => mask |= 1_i64 << 46, _ => {} }
        }
        if let Some(Ok((unknown,rejected,stranded,pre_dispatch))) = self.discord_health {
            for (bit,count) in [(51,unknown),(52,rejected),(53,stranded),(54,pre_dispatch)] { if count>0 { mask |= 1_i64 << bit; } }
        }
        if self.discord_health.is_some_and(|v| v.is_err()) { mask |= 1_i64 << 55; }
        if self.discord_retry_health.is_some_and(|v| matches!(v,Ok((_,overdue)) if overdue>0)) { mask |= 1_i64 << 56; }
        if self.discord_retry_health.is_some_and(|v| v.is_err()) { mask |= 1_i64 << 57; }
        mask
    }
    pub fn observed_condition_mask_v2(&self) -> i64 {
        let mut known = i64::from(self.observed_condition_mask() as u32) & !((1_i64 << 18) | (1_i64 << 19));
        if self.failed_jobs.is_some() && !self.failed_jobs_check_failed { known |= 1_i64 << 18; }
        if self.failed_jobs.is_some() || self.failed_jobs_check_failed { known |= 1_i64 << 19; }
        if self.resend_auto.is_some_and(|v| v.is_ok()) { known |= 1_i64 << 32; }
        if self.resend_auto.is_some() { known |= 1_i64 << 33; }
        if self.slack_cooldown.is_some_and(|v| v.is_ok()) { known |= 1_i64 << 34; }
        if self.slack_cooldown.is_some() { known |= 1_i64 << 35; }
        if self.twilio_delayed.is_some_and(|v| v.is_ok()) { known |= 1_i64 << 36; }
        if self.twilio_delayed.is_some() { known |= 1_i64 << 37; }
        if self.fcm_partial.is_some_and(|v| v.is_ok()) { known |= 1_i64 << 38; }
        if self.fcm_partial.is_some() { known |= 1_i64 << 39; }
        if self.fcm_unreviewed.is_some_and(|v| v.is_ok()) { known |= 1_i64 << 40; }
        if self.fcm_unreviewed.is_some() { known |= 1_i64 << 41; }
        if self.fcm_unrecorded_claims.is_some_and(|v| v.is_ok()) { known |= 1_i64 << 49; }
        if self.fcm_unrecorded_claims.is_some() { known |= 1_i64 << 50; }
        if self.fcm_cooldown.is_some_and(|v| v.is_ok()) { known |= 1_i64 << 42; }
        if self.fcm_cooldown.is_some() { known |= 1_i64 << 43; }
        if self.resend_cooldown.is_some_and(|v| v.is_ok()) { known |= 1_i64 << 47; }
        if self.resend_cooldown.is_some() { known |= 1_i64 << 48; }
        if self.fcm_cooldown_distinct_source && self.fcm_cooldown_source.is_some() && self.fcm_cooldown.is_some_and(|v| v.is_ok()) { known |= (1_i64 << 44) | (1_i64 << 45) | (1_i64 << 46); }
        if self.discord_health.is_some_and(|v| v.is_ok()) { known |= (1_i64 << 51) | (1_i64 << 52) | (1_i64 << 53) | (1_i64 << 54); }
        if self.discord_health.is_some() { known |= 1_i64 << 55; }
        if self.discord_retry_health.is_some_and(|v| v.is_ok()) { known |= 1_i64 << 56; }
        if self.discord_retry_health.is_some() { known |= 1_i64 << 57; }
        known
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
            self.slack_attempts.is_some() || self.slack_policy_cancelled.is_some(),
        ]
        .into_iter()
        .zip(self.failed_checks)
        .enumerate()
        {
            if enabled.0 || enabled.1 {
                known |= 1 << (13 + i);
            }
        }
        // Both checks share the existing failed-job cooldown category. Do
        // not mark it observed healthy if either enabled check failed.
        if (self.discord_retry_health.is_some() || self.discord_health.is_some() || self.fcm_unrecorded_claims.is_some() || self.failed_jobs.is_some() || self.resend_auto.is_some() || self.slack_cooldown.is_some() || self.resend_cooldown.is_some() || self.twilio_delayed.is_some() || self.fcm_partial.is_some() || self.fcm_unreviewed.is_some() || self.fcm_cooldown.is_some())
            && !self.discord_retry_health.is_some_and(|value| value.is_err())
            && !self.discord_health.is_some_and(|value| value.is_err())
            && !self.fcm_unrecorded_claims.is_some_and(|value| value.is_err())
            && !self.failed_jobs_check_failed
            && !self.resend_auto.is_some_and(|value| value.is_err())
            && !self.slack_cooldown.is_some_and(|value| value.is_err())
            && !self.resend_cooldown.is_some_and(|value| value.is_err())
            && !self.twilio_delayed.is_some_and(|value| value.is_err())
            && !self.fcm_partial.is_some_and(|value| value.is_err())
            && !self.fcm_unreviewed.is_some_and(|value| value.is_err())
            && !self.fcm_cooldown.is_some_and(|value| value.is_err())
        {
            known |= 1 << 18;
        }
        if self.discord_retry_health.is_some() || self.discord_health.is_some() || self.fcm_unrecorded_claims.is_some() || self.failed_jobs.is_some() || self.failed_jobs_check_failed || self.resend_auto.is_some() || self.slack_cooldown.is_some() || self.resend_cooldown.is_some() || self.twilio_delayed.is_some() || self.fcm_partial.is_some() || self.fcm_unreviewed.is_some() || self.fcm_cooldown.is_some()
        {
            known |= 1 << 19;
        }
        if self.database_probe.is_some() || self.operator_key_probe.is_some() || self.resend_webhook_key_probe.is_some() || self.twilio_webhook_key_probe.is_some() {
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
        if self.slack_policy_cancelled.is_some() {
            known |= 1 << 25;
        }
        if self.email_policy_cancelled.is_some() {
            known |= 1 << 26;
        }
        if self.sms_policy_cancelled.is_some() {
            known |= 1 << 27;
        }
        if self.push_policy_cancelled.is_some() {
            known |= 1 << 29;
        }
        if self.sendgrid_webhook_keys.is_some() {
            known |= 1 << 30;
        }
        if self.fcm_credentials.is_some() {
            known |= i32::MIN;
        }
        if self.email_policy_cancelled.is_some()
            || self.sms_policy_cancelled.is_some()
            || self.push_policy_cancelled.is_some()
            || self.channel_policy_check_failed
        {
            known |= 1 << 28;
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
        if let Some(result) = self.sendgrid_webhook_keys {
            match result {
                Ok(count) => details.push(format!("SendGrid active webhook keys: {count}")),
                Err(()) => details.push("SendGrid webhook key check failed".into()),
            }
        }
        if let Some(result) = self.fcm_credentials {
            match result {
                Ok((healthy,unhealthy,stale,minimum))=>details.push(format!("FCM credential workers: {healthy} healthy, {unhealthy} unhealthy, {stale} stale; minimum healthy: {minimum}")),
                Err(())=>details.push("FCM credential worker check failed".into()),
            }
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
        if let Some(result) = self.discord_retry_health {
            match result { Ok((pending,overdue)) => details.push(format!("Discord retry pending: {pending}; overdue: {overdue}")), Err(()) => details.push("Discord retry health check failed".into()) }
        }
        if let Some(result) = self.discord_health {
            match result {
                Ok((unknown,rejected,stranded,pre_dispatch)) => details.push(format!("Discord unknown attempts: {unknown}; rejected attempts: {rejected}; stranded dispatches: {stranded}; pre-dispatch failures: {pre_dispatch}")),
                Err(()) => details.push("Discord health check failed".into()),
            }
        }
        if let Some(count) = self.slack_policy_cancelled {
            details.push(format!("Slack workflow policy cancellations: {count}"));
        }
        if let Some(n) = self.email_policy_cancelled {
            details.push(format!("Email workflow policy cancellations: {n}"));
        }
        if let Some(n) = self.sms_policy_cancelled {
            details.push(format!("SMS workflow policy cancellations: {n}"));
        }
        if let Some(n) = self.push_policy_cancelled {
            details.push(format!("Push workflow policy cancellations: {n}"));
        }
        if self.channel_policy_check_failed {
            details.push("Channel policy cancellation check failed".into());
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
        if let Some(result) = self.resend_auto {
            match result {Ok((rate,exhausted,deferred,overdue))=>details.push(format!("Resend 429 aged: {rate}; retries exhausted: {exhausted}; guard deferred: {deferred}; automatic retries overdue: {overdue}")),Err(())=>details.push("Resend automatic retry check failed".into())}
        }
        if let Some(result) = self.slack_cooldown {
            match result { Ok((active,deferred,overdue))=>details.push(format!("Slack shared cooldown active: {active}; deferred jobs: {deferred}; overdue deferred jobs: {overdue}")),Err(())=>details.push("Slack shared cooldown check failed".into()) }
        }
        if let Some(result) = self.resend_cooldown {
            match result { Ok((active,deferred,overdue))=>details.push(format!("Resend shared cooldown active: {active}; deferred jobs: {deferred}; overdue deferred jobs: {overdue}")),Err(())=>details.push("Resend shared cooldown check failed".into()) }
        }
        if let Some(result)=self.twilio_delayed {
            match result { Ok((pending,expired,blocked))=>details.push(format!("Twilio delayed retries overdue: {pending}; expired leases: {expired}; callback blocked: {blocked}")),Err(())=>details.push("Twilio delayed retry check failed".into()) }
        }
        if let Some(result)=self.fcm_unrecorded_claims {
            match result {Ok(n)=>details.push(format!("FCM jobs with repeated unrecorded claims: {n}")),Err(())=>details.push("FCM unrecorded claims check failed".into())}
        }
        if let Some(result)=self.fcm_cooldown {
            match result {
                Ok((active,deferred,overdue)) => {
                    let suffix = match self.fcm_cooldown_source {
                        Some(Some(429)) => "; deadline extended by HTTP 429",
                        Some(Some(503)) => "; deadline extended by HTTP 503",
                        Some(_) if active => "; deadline source unknown",
                        _ => "",
                    };
                    details.push(format!("FCM shared cooldown active: {active}; deferred jobs: {deferred}; overdue deferred jobs: {overdue}{suffix}"));
                }
                Err(()) => details.push("FCM shared cooldown check failed".into()),
            }
        }
        if let Some(result) = self.fcm_unreviewed {
            match result { Ok(count) => details.push(format!("unreviewed FCM partial jobs: {count}")), Err(()) => details.push("FCM unreviewed check failed".into()) }
        }
        if let Some(result) = self.fcm_partial {
            match result { Ok(count) => details.push(format!("recent FCM partially rejected jobs: {count}")), Err(()) => details.push("FCM partial failure check failed".into()) }
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
        if let Some((name, unhealthy)) = &self.operator_key_probe {
            if name.is_empty() || name.len() > 48 || !name.bytes().all(|c| c.is_ascii_alphanumeric() || b"._-".contains(&c)) {
                return Err(AlertError::InvalidConfiguration);
            }
            details.push(format!("operator key reload {name} probe failed: {unhealthy}"));
        }
        if let Some((name, unhealthy)) = &self.resend_webhook_key_probe {
            if name.is_empty() || name.len()>48 || !name.bytes().all(|c| c.is_ascii_alphanumeric() || b"._-".contains(&c)) {return Err(AlertError::InvalidConfiguration);}
            details.push(format!("Resend webhook key {name} probe failed: {unhealthy}"));
        }
        if let Some((name, unhealthy)) = &self.twilio_webhook_key_probe {
            if name.is_empty() || name.len()>48 || !name.bytes().all(|c| c.is_ascii_alphanumeric() || b"._-".contains(&c)) {return Err(AlertError::InvalidConfiguration);}
            details.push(format!("Twilio webhook key {name} probe failed: {unhealthy}"));
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

/// The provider is selected by the operator; changing it requires a new target ID.
pub enum AlertEmailProvider {
    Resend(Resend),
    Sendgrid(crate::sendgrid::Sendgrid),
    Smtp(crate::smtp_alert::SmtpAlert),
}

pub fn email_from_env() -> DbResult<Option<(String, AlertEmailProvider)>> {
    let to = match std::env::var("SWALLOW_ALERT_EMAIL_TO") {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => return Ok(None),
        Err(_) => return Err("invalid email recipient".into()),
    };
    let provider = match std::env::var("SWALLOW_ALERT_EMAIL_PROVIDER").as_deref() {
        Ok("resend") | Err(std::env::VarError::NotPresent) => AlertEmailProvider::Resend(
            Resend::new(crate::resend::Config {
                api_key: novu_rust_contracts::secret::from_env(
                    "NOVU_RESEND_API_KEY",
                    "NOVU_RESEND_API_KEY_FILE",
                )?
                .ok_or("Resend API key is required")?,
                from: std::env::var("NOVU_RESEND_FROM")?,
                sender_name: std::env::var("NOVU_RESEND_SENDER_NAME").unwrap_or_default(),
            })
            .map_err(|_| "invalid email configuration")?,
        ),
        Ok("sendgrid") => {
            let region = match std::env::var("NOVU_SENDGRID_REGION").as_deref() {
                Ok("global") | Err(std::env::VarError::NotPresent) => {
                    crate::sendgrid::Region::Global
                }
                Ok("eu") => crate::sendgrid::Region::Eu,
                _ => return Err("invalid SendGrid region".into()),
            };
            let mut provider = crate::sendgrid::Sendgrid::new(crate::sendgrid::Config {
                api_key: novu_rust_contracts::secret::from_env(
                    "NOVU_SENDGRID_API_KEY",
                    "NOVU_SENDGRID_API_KEY_FILE",
                )?
                .ok_or("SendGrid API key is required")?,
                from: std::env::var("NOVU_SENDGRID_FROM")?,
                sender_name: std::env::var("NOVU_SENDGRID_SENDER_NAME").unwrap_or_default(),
                region,
            })
            .map_err(|_| "invalid email configuration")?;
            match std::env::var("NOVU_SENDGRID_IP_POOL_NAME") {
                Ok(pool) if pool.len() <= 256 && !pool.contains(['\r', '\n', '\0']) => {
                    provider = provider.with_default_ip_pool(pool);
                }
                Err(std::env::VarError::NotPresent) => {}
                _ => return Err("invalid SendGrid IP pool setting".into()),
            }
            AlertEmailProvider::Sendgrid(provider)
        }
        Ok("smtp") => AlertEmailProvider::Smtp(crate::smtp_alert::SmtpAlert::from_env().map_err(|_| "invalid SMTP configuration")?),
        _ => return Err("invalid email alert provider".into()),
    };
    if matches!(&provider, AlertEmailProvider::Smtp(_)) && (to.len()>320 || to.contains(['\r','\n','\0','<','>']) || to.parse::<lettre::Address>().is_err()) {
        return Err("invalid SMTP recipient".into());
    }
    Ok(Some((to, provider)))
}
/// The same secret must be configured on every cooperating sender.
pub fn binding_key_from_env() -> DbResult<Option<ring::hmac::Key>> {
    let value = novu_rust_contracts::secret::from_env(
        "SWALLOW_ALERT_BINDING_KEY",
        "SWALLOW_ALERT_BINDING_KEY_FILE",
    )?;
    match value {
        Some(value) if value.len() >= 32 => Ok(Some(ring::hmac::Key::new(
            ring::hmac::HMAC_SHA256,
            value.as_bytes(),
        ))),
        Some(_) => Err("alert binding key must contain at least 32 bytes".into()),
        None => Ok(None),
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EmailBindingProfile {
    Destination,
    Configuration,
}

pub fn email_binding_profile_from_env(key_present: bool) -> DbResult<EmailBindingProfile> {
    match std::env::var("SWALLOW_ALERT_EMAIL_BINDING_PROFILE").as_deref() {
        Ok("destination") | Err(std::env::VarError::NotPresent) => {
            Ok(EmailBindingProfile::Destination)
        }
        Ok("configuration") if key_present => Ok(EmailBindingProfile::Configuration),
        Ok("configuration") => Err("email configuration binding requires a binding key".into()),
        _ => Err("invalid email binding profile".into()),
    }
}
fn destination_fingerprint(
    key: &ring::hmac::Key,
    channel: &str,
    provider: &str,
    destination: &str,
) -> String {
    let mut context = ring::hmac::Context::with_key(key);
    for part in ["swallow-alert-binding-v1", channel, provider, destination] {
        context.update(&(part.len() as u64).to_be_bytes());
        context.update(part.as_bytes());
    }
    context
        .sign()
        .as_ref()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}
pub async fn bind_slack_destination(
    db: &PgClient,
    environment: &str,
    target: &str,
    key: &ring::hmac::Key,
    provider: &SlackWebhook,
) -> DbResult<bool> {
    bind_destination(
        db,
        environment,
        "slack",
        target,
        &destination_fingerprint(key, "slack", "webhook", provider.endpoint.as_str()),
    )
    .await
}
pub async fn bind_email_destination(
    db: &PgClient,
    environment: &str,
    target: &str,
    key: &ring::hmac::Key,
    provider: &AlertEmailProvider,
    recipient: &str,
    profile: EmailBindingProfile,
) -> DbResult<bool> {
    if recipient.is_empty()
        || recipient.len() > 320
        || recipient.contains(['\r', '\n', '<', '>'])
        || !recipient.contains('@')
    {
        return Err("invalid alert recipient".into());
    }
    let name = match provider {
        AlertEmailProvider::Resend(_) => "resend",
        AlertEmailProvider::Sendgrid(_) => "sendgrid",
        AlertEmailProvider::Smtp(_) => "smtp",
    };
    let fingerprint = match profile {
        EmailBindingProfile::Destination => destination_fingerprint(key, "email", name, recipient),
        EmailBindingProfile::Configuration => {
            let mut hmac = ring::hmac::Context::with_key(key);
            let mut add = |value: &str| {
                hmac.update(&(value.len() as u64).to_be_bytes());
                hmac.update(value.as_bytes());
            };
            for value in ["swallow-alert-email-configuration-v2", name, recipient] {
                add(value);
            }
            match provider {
                AlertEmailProvider::Resend(provider) => {
                    for part in provider.binding_parts() { add(part); }
                }
                AlertEmailProvider::Sendgrid(provider) => {
                    for part in provider.binding_parts() { add(part); }
                }
                AlertEmailProvider::Smtp(provider) => {
                    for part in provider.binding_parts() { add(part); }
                }
            }
            hmac.sign().as_ref().iter().map(|b| format!("{b:02x}")).collect()
        }
    };
    bind_destination(
        db,
        environment,
        "email",
        target,
        &fingerprint,
    )
    .await
}
async fn bind_destination(
    db: &PgClient,
    environment: &str,
    channel: &str,
    target: &str,
    fingerprint: &str,
) -> DbResult<bool> {
    Ok(db
        .query_one(
            "SELECT public.novu_rust_bind_alert_destination($1,$2,$3,$4)",
            &[&environment, &channel, &target, &fingerprint],
        )
        .await?
        .get(0))
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
            crate::resend::SendError::RateLimited(_) => AlertError::Rejected(429),
            crate::resend::SendError::Provider(code)
                if (400..500).contains(&code) && code != 408 && code != 409 =>
            {
                AlertError::Rejected(code)
            }
            _ => AlertError::Unknown,
        })
}

async fn send_selected_email(
    provider: &AlertEmailProvider,
    recipient: &str,
    alert: &Alert,
    idempotency_key: &str,
) -> Result<(), AlertError> {
    match provider {
        AlertEmailProvider::Resend(provider) => {
            send_email(provider, recipient, alert, idempotency_key).await
        }
        AlertEmailProvider::Smtp(provider) => {
            if recipient.is_empty() || recipient.len()>320 || recipient.contains(['\r','\n','<','>']) || !recipient.contains('@') { return Err(AlertError::InvalidConfiguration); }
            let text=alert.text()?;
            provider.send(recipient,text).await.map_err(|_| AlertError::Unknown)
        }
        AlertEmailProvider::Sendgrid(provider) => {
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
                .send(&email)
                .await
                .map(|_| ())
                .map_err(|error| match error {
                    crate::sendgrid::SendError::InvalidConfiguration
                    | crate::sendgrid::SendError::InvalidMessage => {
                        AlertError::InvalidConfiguration
                    }
                    crate::sendgrid::SendError::Provider(code)
                        if (400..500).contains(&code) && code != 408 && code != 409 =>
                    {
                        AlertError::Rejected(code)
                    }
                    _ => AlertError::Unknown,
                })
        }
    }
}

pub async fn send_uncoordinated_selected(
    alert: &Alert,
    slack: Option<&SlackWebhook>,
    email: Option<(&AlertEmailProvider, &str)>,
    timeout: Duration,
    parallel: bool,
) -> (
    Option<Result<(), AlertError>>,
    Option<Result<(), AlertError>>,
) {
    let key = format!("swallow-db-fallback/{}", Uuid::new_v4());
    let slack_send = async {
        if let Some(provider) = slack {
            Some(
                tokio::time::timeout(timeout, provider.send(alert))
                    .await
                    .unwrap_or(Err(AlertError::Unknown)),
            )
        } else {
            None
        }
    };
    let email_send = async {
        if let Some((provider, to)) = email {
            Some(
                tokio::time::timeout(timeout, send_selected_email(provider, to, alert, &key))
                    .await
                    .unwrap_or(Err(AlertError::Unknown)),
            )
        } else {
            None
        }
    };
    if parallel {
        tokio::join!(slack_send, email_send)
    } else {
        (slack_send.await, email_send.await)
    }
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

pub struct DispatchPair<'a> {
    pub alert: &'a Alert,
    pub slack: Option<(&'a SlackWebhook, &'a str)>,
    pub email: Option<(&'a AlertEmailProvider, &'a str, &'a str)>,
    pub cooldown: i32,
    pub on_new: bool,
    pub scoped: bool,
    pub timeout: Duration,
}

type DispatchOutcome = Option<DbResult<Dispatch>>;

/// Each destination gets its own reservation and timeout. If a second client
/// is provided, the two operations can overlap without sharing a transaction.
pub async fn dispatch_pair(
    db: &mut PgClient,
    other: Option<&mut PgClient>,
    cfg: DispatchPair<'_>,
) -> (DispatchOutcome, DispatchOutcome) {
    async fn slack(db: &mut PgClient, cfg: &DispatchPair<'_>) -> DispatchOutcome {
        let (provider, target) = cfg.slack?;
        Some(
            tokio::time::timeout(
                cfg.timeout,
                dispatch_slack_with_options(
                    db,
                    target,
                    cfg.cooldown,
                    cfg.alert,
                    provider,
                    cfg.on_new,
                    cfg.scoped,
                ),
            )
            .await
            .unwrap_or_else(|_| Err("Slack dispatch timed out".into())),
        )
    }
    async fn email(db: &mut PgClient, cfg: &DispatchPair<'_>) -> DispatchOutcome {
        let (provider, target, recipient) = cfg.email?;
        Some(
            tokio::time::timeout(
                cfg.timeout,
                dispatch_selected_email_with_options(
                    db,
                    target,
                    cfg.cooldown,
                    cfg.alert,
                    provider,
                    recipient,
                    (cfg.on_new, cfg.scoped),
                ),
            )
            .await
            .unwrap_or_else(|_| Err("email dispatch timed out".into())),
        )
    }
    if let Some(other) = other {
        tokio::join!(slack(db, &cfg), email(other, &cfg))
    } else {
        (slack(db, &cfg).await, email(db, &cfg).await)
    }
}

/// Versioned scoped dispatch. Call only with all senders for a target using V2.
pub async fn dispatch_pair_v2(db: &mut PgClient, other: Option<&mut PgClient>, cfg: DispatchPair<'_>) -> (DispatchOutcome, DispatchOutcome) {
    async fn dispatch_v2<F, Fut>(db: &mut PgClient, cfg: &DispatchPair<'_>, channel: &str, target: &str, send: F) -> DbResult<Dispatch>
    where F: FnOnce(Uuid) -> Fut, Fut: std::future::Future<Output=Result<(), AlertError>> {
        let mask = cfg.alert.condition_mask_v2();
        let row = db.query_one("SELECT result,attempt_id FROM public.novu_rust_reserve_alert_v2($1,$2,$3,$4,$5)", &[&cfg.alert.environment,&channel,&target,&cfg.cooldown,&mask]).await?;
        let result: String = row.get(0); let id: Option<Uuid> = row.get(1);
        match (result.as_str(), id) {
            ("cooldown",None) => Ok(Dispatch::CoolingDown), ("inflight",None) => Ok(Dispatch::InFlight),
            ("blocked",None) => Ok(Dispatch::NeedsReview), ("claimed",Some(id)) => {
                let result = send(id).await;
                let recorded = finish_scoped(db,&cfg.alert.environment,channel,target,id,&result).await?;
                if result.is_ok() && recorded { Ok(Dispatch::Accepted) } else { Ok(Dispatch::NeedsReview) }
            }, _ => Err("invalid v2 reservation result".into())
        }
    }
    async fn slack(db: &mut PgClient,cfg: &DispatchPair<'_>) -> DispatchOutcome {
        let (provider,target)=cfg.slack?;
        Some(tokio::time::timeout(cfg.timeout,dispatch_v2(db,cfg,"slack",target,|_|provider.send(cfg.alert))).await.unwrap_or_else(|_|Err("Slack dispatch timed out".into())))
    }
    async fn email(db: &mut PgClient,cfg: &DispatchPair<'_>) -> DispatchOutcome {
        let (provider,target,recipient)=cfg.email?;
        if recipient.is_empty() || recipient.len() > 320 || recipient.contains(['\r', '\n', '<', '>']) || !recipient.contains('@') { return Some(Err("invalid alert recipient".into())); }
        Some(tokio::time::timeout(cfg.timeout,dispatch_v2(db,cfg,"email",target,|id|async move { send_selected_email(provider,recipient,cfg.alert,&format!("swallow-alert/{id}")).await })).await.unwrap_or_else(|_|Err("email dispatch timed out".into())))
    }
    if let Some(other)=other { tokio::join!(slack(db,&cfg),email(other,&cfg)) } else { (slack(db,&cfg).await,email(db,&cfg).await) }
}

pub fn parallel_delivery_from_env() -> DbResult<bool> {
    match std::env::var("SWALLOW_ALERT_DELIVERY_POLICY").as_deref() {
        Ok("parallel") => Ok(true),
        Ok("sequential") | Err(std::env::VarError::NotPresent) => Ok(false),
        _ => Err("invalid alert delivery policy".into()),
    }
}

/// Connect before starting either send; callers can safely fall back to a
/// sequential dispatch if this fails. The connection driver exits on drop.
pub async fn additional_delivery_connection(
    config: &tokio_postgres::Config,
    tls: novu_rust_contracts::postgres::MakeRustlsConnect,
    timeout: Duration,
) -> DbResult<PgClient> {
    let (client, driver) = tokio::time::timeout(timeout, config.connect(tls)).await??;
    tokio::spawn(async move {
        let _ = driver.await;
    });
    Ok(client)
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

pub async fn dispatch_selected_email_with_options(
    db: &mut PgClient,
    target: &str,
    cooldown: i32,
    alert: &Alert,
    provider: &AlertEmailProvider,
    recipient: &str,
    policy: (bool, bool),
) -> DbResult<Dispatch> {
    alert.text().map_err(|_| "invalid alert environment")?;
    if recipient.is_empty()
        || recipient.len() > 320
        || recipient.contains(['\r', '\n', '<', '>'])
        || !recipient.contains('@')
    {
        return Err("invalid alert recipient".into());
    }
    dispatch(
        db,
        &alert.environment,
        "email",
        target,
        cooldown,
        (policy.0.then(|| alert.condition_mask()), policy.1),
        |id| async move {
            send_selected_email(provider, recipient, alert, &format!("swallow-alert/{id}")).await
        },
    )
    .await
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
            sendgrid_webhook_keys: None,
            fcm_credentials: None,
            slack_attempts: None,
            slack_policy_cancelled: None,
            discord_health: None,
            discord_retry_health: None,
            email_policy_cancelled: None,
            sms_policy_cancelled: None,
            push_policy_cancelled: None,
            channel_policy_check_failed: false,
            failed_checks: [false; 5],
            resend_auto: None,
            slack_cooldown: None,
            resend_cooldown: None,
            twilio_delayed: None,
            fcm_partial: None,
            fcm_unreviewed: None,
            fcm_unrecorded_claims: None,
            fcm_cooldown: None,
            fcm_cooldown_source: None,
            fcm_cooldown_distinct_source: false,
            failed_jobs: None,
            failed_jobs_check_failed: false,
            database_probe: None,
            operator_key_probe: None,
            resend_webhook_key_probe: None,
            twilio_webhook_key_probe: None,
            alert_store_unavailable: false,
        }
    }

    #[test]
    fn resend_automatic_retry_health_shares_failed_job_cooldown_safely() {
        let mut a = alert();
        a.resend_enabled = false;
        a.resend_auto = Some(Ok((0, 0, 0, 0)));
        assert_eq!(a.condition_mask() & (1 << 18), 0);
        assert_ne!(a.observed_condition_mask() & (1 << 18), 0);
        a.resend_auto = Some(Ok((0, 0, 1, 0)));
        assert_ne!(a.condition_mask() & (1 << 18), 0);
        assert!(a.text().unwrap().contains("guard deferred: 1"));
        a.failed_jobs = Some(0);
        a.resend_auto = Some(Err(()));
        assert_ne!(a.condition_mask() & (1 << 19), 0);
        assert_eq!(a.observed_condition_mask() & (1 << 18), 0);
        a.resend_auto = Some(Ok((0, 0, 0, 0)));
        a.failed_jobs_check_failed = true;
        assert_eq!(a.observed_condition_mask() & (1 << 18), 0);
    }

    #[tokio::test]
    async fn resend_automatic_retry_health_reaches_slack_payload() {
        let mut a = alert();
        a.resend_enabled = false;
        a.resend_auto = Some(Ok((2, 1, 3, 4)));
        let (url, server) = mock_server("200 OK", "ok").await;
        let slack = SlackWebhook::with_endpoint(url).unwrap();
        let (email_url, email_server) = mock_server("200 OK", r#"{"id":"mock-email"}"#).await;
        let email = AlertEmailProvider::Resend(
            crate::resend::Resend::test_endpoint(
                crate::resend::Config {
                    api_key: "re_test".into(),
                    from: "sender@example.test".into(),
                    sender_name: String::new(),
                },
                email_url.as_str(),
            )
            .unwrap(),
        );
        let result = send_uncoordinated_selected(
            &a,
            Some(&slack),
            Some((&email, "ops@example.test")),
            Duration::from_secs(2),
            false,
        )
        .await;
        assert_eq!(result.0, Some(Ok(())));
        assert_eq!(result.1, Some(Ok(())));
        let request = server.await.unwrap();
        let email_request = email_server.await.unwrap();
        for payload in [request, email_request] {
            assert!(payload.contains("Resend 429 aged: 2"));
            assert!(payload.contains("retries exhausted: 1"));
            assert!(payload.contains("guard deferred: 3"));
            assert!(payload.contains("automatic retries overdue: 4"));
        }
    }

    #[tokio::test]
    #[ignore = "requires fully migrated disposable PostgreSQL"]
    async fn v2_dispatch_new_condition_bypasses_cooldown_with_mock_slack() {
        let url=std::env::var("TEST_DATABASE_URL").unwrap();
        let (mut db,driver)=tokio_postgres::connect(&url,tokio_postgres::NoTls).await.unwrap();
        tokio::spawn(async move {let _=driver.await;});
        let env=format!("alert-v2-mock-{}",uuid::Uuid::new_v4());
        db.execute("INSERT INTO public.novu_rust_environments VALUES($1,'org')",&[&env]).await.unwrap();
        db.execute("INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT $1,rolname,'alert_send',oid FROM pg_roles WHERE rolname=current_user",&[&env]).await.unwrap();
        let mut a=alert();a.environment=env;a.resend_enabled=false;a.failed_jobs=Some(1);
        let (url,server)=mock_server("200 OK","ok").await;let slack=SlackWebhook::with_endpoint(url).unwrap();
        let run=|a: &Alert| a.condition_mask_v2();assert_eq!(run(&a),1<<18);
        let cfg=DispatchPair {alert:&a,slack:Some((&slack,"ops")),email:None,cooldown:60,on_new:true,scoped:true,timeout:Duration::from_secs(3)};
        assert_eq!(dispatch_pair_v2(&mut db,None,cfg).await.0.unwrap().unwrap(),Dispatch::Accepted);server.await.unwrap();
        a.slack_cooldown=Some(Ok((true,0,0)));
        let (url,server)=mock_server("200 OK","ok").await;let slack=SlackWebhook::with_endpoint(url).unwrap();
        let cfg=DispatchPair {alert:&a,slack:Some((&slack,"ops")),email:None,cooldown:60,on_new:true,scoped:true,timeout:Duration::from_secs(3)};
        assert_eq!(dispatch_pair_v2(&mut db,None,cfg).await.0.unwrap().unwrap(),Dispatch::Accepted);server.await.unwrap();
        let cfg=DispatchPair {alert:&a,slack:Some((&slack,"ops")),email:None,cooldown:60,on_new:true,scoped:true,timeout:Duration::from_secs(3)};
        assert_eq!(dispatch_pair_v2(&mut db,None,cfg).await.0.unwrap().unwrap(),Dispatch::CoolingDown);
    }
    #[tokio::test]
    #[ignore = "requires fully migrated disposable PostgreSQL and role creation"]
    async fn fcm_source_change_sends_once_and_recovery_allows_recurrence() {
        let url=std::env::var("TEST_DATABASE_URL").unwrap();
        let (admin,driver)=tokio_postgres::connect(&url,tokio_postgres::NoTls).await.unwrap();tokio::spawn(async move {let _=driver.await;});
        let env=format!("source-alert-{}",uuid::Uuid::new_v4());let role=format!("source_alert_{}",uuid::Uuid::new_v4().simple());
        admin.execute("INSERT INTO public.novu_rust_environments VALUES($1,'org')", &[&env]).await.unwrap();
        admin.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_reserve_alert_v2(text,text,text,integer,bigint),public.novu_rust_observe_alert_conditions_v2(text,text,text,bigint,bigint),public.novu_rust_finish_alert(text,text,text,uuid,text,integer) TO {role}")).await.unwrap();
        admin.execute("INSERT INTO public.novu_rust_delivery_operator_grants SELECT $1,$2,'alert_send',oid FROM pg_roles WHERE rolname::text=$2", &[&env,&role]).await.unwrap();
        let (mut db,driver)=tokio_postgres::connect(&url,tokio_postgres::NoTls).await.unwrap();tokio::spawn(async move {let _=driver.await;});
        db.batch_execute(&format!("SET SESSION AUTHORIZATION {role}")).await.unwrap();
        let mut a=alert();a.environment=env.clone();a.resend_enabled=false;a.fcm_cooldown=Some(Ok((true,0,0)));a.fcm_cooldown_distinct_source=true;a.fcm_cooldown_source=Some(Some(429));
        let (endpoint,server)=mock_server("200 OK","ok").await;let slack=SlackWebhook::with_endpoint(endpoint).unwrap();
        let cfg=DispatchPair{alert:&a,slack:Some((&slack,"ops")),email:None,cooldown:3600,on_new:true,scoped:true,timeout:Duration::from_secs(3)};
        assert_eq!(dispatch_pair_v2(&mut db,None,cfg).await.0.unwrap().unwrap(),Dispatch::Accepted);assert!(server.await.unwrap().contains("HTTP 429"));
        a.fcm_cooldown_source=Some(Some(503));
        db.query_one("SELECT public.novu_rust_observe_alert_conditions_v2($1,'slack','ops',$2,$3)", &[&env,&a.condition_mask_v2(),&a.observed_condition_mask_v2()]).await.unwrap();
        let (endpoint,server)=mock_server("200 OK","ok").await;let slack=SlackWebhook::with_endpoint(endpoint).unwrap();
        let cfg=DispatchPair{alert:&a,slack:Some((&slack,"ops")),email:None,cooldown:3600,on_new:true,scoped:true,timeout:Duration::from_secs(3)};
        assert_eq!(dispatch_pair_v2(&mut db,None,cfg).await.0.unwrap().unwrap(),Dispatch::Accepted);assert!(server.await.unwrap().contains("HTTP 503"));
        let cfg=DispatchPair{alert:&a,slack:Some((&slack,"ops")),email:None,cooldown:3600,on_new:true,scoped:true,timeout:Duration::from_secs(3)};
        assert_eq!(dispatch_pair_v2(&mut db,None,cfg).await.0.unwrap().unwrap(),Dispatch::CoolingDown);
        a.fcm_cooldown_source=Some(Some(429));
        db.query_one("SELECT public.novu_rust_observe_alert_conditions_v2($1,'slack','ops',$2,$3)", &[&env,&a.condition_mask_v2(),&a.observed_condition_mask_v2()]).await.unwrap();
        let (endpoint,server)=mock_server("200 OK","ok").await;let slack=SlackWebhook::with_endpoint(endpoint).unwrap();
        let cfg=DispatchPair{alert:&a,slack:Some((&slack,"ops")),email:None,cooldown:3600,on_new:true,scoped:true,timeout:Duration::from_secs(3)};
        assert_eq!(dispatch_pair_v2(&mut db,None,cfg).await.0.unwrap().unwrap(),Dispatch::Accepted);assert!(server.await.unwrap().contains("HTTP 429"));
        assert_eq!(admin.query_one("SELECT count(*) FROM public.novu_rust_alert_delivery_attempts WHERE environment_id=$1", &[&env]).await.unwrap().get::<_,i64>(0),3);
    }
    #[tokio::test]
    async fn twilio_delayed_health_has_independent_v2_bits_and_reaches_both_channels() {
        let mut a=alert();a.resend_enabled=false;a.twilio_delayed=Some(Ok((2,1,3)));
        assert_eq!(a.condition_mask_v2(),1_i64<<36);assert_ne!(a.observed_condition_mask_v2()&(1_i64<<36),0);
        let (url,server)=mock_server("200 OK","ok").await;let slack=SlackWebhook::with_endpoint(url).unwrap();
        let (email_url,email_server)=mock_server("200 OK",r#"{"id":"mock-email"}"#).await;
        let email=AlertEmailProvider::Resend(crate::resend::Resend::test_endpoint(crate::resend::Config{api_key:"re_test".into(),from:"sender@example.test".into(),sender_name:String::new()},email_url.as_str()).unwrap());
        let result=send_uncoordinated_selected(&a,Some(&slack),Some((&email,"ops@example.test")),Duration::from_secs(2),false).await;
        assert_eq!(result.0,Some(Ok(())));assert_eq!(result.1,Some(Ok(())));
        for payload in [server.await.unwrap(),email_server.await.unwrap()] {assert!(payload.contains("Twilio delayed retries overdue: 2; expired leases: 1; callback blocked: 3"));}
        a.twilio_delayed=Some(Err(()));assert_eq!(a.observed_condition_mask_v2()&(1_i64<<36),0);assert_ne!(a.condition_mask_v2()&(1_i64<<37),0);
    }
    #[tokio::test]
    async fn discord_retry_overdue_is_optional_and_reaches_both_destinations() {
        let mut a=alert();a.resend_enabled=false;a.discord_retry_health=Some(Ok((3,0)));
        assert_eq!(a.condition_mask_v2(),0);assert_ne!(a.observed_condition_mask_v2()&(1_i64<<56),0);
        a.discord_retry_health=Some(Ok((3,1)));assert_eq!(a.condition_mask_v2(),1_i64<<56);
        let (url,server)=mock_server("200 OK","ok").await;let slack=SlackWebhook::with_endpoint(url).unwrap();
        let (email_url,email_server)=mock_server("200 OK",r#"{"id":"mock-email"}"#).await;
        let email=AlertEmailProvider::Resend(crate::resend::Resend::test_endpoint(crate::resend::Config{api_key:"re_test".into(),from:"sender@example.test".into(),sender_name:String::new()},email_url.as_str()).unwrap());
        let sent=send_uncoordinated_selected(&a,Some(&slack),Some((&email,"ops@example.test")),Duration::from_secs(2),false).await;
        assert_eq!(sent.0,Some(Ok(())));assert_eq!(sent.1,Some(Ok(())));
        for payload in [server.await.unwrap(),email_server.await.unwrap()] {assert!(payload.contains("Discord retry pending: 3; overdue: 1"));}
        a.discord_retry_health=Some(Err(()));assert_eq!(a.condition_mask_v2(),1_i64<<57);assert_eq!(a.observed_condition_mask_v2()&(1_i64<<56),0);
    }
    #[tokio::test]
    async fn discord_health_uses_independent_bits_and_both_destinations() {
        let mut a=alert(); a.resend_enabled=false; a.discord_health=Some(Ok((1,2,3,4)));
        assert_eq!(a.condition_mask_v2(),(1_i64<<51)|(1_i64<<52)|(1_i64<<53)|(1_i64<<54));
        let (url,server)=mock_server("200 OK","ok").await; let slack=SlackWebhook::with_endpoint(url).unwrap();
        let (email_url,email_server)=mock_server("200 OK",r#"{"id":"mock-email"}"#).await;
        let email=AlertEmailProvider::Resend(crate::resend::Resend::test_endpoint(crate::resend::Config{api_key:"re_test".into(),from:"sender@example.test".into(),sender_name:String::new()},email_url.as_str()).unwrap());
        let sent=send_uncoordinated_selected(&a,Some(&slack),Some((&email,"ops@example.test")),Duration::from_secs(2),false).await;
        assert_eq!(sent.0,Some(Ok(()))); assert_eq!(sent.1,Some(Ok(())));
        for payload in [server.await.unwrap(),email_server.await.unwrap()] {assert!(payload.contains("Discord unknown attempts: 1; rejected attempts: 2; stranded dispatches: 3; pre-dispatch failures: 4"));}
        a.discord_health=Some(Err(()));
        assert_eq!(a.condition_mask_v2(),1_i64<<55);
        assert_eq!(a.observed_condition_mask_v2()&(1_i64<<51),0);
        assert_ne!(a.observed_condition_mask_v2()&(1_i64<<55),0);
    }
    #[tokio::test]
    async fn fcm_partial_alert_uses_independent_bits_and_both_destinations() {
        let mut a=alert(); a.resend_enabled=false; a.fcm_partial=Some(Ok(2));
        assert_eq!(a.condition_mask_v2(),1_i64<<38);
        assert_ne!(a.observed_condition_mask_v2() & (1_i64<<38),0);
        let (url,server)=mock_server("200 OK","ok").await; let slack=SlackWebhook::with_endpoint(url).unwrap();
        let (email_url,email_server)=mock_server("200 OK",r#"{"id":"mock-email"}"#).await;
        let email=AlertEmailProvider::Resend(crate::resend::Resend::test_endpoint(crate::resend::Config{api_key:"re_test".into(),from:"sender@example.test".into(),sender_name:String::new()},email_url.as_str()).unwrap());
        let sent=send_uncoordinated_selected(&a,Some(&slack),Some((&email,"ops@example.test")),Duration::from_secs(2),false).await;
        assert_eq!(sent.0,Some(Ok(()))); assert_eq!(sent.1,Some(Ok(())));
        for payload in [server.await.unwrap(),email_server.await.unwrap()] { assert!(payload.contains("recent FCM partially rejected jobs: 2")); }
        a.fcm_partial=Some(Err(())); assert_eq!(a.observed_condition_mask_v2() & (1_i64<<38),0); assert_ne!(a.condition_mask_v2() & (1_i64<<39),0);
    }
    #[tokio::test]
    async fn fcm_unrecorded_claims_alert_has_separate_conditions_and_destinations() {
        let mut a=alert(); a.resend_enabled=false; a.fcm_unrecorded_claims=Some(Ok(3));
        assert_eq!(a.condition_mask_v2(),1_i64<<49);assert_ne!(a.observed_condition_mask_v2()&(1_i64<<49),0);
        let (url,server)=mock_server("200 OK","ok").await;let slack=SlackWebhook::with_endpoint(url).unwrap();
        let (email_url,email_server)=mock_server("200 OK",r#"{"id":"mock-email"}"#).await;
        let email=AlertEmailProvider::Resend(crate::resend::Resend::test_endpoint(crate::resend::Config{api_key:"re_test".into(),from:"sender@example.test".into(),sender_name:String::new()},email_url.as_str()).unwrap());
        let sent=send_uncoordinated_selected(&a,Some(&slack),Some((&email,"ops@example.test")),Duration::from_secs(2),false).await;
        assert_eq!(sent.0,Some(Ok(())));assert_eq!(sent.1,Some(Ok(())));
        for payload in [server.await.unwrap(),email_server.await.unwrap()] {assert!(payload.contains("FCM jobs with repeated unrecorded claims: 3"));}
        a.fcm_unrecorded_claims=Some(Err(()));assert_eq!(a.observed_condition_mask_v2()&(1_i64<<49),0);assert_ne!(a.condition_mask_v2()&(1_i64<<50),0);
    }
    #[tokio::test]
    async fn fcm_unreviewed_alert_uses_separate_v2_conditions_and_destinations() {
        let mut a=alert(); a.resend_enabled=false; a.fcm_unreviewed=Some(Ok(3));
        assert_eq!(a.condition_mask_v2(),1_i64<<40);assert_ne!(a.observed_condition_mask_v2()&(1_i64<<40),0);
        let (url,server)=mock_server("200 OK","ok").await;let slack=SlackWebhook::with_endpoint(url).unwrap();
        let (email_url,email_server)=mock_server("200 OK",r#"{"id":"mock-email"}"#).await;
        let email=AlertEmailProvider::Resend(crate::resend::Resend::test_endpoint(crate::resend::Config{api_key:"re_test".into(),from:"sender@example.test".into(),sender_name:String::new()},email_url.as_str()).unwrap());
        let sent=send_uncoordinated_selected(&a,Some(&slack),Some((&email,"ops@example.test")),Duration::from_secs(2),false).await;
        assert_eq!(sent.0,Some(Ok(())));assert_eq!(sent.1,Some(Ok(())));
        for payload in [server.await.unwrap(),email_server.await.unwrap()] {assert!(payload.contains("unreviewed FCM partial jobs: 3"));}
        a.fcm_unreviewed=Some(Err(()));assert_eq!(a.observed_condition_mask_v2()&(1_i64<<40),0);assert_ne!(a.condition_mask_v2()&(1_i64<<41),0);
    }
    #[tokio::test]
    async fn fcm_cooldown_alert_has_independent_v2_bits_and_both_destinations() {
        let mut a=alert();a.resend_enabled=false;a.fcm_cooldown=Some(Ok((true,2,1)));a.fcm_cooldown_source=Some(Some(503));
        assert_eq!(a.condition_mask_v2(),1_i64<<42);assert_ne!(a.observed_condition_mask_v2()&(1_i64<<42),0);
        a.fcm_cooldown_source=Some(None);assert!(a.text().unwrap().contains("deadline source unknown"));
        a.fcm_cooldown_source=Some(Some(429));assert!(a.text().unwrap().contains("deadline extended by HTTP 429"));
        a.fcm_cooldown_source=Some(Some(503));
        let (url,server)=mock_server("200 OK","ok").await;let slack=SlackWebhook::with_endpoint(url).unwrap();
        let (email_url,email_server)=mock_server("200 OK",r#"{"id":"mock-email"}"#).await;
        let email=AlertEmailProvider::Resend(crate::resend::Resend::test_endpoint(crate::resend::Config{api_key:"re_test".into(),from:"sender@example.test".into(),sender_name:String::new()},email_url.as_str()).unwrap());
        let sent=send_uncoordinated_selected(&a,Some(&slack),Some((&email,"ops@example.test")),Duration::from_secs(2),false).await;
        assert_eq!(sent.0,Some(Ok(())));assert_eq!(sent.1,Some(Ok(())));
        for payload in [server.await.unwrap(),email_server.await.unwrap()] {assert!(payload.contains("FCM shared cooldown active: true; deferred jobs: 2; overdue deferred jobs: 1; deadline extended by HTTP 503"));}
        a.fcm_cooldown=Some(Err(()));assert_eq!(a.observed_condition_mask_v2()&(1_i64<<42),0);assert_ne!(a.condition_mask_v2()&(1_i64<<43),0);
    }
    #[test]
    fn fcm_source_conditions_track_changes_and_preserve_failed_observations() {
        let mut a=alert(); a.resend_enabled=false; a.fcm_cooldown=Some(Ok((true,0,0)));
        a.fcm_cooldown_distinct_source=true; a.fcm_cooldown_source=Some(Some(429));
        let all=(1_i64<<44)|(1_i64<<45)|(1_i64<<46);
        assert_eq!(a.condition_mask_v2() & all,1_i64<<44);
        assert_eq!(a.observed_condition_mask_v2() & all,all);
        a.fcm_cooldown_source=Some(Some(503));assert_eq!(a.condition_mask_v2() & all,1_i64<<45);
        a.fcm_cooldown_source=Some(None);assert_eq!(a.condition_mask_v2() & all,1_i64<<46);
        a.fcm_cooldown=Some(Err(()));assert_eq!(a.condition_mask_v2() & all,0);assert_eq!(a.observed_condition_mask_v2() & all,0);
        a.fcm_cooldown=Some(Ok((false,0,0)));assert_eq!(a.condition_mask_v2() & all,0);assert_eq!(a.observed_condition_mask_v2() & all,all);
        a.fcm_cooldown_distinct_source=false;assert_eq!(a.observed_condition_mask_v2() & all,0);
    }
    #[test]
    fn v2_independent_shared_checks_preserve_unknown_and_sign_bit() {
        let mut a=alert(); a.resend_enabled=false; a.failed_jobs=Some(1); a.resend_auto=Some(Ok((0,0,0,0))); a.slack_cooldown=Some(Ok((false,0,0)));
        let first=a.condition_mask_v2(); assert_eq!(first,1_i64<<18);
        a.resend_auto=Some(Ok((1,0,0,0))); assert_eq!(a.condition_mask_v2() ^ first,1_i64<<32);
        a.slack_cooldown=Some(Ok((true,0,0))); assert_ne!(a.condition_mask_v2() & (1_i64<<34),0);
        a.resend_auto=Some(Err(())); assert_eq!(a.observed_condition_mask_v2() & (1_i64<<32),0); assert_ne!(a.condition_mask_v2() & (1_i64<<33),0); assert_ne!(a.observed_condition_mask_v2() & (1_i64<<34),0);
        a.fcm_credentials=Some(Err(())); assert_ne!(a.condition_mask_v2() & (1_i64<<31),0); assert_eq!(a.condition_mask_v2() & !a.observed_condition_mask_v2(),0);
    }
    #[test]
    fn slack_cooldown_health_shares_failed_job_bits_without_clearing_failed_checks() {
        let mut a=alert();a.resend_enabled=false;a.slack_cooldown=Some(Ok((false,0,0)));
        assert_eq!(a.condition_mask()&(1<<18),0);assert_ne!(a.observed_condition_mask()&(1<<18),0);
        a.slack_cooldown=Some(Ok((true,0,0)));assert_ne!(a.condition_mask()&(1<<18),0);
        a.slack_cooldown=Some(Ok((false,2,1)));assert!(a.text().unwrap().contains("deferred jobs: 2; overdue deferred jobs: 1"));
        a.failed_jobs=Some(0);a.slack_cooldown=Some(Err(()));assert_ne!(a.condition_mask()&(1<<19),0);assert_eq!(a.observed_condition_mask()&(1<<18),0);
        a.slack_cooldown=Some(Ok((false,0,0)));a.resend_auto=Some(Err(()));assert_eq!(a.observed_condition_mask()&(1<<18),0);
    }
    #[tokio::test]
    async fn slack_cooldown_reaches_slack_and_resend_alert_payloads() {
        let mut a=alert();a.resend_enabled=false;a.slack_cooldown=Some(Ok((true,2,1)));
        let (url,server)=mock_server("200 OK","ok").await;let slack=SlackWebhook::with_endpoint(url).unwrap();
        let (email_url,email_server)=mock_server("200 OK",r#"{"id":"mock-email"}"#).await;
        let email=AlertEmailProvider::Resend(crate::resend::Resend::test_endpoint(crate::resend::Config{api_key:"re_test".into(),from:"sender@example.test".into(),sender_name:String::new()},email_url.as_str()).unwrap());
        let result=send_uncoordinated_selected(&a,Some(&slack),Some((&email,"ops@example.test")),Duration::from_secs(2),false).await;
        assert_eq!(result.0,Some(Ok(())));assert_eq!(result.1,Some(Ok(())));
        for payload in [server.await.unwrap(),email_server.await.unwrap()]{assert!(payload.contains("Slack shared cooldown active: true"));assert!(payload.contains("deferred jobs: 2"));assert!(payload.contains("overdue deferred jobs: 1"));}
    }
    #[test]
    fn sendgrid_webhook_key_health_is_optional_and_independent() {
        let mut a = alert();
        assert_eq!(a.observed_condition_mask() & (1 << 30), 0);
        for (value, unhealthy) in [
            (Ok(0), true),
            (Ok(1), false),
            (Ok(10), false),
            (Ok(11), true),
            (Err(()), true),
        ] {
            a.sendgrid_webhook_keys = Some(value);
            assert_ne!(a.observed_condition_mask() & (1 << 30), 0);
            assert_eq!(a.condition_mask() & (1 << 30) != 0, unhealthy);
        }
        assert!(
            a.text()
                .unwrap()
                .contains("SendGrid webhook key check failed")
        );
    }

    #[test]
    fn fcm_credential_health_uses_distinct_signed_condition() {
        let mut a = alert();
        assert_eq!(a.observed_condition_mask() & i32::MIN, 0);
        for (state, unhealthy) in [
            (Ok((2, 0, 1, 1)), false),
            (Ok((0, 0, 0, 1)), true),
            (Ok((2, 1, 0, 1)), true),
            (Err(()), true),
        ] {
            a.fcm_credentials = Some(state);
            assert_ne!(a.observed_condition_mask() & i32::MIN, 0);
            assert_eq!(a.condition_mask() & i32::MIN != 0, unhealthy);
        }
        assert!(
            a.text()
                .unwrap()
                .contains("FCM credential worker check failed")
        );
    }

    #[test]
    fn channel_policy_conditions_are_independent_and_observed() {
        let mut a = alert();
        a.email_policy_cancelled = Some(2);
        a.sms_policy_cancelled = Some(0);
        a.push_policy_cancelled = Some(1);
        assert_ne!(a.condition_mask() & (1 << 29), 0);
        assert!(
            a.text()
                .unwrap()
                .contains("Push workflow policy cancellations: 1")
        );
        assert_ne!(a.condition_mask() & (1 << 26), 0);
        assert_eq!(a.condition_mask() & (1 << 27), 0);
        assert_ne!(a.observed_condition_mask() & (1 << 27), 0);
        assert!(
            a.text()
                .unwrap()
                .contains("Email workflow policy cancellations: 2")
        );
        a.sms_policy_cancelled = None;
        a.channel_policy_check_failed = true;
        assert_ne!(a.condition_mask() & (1 << 28), 0);
        assert_eq!(a.observed_condition_mask() & (1 << 27), 0);
        assert!(
            a.text()
                .unwrap()
                .contains("Channel policy cancellation check failed")
        );
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
    fn slack_policy_alert_tracks_observed_and_failed_states() {
        let mut a = alert();
        assert_eq!(a.observed_condition_mask() & (1 << 25), 0);
        a.slack_policy_cancelled = Some(2);
        assert_ne!(a.condition_mask() & (1 << 25), 0);
        assert_ne!(a.observed_condition_mask() & (1 << 25), 0);
        assert_ne!(a.observed_condition_mask() & (1 << 17), 0);
        assert!(
            a.text()
                .unwrap()
                .contains("Slack workflow policy cancellations: 2")
        );
        a.slack_policy_cancelled = Some(0);
        assert_eq!(a.condition_mask() & (1 << 25), 0);
        a.slack_policy_cancelled = None;
        a.failed_checks[4] = true;
        assert_ne!(a.condition_mask() & (1 << 17), 0);
        assert_eq!(a.observed_condition_mask() & (1 << 25), 0);
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

    #[tokio::test]
    #[ignore = "requires disposable PostgreSQL and mock loopback servers"]
    async fn fcm_health_alert_reaches_both_destinations_and_recurs_after_recovery() {
        let url = std::env::var("TEST_DATABASE_URL").unwrap();
        let (mut db, driver) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(async move {
            let _ = driver.await;
        });
        db.batch_execute("CREATE TABLE public.novu_rust_environments(id text PRIMARY KEY, organization_id text NOT NULL); INSERT INTO public.novu_rust_environments VALUES ('env-a','org')").await.unwrap();
        for sql in [
            crate::MIGRATION,
            crate::reconciliation::MIGRATION,
            crate::reconciliation::OPERATOR_GRANTS_MIGRATION,
            crate::reconciliation::OPERATOR_GRANT_LOCK_MIGRATION,
            MIGRATION,
            include_str!("../migrations/0072_operator_grant_management.sql"),
            include_str!("../migrations/0073_operator_role_identity.sql"),
            include_str!("../migrations/0087_alert_view_permission.sql"),
            include_str!("../migrations/0085_alert_condition_cooldown.sql"),
            include_str!("../migrations/0088_scoped_alert_dispatch.sql"),
            include_str!("../migrations/0089_scoped_delivery_health.sql"),
            include_str!("../migrations/0093_alert_condition_recovery.sql"),
            include_str!("../migrations/0095_fcm_routing_guard.sql"),
            include_str!("../migrations/0124_push_workflow_worker_policy.sql"),
            include_str!("../migrations/0142_fcm_credential_health.sql"),
            include_str!("../migrations/0143_alert_signed_condition_masks.sql"),
        ] {
            db.batch_execute(sql).await.unwrap();
        }
        db.execute("INSERT INTO public.novu_rust_delivery_operator_grants SELECT 'env-a',session_user,v,(SELECT oid FROM pg_roles WHERE rolname=session_user) FROM unnest(ARRAY['alert_view','alert_send']) v", &[]).await.unwrap();
        db.execute("INSERT INTO public.novu_rust_fcm_routing_workers SELECT 'env-a',rolname,oid FROM pg_roles WHERE rolname=session_user", &[]).await.unwrap();
        let instance = Uuid::new_v4();
        db.query_one(
            "SELECT public.novu_rust_fcm_credential_heartbeat('env-a',$1,'unhealthy')",
            &[&instance],
        )
        .await
        .unwrap();
        let row = db.query_one("SELECT healthy,unhealthy,stale FROM public.novu_rust_fcm_credential_health_check('env-a',60)", &[]).await.unwrap();
        let mut value = alert();
        value.resend_enabled = false;
        value.fcm_credentials = Some(Ok((
            row.get::<_, i64>(0) as u64,
            row.get::<_, i64>(1) as u64,
            row.get::<_, i64>(2) as u64,
            1,
        )));
        assert_eq!(value.condition_mask(), i32::MIN);
        let (url, slack_request) = mock_server("200 OK", "ok").await;
        let slack = SlackWebhook::with_endpoint(url).unwrap();
        assert_eq!(
            dispatch_slack_with_options(&mut db, "fcm", 60, &value, &slack, true, true)
                .await
                .unwrap(),
            Dispatch::Accepted
        );
        assert!(slack_request.await.unwrap().contains(
            "FCM credential workers: 0 healthy, 1 unhealthy, 0 stale; minimum healthy: 1"
        ));
        let (url, email_request) = mock_server("200 OK", r#"{"id":"mock-fcm"}"#).await;
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
                "fcm",
                60,
                &value,
                &email,
                "ops@example.test",
                true
            )
            .await
            .unwrap(),
            Dispatch::Accepted
        );
        assert!(email_request.await.unwrap().contains(
            "FCM credential workers: 0 healthy, 1 unhealthy, 0 stale; minimum healthy: 1"
        ));
        assert_eq!(
            dispatch_slack_with_options(&mut db, "fcm", 60, &value, &slack, true, true)
                .await
                .unwrap(),
            Dispatch::CoolingDown
        );
        db.query_one(
            "SELECT public.novu_rust_fcm_credential_heartbeat('env-a',$1,'healthy')",
            &[&instance],
        )
        .await
        .unwrap();
        let row = db.query_one("SELECT healthy,unhealthy,stale FROM public.novu_rust_fcm_credential_health_check('env-a',60)", &[]).await.unwrap();
        assert_eq!(
            (
                row.get::<_, i64>(0),
                row.get::<_, i64>(1),
                row.get::<_, i64>(2)
            ),
            (1, 0, 0)
        );
        value.fcm_credentials = Some(Ok((1, 0, 0, 1)));
        for channel in ["slack", "email"] {
            observe_conditions(
                &db,
                "env-a",
                channel,
                "fcm",
                value.condition_mask(),
                value.observed_condition_mask(),
            )
            .await
            .unwrap();
        }
        db.query_one(
            "SELECT public.novu_rust_fcm_credential_heartbeat('env-a',$1,'unhealthy')",
            &[&instance],
        )
        .await
        .unwrap();
        let row = db.query_one("SELECT healthy,unhealthy,stale FROM public.novu_rust_fcm_credential_health_check('env-a',60)", &[]).await.unwrap();
        assert_eq!(
            (
                row.get::<_, i64>(0),
                row.get::<_, i64>(1),
                row.get::<_, i64>(2)
            ),
            (0, 1, 0)
        );
        value.fcm_credentials = Some(Ok((0, 1, 0, 1)));
        let (url, slack_request) = mock_server("200 OK", "ok").await;
        let slack = SlackWebhook::with_endpoint(url).unwrap();
        assert_eq!(
            dispatch_slack_with_options(&mut db, "fcm", 60, &value, &slack, true, true)
                .await
                .unwrap(),
            Dispatch::Accepted
        );
        slack_request.await.unwrap();
        let (url, email_request) = mock_server("200 OK", r#"{"id":"mock-fcm-again"}"#).await;
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
                "fcm",
                60,
                &value,
                &email,
                "ops@example.test",
                true
            )
            .await
            .unwrap(),
            Dispatch::Accepted
        );
        email_request.await.unwrap();
    }

    #[tokio::test]
    async fn resend_cooldown_has_distinct_v2_conditions_and_transports() {
        let mut a = alert(); a.resend_enabled=false;
        a.resend_cooldown=Some(Ok((true,2,1)));
        assert_eq!(a.condition_mask_v2(),1_i64<<47);
        assert_ne!(a.observed_condition_mask_v2() & (1_i64<<47),0);
        let (url, server)=mock_server("200 OK","ok").await;
        assert_eq!(SlackWebhook::with_endpoint(url).unwrap().send(&a).await,Ok(()));
        assert!(server.await.unwrap().contains("Resend shared cooldown active: true"));
        let (url, server)=mock_server("200 OK",r#"{"id":"fixture"}"#).await;
        let provider=Resend::test_endpoint(crate::resend::Config {api_key:"fixture".into(),from:"from@example.test".into(),sender_name:String::new()},url.as_str()).unwrap();
        assert_eq!(send_email(&provider,"ops@example.test",&a,"fixture").await,Ok(()));
        assert!(server.await.unwrap().contains("Resend shared cooldown active: true"));
        a.resend_cooldown=Some(Err(()));
        assert_eq!(a.condition_mask_v2(),1_i64<<48);
        assert_eq!(a.observed_condition_mask_v2() & (1_i64<<47),0);
    }

    #[test]
    fn operator_key_probe_reports_only_the_safe_label_and_condition() {
        let mut a = alert();
        a.resend_enabled = false;
        a.operator_key_probe = Some(("api-1".into(), true));
        assert_eq!(a.condition_mask() & (1 << 20), 1 << 20);
        assert_eq!(a.observed_condition_mask() & (1 << 20), 1 << 20);
        assert!(a.text().unwrap().contains("operator key reload api-1 probe failed: true"));
        a.operator_key_probe = Some(("invalid\nlabel".into(), true));
        assert!(a.text().is_err());
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
    async fn operator_key_probe_alert_uses_both_mock_transports() {
        let mut a = alert();
        a.resend_enabled = false;
        a.operator_key_probe = Some(("instance-1".into(), true));
        let expected = "operator key reload instance-1 probe failed: true";
        let (url, slack_server) = mock_server("200 OK", "ok").await;
        assert_eq!(SlackWebhook::with_endpoint(url).unwrap().send(&a).await, Ok(()));
        assert!(slack_server.await.unwrap().contains(expected));
        let (url, mail_server) = mock_server("200 OK", r#"{"id":"fixture"}"#).await;
        let provider = Resend::test_endpoint(crate::resend::Config { api_key:"fixture-key".into(), from:"from@example.test".into(), sender_name:String::new() }, url.as_str()).unwrap();
        assert_eq!(send_email(&provider, "ops@example.test", &a, "key-alert-fixture").await, Ok(()));
        assert!(mail_server.await.unwrap().contains(expected));
    }

    #[tokio::test]
    async fn resend_webhook_key_probe_alert_uses_both_mock_transports() {
        let mut a = alert();
        a.resend_enabled = false;
        a.resend_webhook_key_probe = Some(("instance-1".into(), true));
        let expected = "Resend webhook key instance-1 probe failed: true";
        let (url, slack_server) = mock_server("200 OK", "ok").await;
        assert_eq!(SlackWebhook::with_endpoint(url).unwrap().send(&a).await, Ok(()));
        assert!(slack_server.await.unwrap().contains(expected));
        let (url, mail_server) = mock_server("200 OK", r#"{"id":"fixture"}"#).await;
        let provider = Resend::test_endpoint(crate::resend::Config { api_key:"fixture-key".into(), from:"from@example.test".into(), sender_name:String::new() }, url.as_str()).unwrap();
        assert_eq!(send_email(&provider, "ops@example.test", &a, "key-alert-fixture").await, Ok(()));
        assert!(mail_server.await.unwrap().contains(expected));
    }

    #[tokio::test]
    async fn twilio_webhook_key_probe_alert_uses_both_mock_transports() {
        let mut a = alert();
        a.resend_enabled = false;
        a.twilio_webhook_key_probe = Some(("instance-1".into(), true));
        let expected = "Twilio webhook key instance-1 probe failed: true";
        let (url, slack_server) = mock_server("200 OK", "ok").await;
        assert_eq!(SlackWebhook::with_endpoint(url).unwrap().send(&a).await, Ok(()));
        assert!(slack_server.await.unwrap().contains(expected));
        let (url, mail_server) = mock_server("200 OK", r#"{"id":"fixture"}"#).await;
        let provider = Resend::test_endpoint(crate::resend::Config { api_key:"fixture-key".into(), from:"from@example.test".into(), sender_name:String::new() }, url.as_str()).unwrap();
        assert_eq!(send_email(&provider, "ops@example.test", &a, "key-alert-fixture").await, Ok(()));
        assert!(mail_server.await.unwrap().contains(expected));
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
    async fn selected_sendgrid_alert_classifies_provider_outcomes() {
        for (status, expected) in [
            ("202 Accepted", Ok(())),
            ("429 Too Many Requests", Err(AlertError::Rejected(429))),
            ("500 Server Error", Err(AlertError::Unknown)),
        ] {
            let (url, server) = mock_server(status, "").await;
            let provider = AlertEmailProvider::Sendgrid(
                crate::sendgrid::Sendgrid::test_endpoint(
                    crate::sendgrid::Config {
                        api_key: "fixture-key".into(),
                        from: "from@example.test".into(),
                        sender_name: "Ops".into(),
                        region: crate::sendgrid::Region::Global,
                    },
                    url.as_str(),
                )
                .unwrap()
                .with_default_ip_pool("ops-pool".into()),
            );
            assert_eq!(
                send_selected_email(&provider, "ops@example.test", &alert(), "ignored").await,
                expected
            );
            let request = server.await.unwrap();
            let body: serde_json::Value =
                serde_json::from_str(request.split_once("\r\n\r\n").unwrap().1).unwrap();
            assert_eq!(
                body["personalizations"][0]["to"][0]["email"],
                "ops@example.test"
            );
            assert_eq!(body["subject"], "Swallow delivery alert");
            assert_eq!(body["ip_pool_name"], "ops-pool");
            assert!(!body.to_string().contains("fixture-key"));
            assert!(!request.to_ascii_lowercase().contains("idempotency-key:"));
        }
    }

    #[tokio::test]
    async fn selected_fallback_attempts_sendgrid_after_slack_failure() {
        let (url, slack_server) = mock_server("500 Server Error", "failed").await;
        let slack = SlackWebhook::with_endpoint(url).unwrap();
        let (url, email_server) = mock_server("202 Accepted", "").await;
        let email = AlertEmailProvider::Sendgrid(
            crate::sendgrid::Sendgrid::test_endpoint(
                crate::sendgrid::Config {
                    api_key: "fixture-key".into(),
                    from: "from@example.test".into(),
                    sender_name: String::new(),
                    region: crate::sendgrid::Region::Global,
                },
                url.as_str(),
            )
            .unwrap(),
        );
        let result = send_uncoordinated_selected(
            &alert(),
            Some(&slack),
            Some((&email, "ops@example.test")),
            Duration::from_secs(2),
            false,
        )
        .await;
        assert_eq!(result, (Some(Err(AlertError::Unknown)), Some(Ok(()))));
        slack_server.await.unwrap();
        assert!(
            email_server
                .await
                .unwrap()
                .contains("Swallow delivery alert")
        );
    }

    #[tokio::test]
    async fn fallback_parallel_policy_starts_email_while_slack_is_stalled() {
        for parallel in [true, false] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint =
                Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
            let (seen_tx, seen_rx) = tokio::sync::oneshot::channel();
            let (release_tx, release_rx) = tokio::sync::oneshot::channel();
            let slack_server = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut buf = [0u8; 4096];
                assert!(stream.read(&mut buf).await.unwrap() > 0);
                seen_tx.send(()).unwrap();
                release_rx.await.unwrap();
                stream
                    .write_all(
                        b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok",
                    )
                    .await
                    .unwrap();
            });
            let slack = SlackWebhook::with_endpoint(endpoint).unwrap();
            let (url, mut email_server) = mock_server("202 Accepted", "").await;
            let email = AlertEmailProvider::Sendgrid(
                crate::sendgrid::Sendgrid::test_endpoint(
                    crate::sendgrid::Config {
                        api_key: "fixture-key".into(),
                        from: "from@example.test".into(),
                        sender_name: String::new(),
                        region: crate::sendgrid::Region::Global,
                    },
                    url.as_str(),
                )
                .unwrap(),
            );
            let operation = tokio::spawn(async move {
                send_uncoordinated_selected(
                    &alert(),
                    Some(&slack),
                    Some((&email, "ops@example.test")),
                    Duration::from_secs(3),
                    parallel,
                )
                .await
            });
            tokio::time::timeout(Duration::from_secs(1), seen_rx)
                .await
                .unwrap()
                .unwrap();
            let early = tokio::time::timeout(Duration::from_millis(250), &mut email_server).await;
            if parallel {
                assert!(early.unwrap().unwrap().contains("Swallow delivery alert"));
            } else {
                assert!(
                    early.is_err(),
                    "sequential mode sent email before Slack completed"
                );
            }
            release_tx.send(()).unwrap();
            slack_server.await.unwrap();
            if !parallel {
                assert!(
                    email_server
                        .await
                        .unwrap()
                        .contains("Swallow delivery alert")
                );
            }
            assert_eq!(operation.await.unwrap(), (Some(Ok(())), Some(Ok(()))));
        }
    }

    #[tokio::test]
    async fn parallel_fallback_keeps_email_success_when_slack_times_out() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
        let stalled = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 4096];
            assert!(stream.read(&mut buf).await.unwrap() > 0);
            std::future::pending::<()>().await;
        });
        let slack = SlackWebhook::with_endpoint(endpoint).unwrap();
        let (url, server) = mock_server("202 Accepted", "").await;
        let email = AlertEmailProvider::Sendgrid(
            crate::sendgrid::Sendgrid::test_endpoint(
                crate::sendgrid::Config {
                    api_key: "fixture-key".into(),
                    from: "from@example.test".into(),
                    sender_name: String::new(),
                    region: crate::sendgrid::Region::Global,
                },
                url.as_str(),
            )
            .unwrap(),
        );
        let result = send_uncoordinated_selected(
            &alert(),
            Some(&slack),
            Some((&email, "ops@example.test")),
            Duration::from_millis(500),
            true,
        )
        .await;
        assert_eq!(result, (Some(Err(AlertError::Unknown)), Some(Ok(()))));
        server.await.unwrap();
        stalled.abort();
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
            sendgrid_webhook_keys: None,
            fcm_credentials: None,
            slack_attempts: None,
            slack_policy_cancelled: None,
            discord_health: None,
            discord_retry_health: None,
            email_policy_cancelled: None,
            sms_policy_cancelled: None,
            push_policy_cancelled: None,
            channel_policy_check_failed: false,
            failed_checks: [false; 5],
            resend_auto: None,
            slack_cooldown: None,
            resend_cooldown: None,
            twilio_delayed: None,
            fcm_partial: None,
            fcm_unreviewed: None,
            fcm_unrecorded_claims: None,
            fcm_cooldown: None,
            fcm_cooldown_source: None,
            fcm_cooldown_distinct_source: false,
            failed_jobs: None,
            failed_jobs_check_failed: false,
            database_probe: None,
            operator_key_probe: None,
            resend_webhook_key_probe: None,
            twilio_webhook_key_probe: None,
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
        // The selected SendGrid path shares reservation and uncertain-outcome fencing.
        let (url, server) = mock_server("202 Accepted", "").await;
        let selected = AlertEmailProvider::Sendgrid(
            crate::sendgrid::Sendgrid::test_endpoint(
                crate::sendgrid::Config {
                    api_key: "fixture-key".into(),
                    from: "from@example.test".into(),
                    sender_name: String::new(),
                    region: crate::sendgrid::Region::Global,
                },
                url.as_str(),
            )
            .unwrap(),
        );
        assert_eq!(
            dispatch_selected_email_with_options(
                &mut db,
                "sendgrid-ops",
                60,
                &alert,
                &selected,
                "ops@example.test",
                (false, false)
            )
            .await
            .unwrap(),
            Dispatch::Accepted
        );
        server.await.unwrap();
        assert_eq!(
            dispatch_selected_email_with_options(
                &mut db,
                "sendgrid-ops",
                60,
                &alert,
                &selected,
                "ops@example.test",
                (false, false)
            )
            .await
            .unwrap(),
            Dispatch::CoolingDown
        );
        let (url, server) = mock_server("500 Server Error", "").await;
        let unknown = AlertEmailProvider::Sendgrid(
            crate::sendgrid::Sendgrid::test_endpoint(
                crate::sendgrid::Config {
                    api_key: "fixture-key".into(),
                    from: "from@example.test".into(),
                    sender_name: String::new(),
                    region: crate::sendgrid::Region::Global,
                },
                url.as_str(),
            )
            .unwrap(),
        );
        assert_eq!(
            dispatch_selected_email_with_options(
                &mut db,
                "sendgrid-unknown",
                60,
                &alert,
                &unknown,
                "ops@example.test",
                (false, false)
            )
            .await
            .unwrap(),
            Dispatch::NeedsReview
        );
        server.await.unwrap();
        assert_eq!(
            dispatch_selected_email_with_options(
                &mut db,
                "sendgrid-unknown",
                60,
                &alert,
                &unknown,
                "ops@example.test",
                (false, false)
            )
            .await
            .unwrap(),
            Dispatch::NeedsReview
        );
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
        let scoped_email = AlertEmailProvider::Resend(scoped_email);
        assert_eq!(
            dispatch_selected_email_with_options(
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
        let (url, server) = mock_server("202 Accepted", "").await;
        let selected = AlertEmailProvider::Sendgrid(
            crate::sendgrid::Sendgrid::test_endpoint(
                crate::sendgrid::Config {
                    api_key: "fixture-key".into(),
                    from: "from@example.test".into(),
                    sender_name: String::new(),
                    region: crate::sendgrid::Region::Global,
                },
                url.as_str(),
            )
            .unwrap(),
        );
        assert_eq!(
            dispatch_selected_email_with_options(
                &mut restricted,
                "limited-sendgrid",
                60,
                &alert,
                &selected,
                "ops@example.test",
                (true, true)
            )
            .await
            .unwrap(),
            Dispatch::Accepted
        );
        server.await.unwrap();
        assert_eq!(db.query_one("SELECT count(*) FROM novu_rust_alert_delivery_attempts WHERE sender_session_user=$1 AND target_id='limited-sendgrid' AND state='accepted'", &[&role]).await.unwrap().get::<_,i64>(0), 1);
        // Both restricted sessions reserve independently: a stalled Slack HTTP
        // request must not prevent the email result from being committed.
        let (mut pair_a, driver) = tokio_postgres::connect(
            &std::env::var("TEST_DATABASE_URL").unwrap(),
            tokio_postgres::NoTls,
        )
        .await
        .unwrap();
        tokio::spawn(async move {
            let _ = driver.await;
        });
        let (mut pair_b, driver) = tokio_postgres::connect(
            &std::env::var("TEST_DATABASE_URL").unwrap(),
            tokio_postgres::NoTls,
        )
        .await
        .unwrap();
        tokio::spawn(async move {
            let _ = driver.await;
        });
        pair_a
            .batch_execute(&format!("SET SESSION AUTHORIZATION {role}"))
            .await
            .unwrap();
        pair_b
            .batch_execute(&format!("SET SESSION AUTHORIZATION {role}"))
            .await
            .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
        let (seen_tx, seen_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let held_server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 4096];
            assert!(stream.read(&mut buf).await.unwrap() > 0);
            seen_tx.send(()).unwrap();
            release_rx.await.unwrap();
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
                .await
                .unwrap();
        });
        let pair_slack = SlackWebhook::with_endpoint(endpoint).unwrap();
        let (url, email_server) = mock_server("202 Accepted", "").await;
        let pair_email = AlertEmailProvider::Sendgrid(
            crate::sendgrid::Sendgrid::test_endpoint(
                crate::sendgrid::Config {
                    api_key: "fixture-key".into(),
                    from: "from@example.test".into(),
                    sender_name: String::new(),
                    region: crate::sendgrid::Region::Global,
                },
                url.as_str(),
            )
            .unwrap(),
        );
        let pair = tokio::spawn(async move {
            dispatch_pair(
                &mut pair_a,
                Some(&mut pair_b),
                DispatchPair {
                    alert: &self::alert(),
                    slack: Some((&pair_slack, "pair-slack")),
                    email: Some((&pair_email, "pair-email", "ops@example.test")),
                    cooldown: 60,
                    on_new: true,
                    scoped: true,
                    timeout: Duration::from_secs(5),
                },
            )
            .await
        });
        tokio::time::timeout(Duration::from_secs(2), seen_rx)
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), email_server)
            .await
            .unwrap()
            .unwrap();
        let mut email_committed = false;
        for _ in 0..100 {
            if db.query_one("SELECT EXISTS(SELECT 1 FROM novu_rust_alert_delivery_slots WHERE target_id='pair-email' AND state='accepted')", &[]).await.unwrap().get::<_,bool>(0) {email_committed = true; break;}
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(email_committed && !pair.is_finished());
        release_tx.send(()).unwrap();
        held_server.await.unwrap();
        let (s, e) = pair.await.unwrap();
        assert_eq!(s.unwrap().unwrap(), Dispatch::Accepted);
        assert_eq!(e.unwrap().unwrap(), Dispatch::Accepted);
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
