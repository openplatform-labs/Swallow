//! Probe a target PostgreSQL database using a separate alert-state database.
use novu_postgres_queue::alerts::{self, Alert, Dispatch, SlackWebhook};
use std::{error::Error, time::Duration};
type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
#[derive(Clone, Copy, PartialEq, Eq)]
enum StateFailureMode {
    ReportOnly,
    Secondary,
    Uncoordinated,
}
struct AbortOnDrop(tokio::task::JoinHandle<()>);
impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}
fn flag(name: &str, default: bool) -> Result<bool> {
    match std::env::var(name).as_deref() {
        Ok("true") => Ok(true),
        Ok("false") => Ok(false),
        Err(std::env::VarError::NotPresent) => Ok(default),
        _ => Err(format!("{name} must be true or false").into()),
    }
}
fn slack() -> Result<Option<SlackWebhook>> {
    novu_rust_contracts::secret::from_env(
        "SWALLOW_ALERT_SLACK_WEBHOOK",
        "SWALLOW_ALERT_SLACK_WEBHOOK_FILE",
    )?
    .map(|s| SlackWebhook::new(&s).map_err(|_| "invalid Slack configuration".into()))
    .transpose()
}
#[tokio::main]
async fn main() -> Result<()> {
    novu_postgres_queue::shutdown::run_until_signal_with_setting(
        "SWALLOW_ALERT_SHUTDOWN_GRACE_SECONDS",
        |_| run(),
    )
    .await
}

async fn run() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.len() != 3 {
        return Err("usage: send-database-alert MONITOR_ENV TARGET_LABEL COOLDOWN_SECONDS".into());
    }
    let cooldown: i32 = args[2].parse()?;
    if !(60..=86400).contains(&cooldown) {
        return Err("invalid cooldown".into());
    }
    let seconds: u64 = std::env::var("SWALLOW_DB_PROBE_TIMEOUT_SECONDS")
        .unwrap_or_else(|_| "5".into())
        .parse()?;
    if !(1..=60).contains(&seconds) {
        return Err("invalid database probe timeout".into());
    }
    let key_probe = match std::env::var("SWALLOW_OPERATOR_KEY_PROBE_URL") {
        Ok(raw) => {
            if std::env::var_os("SWALLOW_DB_PROBE_URL").is_some() { return Err("select exactly one probe".into()); }
            let url = reqwest::Url::parse(&raw).map_err(|_| "invalid operator key probe URL")?;
            if !url.username().is_empty() || url.password().is_some() || url.query().is_some() || url.fragment().is_some()
                || url.path() != "/health/operator-keys"
                || !(url.scheme() == "https" || (url.scheme() == "http" && matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "[::1]")))) {
                return Err("invalid operator key probe URL".into());
            }
            Some(url)
        }
        Err(std::env::VarError::NotPresent) => None,
        Err(_) => return Err("invalid operator key probe URL".into()),
    };
    let probe_role = if key_probe.is_none() { Some(novu_postgres_queue::db_probe::Role::from_env()?) } else { None };
    let operation_seconds: u64 = std::env::var("SWALLOW_DB_ALERT_OPERATION_TIMEOUT_SECONDS")
        .unwrap_or_else(|_| "30".into())
        .parse()?;
    if !(1..=300).contains(&operation_seconds) {
        return Err("invalid alert operation timeout".into());
    }
    let operation_timeout = Duration::from_secs(operation_seconds);
    let parallel_delivery = alerts::parallel_delivery_from_env()?;
    let on_new = match std::env::var("SWALLOW_ALERT_COOLDOWN_POLICY").as_deref() {
        Ok("new-condition") => true,
        Ok("destination") | Err(std::env::VarError::NotPresent) => false,
        _ => return Err("invalid alert cooldown policy".into()),
    };
    let fallback = match std::env::var("SWALLOW_DB_ALERT_STATE_FAILURE_MODE").as_deref() {
        Ok("send-uncoordinated") => StateFailureMode::Uncoordinated,
        Ok("secondary-store") => StateFailureMode::Secondary,
        Ok("report-only") | Err(std::env::VarError::NotPresent) => StateFailureMode::ReportOnly,
        _ => return Err("invalid state database failure mode".into()),
    };
    let parallel_fallback = match std::env::var("SWALLOW_DB_ALERT_FALLBACK_DELIVERY").as_deref() {
        Ok("parallel") => true,
        Ok("sequential") | Err(std::env::VarError::NotPresent) => false,
        _ => return Err("invalid fallback delivery policy".into()),
    };
    let scoped = flag("SWALLOW_ALERT_SCOPED_DELIVERY", false)?;
    let mut bad_config = false;
    let mut slack = match slack() {
        Ok(v) => v,
        Err(_) => {
            bad_config = true;
            eprintln!("Slack alert configuration failed");
            None
        }
    };
    let mut email = match alerts::email_from_env() {
        Ok(v) => v,
        Err(_) => {
            bad_config = true;
            eprintln!("email alert configuration failed");
            None
        }
    };
    if slack.is_none() && email.is_none() {
        return Err("no usable alert destination configured".into());
    }
    let binding_key = alerts::binding_key_from_env()?;
    let email_binding_profile = alerts::email_binding_profile_from_env(binding_key.is_some())?;
    let mut alert = Alert {
        environment: args[0].clone(),
        overdue_undelivered: 0,
        projector_stale: false,
        resend_enabled: false,
        queue: None,
        attempts: None,
        twilio: None,
        sendgrid: None,
        sendgrid_check_failed: false,
        sendgrid_webhook_keys: None,
        fcm_credentials: None,
        slack_attempts: None,
        slack_policy_cancelled: None,
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
        database_probe: key_probe.is_none().then(|| (args[1].clone(), false)),
        operator_key_probe: key_probe.as_ref().map(|_| (args[1].clone(), false)),
        alert_store_unavailable: false,
    };
    alert.text().map_err(|_| "invalid alert label")?;
    let default_target = format!("{}-{}", if key_probe.is_some() { "key" } else { "db" }, args[1]);
    let slack_id = std::env::var("SWALLOW_DB_ALERT_SLACK_TARGET_ID")
        .unwrap_or_else(|_| default_target.clone());
    let email_id = std::env::var("SWALLOW_DB_ALERT_EMAIL_TARGET_ID").unwrap_or(default_target);
    let (monitor_config, monitor_tls) =
        novu_rust_contracts::postgres::configure(&std::env::var("DATABASE_URL")?)
            .map_err(|_| "invalid monitor database configuration")?;
    let secondary_config = if fallback == StateFailureMode::Secondary {
        Some(novu_rust_contracts::postgres::configure(&std::env::var("SWALLOW_DB_ALERT_BACKUP_DATABASE_URL")?)
            .map_err(|_| "invalid secondary database configuration")?)
    } else { None };
    let probe_unhealthy = if let Some(url) = key_probe {
        let client = reqwest::Client::builder().redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(seconds)).build().map_err(|_| "invalid probe client")?;
        !matches!(tokio::time::timeout(Duration::from_secs(seconds), async {
            let mut response = client.get(url).send().await.map_err(|_| ())?;
            if response.status() != reqwest::StatusCode::OK || response.content_length().is_some_and(|size| size > 4096) { return Err(()); }
            let mut body = Vec::new();
            while let Some(chunk) = response.chunk().await.map_err(|_| ())? {
                if body.len() + chunk.len() > 4096 { return Err(()); }
                body.extend_from_slice(&chunk);
            }
            let body: serde_json::Value = serde_json::from_slice(&body).map_err(|_| ())?;
            if body.get("status").and_then(|status| status.as_str()) == Some("healthy") { Ok(()) } else { Err(()) }
        }).await, Ok(Ok(())))
    } else {
        let (probe_config, probe_tls) = novu_rust_contracts::postgres::configure(&std::env::var("SWALLOW_DB_PROBE_URL")?)
            .map_err(|_| "invalid target database configuration")?;
        let probe = tokio::time::timeout(Duration::from_secs(seconds), async move {
            let (client, connection) = probe_config.connect(probe_tls).await?;
            let _driver = AbortOnDrop(tokio::spawn(async move { let _ = connection.await; }));
            novu_postgres_queue::db_probe::healthy(&client, probe_role.expect("DB probe role configured")).await
        }).await;
        !matches!(probe, Ok(Ok(true)))
    };
    if let Some((_, unhealthy)) = alert.database_probe.as_mut() { *unhealthy = probe_unhealthy; }
    if let Some((_, unhealthy)) = alert.operator_key_probe.as_mut() { *unhealthy = probe_unhealthy; }
    let mut delivery_config = (monitor_config.clone(), monitor_tls.clone());
    let monitor = tokio::time::timeout(
        Duration::from_secs(seconds),
        monitor_config.connect(monitor_tls),
    )
    .await;
    let mut primary_unavailable = false;
    let (mut db, connection) = match monitor {
        Ok(Ok(value)) => value,
        _ if fallback == StateFailureMode::Secondary => {
            primary_unavailable = true;
            alert.alert_store_unavailable = true;
            eprintln!("monitor database connection failed; using secondary alert state database");
            let (config, tls) =
                secondary_config.ok_or("secondary database configuration missing")?;
            delivery_config = (config.clone(), tls.clone());
            tokio::time::timeout(Duration::from_secs(seconds), config.connect(tls))
                .await
                .map_err(|_| "secondary database connection timed out")?
                .map_err(|_| "secondary database connection failed")?
        }
        _ if fallback == StateFailureMode::Uncoordinated => {
            if binding_key.is_some() {
                return Err("destination binding unavailable in uncoordinated fallback".into());
            }
            alert.alert_store_unavailable = true;
            eprintln!("monitor database connection failed; sending uncoordinated alerts");
            let result = alerts::send_uncoordinated_selected(
                &alert,
                slack.as_ref(),
                email.as_ref().map(|(to, provider)| (provider, to.as_str())),
                operation_timeout,
                parallel_fallback,
            )
            .await;
            for (channel, outcome) in [("Slack", result.0), ("email", result.1)] {
                if let Some(outcome) = outcome {
                    match outcome {
                        Ok(()) => println!("{channel} uncoordinated alert accepted"),
                        Err(
                            alerts::AlertError::Rejected(_)
                            | alerts::AlertError::InvalidConfiguration,
                        ) => eprintln!("{channel} uncoordinated alert rejected"),
                        Err(alerts::AlertError::Unknown) => {
                            eprintln!("{channel} uncoordinated alert outcome unknown")
                        }
                    }
                }
            }
            std::process::exit(1);
        }
        _ => return Err("monitor database connection failed or timed out".into()),
    };
    let _monitor_driver = AbortOnDrop(tokio::spawn(async move {
        let _ = connection.await;
    }));
    if let Some(key) = &binding_key {
        if let Some(provider) = &slack
            && !matches!(
                tokio::time::timeout(
                    operation_timeout,
                    alerts::bind_slack_destination(&db, &args[0], &slack_id, key, provider)
                )
                .await,
                Ok(Ok(true))
            )
        {
            bad_config = true;
            slack = None;
            eprintln!("Slack alert destination binding failed");
        }
        if let Some((recipient, provider)) = &email
            && !matches!(
                tokio::time::timeout(
                    operation_timeout,
                    alerts::bind_email_destination(
                        &db,
                        &args[0],
                        &email_id,
                        key,
                        provider,
                        recipient,
                        email_binding_profile
                    )
                )
                .await,
                Ok(Ok(true))
            )
        {
            bad_config = true;
            email = None;
            eprintln!("email alert destination binding failed");
        }
        if slack.is_none() && email.is_none() {
            return Err("no bound alert destination available".into());
        }
    }
    let unhealthy = probe_unhealthy || primary_unavailable;
    let mut error = bad_config || primary_unavailable;
    if on_new {
        for (channel, target, enabled) in [
            ("slack", slack_id.as_str(), slack.is_some()),
            ("email", email_id.as_str(), email.is_some()),
        ] {
            if enabled {
                let result = tokio::time::timeout(
                    operation_timeout,
                    alerts::observe_conditions(
                        &db,
                        &args[0],
                        channel,
                        target,
                        alert.condition_mask(),
                        alert.observed_condition_mask(),
                    ),
                )
                .await;
                if !matches!(result, Ok(Ok(()))) {
                    error = true;
                    eprintln!("{channel} alert observation failed or timed out");
                }
            }
        }
    }
    if !unhealthy {
        if error {
            std::process::exit(1);
        }
        println!("{{\"status\":\"healthy\"}}");
        return Ok(());
    }
    let mut other = if parallel_delivery && slack.is_some() && email.is_some() {
        match alerts::additional_delivery_connection(
            &delivery_config.0,
            delivery_config.1,
            operation_timeout,
        )
        .await
        {
            Ok(client) => Some(client),
            Err(_) => {
                error = true;
                eprintln!("parallel alert connection failed; using sequential delivery");
                None
            }
        }
    } else {
        None
    };
    let (slack_result, email_result) = alerts::dispatch_pair(
        &mut db,
        other.as_mut(),
        alerts::DispatchPair {
            alert: &alert,
            slack: slack.as_ref().map(|p| (p, slack_id.as_str())),
            email: email
                .as_ref()
                .map(|(to, p)| (p, email_id.as_str(), to.as_str())),
            cooldown,
            on_new,
            scoped,
            timeout: operation_timeout,
        },
    )
    .await;
    for (channel, outcome) in [("Slack", slack_result), ("email", email_result)] {
        if let Some(outcome) = outcome {
            match outcome {
                Ok(Dispatch::NeedsReview) => eprintln!("{channel} alert requires operator review"),
                Ok(result) => println!("{channel} alert: {result:?}"),
                Err(_) => {
                    error = true;
                    eprintln!("{channel} alert dispatch failed or timed out");
                }
            }
        }
    }
    if error {
        std::process::exit(1);
    }
    std::process::exit(2)
}
