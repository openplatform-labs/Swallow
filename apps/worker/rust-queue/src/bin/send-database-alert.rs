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
    let slack = match slack() {
        Ok(v) => v,
        Err(_) => {
            bad_config = true;
            eprintln!("Slack alert configuration failed");
            None
        }
    };
    let email = match alerts::email_from_env() {
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
        slack_attempts: None,
        slack_policy_cancelled: None,
        failed_checks: [false; 5],
        failed_jobs: None,
        failed_jobs_check_failed: false,
        database_probe: Some((args[1].clone(), false)),
        alert_store_unavailable: false,
    };
    alert.text().map_err(|_| "invalid alert label")?;
    let default_target = format!("db-{}", args[1]);
    let slack_id = std::env::var("SWALLOW_DB_ALERT_SLACK_TARGET_ID")
        .unwrap_or_else(|_| default_target.clone());
    let email_id = std::env::var("SWALLOW_DB_ALERT_EMAIL_TARGET_ID").unwrap_or(default_target);
    let (monitor_config, monitor_tls) =
        novu_rust_contracts::postgres::configure(&std::env::var("DATABASE_URL")?)
            .map_err(|_| "invalid monitor database configuration")?;
    let (probe_config, probe_tls) =
        novu_rust_contracts::postgres::configure(&std::env::var("SWALLOW_DB_PROBE_URL")?)
            .map_err(|_| "invalid target database configuration")?;
    let secondary_config = if fallback == StateFailureMode::Secondary {
        Some(
            novu_rust_contracts::postgres::configure(&std::env::var(
                "SWALLOW_DB_ALERT_BACKUP_DATABASE_URL",
            )?)
            .map_err(|_| "invalid secondary database configuration")?,
        )
    } else {
        None
    };
    let probe = tokio::time::timeout(Duration::from_secs(seconds), async move {
        let (client, connection) = probe_config.connect(probe_tls).await?;
        let _driver = AbortOnDrop(tokio::spawn(async move {
            let _ = connection.await;
        }));
        client.simple_query("SELECT 1").await?;
        Ok::<(), tokio_postgres::Error>(())
    })
    .await;
    let probe_unhealthy = !matches!(probe, Ok(Ok(())));
    alert.database_probe = Some((args[1].clone(), probe_unhealthy));
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
