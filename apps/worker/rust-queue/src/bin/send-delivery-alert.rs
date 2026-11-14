//! One scheduled pass. Explicitly configured destinations only.
use novu_postgres_queue::{
    alerts::{self, Alert, Dispatch, SlackWebhook},
    reconciliation,
};
use std::{error::Error, time::Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.len() != 4 {
        return Err(
            "usage: send-delivery-alert ENV GRACE_SECONDS HEARTBEAT_MAX_SECONDS COOLDOWN_SECONDS"
                .into(),
        );
    }
    let operation_seconds: u64 = match std::env::var("SWALLOW_ALERT_OPERATION_TIMEOUT_SECONDS") {
        Ok(value) => value
            .parse()
            .map_err(|_| "invalid alert operation timeout")?,
        Err(std::env::VarError::NotPresent) => 30,
        Err(_) => return Err("invalid alert operation timeout".into()),
    };
    if !(1..=300).contains(&operation_seconds) {
        return Err("invalid alert operation timeout".into());
    }
    let operation_timeout = Duration::from_secs(operation_seconds);
    let parallel_delivery = alerts::parallel_delivery_from_env()?;
    let on_new_condition = match std::env::var("SWALLOW_ALERT_COOLDOWN_POLICY").as_deref() {
        Ok("new-condition") => true,
        Ok("destination") | Err(std::env::VarError::NotPresent) => false,
        _ => return Err("invalid alert cooldown policy".into()),
    };
    let grace: i32 = args[1].parse()?;
    let heartbeat: i32 = args[2].parse()?;
    let cooldown: i32 = args[3].parse()?;
    if !(60..=86_400).contains(&cooldown) {
        return Err("invalid cooldown (60-86400 seconds)".into());
    }
    // A broken destination must not suppress a second configured destination.
    // Never print provider configuration or the underlying error: it may contain secrets.
    let mut configuration_error = false;
    let slack = match slack_config() {
        Ok(value) => value,
        Err(_) => {
            eprintln!("Slack alert configuration failed");
            configuration_error = true;
            None
        }
    };
    let email = match alerts::email_from_env() {
        Ok(value) => value,
        Err(_) => {
            eprintln!("email alert configuration failed");
            configuration_error = true;
            None
        }
    };
    if slack.is_none() && email.is_none() {
        return Err("no usable alert destination configured".into());
    }
    let slack_id =
        std::env::var("SWALLOW_ALERT_SLACK_TARGET_ID").unwrap_or_else(|_| "default".into());
    let email_id =
        std::env::var("SWALLOW_ALERT_EMAIL_TARGET_ID").unwrap_or_else(|_| "default".into());
    let (config, tls) = novu_rust_contracts::postgres::configure(&std::env::var("DATABASE_URL")?)?;
    let (mut db, connection) = tokio::time::timeout(operation_timeout, config.connect(tls.clone()))
        .await
        .map_err(|_| "alert database connection timed out")??;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let statement_timeout = format!("{}ms", operation_timeout.as_millis());
    tokio::time::timeout(
        operation_timeout,
        db.query_one(
            "SELECT set_config('statement_timeout',$1,false)",
            &[&statement_timeout],
        ),
    )
    .await
    .map_err(|_| "alert database setup timed out")??;
    let scoped_delivery = env_bool("SWALLOW_ALERT_SCOPED_DELIVERY", false)?;
    let scoped_checks = env_bool("SWALLOW_ALERT_CHECK_SCOPED_ACCESS", false)?;
    let resend_enabled = env_bool("SWALLOW_ALERT_RESEND_ENABLED", true)?;
    let attempts_enabled = env_bool("SWALLOW_ALERT_ATTEMPTS_ENABLED", false)?;
    let slack_jobs_enabled = env_bool("SWALLOW_ALERT_SLACK_JOBS_ENABLED", false)?;
    let failed_jobs_enabled = env_bool("SWALLOW_ALERT_FAILED_JOBS_ENABLED", false)?;
    let slack_missing_enabled = env_bool("SWALLOW_ALERT_SLACK_MISSING_ENDPOINTS_ENABLED", false)?;
    let slack_policy_enabled = env_bool("SWALLOW_ALERT_SLACK_POLICY_CANCELLED_ENABLED", false)?;
    let twilio_grace = env_optional_i32("SWALLOW_ALERT_TWILIO_GRACE_SECONDS")?;
    let twilio_heartbeat = env_optional_i32("SWALLOW_ALERT_TWILIO_HEARTBEAT_MAX_SECONDS")?;
    let twilio_thresholds = match (twilio_grace, twilio_heartbeat) {
        (None, None) => None,
        (Some(grace), Some(heartbeat)) => Some((grace, heartbeat)),
        _ => return Err("both Twilio alert thresholds must be configured".into()),
    };
    let sendgrid_thresholds = match (
        env_optional_i32("SWALLOW_ALERT_SENDGRID_GRACE_SECONDS")?,
        env_optional_i32("SWALLOW_ALERT_SENDGRID_HEARTBEAT_MAX_SECONDS")?,
    ) {
        (None, None) => None,
        (Some(g), Some(h)) => Some((g, h)),
        _ => return Err("both SendGrid alert thresholds must be configured".into()),
    };
    let queue_wait = match std::env::var("SWALLOW_ALERT_QUEUE_MAX_WAIT_SECONDS") {
        Ok(value) => Some(value.parse::<i32>()?),
        Err(std::env::VarError::NotPresent) => None,
        Err(_) => return Err("invalid queue wait threshold".into()),
    };
    if !resend_enabled
        && !attempts_enabled
        && !slack_jobs_enabled
        && !slack_policy_enabled
        && !failed_jobs_enabled
        && queue_wait.is_none()
        && twilio_thresholds.is_none()
        && sendgrid_thresholds.is_none()
    {
        return Err("no alert checks configured".into());
    }
    if (resend_enabled && (!(1..=86_400).contains(&grace) || !(1..=86_400).contains(&heartbeat)))
        || queue_wait.is_some_and(|v| !(1..=86_400).contains(&v))
        || sendgrid_thresholds
            .is_some_and(|(g, h)| !(1..=86_400).contains(&g) || !(1..=86_400).contains(&h))
        || twilio_thresholds
            .is_some_and(|(g, h)| !(1..=86_400).contains(&g) || !(1..=86_400).contains(&h))
    {
        return Err("invalid alert check threshold (1-86400 seconds)".into());
    }
    let mut failed_checks = [false; 5];
    let resend = if resend_enabled {
        let result = tokio::time::timeout(operation_timeout, async {
            if scoped_checks {
                reconciliation::scoped_provider_health(&db, &args[0], "resend", grace, heartbeat)
                    .await
            } else {
                reconciliation::resend_health(&db, &args[0], grace, heartbeat).await
            }
        })
        .await;
        let values = result.ok().and_then(Result::ok).and_then(|health| {
            Some((
                health["overdueUndelivered"].as_u64()?,
                health["projectorStale"].as_bool()?,
            ))
        });
        if values.is_none() {
            failed_checks[0] = true;
            eprintln!("Resend health check failed");
        }
        values
    } else {
        None
    };
    let queue = if let Some(wait) = queue_wait {
        let result = tokio::time::timeout(operation_timeout, async {
            if scoped_checks {
                reconciliation::scoped_queue_health(&db, &args[0], wait).await
            } else {
                reconciliation::queue_health(&db, &args[0], wait).await
            }
        })
        .await;
        let values = result.ok().and_then(Result::ok).and_then(|health| {
            Some((
                health["overdueJobs"].as_u64()?,
                health["expiredLeases"].as_u64()?,
            ))
        });
        if values.is_none() {
            failed_checks[1] = true;
            eprintln!("queue health check failed");
        }
        values
    } else {
        None
    };
    let attempts = if attempts_enabled {
        let result = tokio::time::timeout(operation_timeout, async {
            if scoped_checks {
                reconciliation::scoped_health(&db, &args[0]).await
            } else {
                reconciliation::health(&db, &args[0]).await
            }
        })
        .await;
        let values = result.ok().and_then(Result::ok).and_then(|health| {
            Some((
                health["unknownAttempts"].as_u64()?,
                health["strandedDispatches"].as_u64()?,
            ))
        });
        if values.is_none() {
            failed_checks[2] = true;
            eprintln!("delivery attempts health check failed");
        }
        values
    } else {
        None
    };
    let twilio = if let Some((grace, heartbeat)) = twilio_thresholds {
        let result = tokio::time::timeout(operation_timeout, async {
            if scoped_checks {
                reconciliation::scoped_provider_health(&db, &args[0], "twilio", grace, heartbeat)
                    .await
            } else {
                reconciliation::twilio_health(&db, &args[0], grace, heartbeat).await
            }
        })
        .await;
        let values = result.ok().and_then(Result::ok).and_then(|health| {
            Some((
                health["overdueUndelivered"].as_u64()?,
                health["projectorStale"].as_bool()?,
            ))
        });
        if values.is_none() {
            failed_checks[3] = true;
            eprintln!("Twilio health check failed");
        }
        values
    } else {
        None
    };
    let mut sendgrid_check_failed = false;
    let sendgrid = if let Some((grace, heartbeat)) = sendgrid_thresholds {
        let result = tokio::time::timeout(operation_timeout, async {
            if scoped_checks {
                reconciliation::scoped_sendgrid_health(&db, &args[0], grace, heartbeat).await
            } else {
                reconciliation::sendgrid_health(&db, &args[0], grace, heartbeat).await
            }
        })
        .await;
        let values = result.ok().and_then(Result::ok).and_then(|health| {
            Some((
                health["overdueUndelivered"].as_u64()?,
                health["projectorStale"].as_bool()?,
            ))
        });
        if values.is_none() {
            sendgrid_check_failed = true;
            eprintln!("SendGrid health check failed");
        }
        values
    } else {
        None
    };
    let slack_attempts = if slack_jobs_enabled {
        let scoped_slack = env_bool("SWALLOW_ALERT_SLACK_SCOPED_ACCESS", false)?;
        let result = tokio::time::timeout(operation_timeout, async {
            if scoped_slack {
                novu_postgres_queue::slack_queue::scoped_health(&db, &args[0]).await
            } else {
                novu_postgres_queue::slack_queue::health(&db, &args[0]).await
            }
        })
        .await;
        let values = result.ok().and_then(Result::ok).and_then(|health| {
            Some((
                health["unknownAttempts"].as_u64()?,
                health["rejectedAttempts"].as_u64()?,
                health["strandedDispatches"].as_u64()?,
                health["preDispatchFailures"].as_u64()?,
                if slack_missing_enabled {
                    Some(health["missingEndpoints"].as_u64()?)
                } else {
                    None
                },
            ))
        });
        if values.is_none() {
            failed_checks[4] = true;
            eprintln!("Slack chat health check failed");
        }
        values
    } else {
        None
    };
    let slack_policy_cancelled = if slack_policy_enabled {
        let scoped = env_bool("SWALLOW_ALERT_SLACK_SCOPED_ACCESS", false)?;
        let result = tokio::time::timeout(
            operation_timeout,
            novu_postgres_queue::slack_queue::policy_cancellations(&db, &args[0], scoped),
        )
        .await;
        match result {
            Ok(Ok(value)) if value >= 0 => Some(value as u64),
            _ => {
                failed_checks[4] = true;
                eprintln!("Slack policy cancellation check failed");
                None
            }
        }
    } else {
        None
    };
    let mut failed_jobs_check_failed = false;
    let failed_jobs = if failed_jobs_enabled {
        match tokio::time::timeout(
            operation_timeout,
            reconciliation::failed_queue_count(&db, &args[0]),
        )
        .await
        {
            Ok(Ok(count)) if count >= 0 => Some(count as u64),
            _ => {
                failed_jobs_check_failed = true;
                eprintln!("failed jobs health check failed");
                None
            }
        }
    } else {
        None
    };
    let unhealthy = sendgrid_check_failed
        || sendgrid.is_some_and(|(overdue, stale)| overdue > 0 || stale)
        || failed_jobs_check_failed
        || failed_jobs.is_some_and(|n| n > 0)
        || failed_checks.iter().any(|failed| *failed)
        || resend.is_some_and(|(overdue, stale)| overdue > 0 || stale)
        || queue.is_some_and(|(overdue, expired)| overdue > 0 || expired > 0)
        || attempts.is_some_and(|(unknown, stranded)| unknown > 0 || stranded > 0)
        || twilio.is_some_and(|(overdue, stale)| overdue > 0 || stale)
        || slack_policy_cancelled.is_some_and(|n| n > 0)
        || slack_attempts.is_some_and(|(unknown, rejected, stranded, pre_dispatch, missing)| {
            unknown > 0
                || rejected > 0
                || stranded > 0
                || pre_dispatch > 0
                || missing.is_some_and(|n| n > 0)
        });
    let (overdue, stale) = resend.unwrap_or((0, false));
    let alert = Alert {
        environment: args[0].clone(),
        overdue_undelivered: overdue,
        projector_stale: stale,
        resend_enabled: resend.is_some(),
        queue,
        attempts,
        twilio,
        sendgrid,
        sendgrid_check_failed,
        slack_attempts,
        slack_policy_cancelled,
        failed_checks,
        failed_jobs,
        failed_jobs_check_failed,
        database_probe: None,
        alert_store_unavailable: false,
    };
    alert.text().map_err(|_| "invalid alert environment")?;
    let mut observation_error = false;
    if on_new_condition {
        for (channel, target, enabled) in [
            ("slack", slack_id.as_str(), slack.is_some()),
            ("email", email_id.as_str(), email.is_some()),
        ] {
            if enabled
                && !matches!(
                    tokio::time::timeout(
                        operation_timeout,
                        alerts::observe_conditions(
                            &db,
                            &args[0],
                            channel,
                            target,
                            alert.condition_mask(),
                            alert.observed_condition_mask(),
                        )
                    )
                    .await,
                    Ok(Ok(()))
                )
            {
                observation_error = true;
                eprintln!("{channel} alert observation failed");
            }
        }
    }
    if !unhealthy {
        if configuration_error || observation_error {
            std::process::exit(1);
        }
        println!("{{\"status\":\"healthy\"}}");
        return Ok(());
    }
    let mut error = configuration_error
        || observation_error
        || failed_jobs_check_failed
        || failed_checks.iter().any(|failed| *failed);
    let mut needs_review = false;
    let mut other = if parallel_delivery && slack.is_some() && email.is_some() {
        match alerts::additional_delivery_connection(&config, tls, operation_timeout).await {
            Ok(client) => {
                let setup = tokio::time::timeout(
                    operation_timeout,
                    client.query_one(
                        "SELECT set_config('statement_timeout',$1,false)",
                        &[&statement_timeout],
                    ),
                )
                .await;
                if matches!(setup, Ok(Ok(_))) {
                    Some(client)
                } else {
                    error = true;
                    eprintln!("parallel alert connection setup failed; using sequential delivery");
                    None
                }
            }
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
            on_new: on_new_condition,
            scoped: scoped_delivery,
            timeout: operation_timeout,
        },
    )
    .await;
    for (channel, outcome) in [("Slack", slack_result), ("email", email_result)] {
        if let Some(outcome) = outcome {
            match outcome {
                Ok(Dispatch::NeedsReview) => {
                    needs_review = true;
                    eprintln!("{channel} alert requires operator review");
                }
                Ok(result) => println!("{channel} alert: {result:?}"),
                Err(_) => {
                    error = true;
                    eprintln!("{channel} alert dispatch failed");
                }
            }
        }
    }
    if error {
        std::process::exit(1);
    }
    if needs_review {
        std::process::exit(2);
    }
    Ok(())
}

fn env_bool(name: &str, default: bool) -> Result<bool, Box<dyn Error + Send + Sync>> {
    match std::env::var(name) {
        Ok(value) if value == "true" => Ok(true),
        Ok(value) if value == "false" => Ok(false),
        Err(std::env::VarError::NotPresent) => Ok(default),
        _ => Err(format!("{name} must be true or false").into()),
    }
}

fn env_optional_i32(name: &str) -> Result<Option<i32>, Box<dyn Error + Send + Sync>> {
    match std::env::var(name) {
        Ok(value) => Ok(Some(value.parse()?)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(_) => Err(format!("invalid {name}").into()),
    }
}

fn slack_config() -> Result<Option<SlackWebhook>, Box<dyn Error + Send + Sync>> {
    Ok(novu_rust_contracts::secret::from_env(
        "SWALLOW_ALERT_SLACK_WEBHOOK",
        "SWALLOW_ALERT_SLACK_WEBHOOK_FILE",
    )?
    .map(|s| SlackWebhook::new(&s).map_err(|_| "invalid Slack configuration"))
    .transpose()?)
}
