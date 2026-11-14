//! Local operator tool. Requires database access and an independently verified outcome.
use std::error::Error;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if ![2, 3, 4, 5, 6, 7].contains(&args.len()) {
        return Err("usage: delivery-reconcile check-failed-jobs ENV | list-failed-jobs ENV [AFTER_JOB] | ack-failed-job ENV JOB OPERATOR REASON | list-slack ENV [AFTER_JOB] | slack-ack ENV JOB OPERATOR REASON | operator-grant|operator-revoke ENV DB_LOGIN PERMISSION REASON | check|check-slack ENV | slack-resolve ENV JOB retry|accepted OPERATOR REASON | check-alert-delivery ENV | alert-status ENV | alert-resolve ENV CHANNEL TARGET DECISION OPERATOR REASON | check-queue ENV MAX_WAIT_SECONDS | check-resend|check-resend-metrics|check-twilio|check-twilio-metrics|check-sendgrid|check-sendgrid-metrics ENV GRACE_SECONDS HEARTBEAT_MAX_SECONDS | list ENV [AFTER_JOB AFTER_TARGET|-] | evidence ENV EMAIL_JOB | email-dispatch-evidence|sendgrid-result ENV EMAIL_JOB | resend-evidence|resend-check|resend-result ENV EMAIL_JOB | sms-evidence|sms-delivery|twilio-result ENV SMS_JOB | retry|accepted ENV JOB TARGET_INDEX|- OPERATOR REASON".into());
    }
    let (config, tls) = novu_rust_contracts::postgres::configure(&std::env::var("DATABASE_URL")?)?;
    let (mut db, connection) = config.connect(tls).await?;
    tokio::spawn(async move {
        if connection.await.is_err() {
            eprintln!("database connection failed");
        }
    });
    if args.len() == 2 && args[0] == "check-failed-jobs" {
        let count = novu_postgres_queue::reconciliation::failed_queue_count(&db, &args[1]).await?;
        println!("{}", serde_json::json!({"unacknowledgedFailedJobs":count}));
        if count > 0 {
            std::process::exit(2);
        }
    } else if (args.len() == 2 || args.len() == 3) && args[0] == "list-failed-jobs" {
        let after = args.get(2).map(|s| Uuid::parse_str(s)).transpose()?;
        let jobs =
            novu_postgres_queue::reconciliation::failed_queue_list(&db, &args[1], after).await?;
        println!("{}", serde_json::to_string(&jobs)?);
    } else if args.len() == 5 && args[0] == "ack-failed-job" {
        let changed = novu_postgres_queue::reconciliation::acknowledge_failed_queue(
            &db,
            &args[1],
            Uuid::parse_str(&args[2])?,
            &args[3],
            &args[4],
        )
        .await?;
        println!(
            "{}",
            if changed {
                "queue failure acknowledgement recorded"
            } else {
                "queue failure acknowledgement unchanged"
            }
        );
    } else if (args.len() == 2 || args.len() == 3) && args[0] == "list-slack" {
        let after = args.get(2).map(|s| Uuid::parse_str(s)).transpose()?;
        let rows = novu_postgres_queue::slack_queue::list_attention(&db, &args[1], after).await?;
        println!("{}", serde_json::to_string(&rows)?);
    } else if args.len() == 5 && args[0] == "slack-ack" {
        let changed = novu_postgres_queue::slack_queue::acknowledge_workflow(
            &mut db,
            &args[1],
            Uuid::parse_str(&args[2])?,
            &args[3],
            &args[4],
        )
        .await?;
        println!(
            "{}",
            if changed {
                "Slack workflow acknowledgement recorded"
            } else {
                "Slack workflow acknowledgement unchanged"
            }
        );
    } else if args.len() == 5 && ["operator-grant", "operator-revoke"].contains(&args[0].as_str()) {
        let action = if args[0] == "operator-grant" {
            "grant"
        } else {
            "revoke"
        };
        let changed: bool = db
            .query_one(
                "SELECT public.novu_rust_manage_operator_grant($1,$2,$3,$4,$5)",
                &[&args[1], &args[2], &args[3], &action, &args[4]],
            )
            .await?
            .get(0);
        println!(
            "{}",
            if changed {
                "operator grant changed"
            } else {
                "operator grant unchanged"
            }
        );
    } else if args.len() == 2 && args[0] == "check" {
        let health = if monitor_scoped_access()? {
            novu_postgres_queue::reconciliation::scoped_health(&db, &args[1]).await?
        } else {
            novu_postgres_queue::reconciliation::health(&db, &args[1]).await?
        };
        println!("{}", serde_json::to_string(&health)?);
        if health["unknownAttempts"]
            .as_i64()
            .ok_or("invalid health result")?
            > 0
            || health["strandedDispatches"]
                .as_i64()
                .ok_or("invalid health result")?
                > 0
        {
            std::process::exit(2);
        }
    } else if args.len() == 2 && args[0] == "check-slack" {
        let scoped = match std::env::var("SWALLOW_SLACK_SCOPED_ACCESS").as_deref() {
            Ok("true") => true,
            Ok("false") | Err(std::env::VarError::NotPresent) => false,
            _ => return Err("invalid Slack scoped access setting".into()),
        };
        let health = if scoped {
            novu_postgres_queue::slack_queue::scoped_health(&db, &args[1]).await?
        } else {
            novu_postgres_queue::slack_queue::health(&db, &args[1]).await?
        };
        let missing = match std::env::var("SWALLOW_SLACK_CHECK_MISSING_ENDPOINTS").as_deref() {
            Ok("true") => true,
            Ok("false") | Err(std::env::VarError::NotPresent) => false,
            _ => return Err("invalid Slack missing endpoint check".into()),
        };
        println!("{}", serde_json::to_string(&health)?);
        if [
            "unknownAttempts",
            "rejectedAttempts",
            "strandedDispatches",
            "preDispatchFailures",
        ]
        .iter()
        .any(|key| health[*key].as_i64().is_none_or(|n| n > 0))
            || (missing && health["missingEndpoints"].as_i64().is_none_or(|n| n > 0))
        {
            std::process::exit(2);
        }
    } else if args.len() == 6 && args[0] == "slack-resolve" {
        novu_postgres_queue::slack_queue::resolve(
            &mut db,
            &args[1],
            Uuid::parse_str(&args[2])?,
            &args[3],
            &args[4],
            &args[5],
        )
        .await?;
        println!("Slack resolution recorded");
    } else if args.len() == 3 && args[0] == "check-queue" {
        let wait = args[2].parse()?;
        let health = if monitor_scoped_access()? {
            novu_postgres_queue::reconciliation::scoped_queue_health(&db, &args[1], wait).await?
        } else {
            novu_postgres_queue::reconciliation::queue_health(&db, &args[1], wait).await?
        };
        println!("{}", serde_json::to_string(&health)?);
        if health["overdueJobs"]
            .as_i64()
            .ok_or("invalid health result")?
            > 0
            || health["expiredLeases"]
                .as_i64()
                .ok_or("invalid health result")?
                > 0
        {
            std::process::exit(2);
        }
    } else if args.len() == 4
        && [
            "check-resend",
            "check-resend-metrics",
            "check-twilio",
            "check-twilio-metrics",
            "check-sendgrid",
            "check-sendgrid-metrics",
        ]
        .contains(&args[0].as_str())
    {
        let twilio = args[0].starts_with("check-twilio");
        let sendgrid = args[0].starts_with("check-sendgrid");
        let health = if monitor_scoped_access()? && sendgrid {
            novu_postgres_queue::reconciliation::scoped_sendgrid_health(
                &db,
                &args[1],
                args[2].parse()?,
                args[3].parse()?,
            )
            .await?
        } else if monitor_scoped_access()? {
            novu_postgres_queue::reconciliation::scoped_provider_health(
                &db,
                &args[1],
                if twilio { "twilio" } else { "resend" },
                args[2].parse()?,
                args[3].parse()?,
            )
            .await?
        } else if sendgrid {
            novu_postgres_queue::reconciliation::sendgrid_health(
                &db,
                &args[1],
                args[2].parse()?,
                args[3].parse()?,
            )
            .await?
        } else if twilio {
            novu_postgres_queue::reconciliation::twilio_health(
                &db,
                &args[1],
                args[2].parse()?,
                args[3].parse()?,
            )
            .await?
        } else {
            novu_postgres_queue::reconciliation::resend_health(
                &db,
                &args[1],
                args[2].parse()?,
                args[3].parse()?,
            )
            .await?
        };
        let overdue = health["overdueUndelivered"]
            .as_i64()
            .ok_or("invalid health result")?;
        let stale = health["projectorStale"]
            .as_bool()
            .ok_or("invalid health result")?;
        if args[0].ends_with("-metrics") {
            let provider = if sendgrid {
                "sendgrid"
            } else if twilio {
                "twilio"
            } else {
                "resend"
            };
            if args[1].chars().any(|c| c.is_control() && c != '\n') {
                return Err("invalid environment for metrics".into());
            }
            let environment = args[1]
                .replace('\\', "\\\\")
                .replace('\n', "\\n")
                .replace('"', "\\\"");
            let eligible = health["eligibleJobs"]
                .as_i64()
                .ok_or("invalid health result")?;
            println!(
                "# TYPE swallow_{provider}_overdue_undelivered gauge\nswallow_{provider}_overdue_undelivered{{environment=\"{environment}\"}} {overdue}\n# TYPE swallow_{provider}_projector_stale gauge\nswallow_{provider}_projector_stale{{environment=\"{environment}\"}} {}\n# TYPE swallow_{provider}_eligible_jobs gauge\nswallow_{provider}_eligible_jobs{{environment=\"{environment}\"}} {eligible}",
                i32::from(stale)
            );
        } else {
            println!("{}", serde_json::to_string(&health)?);
        }
        if !args[0].ends_with("-metrics") && (overdue > 0 || stale) {
            std::process::exit(2);
        }
    } else if args.len() == 2 && args[0] == "check-alert-delivery" {
        let health = if alert_scoped_access()? {
            novu_postgres_queue::alerts::scoped_health(&db, &args[1]).await?
        } else {
            novu_postgres_queue::alerts::health(&db, &args[1]).await?
        };
        println!("{}", serde_json::to_string(&health)?);
        if ["unknown", "rejected", "expired"]
            .iter()
            .any(|key| health[*key].as_i64().is_none_or(|count| count > 0))
        {
            std::process::exit(2);
        }
    } else if args.len() == 2 && args[0] == "alert-status" {
        let status = if alert_scoped_access()? {
            novu_postgres_queue::alerts::scoped_status(&db, &args[1]).await?
        } else {
            novu_postgres_queue::alerts::status(&db, &args[1]).await?
        };
        println!("{}", serde_json::to_string(&status)?);
    } else if args.len() == 7 && args[0] == "alert-resolve" {
        novu_postgres_queue::alerts::resolve(
            &mut db, &args[1], &args[2], &args[3], &args[4], &args[5], &args[6],
        )
        .await?;
        println!("alert resolution recorded");
    } else if (args.len() == 2 || args.len() == 4) && args[0] == "list" {
        let after = if args.len() == 4 {
            let job = Uuid::parse_str(&args[2])?;
            let target = if args[3] == "-" {
                None
            } else {
                Some(args[3].parse::<i32>()?)
            };
            Some((job, target))
        } else {
            None
        };
        let rows = if monitor_scoped_access()? {
            novu_postgres_queue::reconciliation::scoped_list_after(&db, &args[1], after).await?
        } else {
            novu_postgres_queue::reconciliation::list_after(&db, &args[1], after).await?
        };
        println!("{}", serde_json::to_string(&rows)?);
    } else if args.len() == 3 && args[0] == "evidence" {
        println!(
            "{}",
            serde_json::to_string(
                &novu_postgres_queue::reconciliation::email_evidence(
                    &db,
                    &args[1],
                    Uuid::parse_str(&args[2])?
                )
                .await?
            )?
        );
    } else if args.len() == 3 && args[0] == "sendgrid-result" {
        let result = novu_postgres_queue::reconciliation::scoped_sendgrid_result(
            &db,
            &args[1],
            Uuid::parse_str(&args[2])?,
        )
        .await?;
        println!("{}", serde_json::to_string(&result)?);
    } else if args.len() == 3 && args[0] == "email-dispatch-evidence" {
        println!(
            "{}",
            serde_json::to_string(
                &novu_postgres_queue::reconciliation::email_dispatch_evidence(
                    &db,
                    &args[1],
                    Uuid::parse_str(&args[2])?
                )
                .await?
            )?
        );
    } else if args.len() == 3 && ["resend-result", "twilio-result"].contains(&args[0].as_str()) {
        let provider = if args[0] == "resend-result" {
            "resend"
        } else {
            "twilio"
        };
        let result = novu_postgres_queue::reconciliation::scoped_channel_result(
            &db,
            &args[1],
            Uuid::parse_str(&args[2])?,
            provider,
        )
        .await?;
        println!("{}", serde_json::to_string(&result)?);
    } else if args.len() == 3 && args[0] == "resend-check" {
        let evidence = novu_postgres_queue::reconciliation::resend_evidence(
            &db,
            &args[1],
            Uuid::parse_str(&args[2])?,
        )
        .await?;
        let attention = novu_postgres_queue::reconciliation::resend_needs_attention(&evidence)?;
        println!(
            "{}",
            serde_json::to_string(
                &serde_json::json!({"needsAttention":attention,"evidence":evidence})
            )?
        );
        if attention {
            std::process::exit(2);
        }
    } else if args.len() == 3 && args[0] == "resend-evidence" {
        println!(
            "{}",
            serde_json::to_string(
                &novu_postgres_queue::reconciliation::resend_evidence(
                    &db,
                    &args[1],
                    Uuid::parse_str(&args[2])?
                )
                .await?
            )?
        );
    } else if args.len() == 3 && args[0] == "sms-delivery" {
        println!(
            "{}",
            serde_json::to_string(
                &novu_postgres_queue::reconciliation::sms_delivery(
                    &db,
                    &args[1],
                    Uuid::parse_str(&args[2])?
                )
                .await?
            )?
        );
    } else if args.len() == 3 && args[0] == "sms-evidence" {
        println!(
            "{}",
            serde_json::to_string(
                &novu_postgres_queue::reconciliation::sms_evidence(
                    &db,
                    &args[1],
                    Uuid::parse_str(&args[2])?
                )
                .await?
            )?
        );
    } else if args.len() == 6 && ["retry", "accepted"].contains(&args[0].as_str()) {
        let job = Uuid::parse_str(&args[2])?;
        let target = if args[3] == "-" {
            None
        } else {
            Some(args[3].parse::<i32>()?)
        };
        novu_postgres_queue::reconciliation::resolve(
            &mut db, &args[1], job, target, &args[0], &args[4], &args[5],
        )
        .await?;
        println!("reconciliation recorded");
    } else {
        return Err("invalid reconciliation command".into());
    }
    Ok(())
}

fn alert_scoped_access() -> Result<bool, Box<dyn Error + Send + Sync>> {
    match std::env::var("SWALLOW_ALERT_STATUS_SCOPED_ACCESS").as_deref() {
        Ok("true") => Ok(true),
        Ok("false") | Err(std::env::VarError::NotPresent) => Ok(false),
        _ => Err("invalid alert status scoped access setting".into()),
    }
}

fn monitor_scoped_access() -> Result<bool, Box<dyn Error + Send + Sync>> {
    match std::env::var("SWALLOW_MONITOR_SCOPED_ACCESS").as_deref() {
        Ok("true") => Ok(true),
        Ok("false") | Err(std::env::VarError::NotPresent) => Ok(false),
        _ => Err("invalid monitor scoped access setting".into()),
    }
}
