//! Local operator tool. Requires database access and an independently verified outcome.
use std::error::Error;
use uuid::Uuid;

async fn resend_evidence_for_cli(
    db: &mut tokio_postgres::Client,
    environment: &str,
    job: Uuid,
) -> Result<serde_json::Value, Box<dyn Error + Send + Sync>> {
    match std::env::var("NOVU_RESEND_CHECK_AUTO_CORRELATION").as_deref() {
        Ok("true") => {
            novu_postgres_queue::reconciliation::resend_evidence_snapshot_with_auto(
                db,
                environment,
                job,
                true,
            )
            .await
        }
        Ok("false") => {
            novu_postgres_queue::reconciliation::resend_evidence_snapshot_with_auto(
                db,
                environment,
                job,
                false,
            )
            .await
        }
        Err(std::env::VarError::NotPresent) => {
            novu_postgres_queue::reconciliation::resend_evidence_snapshot_detect_auto(
                db,
                environment,
                job,
            )
            .await
        }
        _ => Err("invalid Resend correlation check setting".into()),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if ![2, 3, 4, 5, 6, 7, 8, 9].contains(&args.len()) {
        return Err("usage: delivery-reconcile discord-retry-policy-read ENV DEST | discord-retry-policy-set ENV DEST REQUEST_ID EXPECTED MODE MAX FALLBACK REASON | discord-retry-policy-request ENV DEST REQUEST_ID | discord-retry-policy-audit ENV DEST [BEFORE] | discord-policy-read ENV | discord-policy-set ENV REQUEST_ID EXPECTED_REVISION enabled|disabled REASON | discord-policy-request ENV REQUEST_ID | discord-policy-audit ENV [BEFORE] | list-discord ENV [AFTER_JOB] | ack-discord ENV JOB ATTEMPT_REVISION|- FAILURE_REVISION OPERATOR REASON | smtp-scope-read ENV LOGIN | smtp-scope-set ENV REQUEST_ID LOGIN absent|enabled|disabled enabled|disabled REASON | smtp-scope-request ENV REQUEST_ID | smtp-scope-audit ENV [AFTER_ID] | smtp-resolution-history ENV JOB [BEFORE_ACTOR_OID BEFORE_REQUEST_ID] | resolve-smtp ENV REQUEST_ID JOB ATTEMPT_REVISION FAILURE_REVISION accepted|retry REASON | get-smtp ENV JOB | check-smtp ENV | list-smtp ENV [AFTER_JOB] | ack-smtp ENV JOB ATTEMPT_REVISION|- FAILURE_REVISION OPERATOR REASON | check-discord ENV | check-discord-retry ENV GRACE_SECONDS | list-discord-retry ENV GRACE_SECONDS all|overdue [AFTER_JOB] | list-fcm-unrecorded-claims ENV MIN_GAP GRACE_SECONDS [AFTER_JOB] | check-fcm-unrecorded-claims ENV MIN_GAP GRACE_SECONDS | check-fcm-credentials ENV MAX_AGE_SECONDS MIN_HEALTHY | check-channel-policy ENV email|sms|push|all | list-channel-policy ENV email|sms|push|all [AFTER_JOB] | ack-channel-policy ENV JOB REVISION OPERATOR REASON | push-policy-evidence ENV JOB REVISION | list-skipped-fcm ENV JOB [AFTER_TARGET] | list-cancelled-push-resolutions ENV JOB REVISION [AFTER_TARGET] | resolve-cancelled-push ENV JOB REVISION TARGET accepted|not_accepted OPERATOR REASON | check-failed-jobs ENV | list-failed-jobs ENV [AFTER_JOB] | ack-failed-job ENV JOB OPERATOR REASON | list-failed-jobs-v2 ENV [AFTER_JOB] | ack-failed-job-v2 ENV JOB REVISION OPERATOR REASON | list-slack ENV [AFTER_JOB] | slack-ack ENV JOB OPERATOR REASON | operator-scope ENV | operator-scope-set ENV DB_LOGIN GRANT_JSON REVOKE_JSON REASON | operator-scope-audit ENV [AFTER_ID] | operator-grants ENV [AFTER_LOGIN AFTER_PERMISSION] | operator-grant-audit ENV [AFTER_ID] | operator-grant|operator-revoke ENV DB_LOGIN PERMISSION REASON | check|check-slack ENV | slack-resolve ENV JOB retry|accepted OPERATOR REASON | list-slack-v2 ENV [AFTER_JOB] | slack-resolve-v2 ENV JOB ATTEMPT_ID retry|accepted OPERATOR REASON | slack-retry-after ENV JOB ATTEMPT_ID OPERATOR REASON | check-alert-delivery ENV | alert-status ENV | alert-resolve ENV CHANNEL TARGET DECISION OPERATOR REASON | alert-resolve-v2 ENV CHANNEL TARGET ATTEMPT_ID DECISION OPERATOR REASON | check-queue ENV MAX_WAIT_SECONDS | list-resend-auto ENV GRACE_SECONDS FILTER [AFTER_JOB] | check-resend-auto ENV GRACE_SECONDS | check-twilio-delayed ENV GRACE_SECONDS | check-fcm-partial ENV WINDOW_SECONDS | check-unreviewed-fcm ENV | list-unreviewed-fcm ENV [AFTER_JOB] | ack-fcm-partial ENV JOB REVISION OPERATOR REASON | check-slack-cooldown ENV GRACE_SECONDS | check-resend-cooldown ENV GRACE_SECONDS | check-fcm-cooldown ENV GRACE_SECONDS | check-resend|check-resend-metrics|check-twilio|check-twilio-metrics|check-sendgrid|check-sendgrid-metrics ENV GRACE_SECONDS HEARTBEAT_MAX_SECONDS | list ENV [AFTER_JOB AFTER_TARGET|-] | evidence ENV EMAIL_JOB | email-dispatch-evidence|sendgrid-result ENV EMAIL_JOB | resend-evidence|resend-check|resend-result ENV EMAIL_JOB | sms-evidence|sms-delivery|twilio-result ENV SMS_JOB | resend-cooldown-policy-read ENV | resend-cooldown-policy-set ENV REQUEST_ID EXPECTED FALLBACK_SECONDS REASON | resend-cooldown-policy-request ENV REQUEST_ID | resend-cooldown-policy-audit ENV [BEFORE] | resend-auto-policy-read ENV | resend-auto-policy-set ENV EXPECTED enabled|disabled MAX now|preserve REASON | resend-auto-policy-set-v2 ENV REQUEST_ID EXPECTED enabled|disabled MAX now|preserve REASON | resend-auto-policy-request ENV REQUEST_ID | resend-auto-policy-audit ENV [BEFORE] | twilio-retry-guidance ENV JOB | twilio-retry-after ENV JOB DISPATCH_ID OPERATOR REASON | resend-retry-after ENV JOB OPERATOR REASON | retry|accepted ENV JOB TARGET_INDEX|- OPERATOR REASON".into());
    }
    let (config, tls) = novu_rust_contracts::postgres::configure(&std::env::var("DATABASE_URL")?)?;
    let (mut db, connection) = config.connect(tls).await?;
    tokio::spawn(async move {
        if connection.await.is_err() {
            eprintln!("database connection failed");
        }
    });
    if (args.len() == 4 || args.len() == 5) && args[0] == "list-resend-auto" {
        let grace = args[2].parse::<i32>()?;
        let after = args
            .get(4)
            .map(|value| Uuid::parse_str(value))
            .transpose()?;
        let row = db
            .query_one(
                "SELECT public.novu_rust_resend_auto_retry_jobs($1,$2,$3,$4)",
                &[&args[1], &grace, &args[3], &after],
            )
            .await?;
        println!("{}", row.get::<_, serde_json::Value>(0));
    } else if args.len()==2 && args[0]=="check-unreviewed-fcm" {
        let count=novu_postgres_queue::reconciliation::fcm_unreviewed_health(&db,&args[1]).await?;
        println!("{}",serde_json::json!({"unreviewedJobs":count}));
        if count!=0 {std::process::exit(2);}
    } else if (args.len()==2 || args.len()==3) && args[0]=="list-unreviewed-fcm" {
        let after=args.get(2).map(|v|Uuid::parse_str(v)).transpose()?;
        let rows=db.query("SELECT job_id,accepted,rejected,revision FROM public.novu_rust_fcm_partial_unreviewed($1,$2)",&[&args[1],&after]).await?;
        let values:Vec<_>=rows.iter().map(|r|serde_json::json!({"jobId":r.get::<_,Uuid>(0),"acceptedCount":r.get::<_,i64>(1),"rejectedCount":r.get::<_,i64>(2),"revision":r.get::<_,String>(3)})).collect();
        println!("{}",serde_json::json!({"data":values}));
    } else if args.len()==6 && args[0]=="ack-fcm-partial" {
        let job=Uuid::parse_str(&args[2])?;
        let row=db.query_one("SELECT public.novu_rust_fcm_partial_ack($1,$2,$3,$4,$5)",&[&args[1],&job,&args[3],&args[4],&args[5]]).await?;
        println!("{}",serde_json::json!({"recorded":row.get::<_,bool>(0)}));
    } else if args.len() == 3 && args[0] == "check-fcm-partial" {
        let count = novu_postgres_queue::reconciliation::fcm_partial_health(&db, &args[1], args[2].parse()?).await?;
        println!("{}", serde_json::json!({"partialJobs":count}));
        if count != 0 { std::process::exit(2); }
    } else if args.len() == 3 && args[0] == "check-twilio-delayed" {
        let health=novu_postgres_queue::reconciliation::twilio_delayed_retry_health(&db,&args[1],args[2].parse()?).await?;
        println!("{}",health);
        if ["pendingOverdue","expiredLease","callbackBlocked"].iter().any(|key|health[*key].as_i64().is_none_or(|v|v!=0)) { std::process::exit(2); }
    } else if args.len() == 3 && args[0] == "check-fcm-cooldown" {
        let grace=args[2].parse::<i32>()?;
        let health=novu_postgres_queue::fcm_queue::cooldown_health(&db,&args[1],grace).await?;
        println!("{}",health);
        if health["active"].as_bool()!=Some(false) || health["deferred"].as_i64()!=Some(0) || health["overdue"].as_i64()!=Some(0) {std::process::exit(2);}
    } else if args.len() == 3 && args[0] == "check-resend-cooldown" {
        let grace=args[2].parse::<i32>()?;
        let health=novu_postgres_queue::resend_cooldown::health(&db,&args[1],grace).await?;
        println!("{}",health);
        if health["active"].as_bool()!=Some(false) || health["deferred"].as_i64()!=Some(0) || health["overdue"].as_i64()!=Some(0) { std::process::exit(2); }
    } else if args.len() == 3 && args[0] == "check-slack-cooldown" {
        let grace=args[2].parse::<i32>()?;
        let health=novu_postgres_queue::slack_cooldown::health(&db,&args[1],grace).await?;
        println!("{}",health);
        if health["active"].as_bool()!=Some(false) || health["deferred"].as_i64()!=Some(0) || health["overdue"].as_i64()!=Some(0) { std::process::exit(2); }
    } else if args.len() == 3 && args[0] == "check-resend-auto" {
        let grace = args[2].parse::<i32>()?;
        let health =
            novu_postgres_queue::reconciliation::resend_auto_health(&db, &args[1], grace).await?;
        println!("{}", health);
        if ["rateLimited", "exhausted", "guardDeferred", "overdueAuto"]
            .iter()
            .any(|key| health[*key].as_i64().is_none_or(|v| v != 0))
        {
            std::process::exit(2);
        }
    } else if args.len() == 4 && args[0] == "check-fcm-credentials" {
        let age: i32 = args[2].parse()?;
        let minimum: i64 = args[3].parse()?;
        if !(1..=86_400).contains(&age) || !(1..=1000).contains(&minimum) {
            return Err("invalid FCM credential health threshold".into());
        }
        let row=db.query_one("SELECT healthy,unhealthy,stale FROM public.novu_rust_fcm_credential_health_check($1,$2)",&[&args[1],&age]).await?;
        let healthy: i64 = row.get(0);
        let unhealthy: i64 = row.get(1);
        let stale: i64 = row.get(2);
        println!(
            "{}",
            serde_json::json!({"healthy":healthy,"unhealthy":unhealthy,"stale":stale,"minimumHealthy":minimum})
        );
        if healthy < minimum || unhealthy > 0 {
            std::process::exit(2);
        }
    } else if (args.len() == 3 || args.len() == 4) && args[0] == "list-skipped-fcm" {
        let after = args
            .get(3)
            .map(|s| s.parse::<i32>())
            .transpose()?
            .unwrap_or(-1);
        let rows = novu_postgres_queue::reconciliation::list_skipped_fcm(
            &db,
            &args[1],
            Uuid::parse_str(&args[2])?,
            after,
        )
        .await?;
        println!("{}", serde_json::to_string(&rows)?);
    } else if args.len() == 8 && args[0] == "resolve-cancelled-push" {
        novu_postgres_queue::reconciliation::resolve_cancelled_push(
            &db,
            &args[1],
            Uuid::parse_str(&args[2])?,
            args[3].parse()?,
            args[4].parse()?,
            &args[5],
            &args[6],
            &args[7],
        )
        .await?;
        println!("cancelled push resolution recorded");
    } else if (args.len() == 4 || args.len() == 5) && args[0] == "list-cancelled-push-resolutions" {
        let after = args
            .get(4)
            .map(|s| s.parse::<i32>())
            .transpose()?
            .unwrap_or(-1);
        let result = novu_postgres_queue::reconciliation::list_cancelled_push_resolutions(
            &db,
            &args[1],
            Uuid::parse_str(&args[2])?,
            args[3].parse()?,
            after,
        )
        .await?;
        println!("{}", serde_json::to_string(&result)?);
    } else if args.len() == 4 && args[0] == "push-policy-evidence" {
        let evidence = novu_postgres_queue::reconciliation::push_policy_evidence(
            &db,
            &args[1],
            Uuid::parse_str(&args[2])?,
            args[3].parse::<i64>()?,
        )
        .await?;
        println!("{}", serde_json::to_string(&evidence)?);
    } else if args.len() == 3 && args[0] == "check-channel-policy" {
        let count =
            novu_postgres_queue::reconciliation::channel_policy_count(&db, &args[1], &args[2])
                .await?;
        println!(
            "{}",
            serde_json::json!({"channel":args[2],"unacknowledgedPolicyCancellations":count})
        );
        if count > 0 {
            std::process::exit(2);
        }
    } else if (args.len() == 3 || args.len() == 4) && args[0] == "list-channel-policy" {
        let after = args.get(3).map(|s| Uuid::parse_str(s)).transpose()?;
        let rows = novu_postgres_queue::reconciliation::channel_policy_list(
            &db, &args[1], &args[2], after,
        )
        .await?;
        println!("{}", serde_json::to_string(&rows)?);
    } else if args.len() == 6 && args[0] == "ack-channel-policy" {
        let changed = novu_postgres_queue::reconciliation::acknowledge_channel_policy(
            &db,
            &args[1],
            Uuid::parse_str(&args[2])?,
            args[3].parse::<i64>()?,
            &args[4],
            &args[5],
        )
        .await?;
        println!(
            "{}",
            if changed {
                "policy cancellation acknowledgement recorded"
            } else {
                "policy cancellation acknowledgement unchanged"
            }
        );
    } else if args.len() == 2 && args[0] == "check-failed-jobs" {
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
    } else if (args.len() == 2 || args.len() == 3) && args[0] == "list-failed-jobs-v2" {
        let after = args.get(2).map(|s| Uuid::parse_str(s)).transpose()?;
        let jobs =
            novu_postgres_queue::reconciliation::failed_queue_list_v2(&db, &args[1], after).await?;
        println!("{}", serde_json::to_string(&jobs)?);
    } else if args.len() == 6 && args[0] == "ack-failed-job-v2" {
        let changed = novu_postgres_queue::reconciliation::acknowledge_failed_queue_v2(
            &db,
            &args[1],
            Uuid::parse_str(&args[2])?,
            args[3].parse()?,
            &args[4],
            &args[5],
        )
        .await?;
        println!(
            "{}",
            if changed {
                "failure acknowledgement recorded"
            } else {
                "failure acknowledgement unchanged"
            }
        );
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
    } else if (args.len() == 2 || args.len() == 3) && args[0] == "list-slack-v2" {
        let after = args.get(2).map(|s| Uuid::parse_str(s)).transpose()?;
        let rows =
            novu_postgres_queue::slack_queue::list_attention_v2(&db, &args[1], after).await?;
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
    } else if args.len() == 6 && args[0] == "operator-scope-set" {
        let grant: Option<Vec<String>> = serde_json::from_str(&args[3])?;
        let revoke: Option<Vec<String>> = serde_json::from_str(&args[4])?;
        let changed: bool = db
            .query_one(
                "SELECT public.novu_rust_set_operator_admin_scope($1,$2,$3,$4,$5)",
                &[&args[1], &args[2], &grant, &revoke, &args[5]],
            )
            .await?
            .get(0);
        println!(
            "{}",
            if changed {
                "operator scope changed"
            } else {
                "operator scope unchanged"
            }
        );
    } else if (args.len() == 2 || args.len() == 3) && args[0] == "operator-scope-audit" {
        let after = args.get(2).map(|s| s.parse::<i64>()).transpose()?;
        let rows = db
            .query(
                "SELECT * FROM public.novu_rust_list_operator_scope_audit($1,$2)",
                &[&args[1], &after],
            )
            .await?;
        let audit: Vec<_> = rows.iter().map(|r| serde_json::json!({"id":r.get::<_,i64>(0).to_string(),"actor":r.get::<_,String>(1),"target":r.get::<_,String>(2),"previousGrant":r.get::<_,Option<Vec<String>>>(3),"previousRevoke":r.get::<_,Option<Vec<String>>>(4),"grant":r.get::<_,Option<Vec<String>>>(5),"revoke":r.get::<_,Option<Vec<String>>>(6),"reason":r.get::<_,String>(7),"createdAt":r.get::<_,String>(8)})).collect();
        println!("{}", serde_json::to_string(&audit)?);
    } else if args.len() == 2 && args[0] == "operator-scope" {
        let row = db
            .query_one(
                "SELECT * FROM public.novu_rust_current_operator_admin_scope($1)",
                &[&args[1]],
            )
            .await?;
        println!(
            "{}",
            serde_json::json!({"grant":row.get::<_,Option<Vec<String>>>(0),"revoke":row.get::<_,Option<Vec<String>>>(1)})
        );
    } else if (args.len() == 2 || args.len() == 4) && args[0] == "operator-grants" {
        let after_principal = args.get(2).map(String::as_str);
        let after_permission = args.get(3).map(String::as_str);
        let rows = db
            .query(
                "SELECT * FROM public.novu_rust_list_operator_grants($1,$2,$3)",
                &[&args[1], &after_principal, &after_permission],
            )
            .await?;
        let grants: Vec<_> = rows.iter().map(|r| serde_json::json!({"databasePrincipal":r.get::<_,String>(0),"permission":r.get::<_,String>(1),"identityCurrent":r.get::<_,bool>(2)})).collect();
        println!("{}", serde_json::to_string(&grants)?);
    } else if (args.len() == 2 || args.len() == 3) && args[0] == "operator-grant-audit" {
        let after = args.get(2).map(|s| s.parse::<i64>()).transpose()?;
        let rows = db
            .query(
                "SELECT * FROM public.novu_rust_list_operator_grant_audit($1,$2)",
                &[&args[1], &after],
            )
            .await?;
        let audit: Vec<_> = rows.iter().map(|r| serde_json::json!({"id":r.get::<_,i64>(0).to_string(),"actor":r.get::<_,String>(1),"target":r.get::<_,String>(2),"permission":r.get::<_,String>(3),"action":r.get::<_,String>(4),"reason":r.get::<_,String>(5),"createdAt":r.get::<_,String>(6)})).collect();
        println!("{}", serde_json::to_string(&audit)?);
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
    } else if args.len()==3 && args[0]=="discord-retry-policy-read" {
        let row=db.query_one("SELECT public.novu_rust_discord_retry_policy_read($1,$2)",&[&args[1],&args[2]]).await?;println!("{}",row.get::<_,serde_json::Value>(0));
    } else if args.len()==9 && args[0]=="discord-retry-policy-set" {
        let request=Uuid::parse_str(&args[3])?;let revision=args[4].parse::<i64>()?;let max=args[6].parse::<i32>()?;let fallback=args[7].parse::<i32>()?;
        let row=db.query_one("SELECT public.novu_rust_discord_retry_policy_set($1,$2,$3,$4,$5,$6,$7,$8)",&[&args[1],&args[2],&request,&revision,&args[5],&max,&fallback,&args[8]]).await?;println!("{}",row.get::<_,serde_json::Value>(0));
    } else if args.len()==4 && args[0]=="discord-retry-policy-request" {
        let request=Uuid::parse_str(&args[3])?;let row=db.query_one("SELECT public.novu_rust_discord_retry_policy_request($1,$2,$3)",&[&args[1],&args[2],&request]).await?;println!("{}",row.get::<_,serde_json::Value>(0));
    } else if (args.len()==3 || args.len()==4) && args[0]=="discord-retry-policy-audit" {
        let before=args.get(3).map(|v|v.parse::<i64>()).transpose()?;let row=db.query_one("SELECT public.novu_rust_discord_retry_policy_audit_read($1,$2,$3)",&[&args[1],&args[2],&before]).await?;println!("{}",row.get::<_,serde_json::Value>(0));
    } else if args.len()==2 && args[0]=="discord-policy-read" {
        let row=db.query_one("SELECT public.novu_rust_discord_policy_read($1)",&[&args[1]]).await?;
        println!("{}",row.get::<_,serde_json::Value>(0));
    } else if args.len()==6 && args[0]=="discord-policy-set" {
        let request=Uuid::parse_str(&args[2])?;let revision=args[3].parse::<i64>()?;
        let enabled=match args[4].as_str() {"enabled"=>true,"disabled"=>false,_=>return Err("invalid Discord policy".into())};
        let row=db.query_one("SELECT public.novu_rust_discord_policy_set($1,$2,$3,$4,$5)",&[&args[1],&request,&revision,&enabled,&args[5]]).await?;
        println!("{}",row.get::<_,serde_json::Value>(0));
    } else if args.len()==3 && args[0]=="discord-policy-request" {
        let request=Uuid::parse_str(&args[2])?;let row=db.query_one("SELECT public.novu_rust_discord_policy_request($1,$2)",&[&args[1],&request]).await?;
        println!("{}",row.get::<_,serde_json::Value>(0));
    } else if (args.len()==2 || args.len()==3) && args[0]=="discord-policy-audit" {
        let before=args.get(2).map(|v|v.parse::<i64>()).transpose()?;let row=db.query_one("SELECT public.novu_rust_discord_policy_audit_read($1,$2)",&[&args[1],&before]).await?;
        println!("{}",row.get::<_,serde_json::Value>(0));
    } else if (args.len()==2 || args.len()==3) && args[0] == "list-discord" {
        let after = args.get(2).map(|v|Uuid::parse_str(v)).transpose()?;
        let rows = db.query("SELECT * FROM public.novu_rust_discord_attention($1,$2)",&[&args[1],&after]).await?;
        let data:Vec<_> = rows.iter().map(|r|serde_json::json!({"jobId":r.get::<_,Uuid>(0),"kind":r.get::<_,String>(1),"status":r.get::<_,String>(2),"attemptState":r.get::<_,Option<String>>(3),"attemptRevision":r.get::<_,Option<Uuid>>(4),"failureRevision":r.get::<_,i64>(5),"attempts":r.get::<_,i32>(6),"httpStatus":r.get::<_,Option<i32>>(7)})).collect();
        println!("{}",serde_json::json!({"data":data}));
    } else if args.len()==7 && args[0] == "ack-discord" {
        let job=Uuid::parse_str(&args[2])?;
        let attempt=if args[3]=="-" {None} else {Some(Uuid::parse_str(&args[3])?)};
        let revision=args[4].parse::<i64>()?;
        let row=db.query_one("SELECT public.novu_rust_ack_discord($1,$2,$3,$4,$5,$6)",&[&args[1],&job,&attempt,&revision,&args[5],&args[6]]).await?;
        println!("{}",serde_json::json!({"recorded":row.get::<_,bool>(0)}));
    } else if (args.len()==4 || args.len()==5) && args[0]=="list-discord-retry" {
        let grace=args[2].parse::<i32>()?;
        let overdue=match args[3].as_str() {"all"=>false,"overdue"=>true,_=>return Err("invalid retry filter".into())};
        let after=args.get(4).map(|v|Uuid::parse_str(v)).transpose()?;
        let rows=db.query("SELECT * FROM public.novu_rust_discord_retry_list($1,$2,$3,$4)",&[&args[1],&grace,&overdue,&after]).await?;
        let more=rows.len()>100;
        let data:Vec<_>=rows.iter().take(100).map(|r|serde_json::json!({"jobId":r.get::<_,Uuid>(0),"kind":r.get::<_,String>(1),"destinationId":r.get::<_,String>(2),"availableAt":r.get::<_,String>(3),"attempts":r.get::<_,i32>(4),"retryCount":r.get::<_,i64>(5),"lastDelaySeconds":r.get::<_,i32>(6),"lastRetryAfterSeconds":r.get::<_,Option<i32>>(7),"overdue":r.get::<_,bool>(8)})).collect();
        let next=if more {data.last().and_then(|v|v["jobId"].as_str())} else {None};
        println!("{}",serde_json::json!({"data":data,"next":next}));
    } else if args.len()==3 && args[0]=="check-discord-retry" {
        let grace=args[2].parse::<i32>()?;
        let row=db.query_one("SELECT * FROM public.novu_rust_discord_retry_health($1,$2)",&[&args[1],&grace]).await?;
        let (pending,overdue)=(row.get::<_,i64>(0),row.get::<_,i64>(1));
        println!("{}",serde_json::json!({"pending":pending,"overdue":overdue}));
        if overdue>0 {std::process::exit(2);}
    } else if args.len()==3 && args[0]=="get-smtp" {
        let job=Uuid::parse_str(&args[2])?;
        let row=db.query_opt("SELECT * FROM public.novu_rust_smtp_failure_lookup($1,$2)",&[&args[1],&job]).await?;
        let result=match row {Some(r)=>serde_json::json!({"found":true,"jobId":r.get::<_,Uuid>(0),"status":r.get::<_,String>(1),"attemptState":r.get::<_,Option<String>>(2),"attemptRevision":r.get::<_,Option<Uuid>>(3),"failureRevision":r.get::<_,i64>(4).to_string(),"attempts":r.get::<_,i32>(5),"reviewed":r.get::<_,bool>(6)}),None=>serde_json::json!({"found":false})};
        println!("{}",result);
    } else if (args.len()==2 || args.len()==3) && args[0]=="list-smtp" {
        let after=args.get(2).map(|v|Uuid::parse_str(v)).transpose()?;
        let rows=db.query("SELECT * FROM public.novu_rust_smtp_attention($1,$2)",&[&args[1],&after]).await?;
        let more=rows.len()>100;
        let data:Vec<_>=rows.iter().take(100).map(|r|serde_json::json!({"jobId":r.get::<_,Uuid>(0),"status":r.get::<_,String>(1),"attemptState":r.get::<_,Option<String>>(2),"attemptRevision":r.get::<_,Option<Uuid>>(3),"failureRevision":r.get::<_,i64>(4),"attempts":r.get::<_,i32>(5)})).collect();
        let next=if more {data.last().and_then(|v|v["jobId"].as_str())} else {None};
        println!("{}",serde_json::json!({"data":data,"next":next}));
    } else if (args.len()==3 || args.len()==5) && args[0]=="smtp-resolution-history" {
        let job=Uuid::parse_str(&args[2])?;
        let actor=args.get(3).map(|v|v.parse::<u32>()).transpose()?;
        let request=args.get(4).map(|v|Uuid::parse_str(v)).transpose()?;
        let row=db.query_one("SELECT public.novu_rust_smtp_resolution_history($1,$2,$3,$4)",&[&args[1],&job,&actor,&request]).await?;
        println!("{}",row.get::<_,serde_json::Value>(0));
    } else if args.len()==8 && args[0]=="resolve-smtp" {
        let request=Uuid::parse_str(&args[2])?;
        let job=Uuid::parse_str(&args[3])?;
        let attempt=Uuid::parse_str(&args[4])?;
        let revision=args[5].parse::<i64>()?;
        let row=db.query_one("SELECT public.novu_rust_resolve_smtp($1,$2,$3,$4,$5,$6,$7)",&[&args[1],&request,&job,&attempt,&revision,&args[6],&args[7]]).await?;
        println!("{}",serde_json::json!({"recorded":row.get::<_,bool>(0)}));
    } else if args.len()==7 && args[0]=="ack-smtp" {
        let job=Uuid::parse_str(&args[2])?;
        let attempt=if args[3]=="-" {None} else {Some(Uuid::parse_str(&args[3])?)};
        let revision=args[4].parse::<i64>()?;
        let row=db.query_one("SELECT public.novu_rust_ack_smtp($1,$2,$3,$4,$5,$6)",&[&args[1],&job,&attempt,&revision,&args[5],&args[6]]).await?;
        println!("{}",serde_json::json!({"recorded":row.get::<_,bool>(0)}));
    } else if args.len()==3 && args[0]=="smtp-scope-read" {
        let row=db.query_one("SELECT public.novu_rust_smtp_scope_admin_read($1,$2)",&[&args[1],&args[2]]).await?;
        println!("{}",row.get::<_,serde_json::Value>(0));
    } else if args.len()==7 && args[0]=="smtp-scope-set" {
        let request=Uuid::parse_str(&args[2])?;
        let desired=match args[5].as_str() {"enabled"=>true,"disabled"=>false,_=>return Err("invalid scope state".into())};
        let row=db.query_one("SELECT public.novu_rust_smtp_scope_admin_set($1,$2,$3,$4,$5,$6)",&[&args[1],&request,&args[3],&args[4],&desired,&args[6]]).await?;
        println!("{}",row.get::<_,serde_json::Value>(0));
    } else if args.len()==3 && args[0]=="smtp-scope-request" {
        let request=Uuid::parse_str(&args[2])?;
        let row=db.query_one("SELECT public.novu_rust_smtp_scope_admin_request($1,$2)",&[&args[1],&request]).await?;
        println!("{}",row.get::<_,serde_json::Value>(0));
    } else if (args.len()==2 || args.len()==3) && args[0]=="smtp-scope-audit" {
        let after=args.get(2).map(|v|v.parse::<i64>()).transpose()?.unwrap_or(0);
        let rows=db.query("SELECT * FROM public.novu_rust_smtp_scope_audit_read($1,$2)",&[&args[1],&after]).await?;
        let data:Vec<_>=rows.iter().map(|r|serde_json::json!({"id":r.get::<_,String>(0),"databasePrincipal":r.get::<_,String>(1),"principalOid":r.get::<_,String>(2),"previousEnvironment":r.get::<_,Option<String>>(3),"environmentId":r.get::<_,Option<String>>(4),"previousEnabled":r.get::<_,Option<bool>>(5),"enabled":r.get::<_,bool>(6),"actor":r.get::<_,String>(7),"actorOid":r.get::<_,Option<String>>(8),"recordedAt":r.get::<_,String>(9)})).collect();
        println!("{}",serde_json::json!({"data":data}));
    } else if args.len() == 2 && args[0] == "check-smtp" {
        let row = db.query_one("SELECT * FROM public.novu_rust_smtp_health($1)",&[&args[1]]).await?;
        let (unknown,stranded,pre_dispatch)=(row.get::<_,i64>(0),row.get::<_,i64>(1),row.get::<_,i64>(2));
        println!("{}",serde_json::json!({"unknownAttempts":unknown,"strandedDispatches":stranded,"preDispatchFailures":pre_dispatch}));
        if unknown>0 || stranded>0 || pre_dispatch>0 {std::process::exit(2);}
    } else if args.len() == 2 && args[0] == "check-discord" {
        let row = db.query_one("SELECT * FROM public.novu_rust_discord_health($1)",&[&args[1]]).await?;
        let (unknown,rejected,stranded,pre_dispatch)=(row.get::<_,i64>(0),row.get::<_,i64>(1),row.get::<_,i64>(2),row.get::<_,i64>(3));
        println!("{}",serde_json::json!({"unknownAttempts":unknown,"rejectedAttempts":rejected,"strandedDispatches":stranded,"preDispatchFailures":pre_dispatch}));
        if unknown>0 || rejected>0 || stranded>0 || pre_dispatch>0 {std::process::exit(2);}
    } else if args.len() == 2 && args[0] == "check-slack" {
        let scoped = match std::env::var("SWALLOW_SLACK_SCOPED_ACCESS").as_deref() {
            Ok("true") => true,
            Ok("false") | Err(std::env::VarError::NotPresent) => false,
            _ => return Err("invalid Slack scoped access setting".into()),
        };
        let mut health = if scoped {
            novu_postgres_queue::slack_queue::scoped_health(&db, &args[1]).await?
        } else {
            novu_postgres_queue::slack_queue::health(&db, &args[1]).await?
        };
        let missing = match std::env::var("SWALLOW_SLACK_CHECK_MISSING_ENDPOINTS").as_deref() {
            Ok("true") => true,
            Ok("false") | Err(std::env::VarError::NotPresent) => false,
            _ => return Err("invalid Slack missing endpoint check".into()),
        };
        let policy = match std::env::var("SWALLOW_SLACK_CHECK_POLICY_CANCELLED").as_deref() {
            Ok("true") => true,
            Ok("false") | Err(std::env::VarError::NotPresent) => false,
            _ => return Err("invalid Slack policy cancellation check".into()),
        };
        if policy {
            let count =
                novu_postgres_queue::slack_queue::policy_cancellations(&db, &args[1], scoped)
                    .await?;
            health["policyCancelled"] = serde_json::json!(count);
        }
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
            || (policy && health["policyCancelled"].as_i64().is_none_or(|n| n > 0))
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
    } else if args.len() == 3 && args[0] == "twilio-retry-guidance" {
        let job=Uuid::parse_str(&args[2])?;
        let row=db.query_one("SELECT public.novu_rust_twilio_retry_guidance($1,$2)", &[&args[1],&job]).await?;
        println!("{}",row.get::<_,serde_json::Value>(0));
    } else if args.len() == 6 && args[0] == "twilio-retry-after" {
        let job=Uuid::parse_str(&args[2])?;let dispatch=Uuid::parse_str(&args[3])?;
        let row=db.query_one("SELECT public.novu_rust_resolve_twilio_after($1,$2,$3,$4,$5)::text", &[&args[1],&job,&dispatch,&args[4],&args[5]]).await?;
        println!("SMS retry recorded; available at {}",row.get::<_,String>(0));
    } else if args.len() == 6 && args[0] == "slack-retry-after" {
        let job = Uuid::parse_str(&args[2])?;
        let attempt = Uuid::parse_str(&args[3])?;
        let row = db.query_one("SELECT public.novu_rust_resolve_slack_after($1,$2,$3,$4,$5)::text", &[&args[1],&job,&attempt,&args[4],&args[5]]).await?;
        println!("Slack retry recorded; available at {}", row.get::<_, String>(0));
    } else if args.len() == 7 && args[0] == "slack-resolve-v2" {
        novu_postgres_queue::slack_queue::resolve_v2(
            &db,
            &args[1],
            Uuid::parse_str(&args[2])?,
            Uuid::parse_str(&args[3])?,
            &args[4],
            &args[5],
            &args[6],
        )
        .await?;
        println!("Slack resolution recorded");
    } else if (args.len() == 4 || args.len() == 5) && args[0] == "list-fcm-unrecorded-claims" {
        let min_gap:i32=args[2].parse()?; let grace:i32=args[3].parse()?;
        let after=if args.len()==5 {Some(uuid::Uuid::parse_str(&args[4])?)} else {None};
        let list=novu_postgres_queue::fcm_queue::unrecorded_claim_list(&db,&args[1],min_gap,grace,after).await?;
        println!("{}",serde_json::to_string(&list)?);
    } else if args.len() == 4 && args[0] == "check-fcm-unrecorded-claims" {
        let min_gap:i32=args[2].parse()?;
        let grace:i32=args[3].parse()?;
        let count=novu_postgres_queue::fcm_queue::unrecorded_claim_health(&db,&args[1],min_gap,grace).await?;
        println!("{}",serde_json::json!({"unrecordedClaimJobs":count,"minimumClaimGap":min_gap,"graceSeconds":grace}));
        if count>0 {std::process::exit(2);}
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
    } else if args.len() == 8 && args[0] == "alert-resolve-v2" {
        let attempt = Uuid::parse_str(&args[4])?;
        db.query_one(
            "SELECT public.novu_rust_resolve_alert_v2($1,$2,$3,$4,$5,$6,$7)",
            &[
                &args[1], &args[2], &args[3], &attempt, &args[5], &args[6], &args[7],
            ],
        )
        .await?;
        println!("alert resolution recorded");
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
        let evidence =
            resend_evidence_for_cli(&mut db, &args[1], Uuid::parse_str(&args[2])?).await?;
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
                &resend_evidence_for_cli(&mut db, &args[1], Uuid::parse_str(&args[2])?).await?
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
    } else if args.len() == 2 && args[0] == "resend-cooldown-policy-read" {
        let row=db.query_one("SELECT public.novu_rust_resend_cooldown_policy_read($1)",&[&args[1]]).await?;
        println!("{}",row.get::<_,serde_json::Value>(0));
    } else if args.len() == 6 && args[0] == "resend-cooldown-policy-set" {
        let request=Uuid::parse_str(&args[2])?; let expected=args[3].parse::<i64>()?; let fallback=args[4].parse::<i32>()?;
        let row=db.query_one("SELECT public.novu_rust_resend_cooldown_policy_set($1,$2,$3,$4,$5)",&[&args[1],&request,&expected,&fallback,&args[5]]).await?;
        println!("{}",row.get::<_,serde_json::Value>(0));
    } else if args.len() == 3 && args[0] == "resend-cooldown-policy-request" {
        let request=Uuid::parse_str(&args[2])?;
        let row=db.query_one("SELECT public.novu_rust_resend_cooldown_policy_request($1,$2)",&[&args[1],&request]).await?;
        println!("{}",row.get::<_,serde_json::Value>(0));
    } else if (args.len()==2 || args.len()==3) && args[0] == "resend-cooldown-policy-audit" {
        let before=args.get(2).map(|value|value.parse::<i64>()).transpose()?;
        let row=db.query_one("SELECT public.novu_rust_resend_cooldown_policy_audit_read($1,$2)",&[&args[1],&before]).await?;
        println!("{}",row.get::<_,serde_json::Value>(0));
    } else if args.len() == 2 && args[0] == "resend-auto-policy-read" {
        let row = db
            .query_one(
                "SELECT public.novu_rust_resend_auto_policy_read($1)",
                &[&args[1]],
            )
            .await?;
        println!("{}", row.get::<_, serde_json::Value>(0));
    } else if args.len() == 3 && args[0] == "resend-auto-policy-request" {
        let request = Uuid::parse_str(&args[2])?;
        let row = db
            .query_one(
                "SELECT public.novu_rust_resend_auto_policy_request($1,$2)",
                &[&args[1], &request],
            )
            .await?;
        println!("{}", row.get::<_, serde_json::Value>(0));
    } else if args.len() == 8 && args[0] == "resend-auto-policy-set-v2" {
        let request = Uuid::parse_str(&args[2])?;
        let expected = args[3].parse::<i64>()?;
        let enabled = match args[4].as_str() {
            "enabled" => true,
            "disabled" => false,
            _ => return Err("invalid policy status".into()),
        };
        let maximum = args[5].parse::<i32>()?;
        let row = db
            .query_one(
                "SELECT public.novu_rust_resend_auto_policy_set_v2($1,$2,$3,$4,$5,$6,$7)",
                &[
                    &args[1], &request, &expected, &enabled, &maximum, &args[6], &args[7],
                ],
            )
            .await?;
        println!("{}", row.get::<_, serde_json::Value>(0));
    } else if args.len() == 7 && args[0] == "resend-auto-policy-set" {
        let expected = args[2].parse::<i64>()?;
        let enabled = match args[3].as_str() {
            "enabled" => true,
            "disabled" => false,
            _ => return Err("invalid policy status".into()),
        };
        let maximum = args[4].parse::<i32>()?;
        let row = db
            .query_one(
                "SELECT public.novu_rust_resend_auto_policy_set($1,$2,$3,$4,$5,$6)",
                &[&args[1], &expected, &enabled, &maximum, &args[5], &args[6]],
            )
            .await?;
        println!("{}", row.get::<_, serde_json::Value>(0));
    } else if (args.len() == 2 || args.len() == 3) && args[0] == "resend-auto-policy-audit" {
        let before = args.get(2).map(|value| value.parse::<i64>()).transpose()?;
        let row = db
            .query_one(
                "SELECT public.novu_rust_resend_auto_policy_audit_read($1,$2)",
                &[&args[1], &before],
            )
            .await?;
        println!("{}", row.get::<_, serde_json::Value>(0));
    } else if args.len() == 5 && args[0] == "resend-retry-after" {
        let job = Uuid::parse_str(&args[2])?;
        let row = db
            .query_one(
                "SELECT public.novu_rust_resolve_resend_after($1,$2,$3,$4)::text",
                &[&args[1], &job, &args[3], &args[4]],
            )
            .await?;
        println!(
            "reconciliation recorded; available at {}",
            row.get::<_, String>(0)
        );
    } else if args.len() == 6 && ["retry", "accepted"].contains(&args[0].as_str()) {
        let job = Uuid::parse_str(&args[2])?;
        let target = if args[3] == "-" {
            None
        } else {
            Some(args[3].parse::<i32>()?)
        };
        let scoped = match std::env::var("SWALLOW_RECONCILIATION_SCOPED_ACCESS").as_deref() {
            Ok("true") => true,
            Ok("false") | Err(std::env::VarError::NotPresent) => false,
            _ => return Err("invalid reconciliation scoped access setting".into()),
        };
        if scoped {
            if let Some(target) = target {
                db.query_one(
                    "SELECT public.novu_rust_resolve_fcm($1,$2,$3,$4,$5,$6)",
                    &[&args[1], &job, &target, &args[0], &args[4], &args[5]],
                )
                .await?;
            } else {
                db.query_one(
                    "SELECT public.novu_rust_resolve_email_sms($1,$2,$3,$4,$5)",
                    &[&args[1], &job, &args[0], &args[4], &args[5]],
                )
                .await?;
            }
        } else {
            novu_postgres_queue::reconciliation::resolve(
                &mut db, &args[1], job, target, &args[0], &args[4], &args[5],
            )
            .await?;
        }
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
