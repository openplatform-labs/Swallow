//! Environment-scoped, single-attempt Twilio queue with a limited SMS workflow step.
use crate::{
    ClaimedJob,
    twilio::{SendError, Sms, Twilio},
};
use std::time::Duration;
use tokio_postgres::{Client, Error, Transaction};
use uuid::Uuid;

pub const JOB_KIND: &str = "experimental_twilio_sms";
pub const MIGRATION: &str = include_str!("../migrations/0041_experimental_twilio_attempts.sql");
pub const DISPATCH_ID_MIGRATION: &str =
    include_str!("../migrations/0055_twilio_attempt_identity.sql");

fn valid_phone(value: Option<&str>) -> Option<&str> {
    value.filter(|v| !v.trim().is_empty())
}

fn apply_status_callback(sms: &mut Sms, provider: &Twilio, job: Uuid, dispatch: Uuid) {
    let explicit_callback = sms
        .provider_data
        .get("_passthrough")
        .and_then(|v| v.get("body"))
        .and_then(|v| v.get("statusCallback"))
        .and_then(serde_json::Value::as_str)
        .is_some()
        || sms
            .provider_data
            .get("statusCallback")
            .and_then(serde_json::Value::as_str)
            .is_some();
    if !explicit_callback && let Some(callback) = provider.status_callback(job, dispatch) {
        sms.provider_data
            .insert("statusCallback".into(), callback.into());
    }
}

async fn claim(client: &Client, environment: &str) -> Result<Option<ClaimedJob>, Error> {
    let token = Uuid::new_v4();
    let lease = Duration::from_secs(60).as_secs_f64();
    let row = client.query_opt("WITH candidate AS (SELECT id FROM novu_queue_jobs WHERE environment_id=$1 AND kind=$2 AND status='pending' AND available_at<=now() ORDER BY available_at,created_at,id FOR UPDATE SKIP LOCKED LIMIT 1) UPDATE novu_queue_jobs j SET status='running',attempts=j.attempts+1,lease_token=$3,lease_until=clock_timestamp()+$4*interval '1 second' FROM candidate WHERE j.id=candidate.id RETURNING j.id,j.environment_id,j.kind,j.payload,j.attempts", &[&environment,&JOB_KIND,&token,&lease]).await?;
    Ok(row.map(|row| ClaimedJob {
        id: row.get(0),
        environment_id: row.get(1),
        kind: row.get(2),
        payload: row.get(3),
        attempts: row.get(4),
        lease_token: token,
    }))
}

async fn lock_live(tx: &Transaction<'_>, job: &ClaimedJob) -> Result<bool, Error> {
    let row = tx.query_opt("SELECT lease_until > clock_timestamp() FROM novu_queue_jobs WHERE id=$1 AND environment_id=$2 AND kind=$3 AND status='running' AND lease_token=$4 FOR UPDATE", &[&job.id,&job.environment_id,&JOB_KIND,&job.lease_token]).await?;
    // Recheck after obtaining the lock; the lease might expire while waiting.
    if row.is_none() {
        return Ok(false);
    }
    Ok(tx
        .query_one(
            "SELECT lease_until > clock_timestamp() FROM novu_queue_jobs WHERE id=$1",
            &[&job.id],
        )
        .await?
        .get(0))
}

pub async fn process_one(
    client: &mut Client,
    environment: &str,
    provider: &Twilio,
) -> Result<bool, Error> {
    let Some(job) = claim(client, environment).await? else {
        return Ok(false);
    };
    let mut sms: Sms = match serde_json::from_value(job.payload.clone()) {
        Ok(sms) => sms,
        Err(_) => {
            crate::fail(client, job.id, job.lease_token, "invalid sms job").await?;
            return Ok(true);
        }
    };
    let tx = client.transaction().await?;
    let subscriber = if let Some(context) = sms.workflow_context.as_ref() {
        tx.query_opt("SELECT s.profile,w.read_only,w.all_enabled,w.sms_enabled,(p.channels->>'sms')::boolean,(sp.channels->>'sms')::boolean,EXISTS(SELECT 1 FROM novu_rust_workflows wf WHERE wf.environment_id=s.environment_id AND wf.identifier=$3 AND ($4::text IS NULL OR wf.inbox_metadata->>'id'=$4)),s.internal_id FROM novu_rust_subscribers s LEFT JOIN novu_rust_workflow_preferences w ON w.environment_id=s.environment_id AND w.workflow_identifier=$3 LEFT JOIN novu_rust_subscriber_preferences p ON p.environment_id=s.environment_id AND p.subscriber_id=s.subscriber_id LEFT JOIN novu_rust_subscriber_workflow_preferences sp ON sp.environment_id=s.environment_id AND sp.subscriber_id=s.subscriber_id AND sp.workflow_identifier=$3 WHERE s.environment_id=$1 AND s.subscriber_id=$2 FOR UPDATE OF s", &[&environment,&context.subscriber_id,&context.workflow_identifier,&context.workflow_internal_id]).await?
    } else {
        None
    };
    if !lock_live(&tx, &job).await? {
        tx.rollback().await?;
        return Ok(true);
    }
    // Serialize with callback inserts on the queue row. Legacy decisions and
    // job-only callbacks lack a dispatch ID and are handled conservatively.
    if provider.tracks_status_callbacks() {
        let evidence = tx.query_opt("SELECT r.retry_dispatch_id FROM (SELECT retry_dispatch_id FROM novu_rust_delivery_reconciliations WHERE environment_id=$1 AND job_id=$2 AND target_index IS NULL AND decision='retry' ORDER BY decision_order DESC LIMIT 1) r WHERE EXISTS(SELECT 1 FROM novu_rust_twilio_delivery_events e WHERE e.environment_id=$1 AND e.job_id=$2 AND (r.retry_dispatch_id IS NULL OR e.dispatch_id IS NULL OR e.dispatch_id=r.retry_dispatch_id))", &[&environment,&job.id]).await?;
        if let Some(evidence) = evidence {
            let dispatch: Option<Uuid> = evidence.get(0);
            let inserted = tx.execute("INSERT INTO novu_rust_twilio_attempts(job_id,environment_id,state,dispatch_id) VALUES ($1,$2,'unknown',$3) ON CONFLICT(job_id) DO NOTHING", &[&job.id,&environment,&dispatch]).await?;
            if inserted == 1 {
                tx.execute("UPDATE novu_rust_sms_messages SET delivery_state='unknown',status='error',error_id='unexpected_sms_error',error_text='sms retry blocked by provider callback' WHERE environment_id=$1 AND sms_job_id=$2", &[&environment,&job.id]).await?;
            }
            tx.execute("UPDATE novu_queue_jobs SET status='failed',last_error='sms retry blocked by provider callback',lease_token=NULL,lease_until=NULL WHERE id=$1 AND environment_id=$2", &[&job.id,&environment]).await?;
            tx.commit().await?;
            return Ok(true);
        }
    }
    if let Some(context) = sms.workflow_context.take() {
        let skip = match subscriber.as_ref() {
            None => Some(("failed", "failed", "subscriber not found")),
            Some(row)
                if !crate::allow_recreated_subscriber()
                    && row.get::<_, String>(7) != context.subscriber_internal_id =>
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
                Some(("canceled", "cancelled", "sms preference disabled"))
            }
            _ => None,
        };
        if let Some((outcome, status, diagnostic)) = skip {
            tx.execute("UPDATE novu_rust_sms_step_outcomes SET status=$3,sms_job_id=NULL WHERE environment_id=$1 AND sms_job_id=$2", &[&environment,&job.id,&outcome]).await?;
            tx.execute("UPDATE novu_queue_jobs SET status=$2,last_error=$3,lease_token=NULL,lease_until=NULL WHERE id=$1", &[&job.id,&status,&diagnostic]).await?;
            tx.commit().await?;
            return Ok(true);
        }
        let mut profile: serde_json::Value = subscriber.as_ref().unwrap().get(0);
        if let Some(fields) = profile.as_object_mut() {
            fields.insert("subscriberId".into(), context.subscriber_id.clone().into());
        }
        let option = |key: &str| {
            context
                .overrides
                .as_ref()
                .and_then(|v| v.get(key))
                .and_then(serde_json::Value::as_str)
                .filter(|v| !v.is_empty())
        };
        let phone = valid_phone(option("to"))
            .or_else(|| {
                valid_phone(
                    context
                        .payload
                        .get("phone")
                        .and_then(serde_json::Value::as_str),
                )
            })
            .or_else(|| valid_phone(profile.get("phone").and_then(serde_json::Value::as_str)));
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
                tx.execute("UPDATE novu_rust_sms_step_outcomes SET status='failed',sms_job_id=NULL WHERE environment_id=$1 AND sms_job_id=$2", &[&environment,&job.id]).await?;
                tx.execute("UPDATE novu_queue_jobs SET status='failed',last_error='actor identity changed or missing',lease_token=NULL,lease_until=NULL WHERE id=$1", &[&job.id]).await?;
                tx.commit().await?;
                return Ok(true);
            }
        };
        let mut resolved_context = serde_json::Map::new();
        if !context.context_keys.is_empty() {
            for row in tx.query("SELECT context_key,data FROM novu_rust_contexts WHERE environment_id=$1 AND context_key=ANY($2)", &[&environment,&context.context_keys]).await? {
                let key: String = row.get(0);
                let data: serde_json::Value = row.get(1);
                if let Some((kind,id)) = key.split_once(':') { resolved_context.insert(kind.to_owned(),serde_json::json!({"id":id,"data":data})); }
            }
        }
        let rendered = novu_rust_contracts::template::render_in_app_with_actor(
            &context.content_template,
            &context.payload,
            &profile,
            &serde_json::Value::Object(resolved_context),
            actor.as_ref(),
        );
        let content = match rendered {
            Ok(content) if !content.is_empty() => content,
            _ => {
                tx.execute("UPDATE novu_rust_sms_step_outcomes SET status='failed',sms_job_id=NULL WHERE environment_id=$1 AND sms_job_id=$2", &[&environment,&job.id]).await?;
                tx.execute("UPDATE novu_queue_jobs SET status='failed',last_error='unsupported sms template or input',lease_token=NULL,lease_until=NULL WHERE id=$1", &[&job.id]).await?;
                tx.commit().await?;
                return Ok(true);
            }
        };
        let stored_content = (std::env::var("STORE_NOTIFICATION_CONTENT").as_deref() == Ok("true"))
            .then_some(content.as_str());
        if let (Some(message_id), Some(notification_id), Some(transaction_id)) = (
            &context.message_id,
            &context.notification_internal_id,
            &context.transaction_id,
        ) {
            tx.execute("INSERT INTO novu_rust_sms_messages(internal_id,environment_id,subscriber_id,subscriber_internal_id,notification_internal_id,workflow_identifier,transaction_id,phone,content,status,sms_job_id) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'sent',$10) ON CONFLICT(internal_id) DO NOTHING", &[&message_id,&environment,&context.subscriber_id,&context.subscriber_internal_id,&notification_id,&context.workflow_identifier,&transaction_id,&phone,&stored_content,&job.id]).await?;
        }
        let Some(phone) = phone else {
            tx.execute("UPDATE novu_rust_sms_messages SET status='warning',error_id='no_subscriber_phone',error_text='Subscriber does not have active phone' WHERE environment_id=$1 AND sms_job_id=$2", &[&environment,&job.id]).await?;
            tx.execute("UPDATE novu_rust_sms_step_outcomes SET status='missing_phone',sms_job_id=NULL WHERE environment_id=$1 AND sms_job_id=$2", &[&environment,&job.id]).await?;
            tx.execute("UPDATE novu_queue_jobs SET status='cancelled',last_error='subscriber missing phone',lease_token=NULL,lease_until=NULL WHERE id=$1", &[&job.id]).await?;
            tx.commit().await?;
            return Ok(true);
        };
        sms.to = phone.to_owned();
        sms.content = option("content").unwrap_or(&content).to_owned();
        sms.from = option("from").map(str::to_owned);
    }
    let dispatch = Uuid::new_v4();
    let inserted = tx.execute("INSERT INTO novu_rust_twilio_attempts(job_id,environment_id,state,dispatch_id) VALUES ($1,$2,'dispatching',$3) ON CONFLICT(job_id) DO NOTHING", &[&job.id,&job.environment_id,&dispatch]).await?;
    if inserted == 0 {
        tx.execute("UPDATE novu_queue_jobs SET status='failed',last_error='sms attempt already exists',lease_token=NULL,lease_until=NULL WHERE id=$1", &[&job.id]).await?;
        tx.commit().await?;
        return Ok(true);
    }
    tx.execute(
        "INSERT INTO novu_rust_twilio_dispatch_ids(id,job_id,environment_id) VALUES ($1,$2,$3)",
        &[&dispatch, &job.id, &job.environment_id],
    )
    .await?;
    apply_status_callback(&mut sms, provider, job.id, dispatch);
    tx.execute("UPDATE novu_rust_sms_messages SET delivery_state='dispatching' WHERE environment_id=$1 AND sms_job_id=$2", &[&environment,&job.id]).await?;
    tx.commit().await?;
    let (state, status, provider_id, diagnostic) = match provider.send(&sms).await {
        Ok(receipt) => (
            "accepted",
            None,
            Some(receipt.id.chars().take(1024).collect::<String>()),
            None,
        ),
        Err(SendError::Provider(code)) if code >= 500 => (
            "unknown",
            Some(i32::from(code)),
            None,
            Some("sms provider outcome unknown"),
        ),
        Err(SendError::Provider(code)) => (
            "rejected",
            Some(i32::from(code)),
            None,
            Some("sms provider rejected request"),
        ),
        Err(SendError::Transport | SendError::InvalidResponse) => {
            ("unknown", None, None, Some("sms delivery outcome unknown"))
        }
        Err(_) => ("rejected", None, None, Some("invalid sms request")),
    };
    let tx = client.transaction().await?;
    if !lock_live(&tx, &job).await? {
        tx.rollback().await?;
        return Ok(true);
    }
    let updated = tx.execute("UPDATE novu_rust_twilio_attempts SET state=$2,http_status=$3,provider_message_id=$4,updated_at=clock_timestamp() WHERE job_id=$1 AND environment_id=$5 AND state='dispatching'", &[&job.id,&state,&status,&provider_id,&environment]).await?;
    if updated == 1 {
        tx.execute("UPDATE novu_rust_sms_messages SET delivery_state=$3 WHERE environment_id=$1 AND sms_job_id=$2", &[&environment,&job.id,&state]).await?;
        if state == "accepted" {
            tx.execute("UPDATE novu_rust_sms_messages SET identifier=$3 WHERE environment_id=$1 AND sms_job_id=$2", &[&environment,&job.id,&provider_id]).await?;
        } else {
            tx.execute("UPDATE novu_rust_sms_messages SET status='error',error_id='unexpected_sms_error',error_text=$3 WHERE environment_id=$1 AND sms_job_id=$2", &[&environment,&job.id,&diagnostic]).await?;
        }
        let job_status = if state == "accepted" {
            "completed"
        } else {
            "failed"
        };
        tx.execute("UPDATE novu_queue_jobs SET status=$2,last_error=$3,lease_token=NULL,lease_until=NULL WHERE id=$1", &[&job.id,&job_status,&diagnostic]).await?;
    }
    tx.commit().await?;
    Ok(true)
}

/// Before dispatch requeue; after dispatch record uncertainty and do not resend.
pub async fn recover_expired(client: &mut Client, environment: &str) -> Result<usize, Error> {
    let tx = client.transaction().await?;
    let rows = tx.query("SELECT id FROM novu_queue_jobs WHERE environment_id=$1 AND kind=$2 AND status='running' AND lease_until<=clock_timestamp() ORDER BY lease_until,id FOR UPDATE SKIP LOCKED LIMIT 100", &[&environment,&JOB_KIND]).await?;
    for row in &rows {
        let id: Uuid = row.get(0);
        let attempted = tx
            .query_opt(
                "SELECT state FROM novu_rust_twilio_attempts WHERE job_id=$1 AND environment_id=$2",
                &[&id, &environment],
            )
            .await?
            .is_some();
        if attempted {
            tx.execute("UPDATE novu_rust_twilio_attempts SET state='unknown',updated_at=clock_timestamp() WHERE job_id=$1 AND environment_id=$2 AND state='dispatching'", &[&id,&environment]).await?;
            tx.execute("UPDATE novu_rust_sms_messages SET delivery_state='unknown' WHERE environment_id=$1 AND sms_job_id=$2", &[&environment,&id]).await?;
            tx.execute("UPDATE novu_queue_jobs SET status='failed',last_error='sms delivery outcome unknown',lease_token=NULL,lease_until=NULL WHERE id=$1", &[&id]).await?;
        } else {
            tx.execute("UPDATE novu_queue_jobs SET status='pending',lease_token=NULL,lease_until=NULL WHERE id=$1", &[&id]).await?;
        }
    }
    tx.commit().await?;
    Ok(rows.len())
}

#[cfg(test)]
mod tests {
    #[test]
    fn callback_is_job_bound_and_respects_overrides() {
        use super::*;
        use crate::twilio::{Config, Region, request_body};
        let config = || Config {
            account_sid: "AC1".into(),
            auth_token: "secret".into(),
            api_key_sid: None,
            api_key_secret: None,
            from: "+123".into(),
            region: Region::Us,
        };
        let base = "https://events.example.test/webhooks/organizations/org-a/environments/env-a/sms/twilio";
        assert!(
            Twilio::new(config())
                .unwrap()
                .with_status_callback(base, "other")
                .is_err()
        );
        assert!(
            Twilio::new(config())
                .unwrap()
                .with_status_callback(&base.replace("https:", "http:"), "env-a")
                .is_err()
        );
        assert!(
            Twilio::new(config())
                .unwrap()
                .with_status_callback(&format!("{base}?x=1"), "env-a")
                .is_err()
        );
        let provider = Twilio::new(config())
            .unwrap()
            .with_status_callback(base, "env-a")
            .unwrap();
        let job = Uuid::parse_str("11111111-1111-4111-8111-111111111111").unwrap();
        let dispatch = Uuid::parse_str("22222222-2222-4222-8222-222222222222").unwrap();
        let mut sms = Sms {
            to: "+456".into(),
            content: "hello".into(),
            ..Default::default()
        };
        apply_status_callback(&mut sms, &provider, job, dispatch);
        assert_eq!(
            request_body(&config(), &sms)
                .unwrap()
                .status_callback
                .unwrap(),
            format!("{base}/jobs/{job}/attempts/{dispatch}")
        );
        sms.provider_data =
            serde_json::json!({"statusCallback":"https://explicit.example/callback"})
                .as_object()
                .unwrap()
                .clone();
        apply_status_callback(&mut sms, &provider, job, dispatch);
        assert_eq!(
            request_body(&config(), &sms).unwrap().status_callback,
            Some("https://explicit.example/callback")
        );
        sms.provider_data = serde_json::json!({"_passthrough":{"body":{"statusCallback":"https://override.example/callback"}}}).as_object().unwrap().clone();
        apply_status_callback(&mut sms, &provider, job, dispatch);
        assert_eq!(
            request_body(&config(), &sms).unwrap().status_callback,
            Some("https://override.example/callback")
        );
    }

    use super::*;
    use crate::twilio::{Config, Region};
    use serde_json::json;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };
    use tokio_postgres::NoTls;

    #[tokio::test]
    #[ignore = "requires differential harness PostgreSQL and loopback access"]
    async fn capture_reference_workflow_sms() {
        let (mut db, driver) =
            tokio_postgres::connect(&std::env::var("TEST_DATABASE_URL").unwrap(), NoTls)
                .await
                .unwrap();
        tokio::spawn(async move { driver.await.unwrap() });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let accept = if std::env::var_os("TEST_SMS_ALLOW_NO_REQUEST").is_some() {
                tokio::time::timeout(Duration::from_secs(1), listener.accept())
                    .await
                    .ok()
            } else {
                Some(listener.accept().await)
            };
            let Some(accept) = accept else {
                std::fs::write(std::env::var("TEST_SMS_CAPTURE_PATH").unwrap(), b"null").unwrap();
                return;
            };
            let (mut socket, _) = accept.unwrap();
            let mut buf = Vec::new();
            let mut tmp = [0; 4096];
            loop {
                let n = socket.read(&mut tmp).await.unwrap();
                assert!(n > 0);
                buf.extend_from_slice(&tmp[..n]);
                if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&buf[..pos]);
                    let length = headers
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length: ")
                                .and_then(|v| v.parse::<usize>().ok())
                        })
                        .unwrap();
                    if buf.len() >= pos + 4 + length {
                        let pairs: Vec<(String, String)> =
                            serde_urlencoded::from_bytes(&buf[pos + 4..pos + 4 + length]).unwrap();
                        let mut form = serde_json::Map::new();
                        for (key, value) in pairs {
                            match form.entry(key) {
                                serde_json::map::Entry::Vacant(entry) => {
                                    entry.insert(value.into());
                                }
                                serde_json::map::Entry::Occupied(mut entry) => {
                                    if let Some(values) = entry.get_mut().as_array_mut() {
                                        values.push(value.into());
                                    } else {
                                        let first = entry.get().clone();
                                        entry.insert(serde_json::json!([first, value]));
                                    }
                                }
                            }
                        }
                        std::fs::write(
                            std::env::var("TEST_SMS_CAPTURE_PATH").unwrap(),
                            serde_json::to_vec(&form).unwrap(),
                        )
                        .unwrap();
                        break;
                    }
                }
            }
            let body = r#"{"sid":"SM123","date_created":"Wed, 28 Oct 2026 10:00:00 +0000"}"#;
            let status = std::env::var("TEST_SMS_HTTP_STATUS").unwrap_or_else(|_| "201".into());
            assert!(status == "201" || status == "429");
            socket.write_all(format!("HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
        });
        let provider = Twilio::test_origin(
            Config {
                account_sid: "AC00000000000000000000000000000001".into(),
                auth_token: "fixture".into(),
                api_key_sid: None,
                api_key_secret: None,
                from: "+15550000001".into(),
                region: Region::Us,
            },
            &origin,
        )
        .unwrap();
        assert!(
            process_one(&mut db, "64a000000000000000000002", &provider)
                .await
                .unwrap()
        );
        server.await.unwrap();
    }

    #[tokio::test]
    #[ignore = "requires disposable PostgreSQL and loopback access"]
    async fn sms_attempt_recovery_and_tenant_isolation() {
        let url = std::env::var("TEST_DATABASE_URL").unwrap();
        let (mut db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
        tokio::spawn(async move { driver.await.unwrap() });
        db.batch_execute("DROP TABLE IF EXISTS novu_rust_twilio_dispatch_ids,novu_rust_twilio_attempts,novu_queue_jobs CASCADE")
            .await
            .unwrap();
        db.batch_execute(crate::MIGRATION).await.unwrap();
        db.batch_execute(MIGRATION).await.unwrap();
        db.batch_execute(DISPATCH_ID_MIGRATION).await.unwrap();
        db.batch_execute(crate::reconciliation::MIGRATION)
            .await
            .unwrap();
        db.batch_execute(crate::reconciliation::RETRY_DISPATCH_MIGRATION)
            .await
            .unwrap();
        db.batch_execute("CREATE TABLE IF NOT EXISTS novu_rust_environments(id text PRIMARY KEY, organization_id text NOT NULL); INSERT INTO novu_rust_environments VALUES ('test','org'),('other','org') ON CONFLICT DO NOTHING").await.unwrap();
        db.batch_execute(include_str!(
            "../../../api/rust-api/migrations/0005_twilio_webhooks.sql"
        ))
        .await
        .unwrap();
        db.batch_execute(include_str!(
            "../../../api/rust-api/migrations/0006_twilio_webhook_job_link.sql"
        ))
        .await
        .unwrap();
        db.batch_execute(include_str!(
            "../../../api/rust-api/migrations/0007_twilio_dispatch_identity.sql"
        ))
        .await
        .unwrap();
        db.batch_execute(crate::in_app::SMS_MESSAGES_MIGRATION)
            .await
            .unwrap();
        db.batch_execute("ALTER TABLE novu_rust_sms_messages ADD COLUMN IF NOT EXISTS delivery_state text NOT NULL DEFAULT 'not_attempted'").await.unwrap();
        let payload = json!({"to":"+15550000002", "content":"Hi & hello", "from":null});
        let other = Uuid::new_v4();
        crate::enqueue(&db, other, "other", JOB_KIND, &payload)
            .await
            .unwrap();
        for (status, expected, malformed) in [
            (201, "accepted", false),
            (429, "rejected", false),
            (500, "unknown", false),
            (201, "unknown", true),
        ] {
            let id = Uuid::new_v4();
            crate::enqueue(&db, id, "test", JOB_KIND, &payload)
                .await
                .unwrap();
            db.execute("INSERT INTO novu_rust_sms_messages(internal_id,environment_id,subscriber_id,subscriber_internal_id,notification_internal_id,workflow_identifier,transaction_id,status,sms_job_id) VALUES ($1,'test','alice','subscriber','notification','workflow','transaction','sent',$2)", &[&id.to_string(),&id]).await.unwrap();
            if status == 201 && !malformed {
                // Older dispatch evidence does not block the latest retry.
                let earlier = Uuid::new_v4();
                let retried = Uuid::new_v4();
                db.execute("INSERT INTO novu_rust_twilio_dispatch_ids(id,job_id,environment_id) VALUES ($1,$3,'test'),($2,$3,'test')", &[&earlier,&retried,&id]).await.unwrap();
                db.execute("INSERT INTO novu_rust_delivery_reconciliations(id,environment_id,job_id,decision,operator_name,reason,retry_dispatch_id) VALUES ($1,'test',$2,'retry','operator','verified',$3)", &[&Uuid::new_v4(),&id,&earlier]).await.unwrap();
                db.execute("INSERT INTO novu_rust_delivery_reconciliations(id,environment_id,job_id,decision,operator_name,reason,retry_dispatch_id) VALUES ($1,'test',$2,'retry','operator','verified',$3)", &[&Uuid::new_v4(),&id,&retried]).await.unwrap();
                db.execute("INSERT INTO novu_rust_twilio_delivery_events(environment_id,account_sid,message_sid,status,job_id,dispatch_id) VALUES ('test','ACfixture',$1,'delivered',$2,$3)", &[&format!("SMold{id}"),&id,&earlier]).await.unwrap();
                // Evidence from a different environment cannot block this job.
                db.execute("INSERT INTO novu_rust_twilio_delivery_events(environment_id,account_sid,message_sid,status,job_id) VALUES ('other','ACfixture',$1,'delivered',$2)", &[&format!("SM{id}"),&id]).await.unwrap();
            }
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let origin = format!("http://{}", listener.local_addr().unwrap());
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut data = Vec::new();
                let mut buffer = [0; 4096];
                let captured_dispatch = loop {
                    let n = socket.read(&mut buffer).await.unwrap();
                    assert!(n > 0);
                    data.extend_from_slice(&buffer[..n]);
                    if let Some(pos) = data.windows(4).position(|w| w == b"\r\n\r\n") {
                        let headers = String::from_utf8_lossy(&data[..pos]);
                        let length = headers
                            .lines()
                            .find_map(|line| {
                                line.to_ascii_lowercase()
                                    .strip_prefix("content-length: ")
                                    .and_then(|v| v.parse::<usize>().ok())
                            })
                            .unwrap();
                        if data.len() >= pos + 4 + length {
                            let form: std::collections::HashMap<String, String> =
                                serde_urlencoded::from_bytes(&data[pos + 4..pos + 4 + length])
                                    .unwrap();
                            let callback = form.get("StatusCallback").unwrap();
                            let prefix = format!(
                                "https://events.example.test/webhooks/organizations/org/environments/test/sms/twilio/jobs/{id}/attempts/"
                            );
                            let dispatch =
                                Uuid::parse_str(callback.strip_prefix(&prefix).unwrap()).unwrap();
                            assert_ne!(dispatch, Uuid::nil());
                            break dispatch;
                        }
                    }
                };
                let body = if malformed {
                    "{}"
                } else {
                    r#"{"sid":"SM123","date_created":"Wed, 28 Oct 2026 10:00:00 +0000"}"#
                };
                socket.write_all(format!("HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
                captured_dispatch
            });
            let provider = Twilio::test_origin(
                Config {
                    account_sid: "ACfixture".into(),
                    auth_token: "fixture".into(),
                    api_key_sid: None,
                    api_key_secret: None,
                    from: "+15550000001".into(),
                    region: Region::Us,
                },
                &origin,
            )
            .unwrap()
            .with_status_callback("https://events.example.test/webhooks/organizations/org/environments/test/sms/twilio", "test")
            .unwrap();
            assert!(process_one(&mut db, "test", &provider).await.unwrap());
            let captured_dispatch = server.await.unwrap();
            let row = db.query_one("SELECT a.state,j.status,a.provider_message_id FROM novu_rust_twilio_attempts a JOIN novu_queue_jobs j ON j.id=a.job_id WHERE a.job_id=$1", &[&id]).await.unwrap();
            let count: i64 = db.query_one("SELECT count(*) FROM novu_rust_twilio_dispatch_ids d JOIN novu_rust_twilio_attempts a ON a.dispatch_id=d.id AND a.job_id=d.job_id AND a.environment_id=d.environment_id WHERE d.job_id=$1 AND d.id=$2", &[&id,&captured_dispatch]).await.unwrap().get(0);
            assert_eq!(count, 1);
            assert_eq!(row.get::<_, String>(0), expected);
            let delivery_state: String = db
                .query_one(
                    "SELECT delivery_state FROM novu_rust_sms_messages WHERE sms_job_id=$1",
                    &[&id],
                )
                .await
                .unwrap()
                .get(0);
            assert_eq!(delivery_state, expected);
            assert_eq!(
                row.get::<_, String>(1),
                if expected == "accepted" {
                    "completed"
                } else {
                    "failed"
                }
            );
            assert_eq!(
                row.get::<_, Option<String>>(2).as_deref(),
                if expected == "accepted" {
                    Some("SM123")
                } else {
                    None
                }
            );
            assert!(!process_one(&mut db, "test", &provider).await.unwrap());
        }
        assert_eq!(
            db.query_one("SELECT status FROM novu_queue_jobs WHERE id=$1", &[&other])
                .await
                .unwrap()
                .get::<_, String>(0),
            "pending"
        );
        let contested = Uuid::new_v4();
        crate::enqueue(&db, contested, "test", JOB_KIND, &payload)
            .await
            .unwrap();
        let (mut second_db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
        tokio::spawn(async move { driver.await.unwrap() });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buffer = [0; 4096];
            assert!(socket.read(&mut buffer).await.unwrap() > 0);
            let body = r#"{"sid":"SM123","date_created":"Wed, 28 Oct 2026 10:00:00 +0000"}"#;
            socket.write_all(format!("HTTP/1.1 201 Created\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
        });
        let provider = Twilio::test_origin(
            Config {
                account_sid: "ACfixture".into(),
                auth_token: "fixture".into(),
                api_key_sid: None,
                api_key_secret: None,
                from: "+15550000001".into(),
                region: Region::Us,
            },
            &origin,
        )
        .unwrap();
        let (first, second) = tokio::join!(
            process_one(&mut db, "test", &provider),
            process_one(&mut second_db, "test", &provider)
        );
        assert_ne!(first.unwrap(), second.unwrap());
        server.await.unwrap();
        assert_eq!(
            db.query_one(
                "SELECT count(*) FROM novu_rust_twilio_attempts WHERE job_id=$1",
                &[&contested]
            )
            .await
            .unwrap()
            .get::<_, i64>(0),
            1
        );
        // An authenticated callback that arrives after a retry decision but
        // before dispatch prevents another provider request.
        let blocked = Uuid::new_v4();
        let old_dispatch = Uuid::new_v4();
        crate::enqueue(&db, blocked, "test", JOB_KIND, &payload)
            .await
            .unwrap();
        db.execute("INSERT INTO novu_rust_twilio_dispatch_ids(id,job_id,environment_id) VALUES ($1,$2,'test')", &[&old_dispatch,&blocked]).await.unwrap();
        db.execute("INSERT INTO novu_rust_delivery_reconciliations(id,environment_id,job_id,decision,operator_name,reason,retry_dispatch_id) VALUES ($1,'test',$2,'retry','operator','verified',$3)", &[&Uuid::new_v4(),&blocked,&old_dispatch]).await.unwrap();
        db.execute("INSERT INTO novu_rust_twilio_delivery_events(environment_id,account_sid,message_sid,status,job_id,dispatch_id) VALUES ('test','ACfixture','SMfixture','delivered',$1,$2)", &[&blocked,&old_dispatch]).await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let provider = Twilio::test_origin(
            Config {
                account_sid: "ACfixture".into(),
                auth_token: "fixture".into(),
                api_key_sid: None,
                api_key_secret: None,
                from: "+15550000001".into(),
                region: Region::Us,
            },
            &format!("http://{}", listener.local_addr().unwrap()),
        )
        .unwrap()
        .with_status_callback(
            "https://events.example.test/webhooks/organizations/org/environments/test/sms/twilio",
            "test",
        )
        .unwrap();
        assert!(process_one(&mut db, "test", &provider).await.unwrap());
        assert!(
            tokio::time::timeout(Duration::from_millis(100), listener.accept())
                .await
                .is_err()
        );
        let row = db.query_one("SELECT j.status,j.last_error,a.state,a.dispatch_id FROM novu_queue_jobs j JOIN novu_rust_twilio_attempts a ON a.job_id=j.id WHERE j.id=$1", &[&blocked]).await.unwrap();
        assert_eq!(row.get::<_, String>(0), "failed");
        assert_eq!(
            row.get::<_, String>(1),
            "sms retry blocked by provider callback"
        );
        assert_eq!(row.get::<_, String>(2), "unknown");
        assert_eq!(row.get::<_, Option<Uuid>>(3), Some(old_dispatch));
        // Pre-migration decisions have no dispatch ID and must still block conservatively.
        let legacy = Uuid::new_v4();
        crate::enqueue(&db, legacy, "test", JOB_KIND, &payload)
            .await
            .unwrap();
        db.execute("INSERT INTO novu_rust_delivery_reconciliations(id,environment_id,job_id,decision,operator_name,reason) VALUES ($1,'test',$2,'retry','operator','legacy')", &[&Uuid::new_v4(),&legacy]).await.unwrap();
        db.execute("INSERT INTO novu_rust_twilio_delivery_events(environment_id,account_sid,message_sid,status,job_id,dispatch_id) VALUES ('test','ACfixture','SMlegacy','delivered',$1,$2)", &[&legacy,&Uuid::new_v4()]).await.unwrap();
        assert!(process_one(&mut db, "test", &provider).await.unwrap());
        assert!(
            tokio::time::timeout(Duration::from_millis(100), listener.accept())
                .await
                .is_err()
        );
        let legacy_dispatch: Option<Uuid> = db.query_one("SELECT dispatch_id FROM novu_rust_twilio_attempts WHERE job_id=$1 AND state='unknown'", &[&legacy]).await.unwrap().get(0);
        assert_eq!(legacy_dispatch, None);
        // A legacy job-only callback also blocks a known retry conservatively.
        let job_only = Uuid::new_v4();
        let known_dispatch = Uuid::new_v4();
        crate::enqueue(&db, job_only, "test", JOB_KIND, &payload)
            .await
            .unwrap();
        db.execute("INSERT INTO novu_rust_delivery_reconciliations(id,environment_id,job_id,decision,operator_name,reason,retry_dispatch_id) VALUES ($1,'test',$2,'retry','operator','legacy callback',$3)", &[&Uuid::new_v4(),&job_only,&known_dispatch]).await.unwrap();
        db.execute("INSERT INTO novu_rust_twilio_delivery_events(environment_id,account_sid,message_sid,status,job_id) VALUES ('test','ACfixture','SMjobonly','delivered',$1)", &[&job_only]).await.unwrap();
        assert!(process_one(&mut db, "test", &provider).await.unwrap());
        assert!(
            tokio::time::timeout(Duration::from_millis(100), listener.accept())
                .await
                .is_err()
        );
        let restored: Option<Uuid> = db.query_one("SELECT dispatch_id FROM novu_rust_twilio_attempts WHERE job_id=$1 AND state='unknown'", &[&job_only]).await.unwrap().get(0);
        assert_eq!(restored, Some(known_dispatch));
        let before = Uuid::new_v4();
        let after = Uuid::new_v4();
        for id in [before, after] {
            crate::enqueue(&db, id, "test", JOB_KIND, &payload)
                .await
                .unwrap();
        }
        db.execute("UPDATE novu_queue_jobs SET status='running',lease_token=$2,lease_until=clock_timestamp()-interval '1 second' WHERE id=ANY($1)", &[&vec![before,after],&Uuid::new_v4()]).await.unwrap();
        db.execute("INSERT INTO novu_rust_twilio_attempts(job_id,environment_id,state) VALUES ($1,'test','dispatching')", &[&after]).await.unwrap();
        db.execute("INSERT INTO novu_rust_sms_messages(internal_id,environment_id,subscriber_id,subscriber_internal_id,notification_internal_id,workflow_identifier,transaction_id,status,sms_job_id,delivery_state) VALUES ($1,'test','alice','subscriber','notification','workflow','transaction','sent',$2,'dispatching')", &[&after.to_string(),&after]).await.unwrap();
        assert_eq!(recover_expired(&mut db, "test").await.unwrap(), 2);
        assert_eq!(
            db.query_one("SELECT status FROM novu_queue_jobs WHERE id=$1", &[&before])
                .await
                .unwrap()
                .get::<_, String>(0),
            "pending"
        );
        let row=db.query_one("SELECT j.status,a.state FROM novu_queue_jobs j JOIN novu_rust_twilio_attempts a ON a.job_id=j.id WHERE j.id=$1", &[&after]).await.unwrap();
        assert_eq!(row.get::<_, String>(0), "failed");
        assert_eq!(row.get::<_, String>(1), "unknown");
        assert_eq!(
            db.query_one(
                "SELECT delivery_state FROM novu_rust_sms_messages WHERE sms_job_id=$1",
                &[&after]
            )
            .await
            .unwrap()
            .get::<_, String>(0),
            "unknown"
        );
    }
}
