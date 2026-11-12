//! Experimental, environment-scoped, one-attempt SendGrid queue consumer.
//! It does not yet implement Novu workflows or integration management.
use crate::{
    ClaimedJob,
    sendgrid::{Email, SendError, Sendgrid},
};
use std::time::Duration;
use tokio_postgres::{Client, Error};
use uuid::Uuid;

pub const JOB_KIND: &str = "experimental_sendgrid_email";
pub const MIGRATION: &str = include_str!("../migrations/0035_experimental_sendgrid_attempts.sql");

async fn claim(
    client: &Client,
    environment: &str,
    lease: Duration,
) -> Result<Option<ClaimedJob>, Error> {
    let token = Uuid::new_v4();
    let seconds = lease.as_secs_f64();
    let row=client.query_opt("WITH candidate AS (SELECT id FROM novu_queue_jobs WHERE environment_id=$1 AND kind=$2 AND status='pending' AND available_at<=now() ORDER BY available_at,created_at,id FOR UPDATE SKIP LOCKED LIMIT 1) UPDATE novu_queue_jobs j SET status='running',attempts=j.attempts+1,lease_token=$3,lease_until=clock_timestamp()+$4*interval '1 second' FROM candidate WHERE j.id=candidate.id RETURNING j.id,j.environment_id,j.kind,j.payload,j.attempts", &[&environment,&JOB_KIND,&token,&seconds]).await?;
    Ok(row.map(|r| ClaimedJob {
        id: r.get(0),
        environment_id: r.get(1),
        kind: r.get(2),
        payload: r.get(3),
        attempts: r.get(4),
        lease_token: token,
    }))
}

async fn lock_live(tx: &tokio_postgres::Transaction<'_>, job: &ClaimedJob) -> Result<bool, Error> {
    let row=tx.query_opt("SELECT lease_until FROM novu_queue_jobs WHERE id=$1 AND environment_id=$2 AND kind=$3 AND status='running' AND lease_token=$4 FOR UPDATE", &[&job.id,&job.environment_id,&JOB_KIND,&job.lease_token]).await?;
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

/// Handle one job for exactly this environment. No credential is stored in the job.
pub async fn process_one(
    client: &mut Client,
    environment: &str,
    provider: &Sendgrid,
) -> Result<bool, Error> {
    let Some(job) = claim(client, environment, Duration::from_secs(60)).await? else {
        return Ok(false);
    };
    let mut email: Email = match serde_json::from_value(job.payload.clone()) {
        Ok(value) => value,
        Err(_) => {
            crate::fail(client, job.id, job.lease_token, "invalid email job").await?;
            return Ok(true);
        }
    };
    let tx = client.transaction().await?;
    // Subscriber mutations lock this row first. Keep the same ordering before
    // locking the job, so preference/delete cannot race with dispatch setup.
    let subscriber = if let Some(context) = &email.workflow_context {
        let id = email.subscriber_id.as_deref().unwrap_or("");
        let workflow = email.workflow_identifier.as_deref().unwrap_or("");
        tx.query_opt("SELECT s.profile,w.read_only,w.all_enabled,w.email_enabled,(p.channels->>'email')::boolean,(sp.channels->>'email')::boolean,EXISTS(SELECT 1 FROM novu_rust_workflows wf WHERE wf.environment_id=s.environment_id AND wf.identifier=$3 AND ($4::text IS NULL OR wf.inbox_metadata->>'id'=$4)),s.internal_id FROM novu_rust_subscribers s LEFT JOIN novu_rust_workflow_preferences w ON w.environment_id=s.environment_id AND w.workflow_identifier=$3 LEFT JOIN novu_rust_subscriber_preferences p ON p.environment_id=s.environment_id AND p.subscriber_id=s.subscriber_id LEFT JOIN novu_rust_subscriber_workflow_preferences sp ON sp.environment_id=s.environment_id AND sp.subscriber_id=s.subscriber_id AND sp.workflow_identifier=$3 WHERE s.environment_id=$1 AND s.subscriber_id=$2 FOR UPDATE OF s", &[&environment,&id,&workflow,&context.workflow_internal_id]).await?
    } else {
        None
    };
    if !lock_live(&tx, &job).await? {
        tx.rollback().await?;
        return Ok(true);
    }
    // Webhook insertion and this check serialize on the job row. Email
    // events lack per-attempt IDs, so older linked evidence is conservative.
    let evidence = tx.query_opt("SELECT 1 FROM novu_rust_delivery_reconciliations r WHERE r.environment_id=$1 AND r.job_id=$2 AND r.target_index IS NULL AND r.decision='retry' AND EXISTS(SELECT 1 FROM novu_rust_sendgrid_delivery_events e JOIN novu_rust_email_messages m ON m.environment_id=e.environment_id AND m.internal_id=e.message_internal_id WHERE m.environment_id=$1 AND m.email_job_id=$2) LIMIT 1", &[&environment,&job.id]).await?;
    if evidence.is_some() {
        let inserted = tx.execute("INSERT INTO novu_rust_sendgrid_attempts(job_id,environment_id,state) VALUES ($1,$2,'unknown') ON CONFLICT(job_id) DO NOTHING", &[&job.id,&environment]).await?;
        if inserted == 1 {
            tx.execute("UPDATE novu_rust_email_messages SET delivery_state='unknown',status='error',error_id='mail_unexpected_error',error_text='email retry blocked by provider callback' WHERE environment_id=$1 AND email_job_id=$2", &[&environment,&job.id]).await?;
        }
        tx.execute("UPDATE novu_queue_jobs SET status='failed',last_error='email retry blocked by provider callback',lease_token=NULL,lease_until=NULL WHERE id=$1 AND environment_id=$2", &[&job.id,&environment]).await?;
        tx.commit().await?;
        return Ok(true);
    }
    if let Some(context) = email.workflow_context.take() {
        let skip = if let Some(row) = subscriber.as_ref() {
            if !crate::allow_recreated_subscriber()
                && row.get::<_, String>(7) != context.subscriber_internal_id
            {
                Some(("failed", "failed", "subscriber identity changed"))
            } else if context.check_workflow && !row.get::<_, bool>(6) {
                Some(("failed", "failed", "workflow not found"))
            } else if !novu_rust_contracts::preference::in_app_enabled(
                row.get(1),
                row.get(2),
                row.get(3),
                row.get(4),
                row.get(5),
            ) {
                Some(("canceled", "cancelled", "email preference disabled"))
            } else {
                None
            }
        } else {
            Some(("failed", "failed", "subscriber not found"))
        };
        if let Some((outcome, status, diagnostic)) = skip {
            tx.execute(
                "DELETE FROM novu_rust_email_messages WHERE environment_id=$1 AND email_job_id=$2",
                &[&environment, &job.id],
            )
            .await?;
            tx.execute("UPDATE novu_rust_email_step_outcomes SET status=$3,email_job_id=NULL WHERE environment_id=$1 AND email_job_id=$2", &[&environment,&job.id,&outcome]).await?;
            tx.execute("UPDATE novu_queue_jobs SET status=$2,last_error=$3,lease_token=NULL,lease_until=NULL WHERE id=$1", &[&job.id,&status,&diagnostic]).await?;
            tx.commit().await?;
            return Ok(true);
        }
        let mut profile: serde_json::Value = subscriber.as_ref().unwrap().get(0);
        if let Some(obj) = profile.as_object_mut() {
            obj.insert(
                "subscriberId".into(),
                email.subscriber_id.clone().unwrap_or_default().into(),
            );
        }
        // The original channel worker resolves actor and contexts at execution
        // time. Old jobs without context_keys retain their stored snapshot.
        let actor = match crate::resolve_actor(
            &tx,
            environment,
            context.actor.as_ref(),
            context.actor_internal_id.as_deref(),
            email.subscriber_id.as_deref().unwrap_or(""),
            &profile,
        )
        .await?
        {
            Ok(actor) => actor,
            Err(()) => {
                tx.execute("DELETE FROM novu_rust_email_messages WHERE environment_id=$1 AND email_job_id=$2", &[&environment,&job.id]).await?;
                tx.execute("UPDATE novu_rust_email_step_outcomes SET status='failed',email_job_id=NULL WHERE environment_id=$1 AND email_job_id=$2", &[&environment,&job.id]).await?;
                tx.execute("UPDATE novu_queue_jobs SET status='failed',last_error='actor identity changed or missing',lease_token=NULL,lease_until=NULL WHERE id=$1", &[&job.id]).await?;
                tx.commit().await?;
                return Ok(true);
            }
        };
        let resolved_context = if let Some(keys) = context.context_keys.as_ref() {
            let mut result = serde_json::Map::new();
            if !keys.is_empty() {
                for row in tx.query("SELECT context_key,data FROM novu_rust_contexts WHERE environment_id=$1 AND context_key=ANY($2)", &[&environment,&keys]).await? {
                    let key: String = row.get(0);
                    let data: serde_json::Value = row.get(1);
                    if let Some((kind,id)) = key.split_once(':') { result.insert(kind.to_owned(),serde_json::json!({"id":id,"data":data})); }
                }
            }
            serde_json::Value::Object(result)
        } else {
            context.context.clone()
        };
        let option = |key: &str| {
            context
                .overrides
                .get(key)
                .and_then(serde_json::Value::as_str)
                .filter(|v| !v.is_empty())
        };
        let address = option("toRecipient").or_else(|| {
            profile
                .get("email")
                .and_then(serde_json::Value::as_str)
                .filter(|v| !v.is_empty())
        });
        if let (
            Some(message_id),
            Some(notification_id),
            Some(subscriber_id),
            Some(workflow),
            Some(transaction),
        ) = (
            email.message_id.as_ref(),
            context.notification_internal_id.as_ref(),
            email.subscriber_id.as_ref(),
            email.workflow_identifier.as_ref(),
            email.transaction_id.as_ref(),
        ) {
            tx.execute("INSERT INTO novu_rust_email_messages(internal_id,environment_id,subscriber_id,subscriber_internal_id,notification_internal_id,workflow_identifier,transaction_id,email,subject,status,email_job_id) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'sent',$10) ON CONFLICT(internal_id) DO NOTHING", &[&message_id,&environment,&subscriber_id,&context.subscriber_internal_id,&notification_id,&workflow,&transaction,&address,&context.subject_template,&job.id]).await?;
        }
        let replace = context
            .overrides
            .get("replaceToRecipient")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        let mut to = Vec::new();
        if !replace && let Some(address) = address {
            to.push(address.to_owned());
        }
        if let Some(extra) = context
            .overrides
            .get("to")
            .and_then(serde_json::Value::as_array)
        {
            for value in extra {
                if let Some(value) = value.as_str()
                    && (replace || !to.iter().any(|v| v == value))
                {
                    to.push(value.to_owned());
                }
            }
        }
        if to.is_empty() || (!replace && address.is_none()) {
            tx.execute("UPDATE novu_rust_email_messages SET status='warning',email=NULL,error_id='mail_unexpected_error',error_text='Subscriber does not have an email address' WHERE environment_id=$1 AND email_job_id=$2", &[&environment,&job.id]).await?;
            tx.execute("UPDATE novu_rust_email_step_outcomes SET status='missing_email',email_job_id=NULL WHERE environment_id=$1 AND email_job_id=$2", &[&environment,&job.id]).await?;
            tx.execute("UPDATE novu_queue_jobs SET status='cancelled',last_error='subscriber missing email',lease_token=NULL,lease_until=NULL WHERE id=$1", &[&job.id]).await?;
            tx.commit().await?;
            return Ok(true);
        }
        let render = |template: &str| {
            novu_rust_contracts::template::render_in_app_with_actor(
                template,
                &context.payload,
                &profile,
                &resolved_context,
                actor.as_ref(),
            )
        };
        let rendered = render(&context.subject_template)
            .and_then(|subject| render(&context.html_template).map(|html| (subject, html)));
        let rendered = rendered.and_then(|(subject, html)| {
            static INLINER: std::sync::OnceLock<css_inline::CSSInliner> =
                std::sync::OnceLock::new();
            INLINER
                .get_or_init(|| {
                    css_inline::CSSInliner::options()
                        .load_remote_stylesheets(false)
                        .keep_link_tags(true)
                        .keep_at_rules(true)
                        .build()
                })
                .inline(&html)
                .map(|html| (subject, html))
                .map_err(|_| "unsupported email html")
        });
        let (subject, html) = match rendered {
            Ok(value) => value,
            Err(_) => {
                tx.execute("UPDATE novu_rust_email_messages SET status='error',error_id='mail_unexpected_error',error_text='unsupported email template or input' WHERE environment_id=$1 AND email_job_id=$2", &[&environment,&job.id]).await?;
                tx.execute("UPDATE novu_rust_email_step_outcomes SET status='failed',email_job_id=NULL WHERE environment_id=$1 AND email_job_id=$2", &[&environment,&job.id]).await?;
                tx.execute("UPDATE novu_queue_jobs SET status='failed',last_error='unsupported email template or input',lease_token=NULL,lease_until=NULL WHERE id=$1", &[&job.id]).await?;
                tx.commit().await?;
                return Ok(true);
            }
        };
        email.to = to;
        email.subject = option("subject").unwrap_or(subject.trim()).to_owned();
        email.html = option("html")
            .or_else(|| option("text"))
            .unwrap_or(&html)
            .to_owned();
        tx.execute("UPDATE novu_rust_email_messages SET email=$3 WHERE environment_id=$1 AND email_job_id=$2", &[&environment,&job.id,&address]).await?;
    }
    let inserted=tx.execute("INSERT INTO novu_rust_sendgrid_attempts(job_id,environment_id,state) VALUES ($1,$2,'dispatching') ON CONFLICT(job_id) DO NOTHING", &[&job.id,&job.environment_id]).await?;
    if inserted == 0 {
        tx.execute("UPDATE novu_queue_jobs SET status='failed',last_error='email attempt already exists',lease_token=NULL,lease_until=NULL WHERE id=$1", &[&job.id]).await?;
        tx.commit().await?;
        return Ok(true);
    }
    tx.execute("UPDATE novu_rust_email_messages SET delivery_state='dispatching' WHERE environment_id=$1 AND email_job_id=$2", &[&environment,&job.id]).await?;
    tx.commit().await?;
    let (state, status, provider_id, diagnostic) = match provider.send(&email).await {
        Ok(receipt) => (
            "accepted",
            None,
            receipt.id.map(|v| v.chars().take(1024).collect::<String>()),
            None,
        ),
        Err(SendError::Provider(code)) if code >= 500 => (
            "unknown",
            Some(i32::from(code)),
            None,
            Some("email provider outcome unknown"),
        ),
        Err(SendError::Provider(code)) => (
            "rejected",
            Some(i32::from(code)),
            None,
            Some("email provider rejected request"),
        ),
        Err(SendError::Transport) => (
            "unknown",
            None,
            None,
            Some("email transport outcome unknown"),
        ),
        Err(_) => ("rejected", None, None, Some("invalid email request")),
    };
    let tx = client.transaction().await?;
    if !lock_live(&tx, &job).await? {
        tx.rollback().await?;
        return Ok(true);
    }
    let updated=tx.execute("UPDATE novu_rust_sendgrid_attempts SET state=$2,http_status=$3,provider_message_id=$4,updated_at=clock_timestamp() WHERE job_id=$1 AND state='dispatching'", &[&job.id,&state,&status,&provider_id]).await?;
    if updated == 1 {
        tx.execute("UPDATE novu_rust_email_messages SET delivery_state=$3 WHERE environment_id=$1 AND email_job_id=$2", &[&environment,&job.id,&state]).await?;
        if state == "accepted" {
            tx.execute("UPDATE novu_rust_email_messages SET identifier=$3 WHERE environment_id=$1 AND email_job_id=$2", &[&job.environment_id,&job.id,&provider_id]).await?;
        } else {
            // Provider response text and headers may contain secrets or PII;
            // preserve a fixed diagnostic rather than persisting raw responses.
            tx.execute("UPDATE novu_rust_email_messages SET status='error',error_id='mail_unexpected_error',error_text=$3 WHERE environment_id=$1 AND email_job_id=$2", &[&job.environment_id,&job.id,&diagnostic]).await?;
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

/// Requeue only leases that expired before dispatch was recorded. Once dispatch
/// was recorded, mark outcome unknown and never send automatically again.
pub async fn recover_expired(client: &mut Client, environment: &str) -> Result<usize, Error> {
    let tx = client.transaction().await?;
    let rows=tx.query("SELECT id FROM novu_queue_jobs WHERE environment_id=$1 AND kind=$2 AND status='running' AND lease_until<=clock_timestamp() ORDER BY lease_until,id FOR UPDATE SKIP LOCKED LIMIT 100", &[&environment,&JOB_KIND]).await?;
    for row in &rows {
        let id: Uuid = row.get(0);
        let attempted=tx.query_opt("SELECT state FROM novu_rust_sendgrid_attempts WHERE job_id=$1 AND environment_id=$2", &[&id,&environment]).await?.is_some();
        if attempted {
            tx.execute("UPDATE novu_rust_sendgrid_attempts SET state='unknown',updated_at=clock_timestamp() WHERE job_id=$1 AND state='dispatching'", &[&id]).await?;
            tx.execute("UPDATE novu_rust_email_messages SET delivery_state='unknown' WHERE environment_id=$1 AND email_job_id=$2", &[&environment,&id]).await?;
            tx.execute("UPDATE novu_queue_jobs SET status='failed',last_error='email delivery outcome unknown',lease_token=NULL,lease_until=NULL WHERE id=$1", &[&id]).await?;
        } else {
            tx.execute("UPDATE novu_queue_jobs SET status='pending',lease_token=NULL,lease_until=NULL WHERE id=$1", &[&id]).await?;
        }
    }
    tx.commit().await?;
    Ok(rows.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sendgrid::{Config, Region};
    use serde_json::json;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };
    use tokio_postgres::NoTls;

    /// Consumes a workflow-produced job against a loopback mock for the
    /// original-server differential harness. No production endpoint override.
    #[tokio::test]
    #[ignore = "requires differential harness PostgreSQL and loopback access"]
    async fn capture_reference_workflow_email() {
        let (mut db, driver) =
            tokio_postgres::connect(&std::env::var("TEST_DATABASE_URL").unwrap(), NoTls)
                .await
                .unwrap();
        tokio::spawn(async move { driver.await.unwrap() });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/v3/mail/send", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let accepted = if std::env::var_os("TEST_EMAIL_ALLOW_NO_REQUEST").is_some() {
                tokio::time::timeout(Duration::from_secs(1), listener.accept())
                    .await
                    .ok()
            } else {
                Some(listener.accept().await)
            };
            let Some(accepted) = accepted else {
                std::fs::write(std::env::var("TEST_EMAIL_CAPTURE_PATH").unwrap(), b"null").unwrap();
                return;
            };
            let (mut socket, _) = accepted.unwrap();
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
                        std::fs::write(
                            std::env::var("TEST_EMAIL_CAPTURE_PATH").unwrap(),
                            &buf[pos + 4..pos + 4 + length],
                        )
                        .unwrap();
                        break;
                    }
                }
            }
            let code = std::env::var("TEST_EMAIL_HTTP_STATUS").unwrap_or_else(|_| "202".into());
            assert!(code == "202" || code == "429");
            socket
                .write_all(
                    format!(
                        "HTTP/1.1 {code} Test\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
        });
        let provider = Sendgrid::test_endpoint(
            Config {
                api_key: "SG.fixture".into(),
                from: "sender@example.test".into(),
                sender_name: "Sender".into(),
                region: Region::Global,
            },
            &endpoint,
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
    async fn email_attempt_recovery_and_tenant_isolation() {
        let url = std::env::var("TEST_DATABASE_URL").unwrap();
        let (mut db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
        tokio::spawn(async move { driver.await.unwrap() });
        db.batch_execute(
            "DROP TABLE IF EXISTS novu_rust_sendgrid_attempts,novu_queue_jobs CASCADE",
        )
        .await
        .unwrap();
        db.batch_execute(crate::MIGRATION).await.unwrap();
        db.batch_execute(MIGRATION).await.unwrap();
        db.batch_execute(crate::in_app::EMAIL_MESSAGES_MIGRATION)
            .await
            .unwrap();
        db.batch_execute(crate::in_app::EMAIL_PROVIDER_RESULT_MIGRATION)
            .await
            .unwrap();
        db.batch_execute("ALTER TABLE novu_rust_email_messages ADD COLUMN IF NOT EXISTS delivery_state text NOT NULL DEFAULT 'not_attempted'").await.unwrap();
        db.batch_execute(crate::reconciliation::MIGRATION)
            .await
            .unwrap();
        db.batch_execute(crate::reconciliation::RETRY_DISPATCH_MIGRATION)
            .await
            .unwrap();
        db.batch_execute("CREATE TABLE IF NOT EXISTS novu_rust_environments(id text PRIMARY KEY, organization_id text NOT NULL); INSERT INTO novu_rust_environments VALUES ('env-a','org') ON CONFLICT DO NOTHING").await.unwrap();
        db.batch_execute(include_str!(
            "../../../api/rust-api/migrations/0004_sendgrid_webhooks.sql"
        ))
        .await
        .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/v3/mail/send", listener.local_addr().unwrap());
        let provider = Sendgrid::test_endpoint(
            Config {
                api_key: "SG.fixture".into(),
                from: "sender@example.test".into(),
                sender_name: "Sender".into(),
                region: Region::Global,
            },
            &endpoint,
        )
        .unwrap();
        let server = tokio::spawn(async move {
            for response in [Some(202), Some(429), Some(500), None] {
                let (mut socket, _) = listener.accept().await.unwrap();
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
                            break;
                        }
                    }
                }
                let text = String::from_utf8(buf).unwrap();
                assert!(
                    text.to_ascii_lowercase()
                        .contains("authorization: bearer sg.fixture")
                );
                assert!(
                    !text
                        .split_once("\r\n\r\n")
                        .unwrap()
                        .1
                        .contains("SG.fixture")
                );
                let body: serde_json::Value =
                    serde_json::from_str(text.split_once("\r\n\r\n").unwrap().1).unwrap();
                assert_eq!(body["template_id"], "d-123");
                assert_eq!(
                    body["personalizations"][0]["dynamic_template_data"]["name"],
                    "Alice"
                );
                assert_eq!(body["attachments"][0]["content"], "aGVsbG8=");
                assert_eq!(body["attachments"][0]["disposition"], "inline");
                if let Some(code) = response {
                    socket.write_all(format!("HTTP/1.1 {code} Test\r\nContent-Length: 0\r\nX-Message-Id: provider-1\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
                }
            }
        });
        let message = json!({"to":["alice@example.test"],"subject":"Hello","html":"<p>Hello</p>","custom_data":{"templateId":"d-123","dynamicTemplateData":{"name":"Alice"}},"attachments":[{"file":[104,101,108,108,111],"name":"a.txt","mime":"text/plain","cid":"part"}]});
        let other = Uuid::new_v4();
        crate::enqueue(&db, other, "env-b", JOB_KIND, &message)
            .await
            .unwrap();
        for (state, expected_status, code) in [
            ("accepted", "completed", None),
            ("rejected", "failed", Some(429_i32)),
            ("unknown", "failed", Some(500_i32)),
            ("unknown", "failed", None),
        ] {
            let id = Uuid::new_v4();
            crate::enqueue(&db, id, "env-a", JOB_KIND, &message)
                .await
                .unwrap();
            let message_id = id.to_string();
            db.execute("INSERT INTO novu_rust_email_messages(internal_id,environment_id,subscriber_id,subscriber_internal_id,notification_internal_id,workflow_identifier,transaction_id,subject,status,email_job_id) VALUES ($1,'env-a','alice','subscriber','notification','workflow','transaction','Hello','sent',$2)", &[&message_id,&id]).await.unwrap();
            assert!(process_one(&mut db, "env-a", &provider).await.unwrap());
            let row=db.query_one("SELECT a.state,j.status,a.http_status,a.provider_message_id FROM novu_rust_sendgrid_attempts a JOIN novu_queue_jobs j ON j.id=a.job_id WHERE j.id=$1", &[&id]).await.unwrap();
            assert_eq!(row.get::<_, String>(0), state);
            assert_eq!(row.get::<_, String>(1), expected_status);
            assert_eq!(row.get::<_, Option<i32>>(2), code);
            if state == "accepted" {
                assert_eq!(
                    row.get::<_, Option<String>>(3).as_deref(),
                    Some("provider-1")
                );
            }
            let message_row = db.query_one("SELECT status,error_id,error_text,identifier FROM novu_rust_email_messages WHERE environment_id='env-a' AND internal_id=$1", &[&message_id]).await.unwrap();
            let delivery_state: String = db
                .query_one(
                    "SELECT delivery_state FROM novu_rust_email_messages WHERE internal_id=$1",
                    &[&message_id],
                )
                .await
                .unwrap()
                .get(0);
            assert_eq!(delivery_state, state);
            if state == "accepted" {
                assert_eq!(message_row.get::<_, String>(0), "sent");
                assert_eq!(
                    message_row.get::<_, Option<String>>(3).as_deref(),
                    Some("provider-1")
                );
            } else {
                assert_eq!(message_row.get::<_, String>(0), "error");
                assert_eq!(
                    message_row.get::<_, Option<String>>(1).as_deref(),
                    Some("mail_unexpected_error")
                );
                assert!(message_row.get::<_, Option<String>>(2).is_some());
                assert!(message_row.get::<_, Option<String>>(3).is_none());
            }
        }
        server.await.unwrap();
        // Competing workers must not both send the same job.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/v3/mail/send", listener.local_addr().unwrap());
        let concurrent_provider = Sendgrid::test_endpoint(
            Config {
                api_key: "SG.fixture".into(),
                from: "sender@example.test".into(),
                sender_name: "Sender".into(),
                region: Region::Global,
            },
            &endpoint,
        )
        .unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
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
                        break;
                    }
                }
            }
            socket
                .write_all(b"HTTP/1.1 202 Test\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .await
                .unwrap();
            assert!(
                tokio::time::timeout(Duration::from_millis(300), listener.accept())
                    .await
                    .is_err(),
                "duplicate provider request"
            );
        });
        let concurrent = Uuid::new_v4();
        crate::enqueue(&db, concurrent, "env-a", JOB_KIND, &message)
            .await
            .unwrap();
        let (mut db2, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
        tokio::spawn(async move { driver.await.unwrap() });
        let (first, second) = tokio::join!(
            process_one(&mut db, "env-a", &concurrent_provider),
            process_one(&mut db2, "env-a", &concurrent_provider)
        );
        assert_ne!(first.unwrap(), second.unwrap());
        server.await.unwrap();
        assert_eq!(db.query_one("SELECT count(*) FROM novu_rust_sendgrid_attempts WHERE job_id=$1 AND state='accepted'", &[&concurrent]).await.unwrap().get::<_,i64>(0),1);
        assert_eq!(
            db.query_one("SELECT status FROM novu_queue_jobs WHERE id=$1", &[&other])
                .await
                .unwrap()
                .get::<_, String>(0),
            "pending"
        );
        assert!(!process_one(&mut db, "env-a", &provider).await.unwrap());
        let sensitive:i64=db.query_one("SELECT count(*) FROM novu_queue_jobs WHERE payload::text LIKE '%SG.fixture%' OR last_error LIKE '%SG.fixture%'",&[]).await.unwrap().get(0);
        assert_eq!(sensitive, 0);
        // Before the durable dispatch marker, an expired lease may be retried.
        let pre = Uuid::new_v4();
        crate::enqueue(&db, pre, "env-a", JOB_KIND, &message)
            .await
            .unwrap();
        let claimed = claim(&db, "env-a", Duration::from_secs(60))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(claimed.id, pre);
        db.execute(
            "UPDATE novu_queue_jobs SET lease_until=now()-interval '1 second' WHERE id=$1",
            &[&pre],
        )
        .await
        .unwrap();
        assert_eq!(recover_expired(&mut db, "env-a").await.unwrap(), 1);
        assert_eq!(
            db.query_one("SELECT status FROM novu_queue_jobs WHERE id=$1", &[&pre])
                .await
                .unwrap()
                .get::<_, String>(0),
            "pending"
        );
        assert!(crate::cancel_pending(&db, "env-a", pre).await.unwrap());
        // After the marker, a crash or lease expiry is ambiguous; never resend.
        let post = Uuid::new_v4();
        crate::enqueue(&db, post, "env-a", JOB_KIND, &message)
            .await
            .unwrap();
        let claimed = claim(&db, "env-a", Duration::from_secs(60))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(claimed.id, post);
        db.execute("INSERT INTO novu_rust_email_messages(internal_id,environment_id,subscriber_id,subscriber_internal_id,notification_internal_id,workflow_identifier,transaction_id,subject,status,email_job_id,delivery_state) VALUES ($1,'env-a','alice','subscriber','notification','workflow','transaction','Hello','sent',$2,'dispatching')", &[&post.to_string(),&post]).await.unwrap();
        db.execute("INSERT INTO novu_rust_sendgrid_attempts(job_id,environment_id,state) VALUES ($1,'env-a','dispatching')", &[&post]).await.unwrap();
        db.execute(
            "UPDATE novu_queue_jobs SET lease_until=now()-interval '1 second' WHERE id=$1",
            &[&post],
        )
        .await
        .unwrap();
        assert_eq!(recover_expired(&mut db, "env-a").await.unwrap(), 1);
        let row=db.query_one("SELECT j.status,a.state FROM novu_queue_jobs j JOIN novu_rust_sendgrid_attempts a ON j.id=a.job_id WHERE j.id=$1", &[&post]).await.unwrap();
        assert_eq!(row.get::<_, String>(0), "failed");
        assert_eq!(row.get::<_, String>(1), "unknown");
        assert_eq!(
            db.query_one(
                "SELECT delivery_state FROM novu_rust_email_messages WHERE email_job_id=$1",
                &[&post]
            )
            .await
            .unwrap()
            .get::<_, String>(0),
            "unknown"
        );
        assert!(!process_one(&mut db, "env-a", &provider).await.unwrap());
        crate::reconciliation::resolve(
            &mut db,
            "env-a",
            post,
            None,
            "retry",
            "fixture-operator",
            "provider confirmed no request",
        )
        .await
        .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/v3/mail/send", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = [0; 8192];
            assert!(socket.read(&mut buf).await.unwrap() > 0);
            socket.write_all(b"HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\nX-Message-Id: retry-receipt\r\nConnection: close\r\n\r\n").await.unwrap();
        });
        let provider = Sendgrid::test_endpoint(
            Config {
                api_key: "SG.fixture".into(),
                from: "sender@example.test".into(),
                sender_name: "Sender".into(),
                region: Region::Global,
            },
            &endpoint,
        )
        .unwrap();
        assert!(process_one(&mut db, "env-a", &provider).await.unwrap());
        server.await.unwrap();
        let row=db.query_one("SELECT j.status,a.state,m.delivery_state FROM novu_queue_jobs j JOIN novu_rust_sendgrid_attempts a ON a.job_id=j.id JOIN novu_rust_email_messages m ON m.email_job_id=j.id WHERE j.id=$1", &[&post]).await.unwrap();
        assert_eq!(row.get::<_, String>(0), "completed");
        assert_eq!(row.get::<_, String>(1), "accepted");
        assert_eq!(row.get::<_, String>(2), "accepted");
        assert!(!process_one(&mut db, "env-a", &provider).await.unwrap());

        // Evidence arriving after the operator's retry decision stops the
        // queued job before another provider request is made.
        let late = Uuid::new_v4();
        crate::enqueue(&db, late, "env-a", JOB_KIND, &message)
            .await
            .unwrap();
        db.execute(
            "UPDATE novu_queue_jobs SET status='failed' WHERE id=$1",
            &[&late],
        )
        .await
        .unwrap();
        db.execute("INSERT INTO novu_rust_sendgrid_attempts(job_id,environment_id,state) VALUES ($1,'env-a','unknown')", &[&late]).await.unwrap();
        db.execute("INSERT INTO novu_rust_email_messages(internal_id,environment_id,subscriber_id,subscriber_internal_id,notification_internal_id,workflow_identifier,transaction_id,subject,status,email_job_id,delivery_state) VALUES ('late-email','env-a','s','s','n','w','t','subject','error',$1,'unknown')", &[&late]).await.unwrap();
        crate::reconciliation::resolve(
            &mut db, "env-a", late, None, "retry", "operator", "verified",
        )
        .await
        .unwrap();
        db.execute("INSERT INTO novu_rust_sendgrid_delivery_events(environment_id,event_id,message_internal_id,event_type,event_timestamp,recipient_hash) VALUES ('env-a','late','late-email','delivered',1,'hash')", &[]).await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let provider = Sendgrid::test_endpoint(
            Config {
                api_key: "SG.fixture".into(),
                from: "sender@example.test".into(),
                sender_name: "Sender".into(),
                region: Region::Global,
            },
            &format!("http://{}/v3/mail/send", listener.local_addr().unwrap()),
        )
        .unwrap();
        assert!(process_one(&mut db, "env-a", &provider).await.unwrap());
        assert!(
            tokio::time::timeout(Duration::from_millis(100), listener.accept())
                .await
                .is_err()
        );
        let row = db.query_one("SELECT j.status,j.last_error,a.state,m.delivery_state FROM novu_queue_jobs j JOIN novu_rust_sendgrid_attempts a ON a.job_id=j.id JOIN novu_rust_email_messages m ON m.email_job_id=j.id WHERE j.id=$1", &[&late]).await.unwrap();
        assert_eq!(row.get::<_, String>(0), "failed");
        assert_eq!(
            row.get::<_, String>(1),
            "email retry blocked by provider callback"
        );
        assert_eq!(row.get::<_, String>(2), "unknown");
        assert_eq!(row.get::<_, String>(3), "unknown");
    }
}
