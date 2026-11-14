//! Experimental Slack workflow fanout and fenced per-endpoint delivery.
use crate::{
    ClaimedJob,
    slack::{Message, SendError, Slack},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::HashMap;
use tokio_postgres::{Client, Error, Transaction};
use uuid::Uuid;

pub const FANOUT_KIND: &str = "experimental_slack_workflow_fanout";
pub const DELIVERY_KIND: &str = "experimental_slack_workflow_delivery";
pub const MIGRATION: &str = include_str!("../migrations/0078_slack_workflow.sql");
pub type Providers = HashMap<(String, String), Slack>;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Context {
    pub subscriber_id: String,
    pub subscriber_internal_id: String,
    pub workflow_identifier: String,
    pub workflow_internal_id: Option<String>,
    pub content_template: String,
    pub payload: Value,
    pub context_keys: Vec<String>,
    pub actor: Option<Value>,
    pub actor_internal_id: Option<String>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Delivery {
    context: Context,
    endpoint_id: Uuid,
    target: String,
    integration: String,
    connection: Option<String>,
}

async fn claim(db: &Client, environment: &str, kind: &str) -> Result<Option<ClaimedJob>, Error> {
    let token = Uuid::new_v4();
    let row=db.query_opt("WITH candidate AS (SELECT id FROM novu_queue_jobs WHERE environment_id=$1 AND kind=$2 AND status='pending' AND available_at<=now() ORDER BY available_at,created_at,id FOR UPDATE SKIP LOCKED LIMIT 1) UPDATE novu_queue_jobs j SET status='running',attempts=j.attempts+1,lease_token=$3,lease_until=clock_timestamp()+interval '60 seconds' FROM candidate WHERE j.id=candidate.id RETURNING j.id,j.environment_id,j.kind,j.payload,j.attempts",&[&environment,&kind,&token]).await?;
    Ok(row.map(|r| ClaimedJob {
        id: r.get(0),
        environment_id: r.get(1),
        kind: r.get(2),
        payload: r.get(3),
        attempts: r.get(4),
        lease_token: token,
    }))
}
async fn live(tx: &Transaction<'_>, job: &ClaimedJob) -> Result<bool, Error> {
    let row=tx.query_opt("SELECT lease_until>clock_timestamp() FROM novu_queue_jobs WHERE id=$1 AND environment_id=$2 AND kind=$3 AND status='running' AND lease_token=$4 FOR UPDATE",&[&job.id,&job.environment_id,&job.kind,&job.lease_token]).await?;
    if row.is_none() {
        return Ok(false);
    }
    Ok(tx
        .query_one(
            "SELECT lease_until>clock_timestamp() FROM novu_queue_jobs WHERE id=$1",
            &[&job.id],
        )
        .await?
        .get(0))
}
struct SubscriberState {
    profile: Value,
    internal_id: String,
    enabled: bool,
    workflow_exists: bool,
}
async fn subscriber(
    tx: &Transaction<'_>,
    job: &ClaimedJob,
    ctx: &Context,
) -> Result<Option<SubscriberState>, Error> {
    let Some(row)=tx.query_opt("SELECT profile,internal_id FROM novu_rust_subscribers WHERE environment_id=$1 AND subscriber_id=$2 FOR UPDATE",&[&job.environment_id,&ctx.subscriber_id]).await? else {return Ok(None)};
    // Use a fresh statement snapshot after acquiring the subscriber lock: a
    // preference update that held that lock may have committed while we waited.
    let pref=tx.query_one("SELECT w.read_only,w.all_enabled,w.chat_enabled,(p.channels->>'chat')::boolean,(sp.channels->>'chat')::boolean,EXISTS(SELECT 1 FROM novu_rust_workflows wf WHERE wf.environment_id=$1 AND wf.identifier=$3 AND ($4::text IS NULL OR wf.inbox_metadata->>'id'=$4)) FROM (SELECT 1) singleton LEFT JOIN novu_rust_workflow_preferences w ON w.environment_id=$1 AND w.workflow_identifier=$3 LEFT JOIN novu_rust_subscriber_preferences p ON p.environment_id=$1 AND p.subscriber_id=$2 LEFT JOIN novu_rust_subscriber_workflow_preferences sp ON sp.environment_id=$1 AND sp.subscriber_id=$2 AND sp.workflow_identifier=$3",&[&job.environment_id,&ctx.subscriber_id,&ctx.workflow_identifier,&ctx.workflow_internal_id]).await?;
    Ok(Some(SubscriberState {
        profile: row.get(0),
        internal_id: row.get(1),
        enabled: novu_rust_contracts::preference::in_app_enabled(
            pref.get(0),
            pref.get(1),
            pref.get(2),
            pref.get(3),
            pref.get(4),
        ),
        workflow_exists: pref.get(5),
    }))
}
fn valid_subscriber(row: Option<&SubscriberState>, ctx: &Context) -> Result<(), &'static str> {
    let Some(row) = row else {
        return Err("subscriber not found");
    };
    if row.internal_id != ctx.subscriber_internal_id {
        return Err("subscriber identity changed");
    }
    if !row.workflow_exists {
        return Err("workflow not found");
    }
    if !row.enabled {
        return Err("chat preference disabled");
    }
    Ok(())
}
async fn terminal(
    tx: &Transaction<'_>,
    job: &ClaimedJob,
    status: &str,
    error: Option<&str>,
) -> Result<(), Error> {
    tx.execute("UPDATE novu_queue_jobs SET status=$2,last_error=$3,lease_token=NULL,lease_until=NULL WHERE id=$1",&[&job.id,&status,&error]).await?;
    Ok(())
}
pub async fn fanout_one(db: &mut Client, environment: &str) -> Result<bool, Error> {
    let Some(job) = claim(db, environment, FANOUT_KIND).await? else {
        return Ok(false);
    };
    let ctx: Context = match serde_json::from_value(job.payload.clone()) {
        Ok(v) => v,
        Err(_) => {
            crate::fail(db, job.id, job.lease_token, "invalid Slack workflow job").await?;
            return Ok(true);
        }
    };
    let tx = db.transaction().await?;
    let row = subscriber(&tx, &job, &ctx).await?;
    if !live(&tx, &job).await? {
        tx.rollback().await?;
        return Ok(true);
    }
    if let Err(reason) = valid_subscriber(row.as_ref(), &ctx) {
        terminal(
            &tx,
            &job,
            if reason == "chat preference disabled" {
                "cancelled"
            } else {
                "failed"
            },
            Some(reason),
        )
        .await?;
        tx.commit().await?;
        return Ok(true);
    }
    let endpoints=tx.query("SELECT internal_id,endpoint_type,endpoint,integration_identifier,connection_identifier FROM novu_rust_slack_channel_endpoints WHERE environment_id=$1 AND subscriber_internal_id=$2 AND context_keys @> $3 AND context_keys <@ $3 AND cardinality(context_keys)=cardinality($3) ORDER BY created_at,internal_id",&[&environment,&ctx.subscriber_internal_id,&ctx.context_keys]).await?;
    for row in endpoints {
        let id: Uuid = row.get(0);
        let kind: String = row.get(1);
        let endpoint: Value = row.get(2);
        let Some(target) = endpoint
            .get(if kind == "slack_channel" {
                "channelId"
            } else {
                "userId"
            })
            .and_then(Value::as_str)
        else {
            continue;
        };
        let delivery = Delivery {
            context: ctx.clone(),
            endpoint_id: id,
            target: target.to_owned(),
            integration: row.get(3),
            connection: row.get(4),
        };
        let payload = serde_json::to_value(delivery).expect("serializable Slack workflow context");
        let child = Uuid::new_v4();
        tx.execute(
            "INSERT INTO novu_queue_jobs(id,environment_id,kind,payload) VALUES ($1,$2,$3,$4)",
            &[&child, &environment, &DELIVERY_KIND, &payload],
        )
        .await?;
        tx.execute("INSERT INTO novu_rust_slack_workflow_fanout(parent_job_id,environment_id,endpoint_internal_id,delivery_job_id) VALUES ($1,$2,$3,$4)",&[&job.id,&environment,&id,&child]).await?;
    }
    terminal(&tx, &job, "completed", None).await?;
    tx.commit().await?;
    Ok(true)
}
pub async fn delivery_one(
    db: &mut Client,
    environment: &str,
    providers: &Providers,
) -> Result<bool, Error> {
    let Some(job) = claim(db, environment, DELIVERY_KIND).await? else {
        return Ok(false);
    };
    let delivery: Delivery = match serde_json::from_value(job.payload.clone()) {
        Ok(v) => v,
        Err(_) => {
            crate::fail(
                db,
                job.id,
                job.lease_token,
                "invalid Slack workflow delivery",
            )
            .await?;
            return Ok(true);
        }
    };
    let ctx = &delivery.context;
    let tx = db.transaction().await?;
    let row = subscriber(&tx, &job, ctx).await?;
    if !live(&tx, &job).await? {
        tx.rollback().await?;
        return Ok(true);
    }
    if let Err(reason) = valid_subscriber(row.as_ref(), ctx) {
        terminal(
            &tx,
            &job,
            if reason == "chat preference disabled" {
                "cancelled"
            } else {
                "failed"
            },
            Some(reason),
        )
        .await?;
        tx.commit().await?;
        return Ok(true);
    }
    let endpoint=tx.query_opt("SELECT endpoint_type,endpoint,integration_identifier,connection_identifier FROM novu_rust_slack_channel_endpoints WHERE environment_id=$1 AND internal_id=$2 AND subscriber_internal_id=$3 AND context_keys @> $4 AND context_keys <@ $4 AND cardinality(context_keys)=cardinality($4) FOR SHARE",&[&environment,&delivery.endpoint_id,&ctx.subscriber_internal_id,&ctx.context_keys]).await?;
    let unchanged = endpoint.as_ref().is_some_and(|r| {
        let kind: String = r.get(0);
        let value: Value = r.get(1);
        let integration: String = r.get(2);
        let connection: Option<String> = r.get(3);
        value
            .get(if kind == "slack_channel" {
                "channelId"
            } else {
                "userId"
            })
            .and_then(Value::as_str)
            == Some(delivery.target.as_str())
            && integration == delivery.integration
            && connection == delivery.connection
    });
    if !unchanged {
        terminal(
            &tx,
            &job,
            "failed",
            Some("Slack endpoint changed or deleted"),
        )
        .await?;
        tx.commit().await?;
        return Ok(true);
    }
    let Some(provider) = providers.get(&(
        delivery.integration.clone(),
        delivery.connection.clone().unwrap_or_default(),
    )) else {
        terminal(
            &tx,
            &job,
            "failed",
            Some("Slack integration credentials not configured"),
        )
        .await?;
        tx.commit().await?;
        return Ok(true);
    };
    let mut profile: Value = row.as_ref().unwrap().profile.clone();
    if let Some(fields) = profile.as_object_mut() {
        fields.insert("subscriberId".into(), ctx.subscriber_id.clone().into());
    }
    let actor = match crate::resolve_actor(
        &tx,
        environment,
        ctx.actor.as_ref(),
        ctx.actor_internal_id.as_deref(),
        &ctx.subscriber_id,
        &profile,
    )
    .await?
    {
        Ok(v) => v,
        Err(()) => {
            terminal(
                &tx,
                &job,
                "failed",
                Some("actor identity changed or missing"),
            )
            .await?;
            tx.commit().await?;
            return Ok(true);
        }
    };
    let mut contexts = serde_json::Map::new();
    if !ctx.context_keys.is_empty() {
        for r in tx.query("SELECT context_key,data FROM novu_rust_contexts WHERE environment_id=$1 AND context_key=ANY($2)",&[&environment,&ctx.context_keys]).await? {let key:String=r.get(0);let data:Value=r.get(1);if let Some((kind,id))=key.split_once(':'){contexts.insert(kind.to_owned(),json!({"id":id,"data":data}));}}
    }
    let text = match novu_rust_contracts::template::render_in_app_with_actor(
        &ctx.content_template,
        &ctx.payload,
        &profile,
        &Value::Object(contexts),
        actor.as_ref(),
    ) {
        Ok(v) => v,
        Err(_) => {
            terminal(
                &tx,
                &job,
                "failed",
                Some("unsupported Slack template or input"),
            )
            .await?;
            tx.commit().await?;
            return Ok(true);
        }
    };
    let message = Message {
        channel: delivery.target,
        text,
        blocks: None,
        custom_data: None,
    };
    if crate::slack::request_body(&message).is_err() {
        terminal(&tx, &job, "failed", Some("invalid Slack request")).await?;
        tx.commit().await?;
        return Ok(true);
    }
    let inserted=tx.execute("INSERT INTO novu_rust_slack_attempts(job_id,environment_id,state) VALUES ($1,$2,'dispatching') ON CONFLICT DO NOTHING",&[&job.id,&environment]).await?;
    if inserted != 1 {
        terminal(&tx, &job, "failed", Some("Slack attempt requires review")).await?;
        tx.commit().await?;
        return Ok(true);
    }
    tx.commit().await?;
    let (state, code, id, diagnostic) = match provider.send(&message).await {
        Ok(id) => ("accepted", None, Some(id), None),
        Err(SendError::Rejected(code)) => (
            "rejected",
            code.map(i32::from),
            None,
            Some("Slack rejected request"),
        ),
        Err(SendError::Invalid) => ("rejected", None, None, Some("invalid Slack request")),
        Err(SendError::Unknown) => ("unknown", None, None, Some("Slack outcome unknown")),
    };
    let tx = db.transaction().await?;
    if !live(&tx, &job).await? {
        tx.rollback().await?;
        return Ok(true);
    }
    let updated=tx.execute("UPDATE novu_rust_slack_attempts SET state=$3,http_status=$4,provider_message_id=$5,updated_at=clock_timestamp() WHERE job_id=$1 AND environment_id=$2 AND state='dispatching'",&[&job.id,&environment,&state,&code,&id]).await?;
    if updated == 1 {
        terminal(
            &tx,
            &job,
            if state == "accepted" {
                "completed"
            } else {
                "failed"
            },
            diagnostic,
        )
        .await?;
    }
    tx.commit().await?;
    Ok(true)
}
pub async fn recover_expired(db: &mut Client, environment: &str) -> Result<usize, Error> {
    let tx = db.transaction().await?;
    let rows=tx.query("SELECT id FROM novu_queue_jobs WHERE environment_id=$1 AND kind=ANY($2) AND status='running' AND lease_until<=clock_timestamp() ORDER BY lease_until,id FOR UPDATE SKIP LOCKED LIMIT 100",&[&environment,&vec![FANOUT_KIND,DELIVERY_KIND]]).await?;
    for row in &rows {
        let id: Uuid = row.get(0);
        let attempted = tx
            .query_opt(
                "SELECT state FROM novu_rust_slack_attempts WHERE environment_id=$1 AND job_id=$2",
                &[&environment, &id],
            )
            .await?
            .is_some();
        if attempted {
            tx.execute("UPDATE novu_rust_slack_attempts SET state='unknown',updated_at=clock_timestamp() WHERE environment_id=$1 AND job_id=$2 AND state='dispatching'",&[&environment,&id]).await?;
            tx.execute("UPDATE novu_queue_jobs SET status='failed',last_error='Slack outcome unknown',lease_token=NULL,lease_until=NULL WHERE id=$1",&[&id]).await?;
        } else {
            tx.execute("UPDATE novu_queue_jobs SET status='pending',lease_token=NULL,lease_until=NULL WHERE id=$1",&[&id]).await?;
        }
    }
    tx.commit().await?;
    Ok(rows.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };
    use tokio_postgres::NoTls;

    #[tokio::test]
    #[ignore = "requires disposable PostgreSQL and loopback HTTP"]
    async fn resolves_endpoints_and_revalidates_each_delivery() {
        let (mut db, driver) =
            tokio_postgres::connect(&std::env::var("TEST_DATABASE_URL").unwrap(), NoTls)
                .await
                .unwrap();
        tokio::spawn(async move {
            let _ = driver.await;
        });
        db.batch_execute("CREATE TABLE novu_rust_environments(id text PRIMARY KEY,organization_id text NOT NULL); INSERT INTO novu_rust_environments VALUES ('env','org'),('other','org'); CREATE TABLE novu_rust_workflows(environment_id text,identifier text,inbox_metadata jsonb,PRIMARY KEY(environment_id,identifier)); INSERT INTO novu_rust_workflows VALUES ('env','workflow','{\"id\":\"wid\"}'); CREATE TABLE novu_rust_workflow_preferences(environment_id text,workflow_identifier text,read_only boolean,all_enabled boolean,PRIMARY KEY(environment_id,workflow_identifier)); CREATE TABLE novu_rust_subscriber_preferences(environment_id text,subscriber_id text,channels jsonb,PRIMARY KEY(environment_id,subscriber_id)); CREATE TABLE novu_rust_subscriber_workflow_preferences(environment_id text,subscriber_id text,workflow_identifier text,channels jsonb,PRIMARY KEY(environment_id,subscriber_id,workflow_identifier));").await.unwrap();
        for migration in [
            crate::MIGRATION,
            crate::in_app::MIGRATION,
            crate::slack_queue::MIGRATION,
            include_str!("../migrations/0075_slack_channel_endpoints.sql"),
            include_str!("../migrations/0076_slack_endpoint_integrations.sql"),
            MIGRATION,
            crate::reconciliation::MIGRATION,
            crate::reconciliation::OPERATOR_GRANTS_MIGRATION,
            crate::alerts::MIGRATION,
            crate::alerts::RESOLUTION_MIGRATION,
            crate::reconciliation::OPERATOR_GRANT_LOCK_MIGRATION,
            crate::slack_queue::OPERATOR_MIGRATION,
            include_str!("../migrations/0072_operator_grant_management.sql"),
            include_str!("../migrations/0073_operator_role_identity.sql"),
        ] {
            db.batch_execute(migration).await.unwrap();
        }
        db.batch_execute("INSERT INTO novu_rust_subscribers(environment_id,subscriber_id,internal_id,profile) VALUES ('env','alice','old','{\"firstName\":\"Jane\"}'),('other','alice','other','{}'); INSERT INTO novu_rust_slack_endpoint_integrations VALUES ('env','main'),('other','main'); INSERT INTO novu_rust_slack_channel_endpoints(environment_id,identifier,subscriber_id,subscriber_internal_id,integration_identifier,endpoint_type,endpoint) VALUES ('env','one','alice','old','main','slack_channel','{\"channelId\":\"C123\"}'),('env','two','alice','old','main','slack_user','{\"userId\":\"U456\"}'),('other','one','alice','other','main','slack_channel','{\"channelId\":\"OTHER\"}')").await.unwrap();
        let ctx = Context {
            subscriber_id: "alice".into(),
            subscriber_internal_id: "old".into(),
            workflow_identifier: "workflow".into(),
            workflow_internal_id: Some("wid".into()),
            content_template: "Hello {{subscriber.firstName}}".into(),
            payload: json!({}),
            context_keys: vec![],
            actor: None,
            actor_internal_id: None,
        };
        let parent = Uuid::new_v4();
        crate::enqueue(
            &db,
            parent,
            "env",
            FANOUT_KIND,
            &serde_json::to_value(&ctx).unwrap(),
        )
        .await
        .unwrap();
        assert!(fanout_one(&mut db, "env").await.unwrap());
        assert_eq!(
            db.query_one(
                "SELECT count(*) FROM novu_rust_slack_workflow_fanout WHERE parent_job_id=$1",
                &[&parent]
            )
            .await
            .unwrap()
            .get::<_, i64>(0),
            2
        );
        db.execute("UPDATE novu_rust_slack_channel_endpoints SET endpoint='{\"userId\":\"U999\"}' WHERE environment_id='env' AND identifier='two'",&[]).await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            let mut buf = [0; 4096];
            loop {
                let n = socket.read(&mut buf).await.unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&buf[..n]);
                if let Some(pos) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&bytes[..pos]);
                    let len: usize = headers
                        .lines()
                        .find_map(|v| {
                            v.to_ascii_lowercase()
                                .strip_prefix("content-length: ")
                                .and_then(|x| x.parse().ok())
                        })
                        .unwrap();
                    if bytes.len() >= pos + 4 + len {
                        break;
                    }
                }
            }
            let body = r#"{"ok":true,"channel":"C123","ts":"1.2"}"#;
            socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            String::from_utf8(bytes).unwrap()
        });
        let mut providers = Providers::new();
        providers.insert(
            ("main".into(), String::new()),
            Slack::test_endpoint("fixture-token".into(), &url).unwrap(),
        );
        assert!(delivery_one(&mut db, "env", &providers).await.unwrap());
        assert!(delivery_one(&mut db, "env", &providers).await.unwrap());
        let request = server.await.unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(request.split_once("\r\n\r\n").unwrap().1).unwrap(),
            json!({"channel":"C123","text":"Hello Jane"})
        );
        assert_eq!(
            db.query_one(
                "SELECT count(*) FROM novu_queue_jobs WHERE kind=$1 AND status='completed'",
                &[&DELIVERY_KIND]
            )
            .await
            .unwrap()
            .get::<_, i64>(0),
            1
        );
        assert_eq!(db.query_one("SELECT count(*) FROM novu_queue_jobs WHERE kind=$1 AND last_error='Slack endpoint changed or deleted'",&[&DELIVERY_KIND]).await.unwrap().get::<_,i64>(0),1);
        let parent2 = Uuid::new_v4();
        crate::enqueue(
            &db,
            parent2,
            "env",
            FANOUT_KIND,
            &serde_json::to_value(&ctx).unwrap(),
        )
        .await
        .unwrap();
        assert!(fanout_one(&mut db, "env").await.unwrap());
        db.execute("DELETE FROM novu_rust_subscribers WHERE environment_id='env' AND subscriber_id='alice'",&[]).await.unwrap();
        db.execute("INSERT INTO novu_rust_subscribers(environment_id,subscriber_id,internal_id) VALUES ('env','alice','new')",&[]).await.unwrap();
        assert!(delivery_one(&mut db, "env", &providers).await.unwrap());
        assert!(delivery_one(&mut db, "env", &providers).await.unwrap());
        assert_eq!(db.query_one("SELECT count(*) FROM novu_queue_jobs WHERE kind=$1 AND last_error='subscriber identity changed'",&[&DELIVERY_KIND]).await.unwrap().get::<_,i64>(0),2);
        db.execute("INSERT INTO novu_rust_slack_channel_endpoints(environment_id,identifier,subscriber_id,subscriber_internal_id,integration_identifier,endpoint_type,endpoint) VALUES ('env','new','alice','new','main','slack_user','{\"userId\":\"U123\"}')",&[]).await.unwrap();
        let mut next_ctx = ctx.clone();
        next_ctx.subscriber_internal_id = "new".into();
        let parent3 = Uuid::new_v4();
        crate::enqueue(
            &db,
            parent3,
            "env",
            FANOUT_KIND,
            &serde_json::to_value(&next_ctx).unwrap(),
        )
        .await
        .unwrap();
        assert!(fanout_one(&mut db, "env").await.unwrap());
        let (mut concurrent, driver) =
            tokio_postgres::connect(&std::env::var("TEST_DATABASE_URL").unwrap(), NoTls)
                .await
                .unwrap();
        tokio::spawn(async move {
            let _ = driver.await;
        });
        let pref_tx = db.transaction().await.unwrap();
        pref_tx.query_one("SELECT 1 FROM novu_rust_subscribers WHERE environment_id='env' AND subscriber_id='alice' FOR UPDATE",&[]).await.unwrap();
        pref_tx.execute("INSERT INTO novu_rust_subscriber_preferences(environment_id,subscriber_id,channels) VALUES ('env','alice','{\"chat\":false}')",&[]).await.unwrap();
        let worker =
            tokio::spawn(
                async move { delivery_one(&mut concurrent, "env", &Providers::new()).await },
            );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(!worker.is_finished());
        pref_tx.commit().await.unwrap();
        assert!(worker.await.unwrap().unwrap());
        assert_eq!(db.query_one("SELECT count(*) FROM novu_queue_jobs WHERE kind=$1 AND status='cancelled' AND last_error='chat preference disabled'",&[&DELIVERY_KIND]).await.unwrap().get::<_,i64>(0),1);
        let expired = Uuid::new_v4();
        crate::enqueue(&db, expired, "env", DELIVERY_KIND, &json!({}))
            .await
            .unwrap();
        db.execute("UPDATE novu_queue_jobs SET status='running',lease_token=$2,lease_until=clock_timestamp()-interval '1 second' WHERE id=$1",&[&expired,&Uuid::new_v4()]).await.unwrap();
        db.execute("INSERT INTO novu_rust_slack_attempts(job_id,environment_id,state) VALUES ($1,'env','dispatching')",&[&expired]).await.unwrap();
        assert_eq!(recover_expired(&mut db, "env").await.unwrap(), 1);
        assert_eq!(
            crate::slack_queue::health(&db, "env").await.unwrap()["unknownAttempts"],
            1
        );
        assert_eq!(
            db.query_one(
                "SELECT status FROM novu_queue_jobs WHERE id=$1",
                &[&expired]
            )
            .await
            .unwrap()
            .get::<_, String>(0),
            "failed"
        );
        db.execute("DELETE FROM novu_rust_subscriber_preferences WHERE environment_id='env' AND subscriber_id='alice'",&[]).await.unwrap();
        let parent4 = Uuid::new_v4();
        crate::enqueue(
            &db,
            parent4,
            "env",
            FANOUT_KIND,
            &serde_json::to_value(&next_ctx).unwrap(),
        )
        .await
        .unwrap();
        assert!(fanout_one(&mut db, "env").await.unwrap());
        let child:Uuid=db.query_one("SELECT delivery_job_id FROM novu_rust_slack_workflow_fanout WHERE parent_job_id=$1",&[&parent4]).await.unwrap().get(0);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        let (received_tx, received_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buffer = [0; 4096];
            assert!(socket.read(&mut buffer).await.unwrap() > 0);
            received_tx.send(()).unwrap();
            release_rx.await.unwrap();
            let body = r#"{"ok":true,"channel":"U123","ts":"1.2"}"#;
            socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
        });
        let (mut worker_db, driver) =
            tokio_postgres::connect(&std::env::var("TEST_DATABASE_URL").unwrap(), NoTls)
                .await
                .unwrap();
        tokio::spawn(async move {
            let _ = driver.await;
        });
        let mut late_providers = Providers::new();
        late_providers.insert(
            ("main".into(), String::new()),
            Slack::test_endpoint("fixture-token".into(), &url).unwrap(),
        );
        let worker =
            tokio::spawn(async move { delivery_one(&mut worker_db, "env", &late_providers).await });
        received_rx.await.unwrap();
        db.execute("UPDATE novu_queue_jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1",&[&child]).await.unwrap();
        assert_eq!(recover_expired(&mut db, "env").await.unwrap(), 1);
        release_tx.send(()).unwrap();
        server.await.unwrap();
        assert!(worker.await.unwrap().unwrap());
        let row=db.query_one("SELECT j.status,a.state FROM novu_queue_jobs j JOIN novu_rust_slack_attempts a ON a.job_id=j.id WHERE j.id=$1",&[&child]).await.unwrap();
        assert_eq!(row.get::<_, String>(0), "failed");
        assert_eq!(row.get::<_, String>(1), "unknown");
        assert!(
            crate::slack_queue::resolve(&mut db, "env", child, "retry", "operator", "checked")
                .await
                .is_err()
        );
        db.execute("INSERT INTO novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'env',session_user,'slack_retry',oid FROM pg_roles WHERE rolname=session_user",&[]).await.unwrap();
        crate::slack_queue::resolve(&mut db, "env", child, "retry", "operator", "checked")
            .await
            .unwrap();
        assert_eq!(
            db.query_one("SELECT status FROM novu_queue_jobs WHERE id=$1", &[&child])
                .await
                .unwrap()
                .get::<_, String>(0),
            "pending"
        );
    }
    #[tokio::test]
    #[ignore = "requires disposable PostgreSQL and loopback HTTP"]
    async fn trigger_fanout_and_delivery_render_subscriber() {
        let (mut db, driver) =
            tokio_postgres::connect(&std::env::var("TEST_DATABASE_URL").unwrap(), NoTls)
                .await
                .unwrap();
        tokio::spawn(async move {
            let _ = driver.await;
        });
        db.batch_execute("CREATE TABLE novu_rust_workflows (environment_id text,identifier text,in_app_content text,has_steps boolean NOT NULL DEFAULT true,PRIMARY KEY(environment_id,identifier));").await.unwrap();
        for migration in [
            crate::MIGRATION,
            crate::in_app::MIGRATION,
            crate::in_app::STATE_MIGRATION,
            crate::in_app::STATE_DATES_MIGRATION,
            crate::in_app::IDENTIFIER_MIGRATION,
            crate::in_app::INBOX_METADATA_MIGRATION,
            crate::in_app::INBOX_DATE_PRESENCE_MIGRATION,
            crate::in_app::INBOX_DATA_MIGRATION,
            crate::in_app::INBOX_DATA_ARRAY_MIGRATION,
            crate::in_app::CONTEXT_MIGRATION,
            crate::in_app::CONTEXT_DATA_MIGRATION,
            crate::in_app::CONTEXT_TIMESTAMPS_MIGRATION,
            crate::in_app::CONTEXT_CURSOR_MIGRATION,
            crate::in_app::SUBSCRIBER_OBJECTID_MIGRATION,
            crate::in_app::GLOBAL_PREFERENCES_MIGRATION,
            crate::in_app::WORKFLOW_PREFERENCES_MIGRATION,
            crate::in_app::SUBSCRIBER_WORKFLOW_PREFERENCES_MIGRATION,
            crate::in_app::STEP_OUTCOMES_MIGRATION,
            crate::in_app::SUBSCRIBER_ONLINE_MIGRATION,
            crate::in_app::TOPICS_MIGRATION,
            crate::in_app::TOPIC_DELETE_RESTRICT_MIGRATION,
            crate::in_app::TOPIC_DATA_MIGRATION,
            crate::in_app::TOPIC_SUBSCRIPTIONS_MIGRATION,
            crate::in_app::TOPIC_SUBSCRIPTIONS_CURSOR_MIGRATION,
            crate::in_app::TOPIC_SUBSCRIPTION_NAME_MIGRATION,
            crate::in_app::TOPIC_SUBSCRIPTION_PREFERENCES_MIGRATION,
            crate::in_app::SUBSCRIBER_SUBSCRIPTION_CURSOR_MIGRATION,
            crate::in_app::SNOOZED_UNTIL_MIGRATION,
            crate::in_app::DELIVERY_HISTORY_MIGRATION,
            crate::in_app::WS_EVENTS_MIGRATION,
            crate::in_app::WS_BEST_EFFORT_MIGRATION,
            crate::in_app::MESSAGE_SEVERITY_MIGRATION,
            crate::in_app::IN_APP_STEP_ACTIVE_MIGRATION,
            crate::in_app::NOTIFICATIONS_MIGRATION,
            crate::in_app::EMAIL_STEP_MIGRATION,
            crate::in_app::EMAIL_MESSAGES_MIGRATION,
            crate::in_app::EMAIL_PROVIDER_RESULT_MIGRATION,
            crate::in_app::EMAIL_STEP_ID_MIGRATION,
            crate::in_app::EMAIL_REVALIDATION_MIGRATION,
            crate::in_app::SMS_STEP_MIGRATION,
            crate::in_app::SMS_MESSAGES_MIGRATION,
            crate::in_app::SMS_STEP_ID_MIGRATION,
            crate::in_app::PUSH_STEP_MIGRATION,
            crate::in_app::FCM_CREDENTIALS_MIGRATION,
            crate::in_app::PUSH_MESSAGES_MIGRATION,
            crate::in_app::IN_APP_SUBSCRIBER_IDENTITY_MIGRATION,
            crate::in_app::MULTIPLE_FCM_CHANNELS_MIGRATION,
            crate::in_app::PUSH_STEP_ID_MIGRATION,
            crate::in_app::WORKFLOW_EMAIL_PROVIDER_MIGRATION,
            crate::sendgrid_queue::MIGRATION,
            crate::sendgrid_queue::DISPATCH_ID_MIGRATION,
            crate::sendgrid_queue::DISPATCH_PROOF_MIGRATION,
            crate::twilio_queue::MIGRATION,
            crate::twilio_queue::DISPATCH_ID_MIGRATION,
            crate::fcm_queue::MIGRATION,
            crate::in_app::DELIVERY_STATE_MIGRATION,
        ] {
            db.batch_execute(migration).await.unwrap();
        }

        db.batch_execute("CREATE TABLE novu_rust_environments(id text PRIMARY KEY, organization_id text NOT NULL); INSERT INTO novu_rust_environments VALUES ('env','org')").await.unwrap();
        for migration in [
            crate::slack_queue::MIGRATION,
            include_str!("../migrations/0075_slack_channel_endpoints.sql"),
            include_str!("../migrations/0076_slack_endpoint_integrations.sql"),
            MIGRATION,
        ] {
            db.batch_execute(migration).await.unwrap();
        }
        db.batch_execute("INSERT INTO novu_rust_subscribers(environment_id,subscriber_id,internal_id,profile) VALUES ('env','alice','alice-internal','{\"firstName\":\"Jane\"}'); INSERT INTO novu_rust_workflows(environment_id,identifier,chat_content,inbox_metadata) VALUES ('env','workflow','Hello {{subscriber.firstName}}','{\"id\":\"wid\"}'); INSERT INTO novu_rust_slack_endpoint_integrations VALUES ('env','main'); INSERT INTO novu_rust_slack_channel_endpoints(environment_id,identifier,subscriber_id,subscriber_internal_id,integration_identifier,endpoint_type,endpoint) VALUES ('env','one','alice','alice-internal','main','slack_channel','{\"channelId\":\"C123\"}')").await.unwrap();
        let trigger = Uuid::new_v4();
        crate::enqueue(&db,trigger,"env",crate::in_app::JOB_KIND,&json!({"workflow":"workflow","transactionId":"t1","content":"","severityOverride":null,"payload":{},"recipients":[{"subscriberId":"alice"}],"recipientIdentitySnapshot":{"alice":"alice-internal"}})).await.unwrap();
        assert!(crate::in_app::process_one(&mut db).await.unwrap());
        let row=db.query_one("SELECT status,channels FROM novu_queue_jobs j JOIN novu_rust_notifications n ON n.job_id=j.id WHERE j.id=$1",&[&trigger]).await.unwrap();
        assert_eq!(row.get::<_, String>(0), "completed");
        assert_eq!(row.get::<_, Vec<String>>(1), vec!["chat"]);
        assert!(fanout_one(&mut db, "env").await.unwrap());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            let mut buf = [0; 4096];
            loop {
                let n = socket.read(&mut buf).await.unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&buf[..n]);
                if let Some(pos) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
                    let h = String::from_utf8_lossy(&bytes[..pos]);
                    let len: usize = h
                        .lines()
                        .find_map(|v| {
                            v.to_ascii_lowercase()
                                .strip_prefix("content-length: ")
                                .and_then(|x| x.parse().ok())
                        })
                        .unwrap();
                    if bytes.len() >= pos + 4 + len {
                        break;
                    }
                }
            }
            let body = r#"{"ok":true,"channel":"C123","ts":"1.2"}"#;
            socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            String::from_utf8(bytes).unwrap()
        });
        let mut providers = Providers::new();
        providers.insert(
            ("main".into(), String::new()),
            Slack::test_endpoint("fixture-token".into(), &url).unwrap(),
        );
        assert!(delivery_one(&mut db, "env", &providers).await.unwrap());
        let request = server.await.unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(request.split_once("\r\n\r\n").unwrap().1).unwrap(),
            json!({"channel":"C123","text":"Hello Jane"})
        );
        let row=db.query_one("SELECT j.status,a.state FROM novu_queue_jobs j JOIN novu_rust_slack_attempts a ON a.job_id=j.id WHERE j.kind=$1",&[&DELIVERY_KIND]).await.unwrap();
        assert_eq!(row.get::<_, String>(0), "completed");
        assert_eq!(row.get::<_, String>(1), "accepted");
    }
}
