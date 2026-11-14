//! Experimental Socket.IO connection and PostgreSQL event fan-out service.
use axum::{Router, http::StatusCode, routing::get};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use hmac::{Hmac, KeyInit, Mac};
use serde_json::{Value, json};
use sha2::Sha256;
use socketioxide::{
    SocketIo,
    extract::{SocketRef, TryData},
};
use std::{
    error::Error,
    future::{IntoFuture, poll_fn},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::{Mutex, MutexGuard, mpsc};
use tokio_postgres::{AsyncMessage, Client};
use uuid::Uuid;

struct Database {
    config: tokio_postgres::Config,
    tls: novu_rust_contracts::postgres::MakeRustlsConnect,
    client: Mutex<Client>,
}

impl Database {
    async fn connect(
        config: &tokio_postgres::Config,
        tls: &novu_rust_contracts::postgres::MakeRustlsConnect,
    ) -> Result<Client, tokio_postgres::Error> {
        let (client, connection) = config.connect(tls.clone()).await?;
        tokio::spawn(async move {
            if let Err(error) = connection.await {
                eprintln!("database connection failed: {error}");
            }
        });
        Ok(client)
    }

    async fn lock(&self) -> Result<MutexGuard<'_, Client>, tokio_postgres::Error> {
        let mut client = self.client.lock().await;
        if client.is_closed() {
            *client = Self::connect(&self.config, &self.tls).await?;
        }
        Ok(client)
    }
}

#[derive(Clone)]
struct Claims {
    id: String,
    environment_id: String,
    context_keys: Vec<String>,
    expires_at: Option<f64>,
}

fn compatibility_mode() -> bool {
    std::env::var("NOVU_ALLOW_RECREATED_SUBSCRIBER").as_deref() == Ok("true")
}

fn token_expired(claims: &Claims) -> bool {
    claims.expires_at.is_some_and(|expiry| {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(true, |now| now.as_secs() as f64 >= expiry)
    })
}

fn disconnect_expired_sockets(io: &SocketIo) {
    let Some(namespace) = io.of("/") else {
        return;
    };
    for socket in namespace.sockets() {
        if socket
            .extensions
            .get::<Claims>()
            .is_some_and(|claims| token_expired(&claims))
        {
            socket.disconnect().ok();
        }
    }
}

#[derive(Clone, Copy)]
struct ConnectionId(Uuid);

enum ListenerEvent {
    Delivery(Uuid),
    SubscriberDeleted {
        environment: String,
        subscriber: String,
    },
}

fn disconnect_all(io: &SocketIo) {
    if let Some(namespace) = io.of("/") {
        for socket in namespace.sockets() {
            socket.disconnect().ok();
        }
    }
}

fn disconnect_identity(io: &SocketIo, environment: &str, subscriber: &str) {
    if io.of("/").is_none() {
        return;
    }
    for socket in io.within(subscriber.to_owned()).sockets() {
        if socket
            .extensions
            .get::<Claims>()
            .is_some_and(|c| c.environment_id == environment && c.id == subscriber)
        {
            socket.disconnect().ok();
        }
    }
}

// The notification is ephemeral. Periodically check connected identities to
// recover from a missed event and from deletion racing socket registration.
async fn reconcile_socket_identities(
    db: &Database,
    io: &SocketIo,
) -> Result<(), tokio_postgres::Error> {
    let Some(namespace) = io.of("/") else {
        return Ok(());
    };
    let sockets: Vec<_> = namespace
        .sockets()
        .into_iter()
        .filter_map(|socket| {
            socket
                .extensions
                .get::<Claims>()
                .map(|claims| (socket, claims))
        })
        .collect();
    if sockets.is_empty() {
        return Ok(());
    }
    let environments: Vec<_> = sockets
        .iter()
        .map(|(_, c)| c.environment_id.clone())
        .collect();
    let subscribers: Vec<_> = sockets.iter().map(|(_, c)| c.id.clone()).collect();
    let rows = db.lock().await?.query("SELECT DISTINCT x.environment_id,x.internal_id FROM unnest($1::text[],$2::text[]) x(environment_id,internal_id) JOIN novu_rust_subscribers s ON s.environment_id=x.environment_id AND s.internal_id=x.internal_id", &[&environments,&subscribers]).await?;
    let existing: std::collections::HashSet<(String, String)> =
        rows.into_iter().map(|r| (r.get(0), r.get(1))).collect();
    for (socket, claims) in sockets {
        if !existing.contains(&(claims.environment_id, claims.id)) {
            socket.disconnect().ok();
        }
    }
    Ok(())
}

// Only renew leases which have not already expired. An instance that pauses
// beyond the lease must close its sockets rather than resurrect stale rows.
async fn maintain_connection_leases(
    db: &Database,
    io: &SocketIo,
) -> Result<(), tokio_postgres::Error> {
    if let Some(namespace) = io.of("/") {
        let sockets: Vec<_> = namespace
            .sockets()
            .into_iter()
            .filter_map(|socket| {
                socket
                    .extensions
                    .get::<ConnectionId>()
                    .map(|id| (socket, id.0))
            })
            .collect();
        if !sockets.is_empty() {
            let ids: Vec<Uuid> = sockets.iter().map(|(_, id)| *id).collect();
            let rows = db.lock().await?.query("UPDATE novu_rust_ws_connections SET lease_expires_at=clock_timestamp()+interval '30 seconds' WHERE id=ANY($1::uuid[]) AND lease_expires_at>clock_timestamp() RETURNING id", &[&ids]).await?;
            let renewed: std::collections::HashSet<Uuid> =
                rows.into_iter().map(|row| row.get(0)).collect();
            for (socket, id) in sockets {
                if !renewed.contains(&id) {
                    socket.disconnect().ok();
                }
            }
        }
    }
    // Acquire the same subscriber lock as connect/disconnect before deleting
    // expired leases and recalculating presence. Exclude all expired leases
    // from the count, including rows deleted by this same statement's CTE.
    for _ in 0..32 {
        let row=db.lock().await?.query_one("WITH candidate AS MATERIALIZED (SELECT s.environment_id,s.internal_id FROM novu_rust_subscribers s WHERE EXISTS(SELECT 1 FROM novu_rust_ws_connections c WHERE c.environment_id=s.environment_id AND c.subscriber_internal_id=s.internal_id AND c.lease_expires_at<=clock_timestamp()) ORDER BY s.environment_id,s.internal_id FOR UPDATE OF s SKIP LOCKED LIMIT 1), removed AS (DELETE FROM novu_rust_ws_connections c USING candidate x WHERE c.environment_id=x.environment_id AND c.subscriber_internal_id=x.internal_id AND c.lease_expires_at<=clock_timestamp() RETURNING c.id), changed AS (UPDATE novu_rust_subscribers s SET is_online=EXISTS(SELECT 1 FROM novu_rust_ws_connections c WHERE c.environment_id=s.environment_id AND c.subscriber_internal_id=s.internal_id AND (c.lease_expires_at IS NULL OR c.lease_expires_at>clock_timestamp())),last_online_at=now(),updated_at=now() FROM candidate x WHERE s.environment_id=x.environment_id AND s.internal_id=x.internal_id AND EXISTS(SELECT 1 FROM removed) RETURNING s.internal_id) SELECT EXISTS(SELECT 1 FROM removed)", &[]).await?;
        if !row.get::<_, bool>(0) {
            break;
        }
    }
    // Deleted subscriber identities do not have a row to lock; connection IDs
    // and internal IDs are immutable, so stale orphan leases can be removed.
    db.lock().await?.execute("WITH orphan AS MATERIALIZED (SELECT c.id FROM novu_rust_ws_connections c WHERE c.lease_expires_at<=clock_timestamp() AND NOT EXISTS(SELECT 1 FROM novu_rust_subscribers s WHERE s.environment_id=c.environment_id AND s.internal_id=c.subscriber_internal_id) ORDER BY c.lease_expires_at,c.id FOR UPDATE OF c SKIP LOCKED LIMIT 100) DELETE FROM novu_rust_ws_connections c USING orphan o WHERE c.id=o.id", &[]).await?;
    Ok(())
}

fn javascript_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(value) => value.as_f64().is_some_and(|value| value != 0.0),
        Value::String(value) => !value.is_empty(),
        Value::Array(_) | Value::Object(_) => true,
    }
}

fn context_matches(message: &[String], inbox: &[String]) -> bool {
    message.len() == inbox.len() && message.iter().all(|key| inbox.contains(key))
}

async fn deliver_event(
    db: &Database,
    io: &SocketIo,
    id: Uuid,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let row = db.lock().await?.query_opt(
        "SELECT e.environment_id,e.subscriber_internal_id,e.context_keys,e.event,e.data,EXISTS(SELECT 1 FROM novu_rust_subscribers s WHERE s.environment_id=e.environment_id AND s.internal_id=e.subscriber_internal_id) FROM novu_rust_ws_events e WHERE e.id=$1", &[&id]
    ).await?;
    let row = row.ok_or("socket event record missing")?;
    let environment: String = row.get(0);
    let subscriber: String = row.get(1);
    let context: Vec<String> = row.get(2);
    let event: String = row.get(3);
    let data: Value = row.get(4);
    if io.of("/").is_none() {
        return Ok(());
    }
    if !compatibility_mode() && !row.get::<_, bool>(5) {
        disconnect_identity(io, &environment, &subscriber);
        return Ok(());
    }
    let sockets: Vec<_> = io
        .within(subscriber.clone())
        .sockets()
        .into_iter()
        .filter(|socket| {
            socket.extensions.get::<Claims>().is_some_and(|claims| {
                if !compatibility_mode() && token_expired(&claims) {
                    socket.clone().disconnect().ok();
                    return false;
                }
                claims.environment_id == environment
                    && context_matches(&context, &claims.context_keys)
            })
        })
        .collect();
    if sockets.is_empty() {
        return Ok(());
    }
    if (event == "unread_count_changed" || event == "unseen_count_changed")
        && data.get("recalculate") == Some(&Value::Bool(true))
    {
        let db = db.lock().await?;
        let (unseen, unread) = resolve_counts(&db, &environment, &subscriber, &context).await?;
        let payload = if event == "unseen_count_changed" {
            unseen
        } else {
            unread
        };
        for socket in sockets {
            let _ = socket.emit(&event, &payload);
        }
        return Ok(());
    }
    if event == "notification_received"
        && let Some(message_id) = data.get("messageId").and_then(Value::as_str)
    {
        let Some((message, unseen, unread)) =
            resolve_message_event(db, &environment, &subscriber, &context, message_id).await?
        else {
            return Ok(());
        };
        for socket in sockets {
            let _ = socket.emit("notification_received", &json!({"message":message}));
            let _ = socket.emit("unseen_count_changed", &unseen);
            let _ = socket.emit("unread_count_changed", &unread);
        }
        return Ok(());
    }
    for socket in sockets {
        let _ = socket.emit(&event, &data);
    }
    Ok(())
}

async fn resolve_message_event(
    db: &Database,
    environment: &str,
    subscriber: &str,
    context: &[String],
    message_id: &str,
) -> Result<Option<(Value, Value, Value)>, tokio_postgres::Error> {
    let db = db.lock().await?;
    let row = db.query_opt("SELECT m.internal_id,m.transaction_id,m.content,m.payload,m.is_read,m.is_seen,m.is_archived,to_char(m.created_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'),to_char(m.delivered_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'),m.tags,m.data,m.workflow_identifier,CASE WHEN m.delivered_history IS NULL THEN NULL ELSE to_jsonb(ARRAY(SELECT to_char(t AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') FROM unnest(m.delivered_history) WITH ORDINALITY h(t,ord) ORDER BY ord)) END AS history,to_char(m.snoozed_until AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') AS snoozed,to_char(m.last_read_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') AS last_read,m.read_at_present,to_char(m.first_seen_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') AS first_seen,to_char(m.archived_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') AS archived_at,m.archived_at_present,e.organization_id,w.inbox_metadata->>'id' AS template_id,m.severity AS message_severity,m.severity_present,m.notification_internal_id FROM novu_rust_in_app_messages m JOIN novu_rust_subscribers s ON s.environment_id=m.environment_id AND s.subscriber_id=m.subscriber_id AND s.internal_id=m.subscriber_internal_id LEFT JOIN novu_rust_environments e ON e.id=m.environment_id LEFT JOIN novu_rust_workflows w ON w.environment_id=m.environment_id AND w.identifier=m.workflow_identifier WHERE m.environment_id=$1 AND s.internal_id=$2 AND m.internal_id=$3", &[&environment,&subscriber,&message_id]).await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let created: String = row.get(7);
    let mut message = json!({"_id":row.get::<_,String>(0),"_environmentId":environment,"_subscriberId":subscriber,"transactionId":row.get::<_,String>(1),"content":row.get::<_,String>(2),"read":row.get::<_,bool>(4),"seen":row.get::<_,bool>(5),"archived":row.get::<_,bool>(6),"createdAt":created,"channel":"in_app","tags":row.get::<_,Value>(9),"templateIdentifier":row.get::<_,String>(11),"status":"sent","cta":{"action":{"buttons":[]}},"contextKeys":context,"deviceTokens":[],"__v":0,"providerId":"novu"});
    if let Some(notification_id) = row.get::<_, Option<String>>("notification_internal_id") {
        message["_notificationId"] = json!(notification_id);
    }
    if row.get::<_, bool>("severity_present") {
        message["severity"] = json!(row.get::<_, Option<String>>("message_severity"));
    }
    if let Some(organization_id) = row.get::<_, Option<String>>("organization_id") {
        message["_organizationId"] = json!(organization_id);
    }
    if let Some(template_id) = row.get::<_, Option<String>>("template_id") {
        message["_templateId"] = json!(template_id);
    }
    // Normalize previously stored prototype messages as well as new ones.
    let payload = novu_rust_contracts::message::in_app_payload(&row.get::<_, Value>(3));
    if payload.as_object().is_some_and(|object| !object.is_empty()) {
        message["payload"] = payload;
    }
    if let Some(history) = row.get::<_, Option<Value>>("history") {
        message["deliveredAt"] = history;
    } else if let Some(delivered) = row.get::<_, Option<String>>(8) {
        message["deliveredAt"] = json!([delivered]);
    }
    if let Some(value) = row.get::<_, Option<String>>("snoozed") {
        message["snoozedUntil"] = json!(value);
    }
    if row.get::<_, bool>("read_at_present") {
        message["lastReadDate"] = json!(row.get::<_, Option<String>>("last_read"));
    }
    if let Some(value) = row.get::<_, Option<String>>("first_seen") {
        message["firstSeenDate"] = json!(value);
    }
    if row.get::<_, bool>("archived_at_present") {
        message["archivedAt"] = json!(row.get::<_, Option<String>>("archived_at"));
    }
    if let Some(data) = row.get::<_, Option<Value>>(10) {
        message["data"] = data;
    }
    let (unseen, unread) = resolve_counts(&db, environment, subscriber, context).await?;
    Ok(Some((message, unseen, unread)))
}

async fn resolve_counts(
    db: &Client,
    environment: &str,
    subscriber: &str,
    context: &[String],
) -> Result<(Value, Value), tokio_postgres::Error> {
    let counts = db.query_one("WITH scoped AS NOT MATERIALIZED (SELECT m.is_read,m.is_seen,m.snoozed_until,m.severity,m.severity_present FROM novu_rust_in_app_messages m JOIN novu_rust_subscribers s ON s.environment_id=m.environment_id AND s.subscriber_id=m.subscriber_id AND s.internal_id=m.subscriber_internal_id WHERE m.environment_id=$1 AND s.internal_id=$2 AND cardinality(m.context_keys)=cardinality($3::text[]) AND m.context_keys @> $3) SELECT (SELECT count(*) FROM (SELECT 1 FROM scoped WHERE NOT is_seen LIMIT 101) q),(SELECT count(*) FROM (SELECT 1 FROM scoped WHERE NOT is_read LIMIT 101) q),(SELECT count(*) FROM (SELECT 1 FROM scoped WHERE NOT is_read AND snoozed_until IS NULL AND severity='high' LIMIT 99) q),(SELECT count(*) FROM (SELECT 1 FROM scoped WHERE NOT is_read AND snoozed_until IS NULL AND severity='medium' LIMIT 99) q),(SELECT count(*) FROM (SELECT 1 FROM scoped WHERE NOT is_read AND snoozed_until IS NULL AND severity='low' LIMIT 99) q),(SELECT count(*) FROM (SELECT 1 FROM scoped WHERE NOT is_read AND snoozed_until IS NULL AND (severity='none' OR NOT severity_present) LIMIT 99) q)", &[&environment,&subscriber,&context]).await?;
    let unseen: i64 = counts.get(0);
    let unread: i64 = counts.get(1);
    let high: i64 = counts.get(2);
    let medium: i64 = counts.get(3);
    let low: i64 = counts.get(4);
    let none: i64 = counts.get(5);
    Ok((
        json!({"unseenCount":unseen.min(100),"hasMore":unseen>100}),
        json!({"unreadCount":unread.min(100),"hasMore":unread>100,"counts":{"total":unread,"severity":{"high":high,"medium":medium,"low":low,"none":none}}}),
    ))
}

fn verify(token: &str, secret: &[u8], require_exp: bool) -> Option<Claims> {
    if secret.is_empty() || token.len() > 16_384 {
        return None;
    }
    let (signed, signature) = token.rsplit_once('.')?;
    let (header, payload) = signed.split_once('.')?;
    let header: Value = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(header).ok()?).ok()?;
    if header.get("alg")?.as_str()? != "HS256" {
        return None;
    }
    let mut mac = Hmac::<Sha256>::new_from_slice(secret).ok()?;
    mac.update(signed.as_bytes());
    mac.verify_slice(&URL_SAFE_NO_PAD.decode(signature).ok()?)
        .ok()?;
    let payload: Value = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(payload).ok()?).ok()?;
    if payload.get("aud")?.as_str()? != "widget_user" {
        return None;
    }
    if require_exp && payload.get("exp").is_none() {
        return None;
    }
    let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_secs();
    if payload
        .get("exp")
        .is_some_and(|v| v.as_f64().is_none_or(|v| now as f64 >= v))
        || payload
            .get("nbf")
            .is_some_and(|v| v.as_f64().is_none_or(|v| (now as f64) < v))
    {
        return None;
    }
    let context_keys = match payload.get("contextKeys") {
        None | Some(Value::Null) => Vec::new(),
        Some(Value::Array(values)) => values
            .iter()
            .map(|v| v.as_str().map(str::to_owned))
            .collect::<Option<Vec<_>>>()?,
        _ => return None,
    };
    Some(Claims {
        id: payload.get("_id")?.as_str()?.to_owned(),
        environment_id: payload.get("environmentId")?.as_str()?.to_owned(),
        context_keys,
        expires_at: payload.get("exp").and_then(Value::as_f64),
    })
}

async fn connect(db: &Database, id: Uuid, claims: &Claims) -> Result<bool, tokio_postgres::Error> {
    let mut client = db.lock().await?;
    let tx = client.transaction().await?;
    let subscriber = tx.query_opt(
        "SELECT 1 FROM novu_rust_subscribers WHERE environment_id=$1 AND internal_id=$2 FOR UPDATE",
        &[&claims.environment_id, &claims.id],
    )
    .await?;
    if subscriber.is_none() && !compatibility_mode() {
        tx.rollback().await?;
        return Ok(false);
    }
    let leased = !compatibility_mode();
    tx.execute("INSERT INTO novu_rust_ws_connections(id,environment_id,subscriber_internal_id,lease_expires_at) VALUES ($1,$2,$3,CASE WHEN $4 THEN clock_timestamp()+interval '30 seconds' ELSE NULL END)", &[&id,&claims.environment_id,&claims.id,&leased]).await?;
    tx.execute("UPDATE novu_rust_subscribers SET is_online=true,updated_at=now() WHERE environment_id=$1 AND internal_id=$2", &[&claims.environment_id,&claims.id]).await?;
    tx.commit().await?;
    Ok(true)
}

async fn disconnect(
    db: &Database,
    id: Uuid,
    claims: &Claims,
    update_presence: bool,
    active_presence: bool,
) -> Result<(), tokio_postgres::Error> {
    let mut client = db.lock().await?;
    let tx = client.transaction().await?;
    tx.query_opt(
        "SELECT 1 FROM novu_rust_subscribers WHERE environment_id=$1 AND internal_id=$2 FOR UPDATE",
        &[&claims.environment_id, &claims.id],
    )
    .await?;
    let removed = tx.execute("DELETE FROM novu_rust_ws_connections WHERE id=$1 AND environment_id=$2 AND subscriber_internal_id=$3", &[&id,&claims.environment_id,&claims.id]).await?;
    if removed > 0 && update_presence {
        // The reference gateway uses > 1 after disconnect. Operators may opt
        // into counting any remaining live connection instead.
        tx.execute("UPDATE novu_rust_subscribers SET is_online=(SELECT count(*) > CASE WHEN $3 THEN 0 ELSE 1 END FROM novu_rust_ws_connections WHERE environment_id=$1 AND subscriber_internal_id=$2 AND (lease_expires_at IS NULL OR lease_expires_at>clock_timestamp())),last_online_at=now(),updated_at=now() WHERE environment_id=$1 AND internal_id=$2", &[&claims.environment_id,&claims.id,&active_presence]).await?;
    }
    tx.commit().await
}

// The delete is idempotent: retrying an uncertain commit does not apply a
// second presence transition. Capture token validity at disconnect time.
async fn retry_disconnect(
    db: Arc<Database>,
    id: Uuid,
    claims: Claims,
    update_presence: bool,
    active_presence: bool,
) {
    let mut delay = std::time::Duration::from_secs(1);
    loop {
        match disconnect(&db, id, &claims, update_presence, active_presence).await {
            Ok(()) => return,
            Err(error) => eprintln!("socket disconnect update failed; retrying: {error}"),
        }
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(std::time::Duration::from_secs(30));
    }
}

// Track registration and cleanup across shutdown, including uncertain commits.
struct Tracked(Arc<AtomicUsize>);
impl Tracked {
    fn new(count: Arc<AtomicUsize>) -> Self {
        count.fetch_add(1, Ordering::SeqCst);
        Self(count)
    }
}
impl Drop for Tracked {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
fn spawn_cleanup(
    count: Arc<AtomicUsize>,
    db: Arc<Database>,
    id: Uuid,
    claims: Claims,
    update_presence: bool,
    active_presence: bool,
) {
    let guard = Tracked::new(count);
    tokio::spawn(async move {
        let _guard = guard;
        retry_disconnect(db, id, claims, update_presence, active_presence).await;
    });
}

// A dedicated listener connection is driven independently of query traffic.
// LISTEN must be active before the server accepts sockets.
async fn start_event_listener(
    database_url: &str,
    event_tx: mpsc::Sender<ListenerEvent>,
) -> Result<(Client, tokio::task::JoinHandle<()>), Box<dyn Error + Send + Sync>> {
    let (mut config, tls) = novu_rust_contracts::postgres::configure(database_url)?;
    config.application_name("novu-rust-ws-listener");
    let (client, mut connection) = config.connect(tls).await?;
    let driver = tokio::spawn(async move {
        while let Some(message) = poll_fn(|cx| connection.poll_message(cx)).await {
            match message {
                Ok(AsyncMessage::Notification(notification))
                    if notification.channel() == "novu_rust_ws_events" =>
                {
                    if let Ok(id) = Uuid::parse_str(notification.payload())
                        && event_tx.send(ListenerEvent::Delivery(id)).await.is_err()
                    {
                        break;
                    }
                }
                Ok(AsyncMessage::Notification(notification))
                    if notification.channel() == "novu_rust_subscriber_deleted" =>
                {
                    if let Ok(payload) = serde_json::from_str::<Value>(notification.payload())
                        && let (Some(environment), Some(subscriber)) = (
                            payload.get("environment").and_then(Value::as_str),
                            payload.get("subscriber").and_then(Value::as_str),
                        )
                        && event_tx
                            .send(ListenerEvent::SubscriberDeleted {
                                environment: environment.to_owned(),
                                subscriber: subscriber.to_owned(),
                            })
                            .await
                            .is_err()
                    {
                        break;
                    }
                }
                Ok(_) => (),
                Err(error) => {
                    eprintln!("socket event listener failed: {error}");
                    break;
                }
            }
        }
    });
    if let Err(error) = client
        .batch_execute("LISTEN novu_rust_subscriber_deleted")
        .await
    {
        driver.abort();
        return Err(error.into());
    }
    if let Err(error) = client.batch_execute("LISTEN novu_rust_ws_events").await {
        driver.abort();
        return Err(error.into());
    }
    Ok((client, driver))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let drain_seconds: u64 = match std::env::var("SWALLOW_WS_DRAIN_SECONDS") {
        Ok(v) => v.parse().map_err(|_| "invalid WS drain setting")?,
        Err(std::env::VarError::NotPresent) => 0,
        Err(_) => return Err("invalid WS drain setting".into()),
    };
    let grace_seconds: u64 = match std::env::var("SWALLOW_WS_SHUTDOWN_GRACE_SECONDS") {
        Ok(v) => v.parse().map_err(|_| "invalid WS shutdown grace")?,
        Err(std::env::VarError::NotPresent) => 30,
        Err(_) => return Err("invalid WS shutdown grace".into()),
    };
    if drain_seconds > 300 || !(1..=300).contains(&grace_seconds) {
        return Err("invalid WS shutdown timing".into());
    }
    #[cfg(unix)]
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let draining = Arc::new(AtomicBool::new(false));
    let pending = Arc::new(AtomicUsize::new(0));
    let require_jwt_exp = match std::env::var("SWALLOW_INBOX_REQUIRE_JWT_EXP").as_deref() {
        Ok("true") => true,
        Ok("false") | Err(std::env::VarError::NotPresent) => false,
        _ => return Err("SWALLOW_INBOX_REQUIRE_JWT_EXP must be true or false".into()),
    };
    let health_enabled = match std::env::var("NOVU_WS_ENABLE_HEALTH_ENDPOINTS").as_deref() {
        Ok("true") => true,
        Ok("false") | Err(std::env::VarError::NotPresent) => false,
        _ => return Err("NOVU_WS_ENABLE_HEALTH_ENDPOINTS must be true or false".into()),
    };
    let active_presence = match std::env::var("NOVU_WS_PRESENCE_POLICY").as_deref() {
        Ok("active") => true,
        Ok("reference") | Err(std::env::VarError::NotPresent) => false,
        _ => return Err("NOVU_WS_PRESENCE_POLICY must be reference or active".into()),
    };
    let database_url = std::env::var("DATABASE_URL")?;
    let secret = novu_rust_contracts::secret::from_env("JWT_SECRET", "JWT_SECRET_FILE")?
        .ok_or("JWT secret is required")?
        .into_bytes();
    let (mut query_config, tls) = novu_rust_contracts::postgres::configure(&database_url)?;
    query_config.application_name("novu-rust-ws-query");
    let client = Database::connect(&query_config, &tls).await?;
    let db = Arc::new(Database {
        config: query_config,
        tls,
        client: Mutex::new(client),
    });
    let (layer, io) = SocketIo::new_layer();
    let disconnect_on_gap = match std::env::var("NOVU_WS_DISCONNECT_ON_EVENT_GAP").as_deref() {
        Ok("true") => true,
        Ok("false") | Err(std::env::VarError::NotPresent) => false,
        _ => return Err("NOVU_WS_DISCONNECT_ON_EVENT_GAP must be true or false".into()),
    };
    let probe_event_recovery = match std::env::var("NOVU_WS_EVENT_LOOKUP_RECOVERY").as_deref() {
        Ok("immediate") | Err(std::env::VarError::NotPresent) => false,
        Ok("probe") => true,
        _ => return Err("NOVU_WS_EVENT_LOOKUP_RECOVERY must be immediate or probe".into()),
    };
    if probe_event_recovery && !disconnect_on_gap {
        return Err("probe event recovery requires NOVU_WS_DISCONNECT_ON_EVENT_GAP=true".into());
    }
    // Optional timeout covers the whole event lookup and emission path. The
    // guarded probe must succeed before accepting new sockets after a timeout.
    let event_timeout = match std::env::var("NOVU_WS_EVENT_LOOKUP_TIMEOUT_SECONDS") {
        Err(std::env::VarError::NotPresent) => None,
        Ok(value) => {
            let seconds: u64 = value
                .parse()
                .map_err(|_| "NOVU_WS_EVENT_LOOKUP_TIMEOUT_SECONDS must be between 1 and 300")?;
            if !(1..=300).contains(&seconds) {
                return Err(
                    "NOVU_WS_EVENT_LOOKUP_TIMEOUT_SECONDS must be between 1 and 300".into(),
                );
            }
            if !disconnect_on_gap || !probe_event_recovery {
                return Err(
                    "event lookup timeout requires disconnect-on-gap and probe recovery".into(),
                );
            }
            Some(std::time::Duration::from_secs(seconds))
        }
        Err(_) => return Err("NOVU_WS_EVENT_LOOKUP_TIMEOUT_SECONDS must be valid Unicode".into()),
    };
    // Even generations mean LISTEN is active. Every outage changes the generation,
    // including an outage that ends while a socket is being registered.
    let listener_generation = Arc::new(AtomicU64::new(0));
    // Odd means event lookups need a successful probe before accepting sockets.
    // Each failure changes the epoch, preventing an older probe from clearing it.
    let event_generation = Arc::new(AtomicU64::new(0));
    let (event_tx, mut event_rx) = mpsc::channel::<ListenerEvent>(1024);
    let initial_listener = start_event_listener(&database_url, event_tx.clone()).await?;
    let listener_ready = Arc::new(AtomicBool::new(true));
    let listener_ready_task = listener_ready.clone();
    let listener_generation_task = listener_generation.clone();
    let listener_io = io.clone();
    tokio::spawn(async move {
        let mut listener = initial_listener;
        loop {
            let (_client, driver) = listener;
            let _ = driver.await;
            listener_ready_task.store(false, Ordering::SeqCst);
            if disconnect_on_gap {
                listener_generation_task.fetch_add(1, Ordering::SeqCst);
                disconnect_all(&listener_io);
            }
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                match start_event_listener(&database_url, event_tx.clone()).await {
                    Ok(reconnected) => {
                        listener = reconnected;
                        listener_ready_task.store(true, Ordering::SeqCst);
                        if disconnect_on_gap {
                            listener_generation_task.fetch_add(1, Ordering::SeqCst);
                        }
                        break;
                    }
                    Err(error) => eprintln!("socket event listener reconnect failed: {error}"),
                }
            }
        }
    });
    let events_db = db.clone();
    let events_io = io.clone();
    let events_generation = listener_generation.clone();
    let event_lookup_generation = event_generation.clone();
    tokio::spawn(async move {
        while let Some(event) = event_rx.recv().await {
            match event {
                ListenerEvent::Delivery(id) => {
                    let result = if let Some(timeout) = event_timeout {
                        match tokio::time::timeout(
                            timeout,
                            deliver_event(&events_db, &events_io, id),
                        )
                        .await
                        {
                            Ok(result) => result,
                            Err(_) => Err("socket event processing timed out".into()),
                        }
                    } else {
                        deliver_event(&events_db, &events_io, id).await
                    };
                    if let Err(error) = result {
                        eprintln!("socket event delivery failed: {error}");
                        if disconnect_on_gap {
                            // Preserve odd/even listener availability, while invalidating
                            // registrations overlapping a failed event lookup.
                            events_generation.fetch_add(2, Ordering::SeqCst);
                            if probe_event_recovery {
                                let _ = event_lookup_generation.fetch_update(
                                    Ordering::SeqCst,
                                    Ordering::SeqCst,
                                    |value| {
                                        Some(value.wrapping_add(if value.is_multiple_of(2) {
                                            1
                                        } else {
                                            2
                                        }))
                                    },
                                );
                            }
                            disconnect_all(&events_io);
                        }
                    }
                }
                ListenerEvent::SubscriberDeleted {
                    environment,
                    subscriber,
                } => {
                    if !compatibility_mode() {
                        disconnect_identity(&events_io, &environment, &subscriber);
                    }
                }
            }
        }
    });
    if probe_event_recovery {
        let probe_db = db.clone();
        let probe_generation = event_generation.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                let generation = probe_generation.load(Ordering::SeqCst);
                if generation.is_multiple_of(2) {
                    continue;
                }
                let probe = tokio::time::timeout(std::time::Duration::from_secs(2), async {
                    probe_db.lock().await?.query_opt(
                        "SELECT e.environment_id,e.subscriber_internal_id,e.context_keys,e.event,e.data,EXISTS(SELECT 1 FROM novu_rust_subscribers s WHERE s.environment_id=e.environment_id AND s.internal_id=e.subscriber_internal_id) FROM novu_rust_ws_events e WHERE e.id=$1",
                        &[&Uuid::nil()],
                    ).await
                }).await;
                if matches!(probe, Ok(Ok(_))) {
                    let _ = probe_generation.compare_exchange(
                        generation,
                        generation.wrapping_add(1),
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    );
                }
            }
        });
    }
    if !compatibility_mode() {
        let expiry_io = io.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
            loop {
                interval.tick().await;
                disconnect_expired_sockets(&expiry_io);
            }
        });
        let check_db = db.clone();
        let check_io = io.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
            loop {
                interval.tick().await;
                if let Err(error) = maintain_connection_leases(&check_db, &check_io).await {
                    eprintln!("socket lease maintenance failed: {error}");
                }
                if let Err(error) = reconcile_socket_identities(&check_db, &check_io).await {
                    eprintln!("socket identity reconciliation failed: {error}");
                }
            }
        });
    }
    let health_db = db.clone();
    let socket_event_generation = event_generation.clone();
    let socket_draining = draining.clone();
    let socket_pending = pending.clone();
    io.ns(
        "/",
        move |socket: SocketRef, TryData(auth): TryData<Value>| {
            let db = db.clone();
            let secret = secret.clone();
            let listener_generation = listener_generation.clone();
            let event_generation = socket_event_generation.clone();
            let draining = socket_draining.clone();
            let pending = socket_pending.clone();
            let registration = Tracked::new(pending.clone());
            async move {
                let _registration = registration;
                if draining.load(Ordering::SeqCst) {
                    socket.disconnect().ok();
                    return;
                }
                let generation = listener_generation.load(Ordering::SeqCst);
                let event_epoch = event_generation.load(Ordering::SeqCst);
                if disconnect_on_gap
                    && (!generation.is_multiple_of(2) || !event_epoch.is_multiple_of(2))
                {
                    socket.disconnect().ok();
                    return;
                }
                let from_auth = auth
                    .ok()
                    .and_then(|v| v.get("token").cloned())
                    .filter(javascript_truthy);
                let from_query = socket.req_parts().uri.query().and_then(|q| {
                    form_urlencoded::parse(q.as_bytes())
                        .find(|(key, _)| key == "token")
                        .map(|(_, value)| value.into_owned())
                });
                let token = match from_auth {
                    Some(Value::String(value)) => Some(value),
                    Some(_) => None,
                    None => from_query,
                };
                let Some(token) = token else {
                    socket.disconnect().ok();
                    return;
                };
                let Some(claims) = verify(&token, &secret, require_jwt_exp) else {
                    socket.disconnect().ok();
                    return;
                };
                let id = Uuid::new_v4();
                match connect(&db, id, &claims).await {
                    Ok(true) => (),
                    Ok(false) => {
                        socket.disconnect().ok();
                        return;
                    }
                    Err(_) => {
                        // A lost COMMIT acknowledgement may mean the row exists.
                        let update_presence = !compatibility_mode()
                            || verify(&token, &secret, require_jwt_exp).is_some();
                        spawn_cleanup(
                            pending.clone(),
                            db.clone(),
                            id,
                            claims,
                            update_presence,
                            active_presence,
                        );
                        socket.disconnect().ok();
                        return;
                    }
                }
                if draining.load(Ordering::SeqCst) {
                    let update_presence =
                        !compatibility_mode() || verify(&token, &secret, require_jwt_exp).is_some();
                    spawn_cleanup(
                        pending.clone(),
                        db,
                        id,
                        claims,
                        update_presence,
                        active_presence,
                    );
                    socket.disconnect().ok();
                    return;
                }
                socket.extensions.insert(ConnectionId(id));
                socket.extensions.insert(claims.clone());
                socket.join(claims.id.clone());
                let cleanup_db = db.clone();
                let cleanup_claims = claims.clone();
                let cleanup_token = token.clone();
                let cleanup_secret = secret.clone();
                let cleanup_pending = pending.clone();
                socket.on_disconnect(move || {
                    let db = cleanup_db.clone();
                    let claims = cleanup_claims.clone();
                    let token = cleanup_token.clone();
                    let secret = cleanup_secret.clone();
                    let update_presence =
                        !compatibility_mode() || verify(&token, &secret, require_jwt_exp).is_some();
                    spawn_cleanup(
                        cleanup_pending.clone(),
                        db,
                        id,
                        claims,
                        update_presence,
                        active_presence,
                    );
                    async {}
                });
                if disconnect_on_gap
                    && (listener_generation.load(Ordering::SeqCst) != generation
                        || event_generation.load(Ordering::SeqCst) != event_epoch)
                {
                    socket.clone().disconnect().ok();
                }
                if !socket.connected() {
                    let update_presence =
                        !compatibility_mode() || verify(&token, &secret, require_jwt_exp).is_some();
                    spawn_cleanup(pending, db, id, claims, update_presence, active_presence);
                }
            }
        },
    );
    let shutdown_db = health_db.clone();
    let health_draining = draining.clone();
    let mut app = Router::new().layer(layer);
    if health_enabled {
        app = app
            .route("/health/live", get(|| async { StatusCode::NO_CONTENT }))
            .route(
                "/health/ready",
                get(move || {
                    let db = health_db.clone();
                    let listener_ready = listener_ready.clone();
                    let event_generation = event_generation.clone();
                    let draining = health_draining.clone();
                    async move {
                        if draining.load(Ordering::SeqCst)
                            || !listener_ready.load(Ordering::SeqCst)
                            || !event_generation.load(Ordering::SeqCst).is_multiple_of(2)
                        {
                            return StatusCode::SERVICE_UNAVAILABLE;
                        }
                        let probe =
                            tokio::time::timeout(std::time::Duration::from_secs(2), async {
                                db.lock().await?.simple_query("SELECT 1").await
                            })
                            .await;
                        if matches!(probe, Ok(Ok(_)))
                            && !draining.load(Ordering::SeqCst)
                            && listener_ready.load(Ordering::SeqCst)
                            && event_generation.load(Ordering::SeqCst).is_multiple_of(2)
                        {
                            StatusCode::NO_CONTENT
                        } else {
                            StatusCode::SERVICE_UNAVAILABLE
                        }
                    }
                }),
            );
    }
    let address = std::env::var("LISTEN_ADDR").unwrap_or_else(|_| "127.0.0.1:3002".into());
    let listener = tokio::net::TcpListener::bind(address).await?;
    let failed = Arc::new(AtomicBool::new(false));
    let failed_shutdown = failed.clone();
    let (notice_tx, notice_rx) = tokio::sync::oneshot::channel();
    let shutdown = async move {
        #[cfg(unix)]
        tokio::select! { _ = sigterm.recv() => {}, _ = tokio::signal::ctrl_c() => {} }
        #[cfg(not(unix))]
        let _ = tokio::signal::ctrl_c().await;
        draining.store(true, Ordering::SeqCst);
        if drain_seconds > 0 {
            tokio::time::sleep(std::time::Duration::from_secs(drain_seconds)).await;
        }
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(grace_seconds);
        let result = tokio::time::timeout_at(deadline, async {
            // The transport can close before socketioxide invokes an async
            // disconnect callback. Schedule idempotent cleanup explicitly.
            if let Some(namespace) = io.of("/") {
                for socket in namespace.sockets() {
                    if let (Some(ConnectionId(id)), Some(claims)) = (
                        socket.extensions.get::<ConnectionId>(),
                        socket.extensions.get::<Claims>(),
                    ) {
                        let update_presence = !compatibility_mode() || !token_expired(&claims);
                        spawn_cleanup(
                            pending.clone(),
                            shutdown_db.clone(),
                            id,
                            claims,
                            update_presence,
                            active_presence,
                        );
                    }
                }
            }
            io.close().await;
            while pending.load(Ordering::SeqCst) != 0 {
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
        })
        .await;
        if result.is_err() {
            failed_shutdown.store(true, Ordering::SeqCst);
        }
        let _ = notice_tx.send(deadline);
    };
    let server = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .into_future();
    tokio::pin!(server);
    tokio::select! {
        result = &mut server => result?,
        deadline = notice_rx => {
            let deadline = deadline.map_err(|_| "WS shutdown coordination failed")?;
            tokio::time::timeout_at(deadline, &mut server).await.map_err(|_| "WS shutdown timed out")??;
        }
    }
    if failed.load(Ordering::SeqCst) {
        return Err("WS cleanup timed out".into());
    }
    Ok(())
}
