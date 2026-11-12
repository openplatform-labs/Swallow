//! Experimental Socket.IO connection and PostgreSQL event fan-out service.
use axum::Router;
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
    future::poll_fn,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::{Mutex, MutexGuard, mpsc};
use tokio_postgres::{AsyncMessage, Client, NoTls};
use uuid::Uuid;

struct Database {
    config: tokio_postgres::Config,
    client: Mutex<Client>,
}

impl Database {
    async fn connect(config: &tokio_postgres::Config) -> Result<Client, tokio_postgres::Error> {
        let (client, connection) = config.connect(NoTls).await?;
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
            *client = Self::connect(&self.config).await?;
        }
        Ok(client)
    }
}

#[derive(Clone)]
struct Claims {
    id: String,
    environment_id: String,
    context_keys: Vec<String>,
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
) -> Result<(), tokio_postgres::Error> {
    let row = db.lock().await?.query_opt(
        "SELECT environment_id,subscriber_internal_id,context_keys,event,data FROM novu_rust_ws_events WHERE id=$1", &[&id]
    ).await?;
    let Some(row) = row else {
        return Ok(());
    };
    let environment: String = row.get(0);
    let subscriber: String = row.get(1);
    let context: Vec<String> = row.get(2);
    let event: String = row.get(3);
    let data: Value = row.get(4);
    let sockets: Vec<_> = io
        .within(subscriber.clone())
        .sockets()
        .into_iter()
        .filter(|socket| {
            socket.extensions.get::<Claims>().is_some_and(|claims| {
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
    let row = db.query_opt("SELECT m.internal_id,m.transaction_id,m.content,m.payload,m.is_read,m.is_seen,m.is_archived,to_char(m.created_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'),to_char(m.delivered_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'),m.tags,m.data,m.workflow_identifier,CASE WHEN m.delivered_history IS NULL THEN NULL ELSE to_jsonb(ARRAY(SELECT to_char(t AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') FROM unnest(m.delivered_history) WITH ORDINALITY h(t,ord) ORDER BY ord)) END AS history,to_char(m.snoozed_until AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') AS snoozed,to_char(m.last_read_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') AS last_read,m.read_at_present,to_char(m.first_seen_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') AS first_seen,to_char(m.archived_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') AS archived_at,m.archived_at_present,e.organization_id,w.inbox_metadata->>'id' AS template_id,m.severity AS message_severity,m.severity_present,m.notification_internal_id FROM novu_rust_in_app_messages m JOIN novu_rust_subscribers s ON s.environment_id=m.environment_id AND s.subscriber_id=m.subscriber_id LEFT JOIN novu_rust_environments e ON e.id=m.environment_id LEFT JOIN novu_rust_workflows w ON w.environment_id=m.environment_id AND w.identifier=m.workflow_identifier WHERE m.environment_id=$1 AND s.internal_id=$2 AND m.internal_id=$3", &[&environment,&subscriber,&message_id]).await?;
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
    let counts = db.query_one("WITH scoped AS NOT MATERIALIZED (SELECT m.is_read,m.is_seen,m.snoozed_until,m.severity,m.severity_present FROM novu_rust_in_app_messages m JOIN novu_rust_subscribers s ON s.environment_id=m.environment_id AND s.subscriber_id=m.subscriber_id WHERE m.environment_id=$1 AND s.internal_id=$2 AND cardinality(m.context_keys)=cardinality($3::text[]) AND m.context_keys @> $3) SELECT (SELECT count(*) FROM (SELECT 1 FROM scoped WHERE NOT is_seen LIMIT 101) q),(SELECT count(*) FROM (SELECT 1 FROM scoped WHERE NOT is_read LIMIT 101) q),(SELECT count(*) FROM (SELECT 1 FROM scoped WHERE NOT is_read AND snoozed_until IS NULL AND severity='high' LIMIT 99) q),(SELECT count(*) FROM (SELECT 1 FROM scoped WHERE NOT is_read AND snoozed_until IS NULL AND severity='medium' LIMIT 99) q),(SELECT count(*) FROM (SELECT 1 FROM scoped WHERE NOT is_read AND snoozed_until IS NULL AND severity='low' LIMIT 99) q),(SELECT count(*) FROM (SELECT 1 FROM scoped WHERE NOT is_read AND snoozed_until IS NULL AND (severity='none' OR NOT severity_present) LIMIT 99) q)", &[&environment,&subscriber,&context]).await?;
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

fn verify(token: &str, secret: &[u8]) -> Option<Claims> {
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
    let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_secs();
    if payload
        .get("exp")
        .is_some_and(|v| v.as_u64().is_none_or(|v| now >= v))
        || payload
            .get("nbf")
            .is_some_and(|v| v.as_u64().is_none_or(|v| now < v))
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
    })
}

async fn connect(db: &Database, id: Uuid, claims: &Claims) -> Result<(), tokio_postgres::Error> {
    let mut client = db.lock().await?;
    let tx = client.transaction().await?;
    tx.query_opt(
        "SELECT 1 FROM novu_rust_subscribers WHERE environment_id=$1 AND internal_id=$2 FOR UPDATE",
        &[&claims.environment_id, &claims.id],
    )
    .await?;
    tx.execute("INSERT INTO novu_rust_ws_connections(id,environment_id,subscriber_internal_id) VALUES ($1,$2,$3)", &[&id,&claims.environment_id,&claims.id]).await?;
    tx.execute("UPDATE novu_rust_subscribers SET is_online=true,updated_at=now() WHERE environment_id=$1 AND internal_id=$2", &[&claims.environment_id,&claims.id]).await?;
    tx.commit().await
}

async fn disconnect(
    db: &Database,
    id: Uuid,
    claims: &Claims,
    update_presence: bool,
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
        // The original gateway tests whether more than one socket remains.
        tx.execute("UPDATE novu_rust_subscribers SET is_online=(SELECT count(*) > 1 FROM novu_rust_ws_connections WHERE environment_id=$1 AND subscriber_internal_id=$2),last_online_at=now(),updated_at=now() WHERE environment_id=$1 AND internal_id=$2", &[&claims.environment_id,&claims.id]).await?;
    }
    tx.commit().await
}

// The delete is idempotent: retrying an uncertain commit does not apply a
// second presence transition. Capture token validity at disconnect time.
async fn retry_disconnect(db: Arc<Database>, id: Uuid, claims: Claims, update_presence: bool) {
    let mut delay = std::time::Duration::from_secs(1);
    loop {
        match disconnect(&db, id, &claims, update_presence).await {
            Ok(()) => return,
            Err(error) => eprintln!("socket disconnect update failed; retrying: {error}"),
        }
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(std::time::Duration::from_secs(30));
    }
}

// A dedicated listener connection is driven independently of query traffic.
// LISTEN must be active before the server accepts sockets.
async fn start_event_listener(
    database_url: &str,
    event_tx: mpsc::Sender<Uuid>,
) -> Result<(Client, tokio::task::JoinHandle<()>), tokio_postgres::Error> {
    let mut config: tokio_postgres::Config = database_url.parse()?;
    config.application_name("novu-rust-ws-listener");
    let (client, mut connection) = config.connect(NoTls).await?;
    let driver = tokio::spawn(async move {
        while let Some(message) = poll_fn(|cx| connection.poll_message(cx)).await {
            match message {
                Ok(AsyncMessage::Notification(notification))
                    if notification.channel() == "novu_rust_ws_events" =>
                {
                    if let Ok(id) = Uuid::parse_str(notification.payload())
                        && event_tx.send(id).await.is_err()
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
    if let Err(error) = client.batch_execute("LISTEN novu_rust_ws_events").await {
        driver.abort();
        return Err(error);
    }
    Ok((client, driver))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let database_url = std::env::var("DATABASE_URL")?;
    let secret = std::env::var("JWT_SECRET")?.into_bytes();
    if secret.is_empty() {
        return Err("JWT_SECRET must not be empty".into());
    }
    let mut query_config: tokio_postgres::Config = database_url.parse()?;
    query_config.application_name("novu-rust-ws-query");
    let client = Database::connect(&query_config).await?;
    let db = Arc::new(Database {
        config: query_config,
        client: Mutex::new(client),
    });
    let (layer, io) = SocketIo::new_layer();
    let (event_tx, mut event_rx) = mpsc::channel::<Uuid>(1024);
    let initial_listener = start_event_listener(&database_url, event_tx.clone()).await?;
    tokio::spawn(async move {
        let mut listener = initial_listener;
        loop {
            let (_client, driver) = listener;
            let _ = driver.await;
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                match start_event_listener(&database_url, event_tx.clone()).await {
                    Ok(reconnected) => {
                        listener = reconnected;
                        break;
                    }
                    Err(error) => eprintln!("socket event listener reconnect failed: {error}"),
                }
            }
        }
    });
    let events_db = db.clone();
    let events_io = io.clone();
    tokio::spawn(async move {
        while let Some(id) = event_rx.recv().await {
            if let Err(error) = deliver_event(&events_db, &events_io, id).await {
                eprintln!("socket event delivery failed: {error}");
            }
        }
    });
    io.ns(
        "/",
        move |socket: SocketRef, TryData(auth): TryData<Value>| {
            let db = db.clone();
            let secret = secret.clone();
            async move {
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
                let Some(claims) = verify(&token, &secret) else {
                    socket.disconnect().ok();
                    return;
                };
                let id = Uuid::new_v4();
                if connect(&db, id, &claims).await.is_err() {
                    // COMMIT may have succeeded even when its acknowledgement was lost.
                    // Deleting this unique ID is harmless if the transaction rolled back.
                    let update_presence = verify(&token, &secret).is_some();
                    tokio::spawn(retry_disconnect(db.clone(), id, claims, update_presence));
                    socket.disconnect().ok();
                    return;
                }
                socket.extensions.insert(claims.clone());
                socket.join(claims.id.clone());
                let cleanup_db = db.clone();
                let cleanup_claims = claims.clone();
                let cleanup_token = token.clone();
                let cleanup_secret = secret.clone();
                socket.on_disconnect(move || {
                    let db = cleanup_db.clone();
                    let claims = cleanup_claims.clone();
                    let token = cleanup_token.clone();
                    let secret = cleanup_secret.clone();
                    async move {
                        let update_presence = verify(&token, &secret).is_some();
                        tokio::spawn(retry_disconnect(db, id, claims, update_presence));
                    }
                });
                if !socket.connected() {
                    let update_presence = verify(&token, &secret).is_some();
                    tokio::spawn(retry_disconnect(db, id, claims, update_presence));
                }
            }
        },
    );
    let app = Router::new().layer(layer);
    let address = std::env::var("LISTEN_ADDR").unwrap_or_else(|_| "127.0.0.1:3002".into());
    let listener = tokio::net::TcpListener::bind(address).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
