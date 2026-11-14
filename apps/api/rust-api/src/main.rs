use axum::{
    Json, Router,
    extract::{Request, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use std::{
    error::Error,
    sync::{Arc, RwLock},
    time::Duration,
};
use tower::ServiceExt;

type SharedRouter = Arc<RwLock<Option<Router>>>;

async fn forward(State(shared): State<SharedRouter>, request: Request) -> Response {
    let router = shared.read().ok().and_then(|guard| guard.clone());
    match router {
        Some(router) => match router.oneshot(request).await {
            Ok(response) => response,
            Err(never) => match never {},
        },
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"message":"Database unavailable","statusCode":503})),
        )
            .into_response(),
    }
}

fn duration_env(name: &str, default: u64) -> Result<Duration, Box<dyn Error + Send + Sync>> {
    let seconds = match std::env::var(name) {
        Ok(value) => value.parse()?,
        Err(std::env::VarError::NotPresent) => default,
        Err(_) => return Err("invalid database health configuration".into()),
    };
    if !(1..=300).contains(&seconds) {
        return Err("database health setting must be 1-300 seconds".into());
    }
    Ok(Duration::from_secs(seconds))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let url = std::env::var("DATABASE_URL")?;
    let (config, tls) = novu_rust_contracts::postgres::configure(&url)?;
    let interval = duration_env("SWALLOW_API_DB_HEALTH_INTERVAL_SECONDS", 10)?;
    let timeout = duration_env("SWALLOW_API_DB_HEALTH_TIMEOUT_SECONDS", 30)?;
    let runtime = novu_rust_api::RuntimeConfig::from_env();
    let shared: SharedRouter = Arc::new(RwLock::new(None));
    let manager_state = shared.clone();
    tokio::spawn(async move {
        loop {
            let (db, connection) =
                match tokio::time::timeout(timeout, config.connect(tls.clone())).await {
                    Ok(Ok(value)) => value,
                    _ => {
                        eprintln!("database unavailable; retrying");
                        tokio::time::sleep(Duration::from_secs(2)).await;
                        continue;
                    }
                };
            let db = Arc::new(db);
            let mut driver = tokio::spawn(connection);
            if let Ok(mut guard) = manager_state.write() {
                *guard = Some(runtime.router(db.clone()));
            }
            loop {
                tokio::select! {
                    _ = &mut driver => break,
                    _ = tokio::time::sleep(interval) => {
                        if !matches!(tokio::time::timeout(timeout, db.simple_query("SELECT 1")).await, Ok(Ok(_))) { break; }
                    }
                }
            }
            if let Ok(mut guard) = manager_state.write() {
                *guard = None;
            }
            driver.abort();
            eprintln!("database connection lost; reconnecting");
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    });
    let address = std::env::var("LISTEN_ADDR").unwrap_or_else(|_| "127.0.0.1:3000".into());
    let listener = tokio::net::TcpListener::bind(&address).await?;
    axum::serve(listener, Router::new().fallback(forward).with_state(shared)).await?;
    Ok(())
}
