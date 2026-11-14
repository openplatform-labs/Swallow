use axum::{
    Json, Router,
    extract::{Request, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use std::{
    error::Error,
    sync::{Arc, RwLock},
    time::Duration,
};
use tower::ServiceExt;

#[derive(Clone)]
struct Connected {
    router: Router,
    db: Arc<tokio_postgres::Client>,
}

#[derive(Clone)]
struct ApiState {
    current: Arc<RwLock<Option<Connected>>>,
    timeout: Duration,
}

async fn forward(State(shared): State<ApiState>, request: Request) -> Response {
    let current = shared.current.read().ok().and_then(|guard| guard.clone());
    match current {
        Some(current) => match current.router.oneshot(request).await {
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

async fn live() -> Response {
    (
        StatusCode::OK,
        [(header::CACHE_CONTROL, "no-store")],
        Json(serde_json::json!({"status":"live"})),
    )
        .into_response()
}

async fn ready(State(shared): State<ApiState>) -> Response {
    let current = shared.current.read().ok().and_then(|guard| guard.clone());
    let ready = if let Some(current) = current {
        let query_ok = matches!(
            tokio::time::timeout(shared.timeout, current.db.simple_query("SELECT 1")).await,
            Ok(Ok(_))
        );
        let still_current = shared
            .current
            .read()
            .ok()
            .and_then(|guard| {
                guard
                    .as_ref()
                    .map(|active| Arc::ptr_eq(&active.db, &current.db))
            })
            .unwrap_or(false);
        query_ok && still_current
    } else {
        false
    };
    let status = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        status,
        [(header::CACHE_CONTROL, "no-store")],
        Json(serde_json::json!({"status":if ready {"ready"} else {"not_ready"}})),
    )
        .into_response()
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
    let health_endpoints = match std::env::var("SWALLOW_API_HEALTH_ENDPOINTS_ENABLED").as_deref() {
        Ok("true") => true,
        Ok("false") | Err(std::env::VarError::NotPresent) => false,
        _ => return Err("invalid health endpoint setting".into()),
    };
    let runtime = novu_rust_api::RuntimeConfig::try_from_env()?;
    let shared = ApiState {
        current: Arc::new(RwLock::new(None)),
        timeout,
    };
    let manager_state = shared.current.clone();
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
                *guard = Some(Connected {
                    router: runtime.router(db.clone()),
                    db: db.clone(),
                });
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
    let mut app = Router::new().fallback(forward);
    if health_endpoints {
        app = app
            .route("/health/live", axum::routing::get(live))
            .route("/health/ready", axum::routing::get(ready));
    }
    axum::serve(listener, app.with_state(shared)).await?;
    Ok(())
}
