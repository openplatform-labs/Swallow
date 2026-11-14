use axum::{
    Json, Router,
    extract::{Request, State},
    http::{HeaderValue, Method, StatusCode, Uri, header},
    response::{IntoResponse, Response},
};
use std::{
    error::Error,
    future::IntoFuture,
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tower::ServiceExt;
use tower_http::cors::{Any, CorsLayer};

#[derive(Clone)]
struct Connected {
    router: Router,
    db: Arc<tokio_postgres::Client>,
}

#[derive(Clone)]
struct ApiState {
    connections: Arc<Vec<Arc<RwLock<Option<Connected>>>>>,
    next: Arc<AtomicUsize>,
    timeout: Duration,
    draining: Arc<AtomicBool>,
}

async fn forward(State(shared): State<ApiState>, request: Request) -> Response {
    if shared.draining.load(Ordering::Acquire) {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            [(header::CACHE_CONTROL, "no-store")],
            Json(serde_json::json!({"message":"Server draining","statusCode":503})),
        )
            .into_response();
    }
    let start = shared.next.fetch_add(1, Ordering::Relaxed) % shared.connections.len();
    let current = (0..shared.connections.len()).find_map(|offset| {
        shared.connections[(start + offset) % shared.connections.len()]
            .read()
            .ok()
            .and_then(|guard| guard.clone())
            .filter(|c| !c.db.is_closed())
    });
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
    let mut probes = tokio::task::JoinSet::new();
    if !shared.draining.load(Ordering::Acquire) {
        for slot in shared.connections.iter() {
            let current = slot
                .read()
                .ok()
                .and_then(|guard| guard.clone())
                .filter(|c| !c.db.is_closed());
            if let Some(current) = current {
                let slot = slot.clone();
                let timeout = shared.timeout;
                probes.spawn(async move {
                    let query_ok = matches!(
                        tokio::time::timeout(timeout, current.db.simple_query("SELECT 1")).await,
                        Ok(Ok(_))
                    );
                    let still_current = slot
                        .read()
                        .ok()
                        .and_then(|guard| {
                            guard
                                .as_ref()
                                .map(|active| Arc::ptr_eq(&active.db, &current.db))
                        })
                        .unwrap_or(false);
                    query_ok && still_current
                });
            }
        }
    }
    let mut ready = false;
    while let Some(result) = probes.join_next().await {
        if matches!(result, Ok(true)) {
            ready = true;
            break;
        }
    }
    probes.abort_all();
    ready &= !shared.draining.load(Ordering::Acquire);
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

// Browser access is opt-in; this is not an authentication or authorization check.
fn cors_config(raw: Option<&str>, credentials: bool) -> Result<Option<CorsLayer>, &'static str> {
    let Some(raw) = raw else {
        return if credentials {
            Err("CORS credentials require allowed origins")
        } else {
            Ok(None)
        };
    };
    if raw.trim().is_empty() {
        return Err("invalid CORS origin list");
    }
    let entries: Vec<&str> = raw.split(',').map(str::trim).collect();
    if entries.len() > 64 || entries.iter().any(|v| v.is_empty()) {
        return Err("invalid CORS origin list");
    }
    let wildcard = entries == ["*"];
    if wildcard && credentials {
        return Err("CORS wildcard cannot allow credentials");
    }
    let mut allowed = Vec::new();
    if !wildcard {
        for origin in entries {
            let uri: Uri = origin.parse().map_err(|_| "invalid CORS origin")?;
            let scheme = uri
                .scheme_str()
                .filter(|s| *s == "http" || *s == "https")
                .ok_or("invalid CORS origin")?;
            let authority = uri.authority().ok_or("invalid CORS origin")?;
            if authority.host().is_empty()
                || authority.as_str().contains('@')
                || format!("{scheme}://{authority}") != origin
            {
                return Err("invalid CORS origin");
            }
            let header = HeaderValue::from_str(origin).map_err(|_| "invalid CORS origin")?;
            if allowed.contains(&header) {
                return Err("duplicate CORS origin");
            }
            allowed.push(header);
        }
    }
    let cors = CorsLayer::new()
        .allow_methods([
            Method::GET,
            Method::HEAD,
            Method::POST,
            Method::PUT,
            Method::PATCH,
            Method::DELETE,
            Method::OPTIONS,
        ])
        .allow_headers([
            header::AUTHORIZATION,
            header::CONTENT_TYPE,
            header::ACCEPT,
            header::HeaderName::from_static("novu-environment-id"),
        ])
        .allow_credentials(credentials)
        .max_age(Duration::from_secs(300));
    Ok(Some(if wildcard {
        cors.allow_origin(Any)
    } else {
        cors.allow_origin(allowed)
    }))
}
fn cors_from_env() -> Result<Option<CorsLayer>, Box<dyn Error + Send + Sync>> {
    let credentials = match std::env::var("SWALLOW_CORS_ALLOW_CREDENTIALS").as_deref() {
        Ok("true") => true,
        Ok("false") | Err(std::env::VarError::NotPresent) => false,
        _ => return Err("invalid CORS credentials setting".into()),
    };
    let raw = match std::env::var("SWALLOW_CORS_ALLOWED_ORIGINS") {
        Ok(value) => Some(value),
        Err(std::env::VarError::NotPresent) => None,
        Err(_) => return Err("invalid CORS origin list".into()),
    };
    cors_config(raw.as_deref(), credentials).map_err(Into::into)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let url = std::env::var("DATABASE_URL")?;
    let (config, tls) = novu_rust_contracts::postgres::configure(&url)?;
    let grace = duration_env("SWALLOW_API_SHUTDOWN_GRACE_SECONDS", 30)?;
    let drain: u64 = match std::env::var("SWALLOW_API_DRAIN_SECONDS") {
        Ok(value) => value.parse().map_err(|_| "invalid drain setting")?,
        Err(std::env::VarError::NotPresent) => 0,
        Err(_) => return Err("invalid drain setting".into()),
    };
    if drain > 300 {
        return Err("invalid drain setting".into());
    }
    #[cfg(unix)]
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let interval = duration_env("SWALLOW_API_DB_HEALTH_INTERVAL_SECONDS", 10)?;
    let timeout = duration_env("SWALLOW_API_DB_HEALTH_TIMEOUT_SECONDS", 30)?;
    let connections: usize = match std::env::var("SWALLOW_API_DB_CONNECTIONS") {
        Ok(value) => value
            .parse()
            .map_err(|_| "invalid API database connection count")?,
        Err(std::env::VarError::NotPresent) => 1,
        Err(_) => return Err("invalid API database connection count".into()),
    };
    if !(1..=16).contains(&connections) {
        return Err("invalid API database connection count".into());
    }
    let health_endpoints = match std::env::var("SWALLOW_API_HEALTH_ENDPOINTS_ENABLED").as_deref() {
        Ok("true") => true,
        Ok("false") | Err(std::env::VarError::NotPresent) => false,
        _ => return Err("invalid health endpoint setting".into()),
    };
    let runtime = novu_rust_api::RuntimeConfig::try_from_env()?;
    let cors = cors_from_env()?;
    let shared = ApiState {
        connections: Arc::new(
            (0..connections)
                .map(|_| Arc::new(RwLock::new(None)))
                .collect(),
        ),
        next: Arc::new(AtomicUsize::new(0)),
        timeout,
        draining: Arc::new(AtomicBool::new(false)),
    };
    for slot in shared.connections.iter() {
        let manager_state = slot.clone();
        let config = config.clone();
        let tls = tls.clone();
        let runtime = runtime.clone();
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
    }
    let address = std::env::var("LISTEN_ADDR").unwrap_or_else(|_| "127.0.0.1:3000".into());
    let listener = tokio::net::TcpListener::bind(&address).await?;
    let mut app = Router::new().fallback(forward);
    if health_endpoints {
        app = app
            .route("/health/live", axum::routing::get(live))
            .route("/health/ready", axum::routing::get(ready));
    }
    if let Some(cors) = cors {
        app = app.layer(cors);
    }
    let draining = shared.draining.clone();
    let (shutdown_started, shutdown_notice) = tokio::sync::oneshot::channel::<()>();
    let shutdown = async move {
        #[cfg(unix)]
        tokio::select! { _ = sigterm.recv() => {}, _ = tokio::signal::ctrl_c() => {} }
        #[cfg(not(unix))]
        let _ = tokio::signal::ctrl_c().await;
        draining.store(true, Ordering::Release);
        if drain > 0 {
            tokio::time::sleep(Duration::from_secs(drain)).await;
        }
        let _ = shutdown_started.send(());
    };
    let server = axum::serve(listener, app.with_state(shared))
        .with_graceful_shutdown(shutdown)
        .into_future();
    tokio::pin!(server);
    tokio::select! {
        result = &mut server => result?,
        _ = shutdown_notice => {
            match tokio::time::timeout(grace, &mut server).await {
                Ok(result) => result?,
                Err(_) => return Err("API graceful shutdown timed out".into()),
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod cors_tests {
    use super::*;
    use axum::{body::Body, http::Request};
    #[test]
    fn validates_origin_configuration() {
        assert!(cors_config(None, false).unwrap().is_none());
        assert!(cors_config(None, true).is_err());
        assert!(cors_config(Some("*"), false).unwrap().is_some());
        assert!(cors_config(Some("*"), true).is_err());
        for bad in [
            "",
            "null",
            "https://example.com/",
            "https://example.com/path",
            "https://user@example.com",
            "https://example.com,*",
            "https://x.test,https://x.test",
        ] {
            assert!(cors_config(Some(bad), false).is_err(), "{bad}");
        }
        assert!(
            cors_config(
                Some("https://console.example.com,http://localhost:4200"),
                true
            )
            .unwrap()
            .is_some()
        );
    }
    #[tokio::test]
    async fn wildcard_and_credential_modes_are_distinct() {
        let make = |value: &str, credentials| {
            Router::new()
                .route("/resource", axum::routing::get(|| async { StatusCode::OK }))
                .layer(cors_config(Some(value), credentials).unwrap().unwrap())
        };
        let request = || {
            Request::builder()
                .uri("/resource")
                .header("origin", "https://console.example.com")
                .body(Body::empty())
                .unwrap()
        };
        let wildcard = make("*", false).oneshot(request()).await.unwrap();
        assert_eq!(wildcard.headers()[header::ACCESS_CONTROL_ALLOW_ORIGIN], "*");
        assert!(
            wildcard
                .headers()
                .get(header::ACCESS_CONTROL_ALLOW_CREDENTIALS)
                .is_none()
        );
        let explicit = make("https://console.example.com", true)
            .oneshot(request())
            .await
            .unwrap();
        assert_eq!(
            explicit.headers()[header::ACCESS_CONTROL_ALLOW_CREDENTIALS],
            "true"
        );
        assert_eq!(
            explicit.headers()[header::ACCESS_CONTROL_ALLOW_ORIGIN],
            "https://console.example.com"
        );
        let none = Router::new()
            .route("/resource", axum::routing::get(|| async { StatusCode::OK }))
            .oneshot(request())
            .await
            .unwrap();
        assert!(
            none.headers()
                .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
                .is_none()
        );
    }
    #[tokio::test]
    async fn preflight_and_responses_obey_selected_origins() {
        let cors = cors_config(Some("https://console.example.com"), false)
            .unwrap()
            .unwrap();
        let app = Router::new()
            .route(
                "/resource",
                axum::routing::get(|| async { StatusCode::UNAUTHORIZED }),
            )
            .layer(cors);
        let request = |origin: &str, headers: &str| {
            Request::builder()
                .method("OPTIONS")
                .uri("/resource")
                .header("origin", origin)
                .header("access-control-request-method", "GET")
                .header("access-control-request-headers", headers)
                .body(Body::empty())
                .unwrap()
        };
        let ok = app
            .clone()
            .oneshot(request(
                "https://console.example.com",
                "authorization,content-type,novu-environment-id",
            ))
            .await
            .unwrap();
        assert_eq!(ok.status(), StatusCode::OK);
        assert_eq!(
            ok.headers()[header::ACCESS_CONTROL_ALLOW_ORIGIN],
            "https://console.example.com"
        );
        assert!(
            ok.headers()
                .get(header::ACCESS_CONTROL_ALLOW_CREDENTIALS)
                .is_none()
        );
        let denied = app
            .clone()
            .oneshot(request("https://other.example.com", "authorization"))
            .await
            .unwrap();
        assert!(
            denied
                .headers()
                .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
                .is_none()
        );
        let denied_headers = app
            .clone()
            .oneshot(request("https://console.example.com", "x-unknown"))
            .await
            .unwrap();
        assert!(
            !denied_headers.headers()[header::ACCESS_CONTROL_ALLOW_HEADERS]
                .to_str()
                .unwrap()
                .contains("x-unknown")
        );
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/resource")
                    .header("origin", "https://console.example.com")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            response.headers()[header::ACCESS_CONTROL_ALLOW_ORIGIN],
            "https://console.example.com"
        );
        assert!(response.headers().contains_key(header::VARY));
    }
}
