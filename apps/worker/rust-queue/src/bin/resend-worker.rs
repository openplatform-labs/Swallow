//! Experimental Resend worker; use only with isolated fixtures.
use novu_postgres_queue::{
    resend::{Config, Resend},
    resend_queue,
};
use std::{error::Error, time::Duration};

// Only test-mocks builds can target a local fixture; production always uses Resend.
fn provider(config: Config) -> Result<Resend, Box<dyn Error + Send + Sync>> {
    #[cfg(feature = "test-mocks")]
    match std::env::var("SWALLOW_TEST_RESEND_ENDPOINT") {
        Ok(endpoint) => {
            let url = reqwest::Url::parse(&endpoint).map_err(|_| "invalid fixture endpoint")?;
            if url.scheme() != "http" || url.host_str() != Some("127.0.0.1") || url.port().is_none()
                || !url.username().is_empty() || url.password().is_some() || url.path() != "/emails"
                || url.query().is_some() || url.fragment().is_some() {
                return Err("invalid fixture endpoint".into());
            }
            return Ok(Resend::test_endpoint(config, &endpoint)?);
        }
        Err(std::env::VarError::NotPresent) => {},
        Err(_) => return Err("invalid fixture endpoint".into()),
    }
    Ok(Resend::new(config)?)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    novu_postgres_queue::shutdown::run_until_signal(run).await
}

async fn run(
    stop: novu_postgres_queue::shutdown::Stop,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let step_policy = novu_postgres_queue::email_step_policy::Policy::from_env()?;
    let shared_cooldown = novu_postgres_queue::resend_cooldown::enabled_from_env()?;
    let environment = std::env::var("NOVU_RESEND_ENVIRONMENT_ID")?;
    if environment.trim().is_empty() {
        return Err("environment is required".into());
    }
    let reload_key_file = match std::env::var("NOVU_RESEND_RELOAD_API_KEY").as_deref() {
        Ok("true") => Some(std::path::PathBuf::from(
            std::env::var_os("NOVU_RESEND_API_KEY_FILE")
                .ok_or("Resend key reload requires a key file")?,
        )),
        Ok("false") | Err(std::env::VarError::NotPresent) => None,
        _ => return Err("invalid Resend key reload setting".into()),
    };
    let api_key = if let Some(path) = &reload_key_file {
        if std::env::var_os("NOVU_RESEND_API_KEY").is_some() {
            return Err("Resend key sources conflict".into());
        }
        novu_rust_contracts::secret::read_regular_file(path)?
    } else {
        novu_rust_contracts::secret::from_env("NOVU_RESEND_API_KEY", "NOVU_RESEND_API_KEY_FILE")?
            .ok_or("Resend API key is required")?
    };
    let mut provider = provider(Config {
        api_key,
        from: std::env::var("NOVU_RESEND_FROM")?,
        sender_name: std::env::var("NOVU_RESEND_SENDER_NAME").unwrap_or_default(),
    })?;
    let (config, tls) = novu_rust_contracts::postgres::configure(&std::env::var("DATABASE_URL")?)?;
    let timeout = match std::env::var("NOVU_RESEND_DB_TIMEOUT_SECONDS") {
        Ok(value) => value
            .parse::<u64>()
            .ok()
            .filter(|n| (1..=300).contains(n))
            .ok_or("invalid Resend database timeout")?,
        Err(std::env::VarError::NotPresent) => 30,
        Err(_) => return Err("invalid Resend database timeout".into()),
    };
    let retry_guard = match std::env::var("NOVU_RESEND_RETRY_EVIDENCE_GUARD").as_deref() {
        Ok("true") => true,
        Ok("false") | Err(std::env::VarError::NotPresent) => false,
        _ => return Err("invalid Resend retry evidence guard setting".into()),
    };
    let correlation = match std::env::var("NOVU_RESEND_CORRELATION_TAG").as_deref() {
        Ok("true") => true,
        Ok("false") | Err(std::env::VarError::NotPresent) => false,
        _ => return Err("invalid Resend correlation tag setting".into()),
    };
    let correlation_guard = match std::env::var("NOVU_RESEND_RETRY_CORRELATION_GUARD").as_deref() {
        Ok("true") => true,
        Ok("false") | Err(std::env::VarError::NotPresent) => false,
        _ => return Err("invalid Resend retry correlation guard setting".into()),
    };
    let retry_key = match std::env::var("NOVU_RESEND_IDEMPOTENCY_SCOPE").as_deref() {
        Ok("job") | Err(std::env::VarError::NotPresent) => false,
        Ok("retry") => true,
        _ => return Err("invalid Resend idempotency scope".into()),
    };
    let replay_uncertain_once = match std::env::var("NOVU_RESEND_REPLAY_UNCERTAIN_ONCE").as_deref() {
        Ok("true") => true,
        Ok("false") | Err(std::env::VarError::NotPresent) => false,
        _ => return Err("invalid Resend immediate replay setting".into()),
    };
    let auto_429 = match std::env::var("NOVU_RESEND_AUTO_RETRY_429").as_deref() {
        Ok("true") => true,
        Ok("false") | Err(std::env::VarError::NotPresent) => false,
        _ => return Err("invalid Resend automatic 429 retry setting".into()),
    };
    if auto_429 && !(retry_guard && correlation && correlation_guard && retry_key) {
        return Err("automatic 429 retries require both evidence guards, correlation tags and retry-scoped idempotency".into());
    }
    let timeout = Duration::from_secs(timeout);
    loop {
        if stop.requested() {
            return Ok(());
        }
        let connection = tokio::time::timeout(timeout, config.connect(tls.clone())).await;
        if let Ok(Ok((mut db, driver))) = connection {
            let task = tokio::spawn(driver);
            let statement_timeout = format!("{}ms", timeout.as_millis());
            let configured = matches!(
                tokio::time::timeout(
                    timeout,
                    db.query_one(
                        "SELECT set_config('statement_timeout',$1,false)",
                        &[&statement_timeout]
                    )
                )
                .await,
                Ok(Ok(_))
            );
            let mut recovery = tokio::time::Instant::now();
            let mut auto_retry = tokio::time::Instant::now();
            if configured {
                match tokio::time::timeout(
                    timeout,
                    novu_postgres_queue::email_step_policy::register(
                        &db,
                        &environment,
                        step_policy,
                    ),
                )
                .await
                {
                    Ok(Ok(true)) => {}
                    Ok(Ok(false)) => {
                        task.abort();
                        return Err("email workers have different workflow step policies".into());
                    }
                    _ => {
                        task.abort();
                        let _ = task.await;
                        eprintln!("email workflow policy registration failed; reconnecting");
                        stop.wait(Duration::from_secs(2)).await;
                        continue;
                    }
                }
                match tokio::time::timeout(
                    timeout,
                    resend_queue::register_worker_policy(&db, &environment, retry_key, shared_cooldown),
                )
                .await
                {
                    Ok(Ok(true)) => {}
                    Ok(Ok(false)) => {
                        task.abort();
                        return Err("Resend workers have different idempotency or shared cooldown policies".into());
                    }
                    _ => {
                        task.abort();
                        let _ = task.await;
                        eprintln!("Resend idempotency policy registration failed; reconnecting");
                        stop.wait(Duration::from_secs(2)).await;
                        continue;
                    }
                }
                if replay_uncertain_once && !matches!(tokio::time::timeout(timeout, db.query_one("SELECT has_column_privilege(current_user,'public.novu_rust_resend_attempts','same_request_replays','UPDATE') AND has_column_privilege(current_user,'public.novu_rust_resend_attempts','same_request_replays','SELECT') AND has_table_privilege(current_user,'public.novu_rust_delivery_reconciliations','SELECT')", &[])).await, Ok(Ok(ref row)) if row.get::<_,bool>(0)) {
                    eprintln!("Resend immediate replay migration or privilege unavailable; reconnecting");
                    task.abort();
                    let _ = task.await;
                    stop.wait(Duration::from_secs(2)).await;
                    continue;
                }
                if shared_cooldown && !matches!(tokio::time::timeout(timeout, db.query_one("SELECT public.novu_rust_resend_cooldown_check($1)", &[&environment])).await, Ok(Ok(_))) {
                    task.abort(); let _ = task.await;
                    eprintln!("Resend shared cooldown check unavailable; reconnecting");
                    stop.wait(Duration::from_secs(2)).await;
                    continue;
                }
                loop {
                    if stop.requested() {
                        break;
                    }
                    if tokio::time::Instant::now() >= recovery {
                        if !matches!(
                            tokio::time::timeout(
                                timeout,
                                resend_queue::recover_expired(&mut db, &environment)
                            )
                            .await,
                            Ok(Ok(_))
                        ) {
                            break;
                        }
                        recovery = tokio::time::Instant::now() + Duration::from_secs(5);
                    }
                    if auto_429 && tokio::time::Instant::now() >= auto_retry {
                        if !matches!(
                            tokio::time::timeout(
                                timeout,
                                db.query_one(
                                    "SELECT public.novu_rust_resend_auto_retry($1)",
                                    &[&environment]
                                )
                            )
                            .await,
                            Ok(Ok(_))
                        ) {
                            eprintln!("Resend automatic retry check failed; reconnecting");
                            break;
                        }
                        auto_retry = tokio::time::Instant::now() + Duration::from_secs(5);
                    }
                    if stop.requested() {
                        break;
                    }
                    if let Some(path) = &reload_key_file
                        && provider.reload_api_key_file(path).is_err()
                    {
                        eprintln!("Resend key reload failed; dispatch paused");
                        stop.wait(Duration::from_secs(2)).await;
                        continue;
                    }
                    match tokio::time::timeout(
                        timeout,
                        resend_queue::process_one_with_immediate_replay(
                            &mut db,
                            &environment,
                            &provider,
                            retry_guard,
                            step_policy,
                            correlation,
                            correlation_guard,
                            retry_key,
                            shared_cooldown,
                            replay_uncertain_once,
                        ),
                    )
                    .await
                    {
                        Ok(Ok(true)) => {}
                        Ok(Ok(false)) => stop.wait(resend_queue::POLL_INTERVAL).await,
                        _ => break,
                    }
                }
            }
            // Dropping the client alone can leave the driver running while a
            // cancelled transaction waits on the server. Abort it before retry.
            task.abort();
            let _ = task.await;
        }
        if stop.requested() {
            return Ok(());
        }
        eprintln!("Resend worker database operation stopped; reconnecting");
        stop.wait(Duration::from_secs(2)).await;
    }
}
