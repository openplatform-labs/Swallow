//! Environment-scoped SMTP worker. Delivery after a recorded attempt is never retried automatically.
use novu_postgres_queue::{email_step_policy, smtp_alert::SmtpAlert, smtp_queue};
use std::{error::Error, time::Duration};
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    novu_postgres_queue::shutdown::run_until_signal(run).await
}
async fn run(
    stop: novu_postgres_queue::shutdown::Stop,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let environment = std::env::var("NOVU_SMTP_ENVIRONMENT_ID")?;
    if environment.trim().is_empty() {
        return Err("environment is required".into());
    }
    let scoped_rls = match std::env::var("SWALLOW_SMTP_SCOPED_RLS").as_deref() {
        Ok("true") => true,
        Ok("false") | Err(std::env::VarError::NotPresent) => false,
        _ => return Err("invalid SMTP scoped RLS setting".into()),
    };
    let reload_path = match std::env::var("NOVU_SMTP_RELOAD_PASSWORD").as_deref() {
        Ok("true") => Some(std::path::PathBuf::from(
            std::env::var_os("SWALLOW_SMTP_PASSWORD_FILE")
                .ok_or("SMTP password reload requires a file")?,
        )),
        Ok("false") | Err(std::env::VarError::NotPresent) => None,
        _ => return Err("invalid SMTP password reload setting".into()),
    };
    let reload_ca_path = match std::env::var("NOVU_SMTP_RELOAD_CA").as_deref() {
        Ok("true") => Some(std::path::PathBuf::from(
            std::env::var_os("SWALLOW_SMTP_CA_FILE").ok_or("SMTP CA reload requires a file")?,
        )),
        Ok("false") | Err(std::env::VarError::NotPresent) => None,
        _ => return Err("invalid SMTP CA reload setting".into()),
    };
    let exit_on_reload_failure = match std::env::var("NOVU_SMTP_RELOAD_FAILURE_POLICY").as_deref() {
        Ok("pause") | Err(std::env::VarError::NotPresent) => false,
        Ok("exit") if reload_path.is_some() || reload_ca_path.is_some() => true,
        _ => return Err("invalid SMTP reload failure policy".into()),
    };
    let credential_health = match std::env::var("NOVU_SMTP_CREDENTIAL_HEALTH_ENABLED").as_deref() {
        Ok("true") => true,
        Ok("false") | Err(std::env::VarError::NotPresent) => false,
        _ => return Err("invalid SMTP credential health setting".into()),
    };
    let ca_health = match std::env::var("NOVU_SMTP_CA_HEALTH_ENABLED").as_deref() {
        Ok("true") => true,
        Ok("false") | Err(std::env::VarError::NotPresent) => false,
        _ => return Err("invalid SMTP CA health setting".into()),
    };
    if ca_health && reload_ca_path.is_none() {
        return Err("SMTP CA health requires CA reload".into());
    }
    if credential_health && reload_path.is_none() {
        return Err("SMTP credential health requires password reload".into());
    }
    let mut provider = SmtpAlert::from_env_with_password_file(reload_path.as_deref())
        .map_err(|_| "invalid SMTP configuration")?;
    let policy = email_step_policy::Policy::from_env()?;
    let seconds = match std::env::var("NOVU_SMTP_DB_TIMEOUT_SECONDS") {
        Ok(v) => v
            .parse::<u64>()
            .ok()
            .filter(|v| (1..=300).contains(v))
            .ok_or("invalid SMTP database timeout")?,
        Err(std::env::VarError::NotPresent) => 30,
        Err(_) => return Err("invalid SMTP database timeout".into()),
    };
    let timeout = Duration::from_secs(seconds);
    let (config, tls) = novu_rust_contracts::postgres::configure(&std::env::var("DATABASE_URL")?)?;
    loop {
        if stop.requested() {
            return Ok(());
        }
        if let Ok(Ok((mut db, driver))) =
            tokio::time::timeout(timeout, config.connect(tls.clone())).await
        {
            let task = tokio::spawn(driver);
            let statement_timeout = format!("{}ms", seconds * 1000);
            let ready = matches!(
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
            if ready {
                if scoped_rls
                    && !matches!(
                        tokio::time::timeout(
                            timeout,
                            db.query_one(
                                "SELECT public.novu_rust_assert_smtp_scoped_login($1)",
                                &[&environment]
                            )
                        )
                        .await,
                        Ok(Ok(_))
                    )
                {
                    task.abort();
                    let _ = task.await;
                    eprintln!("SMTP scoped isolation unavailable; reconnecting");
                    stop.wait(Duration::from_secs(2)).await;
                    continue;
                }
                match tokio::time::timeout(
                    timeout,
                    email_step_policy::register(&db, &environment, policy),
                )
                .await
                {
                    Ok(Ok(true)) => {}
                    Ok(Ok(false)) => {
                        task.abort();
                        return Err("email workers have different workflow policies".into());
                    }
                    _ => {
                        task.abort();
                        eprintln!("SMTP policy registration failed; reconnecting");
                        stop.wait(Duration::from_secs(2)).await;
                        continue;
                    }
                }
                let mut recovery = tokio::time::Instant::now();
                let health_instance = uuid::Uuid::new_v4();
                let mut health_previous: Option<bool> = None;
                let mut health_next = tokio::time::Instant::now();
                let mut ca_previous: Option<bool> = None;
                let mut ca_next = tokio::time::Instant::now();
                loop {
                    if stop.requested() {
                        break;
                    }
                    if tokio::time::Instant::now() >= recovery {
                        if !matches!(
                            tokio::time::timeout(
                                timeout,
                                smtp_queue::recover_expired(&mut db, &environment)
                            )
                            .await,
                            Ok(Ok(_))
                        ) {
                            break;
                        }
                        recovery = tokio::time::Instant::now() + Duration::from_secs(5);
                    }
                    let reload_failed = reload_path
                        .as_ref()
                        .is_some_and(|path| provider.reload_password_file(path).is_err());
                    let ca_reload_failed = reload_ca_path
                        .as_ref()
                        .is_some_and(|path| provider.reload_ca_file(path).is_err());
                    let healthy = !reload_failed;
                    if credential_health
                        && (health_previous != Some(healthy)
                            || tokio::time::Instant::now() >= health_next)
                    {
                        let state = if healthy { "healthy" } else { "unhealthy" };
                        let result = tokio::time::timeout(
                            timeout,
                            db.query_one(
                                "SELECT public.novu_rust_smtp_credential_heartbeat($1,$2,$3)",
                                &[&environment, &health_instance, &state],
                            ),
                        )
                        .await;
                        if !matches!(result,Ok(Ok(row)) if row.get::<_,bool>(0)) {
                            eprintln!("SMTP credential health observation failed; reconnecting");
                            break;
                        }
                        health_previous = Some(healthy);
                        health_next = tokio::time::Instant::now() + Duration::from_secs(10);
                    }
                    let ca_healthy = !ca_reload_failed;
                    if ca_health && (ca_previous != Some(ca_healthy) || tokio::time::Instant::now() >= ca_next) {
                        let state = if ca_healthy { "healthy" } else { "unhealthy" };
                        let result = tokio::time::timeout(timeout, db.query_one(
                            "SELECT public.novu_rust_smtp_ca_heartbeat($1,$2,$3)",
                            &[&environment, &health_instance, &state],
                        )).await;
                        if !matches!(result, Ok(Ok(row)) if row.get::<_,bool>(0)) {
                            eprintln!("SMTP CA health observation failed; reconnecting");
                            break;
                        }
                        ca_previous = Some(ca_healthy);
                        ca_next = tokio::time::Instant::now() + Duration::from_secs(10);
                    }
                    if reload_failed || ca_reload_failed {
                        eprintln!("SMTP credential or CA reload failed; dispatch paused");
                        if exit_on_reload_failure {
                            task.abort();
                            let _ = task.await;
                            return Err("SMTP credential or CA reload failed".into());
                        }
                        stop.wait(Duration::from_secs(2)).await;
                        continue;
                    }
                    match tokio::time::timeout(
                        timeout,
                        smtp_queue::process_one(&mut db, &environment, &provider, policy),
                    )
                    .await
                    {
                        Ok(Ok(true)) => {}
                        Ok(Ok(false)) => stop.wait(Duration::from_secs(1)).await,
                        _ => break,
                    }
                }
            }
            task.abort();
            let _ = task.await;
        }
        if stop.requested() {
            return Ok(());
        }
        eprintln!("SMTP worker unavailable; reconnecting");
        stop.wait(Duration::from_secs(2)).await;
    }
}
