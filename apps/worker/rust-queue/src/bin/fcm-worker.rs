//! Experimental environment-scoped FCM worker; not a full Novu worker.
use novu_postgres_queue::{fcm::Fcm, fcm_queue};
use std::{
    collections::{BTreeMap, HashMap},
    error::Error,
    fs::File,
    io::{self, Read},
    time::Duration,
};

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct IntegrationConfig {
    project_id: String,
    access_token: Option<String>,
    client_email: Option<String>,
    private_key: Option<String>,
}

// PEM and JSON may contain newlines. Never include a path or value in errors.
fn multiline_secret(name: &str, file_name: &str, limit: u64) -> Result<Option<String>, io::Error> {
    let inline = match std::env::var(name) {
        Ok(value) => Some(value),
        Err(std::env::VarError::NotPresent) => None,
        Err(_) => return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid secret")),
    };
    let path = std::env::var_os(file_name);
    if inline.is_some() && path.is_some() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "multiple secret sources",
        ));
    }
    let bytes = match (inline, path) {
        (Some(value), None) => value.into_bytes(),
        (None, Some(path)) => {
            let mut bytes = Vec::new();
            File::open(path)
                .map_err(|_| io::Error::other("secret file unavailable"))?
                .take(limit + 1)
                .read_to_end(&mut bytes)
                .map_err(|_| io::Error::other("secret file unavailable"))?;
            bytes
        }
        (None, None) => return Ok(None),
        _ => unreachable!(),
    };
    if bytes.is_empty() || bytes.len() as u64 > limit || bytes.contains(&0) {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid secret"));
    }
    String::from_utf8(bytes)
        .map(Some)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid secret"))
}

fn build_integrations(raw: &str) -> Result<HashMap<String, Fcm>, Box<dyn Error + Send + Sync>> {
    let settings: HashMap<String, IntegrationConfig> =
        serde_json::from_str(raw).map_err(|_| "invalid NOVU_FCM_INTEGRATIONS configuration")?;
    if settings.is_empty() {
        return Err("NOVU_FCM_INTEGRATIONS must not be empty".into());
    }
    let mut providers = HashMap::new();
    for (id, config) in settings {
        if id.is_empty() {
            return Err("FCM integration ID must not be empty".into());
        }
        let provider = match (config.access_token, config.client_email, config.private_key) {
            (Some(token), None, None) => Fcm::new(&config.project_id, token)?,
            (None, Some(email), Some(key)) => {
                Fcm::with_service_account(&config.project_id, email, &key)?
            }
            _ => return Err("invalid FCM integration authentication configuration".into()),
        };
        providers.insert(id, provider);
    }
    Ok(providers)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let environment = std::env::var("NOVU_FCM_ENVIRONMENT_ID")?;
    if environment.is_empty() {
        return Err("NOVU_FCM_ENVIRONMENT_ID must not be empty".into());
    }
    let shared_provider = match std::env::var("SWALLOW_FCM_ALLOW_SHARED_PROVIDER").as_deref() {
        Ok("true") => true,
        Ok("false") | Err(std::env::VarError::NotPresent) => false,
        _ => return Err("invalid shared FCM provider setting".into()),
    };
    let (provider, integrations) = if let Some(raw) = multiline_secret(
        "NOVU_FCM_INTEGRATIONS",
        "NOVU_FCM_INTEGRATIONS_FILE",
        1_048_576,
    )? {
        (None, Some(build_integrations(&raw)?))
    } else {
        let project = std::env::var("NOVU_FCM_PROJECT_ID")?;
        let provider = if let Some(token) = novu_rust_contracts::secret::from_env(
            "NOVU_FCM_ACCESS_TOKEN",
            "NOVU_FCM_ACCESS_TOKEN_FILE",
        )? {
            Fcm::new(&project, token)?
        } else {
            Fcm::with_service_account(
                &project,
                std::env::var("NOVU_FCM_CLIENT_EMAIL")?,
                &multiline_secret("NOVU_FCM_PRIVATE_KEY", "NOVU_FCM_PRIVATE_KEY_FILE", 65_536)?
                    .ok_or("FCM private key required")?,
            )?
        };
        (Some(provider), None)
    };
    if shared_provider && integrations.is_some() {
        return Err("shared FCM provider cannot be combined with an integration map".into());
    }
    let enforce_routing = match std::env::var("SWALLOW_FCM_ENFORCE_ROUTING_CONFIG").as_deref() {
        Ok("true") => true,
        Ok("false") | Err(std::env::VarError::NotPresent) => false,
        _ => return Err("invalid FCM routing guard setting".into()),
    };
    let routing_hash = if enforce_routing {
        let (mode, projects) = if let Some(integrations) = &integrations {
            (
                "mapped",
                integrations
                    .iter()
                    .map(|(id, fcm)| (id.clone(), fcm.project_id().to_owned()))
                    .collect::<BTreeMap<_, _>>(),
            )
        } else {
            (
                if shared_provider { "shared" } else { "single" },
                BTreeMap::from([(
                    String::new(),
                    provider.as_ref().unwrap().project_id().to_owned(),
                )]),
            )
        };
        let value = serde_json::to_vec(&serde_json::json!({"mode":mode,"projects":projects}))?;
        Some(
            ring::digest::digest(&ring::digest::SHA256, &value)
                .as_ref()
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>(),
        )
    } else {
        None
    };
    let (config, tls) = novu_rust_contracts::postgres::configure(&std::env::var("DATABASE_URL")?)?;
    let timeout = match std::env::var("NOVU_FCM_DB_TIMEOUT_SECONDS") {
        Ok(value) => value
            .parse::<u64>()
            .ok()
            .filter(|n| (1..=300).contains(n))
            .ok_or("invalid FCM database timeout")?,
        Err(std::env::VarError::NotPresent) => 30,
        Err(_) => return Err("invalid FCM database timeout".into()),
    };
    let timeout = Duration::from_secs(timeout);
    loop {
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
            if configured && let Some(hash) = &routing_hash {
                match tokio::time::timeout(
                    timeout,
                    db.query_one(
                        "SELECT public.novu_rust_claim_fcm_routing($1,$2)",
                        &[&environment, hash],
                    ),
                )
                .await
                {
                    Ok(Ok(row)) if row.get::<_, bool>(0) => {}
                    Ok(Ok(_)) => {
                        task.abort();
                        let _ = task.await;
                        return Err("FCM workers have different routing configurations".into());
                    }
                    Ok(Err(error)) if error.as_db_error().is_some() => {
                        task.abort();
                        let _ = task.await;
                        return Err("FCM routing guard rejected database configuration".into());
                    }
                    _ => {
                        task.abort();
                        let _ = task.await;
                        eprintln!("FCM routing guard connection failed; reconnecting");
                        tokio::time::sleep(Duration::from_secs(2)).await;
                        continue;
                    }
                }
            }
            let mut recovery = tokio::time::Instant::now();
            if configured {
                loop {
                    if tokio::time::Instant::now() >= recovery {
                        if !matches!(
                            tokio::time::timeout(
                                timeout,
                                fcm_queue::recover_expired(&mut db, &environment)
                            )
                            .await,
                            Ok(Ok(_))
                        ) {
                            break;
                        }
                        recovery = tokio::time::Instant::now() + Duration::from_secs(5);
                    }
                    let dispatch = async {
                        if let Some(integrations) = integrations.as_ref() {
                            fcm_queue::process_one_with_integrations(
                                &mut db,
                                &environment,
                                integrations,
                            )
                            .await
                        } else if shared_provider {
                            fcm_queue::process_one_with_shared_provider(
                                &mut db,
                                &environment,
                                provider.as_ref().expect("single provider configured"),
                            )
                            .await
                        } else {
                            fcm_queue::process_one(
                                &mut db,
                                &environment,
                                provider.as_ref().expect("single provider configured"),
                            )
                            .await
                        }
                    };
                    match tokio::time::timeout(timeout, dispatch).await {
                        Ok(Ok(true)) => {}
                        Ok(Ok(false)) => tokio::time::sleep(Duration::from_secs(1)).await,
                        _ => break,
                    }
                }
            }
            // Dropping the client alone can leave the driver running while a
            // cancelled transaction waits on the server. Abort it before retry.
            task.abort();
            let _ = task.await;
        }
        eprintln!("FCM worker database operation stopped; reconnecting");
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::build_integrations;

    #[tokio::test]
    async fn validates_integration_config_without_echoing_secrets() {
        let good = r#"{"first":{"project_id":"demo-a","access_token":"test-secret-a"},"second":{"project_id":"demo-b","access_token":"test-secret-b"}}"#;
        let providers = build_integrations(good).unwrap();
        assert!(providers.contains_key("first") && providers.contains_key("second"));
        for bad in [
            "{}",
            r#"{"":{"project_id":"demo","access_token":"test-secret"}}"#,
            r#"{"x":{"project_id":"demo","access_token":"test-secret","private_key":"test-secret"}}"#,
            r#"{"x":{"project_id":"demo","access_token":"test-secret","unknown":"test-secret"}}"#,
        ] {
            let error = build_integrations(bad)
                .err()
                .expect("invalid configuration")
                .to_string();
            assert!(!error.contains("test-secret"));
        }
    }
}
