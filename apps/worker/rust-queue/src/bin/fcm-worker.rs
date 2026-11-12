//! Experimental environment-scoped FCM worker; not a full Novu worker.
use novu_postgres_queue::{fcm::Fcm, fcm_queue};
use std::{collections::HashMap, error::Error, time::Duration};

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct IntegrationConfig {
    project_id: String,
    access_token: Option<String>,
    client_email: Option<String>,
    private_key: Option<String>,
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
    let (provider, integrations) = if let Ok(raw) = std::env::var("NOVU_FCM_INTEGRATIONS") {
        (None, Some(build_integrations(&raw)?))
    } else {
        let project = std::env::var("NOVU_FCM_PROJECT_ID")?;
        let provider = if let Ok(token) = std::env::var("NOVU_FCM_ACCESS_TOKEN") {
            Fcm::new(&project, token)?
        } else {
            Fcm::with_service_account(
                &project,
                std::env::var("NOVU_FCM_CLIENT_EMAIL")?,
                &std::env::var("NOVU_FCM_PRIVATE_KEY")?,
            )?
        };
        (Some(provider), None)
    };
    let (config, tls) = novu_rust_contracts::postgres::configure(&std::env::var("DATABASE_URL")?)?;
    let (mut db, connection) = config.connect(tls).await?;
    tokio::spawn(async move {
        if connection.await.is_err() {
            eprintln!("push worker database connection failed");
        }
    });
    let mut next_recovery = tokio::time::Instant::now();
    loop {
        if tokio::time::Instant::now() >= next_recovery {
            fcm_queue::recover_expired(&mut db, &environment).await?;
            next_recovery = tokio::time::Instant::now() + Duration::from_secs(5);
        }
        let processed = if let Some(integrations) = integrations.as_ref() {
            fcm_queue::process_one_with_integrations(&mut db, &environment, integrations).await?
        } else {
            fcm_queue::process_one(
                &mut db,
                &environment,
                provider.as_ref().expect("single provider configured"),
            )
            .await?
        };
        if !processed {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
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
