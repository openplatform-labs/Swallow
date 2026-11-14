//! Experimental Slack workflow worker; use a process supervisor and external monitoring.
use base64::{Engine, engine::general_purpose::STANDARD};
use novu_postgres_queue::{
    slack::Slack,
    slack_workflow::{self, Providers},
};
use std::collections::BTreeMap;
use std::{error::Error, time::Duration};
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let step_policy = slack_workflow::StepPolicy::from_env()?;
    let endpoint_policy = slack_workflow::EndpointPolicy::from_env()?;
    let environment = std::env::var("NOVU_SLACK_ENVIRONMENT_ID")?;
    if environment.trim().is_empty() {
        return Err("Slack environment is required".into());
    }
    let raw = novu_rust_contracts::secret::from_env(
        "NOVU_SLACK_WORKFLOW_TOKENS",
        "NOVU_SLACK_WORKFLOW_TOKENS_FILE",
    )?
    .ok_or("Slack workflow tokens required")?;
    let config: BTreeMap<String, BTreeMap<String, String>> =
        serde_json::from_str(&raw).map_err(|_| "invalid Slack workflow token map")?;
    let fingerprint = STANDARD.encode(
        ring::digest::digest(&ring::digest::SHA256, &serde_json::to_vec(&config)?).as_ref(),
    );
    let mut providers = Providers::new();
    if config.is_empty() {
        return Err("Slack workflow token map is empty".into());
    }
    for (integration, connections) in config {
        if integration.is_empty() || connections.is_empty() {
            return Err("invalid Slack workflow token map".into());
        }
        for (connection, token) in connections {
            providers.insert((integration.clone(), connection), Slack::new(token)?);
        }
    }
    let (config, tls) = novu_rust_contracts::postgres::configure(&std::env::var("DATABASE_URL")?)?;
    let timeout = match std::env::var("NOVU_SLACK_WORKFLOW_DB_TIMEOUT_SECONDS") {
        Ok(value) => value
            .parse::<u64>()
            .ok()
            .filter(|n| (1..=300).contains(n))
            .ok_or("invalid Slack database timeout")?,
        Err(std::env::VarError::NotPresent) => 30,
        Err(_) => return Err("invalid Slack database timeout".into()),
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
            if configured {
                match tokio::time::timeout(timeout,db.query_opt("INSERT INTO novu_rust_slack_workflow_worker_config(environment_id,credential_map_hash,step_policy,endpoint_policy) VALUES ($1,$2,$3,$4) ON CONFLICT(environment_id) DO UPDATE SET credential_map_hash=EXCLUDED.credential_map_hash,step_policy=EXCLUDED.step_policy,endpoint_policy=EXCLUDED.endpoint_policy WHERE novu_rust_slack_workflow_worker_config.credential_map_hash=EXCLUDED.credential_map_hash AND (novu_rust_slack_workflow_worker_config.step_policy IS NULL OR novu_rust_slack_workflow_worker_config.step_policy=EXCLUDED.step_policy) AND (novu_rust_slack_workflow_worker_config.endpoint_policy IS NULL OR novu_rust_slack_workflow_worker_config.endpoint_policy=EXCLUDED.endpoint_policy) RETURNING 1",&[&environment,&fingerprint,&step_policy.as_str(),&endpoint_policy.as_str()])).await {
                    Ok(Ok(Some(_)))=>{},
                    Ok(Ok(None))=>{task.abort();return Err("Slack workflow workers have different credential maps or workflow policies".into());},
                    _=>{task.abort();let _=task.await;eprintln!("Slack workflow configuration registration failed; reconnecting");tokio::time::sleep(Duration::from_secs(2)).await;continue;}
                }
            }
            let mut recovery = tokio::time::Instant::now();
            if configured {
                loop {
                    if tokio::time::Instant::now() >= recovery {
                        if !matches!(
                            tokio::time::timeout(
                                timeout,
                                slack_workflow::recover_expired(&mut db, &environment)
                            )
                            .await,
                            Ok(Ok(_))
                        ) {
                            break;
                        }
                        recovery = tokio::time::Instant::now() + Duration::from_secs(5);
                    }
                    let fanout = tokio::time::timeout(
                        timeout,
                        slack_workflow::fanout_one_with_policies(
                            &mut db,
                            &environment,
                            step_policy,
                            endpoint_policy,
                        ),
                    )
                    .await;
                    if !matches!(fanout, Ok(Ok(_))) {
                        break;
                    }
                    let delivery = tokio::time::timeout(
                        timeout,
                        slack_workflow::delivery_one_with_policy(
                            &mut db,
                            &environment,
                            &providers,
                            step_policy,
                        ),
                    )
                    .await;
                    match delivery {
                        Ok(Ok(processed)) => {
                            if !processed && matches!(fanout, Ok(Ok(false))) {
                                tokio::time::sleep(Duration::from_secs(1)).await;
                            }
                        }
                        _ => break,
                    }
                }
            }
            // Dropping the client alone can leave the driver running while a
            // cancelled transaction waits on the server. Abort it before retry.
            task.abort();
            let _ = task.await;
        }
        eprintln!("Slack worker database operation stopped; reconnecting");
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}
