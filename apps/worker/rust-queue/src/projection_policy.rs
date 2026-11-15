//! Coordinate selectable projection heartbeat semantics across new workers.
use tokio_postgres::{Client, Error};

pub const MIGRATION: &str = include_str!("../migrations/0151_projection_policy_agreement.sql");

/// Returns false if another worker registered a different policy.
pub async fn register(
    db: &Client,
    environment: &str,
    provider: &str,
    require_full_scan: bool,
) -> Result<bool, Error> {
    Ok(db.query_opt("INSERT INTO public.novu_rust_projection_worker_policy(environment_id,provider,require_full_scan) VALUES ($1,$2,$3) ON CONFLICT (environment_id,provider) DO UPDATE SET require_full_scan=EXCLUDED.require_full_scan WHERE novu_rust_projection_worker_policy.require_full_scan=EXCLUDED.require_full_scan RETURNING 1", &[&environment,&provider,&require_full_scan]).await?.is_some())
}

/// Select the restricted registration function when explicitly enabled.
pub fn scoped_from_env() -> Result<bool, &'static str> {
    match std::env::var("SWALLOW_PROJECTION_SCOPED_POLICY").as_deref() {
        Ok("true") => Ok(true),
        Ok("false") | Err(std::env::VarError::NotPresent) => Ok(false),
        _ => Err("invalid scoped projection policy setting"),
    }
}

pub async fn register_scoped(
    db: &Client,
    environment: &str,
    provider: &str,
    require_full_scan: bool,
) -> Result<bool, Error> {
    Ok(db
        .query_one(
            "SELECT public.novu_rust_register_projection_policy($1,$2,$3)",
            &[&environment, &provider, &require_full_scan],
        )
        .await?
        .get(0))
}

/// Recheck a scoped registration while a connection is running.
pub async fn check_scoped(
    db: &Client,
    environment: &str,
    provider: &str,
    require_full_scan: bool,
) -> Result<bool, Error> {
    Ok(db
        .query_one(
            "SELECT public.novu_rust_check_projection_policy($1,$2,$3)",
            &[&environment, &provider, &require_full_scan],
        )
        .await?
        .get(0))
}

/// Write the scoped heartbeat while holding authorization locks through commit.
pub async fn scoped_heartbeat(
    db: &Client,
    environment: &str,
    provider: &str,
    require_full_scan: bool,
) -> Result<(), Error> {
    db.execute(
        "SELECT public.novu_rust_scoped_projection_heartbeat($1,$2,$3)",
        &[&environment, &provider, &require_full_scan],
    )
    .await?;
    Ok(())
}
