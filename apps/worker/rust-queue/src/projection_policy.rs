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
