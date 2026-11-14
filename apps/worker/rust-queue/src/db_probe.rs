//! Optional PostgreSQL role checks for an independently stored alert.
use tokio_postgres::{Client, Error};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Any,
    WritablePrimary,
    Standby,
}

impl Role {
    pub fn from_env() -> Result<Self, &'static str> {
        match std::env::var("SWALLOW_DB_PROBE_ROLE").as_deref() {
            Err(std::env::VarError::NotPresent) | Ok("any") => Ok(Self::Any),
            Ok("writable-primary") => Ok(Self::WritablePrimary),
            Ok("standby") => Ok(Self::Standby),
            _ => Err("invalid database probe role"),
        }
    }
}

/// This is a point-in-time check of this connection, not a fencing guarantee.
pub async fn healthy(client: &Client, role: Role) -> Result<bool, Error> {
    let row = client.query_one("SELECT pg_catalog.pg_is_in_recovery(), current_setting('transaction_read_only')::boolean", &[]).await?;
    let recovery: bool = row.get(0);
    let read_only: bool = row.get(1);
    Ok(match role {
        Role::Any => true,
        Role::WritablePrimary => !recovery && !read_only,
        Role::Standby => recovery,
    })
}
