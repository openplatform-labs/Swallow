//! Optional PostgreSQL role checks for an independently stored alert.
use tokio_postgres::{Client, Error};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Any,
    WritablePrimary,
    SynchronousPrimary { min_standbys: i64 },
    Standby,
}

impl Role {
    pub fn from_env() -> Result<Self, &'static str> {
        match std::env::var("SWALLOW_DB_PROBE_ROLE").as_deref() {
            Err(std::env::VarError::NotPresent) | Ok("any") => Ok(Self::Any),
            Ok("writable-primary") => Ok(Self::WritablePrimary),
            Ok("synchronous-primary") => {
                let value = match std::env::var("SWALLOW_DB_PROBE_MIN_SYNC_STANDBYS") {
                    Ok(value) => value,
                    Err(std::env::VarError::NotPresent) => "1".into(),
                    Err(_) => return Err("invalid minimum synchronous standbys"),
                };
                let min_standbys = value
                    .parse::<i64>()
                    .map_err(|_| "invalid minimum synchronous standbys")?;
                if !(1..=32).contains(&min_standbys) {
                    return Err("invalid minimum synchronous standbys");
                }
                Ok(Self::SynchronousPrimary { min_standbys })
            }
            Ok("standby") => Ok(Self::Standby),
            _ => Err("invalid database probe role"),
        }
    }
}

/// This is a point-in-time check of this connection, not a fencing guarantee.
pub async fn healthy(client: &Client, role: Role) -> Result<bool, Error> {
    if let Role::SynchronousPrimary { min_standbys } = role {
        let row=client.query_one("SELECT NOT pg_catalog.pg_is_in_recovery() AND NOT current_setting('transaction_read_only')::boolean AND current_setting('synchronous_commit') IN ('on','remote_apply') AND current_setting('synchronous_standby_names')<>'' AND (pg_catalog.pg_has_role(current_user,'pg_read_all_stats','MEMBER') OR (SELECT rolsuper FROM pg_catalog.pg_roles WHERE rolname=current_user)) AND (SELECT count(*) FROM pg_catalog.pg_stat_replication r WHERE r.state='streaming' AND r.sync_state IN ('sync','quorum') AND r.flush_lsn IS NOT NULL AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_replication_slots s WHERE s.active_pid=r.pid AND s.slot_type='logical'))>=$1",&[&min_standbys]).await?;
        return Ok(row.get(0));
    }
    let row = client.query_one("SELECT pg_catalog.pg_is_in_recovery(), current_setting('transaction_read_only')::boolean", &[]).await?;
    let recovery: bool = row.get(0);
    let read_only: bool = row.get(1);
    Ok(match role {
        Role::Any => true,
        Role::WritablePrimary => !recovery && !read_only,
        Role::SynchronousPrimary { .. } => unreachable!(),
        Role::Standby => recovery,
    })
}
