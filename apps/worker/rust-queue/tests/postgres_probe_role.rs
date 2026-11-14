//! Run against a disposable primary and physical standby before failover.
use novu_postgres_queue::db_probe::{Role, healthy};
use std::{
    process::{Child, Command, Stdio},
    time::Duration,
};
use tokio_postgres::{Client, NoTls};

struct LogicalReceiver(Child);
impl Drop for LogicalReceiver {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

async fn connect(url: &str) -> Client {
    let (db, driver) = tokio_postgres::connect(url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    db
}

#[tokio::test]
#[ignore = "requires a disposable primary and streaming standby"]
async fn detects_actual_primary_standby_and_session_read_only() {
    let primary = connect(&std::env::var("TEST_PRIMARY_URL").unwrap()).await;
    let standby = connect(&std::env::var("TEST_STANDBY_URL").unwrap()).await;
    for db in [&primary, &standby] {
        assert!(healthy(db, Role::Any).await.unwrap());
    }
    assert!(healthy(&primary, Role::WritablePrimary).await.unwrap());
    assert!(!healthy(&primary, Role::Standby).await.unwrap());
    assert!(!healthy(&standby, Role::WritablePrimary).await.unwrap());
    assert!(healthy(&standby, Role::Standby).await.unwrap());
    assert!(
        healthy(
            &primary,
            Role::SynchronousPrimary {
                min_standbys: 1,
                max_replay_lag_bytes: None
            }
        )
        .await
        .unwrap()
    );
    assert!(
        !healthy(
            &primary,
            Role::SynchronousPrimary {
                min_standbys: 2,
                max_replay_lag_bytes: None
            }
        )
        .await
        .unwrap()
    );
    assert!(
        !healthy(
            &standby,
            Role::SynchronousPrimary {
                min_standbys: 1,
                max_replay_lag_bytes: None
            }
        )
        .await
        .unwrap()
    );
    primary
        .batch_execute("SET synchronous_commit=local")
        .await
        .unwrap();
    assert!(
        !healthy(
            &primary,
            Role::SynchronousPrimary {
                min_standbys: 1,
                max_replay_lag_bytes: None
            }
        )
        .await
        .unwrap()
    );
    primary
        .batch_execute("RESET synchronous_commit; CREATE ROLE fixture_probe LOGIN")
        .await
        .unwrap();
    let probe_url =
        std::env::var("TEST_PRIMARY_URL")
            .unwrap()
            .replacen("postgres@", "fixture_probe@", 1);
    let limited = connect(&probe_url).await;
    assert!(
        !healthy(
            &limited,
            Role::SynchronousPrimary {
                min_standbys: 1,
                max_replay_lag_bytes: None
            }
        )
        .await
        .unwrap()
    );
    primary
        .batch_execute("GRANT pg_read_all_stats TO fixture_probe")
        .await
        .unwrap();
    assert!(
        healthy(
            &limited,
            Role::SynchronousPrimary {
                min_standbys: 1,
                max_replay_lag_bytes: None
            }
        )
        .await
        .unwrap()
    );
    assert!(
        !healthy(
            &standby,
            Role::SynchronousPrimary {
                min_standbys: 1,
                max_replay_lag_bytes: Some(0)
            }
        )
        .await
        .unwrap()
    );
    // Pause replay while still allowing WAL reception and flush, then write
    // asynchronously so the primary does not wait on the paused standby.
    standby
        .query_one("SELECT pg_catalog.pg_wal_replay_pause()", &[])
        .await
        .unwrap();
    for _ in 0..100 {
        if standby
            .query_one("SELECT pg_catalog.pg_is_wal_replay_paused()", &[])
            .await
            .unwrap()
            .get::<_, bool>(0)
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let writer = connect(&std::env::var("TEST_PRIMARY_URL").unwrap()).await;
    writer
        .batch_execute("SET synchronous_commit=off")
        .await
        .unwrap();
    writer
        .query_one(
            "SELECT pg_catalog.pg_logical_emit_message(true,'probe',repeat('x',200000))",
            &[],
        )
        .await
        .unwrap();
    let mut lagged = false;
    for _ in 0..100 {
        let row=primary.query_one("SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_stat_replication WHERE application_name='standby1' AND replay_lsn IS NOT NULL AND pg_catalog.pg_wal_lsn_diff(pg_catalog.pg_current_wal_lsn(),replay_lsn)>1000)",&[]).await.unwrap();
        if row.get::<_, bool>(0) {
            lagged = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(lagged, "standby replay lag did not appear");
    assert!(
        healthy(
            &limited,
            Role::SynchronousPrimary {
                min_standbys: 1,
                max_replay_lag_bytes: None
            }
        )
        .await
        .unwrap()
    );
    assert!(
        !healthy(
            &limited,
            Role::SynchronousPrimary {
                min_standbys: 1,
                max_replay_lag_bytes: Some(1000)
            }
        )
        .await
        .unwrap()
    );
    assert!(
        healthy(
            &limited,
            Role::SynchronousPrimary {
                min_standbys: 1,
                max_replay_lag_bytes: Some(10_000_000)
            }
        )
        .await
        .unwrap()
    );
    standby
        .query_one("SELECT pg_catalog.pg_wal_replay_resume()", &[])
        .await
        .unwrap();
    // Make one physical standby and one logical receiver synchronous quorum
    // members. A logical subscriber is not a promotable physical standby.
    primary.query_one("SELECT * FROM pg_catalog.pg_create_logical_replication_slot('fixture_probe_slot','test_decoding')",&[]).await.unwrap();
    let container = std::env::var("TEST_PRIMARY_CONTAINER").unwrap();
    let _receiver = LogicalReceiver(
        Command::new("docker")
            .args([
                "exec",
                "-u",
                "postgres",
                &container,
                "pg_recvlogical",
                "-d",
                "host=127.0.0.1 dbname=postgres user=postgres application_name=logical_probe",
                "--slot=fixture_probe_slot",
                "--start",
                "--status-interval=1",
                "-f",
                "-",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    primary
        .batch_execute(
            "ALTER SYSTEM SET synchronous_standby_names='ANY 1 (standby1,logical_probe)'",
        )
        .await
        .unwrap();
    primary
        .query_one("SELECT pg_catalog.pg_reload_conf()", &[])
        .await
        .unwrap();
    let mut ready = false;
    for _ in 0..100 {
        let row=primary.query_one("SELECT count(*) FROM pg_catalog.pg_stat_replication WHERE application_name IN ('standby1','logical_probe') AND state='streaming' AND sync_state='quorum' AND flush_lsn IS NOT NULL",&[]).await.unwrap();
        if row.get::<_, i64>(0) == 2 {
            ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        ready,
        "physical and logical senders did not become synchronous quorum members"
    );
    assert!(
        healthy(
            &limited,
            Role::SynchronousPrimary {
                min_standbys: 1,
                max_replay_lag_bytes: None
            }
        )
        .await
        .unwrap()
    );
    assert!(
        !healthy(
            &limited,
            Role::SynchronousPrimary {
                min_standbys: 2,
                max_replay_lag_bytes: None
            }
        )
        .await
        .unwrap(),
        "logical receiver counted as a physical standby"
    );
    primary
        .batch_execute("ALTER SYSTEM SET synchronous_standby_names='FIRST 1 (standby1)'")
        .await
        .unwrap();
    primary
        .query_one("SELECT pg_catalog.pg_reload_conf()", &[])
        .await
        .unwrap();
    let mut restored = false;
    for _ in 0..100 {
        if healthy(
            &limited,
            Role::SynchronousPrimary {
                min_standbys: 1,
                max_replay_lag_bytes: None,
            },
        )
        .await
        .unwrap()
        {
            restored = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        restored,
        "physical synchronous standby did not return after configuration reset"
    );
    primary.query("SELECT pg_catalog.pg_terminate_backend(pid) FROM pg_catalog.pg_stat_replication WHERE application_name='logical_probe'",&[]).await.unwrap();
    drop(_receiver);
    primary
        .query_one(
            "SELECT pg_catalog.pg_drop_replication_slot('fixture_probe_slot')",
            &[],
        )
        .await
        .unwrap();
    primary
        .batch_execute("REVOKE pg_read_all_stats FROM fixture_probe")
        .await
        .unwrap();
    assert!(
        !healthy(
            &limited,
            Role::SynchronousPrimary {
                min_standbys: 1,
                max_replay_lag_bytes: None
            }
        )
        .await
        .unwrap()
    );
    primary
        .batch_execute("SET default_transaction_read_only = on")
        .await
        .unwrap();
    assert!(!healthy(&primary, Role::WritablePrimary).await.unwrap());
    assert!(
        !healthy(
            &primary,
            Role::SynchronousPrimary {
                min_standbys: 1,
                max_replay_lag_bytes: None
            }
        )
        .await
        .unwrap()
    );
    assert!(healthy(&primary, Role::Any).await.unwrap());
}
