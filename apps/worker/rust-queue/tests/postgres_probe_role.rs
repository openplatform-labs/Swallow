//! Run against a disposable primary and physical standby before failover.
use novu_postgres_queue::db_probe::{Role, healthy};
use tokio_postgres::{Client, NoTls};

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
        healthy(&primary, Role::SynchronousPrimary { min_standbys: 1 })
            .await
            .unwrap()
    );
    assert!(
        !healthy(&primary, Role::SynchronousPrimary { min_standbys: 2 })
            .await
            .unwrap()
    );
    assert!(
        !healthy(&standby, Role::SynchronousPrimary { min_standbys: 1 })
            .await
            .unwrap()
    );
    primary
        .batch_execute("SET synchronous_commit=local")
        .await
        .unwrap();
    assert!(
        !healthy(&primary, Role::SynchronousPrimary { min_standbys: 1 })
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
        !healthy(&limited, Role::SynchronousPrimary { min_standbys: 1 })
            .await
            .unwrap()
    );
    primary
        .batch_execute("GRANT pg_read_all_stats TO fixture_probe")
        .await
        .unwrap();
    assert!(
        healthy(&limited, Role::SynchronousPrimary { min_standbys: 1 })
            .await
            .unwrap()
    );
    primary
        .batch_execute("REVOKE pg_read_all_stats FROM fixture_probe")
        .await
        .unwrap();
    assert!(
        !healthy(&limited, Role::SynchronousPrimary { min_standbys: 1 })
            .await
            .unwrap()
    );
    primary
        .batch_execute("SET default_transaction_read_only = on")
        .await
        .unwrap();
    assert!(!healthy(&primary, Role::WritablePrimary).await.unwrap());
    assert!(
        !healthy(&primary, Role::SynchronousPrimary { min_standbys: 1 })
            .await
            .unwrap()
    );
    assert!(healthy(&primary, Role::Any).await.unwrap());
}
