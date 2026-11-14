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
    primary
        .batch_execute("SET default_transaction_read_only = on")
        .await
        .unwrap();
    assert!(!healthy(&primary, Role::WritablePrimary).await.unwrap());
    assert!(healthy(&primary, Role::Any).await.unwrap());
}
