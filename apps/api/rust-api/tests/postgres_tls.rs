//! Run only against the disposable servers created by test-postgres-tls.sh.
#[tokio::test]
#[ignore = "requires disposable PostgreSQL TLS fixture"]
async fn verifies_postgres_transport() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let expected = std::env::var("TEST_POSTGRES_TLS_EXPECT").unwrap();
    let (config, tls) = novu_rust_contracts::postgres::configure(&url).unwrap();
    let result = config.connect(tls).await;
    if expected == "reject" {
        assert!(result.is_err(), "untrusted, wrong-host or plaintext server was accepted");
        return;
    }
    let (client, connection) = result.expect("expected database connection");
    tokio::spawn(async move { connection.await.unwrap() });
    let encrypted: bool = client.query_one("SELECT ssl FROM pg_stat_ssl WHERE pid=pg_backend_pid()", &[]).await.unwrap().get(0);
    assert_eq!(encrypted, expected == "tls");
}
