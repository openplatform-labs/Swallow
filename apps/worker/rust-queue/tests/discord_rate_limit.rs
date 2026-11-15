use novu_postgres_queue::{self as queue, discord::Webhook, discord_queue};
use serde_json::json;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use tokio_postgres::NoTls;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires disposable PostgreSQL and test-mocks"]
async fn fractional_rate_limit_guidance_is_persisted_without_retry() {
    let (mut db, driver) =
        tokio_postgres::connect(&std::env::var("TEST_DATABASE_URL").unwrap(), NoTls)
            .await
            .unwrap();
    tokio::spawn(driver);
    db.batch_execute("CREATE TABLE novu_rust_environments(id text PRIMARY KEY); INSERT INTO novu_rust_environments VALUES('env')").await.unwrap();
    db.batch_execute(queue::MIGRATION).await.unwrap();
    db.batch_execute(discord_queue::MIGRATION).await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/webhook", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 4096];
        let _ = socket.read(&mut buf).await.unwrap();
        let body = r#"{"retry_after":1.25,"global":false}"#;
        socket.write_all(format!("HTTP/1.1 429 Too Many Requests\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
    });
    let id = Uuid::new_v4();
    queue::enqueue(
        &db,
        id,
        "env",
        discord_queue::JOB_KIND,
        &json!({"destination":"one","content":"test"}),
    )
    .await
    .unwrap();
    assert!(
        discord_queue::process_one(
            &mut db,
            "env",
            "one",
            &Webhook::test_endpoint(&url).unwrap()
        )
        .await
        .unwrap()
    );
    server.await.unwrap();
    let row=db.query_one("SELECT j.status,a.state,a.http_status,a.retry_after_seconds FROM novu_queue_jobs j JOIN novu_rust_discord_attempts a ON a.job_id=j.id WHERE j.id=$1",&[&id]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "failed");
    assert_eq!(row.get::<_, String>(1), "rejected");
    assert_eq!(row.get::<_, Option<i32>>(2), Some(429));
    assert_eq!(row.get::<_, Option<i32>>(3), Some(2));
    assert!(
        !discord_queue::process_one(
            &mut db,
            "env",
            "one",
            &Webhook::test_endpoint(&url).unwrap()
        )
        .await
        .unwrap()
    );
}
