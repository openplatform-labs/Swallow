use novu_postgres_queue::{self as queue, discord::Webhook, discord_queue};
use serde_json::json;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use tokio_postgres::NoTls;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires fresh disposable PostgreSQL and test-mocks"]
async fn cooldown_is_opt_in_shared_and_fences_legacy_workers() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (mut db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(driver);
    db.batch_execute("CREATE TABLE novu_rust_environments(id text PRIMARY KEY); INSERT INTO novu_rust_environments VALUES('env')").await.unwrap();
    for migration in [
        queue::MIGRATION,
        discord_queue::MIGRATION,
        discord_queue::RETRY_POLICY_MIGRATION,
        discord_queue::SHARED_COOLDOWN_MIGRATION,
    ] {
        db.batch_execute(migration).await.unwrap();
    }
    db.execute("INSERT INTO public.novu_rust_discord_shared_cooldown(environment_id,destination_id) VALUES('env','shared')", &[]).await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/webhook", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        for (status, body) in [
            (429, r#"{"retry_after":60.5}"#),
            (200, r#"{"id":"123","timestamp":"2026-01-01Z"}"#),
            (200, r#"{"id":"124","timestamp":"2026-01-01Z"}"#),
            (429, "{}"),
        ] {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = [0; 4096];
            stream.read(&mut buf).await.unwrap();
            stream.write_all(format!("HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
        }
    });
    let provider = Webhook::test_endpoint(&endpoint).unwrap();
    let first = Uuid::new_v4();
    queue::enqueue(
        &db,
        first,
        "env",
        discord_queue::JOB_KIND,
        &json!({"destination":"shared","content":"first"}),
    )
    .await
    .unwrap();
    assert!(
        discord_queue::process_one(&mut db, "env", "shared", &provider)
            .await
            .unwrap()
    );
    let r=db.query_one("SELECT not_before > clock_timestamp()+interval '55 seconds' FROM novu_rust_discord_shared_cooldown WHERE environment_id='env' AND destination_id='shared'",&[]).await.unwrap();
    assert!(r.get::<_, bool>(0));
    let next = Uuid::new_v4();
    queue::enqueue(
        &db,
        next,
        "env",
        discord_queue::JOB_KIND,
        &json!({"destination":"shared","content":"next"}),
    )
    .await
    .unwrap();
    // A simulated legacy worker must fail closed when the destination opts in.
    let lease = Uuid::new_v4();
    db.execute("UPDATE novu_queue_jobs SET status='running',lease_token=$2,lease_until=clock_timestamp()+interval '1 minute' WHERE id=$1",&[&next,&lease]).await.unwrap();
    assert!(db.execute("INSERT INTO novu_rust_discord_attempts(job_id,environment_id,state) VALUES($1,'env','dispatching')",&[&next]).await.is_err());
    db.execute(
        "UPDATE novu_queue_jobs SET status='pending',lease_token=NULL,lease_until=NULL WHERE id=$1",
        &[&next],
    )
    .await
    .unwrap();
    let (mut db2, driver2) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(driver2);
    let (a, b) = tokio::join!(
        discord_queue::process_one(&mut db, "env", "shared", &provider),
        discord_queue::process_one(&mut db2, "env", "shared", &provider)
    );
    let a = a.unwrap();
    let b = b.unwrap();
    assert!(a || b);
    let r=db.query_one("SELECT status,attempts,available_at>clock_timestamp(),(SELECT count(*) FROM novu_rust_discord_attempts WHERE job_id=$1) FROM novu_queue_jobs WHERE id=$1",&[&next]).await.unwrap();
    assert_eq!(r.get::<_, String>(0), "pending");
    assert_eq!(r.get::<_, i32>(1), 0);
    assert!(r.get::<_, bool>(2));
    assert_eq!(r.get::<_, i64>(3), 0);
    // An unrelated, non-opted-in destination remains usable.
    let other = Uuid::new_v4();
    queue::enqueue(
        &db,
        other,
        "env",
        discord_queue::JOB_KIND,
        &json!({"destination":"other","content":"other"}),
    )
    .await
    .unwrap();
    assert!(
        discord_queue::process_one(&mut db, "env", "other", &provider)
            .await
            .unwrap()
    );
    db.execute("UPDATE novu_rust_discord_shared_cooldown SET not_before=clock_timestamp()-interval '1 second' WHERE environment_id='env' AND destination_id='shared'",&[]).await.unwrap();
    db.execute(
        "UPDATE novu_queue_jobs SET available_at=clock_timestamp()-interval '1 second' WHERE id=$1",
        &[&next],
    )
    .await
    .unwrap();
    assert!(
        discord_queue::process_one(&mut db, "env", "shared", &provider)
            .await
            .unwrap()
    );
    let r = db
        .query_one("SELECT status FROM novu_queue_jobs WHERE id=$1", &[&next])
        .await
        .unwrap();
    assert_eq!(r.get::<_, String>(0), "completed");
    db.execute("INSERT INTO novu_rust_discord_shared_cooldown(environment_id,destination_id) VALUES('env','missing')", &[]).await.unwrap();
    let missing = Uuid::new_v4();
    queue::enqueue(
        &db,
        missing,
        "env",
        discord_queue::JOB_KIND,
        &json!({"destination":"missing","content":"missing guidance"}),
    )
    .await
    .unwrap();
    assert!(
        discord_queue::process_one(&mut db, "env", "missing", &provider)
            .await
            .unwrap()
    );
    assert!(!db.query_one("SELECT not_before > clock_timestamp() FROM novu_rust_discord_shared_cooldown WHERE environment_id='env' AND destination_id='missing'", &[]).await.unwrap().get::<_, bool>(0));
    server.await.unwrap();
}
