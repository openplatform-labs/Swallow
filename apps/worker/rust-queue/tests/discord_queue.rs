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
async fn dispatch_and_recovery_are_scoped_and_fenced() {
    let (mut db, connection) =
        tokio_postgres::connect(&std::env::var("TEST_DATABASE_URL").unwrap(), NoTls)
            .await
            .unwrap();
    tokio::spawn(connection);
    db.batch_execute("CREATE TABLE novu_rust_environments(id text PRIMARY KEY); INSERT INTO novu_rust_environments VALUES ('a'),('b')").await.unwrap();
    db.batch_execute(queue::MIGRATION).await.unwrap();
    db.batch_execute(discord_queue::MIGRATION).await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!(
        "http://{}/api/webhooks/1/secret",
        listener.local_addr().unwrap()
    );
    let server = tokio::spawn(async move {
        for response in [
            Some(
                "HTTP/1.1 200 OK\r\nContent-Length: 36\r\nConnection: close\r\n\r\n{\"id\":\"1\",\"timestamp\":\"2026-01-01Z\"}",
            ),
            None,
        ] {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            let mut buffer = [0; 4096];
            loop {
                let n = socket.read(&mut buffer).await.unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&buffer[..n]);
                if let Some(pos) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&bytes[..pos]);
                    let length: usize = headers
                        .lines()
                        .find_map(|l| {
                            l.to_ascii_lowercase()
                                .strip_prefix("content-length: ")
                                .and_then(|v| v.parse().ok())
                        })
                        .unwrap();
                    if bytes.len() >= pos + 4 + length {
                        break;
                    }
                }
            }
            assert!(String::from_utf8_lossy(&bytes).contains("wait=true"));
            if let Some(response) = response {
                socket.write_all(response.as_bytes()).await.unwrap();
            }
        }
    });
    let provider = Webhook::test_endpoint(&url).unwrap();
    let ids: Vec<Uuid> = (0..6).map(|_| Uuid::new_v4()).collect();
    for (id, env, destination) in [
        (ids[0], "a", "one"),
        (ids[1], "a", "one"),
        (ids[2], "a", "two"),
        (ids[3], "b", "one"),
        (ids[4], "a", "one"),
        (ids[5], "a", "one"),
    ] {
        queue::enqueue(
            &db,
            id,
            env,
            discord_queue::JOB_KIND,
            &json!({"destination":destination,"content":"test"}),
        )
        .await
        .unwrap();
    }
    // Hold recovery fixtures so that they cannot be selected for dispatch.
    db.execute("UPDATE novu_queue_jobs SET status='running',lease_token=$2,lease_until=clock_timestamp()-interval '1 second' WHERE id=ANY($1)", &[&vec![ids[4],ids[5]],&Uuid::new_v4()]).await.unwrap();
    db.execute("INSERT INTO novu_rust_discord_attempts(job_id,environment_id,state) VALUES($1,'a','dispatching')", &[&ids[5]]).await.unwrap();
    assert!(
        discord_queue::process_one(&mut db, "a", "one", &provider)
            .await
            .unwrap()
    );
    assert!(
        discord_queue::process_one(&mut db, "a", "one", &provider)
            .await
            .unwrap()
    );
    server.await.unwrap();
    let rows=db.query("SELECT j.id,j.status,a.state FROM novu_queue_jobs j LEFT JOIN novu_rust_discord_attempts a ON a.job_id=j.id WHERE j.id=ANY($1)", &[&ids]).await.unwrap();
    let get = |id| {
        let row = rows.iter().find(|r| r.get::<_, Uuid>(0) == id).unwrap();
        (row.get::<_, String>(1), row.get::<_, Option<String>>(2))
    };
    // Selection order is created_at then UUID, so check aggregate outcomes rather than IDs.
    let states: Vec<_> = [get(ids[0]), get(ids[1])]
        .into_iter()
        .map(|v| v.1.unwrap())
        .collect();
    assert!(states.contains(&"accepted".into()) && states.contains(&"unknown".into()));
    assert_eq!(get(ids[2]).0, "pending");
    assert_eq!(get(ids[3]).0, "pending");
    assert_eq!(
        discord_queue::recover_expired(&mut db, "a", "one")
            .await
            .unwrap(),
        2
    );
    let rows=db.query("SELECT j.status,a.state FROM novu_queue_jobs j LEFT JOIN novu_rust_discord_attempts a ON a.job_id=j.id WHERE j.id=$1", &[&ids[4]]).await.unwrap();
    assert_eq!(rows[0].get::<_, String>(0), "pending");
    assert_eq!(rows[0].get::<_, Option<String>>(1), None);
    let row=db.query_one("SELECT j.status,a.state FROM novu_queue_jobs j JOIN novu_rust_discord_attempts a ON a.job_id=j.id WHERE j.id=$1", &[&ids[5]]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "failed");
    assert_eq!(row.get::<_, String>(1), "unknown");
    // Exercise the actual worker's recovery without making an external request.
    let process_job = Uuid::new_v4();
    queue::enqueue(
        &db,
        process_job,
        "a",
        discord_queue::JOB_KIND,
        &json!({"destination":"process","content":"test"}),
    )
    .await
    .unwrap();
    db.execute("UPDATE novu_queue_jobs SET status='running',lease_token=$2,lease_until=clock_timestamp()-interval '1 second' WHERE id=$1", &[&process_job,&Uuid::new_v4()]).await.unwrap();
    db.execute("INSERT INTO novu_rust_discord_attempts(job_id,environment_id,state) VALUES($1,'a','dispatching')", &[&process_job]).await.unwrap();
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_discord-webhook-worker"))
        .env("DATABASE_URL", std::env::var("TEST_DATABASE_URL").unwrap())
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .env("NOVU_DISCORD_ENVIRONMENT_ID", "a")
        .env("NOVU_DISCORD_WEBHOOK_DESTINATION_ID", "process")
        .env(
            "NOVU_DISCORD_WEBHOOK_URL",
            "https://discord.com/api/webhooks/123/fixture",
        )
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let observed = tokio::time::timeout(std::time::Duration::from_secs(10),async {
        loop {
            let row=db.query_one("SELECT j.status,a.state FROM novu_queue_jobs j JOIN novu_rust_discord_attempts a ON a.job_id=j.id WHERE j.id=$1", &[&process_job]).await.unwrap();
            if row.get::<_,String>(0)=="failed" && row.get::<_,String>(1)=="unknown" { break; }
            assert!(child.try_wait().unwrap().is_none(),"worker exited prematurely");
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }).await;
    observed.expect("worker did not recover expired dispatch");
    let mut mismatch = tokio::process::Command::new(env!("CARGO_BIN_EXE_discord-webhook-worker"))
        .env("DATABASE_URL", std::env::var("TEST_DATABASE_URL").unwrap())
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .env("NOVU_DISCORD_ENVIRONMENT_ID", "a")
        .env("NOVU_DISCORD_WEBHOOK_DESTINATION_ID", "process")
        .env(
            "NOVU_DISCORD_WEBHOOK_URL",
            "https://discord.com/api/webhooks/123/different",
        )
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let status = tokio::time::timeout(std::time::Duration::from_secs(5), mismatch.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(!status.success(), "worker accepted a conflicting endpoint");
    child.kill().await.unwrap();
}
