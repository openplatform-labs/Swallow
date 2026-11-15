//! A real alert process with a restricted DB login and a loopback health fixture.
use std::{sync::{Arc, atomic::{AtomicUsize, Ordering}}, time::Duration};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpListener, process::Command};
use tokio_postgres::NoTls;
use uuid::Uuid;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a fully migrated disposable PostgreSQL database and role creation"]
async fn key_probe_triggers_coordinated_alert_and_recovery() {
    let database = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, driver) = tokio_postgres::connect(&database, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    let suffix = Uuid::new_v4().simple().to_string();
    let environment = format!("key-probe-{suffix}");
    let role = format!("key_alert_{suffix}");
    db.execute("INSERT INTO novu_rust_environments VALUES($1,'org')", &[&environment]).await.unwrap();
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_reserve_alert(text,text,text,integer,integer),public.novu_rust_finish_alert(text,text,text,uuid,text,integer),public.novu_rust_observe_alert_conditions(text,text,text,integer,integer) TO {role}")).await.unwrap();
    db.execute("INSERT INTO novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT $1,$2,'alert_send',oid FROM pg_roles WHERE rolname::text=$2", &[&environment,&role]).await.unwrap();
    let mut role_url = reqwest::Url::parse(&database).unwrap(); role_url.set_username(&role).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address=listener.local_addr().unwrap();
    let state=Arc::new(AtomicUsize::new(0)); let serving=state.clone();
    let server=tokio::spawn(async move { loop {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mode=serving.load(Ordering::SeqCst);
        tokio::spawn(async move {
            let mut request=[0u8; 2048]; let _=socket.read(&mut request).await;
            let (status, body) = match mode {
                0 => ("200 OK", "{\"status\":\"healthy\"}".to_string()),
                1 => ("503 Service Unavailable", "{\"status\":\"unhealthy\"}".to_string()),
                2 => ("200 OK", "{\"status\":\"wrong\"}".to_string()),
                3 => ("302 Found", "redirect".to_string()),
                _ => ("200 OK", "x".repeat(4097)),
            };
            let response=format!("HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len());
            let _=socket.write_all(response.as_bytes()).await;
        });
    }});
    let probe = format!("http://{address}/health/operator-keys");
    let run = |url: &str, add_db_probe: bool| {
        let mut cmd=Command::new(env!("CARGO_BIN_EXE_send-database-alert"));
        cmd.env_clear().args([&environment,"one","60"])
            .env("DATABASE_URL",role_url.as_str()).env("NOVU_POSTGRES_ALLOW_INSECURE","true")
            .env("SWALLOW_OPERATOR_KEY_PROBE_URL",url)
            .env("SWALLOW_ALERT_SLACK_WEBHOOK","https://hooks.slack.com/services/FAKE/FAKE/FAKE")
            .env("HTTPS_PROXY","http://127.0.0.1:1").env("NO_PROXY","127.0.0.1,localhost")
            .env("SWALLOW_ALERT_SCOPED_DELIVERY","true").env("SWALLOW_ALERT_COOLDOWN_POLICY","new-condition")
            .env("SWALLOW_DB_ALERT_OPERATION_TIMEOUT_SECONDS","3");
        if add_db_probe { cmd.env("SWALLOW_DB_PROBE_URL", &database); }
        cmd
    };
    let healthy=tokio::time::timeout(Duration::from_secs(10), run(&probe,false).output()).await.unwrap().unwrap();
    assert_eq!(healthy.status.code(),Some(0),"{}",String::from_utf8_lossy(&healthy.stderr));
    assert_eq!(run(&probe,true).output().await.unwrap().status.code(),Some(1));
    assert_eq!(run("http://example.test/health/operator-keys",false).output().await.unwrap().status.code(),Some(1));
    state.store(1,Ordering::SeqCst);
    let bad=tokio::time::timeout(Duration::from_secs(10),run(&probe,false).output()).await.unwrap().unwrap();
    assert_eq!(bad.status.code(),Some(2),"{}",String::from_utf8_lossy(&bad.stderr));
    let row=db.query_one("SELECT state,last_condition_mask FROM novu_rust_alert_delivery_slots WHERE environment_id=$1 AND channel='slack' AND target_id='key-one'", &[&environment]).await.unwrap();
    assert_eq!(row.get::<_,String>(0),"unknown"); assert_eq!(row.get::<_,i32>(1) & (1<<20),1<<20);
    state.store(2,Ordering::SeqCst);
    assert_eq!(run(&probe,false).output().await.unwrap().status.code(),Some(2));
    for mode in [3, 4] {
        state.store(mode,Ordering::SeqCst);
        assert_eq!(run(&probe,false).output().await.unwrap().status.code(),Some(2));
    }
    let attempts:i64=db.query_one("SELECT count(*) FROM novu_rust_alert_delivery_attempts WHERE environment_id=$1 AND target_id='key-one'", &[&environment]).await.unwrap().get(0);
    assert_eq!(attempts,1);
    state.store(0,Ordering::SeqCst);
    assert_eq!(run(&probe,false).output().await.unwrap().status.code(),Some(0));
    let mask:i32=db.query_one("SELECT last_condition_mask FROM novu_rust_alert_delivery_slots WHERE environment_id=$1 AND target_id='key-one'", &[&environment]).await.unwrap().get(0);
    assert_eq!(mask & (1<<20),1<<20, "unknown delivery must remain pending review");
    // Fixture-only transition represents an independently reviewed accepted attempt.
    db.execute("UPDATE novu_rust_alert_delivery_slots SET state='accepted' WHERE environment_id=$1 AND target_id='key-one'", &[&environment]).await.unwrap();
    assert_eq!(run(&probe,false).output().await.unwrap().status.code(),Some(0));
    let recovered:i32=db.query_one("SELECT last_condition_mask FROM novu_rust_alert_delivery_slots WHERE environment_id=$1 AND target_id='key-one'", &[&environment]).await.unwrap().get(0);
    assert_eq!(recovered & (1<<20),0);
    server.abort();
}
