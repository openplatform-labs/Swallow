//! Controlled single-host synchronous promotion with two real API processes.
use sha2::{Digest,Sha256};
use std::{process::{Child,Command,Stdio},time::Duration};
use tokio_postgres::{Client,NoTls};
use serde_json::json;

async fn connect(url:&str)->Client {for _ in 0..100{if let Ok((db,driver))=tokio_postgres::connect(url,NoTls).await{tokio::spawn(driver);return db;}tokio::time::sleep(Duration::from_millis(100)).await;}panic!("database unavailable")}
struct Server(Child);
impl Drop for Server {fn drop(&mut self){let _=self.0.kill();let _=self.0.wait();}}
fn port()->u16 {let socket=std::net::TcpListener::bind("127.0.0.1:0").unwrap();socket.local_addr().unwrap().port()}
fn server(port:u16,url:&str)->Server {Server(Command::new(env!("CARGO_BIN_EXE_novu-rust-api")).env_clear().env("DATABASE_URL",url).env("NOVU_POSTGRES_ALLOW_INSECURE","true").env("LISTEN_ADDR",format!("127.0.0.1:{port}")).env("SWALLOW_API_HEALTH_ENDPOINTS_ENABLED","true").env("SWALLOW_API_DB_HEALTH_INTERVAL_SECONDS","1").env("SWALLOW_API_DB_HEALTH_TIMEOUT_SECONDS","2").stdout(Stdio::null()).stderr(Stdio::null()).spawn().unwrap())}
async fn ready(client:&reqwest::Client,port:u16){for _ in 0..200{if client.get(format!("http://127.0.0.1:{port}/health/ready")).send().await.is_ok_and(|r|r.status().as_u16()==200){return;}tokio::time::sleep(Duration::from_millis(100)).await;}panic!("API did not become ready")}
async fn trigger(client:&reqwest::Client,port:u16,key:&str,txn:&str)->reqwest::Result<reqwest::Response>{client.post(format!("http://127.0.0.1:{port}/v1/events/trigger")).header("Authorization",format!("ApiKey {key}")).json(&json!({"name":"workflow","to":"alice","transactionId":txn,"payload":{}})).send().await}
async fn await_trigger(client:&reqwest::Client,port:u16,key:&str,txn:&str,expected:u16){for _ in 0..150{if trigger(client,port,key,txn).await.is_ok_and(|r|r.status().as_u16()==expected){return;}tokio::time::sleep(Duration::from_millis(100)).await;}panic!("trigger status did not recover")}

#[tokio::test]
#[ignore="requires disposable synchronous PostgreSQL pair; stops primary"]
async fn trigger_guard_handles_synchronous_promotion() {
 let primary_url=std::env::var("TEST_PRIMARY_URL").unwrap();let standby_url=std::env::var("TEST_STANDBY_URL").unwrap();let cluster_url=std::env::var("TEST_FAILOVER_URL").unwrap();let primary=std::env::var("TEST_LOCAL_PRIMARY_DATA").unwrap();let standby=std::env::var("TEST_LOCAL_STANDBY_DATA").unwrap();let pgctl=std::env::var("TEST_LOCAL_PG_CTL").unwrap();
 let mut db=connect(&primary_url).await;let observer=connect(&primary_url).await;let replica=connect(&standby_url).await;
 db.batch_execute("INSERT INTO public.novu_rust_users VALUES('ha-trigger-user'); INSERT INTO public.novu_rust_environments VALUES('ha-trigger-env','org'); INSERT INTO public.novu_rust_workflows(environment_id,identifier,active,has_steps,in_app_content) VALUES('ha-trigger-env','workflow',true,true,'Hello')").await.unwrap();
 for key in ["revoked-fixture","backup-fixture"] {let hash=format!("{:x}",Sha256::digest(key.as_bytes()));db.execute("INSERT INTO public.novu_rust_api_keys(key_hash,environment_id,user_id) VALUES($1,'ha-trigger-env','ha-trigger-user')",&[&hash]).await.unwrap();}
 let p1=port();let mut p2=port();while p1==p2{p2=port();}let mut first=server(p1,&cluster_url);let mut second=server(p2,&cluster_url);let client=reqwest::Client::builder().timeout(Duration::from_secs(5)).build().unwrap();ready(&client,p1).await;ready(&client,p2).await;
 if std::env::var("TEST_TRIGGER_INFLIGHT").as_deref()==Ok("true") {
  assert_eq!(trigger(&client,p2,"backup-fixture","committed-before").await.unwrap().status().as_u16(),201);
  let mut replicated=false;for _ in 0..100{replicated=replica.query_one("SELECT EXISTS(SELECT 1 FROM public.novu_queue_jobs WHERE environment_id='ha-trigger-env' AND payload->>'transactionId'='committed-before')",&[]).await.unwrap().get(0);if replicated{break}tokio::time::sleep(Duration::from_millis(50)).await;}assert!(replicated);
  db.batch_execute("CREATE FUNCTION public.test_trigger_ha_pause() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.transaction_id='inflight' THEN PERFORM pg_advisory_xact_lock(874214); END IF; RETURN NEW; END $$; CREATE TRIGGER test_trigger_ha_pause BEFORE INSERT ON public.novu_rust_trigger_transactions FOR EACH ROW EXECUTE FUNCTION public.test_trigger_ha_pause()").await.unwrap();
  let blocker=db.transaction().await.unwrap();blocker.query_one("SELECT pg_advisory_xact_lock(874214)",&[]).await.unwrap();
  let requester=client.clone();let pending=tokio::spawn(async move{trigger(&requester,p1,"backup-fixture","inflight").await});
  let mut waiting=false;for _ in 0..150{waiting=observer.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event='advisory' AND query LIKE '%novu_rust_lock_trigger_api_key%')",&[]).await.unwrap().get(0);if waiting{break}assert!(first.0.try_wait().unwrap().is_none()&&second.0.try_wait().unwrap().is_none());tokio::time::sleep(Duration::from_millis(20)).await;}assert!(waiting);
  assert!(Command::new(&pgctl).args(["-D",&primary,"-m","immediate","stop","-w"]).stdout(Stdio::null()).status().unwrap().success());assert!(Command::new(&pgctl).args(["-D",&standby,"promote","-w"]).stdout(Stdio::null()).status().unwrap().success());
  let result=pending.await.unwrap();assert!(!result.is_ok_and(|r|r.status().as_u16()==201));drop(blocker);
  ready(&client,p1).await;ready(&client,p2).await;let promoted=connect(&standby_url).await;
  let row=promoted.query_one("SELECT (SELECT count(*) FROM public.novu_rust_trigger_transactions WHERE environment_id='ha-trigger-env' AND transaction_id='inflight'),(SELECT count(*) FROM public.novu_queue_jobs WHERE environment_id='ha-trigger-env' AND payload->>'transactionId'='inflight'),(SELECT count(*) FROM public.novu_queue_jobs WHERE environment_id='ha-trigger-env' AND payload->>'transactionId'='committed-before')",&[]).await.unwrap();assert_eq!(row.get::<_,i64>(0),0);assert_eq!(row.get::<_,i64>(1),0);assert_eq!(row.get::<_,i64>(2),1);
  tokio::join!(await_trigger(&client,p1,"backup-fixture","inflight",201),await_trigger(&client,p2,"backup-fixture","inflight",201));await_trigger(&client,p1,"backup-fixture","committed-before",201).await;
  let row=promoted.query_one("SELECT (SELECT count(*) FROM public.novu_queue_jobs WHERE environment_id='ha-trigger-env' AND payload->>'transactionId'='inflight'),(SELECT count(*) FROM public.novu_queue_jobs WHERE environment_id='ha-trigger-env' AND payload->>'transactionId'='committed-before')",&[]).await.unwrap();assert_eq!(row.get::<_,i64>(0),1);assert_eq!(row.get::<_,i64>(1),1);
  return;
 }
 let hash=format!("{:x}",Sha256::digest(b"revoked-fixture"));let tx=db.transaction().await.unwrap();tx.execute("UPDATE public.novu_rust_api_keys SET enabled=false WHERE key_hash=$1",&[&hash]).await.unwrap();
 let requester=client.clone();let pending=tokio::spawn(async move{trigger(&requester,p1,"revoked-fixture","blocked-before").await});
 let mut waiting=false;for _ in 0..150 {waiting=observer.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE '%novu_rust_lock_trigger_api_key%')",&[]).await.unwrap().get(0);if waiting{break}assert!(first.0.try_wait().unwrap().is_none()&&second.0.try_wait().unwrap().is_none());tokio::time::sleep(Duration::from_millis(20)).await;}assert!(waiting);tx.commit().await.unwrap();assert_eq!(pending.await.unwrap().unwrap().status().as_u16(),401);
 let mut replicated=false;for _ in 0..100 {replicated=replica.query_one("SELECT NOT enabled FROM public.novu_rust_api_keys WHERE key_hash=$1",&[&hash]).await.unwrap().get(0);if replicated{break}tokio::time::sleep(Duration::from_millis(50)).await;}assert!(replicated);
 assert!(Command::new(&pgctl).args(["-D",&primary,"-m","immediate","stop","-w"]).stdout(Stdio::null()).status().unwrap().success());assert!(Command::new(&pgctl).args(["-D",&standby,"promote","-w"]).stdout(Stdio::null()).status().unwrap().success());
 ready(&client,p1).await;ready(&client,p2).await;
 for port in [p1,p2] {await_trigger(&client,port,"revoked-fixture","blocked-after",401).await;}
 await_trigger(&client,p1,"backup-fixture","valid-after",201).await;await_trigger(&client,p2,"backup-fixture","valid-after",201).await;
 let promoted=connect(&standby_url).await;let row=promoted.query_one("SELECT (SELECT count(*) FROM public.novu_rust_trigger_transactions WHERE environment_id='ha-trigger-env' AND transaction_id IN ('blocked-before','blocked-after')),(SELECT count(*) FROM public.novu_queue_jobs WHERE environment_id='ha-trigger-env' AND payload->>'transactionId' IN ('blocked-before','blocked-after')),(SELECT count(*) FROM public.novu_queue_jobs WHERE environment_id='ha-trigger-env' AND payload->>'transactionId'='valid-after')",&[]).await.unwrap();assert_eq!(row.get::<_,i64>(0),0);assert_eq!(row.get::<_,i64>(1),0);assert_eq!(row.get::<_,i64>(2),1);
}
