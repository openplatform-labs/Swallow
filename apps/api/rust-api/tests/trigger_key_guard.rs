use axum::{body::Body,http::{Request,StatusCode}};
use serde_json::json;
use sha2::{Digest,Sha256};
use std::{sync::Arc,time::Duration};
use tokio_postgres::{Client,NoTls};
use tower::ServiceExt;
use uuid::Uuid;

async fn connect(url:&str)->Client{let (db,driver)=tokio_postgres::connect(url,NoTls).await.unwrap();tokio::spawn(driver);db}
fn request(workflow:&str,txn:&str)->Request<Body>{Request::builder().method("POST").uri("/v1/events/trigger").header("authorization","ApiKey trigger-fixture-key").header("content-type","application/json").body(Body::from(json!({"name":workflow,"to":"alice","transactionId":txn,"payload":{}}).to_string())).unwrap()}
async fn wait_for_lock(db:&Client){for _ in 0..150 {let waiting:bool=db.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE '%novu_rust_lock_trigger_api_key%')",&[]).await.unwrap().get(0);if waiting{return}tokio::time::sleep(Duration::from_millis(20)).await;}panic!("trigger key lock not reached")}
#[tokio::test]
#[ignore="requires fully migrated disposable PostgreSQL"]
async fn trigger_revocation_fences_atomic_enqueue() {
 let url=std::env::var("TEST_DATABASE_URL").unwrap();let mut db=connect(&url).await;let observer=connect(&url).await;
 db.batch_execute("INSERT INTO public.novu_rust_users VALUES('trigger-guard-user'); INSERT INTO public.novu_rust_environments VALUES('trigger-guard-env','org'); INSERT INTO public.novu_rust_workflows(environment_id,identifier,active,has_steps,in_app_content) VALUES('trigger-guard-env','active',true,true,'Hello'),('trigger-guard-env','inactive',false,true,'Hello')").await.unwrap();
 let hash=format!("{:x}",Sha256::digest(b"trigger-fixture-key"));db.execute("INSERT INTO public.novu_rust_api_keys(key_hash,environment_id,user_id) VALUES($1,'trigger-guard-env','trigger-guard-user')",&[&hash]).await.unwrap();
 let role=format!("trigger_key_{}",Uuid::new_v4().simple());db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_lock_trigger_api_key(text,text) TO {role}")).await.unwrap();
 let restricted_url=url.replacen("postgres://postgres@",&format!("postgres://{role}@"),1);let restricted=connect(&restricted_url).await;assert!(restricted.query("SELECT * FROM public.novu_rust_api_keys",&[]).await.is_err());assert!(restricted.execute("UPDATE public.novu_rust_api_keys SET enabled=false",&[]).await.is_err());assert!(restricted.query_one("SELECT public.novu_rust_lock_trigger_api_key($1,$2)",&[&hash,&"trigger-guard-env"]).await.unwrap().get::<_,bool>(0));
 let app=novu_rust_api::router(Arc::new(connect(&url).await));
 for (workflow,commit,txn) in [("active",false,"rollback-active"),("active",true,"revoked-active"),("inactive",true,"revoked-inactive")] {
  db.execute("UPDATE public.novu_rust_api_keys SET enabled=true WHERE key_hash=$1",&[&hash]).await.unwrap();let tx=db.transaction().await.unwrap();tx.execute("UPDATE public.novu_rust_api_keys SET enabled=false WHERE key_hash=$1",&[&hash]).await.unwrap();
  let pending=tokio::spawn(app.clone().oneshot(request(workflow,txn)));wait_for_lock(&observer).await;
  if commit {tx.commit().await.unwrap();assert_eq!(pending.await.unwrap().unwrap().status(),StatusCode::UNAUTHORIZED);}else{tx.rollback().await.unwrap();assert_eq!(pending.await.unwrap().unwrap().status(),StatusCode::CREATED);}
  let row=observer.query_one("SELECT (SELECT count(*) FROM public.novu_rust_trigger_transactions WHERE environment_id='trigger-guard-env' AND transaction_id=$1),(SELECT count(*) FROM public.novu_queue_jobs WHERE environment_id='trigger-guard-env' AND payload->>'transactionId'=$1)",&[&txn]).await.unwrap();let expected=if commit{0}else{1};assert_eq!(row.get::<_,i64>(0),expected);assert_eq!(row.get::<_,i64>(1),expected);
 }
 db.execute("UPDATE public.novu_rust_api_keys SET enabled=true WHERE key_hash=$1",&[&hash]).await.unwrap();
 assert_eq!(app.clone().oneshot(request("active","rollback-active")).await.unwrap().status(),StatusCode::CREATED);assert_eq!(db.query_one("SELECT count(*) FROM public.novu_queue_jobs WHERE environment_id='trigger-guard-env' AND payload->>'transactionId'='rollback-active'",&[]).await.unwrap().get::<_,i64>(0),1);
 db.batch_execute("CREATE FUNCTION public.test_trigger_pause() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.transaction_id='before-revoke' THEN PERFORM pg_advisory_xact_lock(874213); END IF; RETURN NEW; END $$; CREATE TRIGGER test_trigger_pause BEFORE INSERT ON public.novu_rust_trigger_transactions FOR EACH ROW EXECUTE FUNCTION public.test_trigger_pause()").await.unwrap();
 let blocker=db.transaction().await.unwrap();blocker.query_one("SELECT pg_advisory_xact_lock(874213)",&[]).await.unwrap();let pending=tokio::spawn(app.clone().oneshot(request("active","before-revoke")));
 let mut waiting=false;for _ in 0..150 {waiting=observer.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event='advisory' AND query LIKE '%novu_rust_lock_trigger_api_key%')",&[]).await.unwrap().get(0);if waiting{break}tokio::time::sleep(Duration::from_millis(20)).await;}assert!(waiting);
 let revoker=connect(&url).await;let hash_copy=hash.clone();let revoke=tokio::spawn(async move{revoker.execute("UPDATE public.novu_rust_api_keys SET enabled=false WHERE key_hash=$1",&[&hash_copy]).await});
 let mut revocation_waiting=false;for _ in 0..150 {revocation_waiting=observer.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE 'UPDATE public.novu_rust_api_keys SET enabled=false%')",&[]).await.unwrap().get(0);if revocation_waiting{break}tokio::time::sleep(Duration::from_millis(20)).await;}assert!(revocation_waiting);
 blocker.rollback().await.unwrap();assert_eq!(pending.await.unwrap().unwrap().status(),StatusCode::CREATED);assert_eq!(revoke.await.unwrap().unwrap(),1);
 assert_eq!(app.clone().oneshot(request("active","after-revoke")).await.unwrap().status(),StatusCode::UNAUTHORIZED);assert_eq!(db.query_one("SELECT count(*) FROM public.novu_queue_jobs WHERE environment_id='trigger-guard-env' AND payload->>'transactionId'='before-revoke'",&[]).await.unwrap().get::<_,i64>(0),1);
 db.batch_execute("DROP TRIGGER test_trigger_pause ON public.novu_rust_trigger_transactions; DROP FUNCTION public.test_trigger_pause()").await.unwrap();db.execute("UPDATE public.novu_rust_api_keys SET enabled=true WHERE key_hash=$1",&[&hash]).await.unwrap();
 let tx=db.transaction().await.unwrap();tx.execute("DELETE FROM public.novu_rust_users WHERE id='trigger-guard-user'",&[]).await.unwrap();let pending=tokio::spawn(app.oneshot(request("active","deleted-user")));wait_for_lock(&observer).await;tx.commit().await.unwrap();assert_eq!(pending.await.unwrap().unwrap().status(),StatusCode::UNAUTHORIZED);
 let count:i64=observer.query_one("SELECT count(*) FROM public.novu_rust_trigger_transactions WHERE environment_id='trigger-guard-env' AND transaction_id='deleted-user'",&[]).await.unwrap().get(0);assert_eq!(count,0);
}
