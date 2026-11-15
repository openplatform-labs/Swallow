//! Disposable PostgreSQL, scoped roles and actual alert fanout/children.
use std::process::Command;
use tokio_postgres::NoTls;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires fully migrated disposable PostgreSQL"]
async fn timing_settings_are_scoped_audited_and_applied_by_fanout() {
    let url=std::env::var("TEST_DATABASE_URL").unwrap();
    let (db,driver)=tokio_postgres::connect(&url,NoTls).await.unwrap();tokio::spawn(async move { let _=driver.await; });
    db.batch_execute("CREATE ROLE timing_reader LOGIN; CREATE ROLE timing_writer LOGIN; INSERT INTO novu_rust_environments VALUES('timing-env','org'),('timing-other','org'); INSERT INTO novu_rust_alert_timing_grants SELECT 'timing-env',rolname,oid,rolname='timing_reader',rolname='timing_writer' FROM pg_roles WHERE rolname IN ('timing_reader','timing_writer'); INSERT INTO novu_rust_alert_timing_grants SELECT 'timing-env',rolname,oid,true,true FROM pg_roles WHERE rolname=session_user; GRANT EXECUTE ON FUNCTION novu_rust_alert_timing_list(text) TO timing_reader,timing_writer; GRANT EXECUTE ON FUNCTION novu_rust_alert_timing_set(text,text,text,bigint,integer,integer,integer,uuid,text) TO timing_reader,timing_writer; INSERT INTO novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'timing-env',rolname,p,oid FROM pg_roles CROSS JOIN unnest(ARRAY['alert_send','alert_view']) p WHERE rolname=session_user; INSERT INTO novu_rust_alert_delivery_slots(environment_id,channel,target_id,state) VALUES ('timing-env','slack','fast','rejected'),('timing-env','slack','slow','rejected'); INSERT INTO novu_rust_resend_projection_heartbeat(environment_id,last_completed_at) VALUES ('timing-env',clock_timestamp())").await.unwrap();
    let reader_url=url.replace("postgres://postgres@","postgres://timing_reader@");let writer_url=url.replace("postgres://postgres@","postgres://timing_writer@");
    let (reader,rd)=tokio_postgres::connect(&reader_url,NoTls).await.unwrap();tokio::spawn(async move {let _=rd.await;});
    let (writer,wd)=tokio_postgres::connect(&writer_url,NoTls).await.unwrap();tokio::spawn(async move {let _=wd.await;});
    let set="SELECT novu_rust_alert_timing_set($1,$2,$3,$4,$5,$6,$7,$8,$9)";
    let request=Uuid::new_v4();let args:&[&(dyn tokio_postgres::types::ToSql+Sync)]=&[&"timing-env",&"slack",&"fast",&0_i64,&Some(60_i32),&Some(300_i32),&Some(90_i32),&request,&"initial"];
    assert!(reader.query(set,args).await.is_err());
    let created:serde_json::Value=writer.query_one(set,args).await.unwrap().get(0);assert_eq!(created["replayed"],false);assert_eq!(created["settings"]["revision"],"1");
    let replay:serde_json::Value=writer.query_one(set,args).await.unwrap().get(0);assert_eq!(replay["replayed"],true);
    assert!(writer.query(set,&[&"timing-env",&"slack",&"fast",&0_i64,&Some(61_i32),&Some(300_i32),&Some(90_i32),&request,&"initial"]).await.is_err());
    assert!(writer.query(set,&[&"timing-env",&"slack",&"fast",&0_i64,&Some(61_i32),&Some(300_i32),&Some(90_i32),&Uuid::new_v4(),&"stale"]).await.is_err());
    assert!(writer.query("SELECT * FROM novu_rust_alert_timing_list($1)",&[&"timing-env"]).await.is_err());
    assert!(reader.query("SELECT * FROM novu_rust_alert_timing_list($1)",&[&"timing-other"]).await.is_err());
    assert!(reader.query("SELECT * FROM novu_rust_alert_timing_settings",&[]).await.is_err());
    writer.query_one(set,&[&"timing-env",&"slack",&"slow",&0_i64,&Some(600_i32),&Some(300_i32),&Option::<i32>::None,&Uuid::new_v4(),&"initial"]).await.unwrap();
    assert_eq!(reader.query("SELECT * FROM novu_rust_alert_timing_list($1)",&[&"timing-env"]).await.unwrap().len(),2);
    let count:i64=db.query_one("SELECT count(*) FROM novu_rust_alert_timing_audit WHERE environment_id='timing-env'",&[]).await.unwrap().get(0);assert_eq!(count,2);
    let concurrent=Uuid::new_v4();let concurrent_args:&[&(dyn tokio_postgres::types::ToSql+Sync)]=&[&"timing-env",&"email",&"unused",&0_i64,&Option::<i32>::None,&Option::<i32>::None,&Some(120_i32),&concurrent,&"concurrent"];
    let (first,second)=tokio::join!(writer.query_one(set,concurrent_args),writer.query_one(set,concurrent_args));
    let first:serde_json::Value=first.unwrap().get(0);let second:serde_json::Value=second.unwrap().get(0);assert_ne!(first["replayed"],second["replayed"]);
    db.batch_execute("CREATE FUNCTION public.test_timing_audit_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture audit failure'; END $$; CREATE TRIGGER test_timing_audit_fail BEFORE INSERT ON public.novu_rust_alert_timing_audit FOR EACH ROW EXECUTE FUNCTION public.test_timing_audit_fail()").await.unwrap();
    let failed_request=Uuid::new_v4();assert!(writer.query(set,&[&"timing-env",&"slack",&"fast",&1_i64,&Some(70_i32),&Some(300_i32),&Some(90_i32),&failed_request,&"audit failure"]).await.is_err());
    assert_eq!(reader.query_one("SELECT revision FROM novu_rust_alert_timing_list($1) WHERE target_id='fast'",&[&"timing-env"]).await.unwrap().get::<_,i64>(0),1);
    db.batch_execute("DROP TRIGGER test_timing_audit_fail ON public.novu_rust_alert_timing_audit; DROP FUNCTION public.test_timing_audit_fail()").await.unwrap();
    let job=Uuid::new_v4();novu_postgres_queue::enqueue(&db,job,"timing-env",novu_postgres_queue::resend_queue::JOB_KIND,&serde_json::json!({})).await.unwrap();db.execute("UPDATE novu_queue_jobs SET available_at=clock_timestamp()-interval '120 seconds' WHERE id=$1",&[&job]).await.unwrap();
    let dir=std::env::temp_dir().join(format!("swallow-timing-db-{}",Uuid::new_v4()));std::fs::create_dir(&dir).unwrap();let secret=dir.join("secret");std::fs::write(&secret,"https://hooks.slack.com/services/A/B/C").unwrap();let config=dir.join("config.json");
    std::fs::write(&config,serde_json::to_vec(&serde_json::json!({"targets":[{"channel":"slack","id":"fast","webhook_file":secret},{"channel":"slack","id":"slow","webhook_file":secret}]})).unwrap()).unwrap();
    let run=|source:&str,db_url:&str| Command::new(env!("CARGO_BIN_EXE_alert-fanout")).args(["delivery","timing-env","300","300","60"]).arg(&config).env("DATABASE_URL",db_url).env("NOVU_POSTGRES_ALLOW_INSECURE","true").env("SWALLOW_ALERT_TIMING_SOURCE",source).env_remove("SWALLOW_ALERT_QUEUE_MAX_WAIT_SECONDS").env_remove("SWALLOW_ALERT_BINDING_KEY").env_remove("SWALLOW_ALERT_BINDING_KEY_FILE").output().unwrap();
    let output=run("postgres",&url);assert_eq!(output.status.code(),Some(2),"{}",String::from_utf8_lossy(&output.stderr));let stdout=String::from_utf8(output.stdout).unwrap();assert!(stdout.contains("slack fast: attention"),"{stdout}");assert!(stdout.contains("slack slow: completed"),"{stdout}");
    let output=run("file",&url);assert_eq!(output.status.code(),Some(0));
    let output=run("postgres","postgres://postgres@127.0.0.1:1/postgres");assert_eq!(output.status.code(),Some(1));assert!(output.stdout.is_empty());
    db.execute("UPDATE novu_rust_alert_timing_grants SET can_write=false WHERE environment_id='timing-env' AND database_principal='timing_writer'",&[]).await.unwrap();
    assert!(writer.query(set,args).await.is_err());
    db.execute("UPDATE novu_rust_alert_timing_grants SET can_read=false WHERE environment_id='timing-env' AND database_principal='timing_reader'",&[]).await.unwrap();
    assert!(reader.query("SELECT * FROM novu_rust_alert_timing_list($1)",&[&"timing-env"]).await.is_err());
    let output=run("postgres",&reader_url);assert_eq!(output.status.code(),Some(1));assert!(output.stdout.is_empty());
    std::fs::remove_dir_all(dir).unwrap();
}
