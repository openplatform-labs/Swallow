//! Transport-mode rollout fence with restricted database credentials.
use tokio_postgres::NoTls;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires a fully migrated disposable PostgreSQL"]
async fn policy_blocks_legacy_and_mismatched_workers_and_serializes_changes() {
    let url=std::env::var("TEST_DATABASE_URL").unwrap();
    let (db,driver)=tokio_postgres::connect(&url,NoTls).await.unwrap();tokio::spawn(driver);
    let env=format!("smtp-fence-{}",Uuid::new_v4().simple());let other=format!("smtp-fence-other-{}",Uuid::new_v4().simple());let role=format!("smtp_fence_{}",Uuid::new_v4().simple());
    db.execute("INSERT INTO public.novu_rust_environments VALUES($1,'org'),($2,'org')",&[&env,&other]).await.unwrap();
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT SELECT ON public.novu_queue_jobs TO {role}; GRANT INSERT ON public.novu_rust_smtp_attempts TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_smtp_transport_check(text,text),public.novu_rust_smtp_transport_guard(text,uuid,uuid,text) TO {role}")).await.unwrap();
    let restricted_url=url.replacen("postgres://postgres@",&format!("postgres://{role}@"),1);
    let (mut worker,driver)=tokio_postgres::connect(&restricted_url,NoTls).await.unwrap();tokio::spawn(driver);
    assert!(worker.query("SELECT * FROM public.novu_rust_smtp_transport_policy",&[]).await.is_err());
    assert!(!worker.query_one("SELECT public.novu_rust_smtp_transport_check($1,'starttls')",&[&env]).await.unwrap().get::<_,bool>(0));
    let job=Uuid::new_v4();let lease=Uuid::new_v4();
    db.execute("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status,lease_token,lease_until) VALUES($1,$2,'experimental_smtp_email','{}','running',$3,clock_timestamp()+interval '1 minute')",&[&job,&env,&lease]).await.unwrap();
    let legacy=worker.transaction().await.unwrap();
    legacy.execute("INSERT INTO public.novu_rust_smtp_attempts(job_id,environment_id,state) VALUES($1,$2,'dispatching')",&[&job,&env]).await.unwrap();
    legacy.rollback().await.unwrap();
    db.execute("UPDATE public.novu_rust_smtp_transport_policy SET allowed_mode='starttls' WHERE environment_id=$1",&[&env]).await.unwrap();
    assert!(worker.execute("INSERT INTO public.novu_rust_smtp_attempts(job_id,environment_id,state) VALUES($1,$2,'dispatching')",&[&job,&env]).await.is_err());
    assert!(!worker.query_one("SELECT public.novu_rust_smtp_transport_check($1,'none')",&[&env]).await.unwrap().get::<_,bool>(0));
    assert!(worker.query_one("SELECT public.novu_rust_smtp_transport_guard($1,$2,$3,'none')",&[&env,&job,&lease]).await.is_err());
    assert!(worker.query_one("SELECT public.novu_rust_smtp_transport_guard($1,$2,$3,'starttls')",&[&other,&job,&lease]).await.is_err());
    let tx=worker.transaction().await.unwrap();
    tx.query_one("SELECT public.novu_rust_smtp_transport_guard($1,$2,$3,'starttls')",&[&env,&job,&lease]).await.unwrap();
    tx.execute("INSERT INTO public.novu_rust_smtp_attempts(job_id,environment_id,state) VALUES($1,$2,'dispatching')",&[&job,&env]).await.unwrap();
    let (admin2,driver)=tokio_postgres::connect(&url,NoTls).await.unwrap();tokio::spawn(driver);
    let mut change=tokio::spawn(async move {admin2.execute("UPDATE public.novu_rust_smtp_transport_policy SET allowed_mode='tls' WHERE environment_id=$1",&[&env]).await.unwrap()});
    assert!(tokio::time::timeout(std::time::Duration::from_millis(200),&mut change).await.is_err());
    tx.commit().await.unwrap();assert_eq!(change.await.unwrap(),1);
    assert_eq!(db.query_one("SELECT state FROM public.novu_rust_smtp_attempts WHERE job_id=$1",&[&job]).await.unwrap().get::<_,String>(0),"dispatching");
}
