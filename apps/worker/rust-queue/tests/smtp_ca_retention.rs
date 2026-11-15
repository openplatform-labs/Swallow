use tokio_postgres::NoTls;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires disposable migrated PostgreSQL and role creation"]
async fn restricted_retention_preserves_live_and_recent_records_and_audits() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(driver);
    let env = format!("smtp-ca-retention-{}", Uuid::new_v4());
    let other = format!("smtp-other-{}", Uuid::new_v4());
    let role = format!("smtp_ca_ret_{}", Uuid::new_v4().simple());
    db.execute(
        "INSERT INTO public.novu_rust_environments VALUES($1,'org'),($2,'org')",
        &[&env, &other],
    )
    .await
    .unwrap();
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_prune_smtp_ca_health(text,integer,integer),public.novu_rust_smtp_ca_health_retention_audit_page(text,bigint,integer) TO {role}")).await.unwrap();
    let mut login = reqwest::Url::parse(&url).unwrap();
    login.set_username(&role).unwrap();
    let (mut client, driver) = tokio_postgres::connect(login.as_str(), NoTls)
        .await
        .unwrap();
    tokio::spawn(driver);
    let sql = "SELECT public.novu_rust_prune_smtp_ca_health($1,$2,$3)";
    assert!(client
        .query_one(sql, &[&env, &86401_i32, &10_i32])
        .await
        .is_err());
    db.execute("INSERT INTO public.novu_rust_smtp_health_retention_operators SELECT $1,rolname,oid FROM pg_roles WHERE rolname::text=$2",&[&env,&role]).await.unwrap();
    assert!(client.query_one(sql, &[&env, &86401_i32, &10_i32]).await.is_err());
    db.execute("INSERT INTO public.novu_rust_smtp_ca_health_retention_operators SELECT $1,rolname,oid FROM pg_roles WHERE rolname::text=$2",&[&env,&role]).await.unwrap();
    assert!(client
        .query_one(sql, &[&other, &86401_i32, &10_i32])
        .await
        .is_err());
    assert!(client
        .query_one(sql, &[&env, &86400_i32, &10_i32])
        .await
        .is_err());
    let live = Uuid::new_v4();
    let dead = Uuid::new_v4();
    let recent = Uuid::new_v4();
    let other_id = Uuid::new_v4();
    db.execute("INSERT INTO public.novu_rust_smtp_ca_health(environment_id,instance_id,database_principal,backend_pid,backend_started_at,state,observed_at) SELECT $1,$2,session_user,pid,backend_start,'healthy',clock_timestamp()-interval '3 days' FROM pg_stat_activity WHERE pid=pg_backend_pid()",&[&env,&live]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_smtp_ca_health VALUES($1,$2,session_user,-1,clock_timestamp()-interval '5 days','unhealthy',clock_timestamp()-interval '3 days'),($1,$3,session_user,-1,clock_timestamp()-interval '5 days','unhealthy',clock_timestamp()),($4,$5,session_user,-1,clock_timestamp()-interval '5 days','unhealthy',clock_timestamp()-interval '3 days')",&[&env,&dead,&recent,&other,&other_id]).await.unwrap();
    let tx = client.transaction().await.unwrap();
    tx.query_one(
        "SELECT set_config('swallow.smtp_ca_health_retention_reason','cleanup',true)",
        &[],
    )
    .await
    .unwrap();
    assert_eq!(
        tx.query_one(sql, &[&env, &86401_i32, &1_i32])
            .await
            .unwrap()
            .get::<_, i64>(0),
        1
    );
    tx.commit().await.unwrap();
    assert_eq!(db.query_one("SELECT count(*) FROM public.novu_rust_smtp_ca_health WHERE instance_id=ANY($1)",&[&vec![live,recent,other_id]]).await.unwrap().get::<_,i64>(0),3);
    let audit=client.query_one("SELECT deleted_count,reason FROM public.novu_rust_smtp_ca_health_retention_audit_page($1,NULL,10)",&[&env]).await.unwrap();
    assert_eq!(audit.get::<_, i64>(0), 1);
    assert_eq!(
        audit.get::<_, Option<String>>(1).as_deref(),
        Some("cleanup")
    );
    assert!(client
        .query(
            "SELECT * FROM public.novu_rust_smtp_ca_health_retention_audit_page($1,NULL,10)",
            &[&other]
        )
        .await
        .is_err());
    db.execute("UPDATE public.novu_rust_smtp_ca_health SET observed_at=clock_timestamp()-interval '3 days' WHERE instance_id=$1",&[&recent]).await.unwrap();
    db.batch_execute("ALTER TABLE public.novu_rust_smtp_ca_health_retention_audit ADD CONSTRAINT force_failure CHECK (deleted_count=0)").await.unwrap_err();
    db.batch_execute("ALTER TABLE public.novu_rust_smtp_ca_health_retention_audit ADD CONSTRAINT force_failure CHECK (deleted_count=0) NOT VALID").await.unwrap();
    assert!(client
        .query_one(sql, &[&env, &86401_i32, &10_i32])
        .await
        .is_err());
    assert!(db
        .query_opt(
            "SELECT 1 FROM public.novu_rust_smtp_ca_health WHERE instance_id=$1",
            &[&recent]
        )
        .await
        .unwrap()
        .is_some());
    db.batch_execute(
        "ALTER TABLE public.novu_rust_smtp_ca_health_retention_audit DROP CONSTRAINT force_failure",
    )
    .await
    .unwrap();
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
        .env("DATABASE_URL", login.as_str())
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .args(["prune-smtp-ca", &env, "86401", "10", "routine"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["deleted"], 1);
    let audit = client.query_one("SELECT deleted_count,reason FROM public.novu_rust_smtp_ca_health_retention_audit_page($1,NULL,1)",&[&env]).await.unwrap();
    assert_eq!(audit.get::<_, i64>(0), 1);
    assert_eq!(
        audit.get::<_, Option<String>>(1).as_deref(),
        Some("routine")
    );
    db.execute(
        "DELETE FROM public.novu_rust_smtp_ca_health_retention_operators WHERE environment_id=$1",
        &[&env],
    )
    .await
    .unwrap();
    assert!(client
        .query_one(sql, &[&env, &86401_i32, &10_i32])
        .await
        .is_err());
}
