use tokio_postgres::NoTls;
use uuid::Uuid;
#[tokio::test]
#[ignore = "requires disposable migrated PostgreSQL and role creation"]
async fn ca_health_is_scoped_and_cli_detects_failure() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(driver);
    let env = format!("smtp-ca-{}", Uuid::new_v4());
    let other = format!("other-{}", Uuid::new_v4());
    let worker = format!("smtp_ca_w_{}", Uuid::new_v4().simple());
    let monitor = format!("smtp_ca_m_{}", Uuid::new_v4().simple());
    db.execute(
        "INSERT INTO public.novu_rust_environments VALUES($1,'org'),($2,'org')",
        &[&env, &other],
    )
    .await
    .unwrap();
    db.batch_execute(&format!("CREATE ROLE {worker} LOGIN; CREATE ROLE {monitor} LOGIN; GRANT USAGE ON SCHEMA public TO {worker},{monitor}; GRANT EXECUTE ON FUNCTION public.novu_rust_smtp_ca_heartbeat(text,uuid,text) TO {worker}; GRANT EXECUTE ON FUNCTION public.novu_rust_smtp_ca_health_check(text,integer) TO {monitor}")).await.unwrap();
    let login = |role: &str| {
        let mut u = reqwest::Url::parse(&url).unwrap();
        u.set_username(role).unwrap();
        u.to_string()
    };
    let (writer, driver) = tokio_postgres::connect(&login(&worker), NoTls)
        .await
        .unwrap();
    tokio::spawn(driver);
    let (reader, driver) = tokio_postgres::connect(&login(&monitor), NoTls)
        .await
        .unwrap();
    tokio::spawn(driver);
    let id = Uuid::new_v4();
    let heartbeat = "SELECT public.novu_rust_smtp_ca_heartbeat($1,$2,$3)";
    assert!(writer
        .query_one(heartbeat, &[&env, &id, &"healthy"])
        .await
        .is_err());
    db.execute("INSERT INTO public.novu_rust_smtp_ca_workers SELECT $1,rolname,oid FROM pg_roles WHERE rolname::text=$2",&[&env,&worker]).await.unwrap();
    assert!(writer
        .query_one(heartbeat, &[&other, &id, &"healthy"])
        .await
        .is_err());
    assert!(writer
        .query_one(heartbeat, &[&env, &id, &"healthy"])
        .await
        .unwrap()
        .get::<_, bool>(0));
    let (spoof, driver) = tokio_postgres::connect(&login(&worker), NoTls)
        .await
        .unwrap();
    tokio::spawn(driver);
    assert!(!spoof
        .query_one(heartbeat, &[&env, &id, &"healthy"])
        .await
        .unwrap()
        .get::<_, bool>(0));
    assert!(reader
        .query_one(
            "SELECT * FROM public.novu_rust_smtp_ca_health_check($1,60)",
            &[&env]
        )
        .await
        .is_err());
    db.execute("INSERT INTO public.novu_rust_delivery_operator_grants SELECT $1,rolname,'alert_view',oid FROM pg_roles WHERE rolname::text=$2",&[&env,&monitor]).await.unwrap();
    assert!(reader
        .query_one(
            "SELECT * FROM public.novu_rust_smtp_ca_health_check($1,60)",
            &[&other]
        )
        .await
        .is_err());
    assert!(writer
        .query_one(heartbeat, &[&env, &id, &"unhealthy"])
        .await
        .unwrap()
        .get::<_, bool>(0));
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
        .env_clear()
        .env("DATABASE_URL", login(&monitor))
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .args(["check-smtp-ca", &env, "60", "1"])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(json["unhealthy"], 1);
    assert!(writer
        .query_one(heartbeat, &[&env, &id, &"healthy"])
        .await
        .unwrap()
        .get::<_, bool>(0));
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
        .env_clear()
        .env("DATABASE_URL", login(&monitor))
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .args(["check-smtp-ca", &env, "60", "1"])
        .output()
        .unwrap();
    assert!(output.status.success());
    db.execute(
        "DELETE FROM public.novu_rust_smtp_ca_workers WHERE environment_id=$1",
        &[&env],
    )
    .await
    .unwrap();
    assert!(writer
        .query_one(heartbeat, &[&env, &id, &"healthy"])
        .await
        .is_err());
    db.execute("DELETE FROM public.novu_rust_delivery_operator_grants WHERE environment_id=$1 AND database_principal=$2",&[&env,&monitor]).await.unwrap();
    assert!(reader
        .query_one(
            "SELECT * FROM public.novu_rust_smtp_ca_health_check($1,60)",
            &[&env]
        )
        .await
        .is_err());
}
