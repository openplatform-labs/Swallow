//! Target-scoped Discord retry policy administration in a disposable DB.
use novu_postgres_queue as queue;
use serde_json::{Value, json};
use tokio::process::Command;
use tokio_postgres::NoTls;
use uuid::Uuid;
async fn cli(url: &str, args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
        .env_clear()
        .env("DATABASE_URL", url)
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .args(args)
        .output()
        .await
        .unwrap()
}
fn value(output: &std::process::Output) -> Value {
    serde_json::from_slice(&output.stdout).unwrap()
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires fresh disposable PostgreSQL and role creation"]
async fn policy_administration_is_scoped_revisioned_and_transactional() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(driver);
    db.batch_execute("CREATE TABLE public.novu_rust_environments(id text PRIMARY KEY,organization_id text NOT NULL)").await.unwrap();
    db.batch_execute(queue::MIGRATION).await.unwrap();
    db.batch_execute(queue::discord_queue::MIGRATION)
        .await
        .unwrap();
    db.batch_execute(queue::discord_queue::RETRY_POLICY_MIGRATION)
        .await
        .unwrap();
    db.batch_execute(include_str!(
        "../migrations/0211_discord_retry_policy_admin.sql"
    ))
    .await
    .unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let env = format!("env-{suffix}");
    let other = format!("other-{suffix}");
    db.execute(
        "INSERT INTO public.novu_rust_environments VALUES($1,'org'),($2,'org')",
        &[&env, &other],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO public.novu_rust_discord_retry_policy(environment_id,destination_id,mode,max_retries,fallback_seconds) VALUES($1,'ops','off',3,60),($1,'other','off',3,60),($2,'ops','off',3,60)",&[&env,&other]).await.unwrap();
    let reader = format!("drp_r_{suffix}");
    let writer = format!("drp_w_{suffix}");
    let auditor = format!("drp_a_{suffix}");
    for role in [&reader, &writer, &auditor] {
        db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_discord_retry_policy_read(text,text),public.novu_rust_discord_retry_policy_set(text,text,uuid,bigint,text,integer,integer,text),public.novu_rust_discord_retry_policy_request(text,text,uuid),public.novu_rust_discord_retry_policy_audit_read(text,text,bigint) TO {role}")).await.unwrap();
    }
    for (role, r, w, a) in [
        (&reader, true, false, false),
        (&writer, false, true, false),
        (&auditor, false, false, true),
    ] {
        db.execute("INSERT INTO public.novu_rust_discord_retry_policy_admins SELECT $1,'ops',rolname,oid,$3,$4,$5 FROM pg_roles WHERE rolname::text=$2",&[&env,&role,&r,&w,&a]).await.unwrap();
    }
    let role_url = |role: &str| {
        let mut u = reqwest::Url::parse(&url).unwrap();
        u.set_username(role).unwrap();
        u.to_string()
    };
    let r = role_url(&reader);
    let w = role_url(&writer);
    let a = role_url(&auditor);
    let (restricted, driver) = tokio_postgres::connect(&w, NoTls).await.unwrap();
    tokio::spawn(driver);
    assert!(
        restricted
            .query("SELECT * FROM public.novu_rust_discord_retry_policy", &[])
            .await
            .is_err()
    );
    assert_eq!(
        value(&cli(&r, &["discord-retry-policy-read", &env, "ops"]).await),
        json!({"mode":"off","maxRetries":3,"fallbackSeconds":60,"revision":"0"})
    );
    assert!(
        !cli(&r, &["discord-retry-policy-read", &env, "other"])
            .await
            .status
            .success()
    );
    assert!(
        !cli(&r, &["discord-retry-policy-read", &other, "ops"])
            .await
            .status
            .success()
    );
    let id = Uuid::new_v4().to_string();
    let change = [
        "discord-retry-policy-set",
        &env,
        "ops",
        &id,
        "0",
        "known",
        "2",
        "30",
        "ticket",
    ];
    assert!(!cli(&r, &change).await.status.success());
    let first = cli(&w, &change).await;
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    assert_eq!(
        value(&first)["policy"],
        json!({"mode":"known","maxRetries":2,"fallbackSeconds":30,"revision":"1"})
    );
    assert_eq!(value(&cli(&w, &change).await)["replayed"], true);
    let conflict = [
        "discord-retry-policy-set",
        &env,
        "ops",
        &id,
        "0",
        "fallback",
        "2",
        "30",
        "ticket",
    ];
    assert!(!cli(&w, &conflict).await.status.success());
    assert_eq!(
        value(&cli(&w, &["discord-retry-policy-request", &env, "ops", &id]).await)["found"],
        true
    );
    db.execute("INSERT INTO public.novu_rust_discord_retry_policy(environment_id,destination_id,mode,max_retries,fallback_seconds) VALUES($1,'ops','known',2,30) ON CONFLICT(environment_id,destination_id) DO UPDATE SET mode=EXCLUDED.mode WHERE (novu_rust_discord_retry_policy.mode,novu_rust_discord_retry_policy.max_retries,novu_rust_discord_retry_policy.fallback_seconds)=(EXCLUDED.mode,EXCLUDED.max_retries,EXCLUDED.fallback_seconds)",&[&env]).await.unwrap();
    assert_eq!(
        value(&cli(&r, &["discord-retry-policy-read", &env, "ops"]).await)["revision"],
        "1"
    );
    let one = Uuid::new_v4().to_string();
    let two = Uuid::new_v4().to_string();
    let c1 = [
        "discord-retry-policy-set",
        &env,
        "ops",
        &one,
        "1",
        "fallback",
        "2",
        "20",
        "first",
    ];
    let c2 = [
        "discord-retry-policy-set",
        &env,
        "ops",
        &two,
        "1",
        "off",
        "1",
        "30",
        "second",
    ];
    let (x, y) = tokio::join!(cli(&w, &c1), cli(&w, &c2));
    assert_ne!(x.status.success(), y.status.success());
    assert_eq!(
        value(&cli(&r, &["discord-retry-policy-read", &env, "ops"]).await)["revision"],
        "2"
    );
    assert!(
        !cli(&w, &["discord-retry-policy-audit", &env, "ops"])
            .await
            .status
            .success()
    );
    assert_eq!(
        value(&cli(&a, &["discord-retry-policy-audit", &env, "ops"]).await)["data"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    db.batch_execute("CREATE FUNCTION public.reject_retry_policy_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture'; END $$; CREATE TRIGGER reject_retry_policy_audit BEFORE INSERT ON public.novu_rust_discord_retry_policy_audit FOR EACH ROW EXECUTE FUNCTION public.reject_retry_policy_audit()").await.unwrap();
    let failed = Uuid::new_v4().to_string();
    assert!(
        !cli(
            &w,
            &[
                "discord-retry-policy-set",
                &env,
                "ops",
                &failed,
                "2",
                "known",
                "5",
                "20",
                "rollback"
            ]
        )
        .await
        .status
        .success()
    );
    db.batch_execute("DROP TRIGGER reject_retry_policy_audit ON public.novu_rust_discord_retry_policy_audit; DROP FUNCTION public.reject_retry_policy_audit()").await.unwrap();
    assert_eq!(
        value(&cli(&r, &["discord-retry-policy-read", &env, "ops"]).await)["revision"],
        "2"
    );
    assert_eq!(
        value(&cli(&w, &["discord-retry-policy-request", &env, "ops", &failed]).await)["found"],
        false
    );
    db.execute("UPDATE public.novu_rust_discord_retry_policy SET max_retries=9 WHERE environment_id=$1 AND destination_id='ops'",&[&env]).await.unwrap();
    assert_eq!(
        value(&cli(&r, &["discord-retry-policy-read", &env, "ops"]).await)["revision"],
        "3"
    );
    db.execute("DELETE FROM public.novu_rust_discord_retry_policy_admins WHERE environment_id=$1 AND destination_id='ops' AND database_principal::text=$2",&[&env,&writer]).await.unwrap();
    assert!(!cli(&w, &change).await.status.success());
}
