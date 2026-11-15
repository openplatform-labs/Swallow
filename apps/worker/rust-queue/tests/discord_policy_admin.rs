//! Policy CLI access, locking and audit on a disposable PostgreSQL instance.
use novu_postgres_queue as queue;
use serde_json::Value;
use std::time::Duration;
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
fn json(output: &std::process::Output) -> Value {
    serde_json::from_slice(&output.stdout).unwrap()
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires fresh disposable PostgreSQL and role creation"]
async fn discord_policy_is_scoped_revisioned_audited_and_replayable() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(driver);
    db.batch_execute("CREATE TABLE public.novu_rust_environments(id text PRIMARY KEY,organization_id text NOT NULL)").await.unwrap();
    db.batch_execute(queue::MIGRATION).await.unwrap();
    db.batch_execute(include_str!(
        "../migrations/0206_discord_workflow_fence.sql"
    ))
    .await
    .unwrap();
    db.batch_execute(include_str!("../migrations/0207_discord_policy_admin.sql"))
        .await
        .unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let env = format!("policy-{suffix}");
    let other = format!("other-{suffix}");
    db.execute(
        "INSERT INTO public.novu_rust_environments VALUES($1,'org'),($2,'org')",
        &[&env, &other],
    )
    .await
    .unwrap();
    let reader = format!("dp_r_{suffix}");
    let writer = format!("dp_w_{suffix}");
    let auditor = format!("dp_a_{suffix}");
    for role in [&reader, &writer, &auditor] {
        db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_discord_policy_read(text),public.novu_rust_discord_policy_set(text,uuid,bigint,boolean,text),public.novu_rust_discord_policy_request(text,uuid),public.novu_rust_discord_policy_audit_read(text,bigint) TO {role}")).await.unwrap();
    }
    for (role, r, w, a) in [
        (&reader, true, false, false),
        (&writer, false, true, false),
        (&auditor, false, false, true),
    ] {
        db.execute("INSERT INTO public.novu_rust_discord_policy_admins SELECT $1,rolname,oid,$3,$4,$5 FROM pg_roles WHERE rolname::text=$2",&[&env,&role,&r,&w,&a]).await.unwrap();
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
            .query(
                "SELECT * FROM public.novu_rust_discord_workflow_policy",
                &[]
            )
            .await
            .is_err()
    );
    assert_eq!(
        json(&cli(&r, &["discord-policy-read", &env]).await),
        serde_json::json!({"enabled":false,"revision":"0"})
    );
    assert!(
        !cli(&r, &["discord-policy-read", &other])
            .await
            .status
            .success()
    );
    assert!(
        !cli(&w, &["discord-policy-read", &env])
            .await
            .status
            .success()
    );
    let id = Uuid::new_v4().to_string();
    assert!(
        !cli(
            &r,
            &["discord-policy-set", &env, &id, "0", "enabled", "ticket"]
        )
        .await
        .status
        .success()
    );
    let set = cli(
        &w,
        &["discord-policy-set", &env, &id, "0", "enabled", "ticket"],
    )
    .await;
    assert!(
        set.status.success(),
        "{}",
        String::from_utf8_lossy(&set.stderr)
    );
    assert_eq!(
        json(&set)["policy"],
        serde_json::json!({"enabled":true,"revision":"1"})
    );
    assert_eq!(
        json(
            &cli(
                &w,
                &["discord-policy-set", &env, &id, "0", "enabled", "ticket"]
            )
            .await
        )["replayed"],
        true
    );
    assert!(
        !cli(
            &w,
            &["discord-policy-set", &env, &id, "0", "disabled", "ticket"]
        )
        .await
        .status
        .success()
    );
    assert_eq!(
        json(&cli(&w, &["discord-policy-request", &env, &id]).await)["found"],
        true
    );
    let id1 = Uuid::new_v4().to_string();
    let id2 = Uuid::new_v4().to_string();
    let first = ["discord-policy-set", &env, &id1, "1", "disabled", "first"];
    let second = ["discord-policy-set", &env, &id2, "1", "enabled", "second"];
    let (one, two) = tokio::join!(cli(&w, &first), cli(&w, &second));
    assert_ne!(one.status.success(), two.status.success());
    let policy = json(&cli(&r, &["discord-policy-read", &env]).await);
    assert_eq!(policy["revision"], "2");
    assert!(
        !cli(&w, &["discord-policy-audit", &env])
            .await
            .status
            .success()
    );
    assert_eq!(
        json(&cli(&a, &["discord-policy-audit", &env]).await)["data"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    db.batch_execute("CREATE FUNCTION public.test_discord_policy_audit_error() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture'; END $$; CREATE TRIGGER test_discord_policy_audit_error BEFORE INSERT ON public.novu_rust_discord_policy_audit FOR EACH ROW EXECUTE FUNCTION public.test_discord_policy_audit_error()").await.unwrap();
    let failed = Uuid::new_v4().to_string();
    assert!(
        !cli(
            &w,
            &[
                "discord-policy-set",
                &env,
                &failed,
                "2",
                "enabled",
                "audit rollback"
            ]
        )
        .await
        .status
        .success()
    );
    db.batch_execute("DROP TRIGGER test_discord_policy_audit_error ON public.novu_rust_discord_policy_audit; DROP FUNCTION public.test_discord_policy_audit_error()").await.unwrap();
    assert_eq!(
        json(&cli(&r, &["discord-policy-read", &env]).await)["revision"],
        "2"
    );
    assert_eq!(
        json(&cli(&w, &["discord-policy-request", &env, &failed]).await)["found"],
        false
    );
    let (mut lock, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(driver);
    let tx = lock.transaction().await.unwrap();
    tx.query_one(
        "SELECT 1 FROM public.novu_rust_discord_workflow_policy WHERE environment_id=$1 FOR SHARE",
        &[&env],
    )
    .await
    .unwrap();
    let change = Uuid::new_v4().to_string();
    let mut pending = tokio::spawn({
        let w = w.clone();
        let env = env.clone();
        async move {
            cli(
                &w,
                &[
                    "discord-policy-set",
                    &env,
                    &change,
                    "2",
                    "enabled",
                    "locking",
                ],
            )
            .await
        }
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(200), &mut pending)
            .await
            .is_err()
    );
    tx.commit().await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(5), pending)
            .await
            .unwrap()
            .unwrap()
            .status
            .success()
    );
    db.execute(
        "UPDATE public.novu_rust_discord_workflow_policy SET enabled=false WHERE environment_id=$1",
        &[&env],
    )
    .await
    .unwrap();
    assert_eq!(
        json(&cli(&r, &["discord-policy-read", &env]).await)["revision"],
        "4"
    );
    assert!(
        !cli(
            &w,
            &[
                "discord-policy-set",
                &env,
                &Uuid::new_v4().to_string(),
                "3",
                "enabled",
                "stale"
            ]
        )
        .await
        .status
        .success()
    );
    db.execute("UPDATE public.novu_rust_discord_policy_admins SET can_write=false WHERE environment_id=$1 AND database_principal=$2",&[&env,&writer]).await.unwrap();
    assert!(
        !cli(&w, &["discord-policy-request", &env, &id])
            .await
            .status
            .success()
    );
}
