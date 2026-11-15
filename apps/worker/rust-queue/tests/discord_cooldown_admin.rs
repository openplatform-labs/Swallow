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
fn value(out: &std::process::Output) -> Value {
    serde_json::from_slice(&out.stdout).unwrap()
}

#[tokio::test]
#[ignore = "requires fresh disposable PostgreSQL and role creation"]
async fn scoped_audited_cooldown_choices_are_revisioned_and_idempotent() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(driver);
    db.batch_execute("CREATE TABLE public.novu_rust_environments(id text PRIMARY KEY)")
        .await
        .unwrap();
    for migration in [
        queue::MIGRATION,
        queue::discord_queue::MIGRATION,
        queue::discord_queue::SHARED_COOLDOWN_MIGRATION,
        include_str!("../migrations/0231_discord_cooldown_admin.sql"),
    ] {
        db.batch_execute(migration).await.unwrap();
    }
    db.batch_execute("INSERT INTO public.novu_rust_environments VALUES('env'),('other'); INSERT INTO public.novu_rust_discord_destinations VALUES('env','main','hash'),('env','other','hash'),('other','main','hash')").await.unwrap();
    let role = format!("dc_admin_{}", Uuid::new_v4().simple());
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_discord_cooldown_read(text,text), public.novu_rust_discord_cooldown_set(text,text,uuid,bigint,boolean,text), public.novu_rust_discord_cooldown_request(text,text,uuid), public.novu_rust_discord_cooldown_audit_read(text,text,bigint) TO {role}")).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_discord_cooldown_admins SELECT 'env','main',rolname,oid,true,true,true FROM pg_roles WHERE rolname::text=$1",&[&role]).await.unwrap();
    let mut parsed = reqwest::Url::parse(&url).unwrap();
    parsed.set_username(&role).unwrap();
    let restricted = parsed.to_string();
    let (user, driver) = tokio_postgres::connect(&restricted, NoTls).await.unwrap();
    tokio::spawn(driver);
    assert!(
        user.query(
            "SELECT * FROM public.novu_rust_discord_shared_cooldown",
            &[]
        )
        .await
        .is_err()
    );
    assert_eq!(
        value(&cli(&restricted, &["discord-cooldown-read", "env", "main"]).await),
        json!({"enabled":false,"revision":"0","notBefore":null})
    );
    for (env, dest) in [("other", "main"), ("env", "other")] {
        assert!(
            !cli(&restricted, &["discord-cooldown-read", env, dest])
                .await
                .status
                .success()
        );
    }
    let request = Uuid::new_v4().to_string();
    let args = [
        "discord-cooldown-set",
        "env",
        "main",
        &request,
        "0",
        "enabled",
        "operation",
    ];
    let out = cli(&restricted, &args).await;
    assert!(out.status.success());
    assert_eq!(
        value(&out)["policy"],
        json!({"enabled":true,"revision":"1"})
    );
    let replay = cli(&restricted, &args).await;
    assert_eq!(value(&replay)["replayed"], true);
    assert!(
        !cli(
            &restricted,
            &[
                "discord-cooldown-set",
                "env",
                "main",
                &request,
                "0",
                "disabled",
                "operation"
            ]
        )
        .await
        .status
        .success()
    );
    let out = cli(
        &restricted,
        &["discord-cooldown-request", "env", "main", &request],
    )
    .await;
    assert_eq!(value(&out)["policy"]["revision"], "1");
    let stale = Uuid::new_v4().to_string();
    assert!(
        !cli(
            &restricted,
            &[
                "discord-cooldown-set",
                "env",
                "main",
                &stale,
                "0",
                "disabled",
                "stale"
            ]
        )
        .await
        .status
        .success()
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM public.novu_rust_discord_cooldown_audit",
            &[]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        1
    );
    let one = Uuid::new_v4().to_string();
    let two = Uuid::new_v4().to_string();
    let one_args = [
        "discord-cooldown-set",
        "env",
        "main",
        &one,
        "1",
        "disabled",
        "concurrent",
    ];
    let two_args = [
        "discord-cooldown-set",
        "env",
        "main",
        &two,
        "1",
        "disabled",
        "concurrent",
    ];
    let (a, b) = tokio::join!(cli(&restricted, &one_args), cli(&restricted, &two_args));
    assert_ne!(a.status.success(), b.status.success());
    assert_eq!(
        value(&cli(&restricted, &["discord-cooldown-read", "env", "main"]).await),
        json!({"enabled":false,"revision":"2","notBefore":null})
    );
    let audit = value(&cli(&restricted, &["discord-cooldown-audit", "env", "main"]).await);
    assert_eq!(audit["data"].as_array().unwrap().len(), 2);
    db.batch_execute("CREATE FUNCTION public.fail_cooldown_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'audit unavailable'; END $$; CREATE TRIGGER fail_cooldown_audit BEFORE INSERT ON public.novu_rust_discord_cooldown_audit FOR EACH ROW EXECUTE FUNCTION public.fail_cooldown_audit()").await.unwrap();
    let failed = Uuid::new_v4().to_string();
    assert!(
        !cli(
            &restricted,
            &[
                "discord-cooldown-set",
                "env",
                "main",
                &failed,
                "2",
                "enabled",
                "audit test"
            ]
        )
        .await
        .status
        .success()
    );
    assert_eq!(
        value(&cli(&restricted, &["discord-cooldown-read", "env", "main"]).await),
        json!({"enabled":false,"revision":"2","notBefore":null})
    );
    assert_eq!(
        value(
            &cli(
                &restricted,
                &["discord-cooldown-request", "env", "main", &failed]
            )
            .await
        ),
        json!({"found":false})
    );
    db.execute("UPDATE public.novu_rust_discord_cooldown_admins SET can_write=false WHERE database_principal::text=$1",&[&role]).await.unwrap();
    assert!(!cli(&restricted, &args).await.status.success());
}
