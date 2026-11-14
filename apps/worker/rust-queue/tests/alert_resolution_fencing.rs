//! Operator decisions are tied to the exact observed alert attempt.
use std::process::Command;
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;

async fn connect(url: &str) -> Client {
    let (db, driver) = tokio_postgres::connect(url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    db
}

#[tokio::test]
#[ignore = "requires fully migrated disposable PostgreSQL and role creation"]
async fn stale_and_concurrent_decisions_are_rejected() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let db = connect(&url).await;
    db.batch_execute(
        "INSERT INTO public.novu_rust_environments VALUES ('fence-a','org'),('fence-b','org')",
    )
    .await
    .unwrap();
    let role = format!("fence_{}", Uuid::new_v4().simple());
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_resolve_alert_v2(text,text,text,uuid,text,text,text) TO {role}")).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'fence-a',rolname,'alert_retry',oid FROM pg_roles WHERE rolname::text=$1", &[&role]).await.unwrap();
    let actor_url = url.replacen("postgres://postgres@", &format!("postgres://{role}@"), 1);
    let actor = connect(&actor_url).await;
    assert!(
        actor
            .query("SELECT * FROM public.novu_rust_alert_delivery_slots", &[])
            .await
            .is_err()
    );
    let old = Uuid::new_v4();
    let current = Uuid::new_v4();
    db.execute("INSERT INTO public.novu_rust_alert_delivery_slots(environment_id,channel,target_id,state,attempt_id) VALUES ('fence-a','slack','ops','unknown',$1)", &[&current]).await.unwrap();
    for id in [old, current] {
        db.execute("INSERT INTO public.novu_rust_alert_delivery_attempts(id,environment_id,channel,target_id,state) VALUES ($1,'fence-a','slack','ops','unknown')", &[&id]).await.unwrap();
    }
    let resolve = |attempt: Uuid, env: &str, decision: &str| {
        Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
            .args([
                "alert-resolve-v2",
                env,
                "slack",
                "ops",
                &attempt.to_string(),
                decision,
                "operator",
                "review",
            ])
            .env("DATABASE_URL", &actor_url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .output()
            .unwrap()
    };
    assert!(!resolve(old, "fence-a", "retry").status.success());
    assert!(!resolve(current, "fence-b", "retry").status.success());
    assert!(!resolve(current, "fence-a", "accepted").status.success());
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM public.novu_rust_alert_delivery_resolutions",
            &[]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    db.batch_execute("CREATE FUNCTION public.test_fence_reject_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture'; END $$; CREATE TRIGGER test_fence_reject_audit BEFORE INSERT ON public.novu_rust_alert_delivery_resolutions FOR EACH ROW EXECUTE FUNCTION public.test_fence_reject_audit()").await.unwrap();
    assert!(!resolve(current, "fence-a", "retry").status.success());
    assert_eq!(db.query_one("SELECT state FROM public.novu_rust_alert_delivery_slots WHERE environment_id='fence-a' AND target_id='ops'",&[]).await.unwrap().get::<_,String>(0),"unknown");
    db.batch_execute("DROP TRIGGER test_fence_reject_audit ON public.novu_rust_alert_delivery_resolutions; DROP FUNCTION public.test_fence_reject_audit()").await.unwrap();
    let a = connect(&actor_url).await;
    let b = connect(&actor_url).await;
    let args: &[&(dyn tokio_postgres::types::ToSql + Sync)] = &[&current];
    let sql = "SELECT public.novu_rust_resolve_alert_v2('fence-a','slack','ops',$1,'retry','operator','race')";
    let (one, two) = tokio::join!(a.query_one(sql, args), b.query_one(sql, args));
    assert_ne!(one.is_ok(), two.is_ok());
    let row=db.query_one("SELECT attempt_id,decision,database_session_user FROM public.novu_rust_alert_delivery_resolutions",&[]).await.unwrap();
    assert_eq!(row.get::<_, Uuid>(0), current);
    assert_eq!(row.get::<_, String>(1), "retry");
    assert_eq!(row.get::<_, String>(2), role);
    // A new unknown attempt after a successful retry cannot be resolved using the old ID.
    let newer = Uuid::new_v4();
    db.execute("INSERT INTO public.novu_rust_alert_delivery_attempts(id,environment_id,channel,target_id,state) VALUES ($1,'fence-a','slack','ops','unknown')", &[&newer]).await.unwrap();
    db.execute("UPDATE public.novu_rust_alert_delivery_slots SET state='unknown',attempt_id=$1 WHERE environment_id='fence-a' AND target_id='ops'", &[&newer]).await.unwrap();
    assert!(!resolve(current, "fence-a", "retry").status.success());
    assert!(resolve(newer, "fence-a", "retry").status.success());
    db.execute("INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'fence-a',rolname,'alert_accepted',oid FROM pg_roles WHERE rolname::text=$1", &[&role]).await.unwrap();
    db.execute("UPDATE public.novu_rust_alert_delivery_slots SET state='rejected' WHERE environment_id='fence-a' AND target_id='ops'", &[]).await.unwrap();
    assert!(!resolve(newer, "fence-a", "accepted").status.success());
    db.execute("UPDATE public.novu_rust_alert_delivery_slots SET state='unknown' WHERE environment_id='fence-a' AND target_id='ops'", &[]).await.unwrap();
    assert!(resolve(newer, "fence-a", "accepted").status.success());
    assert_eq!(db.query_one("SELECT state FROM public.novu_rust_alert_delivery_slots WHERE environment_id='fence-a' AND target_id='ops'", &[]).await.unwrap().get::<_,String>(0), "accepted");
    let email = Uuid::new_v4();
    db.execute("INSERT INTO public.novu_rust_alert_delivery_slots(environment_id,channel,target_id,state,attempt_id) VALUES ('fence-a','email','mail','unknown',$1)", &[&email]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_alert_delivery_attempts(id,environment_id,channel,target_id,state) VALUES ($1,'fence-a','email','mail','unknown')", &[&email]).await.unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
        .args([
            "alert-resolve-v2",
            "fence-a",
            "email",
            "mail",
            &email.to_string(),
            "accepted",
            "operator",
            "review",
        ])
        .env("DATABASE_URL", &actor_url)
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .output()
        .unwrap();
    assert!(output.status.success());
    assert_eq!(db.query_one("SELECT state FROM public.novu_rust_alert_delivery_slots WHERE environment_id='fence-a' AND target_id='mail'", &[]).await.unwrap().get::<_,String>(0), "accepted");
}
