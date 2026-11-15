//! Scoped projection-policy registration against a fully migrated disposable database.
use novu_postgres_queue::projection_policy;
use std::{
    process::{Child, Command, Stdio},
    time::Duration,
};
use tokio_postgres::NoTls;
use uuid::Uuid;
struct Worker(Child);
impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test]
#[ignore = "requires fully migrated disposable PostgreSQL and role creation"]
async fn scoped_registration_isolated_and_used_by_real_workers() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (mut db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    db.batch_execute(
        "INSERT INTO public.novu_rust_environments VALUES ('policy-a','org'),('policy-b','org')",
    )
    .await
    .unwrap();
    let role = format!("proj_{}", Uuid::new_v4().simple());
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN PASSWORD 'fixture-password'; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_register_projection_policy(text,text,boolean) TO {role}; GRANT SELECT ON public.novu_rust_environments, public.novu_queue_jobs, public.novu_rust_sendgrid_attempts, public.novu_rust_resend_attempts, public.novu_rust_twilio_attempts TO {role}; GRANT SELECT,INSERT,UPDATE ON public.novu_rust_sendgrid_projection_heartbeat, public.novu_rust_resend_projection_heartbeat, public.novu_rust_twilio_projection_heartbeat TO {role}" )).await.unwrap();
    for provider in ["sendgrid", "resend", "twilio"] {
        db.execute("INSERT INTO public.novu_rust_projection_policy_logins SELECT 'policy-a',$1,rolname,oid FROM pg_catalog.pg_roles WHERE rolname=$2", &[&provider,&role]).await.unwrap();
    }
    let mut role_url = reqwest::Url::parse(&url).unwrap();
    role_url.set_username(&role).unwrap();
    role_url.set_password(Some("fixture-password")).unwrap();
    let (actor, driver) = tokio_postgres::connect(role_url.as_str(), NoTls)
        .await
        .unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    assert!(
        actor
            .query(
                "SELECT * FROM public.novu_rust_projection_worker_policy",
                &[]
            )
            .await
            .is_err()
    );
    assert!(
        projection_policy::register_scoped(&actor, "policy-b", "sendgrid", true)
            .await
            .is_err()
    );
    for (provider, bin, env_key, full_key) in [
        (
            "sendgrid",
            env!("CARGO_BIN_EXE_sendgrid-projection-worker"),
            "NOVU_SENDGRID_ENVIRONMENT_ID",
            "NOVU_SENDGRID_PROJECTION_REQUIRE_FULL_SCAN",
        ),
        (
            "resend",
            env!("CARGO_BIN_EXE_resend-projection-worker"),
            "NOVU_RESEND_ENVIRONMENT_ID",
            "NOVU_RESEND_PROJECTION_REQUIRE_FULL_SCAN",
        ),
        (
            "twilio",
            env!("CARGO_BIN_EXE_twilio-projection-worker"),
            "NOVU_TWILIO_ENVIRONMENT_ID",
            "NOVU_TWILIO_PROJECTION_REQUIRE_FULL_SCAN",
        ),
    ] {
        let mut worker = Worker(
            Command::new(bin)
                .env("DATABASE_URL", role_url.as_str())
                .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
                .env("SWALLOW_PROJECTION_SCOPED_POLICY", "true")
                .env(env_key, "policy-a")
                .env(full_key, "true")
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap(),
        );
        let mut heartbeat = false;
        for _ in 0..100 {
            heartbeat=db.query_one(&format!("SELECT EXISTS(SELECT 1 FROM public.novu_rust_{provider}_projection_heartbeat WHERE environment_id='policy-a')"),&[]).await.unwrap().get(0);
            if heartbeat {
                break;
            }
            if worker.0.try_wait().unwrap().is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(
            heartbeat && worker.0.try_wait().unwrap().is_none(),
            "{provider} scoped worker did not start"
        );
        assert!(
            projection_policy::register_scoped(&actor, "policy-a", provider, true)
                .await
                .unwrap()
        );
        assert!(
            !projection_policy::register_scoped(&actor, "policy-a", provider, false)
                .await
                .unwrap()
        );
        drop(worker);
    }
    db.execute("UPDATE public.novu_rust_projection_policy_logins SET principal_oid=0 WHERE environment_id='policy-a' AND provider='sendgrid' AND database_principal=$1", &[&role]).await.unwrap();
    assert!(
        projection_policy::register_scoped(&actor, "policy-a", "sendgrid", true)
            .await
            .is_err()
    );
    db.execute("UPDATE public.novu_rust_projection_policy_logins SET principal_oid=(SELECT oid FROM pg_catalog.pg_roles WHERE rolname=$1) WHERE environment_id='policy-a' AND provider='sendgrid' AND database_principal=$1", &[&role]).await.unwrap();
    // Revocation commits before the waiting registration is allowed to proceed.
    let tx = db.transaction().await.unwrap();
    tx.execute("DELETE FROM public.novu_rust_projection_policy_logins WHERE environment_id='policy-a' AND provider='sendgrid' AND database_principal=$1", &[&role]).await.unwrap();
    let waiting = tokio::spawn(async move {
        projection_policy::register_scoped(&actor, "policy-a", "sendgrid", true).await
    });
    let (observer, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    let mut blocked = false;
    for _ in 0..100 {
        if observer.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE usename=$1 AND wait_event_type='Lock')", &[&role]).await.unwrap().get::<_,bool>(0) {blocked=true;break;}
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(blocked);
    tx.commit().await.unwrap();
    assert!(waiting.await.unwrap().is_err());
}
