//! A scoped projection holds its authorization locks through the job commit.
use novu_postgres_queue as queue;
use serde_json::json;
use std::{
    process::{Child, Command, Stdio},
    time::Duration,
};
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;
struct Worker(Child);
impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

async fn connect(url: &str, name: &str) -> Client {
    let (db, driver) = tokio_postgres::connect(&format!("{url}?application_name={name}"), NoTls)
        .await
        .unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    db
}
async fn wait_locked(db: &Client, name: &str) {
    for _ in 0..150 {
        if db.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock')", &[&name]).await.unwrap().get::<_,bool>(0) {return;}
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("{name} did not wait for lock");
}
#[tokio::test]
#[ignore = "requires fully migrated disposable PostgreSQL"]
async fn projection_and_revocation_serialize_in_both_orders() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let admin = connect(&url, "policy_setup").await;
    admin
        .batch_execute("INSERT INTO public.novu_rust_environments VALUES ('tx-env','org')")
        .await
        .unwrap();
    for provider in ["sendgrid", "resend", "twilio"] {
        let kind = match provider {
            "sendgrid" => queue::sendgrid_queue::JOB_KIND,
            "resend" => queue::resend_queue::JOB_KIND,
            _ => queue::twilio_queue::JOB_KIND,
        };
        let job = Uuid::new_v4();
        queue::enqueue(&admin, job, "tx-env", kind, &json!({}))
            .await
            .unwrap();
        if provider == "sendgrid" {
            let dispatch = Uuid::new_v4();
            admin.execute("INSERT INTO public.novu_rust_sendgrid_dispatch_ids(id,job_id,environment_id) VALUES ($1,$2,'tx-env')", &[&dispatch,&job]).await.unwrap();
            admin.execute("INSERT INTO public.novu_rust_sendgrid_attempts(job_id,environment_id,state,dispatch_id) VALUES ($1,'tx-env','accepted',$2)", &[&job,&dispatch]).await.unwrap();
            admin.execute("INSERT INTO public.novu_rust_sendgrid_recipients(dispatch_id,environment_id,recipient_kind,recipient_index,recipient_hash) VALUES ($1,'tx-env','to',0,repeat('a',64))", &[&dispatch]).await.unwrap();
        } else if provider == "resend" {
            admin.execute("INSERT INTO public.novu_rust_resend_attempts(job_id,environment_id,state,provider_message_id) VALUES ($1,'tx-env','accepted','provider-id')", &[&job]).await.unwrap();
        } else {
            admin.execute("INSERT INTO public.novu_rust_twilio_attempts(job_id,environment_id,state,provider_message_id) VALUES ($1,'tx-env','accepted','provider-id')", &[&job]).await.unwrap();
        }
        admin.execute("INSERT INTO public.novu_rust_projection_policy_logins SELECT 'tx-env',$1,rolname,oid FROM pg_catalog.pg_roles WHERE rolname=session_user", &[&provider]).await.unwrap();
        assert!(
            queue::projection_policy::register_scoped(&admin, "tx-env", provider, true)
                .await
                .unwrap()
        );
        let mut revoker = connect(&url, "revoke_first").await;
        let revocation = revoker.transaction().await.unwrap();
        revocation.execute("DELETE FROM public.novu_rust_projection_policy_logins WHERE environment_id='tx-env' AND provider=$1 AND database_principal=session_user", &[&provider]).await.unwrap();
        let mut actor = connect(&url, "projection_after_revoke").await;
        let work = tokio::spawn(async move {
            match provider {
                "sendgrid" => {
                    queue::sendgrid_projection::project_with_policy(
                        &mut actor,
                        "tx-env",
                        job,
                        Some(true),
                    )
                    .await
                }
                "resend" => {
                    queue::resend_projection::project_with_policy(
                        &mut actor,
                        "tx-env",
                        job,
                        true,
                        Some(true),
                    )
                    .await
                }
                _ => {
                    queue::twilio_projection::project_with_policy(
                        &mut actor,
                        "tx-env",
                        job,
                        Some(true),
                    )
                    .await
                }
            }
        });
        wait_locked(&admin, "projection_after_revoke").await;
        revocation.commit().await.unwrap();
        assert!(work.await.unwrap().is_err());
        admin.execute("INSERT INTO public.novu_rust_projection_policy_logins SELECT 'tx-env',$1,rolname,oid FROM pg_catalog.pg_roles WHERE rolname=session_user", &[&provider]).await.unwrap();
        let mut locker = connect(&url, "evidence_lock").await;
        let lock = locker.transaction().await.unwrap();
        lock.batch_execute(&format!(
            "LOCK TABLE public.novu_rust_{provider}_delivery_events IN ACCESS EXCLUSIVE MODE"
        ))
        .await
        .unwrap();
        let mut actor = connect(&url, "projection_before_revoke").await;
        let work = tokio::spawn(async move {
            match provider {
                "sendgrid" => {
                    queue::sendgrid_projection::project_with_policy(
                        &mut actor,
                        "tx-env",
                        job,
                        Some(true),
                    )
                    .await
                }
                "resend" => {
                    queue::resend_projection::project_with_policy(
                        &mut actor,
                        "tx-env",
                        job,
                        true,
                        Some(true),
                    )
                    .await
                }
                _ => {
                    queue::twilio_projection::project_with_policy(
                        &mut actor,
                        "tx-env",
                        job,
                        Some(true),
                    )
                    .await
                }
            }
        });
        wait_locked(&admin, "projection_before_revoke").await;
        let revoker = connect(&url, "revoke_after_projection").await;
        let revoke = tokio::spawn(async move {
            revoker.execute("DELETE FROM public.novu_rust_projection_policy_logins WHERE environment_id='tx-env' AND provider=$1 AND database_principal=session_user", &[&provider]).await
        });
        wait_locked(&admin, "revoke_after_projection").await;
        lock.commit().await.unwrap();
        assert!(work.await.unwrap().unwrap());
        assert_eq!(revoke.await.unwrap().unwrap(), 1);
        // The actual worker uses the same transaction-scoped guard.
        admin.execute("INSERT INTO public.novu_rust_projection_policy_logins SELECT 'tx-env',$1,rolname,oid FROM pg_catalog.pg_roles WHERE rolname=session_user", &[&provider]).await.unwrap();
        let mut locker = connect(&url, "worker_evidence_lock").await;
        let lock = locker.transaction().await.unwrap();
        lock.batch_execute(&format!(
            "LOCK TABLE public.novu_rust_{provider}_delivery_events IN ACCESS EXCLUSIVE MODE"
        ))
        .await
        .unwrap();
        let (bin, env_key, full_key) = match provider {
            "sendgrid" => (
                env!("CARGO_BIN_EXE_sendgrid-projection-worker"),
                "NOVU_SENDGRID_ENVIRONMENT_ID",
                "NOVU_SENDGRID_PROJECTION_REQUIRE_FULL_SCAN",
            ),
            "resend" => (
                env!("CARGO_BIN_EXE_resend-projection-worker"),
                "NOVU_RESEND_ENVIRONMENT_ID",
                "NOVU_RESEND_PROJECTION_REQUIRE_FULL_SCAN",
            ),
            _ => (
                env!("CARGO_BIN_EXE_twilio-projection-worker"),
                "NOVU_TWILIO_ENVIRONMENT_ID",
                "NOVU_TWILIO_PROJECTION_REQUIRE_FULL_SCAN",
            ),
        };
        let worker = Worker(
            Command::new(bin)
                .env(
                    "DATABASE_URL",
                    format!("{url}?application_name=scoped_real_worker"),
                )
                .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
                .env("SWALLOW_PROJECTION_SCOPED_POLICY", "true")
                .env("NOVU_RESEND_PROJECTION_AUTO_CORRELATE", "true")
                .env(env_key, "tx-env")
                .env(full_key, "true")
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap(),
        );
        wait_locked(&admin, "scoped_real_worker").await;
        let revoker = connect(&url, "revoke_real_worker").await;
        let revoke = tokio::spawn(async move {
            revoker.execute("DELETE FROM public.novu_rust_projection_policy_logins WHERE environment_id='tx-env' AND provider=$1 AND database_principal=session_user", &[&provider]).await
        });
        wait_locked(&admin, "revoke_real_worker").await;
        lock.commit().await.unwrap();
        assert_eq!(revoke.await.unwrap().unwrap(), 1);
        drop(worker);
    }
}
