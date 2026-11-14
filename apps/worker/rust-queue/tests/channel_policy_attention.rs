//! Isolated PostgreSQL, restricted login and blocked alert destination.
use novu_postgres_queue as queue;
use serde_json::{Value, json};
use std::{path::Path, process::Command};
use tokio_postgres::NoTls;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires disposable PostgreSQL and roles"]
async fn cancellation_alerts_are_scoped_and_acknowledge_each_revision() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let api = root.join("../../api/rust-api/migrations");
    let worker = root.join("migrations");
    for args in [
        vec!["apply", "api", api.to_str().unwrap(), "--through", "17"],
        vec![
            "apply",
            "worker",
            worker.to_str().unwrap(),
            "--through",
            "122",
        ],
        vec!["apply", "api", api.to_str().unwrap()],
    ] {
        let out = Command::new(env!("CARGO_BIN_EXE_migrate-worker"))
            .args(args)
            .env("DATABASE_URL", &url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    let (mut db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    db.batch_execute("INSERT INTO public.novu_rust_environments VALUES ('a','org'),('b','org')")
        .await
        .unwrap();
    let role = format!("channel_policy_{}", Uuid::new_v4().simple());
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN PASSWORD 'fixture-password'; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_list_channel_policy_attention(text,text,uuid),public.novu_rust_ack_channel_policy(text,uuid,text,text) TO {role}")).await.unwrap();
    let upgrade = Command::new(env!("CARGO_BIN_EXE_migrate-worker"))
        .args([
            "apply",
            "worker",
            worker.to_str().unwrap(),
            "--through",
            "124",
        ])
        .env("DATABASE_URL", &url)
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .output()
        .unwrap();
    assert!(
        upgrade.status.success(),
        "{}",
        String::from_utf8_lossy(&upgrade.stderr)
    );
    assert!(db.query_one("SELECT to_regprocedure('public.novu_rust_ack_channel_policy(text,uuid,text,text)') IS NULL", &[]).await.unwrap().get::<_,bool>(0));
    assert!(!db.query_one("SELECT has_function_privilege($1,'public.novu_rust_list_channel_policy_attention(text,text,uuid)','execute')", &[&role]).await.unwrap().get::<_,bool>(0));
    db.batch_execute(&format!("GRANT EXECUTE ON FUNCTION public.novu_rust_count_channel_policy_attention(text,text),public.novu_rust_list_channel_policy_attention(text,text,uuid),public.novu_rust_ack_channel_policy(text,uuid,bigint,text,text),public.novu_rust_reserve_alert(text,text,text,integer,integer),public.novu_rust_finish_alert(text,text,text,uuid,text,integer) TO {role}")).await.unwrap();
    let upgrade = Command::new(env!("CARGO_BIN_EXE_migrate-worker"))
        .args(["apply", "worker", worker.to_str().unwrap()])
        .env("DATABASE_URL", &url)
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .output()
        .unwrap();
    assert!(
        upgrade.status.success(),
        "{}",
        String::from_utf8_lossy(&upgrade.stderr)
    );
    assert!(db.query_one("SELECT has_function_privilege($1,'public.novu_rust_list_channel_policy_attention(text,text,uuid)','execute')", &[&role]).await.unwrap().get::<_,bool>(0));
    db.batch_execute(&format!("GRANT EXECUTE ON FUNCTION public.novu_rust_push_policy_evidence(text,uuid,bigint) TO {role}")).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'a',$1,p.permission,r.oid FROM pg_roles r CROSS JOIN (VALUES ('alert_view'),('queue_ack'),('alert_send')) p(permission) WHERE r.rolname::text=$1", &[&role]).await.unwrap();
    let email = Uuid::new_v4();
    let sms = Uuid::new_v4();
    let other = Uuid::new_v4();
    let push = Uuid::new_v4();
    for (id, env, kind, reason) in [
        (
            email,
            "a",
            queue::sendgrid_queue::JOB_KIND,
            "email workflow step changed",
        ),
        (
            sms,
            "a",
            queue::twilio_queue::JOB_KIND,
            "sms workflow step disabled",
        ),
        (
            other,
            "b",
            queue::resend_queue::JOB_KIND,
            "email workflow step unavailable",
        ),
    ] {
        queue::enqueue(&db, id, env, kind, &json!({"secret":"not returned"}))
            .await
            .unwrap();
        db.execute(
            "UPDATE public.novu_queue_jobs SET status='cancelled',last_error=$2 WHERE id=$1",
            &[&id, &reason],
        )
        .await
        .unwrap();
    }
    queue::enqueue(
        &db,
        push,
        "a",
        queue::fcm_queue::JOB_KIND,
        &json!({"prepared_messages":[{},{}],"secret":"not returned"}),
    )
    .await
    .unwrap();
    db.execute("INSERT INTO public.novu_rust_fcm_attempts(job_id,environment_id,target_index,state) VALUES ($1,'a',0,'accepted')", &[&push]).await.unwrap();
    db.execute("UPDATE public.novu_queue_jobs SET status='cancelled',last_error='push workflow step changed' WHERE id=$1", &[&push]).await.unwrap();
    let mut role_url = reqwest::Url::parse(&url).unwrap();
    role_url.set_username(&role).unwrap();
    role_url.set_password(Some("fixture-password")).unwrap();
    let (operator, driver) = tokio_postgres::connect(role_url.as_str(), NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    assert!(
        operator
            .query("SELECT payload FROM public.novu_queue_jobs", &[])
            .await
            .is_err()
    );
    assert_eq!(
        queue::reconciliation::channel_policy_count(&operator, "a", "email")
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        queue::reconciliation::channel_policy_count(&operator, "a", "sms")
            .await
            .unwrap(),
        1
    );
    assert!(
        queue::reconciliation::channel_policy_count(&operator, "b", "all")
            .await
            .is_err()
    );
    assert!(
        queue::reconciliation::channel_policy_count(&operator, "a", "unknown")
            .await
            .is_err()
    );
    let rows = queue::reconciliation::channel_policy_list(&operator, "a", "all", None)
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);
    assert!(!format!("{rows:?}").contains("not returned"));
    assert_eq!(
        queue::reconciliation::channel_policy_count(&operator, "a", "push")
            .await
            .unwrap(),
        1
    );
    let evidence = queue::reconciliation::push_policy_evidence(&operator, "a", push, 1)
        .await
        .unwrap();
    assert_eq!(
        evidence,
        json!({"accepted":1,"rejected":0,"unknown":0,"dispatching":0,"unattempted":1})
    );
    assert!(
        queue::reconciliation::push_policy_evidence(&operator, "b", push, 1)
            .await
            .is_err()
    );
    assert!(
        queue::reconciliation::push_policy_evidence(&operator, "a", push, 0)
            .await
            .is_err()
    );
    let run = |args: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
            .args(args)
            .env("DATABASE_URL", role_url.as_str())
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .output()
            .unwrap()
    };
    let check = run(&["check-channel-policy", "a", "email"]);
    assert_eq!(check.status.code(), Some(2));
    assert_eq!(
        serde_json::from_slice::<Value>(&check.stdout).unwrap()["unacknowledgedPolicyCancellations"],
        1
    );
    let evidence_cli = run(&["push-policy-evidence", "a", &push.to_string(), "1"]);
    assert!(evidence_cli.status.success());
    assert_eq!(
        serde_json::from_slice::<Value>(&evidence_cli.stdout).unwrap(),
        evidence
    );
    db.execute("INSERT INTO public.novu_rust_alert_delivery_slots(environment_id,channel,target_id,state) VALUES ('a','slack','ops','rejected')", &[]).await.unwrap();
    let alert = Command::new(env!("CARGO_BIN_EXE_send-delivery-alert"))
        .args(["a", "300", "120", "60"])
        .env("DATABASE_URL", role_url.as_str())
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .env("SWALLOW_ALERT_RESEND_ENABLED", "false")
        .env("SWALLOW_ALERT_EMAIL_POLICY_CANCELLED_ENABLED", "false")
        .env("SWALLOW_ALERT_SMS_POLICY_CANCELLED_ENABLED", "false")
        .env("SWALLOW_ALERT_PUSH_POLICY_CANCELLED_ENABLED", "true")
        .env("SWALLOW_ALERT_SCOPED_DELIVERY", "true")
        .env(
            "SWALLOW_ALERT_SLACK_WEBHOOK",
            "https://hooks.slack.com/services/A/B/C",
        )
        .env("SWALLOW_ALERT_SLACK_TARGET_ID", "ops")
        .env_remove("SWALLOW_ALERT_SLACK_WEBHOOK_FILE")
        .env_remove("SWALLOW_ALERT_EMAIL_TO")
        .output()
        .unwrap();
    assert_eq!(
        alert.status.code(),
        Some(2),
        "{}",
        String::from_utf8_lossy(&alert.stderr)
    );
    assert!(String::from_utf8_lossy(&alert.stderr).contains("requires operator review"));
    assert!(
        queue::reconciliation::acknowledge_channel_policy(
            &operator,
            "a",
            push,
            1,
            "operator",
            "reviewed partial delivery"
        )
        .await
        .unwrap()
    );
    assert_eq!(
        queue::reconciliation::channel_policy_count(&operator, "a", "push")
            .await
            .unwrap(),
        0
    );
    db.execute(
        "UPDATE public.novu_queue_jobs SET status='pending' WHERE id=$1",
        &[&push],
    )
    .await
    .unwrap();
    db.execute("UPDATE public.novu_queue_jobs SET status='cancelled',last_error='push workflow step changed' WHERE id=$1", &[&push]).await.unwrap();
    assert_eq!(
        queue::reconciliation::channel_policy_count(&operator, "a", "push")
            .await
            .unwrap(),
        1
    );
    assert!(
        queue::reconciliation::push_policy_evidence(&operator, "a", push, 1)
            .await
            .is_err()
    );
    assert_eq!(
        queue::reconciliation::push_policy_evidence(&operator, "a", push, 2)
            .await
            .unwrap(),
        evidence
    );
    let unprepared = Uuid::new_v4();
    queue::enqueue(
        &db,
        unprepared,
        "a",
        queue::fcm_queue::JOB_KIND,
        &json!({"workflow_context":{}}),
    )
    .await
    .unwrap();
    db.execute("UPDATE public.novu_queue_jobs SET status='cancelled',last_error='push workflow step disabled' WHERE id=$1", &[&unprepared]).await.unwrap();
    assert_eq!(
        queue::reconciliation::push_policy_evidence(&operator, "a", unprepared, 1)
            .await
            .unwrap()["unattempted"],
        Value::Null
    );
    assert!(
        queue::reconciliation::acknowledge_channel_policy(
            &operator, "a", sms, 1, "operator", "ticket"
        )
        .await
        .unwrap()
    );
    assert!(
        !queue::reconciliation::acknowledge_channel_policy(
            &operator, "a", sms, 1, "operator", "ticket"
        )
        .await
        .unwrap()
    );
    assert_eq!(
        queue::reconciliation::channel_policy_count(&operator, "a", "sms")
            .await
            .unwrap(),
        0
    );
    let (concurrent_operator, driver) = tokio_postgres::connect(role_url.as_str(), NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let transaction = db.transaction().await.unwrap();
    transaction
        .execute(
            "UPDATE public.novu_queue_jobs SET status='pending' WHERE id=$1",
            &[&sms],
        )
        .await
        .unwrap();
    transaction.execute("UPDATE public.novu_queue_jobs SET status='cancelled',last_error='sms workflow step disabled' WHERE id=$1", &[&sms]).await.unwrap();
    let pending_ack = tokio::spawn(async move {
        queue::reconciliation::acknowledge_channel_policy(
            &concurrent_operator,
            "a",
            sms,
            1,
            "operator",
            "ticket",
        )
        .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert!(
        !pending_ack.is_finished(),
        "acknowledgement must wait for the changed job"
    );
    transaction.commit().await.unwrap();
    let concurrent_error = pending_ack.await.unwrap().unwrap_err();
    assert_eq!(
        concurrent_error
            .downcast_ref::<tokio_postgres::Error>()
            .unwrap()
            .code(),
        Some(&tokio_postgres::error::SqlState::T_R_SERIALIZATION_FAILURE)
    );
    assert_eq!(
        queue::reconciliation::channel_policy_count(&operator, "a", "sms")
            .await
            .unwrap(),
        1
    );
    let stale = queue::reconciliation::acknowledge_channel_policy(
        &operator, "a", sms, 1, "operator", "ticket",
    )
    .await
    .unwrap_err();
    assert_eq!(
        stale
            .downcast_ref::<tokio_postgres::Error>()
            .unwrap()
            .code(),
        Some(&tokio_postgres::error::SqlState::T_R_SERIALIZATION_FAILURE)
    );
    let current = queue::reconciliation::channel_policy_list(&operator, "a", "sms", None)
        .await
        .unwrap();
    assert_eq!(current[0]["revision"], "2");
    let changed = run(&[
        "ack-channel-policy",
        "a",
        &sms.to_string(),
        "2",
        "operator",
        "ticket",
    ]);
    assert!(
        changed.status.success(),
        "{}",
        String::from_utf8_lossy(&changed.stderr)
    );
    assert_eq!(
        queue::reconciliation::channel_policy_count(&operator, "a", "sms")
            .await
            .unwrap(),
        0
    );
    assert!(
        queue::reconciliation::acknowledge_channel_policy(
            &operator, "b", other, 1, "operator", "ticket"
        )
        .await
        .is_err()
    );
    let revoke = db.transaction().await.unwrap();
    revoke.execute("DELETE FROM public.novu_rust_delivery_operator_grants WHERE environment_id='a' AND database_principal=$1 AND decision='queue_ack'", &[&role]).await.unwrap();
    let pending = tokio::spawn(async move {
        queue::reconciliation::acknowledge_channel_policy(
            &operator, "a", email, 1, "operator", "ticket",
        )
        .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert!(
        !pending.is_finished(),
        "acknowledgement must wait for revocation"
    );
    revoke.commit().await.unwrap();
    assert!(pending.await.unwrap().is_err());
}
