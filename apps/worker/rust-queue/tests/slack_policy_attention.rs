//! Real guarded cancellations, restricted monitoring, acknowledgements and CLI.
use novu_postgres_queue as queue;
use serde_json::{Value, json};
use std::{path::Path, process::Command};
use tokio_postgres::NoTls;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires disposable PostgreSQL and roles"]
async fn policy_cancellations_are_selectively_monitored_and_acknowledged() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let api = root.join("../../api/rust-api/migrations");
    let worker = root.join("migrations");
    for args in [
        vec!["apply", "api", api.to_str().unwrap(), "--through", "17"],
        vec!["apply", "worker", worker.to_str().unwrap()],
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
    db.batch_execute("INSERT INTO public.novu_rust_environments VALUES ('a','org'),('b','org'); INSERT INTO public.novu_rust_workflows(environment_id,identifier,active,has_steps,chat_step_active,chat_content) VALUES ('a','wf',true,true,false,'old'); INSERT INTO public.novu_rust_subscribers(environment_id,subscriber_id,internal_id) VALUES ('a','alice','alice-id')").await.unwrap();
    let role = format!("policy_operator_{}", Uuid::new_v4().simple());
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN PASSWORD 'fixture-password'; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_slack_health(text),public.novu_rust_slack_policy_cancelled(text),public.novu_rust_list_slack_attention(text,uuid),public.novu_rust_ack_slack_workflow(text,uuid,text,text),public.novu_rust_reserve_alert(text,text,text,integer,integer),public.novu_rust_finish_alert(text,text,text,uuid,text,integer) TO {role}")).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'a',$1,p.permission,r.oid FROM pg_roles r CROSS JOIN (VALUES ('slack_view'),('slack_ack'),('alert_send')) p(permission) WHERE r.rolname::text=$1",&[&role]).await.unwrap();
    let ctx = queue::slack_workflow::Context {
        subscriber_id: "alice".into(),
        subscriber_internal_id: "alice-id".into(),
        workflow_identifier: "wf".into(),
        workflow_internal_id: None,
        content_template: "old".into(),
        payload: json!({}),
        context_keys: vec![],
        actor: None,
        actor_internal_id: None,
        endpoint_snapshot: None,
    };
    let job = Uuid::new_v4();
    queue::enqueue(
        &db,
        job,
        "a",
        queue::slack_workflow::FANOUT_KIND,
        &serde_json::to_value(&ctx).unwrap(),
    )
    .await
    .unwrap();
    assert!(
        queue::slack_workflow::fanout_one_with_policy(
            &mut db,
            "a",
            queue::slack_workflow::StepPolicy::Active
        )
        .await
        .unwrap()
    );
    let changed = Uuid::new_v4();
    let other = Uuid::new_v4();
    for (id, env) in [(changed, "a"), (other, "b")] {
        queue::enqueue(
            &db,
            id,
            env,
            queue::slack_workflow::DELIVERY_KIND,
            &json!({}),
        )
        .await
        .unwrap();
        db.execute("UPDATE public.novu_queue_jobs SET status='cancelled',last_error='Slack workflow content changed' WHERE id=$1",&[&id]).await.unwrap();
    }
    let mut role_url = reqwest::Url::parse(&url).unwrap();
    role_url.set_username(&role).unwrap();
    role_url.set_password(Some("fixture-password")).unwrap();
    let (restricted, driver) = tokio_postgres::connect(role_url.as_str(), NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    assert!(
        restricted
            .query("SELECT payload FROM public.novu_queue_jobs", &[])
            .await
            .is_err()
    );
    assert_eq!(
        queue::slack_queue::policy_cancellations(&db, "a", false)
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        queue::slack_queue::policy_cancellations(&restricted, "a", true)
            .await
            .unwrap(),
        2
    );
    assert!(
        queue::slack_queue::policy_cancellations(&restricted, "b", true)
            .await
            .is_err()
    );
    let list = queue::slack_queue::list_attention(&restricted, "a", None)
        .await
        .unwrap();
    assert_eq!(list.len(), 2);
    assert!(list.iter().all(|v| v["category"] == "stepPolicyCancelled"));
    let run = |args: &[&str], policy: bool| {
        Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
            .args(args)
            .env("DATABASE_URL", role_url.as_str())
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("SWALLOW_SLACK_SCOPED_ACCESS", "true")
            .env(
                "SWALLOW_SLACK_CHECK_POLICY_CANCELLED",
                if policy { "true" } else { "false" },
            )
            .env_remove("SWALLOW_SLACK_CHECK_MISSING_ENDPOINTS")
            .output()
            .unwrap()
    };
    assert_eq!(run(&["check-slack", "a"], false).status.code(), Some(0));
    let monitored = run(&["check-slack", "a"], true);
    assert_eq!(monitored.status.code(), Some(2));
    assert_eq!(
        serde_json::from_slice::<Value>(&monitored.stdout).unwrap()["policyCancelled"],
        2
    );
    db.execute("INSERT INTO public.novu_rust_alert_delivery_slots(environment_id,channel,target_id,state) VALUES ('a','slack','ops','rejected')",&[]).await.unwrap();
    let alert = || {
        Command::new(env!("CARGO_BIN_EXE_send-delivery-alert"))
            .args(["a", "300", "120", "60"])
            .env("DATABASE_URL", role_url.as_str())
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("SWALLOW_ALERT_RESEND_ENABLED", "false")
            .env("SWALLOW_ALERT_SLACK_POLICY_CANCELLED_ENABLED", "true")
            .env("SWALLOW_ALERT_SLACK_SCOPED_ACCESS", "true")
            .env("SWALLOW_ALERT_SCOPED_DELIVERY", "true")
            .env(
                "SWALLOW_ALERT_SLACK_WEBHOOK",
                "https://hooks.slack.com/services/A/B/C",
            )
            .env("SWALLOW_ALERT_SLACK_TARGET_ID", "ops")
            .env_remove("SWALLOW_ALERT_SLACK_WEBHOOK_FILE")
            .env_remove("SWALLOW_ALERT_EMAIL_TO")
            .output()
            .unwrap()
    };
    let out = alert();
    assert_eq!(
        out.status.code(),
        Some(2),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(String::from_utf8_lossy(&out.stderr).contains("requires operator review"));
    let (mut reviewer, driver) = tokio_postgres::connect(role_url.as_str(), NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    assert!(
        queue::slack_queue::acknowledge_workflow(&mut reviewer, "a", job, "operator", "ticket")
            .await
            .unwrap()
    );
    assert!(
        !queue::slack_queue::acknowledge_workflow(&mut reviewer, "a", job, "operator", "ticket")
            .await
            .unwrap()
    );
    assert_eq!(
        queue::slack_queue::policy_cancellations(&restricted, "a", true)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        run(
            &[
                "slack-ack",
                "a",
                &changed.to_string(),
                "operator",
                "reviewed"
            ],
            true
        )
        .status
        .code(),
        Some(0)
    );
    assert_eq!(
        queue::slack_queue::policy_cancellations(&restricted, "a", true)
            .await
            .unwrap(),
        0
    );
    assert_eq!(run(&["check-slack", "a"], true).status.code(), Some(0));
    assert_eq!(alert().status.code(), Some(0));
    // Existing failure and missing-endpoint acknowledgements remain supported.
    for (kind, status, reason) in [
        (
            queue::slack_workflow::FANOUT_KIND,
            "failed",
            "invalid Slack template",
        ),
        (
            queue::slack_workflow::FANOUT_KIND,
            "cancelled",
            "no Slack endpoints",
        ),
    ] {
        let id = Uuid::new_v4();
        queue::enqueue(&db, id, "a", kind, &json!({}))
            .await
            .unwrap();
        db.execute(
            "UPDATE public.novu_queue_jobs SET status=$2,last_error=$3 WHERE id=$1",
            &[&id, &status, &reason],
        )
        .await
        .unwrap();
        assert!(
            queue::slack_queue::acknowledge_workflow(&mut reviewer, "a", id, "operator", "ticket")
                .await
                .unwrap()
        );
    }
    assert_eq!(run(&["check-slack", "a"], true).status.code(), Some(0));
    // A new transition reopens attention, even with the same diagnostic.
    db.execute(
        "UPDATE public.novu_queue_jobs SET status='pending',last_error=NULL WHERE id=$1",
        &[&job],
    )
    .await
    .unwrap();
    db.execute("UPDATE public.novu_queue_jobs SET status='cancelled',last_error='Slack workflow step disabled' WHERE id=$1",&[&job]).await.unwrap();
    assert_eq!(
        queue::slack_queue::policy_cancellations(&restricted, "a", true)
            .await
            .unwrap(),
        1
    );
    db.batch_execute(&format!(
        "REVOKE EXECUTE ON FUNCTION public.novu_rust_slack_policy_cancelled(text) FROM {role}"
    ))
    .await
    .unwrap();
    assert!(
        queue::slack_queue::policy_cancellations(&restricted, "a", true)
            .await
            .is_err()
    );
    assert_eq!(run(&["check-slack", "a"], true).status.code(), Some(1));
    let failed = alert();
    assert_eq!(failed.status.code(), Some(1));
    assert!(
        String::from_utf8_lossy(&failed.stderr).contains("Slack policy cancellation check failed")
    );
}
