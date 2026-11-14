//! Proves the old index failure and upgrade without losing acknowledgement history.
use novu_postgres_queue::{self as queue, reconciliation, slack_queue, slack_workflow};
use serde_json::json;
use std::{fs, path::Path, process::Command};
use tokio_postgres::NoTls;
use uuid::Uuid;
#[tokio::test]
#[ignore = "requires disposable PostgreSQL"]
async fn long_diagnostics_can_be_acknowledged_after_upgrade() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let (mut db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let dir = std::env::temp_dir().join(format!("swallow-old-migrations-{}", Uuid::new_v4()));
    fs::create_dir(&dir).unwrap();
    for entry in fs::read_dir(root.join("migrations")).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name();
        if name.to_string_lossy().ends_with(".sql") && name.to_string_lossy().as_ref() < "0094" {
            fs::copy(entry.path(), dir.join(name)).unwrap();
        }
    }
    let api_dir = root.join("../../api/rust-api/migrations");
    let initial = Command::new(env!("CARGO_BIN_EXE_migrate-worker"))
        .args(["apply", "api", api_dir.to_str().unwrap(), "--through", "17"])
        .env("DATABASE_URL", &url)
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .output()
        .unwrap();
    assert!(
        initial.status.success(),
        "{}",
        String::from_utf8_lossy(&initial.stderr)
    );
    for (service, path) in [
        ("worker", dir.clone()),
        ("api", root.join("../../api/rust-api/migrations")),
    ] {
        let out = Command::new(env!("CARGO_BIN_EXE_migrate-worker"))
            .args(["apply", service, path.to_str().unwrap()])
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
    db.batch_execute("INSERT INTO novu_rust_environments VALUES ('a','org')")
        .await
        .unwrap();
    db.execute("INSERT INTO novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'a',session_user,decision,oid FROM pg_roles CROSS JOIN unnest(ARRAY['queue_ack','slack_ack','alert_view','slack_view']) decision WHERE rolname=session_user",&[]).await.unwrap();
    let short = Uuid::new_v4();
    let generic = Uuid::new_v4();
    let slack = Uuid::new_v4();
    for (id, kind) in [
        (short, "fixture"),
        (generic, "fixture"),
        (slack, slack_workflow::DELIVERY_KIND),
    ] {
        queue::enqueue(&db, id, "a", kind, &json!({}))
            .await
            .unwrap();
    }
    db.execute(
        "UPDATE novu_queue_jobs SET status='failed',last_error='old short' WHERE id=$1",
        &[&short],
    )
    .await
    .unwrap();
    assert!(
        reconciliation::acknowledge_failed_queue(&db, "a", short, "operator", "prior")
            .await
            .unwrap()
    );
    let diagnostic = (0..500)
        .map(|_| Uuid::new_v4().to_string())
        .collect::<Vec<_>>()
        .join("");
    assert!(diagnostic.len() > 16000);
    for id in [generic, slack] {
        db.execute(
            "UPDATE novu_queue_jobs SET status='failed',last_error=$2 WHERE id=$1",
            &[&id, &diagnostic],
        )
        .await
        .unwrap();
    }
    assert!(
        reconciliation::acknowledge_failed_queue(&db, "a", generic, "operator", "before")
            .await
            .is_err()
    );
    assert!(
        slack_queue::acknowledge_workflow(&mut db, "a", slack, "operator", "before")
            .await
            .is_err()
    );
    db.batch_execute(include_str!(
        "../migrations/0094_long_error_acknowledgements.sql"
    ))
    .await
    .unwrap();
    db.batch_execute(include_str!(
        "../migrations/0094_long_error_acknowledgements.sql"
    ))
    .await
    .unwrap();
    assert!(
        !reconciliation::acknowledge_failed_queue(&db, "a", short, "operator", "duplicate")
            .await
            .unwrap()
    );
    assert!(
        reconciliation::acknowledge_failed_queue(&db, "a", generic, "operator", "reviewed")
            .await
            .unwrap()
    );
    assert!(
        !reconciliation::acknowledge_failed_queue(&db, "a", generic, "operator", "duplicate")
            .await
            .unwrap()
    );
    assert!(
        slack_queue::acknowledge_workflow(&mut db, "a", slack, "operator", "reviewed")
            .await
            .unwrap()
    );
    assert!(
        !slack_queue::acknowledge_workflow(&mut db, "a", slack, "operator", "duplicate")
            .await
            .unwrap()
    );
    assert_eq!(
        db.query_one(
            "SELECT job_error FROM novu_rust_queue_failure_acknowledgements WHERE job_id=$1",
            &[&generic]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        diagnostic
    );
    assert_eq!(
        db.query_one(
            "SELECT job_error FROM novu_rust_slack_workflow_acknowledgements WHERE job_id=$1",
            &[&slack]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        diagnostic
    );
    assert_eq!(
        slack_queue::scoped_health(&db, "a").await.unwrap()["preDispatchFailures"],
        0
    );
    db.execute(
        "UPDATE novu_queue_jobs SET status='pending' WHERE id=$1",
        &[&slack],
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE novu_queue_jobs SET status='failed' WHERE id=$1",
        &[&slack],
    )
    .await
    .unwrap();
    assert_eq!(
        slack_queue::scoped_health(&db, "a").await.unwrap()["preDispatchFailures"],
        1
    );
    assert!(
        slack_queue::acknowledge_workflow(&mut db, "a", slack, "operator", "recurrence")
            .await
            .unwrap()
    );
    let role = format!("failure_ack_{}", Uuid::new_v4().simple());
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_ack_failed_queue(text,uuid,text,text),public.novu_rust_ack_slack_workflow(text,uuid,text,text) TO {role}")).await.unwrap();
    db.execute("INSERT INTO novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'a',$1,decision,oid FROM pg_roles CROSS JOIN unnest(ARRAY['queue_ack','slack_ack']) decision WHERE rolname::text=$1",&[&role]).await.unwrap();
    let (mut actor, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    actor
        .batch_execute(&format!("SET SESSION AUTHORIZATION {role}"))
        .await
        .unwrap();
    let limited = Uuid::new_v4();
    queue::enqueue(&db, limited, "a", slack_workflow::DELIVERY_KIND, &json!({}))
        .await
        .unwrap();
    db.execute(
        "UPDATE novu_queue_jobs SET status='failed',last_error=$2 WHERE id=$1",
        &[&limited, &diagnostic],
    )
    .await
    .unwrap();
    assert!(
        reconciliation::acknowledge_failed_queue(&actor, "a", limited, "operator", "restricted")
            .await
            .unwrap()
    );
    assert!(
        slack_queue::acknowledge_workflow(&mut actor, "a", limited, "operator", "restricted")
            .await
            .unwrap()
    );
    assert!(
        actor
            .query(
                "SELECT job_error FROM public.novu_rust_queue_failure_acknowledgements",
                &[]
            )
            .await
            .is_err()
    );
    actor
        .batch_execute("RESET SESSION AUTHORIZATION")
        .await
        .unwrap();
    db.execute(
        "DELETE FROM novu_rust_delivery_operator_grants WHERE database_principal=$1",
        &[&role],
    )
    .await
    .unwrap();
    db.batch_execute(&format!("DROP OWNED BY {role}; DROP ROLE {role}"))
        .await
        .unwrap();
    fs::remove_dir_all(dir).unwrap();
}
