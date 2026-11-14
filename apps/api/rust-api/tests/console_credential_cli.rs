//! Scoped console credential CLI with real PostgreSQL and isolated temporary files.
#![cfg(unix)]
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use sha2::{Digest, Sha256};
use std::{os::unix::fs::PermissionsExt, process::Command, sync::Arc};
use tokio_postgres::NoTls;
use tower::ServiceExt;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires migrated disposable PostgreSQL and role creation"]
async fn initial_permissions_are_atomic_and_independent() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    db.batch_execute(
        "INSERT INTO public.novu_rust_environments VALUES ('initial-a','org'),('initial-b','org')",
    )
    .await
    .unwrap();
    let role = format!("console_initial_{}", Uuid::new_v4().simple());
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_issue_console_credential_permissions(text,text,timestamptz,boolean,boolean,text) TO {role}")).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_console_credential_admins(environment_id,database_principal,principal_oid) SELECT 'initial-a',rolname,oid FROM pg_roles WHERE rolname::text=$1", &[&role]).await.unwrap();
    let actor_url = url.replacen("postgres://postgres@", &format!("postgres://{role}@"), 1);
    let directory = std::env::temp_dir().join(format!("swallow-initial-{}", Uuid::new_v4()));
    std::fs::create_dir(&directory).unwrap();
    let (api_db, api_driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { api_driver.await.unwrap() });
    let router = novu_rust_api::router(Arc::new(api_db));
    let run = |env: &str, choice: &str, path: &std::path::Path| {
        Command::new(env!("CARGO_BIN_EXE_console-credential"))
            .args([
                "issue-permissions",
                env,
                "3600",
                path.to_str().unwrap(),
                choice,
                "initial-test",
            ])
            .env("DATABASE_URL", &actor_url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .output()
            .unwrap()
    };
    for (choice, subscriber, alert) in [
        ("none", false, false),
        ("subscriber", true, false),
        ("alert", false, true),
        ("both", true, true),
    ] {
        let path = directory.join(choice);
        let result = run("initial-a", choice, &path);
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        let value: serde_json::Value = serde_json::from_slice(&result.stdout).unwrap();
        let id = Uuid::parse_str(value["credentialId"].as_str().unwrap()).unwrap();
        let token = std::fs::read_to_string(&path).unwrap();
        let hash = format!("{:x}", Sha256::digest(token.as_bytes()));
        let row = db.query_one("SELECT c.subscriber_read, EXISTS(SELECT 1 FROM public.novu_rust_console_alert_grants g WHERE g.credential_id=c.credential_id), (SELECT count(*) FROM public.novu_rust_console_credential_audit a WHERE a.credential_id=c.credential_id), (SELECT count(*) FROM public.novu_rust_console_subscriber_scope_audit a WHERE a.credential_id=c.credential_id), (SELECT count(*) FROM public.novu_rust_console_alert_grant_audit a WHERE a.credential_id=c.credential_id) FROM public.novu_rust_console_credentials c WHERE c.credential_id=$1", &[&id]).await.unwrap();
        assert_eq!(
            (row.get::<_, bool>(0), row.get::<_, bool>(1)),
            (subscriber, alert)
        );
        assert_eq!(
            (
                row.get::<_, i64>(2),
                row.get::<_, i64>(3),
                row.get::<_, i64>(4)
            ),
            (1, 1, i64::from(alert))
        );
        assert_eq!(db.query_one("SELECT token_hash=$2 FROM public.novu_rust_console_credentials WHERE credential_id=$1", &[&id,&hash]).await.unwrap().get::<_,bool>(0), true);
        for (endpoint, allowed) in [("subscribers", subscriber), ("alerts", alert)] {
            let request = Request::builder()
                .uri(format!("/v1/experimental/console/{endpoint}"))
                .header("authorization", format!("SwallowConsole {token}"))
                .body(Body::empty())
                .unwrap();
            let status = router.clone().oneshot(request).await.unwrap().status();
            assert_eq!(
                status,
                if allowed {
                    StatusCode::OK
                } else {
                    StatusCode::UNAUTHORIZED
                }
            );
        }
        std::fs::remove_file(path).unwrap();
    }
    db.batch_execute("CREATE FUNCTION public.test_initial_reject_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture'; END $$; CREATE TRIGGER test_initial_reject_audit BEFORE INSERT ON public.novu_rust_console_alert_grant_audit FOR EACH ROW EXECUTE FUNCTION public.test_initial_reject_audit()").await.unwrap();
    let failed = directory.join("failed");
    assert!(!run("initial-a", "both", &failed).status.success());
    let hash = format!("{:x}", Sha256::digest(std::fs::read(&failed).unwrap()));
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM public.novu_rust_console_credentials WHERE token_hash=$1",
            &[&hash]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    db.batch_execute("DROP TRIGGER test_initial_reject_audit ON public.novu_rust_console_alert_grant_audit; DROP FUNCTION public.test_initial_reject_audit()").await.unwrap();
    let denied = directory.join("denied");
    assert!(!run("initial-b", "both", &denied).status.success());
    let invalid = directory.join("invalid");
    assert!(!run("initial-a", "all", &invalid).status.success());
    assert!(!invalid.exists());
    std::fs::remove_file(failed).unwrap();
    std::fs::remove_file(denied).unwrap();
    std::fs::remove_dir(directory).unwrap();
}

#[tokio::test]
#[ignore = "requires migrated disposable PostgreSQL and role creation"]
async fn issue_file_list_revoke_and_unknown_outcome_are_safe() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (mut db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    db.batch_execute("INSERT INTO public.novu_rust_environments VALUES ('cli-a','org'),('cli-b','org'); INSERT INTO public.novu_rust_subscribers(environment_id,subscriber_id,internal_id,profile) VALUES ('cli-a','alice','id-a','{\"firstName\":\"Alice\"}')").await.unwrap();
    let role = format!("console_cli_{}", Uuid::new_v4().simple());
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_issue_console_credential_scoped(text,text,timestamptz,boolean,text),public.novu_rust_manage_console_subscriber_scope(text,uuid,text,text),public.novu_rust_list_console_subscriber_scopes(text,uuid),public.novu_rust_list_console_subscriber_scope_audit(text,bigint),public.novu_rust_manage_console_alert_grant(text,uuid,text,text),public.novu_rust_list_console_alert_grants(text,uuid),public.novu_rust_list_console_alert_grant_audit(text,bigint),public.novu_rust_inspect_console_credential(text,text),public.novu_rust_issue_console_credential(text,text,timestamptz,text),public.novu_rust_revoke_console_credential(text,uuid,text),public.novu_rust_list_console_credentials(text,uuid),public.novu_rust_list_console_credential_audit(text,bigint) TO {role}")).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_console_credential_admins(environment_id,database_principal,principal_oid) SELECT 'cli-a',rolname,oid FROM pg_roles WHERE rolname::text=$1",&[&role]).await.unwrap();
    let actor_url = url.replacen("postgres://postgres@", &format!("postgres://{role}@"), 1);
    let run = |args: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_console-credential"))
            .args(args)
            .env("DATABASE_URL", &actor_url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .output()
            .unwrap()
    };
    let directory = std::env::temp_dir().join(format!("swallow-cli-{}", Uuid::new_v4()));
    std::fs::create_dir(&directory).unwrap();
    let path = directory.join("token");
    let path_string = path.to_str().unwrap();
    let out = run(&["issue", "cli-a", "3600", path_string, "test-ticket"]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let token = std::fs::read_to_string(&path).unwrap();
    assert_eq!(token.len(), 64);
    assert!(
        token
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
    );
    assert_eq!(
        std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    assert!(!String::from_utf8_lossy(&out.stdout).contains(&token));
    assert!(!String::from_utf8_lossy(&out.stderr).contains(&token));
    let result: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    let id = result["credentialId"].as_str().unwrap();
    let inspected = run(&["inspect", "cli-a", path_string]);
    assert!(inspected.status.success());
    let found: serde_json::Value = serde_json::from_slice(&inspected.stdout).unwrap();
    assert_eq!(found["found"], true);
    assert_eq!(found["credentialId"], id);
    assert_eq!(found["enabled"], true);
    assert!(!String::from_utf8_lossy(&inspected.stdout).contains(&token));
    assert!(!run(&["inspect", "cli-b", path_string]).status.success());
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
    assert!(!run(&["inspect", "cli-a", path_string]).status.success());
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    let pending_path = directory.join("pending");
    let pending_token = "f".repeat(64);
    std::fs::write(&pending_path, &pending_token).unwrap();
    std::fs::set_permissions(&pending_path, std::fs::Permissions::from_mode(0o600)).unwrap();
    let pending_hash = format!("{:x}", Sha256::digest(pending_token.as_bytes()));
    let tx = db.transaction().await.unwrap();
    tx.execute("INSERT INTO public.novu_rust_console_credentials(token_hash,environment_id,expires_at) VALUES ($1,'cli-a',clock_timestamp()+interval '1 hour')",&[&pending_hash]).await.unwrap();
    let while_pending = run(&["inspect", "cli-a", pending_path.to_str().unwrap()]);
    assert!(while_pending.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&while_pending.stdout).unwrap()["found"],
        false
    );
    tx.commit().await.unwrap();
    let after_commit = run(&["inspect", "cli-a", pending_path.to_str().unwrap()]);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&after_commit.stdout).unwrap()["found"],
        true
    );
    std::fs::remove_file(pending_path).unwrap();
    let duplicate = run(&["issue", "cli-a", "3600", path_string, "duplicate"]);
    assert!(!duplicate.status.success());
    assert_eq!(std::fs::read_to_string(&path).unwrap(), token);
    let link = directory.join("existing-link");
    std::os::unix::fs::symlink(&path, &link).unwrap();
    let symlink = run(&["issue", "cli-a", "3600", link.to_str().unwrap(), "symlink"]);
    assert!(!symlink.status.success());
    assert!(
        !run(&["inspect", "cli-a", link.to_str().unwrap()])
            .status
            .success()
    );
    assert_eq!(std::fs::read_to_string(&path).unwrap(), token);
    let invalid = directory.join("invalid-lifetime");
    assert!(
        !run(&[
            "issue",
            "cli-a",
            "2592001",
            invalid.to_str().unwrap(),
            "invalid"
        ])
        .status
        .success()
    );
    assert!(!invalid.exists());
    assert!(!run(&["list", "cli-b"]).status.success());
    std::fs::remove_file(link).unwrap();
    let listing = run(&["list", "cli-a"]);
    assert!(listing.status.success());
    let list: serde_json::Value = serde_json::from_slice(&listing.stdout).unwrap();
    assert!(
        list.as_array()
            .unwrap()
            .iter()
            .any(|item| item["credentialId"] == id)
    );
    assert!(
        !listing
            .stdout
            .windows(token.len())
            .any(|v| v == token.as_bytes())
    );
    // A failed audit write must roll the permission change back.
    db.batch_execute("CREATE FUNCTION public.test_reject_alert_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture'; END $$; CREATE TRIGGER test_reject_alert_audit BEFORE INSERT ON public.novu_rust_console_alert_grant_audit FOR EACH ROW EXECUTE FUNCTION public.test_reject_alert_audit()").await.unwrap();
    let failed = run(&["alert-access", "cli-a", id, "grant", "test"]);
    assert!(!failed.status.success());
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM public.novu_rust_console_alert_grants WHERE credential_id=$1",
            &[&Uuid::parse_str(id).unwrap()]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    db.batch_execute("DROP TRIGGER test_reject_alert_audit ON public.novu_rust_console_alert_grant_audit; DROP FUNCTION public.test_reject_alert_audit()").await.unwrap();
    assert!(
        !run(&["alert-access", "cli-b", id, "grant", "test"])
            .status
            .success()
    );
    let grant = run(&["alert-access", "cli-a", id, "grant", "test-ticket"]);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&grant.stdout).unwrap()["changed"],
        true
    );
    let again = run(&["alert-access", "cli-a", id, "grant", "test-ticket"]);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&again.stdout).unwrap()["changed"],
        false
    );
    let list = run(&["alert-access-list", "cli-a"]);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&list.stdout).unwrap()[0]["credentialId"],
        id
    );
    assert!(!run(&["alert-access-list", "cli-b"]).status.success());
    let grant_audit = run(&["alert-access-audit", "cli-a"]);
    let audit: serde_json::Value = serde_json::from_slice(&grant_audit.stdout).unwrap();
    assert_eq!(audit.as_array().unwrap().len(), 1);
    assert_eq!(audit[0]["action"], "grant");
    assert_eq!(audit[0]["actor"], role);
    assert!(!String::from_utf8_lossy(&grant_audit.stdout).contains(&token));
    let revoke = run(&["alert-access", "cli-a", id, "revoke", "test-ticket"]);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&revoke.stdout).unwrap()["changed"],
        true
    );
    let audit = run(&["alert-access-audit", "cli-a"]);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&audit.stdout)
            .unwrap()
            .as_array()
            .unwrap()
            .iter()
            .filter(|v| v["credentialId"] == id)
            .count(),
        2
    );
    // A revocation waits for an in-flight read holding the credential row.
    assert!(
        run(&["alert-access", "cli-a", id, "grant", "lock-test"])
            .status
            .success()
    );
    let tx = db.transaction().await.unwrap();
    let credential_id = Uuid::parse_str(id).unwrap();
    tx.query_one("SELECT credential_id FROM public.novu_rust_console_credentials WHERE credential_id=$1 FOR SHARE", &[&credential_id]).await.unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_console-credential"))
        .args(["alert-access", "cli-a", id, "revoke", "lock-test"])
        .env("DATABASE_URL", &actor_url)
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert!(child.try_wait().unwrap().is_none());
    tx.commit().await.unwrap();
    let finished = child.wait_with_output().unwrap();
    assert!(finished.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&finished.stdout).unwrap()["changed"],
        true
    );
    // Scope choice and issuance audit are atomic even if the scope audit fails.
    db.batch_execute("CREATE FUNCTION public.test_reject_scope_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture'; END $$; CREATE TRIGGER test_reject_scope_audit BEFORE INSERT ON public.novu_rust_console_subscriber_scope_audit FOR EACH ROW EXECUTE FUNCTION public.test_reject_scope_audit()").await.unwrap();
    let failed_path = directory.join("scope-failure");
    let failed_scope = run(&[
        "issue-scoped",
        "cli-a",
        "3600",
        failed_path.to_str().unwrap(),
        "none",
        "failure-test",
    ]);
    assert!(!failed_scope.status.success());
    let failed_secret = std::fs::read_to_string(&failed_path).unwrap();
    let failed_hash = format!("{:x}", Sha256::digest(failed_secret.as_bytes()));
    assert!(
        db.query_opt(
            "SELECT 1 FROM public.novu_rust_console_credentials WHERE token_hash=$1",
            &[&failed_hash]
        )
        .await
        .unwrap()
        .is_none()
    );
    std::fs::remove_file(failed_path).unwrap();
    db.batch_execute("DROP TRIGGER test_reject_scope_audit ON public.novu_rust_console_subscriber_scope_audit; DROP FUNCTION public.test_reject_scope_audit()").await.unwrap();
    let scoped_path = directory.join("alert-only");
    let scoped = run(&[
        "issue-scoped",
        "cli-a",
        "3600",
        scoped_path.to_str().unwrap(),
        "none",
        "alert-only",
    ]);
    assert!(scoped.status.success());
    let scoped_id =
        serde_json::from_slice::<serde_json::Value>(&scoped.stdout).unwrap()["credentialId"]
            .as_str()
            .unwrap()
            .to_owned();
    let scoped_token = std::fs::read_to_string(&scoped_path).unwrap();
    let listing = run(&["subscriber-access-list", "cli-a"]);
    let scopes: serde_json::Value = serde_json::from_slice(&listing.stdout).unwrap();
    assert!(
        scopes
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v["credentialId"] == scoped_id && v["subscriberRead"] == false)
    );
    assert!(
        !run(&["subscriber-access", "cli-b", &scoped_id, "grant", "test"])
            .status
            .success()
    );
    let audit = run(&["subscriber-access-audit", "cli-a"]);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&audit.stdout).unwrap()[0]["action"],
        "initial_none"
    );
    db.batch_execute("CREATE FUNCTION public.test_reject_scope_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture'; END $$; CREATE TRIGGER test_reject_scope_audit BEFORE INSERT ON public.novu_rust_console_subscriber_scope_audit FOR EACH ROW EXECUTE FUNCTION public.test_reject_scope_audit()").await.unwrap();
    assert!(
        !run(&[
            "subscriber-access",
            "cli-a",
            &scoped_id,
            "grant",
            "failure-test"
        ])
        .status
        .success()
    );
    let scoped_uuid = Uuid::parse_str(&scoped_id).unwrap();
    assert!(!db.query_one("SELECT subscriber_read FROM public.novu_rust_console_credentials WHERE credential_id=$1", &[&scoped_uuid]).await.unwrap().get::<_,bool>(0));
    db.batch_execute("DROP TRIGGER test_reject_scope_audit ON public.novu_rust_console_subscriber_scope_audit; DROP FUNCTION public.test_reject_scope_audit()").await.unwrap();
    let subscriber_path = directory.join("subscriber-only");
    let subscriber_result = run(&[
        "issue-scoped",
        "cli-a",
        "3600",
        subscriber_path.to_str().unwrap(),
        "subscriber",
        "subscriber-only",
    ]);
    assert!(subscriber_result.status.success());
    let subscriber_token = std::fs::read_to_string(&subscriber_path).unwrap();
    let router = novu_rust_api::router_with_subscriber_secret(Arc::new(db), None);
    let subscriber_request = |path: &str| {
        Request::builder()
            .uri(path)
            .header(
                "authorization",
                format!("SwallowConsole {subscriber_token}"),
            )
            .body(Body::empty())
            .unwrap()
    };
    assert_eq!(
        router
            .clone()
            .oneshot(subscriber_request("/v1/experimental/console/subscribers"))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        router
            .clone()
            .oneshot(subscriber_request("/v1/experimental/console/alerts"))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    std::fs::remove_file(subscriber_path).unwrap();
    let scoped_request = |path: &str| {
        Request::builder()
            .uri(path)
            .header("authorization", format!("SwallowConsole {scoped_token}"))
            .body(Body::empty())
            .unwrap()
    };
    assert_eq!(
        router
            .clone()
            .oneshot(scoped_request("/v1/experimental/console/subscribers"))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert!(
        run(&["alert-access", "cli-a", &scoped_id, "grant", "test"])
            .status
            .success()
    );
    assert_eq!(
        router
            .clone()
            .oneshot(scoped_request("/v1/experimental/console/alerts"))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        router
            .clone()
            .oneshot(scoped_request("/v1/experimental/console/subscribers"))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert!(
        run(&["subscriber-access", "cli-a", &scoped_id, "grant", "test"])
            .status
            .success()
    );
    assert_eq!(
        router
            .clone()
            .oneshot(scoped_request("/v1/experimental/console/subscribers"))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert!(
        run(&["subscriber-access", "cli-a", &scoped_id, "revoke", "test"])
            .status
            .success()
    );
    assert_eq!(
        router
            .clone()
            .oneshot(scoped_request("/v1/experimental/console/subscribers"))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        router
            .clone()
            .oneshot(scoped_request("/v1/experimental/console/alerts"))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    let scope_audit = run(&["subscriber-access-audit", "cli-a"]);
    let scope_events: serde_json::Value = serde_json::from_slice(&scope_audit.stdout).unwrap();
    assert_eq!(
        scope_events
            .as_array()
            .unwrap()
            .iter()
            .filter(|v| v["credentialId"] == scoped_id)
            .count(),
        3
    );
    assert!(!String::from_utf8_lossy(&scope_audit.stdout).contains(&scoped_token));
    std::fs::remove_file(scoped_path).unwrap();
    let alert_request = || {
        Request::builder()
            .uri("/v1/experimental/console/alerts")
            .header("authorization", format!("SwallowConsole {token}"))
            .body(Body::empty())
            .unwrap()
    };
    assert_eq!(
        router
            .clone()
            .oneshot(alert_request())
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert!(
        run(&["alert-access", "cli-a", id, "grant", "http-test"])
            .status
            .success()
    );
    assert_eq!(
        router
            .clone()
            .oneshot(alert_request())
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert!(
        run(&["alert-access", "cli-a", id, "revoke", "http-test"])
            .status
            .success()
    );
    assert_eq!(
        router
            .clone()
            .oneshot(alert_request())
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    let request = || {
        Request::builder()
            .uri("/v1/experimental/console/subscribers")
            .header("authorization", format!("SwallowConsole {token}"))
            .body(Body::empty())
            .unwrap()
    };
    assert_eq!(
        router.clone().oneshot(request()).await.unwrap().status(),
        StatusCode::OK
    );
    let revoked = run(&["revoke", "cli-a", id, "test-review"]);
    assert!(revoked.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&revoked.stdout).unwrap()["changed"],
        true
    );
    assert!(
        !run(&["alert-access", "cli-a", id, "grant", "test"])
            .status
            .success()
    );
    let inspected = run(&["inspect", "cli-a", path_string]);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&inspected.stdout).unwrap()["enabled"],
        false
    );
    assert_eq!(
        router.oneshot(request()).await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
    let audit = run(&["audit", "cli-a"]);
    assert!(audit.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&audit.stdout)
            .unwrap()
            .as_array()
            .unwrap()
            .iter()
            .filter(|v| v["credentialId"] == id)
            .count(),
        2
    );
    let denied = directory.join("denied");
    let denied_str = denied.to_str().unwrap();
    let out = run(&["issue", "cli-b", "3600", denied_str, "denied"]);
    assert!(!out.status.success());
    assert!(denied.exists());
    assert_eq!(
        std::fs::metadata(&denied).unwrap().permissions().mode() & 0o777,
        0o600
    );
    let denied_token = std::fs::read_to_string(&denied).unwrap();
    assert!(!String::from_utf8_lossy(&out.stderr).contains(&denied_token));
    assert!(String::from_utf8_lossy(&out.stderr).contains("file retained"));
    std::fs::remove_file(path).unwrap();
    std::fs::remove_file(denied).unwrap();
    std::fs::remove_dir(directory).unwrap();
}
