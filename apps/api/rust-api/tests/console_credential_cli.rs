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
async fn issue_file_list_revoke_and_unknown_outcome_are_safe() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (mut db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    db.batch_execute("INSERT INTO public.novu_rust_environments VALUES ('cli-a','org'),('cli-b','org'); INSERT INTO public.novu_rust_subscribers(environment_id,subscriber_id,internal_id,profile) VALUES ('cli-a','alice','id-a','{\"firstName\":\"Alice\"}')").await.unwrap();
    let role = format!("console_cli_{}", Uuid::new_v4().simple());
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_inspect_console_credential(text,text),public.novu_rust_issue_console_credential(text,text,timestamptz,text),public.novu_rust_revoke_console_credential(text,uuid,text),public.novu_rust_list_console_credentials(text,uuid),public.novu_rust_list_console_credential_audit(text,bigint) TO {role}")).await.unwrap();
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
    let router = novu_rust_api::router_with_subscriber_secret(Arc::new(db), None);
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
            .len(),
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
