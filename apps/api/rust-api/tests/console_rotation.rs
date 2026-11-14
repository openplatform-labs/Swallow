//! Atomic console credential rotation against a disposable PostgreSQL instance.
#![cfg(unix)]
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use std::{process::Command, sync::Arc};
use tokio_postgres::{Client, NoTls};
use tower::ServiceExt;
use uuid::Uuid;

async fn connect(url: &str) -> Client {
    let (client, driver) = tokio_postgres::connect(url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    client
}

#[tokio::test]
#[ignore = "requires migrated disposable PostgreSQL and role creation"]
async fn rotation_preserves_scopes_and_rolls_back_on_failure() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let db = connect(&url).await;
    db.batch_execute(
        "INSERT INTO public.novu_rust_environments VALUES ('rotate-a','org'),('rotate-b','org')",
    )
    .await
    .unwrap();
    let role = format!("rotate_{}", Uuid::new_v4().simple());
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_issue_console_credential_permissions(text,text,timestamptz,boolean,boolean,text),public.novu_rust_rotate_console_credential(text,uuid,text,timestamptz,text),public.novu_rust_list_console_rotation_audit(text,bigint) TO {role}")).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_console_credential_admins(environment_id,database_principal,principal_oid) SELECT 'rotate-a',rolname,oid FROM pg_roles WHERE rolname::text=$1", &[&role]).await.unwrap();
    let actor_url = url.replacen("postgres://postgres@", &format!("postgres://{role}@"), 1);
    let directory = std::env::temp_dir().join(format!("swallow-rotation-{}", Uuid::new_v4()));
    std::fs::create_dir(&directory).unwrap();
    let run = |args: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_console-credential"))
            .args(args)
            .env("DATABASE_URL", &actor_url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .output()
            .unwrap()
    };
    let router = novu_rust_api::router(Arc::new(connect(&url).await));
    let mut both_id = Uuid::nil();
    for (choice, subscriber, alert) in [
        ("none", false, false),
        ("subscriber", true, false),
        ("alert", false, true),
        ("both", true, true),
    ] {
        let old_file = directory.join(format!("{choice}-old"));
        let new_file = directory.join(format!("{choice}-new"));
        let old_file_str = old_file.to_str().unwrap();
        let new_file_str = new_file.to_str().unwrap();
        let issue = run(&[
            "issue-permissions",
            "rotate-a",
            "3600",
            old_file_str,
            choice,
            "issue",
        ]);
        assert!(issue.status.success());
        let old =
            serde_json::from_slice::<serde_json::Value>(&issue.stdout).unwrap()["credentialId"]
                .as_str()
                .unwrap()
                .to_owned();
        let rotate = run(&[
            "rotate",
            "rotate-a",
            &old,
            "3600",
            new_file_str,
            "planned rotation",
        ]);
        assert!(
            rotate.status.success(),
            "{}",
            String::from_utf8_lossy(&rotate.stderr)
        );
        let new =
            serde_json::from_slice::<serde_json::Value>(&rotate.stdout).unwrap()["credentialId"]
                .as_str()
                .unwrap()
                .to_owned();
        if choice == "both" {
            both_id = Uuid::parse_str(&new).unwrap();
        }
        let old_token = std::fs::read_to_string(&old_file).unwrap();
        let new_token = std::fs::read_to_string(&new_file).unwrap();
        assert_ne!(old_token, new_token);
        for (endpoint, allowed) in [("subscribers", subscriber), ("alerts", alert)] {
            for (token, expected) in [(&old_token, false), (&new_token, allowed)] {
                let req = Request::builder()
                    .uri(format!("/v1/experimental/console/{endpoint}"))
                    .header("authorization", format!("SwallowConsole {token}"))
                    .body(Body::empty())
                    .unwrap();
                assert_eq!(
                    router.clone().oneshot(req).await.unwrap().status(),
                    if expected {
                        StatusCode::OK
                    } else {
                        StatusCode::UNAUTHORIZED
                    }
                );
            }
        }
        let again = directory.join(format!("{choice}-again"));
        assert!(
            !run(&[
                "rotate",
                "rotate-a",
                &old,
                "3600",
                again.to_str().unwrap(),
                "duplicate"
            ])
            .status
            .success()
        );
        std::fs::remove_file(again).unwrap();
        std::fs::remove_file(old_file).unwrap();
        std::fs::remove_file(new_file).unwrap();
    }
    let audit = run(&["rotation-audit", "rotate-a"]);
    assert!(audit.status.success());
    let rows = serde_json::from_slice::<serde_json::Value>(&audit.stdout).unwrap();
    assert_eq!(rows.as_array().unwrap().len(), 4);
    assert!(
        rows.as_array()
            .unwrap()
            .iter()
            .any(|row| row["newCredentialId"] == both_id.to_string())
    );
    assert!(!run(&["rotation-audit", "rotate-b"]).status.success());
    db.batch_execute("CREATE FUNCTION public.test_rotation_reject_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture'; END $$; CREATE TRIGGER test_rotation_reject_audit BEFORE INSERT ON public.novu_rust_console_rotation_audit FOR EACH ROW EXECUTE FUNCTION public.test_rotation_reject_audit()").await.unwrap();
    let failed = directory.join("failed");
    assert!(
        !run(&[
            "rotate",
            "rotate-a",
            &both_id.to_string(),
            "3600",
            failed.to_str().unwrap(),
            "fail"
        ])
        .status
        .success()
    );
    assert!(
        db.query_one(
            "SELECT enabled FROM public.novu_rust_console_credentials WHERE credential_id=$1",
            &[&both_id]
        )
        .await
        .unwrap()
        .get::<_, bool>(0)
    );
    let failed_hash = format!(
        "{:x}",
        sha2::Sha256::digest(std::fs::read(&failed).unwrap())
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM public.novu_rust_console_credentials WHERE token_hash=$1",
            &[&failed_hash]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    db.batch_execute("DROP TRIGGER test_rotation_reject_audit ON public.novu_rust_console_rotation_audit; DROP FUNCTION public.test_rotation_reject_audit()").await.unwrap();
    std::fs::remove_file(failed).unwrap();
    std::fs::remove_dir(directory).unwrap();
    let a = connect(&actor_url).await;
    let b = connect(&actor_url).await;
    let hash_a = "a".repeat(64);
    let hash_b = "b".repeat(64);
    let args_a: &[&(dyn tokio_postgres::types::ToSql + Sync)] = &[&both_id, &hash_a];
    let args_b: &[&(dyn tokio_postgres::types::ToSql + Sync)] = &[&both_id, &hash_b];
    let (one,two) = tokio::join!(a.query_one("SELECT public.novu_rust_rotate_console_credential('rotate-a',$1,$2,clock_timestamp()+interval '1 hour','race')", args_a), b.query_one("SELECT public.novu_rust_rotate_console_credential('rotate-a',$1,$2,clock_timestamp()+interval '1 hour','race')", args_b));
    assert_ne!(one.is_ok(), two.is_ok());
}
use sha2::Digest;

#[tokio::test]
#[ignore = "requires migrated disposable PostgreSQL and role creation"]
async fn overlap_expires_and_cannot_be_rotated_twice() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let db = connect(&url).await;
    db.batch_execute(
        "INSERT INTO public.novu_rust_environments VALUES ('overlap-a','org'),('overlap-b','org')",
    )
    .await
    .unwrap();
    let role = format!("overlap_{}", Uuid::new_v4().simple());
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_issue_console_credential_permissions(text,text,timestamptz,boolean,boolean,text),public.novu_rust_rotate_console_credential(text,uuid,text,timestamptz,text),public.novu_rust_rotate_console_credential_overlap(text,uuid,text,timestamptz,integer,text),public.novu_rust_list_console_rotation_audit_v2(text,bigint) TO {role}")).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_console_credential_admins(environment_id,database_principal,principal_oid) SELECT 'overlap-a',rolname,oid FROM pg_roles WHERE rolname::text=$1", &[&role]).await.unwrap();
    let actor_url = url.replacen("postgres://postgres@", &format!("postgres://{role}@"), 1);
    let run = |args: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_console-credential"))
            .args(args)
            .env("DATABASE_URL", &actor_url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .output()
            .unwrap()
    };
    let directory = std::env::temp_dir().join(format!("swallow-overlap-{}", Uuid::new_v4()));
    std::fs::create_dir(&directory).unwrap();
    let old_path = directory.join("old");
    let new_path = directory.join("new");
    let result = run(&[
        "issue-permissions",
        "overlap-a",
        "3600",
        old_path.to_str().unwrap(),
        "both",
        "issue",
    ]);
    assert!(result.status.success());
    let old_id =
        serde_json::from_slice::<serde_json::Value>(&result.stdout).unwrap()["credentialId"]
            .as_str()
            .unwrap()
            .to_owned();
    let result = run(&[
        "rotate-overlap",
        "overlap-a",
        &old_id,
        "3600",
        "5",
        new_path.to_str().unwrap(),
        "cutover",
    ]);
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let result: serde_json::Value = serde_json::from_slice(&result.stdout).unwrap();
    assert!(result["oldValidUntil"].is_string());
    let new_id = result["credentialId"].as_str().unwrap();
    let router = novu_rust_api::router(Arc::new(connect(&url).await));
    let old_token = std::fs::read_to_string(&old_path).unwrap();
    let new_token = std::fs::read_to_string(&new_path).unwrap();
    let request = |token: &str| {
        Request::builder()
            .uri("/v1/experimental/console/alerts")
            .header("authorization", format!("SwallowConsole {token}"))
            .body(Body::empty())
            .unwrap()
    };
    assert_eq!(
        router
            .clone()
            .oneshot(request(&old_token))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        router
            .clone()
            .oneshot(request(&new_token))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    let retry = directory.join("retry");
    assert!(
        !run(&[
            "rotate",
            "overlap-a",
            &old_id,
            "3600",
            retry.to_str().unwrap(),
            "duplicate"
        ])
        .status
        .success()
    );
    let audit = run(&["rotation-audit-v2", "overlap-a"]);
    let rows: serde_json::Value = serde_json::from_slice(&audit.stdout).unwrap();
    assert_eq!(rows[0]["newCredentialId"], new_id);
    assert_eq!(rows[0]["oldValidUntil"], result["oldValidUntil"]);
    assert!(!run(&["rotation-audit-v2", "overlap-b"]).status.success());
    tokio::time::sleep(std::time::Duration::from_secs(6)).await;
    assert_eq!(
        router
            .clone()
            .oneshot(request(&old_token))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        router.oneshot(request(&new_token)).await.unwrap().status(),
        StatusCode::OK
    );
    let bounded = directory.join("bounded");
    let replacement = directory.join("replacement");
    let issued = run(&[
        "issue-permissions",
        "overlap-a",
        "3600",
        bounded.to_str().unwrap(),
        "none",
        "issue",
    ]);
    let bounded_id =
        serde_json::from_slice::<serde_json::Value>(&issued.stdout).unwrap()["credentialId"]
            .as_str()
            .unwrap()
            .to_owned();
    let bounded_uuid = Uuid::parse_str(&bounded_id).unwrap();
    db.execute("UPDATE public.novu_rust_console_credentials SET expires_at=clock_timestamp()+interval '30 seconds' WHERE credential_id=$1", &[&bounded_uuid]).await.unwrap();
    let original: String = db.query_one("SELECT expires_at::text FROM public.novu_rust_console_credentials WHERE credential_id=$1", &[&bounded_uuid]).await.unwrap().get(0);
    db.batch_execute("CREATE FUNCTION public.test_overlap_reject_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture'; END $$; CREATE TRIGGER test_overlap_reject_audit BEFORE INSERT ON public.novu_rust_console_rotation_audit FOR EACH ROW EXECUTE FUNCTION public.test_overlap_reject_audit()").await.unwrap();
    assert!(
        !run(&[
            "rotate-overlap",
            "overlap-a",
            &bounded_id,
            "3600",
            "1",
            replacement.to_str().unwrap(),
            "failure"
        ])
        .status
        .success()
    );
    assert_eq!(db.query_one("SELECT expires_at::text FROM public.novu_rust_console_credentials WHERE credential_id=$1", &[&bounded_uuid]).await.unwrap().get::<_,String>(0),original);
    let failed_hash = format!(
        "{:x}",
        sha2::Sha256::digest(std::fs::read(&replacement).unwrap())
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM public.novu_rust_console_credentials WHERE token_hash=$1",
            &[&failed_hash]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    db.batch_execute("DROP TRIGGER test_overlap_reject_audit ON public.novu_rust_console_rotation_audit; DROP FUNCTION public.test_overlap_reject_audit()").await.unwrap();
    std::fs::remove_file(&replacement).unwrap();
    let bounded_result = run(&[
        "rotate-overlap",
        "overlap-a",
        &bounded_id,
        "3600",
        "3600",
        replacement.to_str().unwrap(),
        "bounded",
    ]);
    assert!(bounded_result.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&bounded_result.stdout).unwrap()["oldValidUntil"],
        original
    );
    std::fs::remove_file(bounded).unwrap();
    std::fs::remove_file(replacement).unwrap();
    for path in [old_path, new_path, retry] {
        std::fs::remove_file(path).unwrap();
    }
    std::fs::remove_dir(directory).unwrap();
}
