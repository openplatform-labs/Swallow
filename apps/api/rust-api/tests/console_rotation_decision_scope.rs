//! Scoped decision permission rotation, using disposable PostgreSQL.
#![cfg(unix)]
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use sha2::{Digest, Sha256};
use std::{process::Command, sync::Arc};
use tokio_postgres::{Client, NoTls};
use tower::ServiceExt;
use uuid::Uuid;

async fn connect(url: &str) -> Client {
    let (db, driver) = tokio_postgres::connect(url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    db
}

#[tokio::test]
#[ignore = "requires migrated disposable PostgreSQL and role creation"]
async fn rotation_copies_only_selected_scopes_atomically() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let db = connect(&url).await;
    db.batch_execute("INSERT INTO public.novu_rust_environments VALUES ('scope-rotation','org'),('scope-foreign','org')").await.unwrap();
    let role = format!("rotation_scope_{}", Uuid::new_v4().simple());
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_rotate_console_credential_scoped(text,uuid,text,timestamptz,integer,text,text) TO {role}")).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_console_credential_admins SELECT 'scope-rotation',rolname,oid FROM pg_roles WHERE rolname::text=$1", &[&role]).await.unwrap();
    let actor_url = url.replacen("postgres://postgres@", &format!("postgres://{role}@"), 1);
    let dir = std::env::temp_dir().join(format!("swallow-scope-{}", Uuid::new_v4()));
    std::fs::create_dir(&dir).unwrap();
    let run = |old: Uuid, grace: &str, file: &str, scope: &str| {
        Command::new(env!("CARGO_BIN_EXE_console-credential"))
            .args([
                "rotate-scoped",
                "scope-rotation",
                &old.to_string(),
                "3600",
                grace,
                file,
                scope,
                "planned rotation",
            ])
            .env("DATABASE_URL", &actor_url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .output()
            .unwrap()
    };
    let router = novu_rust_api::router(Arc::new(connect(&url).await));
    for (scope, retry, accepted, grace) in [
        ("none", false, false, "0"),
        ("retry", true, false, "0"),
        ("accepted", false, true, "0"),
        ("both", true, true, "0"),
        ("copy", true, true, "60"),
    ] {
        let token = format!("{:064x}", Uuid::new_v4().as_u128());
        let hash = format!("{:x}", Sha256::digest(token.as_bytes()));
        let old: Uuid = db.query_one("INSERT INTO public.novu_rust_console_credentials(token_hash,environment_id,expires_at,subscriber_read) VALUES($1,'scope-rotation',clock_timestamp()+interval '1 hour',true) RETURNING credential_id", &[&hash]).await.unwrap().get(0);
        db.execute("INSERT INTO public.novu_rust_console_alert_decision_grants VALUES($1,'retry'),($1,'accepted')", &[&old]).await.unwrap();
        let file = dir.join(scope);
        let result = run(old, grace, file.to_str().unwrap(), scope);
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        let response: serde_json::Value = serde_json::from_slice(&result.stdout).unwrap();
        let new = Uuid::parse_str(response["credentialId"].as_str().unwrap()).unwrap();
        assert_eq!(response["oldValidUntil"].is_string(), grace != "0");
        let row=db.query_one("SELECT EXISTS(SELECT 1 FROM public.novu_rust_console_alert_decision_grants WHERE credential_id=$1 AND decision='retry'), EXISTS(SELECT 1 FROM public.novu_rust_console_alert_decision_grants WHERE credential_id=$1 AND decision='accepted'), (SELECT subscriber_read FROM public.novu_rust_console_credentials WHERE credential_id=$1)",&[&new]).await.unwrap();
        assert_eq!(
            (
                row.get::<_, bool>(0),
                row.get::<_, bool>(1),
                row.get::<_, bool>(2)
            ),
            (retry, accepted, true)
        );
        let audit: i64=db.query_one("SELECT count(*) FROM public.novu_rust_console_alert_decision_grant_audit WHERE credential_id=$1 AND action='grant'",&[&new]).await.unwrap().get(0);
        assert_eq!(audit, i64::from(retry) + i64::from(accepted));
        let new_token = std::fs::read_to_string(&file).unwrap();
        for (key, allowed) in [(&new_token, retry || accepted), (&token, grace != "0")] {
            let req = Request::builder()
                .uri(format!(
                    "/v1/experimental/console/alerts/request?requestId={}",
                    Uuid::new_v4()
                ))
                .header("authorization", format!("SwallowConsole {key}"))
                .body(Body::empty())
                .unwrap();
            assert_eq!(
                router.clone().oneshot(req).await.unwrap().status(),
                if allowed {
                    StatusCode::OK
                } else {
                    StatusCode::UNAUTHORIZED
                }
            );
        }
        std::fs::remove_file(file).unwrap();
    }
    let old:Uuid=db.query_one("INSERT INTO public.novu_rust_console_credentials(token_hash,environment_id,expires_at) VALUES(repeat('f',64),'scope-rotation',clock_timestamp()+interval '1 hour') RETURNING credential_id",&[]).await.unwrap().get(0);
    db.execute(
        "INSERT INTO public.novu_rust_console_alert_decision_grants VALUES($1,'retry')",
        &[&old],
    )
    .await
    .unwrap();
    let failed = dir.join("failed");
    assert!(
        !run(old, "0", failed.to_str().unwrap(), "both")
            .status
            .success()
    );
    assert!(
        db.query_one(
            "SELECT enabled FROM public.novu_rust_console_credentials WHERE credential_id=$1",
            &[&old]
        )
        .await
        .unwrap()
        .get::<_, bool>(0)
    );
    std::fs::remove_file(&failed).unwrap();
    db.batch_execute("CREATE FUNCTION public.test_rotation_scope_reject() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture'; END $$; CREATE TRIGGER test_rotation_scope_reject BEFORE INSERT ON public.novu_rust_console_alert_decision_grant_audit FOR EACH ROW EXECUTE FUNCTION public.test_rotation_scope_reject()").await.unwrap();
    assert!(
        !run(old, "0", failed.to_str().unwrap(), "retry")
            .status
            .success()
    );
    assert!(
        db.query_one(
            "SELECT enabled FROM public.novu_rust_console_credentials WHERE credential_id=$1",
            &[&old]
        )
        .await
        .unwrap()
        .get::<_, bool>(0)
    );
    let failed_hash = format!("{:x}", Sha256::digest(std::fs::read(&failed).unwrap()));
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
    assert_eq!(db.query_one("SELECT count(*) FROM public.novu_rust_console_rotation_audit WHERE old_credential_id=$1",&[&old]).await.unwrap().get::<_,i64>(0),0);
    db.batch_execute("DROP TRIGGER test_rotation_scope_reject ON public.novu_rust_console_alert_decision_grant_audit; DROP FUNCTION public.test_rotation_scope_reject()").await.unwrap();
    db.batch_execute(&format!("GRANT EXECUTE ON FUNCTION public.novu_rust_manage_console_alert_decision(text,uuid,text,text,text) TO {role}")).await.unwrap();
    let mut grant_actor = connect(&actor_url).await;
    let tx = grant_actor.transaction().await.unwrap();
    tx.execute("SELECT public.novu_rust_manage_console_alert_decision('scope-rotation',$1,'retry','revoke','revoke first')", &[&old]).await.unwrap();
    let rotating = connect(&actor_url).await;
    let pending = tokio::spawn(async move {
        rotating.query_one("SELECT * FROM public.novu_rust_rotate_console_credential_scoped('scope-rotation',$1,repeat('e',64),clock_timestamp()+interval '1 hour',0,'copy','after revoke')", &[&old]).await
    });
    let mut waiting = false;
    for _ in 0..100 {
        waiting = db.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE '%novu_rust_rotate_console_credential_scoped%')", &[]).await.unwrap().get(0);
        if waiting {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(waiting);
    tx.commit().await.unwrap();
    let rotated: Uuid = pending.await.unwrap().unwrap().get(0);
    assert_eq!(db.query_one("SELECT count(*) FROM public.novu_rust_console_alert_decision_grants WHERE credential_id=$1", &[&rotated]).await.unwrap().get::<_,i64>(0), 0);
    std::fs::remove_file(failed).unwrap();
    std::fs::remove_dir(dir).unwrap();
}
