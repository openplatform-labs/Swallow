//! Restricted operator console decision path using a real PostgreSQL database.
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
#[ignore = "requires fully migrated disposable PostgreSQL and role creation"]
async fn independently_scoped_decisions_are_fenced_and_audited() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let db = connect(&url).await;
    db.batch_execute("INSERT INTO public.novu_rust_environments VALUES ('decision-a','org'),('decision-b','org')").await.unwrap();
    let admin_role = format!("decision_admin_{}", Uuid::new_v4().simple());
    let api_role = format!("decision_api_{}", Uuid::new_v4().simple());
    db.batch_execute(&format!("CREATE ROLE {admin_role} LOGIN; CREATE ROLE {api_role} LOGIN; GRANT USAGE ON SCHEMA public TO {admin_role},{api_role}; GRANT EXECUTE ON FUNCTION public.novu_rust_manage_console_alert_decision(text,uuid,text,text,text),public.novu_rust_list_console_alert_decisions(text,uuid),public.novu_rust_list_console_decision_audit(text,bigint,text) TO {admin_role}; GRANT EXECUTE ON FUNCTION public.novu_rust_console_resolve_alert(text,text,text,uuid,text,text) TO {api_role}")).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_console_credential_admins(environment_id,database_principal,principal_oid) SELECT 'decision-a',rolname,oid FROM pg_roles WHERE rolname::text=$1", &[&admin_role]).await.unwrap();
    for decision in ["alert_retry", "alert_accepted"] {
        db.execute("INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid) SELECT 'decision-a',rolname,$2,oid FROM pg_roles WHERE rolname::text=$1", &[&api_role,&decision]).await.unwrap();
    }
    let token = "a".repeat(64);
    let hash = format!("{:x}", Sha256::digest(token.as_bytes()));
    let id:Uuid=db.query_one("INSERT INTO public.novu_rust_console_credentials(token_hash,environment_id,expires_at,subscriber_read) VALUES($1,'decision-a',clock_timestamp()+interval '1 hour',false) RETURNING credential_id", &[&hash]).await.unwrap().get(0);
    let attempt = Uuid::new_v4();
    db.execute("INSERT INTO public.novu_rust_alert_delivery_slots(environment_id,channel,target_id,state,attempt_id) VALUES('decision-a','slack','ops','unknown',$1),('decision-b','email','other','unknown',$1)", &[&attempt]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_alert_delivery_attempts(id,environment_id,channel,target_id,state) VALUES($1,'decision-a','slack','ops','unknown')", &[&attempt]).await.unwrap();
    let admin_url = url.replacen(
        "postgres://postgres@",
        &format!("postgres://{admin_role}@"),
        1,
    );
    let api_url = url.replacen(
        "postgres://postgres@",
        &format!("postgres://{api_role}@"),
        1,
    );
    let api = connect(&api_url).await;
    assert!(
        api.query("SELECT * FROM public.novu_rust_alert_delivery_slots", &[])
            .await
            .is_err()
    );
    let router = novu_rust_api::router(Arc::new(api));
    let request = move |decision: &str, attempt_id: Uuid, target: &str| {
        Request::builder().method("POST").uri("/v1/experimental/console/alerts/resolve").header("authorization",format!("SwallowConsole {token}")).header("content-type","application/json").body(Body::from(serde_json::json!({"channel":"slack","targetId":target,"attemptId":attempt_id.to_string(),"decision":decision,"reason":"verified"}).to_string())).unwrap()
    };
    let run = |args: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_console-credential"))
            .args(args)
            .env("DATABASE_URL", &admin_url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .output()
            .unwrap()
    };
    assert_eq!(
        router
            .clone()
            .oneshot(request("retry", attempt, "ops"))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    let output = run(&[
        "decision-access",
        "decision-a",
        &id.to_string(),
        "retry",
        "grant",
        "ticket",
    ]);
    assert!(output.status.success());
    let list = run(&["decision-access-list", "decision-a"]);
    let value: serde_json::Value = serde_json::from_slice(&list.stdout).unwrap();
    assert_eq!(value[0]["retry"], true);
    assert_eq!(value[0]["accepted"], false);
    assert!(
        !run(&[
            "decision-access",
            "decision-b",
            &id.to_string(),
            "accepted",
            "grant",
            "ticket"
        ])
        .status
        .success()
    );
    assert_eq!(
        router
            .clone()
            .oneshot(request("accepted", attempt, "ops"))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        router
            .clone()
            .oneshot(request("retry", Uuid::new_v4(), "ops"))
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        router
            .clone()
            .oneshot(request("retry", attempt, "other"))
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
    db.batch_execute("CREATE FUNCTION public.test_console_decision_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture'; END $$; CREATE TRIGGER test_console_decision_fail BEFORE INSERT ON public.novu_rust_console_alert_decision_audit FOR EACH ROW EXECUTE FUNCTION public.test_console_decision_fail()").await.unwrap();
    assert_eq!(
        router
            .clone()
            .oneshot(request("retry", attempt, "ops"))
            .await
            .unwrap()
            .status(),
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(db.query_one("SELECT state FROM public.novu_rust_alert_delivery_slots WHERE environment_id='decision-a' AND target_id='ops'",&[]).await.unwrap().get::<_,String>(0),"unknown");
    assert_eq!(db.query_one("SELECT count(*) FROM public.novu_rust_alert_delivery_resolutions WHERE environment_id='decision-a'",&[]).await.unwrap().get::<_,i64>(0),0);
    db.batch_execute("DROP TRIGGER test_console_decision_fail ON public.novu_rust_console_alert_decision_audit; DROP FUNCTION public.test_console_decision_fail()").await.unwrap();
    let other_router = novu_rust_api::router(Arc::new(connect(&api_url).await));
    let (first, second) = tokio::join!(
        router.clone().oneshot(request("retry", attempt, "ops")),
        other_router.oneshot(request("retry", attempt, "ops"))
    );
    let statuses = [first.unwrap().status(), second.unwrap().status()];
    assert!(statuses.contains(&StatusCode::OK));
    assert!(statuses.contains(&StatusCode::BAD_REQUEST));
    let audit = run(&["decision-audit", "decision-a", "resolution"]);
    assert!(audit.status.success());
    let audit: serde_json::Value = serde_json::from_slice(&audit.stdout).unwrap();
    assert_eq!(audit[0]["credentialId"], id.to_string());
    assert_eq!(audit[0]["details"]["attemptId"], attempt.to_string());
    assert!(
        run(&[
            "decision-access",
            "decision-a",
            &id.to_string(),
            "accepted",
            "grant",
            "ticket"
        ])
        .status
        .success()
    );
    db.execute("UPDATE public.novu_rust_alert_delivery_slots SET state='rejected' WHERE environment_id='decision-a' AND target_id='ops'", &[]).await.unwrap();
    assert_eq!(
        router
            .clone()
            .oneshot(request("accepted", attempt, "ops"))
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
    db.execute("UPDATE public.novu_rust_alert_delivery_slots SET state='unknown' WHERE environment_id='decision-a' AND target_id='ops'", &[]).await.unwrap();
    assert_eq!(
        router
            .clone()
            .oneshot(request("accepted", attempt, "ops"))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert!(
        run(&[
            "decision-access",
            "decision-a",
            &id.to_string(),
            "retry",
            "revoke",
            "ticket"
        ])
        .status
        .success()
    );
    assert_eq!(
        router
            .clone()
            .oneshot(request("retry", attempt, "ops"))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert!(
        run(&[
            "decision-access",
            "decision-a",
            &id.to_string(),
            "retry",
            "grant",
            "ticket"
        ])
        .status
        .success()
    );
    db.execute("UPDATE public.novu_rust_alert_delivery_slots SET state='unknown' WHERE environment_id='decision-a' AND target_id='ops'", &[]).await.unwrap();
    db.execute("UPDATE public.novu_rust_console_credentials SET expires_at=clock_timestamp()+interval '2 seconds' WHERE credential_id=$1", &[&id]).await.unwrap();
    let mut blocker = connect(&url).await;
    let tx = blocker.transaction().await.unwrap();
    tx.execute("UPDATE public.novu_rust_alert_delivery_slots SET state=state WHERE environment_id='decision-a' AND target_id='ops'", &[]).await.unwrap();
    let pending = tokio::spawn(async move {
        router
            .oneshot(request("retry", attempt, "ops"))
            .await
            .unwrap()
            .status()
    });
    let mut waiting = false;
    for _ in 0..100 {
        waiting=db.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE '%novu_rust_console_resolve_alert%')", &[]).await.unwrap().get(0);
        if waiting {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(waiting);
    tokio::time::sleep(std::time::Duration::from_millis(2200)).await;
    tx.commit().await.unwrap();
    assert_eq!(pending.await.unwrap(), StatusCode::UNAUTHORIZED);
    assert_eq!(db.query_one("SELECT count(*) FROM public.novu_rust_console_alert_decision_audit WHERE environment_id='decision-a'", &[]).await.unwrap().get::<_,i64>(0),2);
}
