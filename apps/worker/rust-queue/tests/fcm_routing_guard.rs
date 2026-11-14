//! Two guarded workers use one disposable PostgreSQL, never real FCM.
use std::{
    path::Path,
    process::{Child, Command, Stdio},
    time::Duration,
};
use tokio_postgres::NoTls;
struct Worker(Child);
impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL"]
async fn guarded_workers_match_routes_and_reset_requires_quiescence() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let api = root.join("../../api/rust-api/migrations");
    let worker = root.join("migrations");
    for args in [
        vec!["apply", "api", api.to_str().unwrap(), "--through", "17"],
        vec!["apply", "worker", worker.to_str().unwrap()],
        vec!["apply", "api", api.to_str().unwrap()],
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_migrate-worker"))
            .args(args)
            .env("DATABASE_URL", &url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    db.batch_execute("INSERT INTO novu_rust_environments VALUES ('guard-env','org'),('map-env','org'),('pref-env','org'); INSERT INTO novu_rust_fcm_routing_workers SELECT e.id,r.rolname,r.oid FROM novu_rust_environments e CROSS JOIN pg_roles r WHERE r.rolname=session_user; INSERT INTO novu_rust_operator_grant_admins(environment_id,database_principal,principal_oid) SELECT 'guard-env',rolname,oid FROM pg_roles WHERE rolname=session_user").await.unwrap();
    let command = |env: &str, project: &str, token: &str| {
        let mut c = Command::new(env!("CARGO_BIN_EXE_fcm-worker"));
        c.env("DATABASE_URL", &url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("NOVU_FCM_ENVIRONMENT_ID", env)
            .env("NOVU_FCM_PROJECT_ID", project)
            .env("NOVU_FCM_ACCESS_TOKEN", token)
            .env("SWALLOW_FCM_ENFORCE_ROUTING_CONFIG", "true")
            .env_remove("NOVU_FCM_ACCESS_TOKEN_FILE")
            .env_remove("NOVU_FCM_INTEGRATIONS")
            .env_remove("NOVU_FCM_INTEGRATIONS_FILE")
            .env_remove("SWALLOW_FCM_ALLOW_SHARED_PROVIDER")
            .env_remove("SWALLOW_FCM_INVALID_TOKEN_POLICY")
            .env_remove("SWALLOW_FCM_TOKEN_CLEANUP_GUARD")
            .env_remove("SWALLOW_PUSH_WORKFLOW_STEP_POLICY")
            .env_remove("SWALLOW_PUSH_PREFERENCE_POLICY")
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        c
    };
    let mut first = Worker(
        command("guard-env", "project-a", "fixture-token-one")
            .spawn()
            .unwrap(),
    );
    for _ in 0..100 {
        let exists:bool=db.query_one("SELECT EXISTS(SELECT 1 FROM novu_rust_fcm_routing_config WHERE environment_id='guard-env')",&[]).await.unwrap().get(0);
        if exists {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(db.query_one("SELECT EXISTS(SELECT 1 FROM novu_rust_fcm_routing_config WHERE environment_id='guard-env')",&[]).await.unwrap().get::<_,bool>(0));
    let mut same = Worker(
        command("guard-env", "project-a", "fixture-token-two")
            .spawn()
            .unwrap(),
    );
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(same.0.try_wait().unwrap().is_none());
    let mismatch_step = command("guard-env", "project-a", "fixture-token")
        .env("SWALLOW_PUSH_WORKFLOW_STEP_POLICY", "active")
        .output()
        .unwrap();
    assert!(!mismatch_step.status.success());
    assert!(
        String::from_utf8_lossy(&mismatch_step.stderr)
            .contains("different workflow or preference policies")
    );
    let invalid_step = command("guard-env", "project-a", "fixture-token")
        .env("SWALLOW_PUSH_WORKFLOW_STEP_POLICY", "invalid")
        .output()
        .unwrap();
    assert!(!invalid_step.status.success());
    assert!(
        String::from_utf8_lossy(&invalid_step.stderr).contains("invalid push workflow step policy")
    );
    let mismatch_preference = command("guard-env", "project-a", "fixture-token")
        .env("SWALLOW_PUSH_PREFERENCE_POLICY", "recheck")
        .output()
        .unwrap();
    assert!(!mismatch_preference.status.success());
    assert!(
        String::from_utf8_lossy(&mismatch_preference.stderr)
            .contains("different workflow or preference policies")
    );
    let invalid_preference = command("guard-env", "project-a", "fixture-token")
        .env("SWALLOW_PUSH_PREFERENCE_POLICY", "invalid")
        .output()
        .unwrap();
    assert!(!invalid_preference.status.success());
    assert!(
        String::from_utf8_lossy(&invalid_preference.stderr)
            .contains("invalid push preference policy")
    );
    let mut guarded_preference = Worker(
        command("pref-env", "project-a", "fixture-token")
            .env("SWALLOW_PUSH_PREFERENCE_POLICY", "recheck")
            .spawn()
            .unwrap(),
    );
    for _ in 0..100 {
        if db.query_one("SELECT EXISTS(SELECT 1 FROM public.novu_rust_push_workflow_worker_config WHERE environment_id='pref-env' AND preference_policy='recheck')",&[]).await.unwrap().get::<_,bool>(0) {break;}
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(db.query_one("SELECT EXISTS(SELECT 1 FROM public.novu_rust_push_workflow_worker_config WHERE environment_id='pref-env' AND preference_policy='recheck')",&[]).await.unwrap().get::<_,bool>(0));
    assert!(guarded_preference.0.try_wait().unwrap().is_none());
    let default_mismatch = command("pref-env", "project-a", "fixture-token")
        .output()
        .unwrap();
    assert!(!default_mismatch.status.success());
    assert!(
        String::from_utf8_lossy(&default_mismatch.stderr)
            .contains("different workflow or preference policies")
    );
    let mismatch = command("guard-env", "project-b", "fixture-token-secret")
        .output()
        .unwrap();
    assert!(!mismatch.status.success());
    let error = String::from_utf8_lossy(&mismatch.stderr);
    assert!(error.contains("different routing configurations"));
    assert!(!error.contains("fixture-token-secret"));
    let mismatched_mode = command("guard-env", "project-a", "fixture-token-secret")
        .env("SWALLOW_FCM_ALLOW_SHARED_PROVIDER", "true")
        .output()
        .unwrap();
    assert!(!mismatched_mode.status.success());
    let mismatched_cleanup = command("guard-env", "project-a", "fixture-token")
        .env("SWALLOW_FCM_TOKEN_CLEANUP_GUARD", "version")
        .output()
        .unwrap();
    assert!(!mismatched_cleanup.status.success());
    let invalid_cleanup = command("guard-env", "project-a", "fixture-token")
        .env("SWALLOW_FCM_TOKEN_CLEANUP_GUARD", "invalid")
        .output()
        .unwrap();
    assert!(!invalid_cleanup.status.success());
    assert!(
        String::from_utf8_lossy(&invalid_cleanup.stderr)
            .contains("invalid FCM token cleanup guard")
    );
    let mismatched_policy = command("guard-env", "project-a", "fixture-token")
        .env("SWALLOW_FCM_INVALID_TOKEN_POLICY", "unregistered")
        .output()
        .unwrap();
    assert!(!mismatched_policy.status.success());
    assert!(
        String::from_utf8_lossy(&mismatched_policy.stderr)
            .contains("different routing configurations")
    );
    let invalid_policy = command("guard-env", "project-a", "fixture-token")
        .env("SWALLOW_FCM_INVALID_TOKEN_POLICY", "invalid")
        .output()
        .unwrap();
    assert!(!invalid_policy.status.success());
    assert!(
        String::from_utf8_lossy(&invalid_policy.stderr)
            .contains("invalid FCM invalid token policy")
    );
    let stored:String=db.query_one("SELECT routing_hash FROM novu_rust_fcm_routing_config WHERE environment_id='guard-env'",&[]).await.unwrap().get(0);
    assert_eq!(stored.len(), 64);
    assert!(!stored.contains("fixture"));
    let reset = || async {
        db.query_one(
            "SELECT public.novu_rust_reset_fcm_routing('guard-env','configuration change')",
            &[],
        )
        .await
        .unwrap()
        .get::<_, bool>(0)
    };
    assert!(!reset().await);
    first.0.kill().unwrap();
    first.0.wait().unwrap();
    assert!(!reset().await);
    same.0.kill().unwrap();
    same.0.wait().unwrap();
    let mut reset_done = false;
    for _ in 0..100 {
        if reset().await {
            reset_done = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(reset_done);
    assert_eq!(db.query_one("SELECT count(*) FROM novu_rust_fcm_routing_audit WHERE environment_id='guard-env' AND actor=session_user AND reason='configuration change'",&[]).await.unwrap().get::<_,i64>(0),1);
    let mut changed = Worker(
        command("guard-env", "project-b", "fixture-token-three")
            .env("SWALLOW_FCM_INVALID_TOKEN_POLICY", "unregistered")
            .env("SWALLOW_FCM_TOKEN_CLEANUP_GUARD", "version")
            .spawn()
            .unwrap(),
    );
    for _ in 0..100 {
        if db.query_one("SELECT EXISTS(SELECT 1 FROM novu_rust_fcm_routing_config WHERE environment_id='guard-env')",&[]).await.unwrap().get::<_,bool>(0) {break;}
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(changed.0.try_wait().unwrap().is_none());
    let wrong = command("guard-env", "project-b", "fixture-token")
        .output()
        .unwrap();
    assert!(!wrong.status.success());
    let mut matching = Worker(
        command("guard-env", "project-b", "fixture-token")
            .env("SWALLOW_FCM_INVALID_TOKEN_POLICY", "unregistered")
            .env("SWALLOW_FCM_TOKEN_CLEANUP_GUARD", "version")
            .spawn()
            .unwrap(),
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(matching.0.try_wait().unwrap().is_none());
    let map = |value: &str| {
        let mut c = command("map-env", "unused", "unused");
        c.env_remove("NOVU_FCM_ACCESS_TOKEN")
            .env("NOVU_FCM_INTEGRATIONS", value);
        c
    };
    let mut map_a=Worker(map(r#"{"first":{"project_id":"project-a","access_token":"token-a"},"second":{"project_id":"project-b","access_token":"token-b"}}"#).spawn().unwrap());
    for _ in 0..100 {
        if db.query_one("SELECT EXISTS(SELECT 1 FROM novu_rust_fcm_routing_config WHERE environment_id='map-env')",&[]).await.unwrap().get::<_,bool>(0) {break;}
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let mut map_same=Worker(map(r#"{"second":{"project_id":"project-b","access_token":"rotated-b"},"first":{"project_id":"project-a","access_token":"rotated-a"}}"#).spawn().unwrap());
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(map_a.0.try_wait().unwrap().is_none() && map_same.0.try_wait().unwrap().is_none());
    let mismatch=map(r#"{"first":{"project_id":"project-b","access_token":"token-a"},"second":{"project_id":"project-a","access_token":"token-b"}}"#).output().unwrap();
    assert!(!mismatch.status.success());
    let map_hash: String = db
        .query_one(
            "SELECT routing_hash FROM novu_rust_fcm_routing_config WHERE environment_id='map-env'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    db.batch_execute("CREATE ROLE fcm_guard_unprivileged LOGIN; INSERT INTO novu_rust_fcm_routing_workers SELECT 'map-env',rolname,oid FROM pg_roles WHERE rolname='fcm_guard_unprivileged'; GRANT EXECUTE ON FUNCTION public.novu_rust_claim_fcm_routing(text,text),public.novu_rust_reset_fcm_routing(text,text) TO fcm_guard_unprivileged; SET SESSION AUTHORIZATION fcm_guard_unprivileged").await.unwrap();
    assert!(
        db.query_one(
            "SELECT public.novu_rust_claim_fcm_routing('map-env',$1)",
            &[&map_hash]
        )
        .await
        .unwrap()
        .get::<_, bool>(0)
    );
    assert!(
        db.query("SELECT * FROM public.novu_rust_fcm_routing_config", &[])
            .await
            .is_err()
    );
    assert!(db.query_one("SELECT pg_advisory_unlock_shared(hashtextextended('swallow-fcm-routing:map-env',82991))",&[]).await.unwrap().get::<_,bool>(0));
    assert!(
        db.query_one(
            "SELECT public.novu_rust_claim_fcm_routing('guard-env',repeat('a',64))",
            &[]
        )
        .await
        .is_err()
    );
    assert!(
        db.query_one(
            "SELECT public.novu_rust_reset_fcm_routing('guard-env','bad')",
            &[]
        )
        .await
        .is_err()
    );
    db.batch_execute("RESET SESSION AUTHORIZATION; DELETE FROM novu_rust_fcm_routing_workers WHERE database_principal='fcm_guard_unprivileged'; DROP OWNED BY fcm_guard_unprivileged; DROP ROLE fcm_guard_unprivileged").await.unwrap();
}
