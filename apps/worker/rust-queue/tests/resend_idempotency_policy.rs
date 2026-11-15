//! Real workers and a disposable PostgreSQL database; no provider requests.
use std::{
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};
use tokio_postgres::NoTls;
use uuid::Uuid;

struct Worker(Child);
impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn start(url: &str, environment: &str, scope: &str) -> Worker {
    Worker(
        Command::new(env!("CARGO_BIN_EXE_resend-worker"))
            .env_clear()
            .env("DATABASE_URL", url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .env("NOVU_RESEND_ENVIRONMENT_ID", environment)
            .env("NOVU_RESEND_API_KEY", "re_test")
            .env("NOVU_RESEND_FROM", "test@example.test")
            .env("NOVU_RESEND_IDEMPOTENCY_SCOPE", scope)
            .env("HTTPS_PROXY", "http://127.0.0.1:9")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    )
}
async fn wait_for_scope(db: &tokio_postgres::Client, environment: &str, expected: &str) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if db.query_opt("SELECT scope FROM public.novu_rust_resend_idempotency_policy WHERE environment_id=$1", &[&environment]).await.unwrap().is_some_and(|r| r.get::<_, String>(0) == expected) { break; }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    }).await.unwrap();
}
#[tokio::test]
#[ignore = "requires a fresh disposable PostgreSQL database"]
async fn workers_reject_conflicting_scopes_and_isolate_environments() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    for args in [
        vec![
            "apply",
            "api",
            "../../api/rust-api/migrations",
            "--through",
            "17",
        ],
        vec!["apply", "worker", "migrations"],
    ] {
        let result = Command::new(env!("CARGO_BIN_EXE_migrate-worker"))
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .args(args)
            .env("DATABASE_URL", &url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .output()
            .unwrap();
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
    }
    let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let a = format!("scope-a-{}", Uuid::new_v4());
    let b = format!("scope-b-{}", Uuid::new_v4());
    db.execute("INSERT INTO public.novu_rust_environments(id,organization_id) VALUES ($1,'org'),($2,'org')", &[&a,&b]).await.unwrap();
    let mut original = start(&url, &a, "job");
    wait_for_scope(&db, &a, "job").await;
    assert!(original.0.try_wait().unwrap().is_none());
    let mut conflicting = start(&url, &a, "retry");
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = conflicting.0.try_wait().unwrap() {
            assert!(!status.success());
            break;
        }
        assert!(Instant::now() < deadline, "conflicting worker did not exit");
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    assert!(original.0.try_wait().unwrap().is_none());
    let mut same = start(&url, &a, "job");
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(same.0.try_wait().unwrap().is_none());
    let mut other = start(&url, &b, "retry");
    wait_for_scope(&db, &b, "retry").await;
    assert!(other.0.try_wait().unwrap().is_none());
    let c = format!("scope-race-{}", Uuid::new_v4());
    db.execute(
        "INSERT INTO public.novu_rust_environments(id,organization_id) VALUES ($1,'org')",
        &[&c],
    )
    .await
    .unwrap();
    let mut job_worker = start(&url, &c, "job");
    let mut retry_worker = start(&url, &c, "retry");
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let job_exit = job_worker.0.try_wait().unwrap();
        let retry_exit = retry_worker.0.try_wait().unwrap();
        if job_exit.is_some() || retry_exit.is_some() {
            assert_ne!(
                job_exit.is_some(),
                retry_exit.is_some(),
                "exactly one scope must win"
            );
            assert!(!job_exit.or(retry_exit).unwrap().success());
            break;
        }
        assert!(
            Instant::now() < deadline,
            "conflicting concurrent workers did not resolve"
        );
        tokio::time::sleep(Duration::from_millis(30)).await;
    }

    // Policy changes wait while the guarded dispatch transaction holds its lock.
    drop(same);
    let (mut guarded, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let tx = guarded.transaction().await.unwrap();
    tx.query_one(
        "SELECT public.novu_rust_check_resend_idempotency_scope($1,'job')",
        &[&a],
    )
    .await
    .unwrap();
    let (writer, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    let env_for_update = a.clone();
    let update = tokio::spawn(async move {
        writer.execute("UPDATE public.novu_rust_resend_idempotency_policy SET scope='retry' WHERE environment_id=$1", &[&env_for_update]).await.unwrap()
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !update.is_finished(),
        "policy update did not wait for guarded transaction"
    );
    tx.commit().await.unwrap();
    assert_eq!(update.await.unwrap(), 1);
    assert_eq!(
        db.query_one(
            "SELECT public.novu_rust_check_resend_idempotency_scope($1,'job')",
            &[&a]
        )
        .await
        .unwrap_err()
        .code()
        .unwrap()
        .code(),
        "55000"
    );

    // A running worker with stale scope must not create an attempt or contact Resend.
    let job = Uuid::new_v4();
    novu_postgres_queue::enqueue(
        &db,
        job,
        &a,
        "experimental_resend_email",
        &serde_json::json!({"to":["test@example.test"],"subject":"test","html":"test"}),
    )
    .await
    .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(exit) = original.0.try_wait().unwrap() {
            assert!(!exit.success());
            break;
        }
        assert!(Instant::now() < deadline, "stale worker did not stop");
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM public.novu_rust_resend_attempts WHERE job_id=$1",
            &[&job]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    assert_eq!(
        db.query_one(
            "SELECT status FROM public.novu_queue_jobs WHERE id=$1",
            &[&job]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "running"
    );
    // The invoker needs explicit function and row-lock privileges.
    let role = format!("resend_policy_{}", Uuid::new_v4().simple());
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT SELECT, UPDATE ON public.novu_rust_resend_idempotency_policy TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_check_resend_idempotency_scope(text,text) TO {role}")).await.unwrap();
    let (restricted, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = driver.await;
    });
    restricted
        .batch_execute(&format!("SET SESSION AUTHORIZATION {role}"))
        .await
        .unwrap();
    restricted
        .query_one(
            "SELECT public.novu_rust_check_resend_idempotency_scope($1,'retry')",
            &[&b],
        )
        .await
        .unwrap();
    db.batch_execute(&format!("REVOKE EXECUTE ON FUNCTION public.novu_rust_check_resend_idempotency_scope(text,text) FROM {role}")).await.unwrap();
    assert_eq!(
        restricted
            .query_one(
                "SELECT public.novu_rust_check_resend_idempotency_scope($1,'retry')",
                &[&b]
            )
            .await
            .unwrap_err()
            .code()
            .unwrap()
            .code(),
        "42501"
    );
    assert_eq!(
        db.query_one(
            "SELECT scope FROM public.novu_rust_resend_idempotency_policy WHERE environment_id=$1",
            &[&a]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "retry"
    );
}

#[tokio::test]
#[ignore = "requires a fully migrated disposable PostgreSQL database"]
async fn workers_reject_conflicting_shared_cooldown_settings() {
    let url=std::env::var("TEST_DATABASE_URL").unwrap();
    let (db,driver)=tokio_postgres::connect(&url,NoTls).await.unwrap();
    tokio::spawn(async move {driver.await.unwrap()});
    let on=format!("resend-guard-on-{}",Uuid::new_v4());
    let off=format!("resend-guard-off-{}",Uuid::new_v4());
    db.execute("INSERT INTO public.novu_rust_environments VALUES($1,'org'),($2,'org')", &[&on,&off]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_resend_cooldown_policies(environment_id,fallback_seconds) VALUES($1,30)", &[&on]).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_resend_cooldown_workers SELECT $1,rolname,oid FROM pg_roles WHERE rolname=session_user", &[&on]).await.unwrap();
    let start=|environment:&str,enabled:bool| Worker(Command::new(env!("CARGO_BIN_EXE_resend-worker"))
        .env_clear().env("DATABASE_URL",&url).env("NOVU_POSTGRES_ALLOW_INSECURE","true")
        .env("NOVU_RESEND_ENVIRONMENT_ID",environment).env("NOVU_RESEND_API_KEY","re_test")
        .env("NOVU_RESEND_FROM","test@example.test").env("NOVU_RESEND_SHARED_COOLDOWN",enabled.to_string())
        .env("HTTPS_PROXY","http://127.0.0.1:9").stdout(Stdio::null()).stderr(Stdio::null()).spawn().unwrap());
    let mut a=start(&on,true); let mut b=start(&off,false);
    wait_for_scope(&db,&on,"job").await;wait_for_scope(&db,&off,"job").await;
    assert!(a.0.try_wait().unwrap().is_none());assert!(b.0.try_wait().unwrap().is_none());
    for (environment,enabled) in [(&on,false),(&off,true)] {
        let mut conflicting=start(environment,enabled);
        let status=tokio::time::timeout(Duration::from_secs(10),async {
            loop {if let Some(status)=conflicting.0.try_wait().unwrap(){break status} tokio::time::sleep(Duration::from_millis(30)).await;}
        }).await.unwrap();
        assert!(!status.success());
    }
    let states=db.query("SELECT shared_cooldown FROM public.novu_rust_resend_idempotency_policy WHERE environment_id=ANY($1) ORDER BY environment_id", &[&vec![&on,&off]]).await.unwrap();
    let flags:Vec<bool>=states.iter().map(|row|row.get(0)).collect();
    assert!(flags.contains(&true) && flags.contains(&false));
}
