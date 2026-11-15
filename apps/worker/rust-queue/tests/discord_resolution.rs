use novu_postgres_queue::{discord::Webhook, discord_queue};
use std::{path::Path, process::Command};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;

async fn migrate(db: &Client, dir: &Path, max: u32) {
    let mut files: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "sql"))
        .collect();
    files.sort();
    for file in files {
        let name = file.file_name().unwrap().to_str().unwrap();
        if name[..4].parse::<u32>().unwrap() <= max {
            db.batch_execute(&std::fs::read_to_string(&file).unwrap())
                .await
                .unwrap_or_else(|e| panic!("{name}: {e}"));
        }
    }
}
#[tokio::test]
#[ignore = "requires fresh disposable PostgreSQL, role creation and test-mocks"]
async fn discord_resolution_is_scoped_fenced_and_delivers_retry() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (mut db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(driver);
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    migrate(&db, &root.join("../../api/rust-api/migrations"), 17).await;
    migrate(&db, &root.join("migrations"), u32::MAX).await;
    let env = format!("discord-resolve-{}", Uuid::new_v4());
    let other = format!("discord-other-{}", Uuid::new_v4());
    let role = format!("discord_resolve_{}", Uuid::new_v4().simple());
    db.execute(
        "INSERT INTO public.novu_rust_environments VALUES($1,'org'),($2,'org')",
        &[&env, &other],
    )
    .await
    .unwrap();
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN PASSWORD 'test-only'; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_resolve_discord(text,uuid,uuid,uuid,bigint,text,text) TO {role}")).await.unwrap();
    let mut role_url = reqwest::Url::parse(&url).unwrap();
    role_url.set_username(&role).unwrap();
    role_url.set_password(Some("test-only")).unwrap();
    let (op, driver) = tokio_postgres::connect(role_url.as_str(), NoTls)
        .await
        .unwrap();
    tokio::spawn(driver);
    let (op2, driver) = tokio_postgres::connect(role_url.as_str(), NoTls)
        .await
        .unwrap();
    tokio::spawn(driver);
    let unknown = Uuid::new_v4();
    let rejected = Uuid::new_v4();
    let pre = Uuid::new_v4();
    for job in [unknown, rejected, pre] {
        db.execute("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status,last_error) VALUES($1,$2,'experimental_discord_webhook',jsonb_build_object('destination','one','content','retry test'),'failed','failure')",&[&job,&env]).await.unwrap();
    }
    for (job, state, status) in [
        (unknown, "unknown", None),
        (rejected, "rejected", Some(429_i32)),
    ] {
        db.execute("INSERT INTO public.novu_rust_discord_attempts(job_id,environment_id,state,http_status,retry_after_seconds) VALUES($1,$2,$3,$4,60)",&[&job,&env,&state,&status]).await.unwrap();
    }
    let row=db.query_one("SELECT a.revision,j.failure_revision FROM public.novu_rust_discord_attempts a JOIN public.novu_queue_jobs j ON j.id=a.job_id WHERE j.id=$1",&[&unknown]).await.unwrap();
    let rev: Uuid = row.get(0);
    let failure: i64 = row.get(1);
    let request = Uuid::new_v4();
    let sql = "SELECT public.novu_rust_resolve_discord($1,$2,$3,$4,$5,$6,$7)";
    assert!(
        op.query_one(
            sql,
            &[
                &env,
                &request,
                &unknown,
                &Some(rev),
                &failure,
                &"accepted",
                &"external evidence"
            ]
        )
        .await
        .is_err()
    );
    db.execute("INSERT INTO public.novu_rust_discord_resolvers SELECT $1,rolname,oid,true,false FROM pg_roles WHERE rolname::text=$2",&[&env,&role]).await.unwrap();
    assert!(
        op.query_one(
            sql,
            &[
                &other,
                &request,
                &unknown,
                &Some(rev),
                &failure,
                &"accepted",
                &"external evidence"
            ]
        )
        .await
        .is_err()
    );
    assert!(
        op.query_one(
            sql,
            &[
                &env,
                &request,
                &unknown,
                &Some(rev),
                &failure,
                &"retry",
                &"external evidence"
            ]
        )
        .await
        .is_err()
    );
    assert!(
        op.query_one(
            sql,
            &[
                &env,
                &request,
                &unknown,
                &Some(Uuid::new_v4()),
                &failure,
                &"accepted",
                &"external evidence"
            ]
        )
        .await
        .is_err()
    );
    db.batch_execute("CREATE FUNCTION public.discord_resolution_test_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'audit failure'; END $$; CREATE TRIGGER discord_resolution_test_fail BEFORE INSERT ON public.novu_rust_discord_resolutions FOR EACH ROW EXECUTE FUNCTION public.discord_resolution_test_fail()").await.unwrap();
    assert!(
        op.query_one(
            sql,
            &[
                &env,
                &request,
                &unknown,
                &Some(rev),
                &failure,
                &"accepted",
                &"external evidence"
            ]
        )
        .await
        .is_err()
    );
    assert_eq!(
        db.query_one(
            "SELECT status FROM public.novu_queue_jobs WHERE id=$1",
            &[&unknown]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "failed"
    );
    db.batch_execute("DROP TRIGGER discord_resolution_test_fail ON public.novu_rust_discord_resolutions; DROP FUNCTION public.discord_resolution_test_fail()").await.unwrap();
    let attempt = Some(rev);
    let params: &[&(dyn tokio_postgres::types::ToSql + Sync)] = &[
        &env,
        &request,
        &unknown,
        &attempt,
        &failure,
        &"accepted",
        &"external evidence",
    ];
    let (a, b) = tokio::join!(op.query_one(sql, params), op2.query_one(sql, params));
    let mut results = [a.unwrap().get::<_, bool>(0), b.unwrap().get::<_, bool>(0)];
    results.sort();
    assert_eq!(results, [false, true]);
    assert!(
        op.query_one(
            sql,
            &[
                &env,
                &request,
                &unknown,
                &attempt,
                &failure,
                &"accepted",
                &"changed"
            ]
        )
        .await
        .is_err()
    );
    let row=db.query_one("SELECT j.status,a.state,a.provider_message_id FROM public.novu_queue_jobs j JOIN public.novu_rust_discord_attempts a ON a.job_id=j.id WHERE j.id=$1",&[&unknown]).await.unwrap();
    assert_eq!(
        (
            row.get::<_, String>(0),
            row.get::<_, String>(1),
            row.get::<_, Option<String>>(2)
        ),
        ("completed".into(), "accepted".into(), None)
    );
    db.execute(
        "UPDATE public.novu_rust_discord_resolvers SET can_retry=true WHERE environment_id=$1",
        &[&env],
    )
    .await
    .unwrap();
    let row=db.query_one("SELECT a.revision,j.failure_revision FROM public.novu_rust_discord_attempts a JOIN public.novu_queue_jobs j ON j.id=a.job_id WHERE j.id=$1",&[&rejected]).await.unwrap();
    let rejected_rev: Uuid = row.get(0);
    let rejected_failure: i64 = row.get(1);
    assert!(
        op.query_one(
            sql,
            &[
                &env,
                &Uuid::new_v4(),
                &rejected,
                &Some(rejected_rev),
                &rejected_failure,
                &"accepted",
                &"no evidence"
            ]
        )
        .await
        .is_err()
    );
    assert!(
        op.query_one(
            sql,
            &[
                &env,
                &Uuid::new_v4(),
                &rejected,
                &Option::<Uuid>::None,
                &rejected_failure,
                &"retry",
                &"wrong snapshot"
            ]
        )
        .await
        .is_err()
    );
    let retry_id = Uuid::new_v4();
    let result = Command::new(env!("CARGO_BIN_EXE_delivery-reconcile"))
        .env_clear()
        .env("DATABASE_URL", role_url.as_str())
        .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
        .args([
            "resolve-discord",
            &env,
            &retry_id.to_string(),
            &rejected.to_string(),
            &rejected_rev.to_string(),
            &rejected_failure.to_string(),
            "retry",
            "operator reviewed rate limit",
        ])
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let row=db.query_one("SELECT attempt_state,http_status,retry_after_seconds FROM public.novu_rust_discord_resolutions WHERE request_id=$1",&[&retry_id]).await.unwrap();
    assert_eq!(
        (
            row.get::<_, String>(0),
            row.get::<_, i32>(1),
            row.get::<_, i32>(2)
        ),
        ("rejected".into(), 429, 60)
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!(
        "http://{}/api/webhooks/1/secret",
        listener.local_addr().unwrap()
    );
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut data = [0_u8; 8192];
        let n = socket.read(&mut data).await.unwrap();
        assert!(String::from_utf8_lossy(&data[..n]).contains("wait=true"));
        socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 36\r\nConnection: close\r\n\r\n{\"id\":\"1\",\"timestamp\":\"2026-01-01Z\"}").await.unwrap();
    });
    let provider = Webhook::test_endpoint(&endpoint).unwrap();
    assert!(
        discord_queue::process_one(&mut db, &env, "one", &provider)
            .await
            .unwrap()
    );
    server.await.unwrap();
    let row=db.query_one("SELECT j.status,a.state FROM public.novu_queue_jobs j JOIN public.novu_rust_discord_attempts a ON a.job_id=j.id WHERE j.id=$1",&[&rejected]).await.unwrap();
    assert_eq!(
        (row.get::<_, String>(0), row.get::<_, String>(1)),
        ("completed".into(), "accepted".into())
    );
    let pre_failure: i64 = db
        .query_one(
            "SELECT failure_revision FROM public.novu_queue_jobs WHERE id=$1",
            &[&pre],
        )
        .await
        .unwrap()
        .get(0);
    assert!(
        op.query_one(
            sql,
            &[
                &env,
                &Uuid::new_v4(),
                &pre,
                &Option::<Uuid>::None,
                &pre_failure,
                &"retry",
                &"fixed"
            ]
        )
        .await
        .unwrap()
        .get::<_, bool>(0)
    );
    db.execute(
        "UPDATE public.novu_rust_discord_resolvers SET can_retry=false WHERE environment_id=$1",
        &[&env],
    )
    .await
    .unwrap();
    assert!(
        op.query_one(
            sql,
            &[
                &env,
                &retry_id,
                &rejected,
                &Some(rejected_rev),
                &rejected_failure,
                &"retry",
                &"operator reviewed rate limit"
            ]
        )
        .await
        .is_err()
    );
}
