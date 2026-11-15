use std::path::Path;
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;

async fn migrate(db: &Client, dir: &Path, range: std::ops::RangeInclusive<u32>) {
    let mut files: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "sql"))
        .collect();
    files.sort();
    for file in files {
        let name = file.file_name().unwrap().to_str().unwrap();
        if range.contains(&name[..4].parse::<u32>().unwrap()) {
            db.batch_execute(&std::fs::read_to_string(&file).unwrap())
                .await
                .unwrap_or_else(|e| panic!("{name}: {e}"));
        }
    }
}
#[tokio::test]
#[ignore = "requires fresh disposable PostgreSQL and role creation"]
async fn smtp_retention_backfill_and_scoped_replay_fence() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let (db, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(driver);
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    migrate(&db, &root.join("../../api/rust-api/migrations"), 1..=17).await;
    migrate(&db, &root.join("migrations"), 1..=215).await;
    let env = format!("smtp-retention-{}", Uuid::new_v4());
    let other = format!("smtp-other-{}", Uuid::new_v4());
    let role = format!("smtp_retention_{}", Uuid::new_v4().simple());
    db.execute(
        "INSERT INTO public.novu_rust_environments VALUES($1,'org'),($2,'org')",
        &[&env, &other],
    )
    .await
    .unwrap();
    let old = Uuid::new_v4();
    let fresh = Uuid::new_v4();
    let pending = Uuid::new_v4();
    let standalone = Uuid::new_v4();
    let foreign = Uuid::new_v4();
    let workflow = serde_json::json!({"workflow_context":{},"attachments":[{"file":[1,2,3]}]});
    let standalone_payload = serde_json::json!({"attachments":[{"file":[1,2,3]}]});
    db.execute("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,status,payload) VALUES($1,$2,'experimental_smtp_email','completed',$3)",&[&old,&env,&workflow]).await.unwrap();
    assert!(
        db.query_one(
            "SELECT attachment_completed_at IS NULL FROM public.novu_queue_jobs WHERE id=$1",
            &[&old]
        )
        .await
        .unwrap()
        .get::<_, bool>(0)
    );
    migrate(&db, &root.join("migrations"), 216..=216).await;
    assert!(
        db.query_one(
            "SELECT attachment_completed_at IS NOT NULL FROM public.novu_queue_jobs WHERE id=$1",
            &[&old]
        )
        .await
        .unwrap()
        .get::<_, bool>(0)
    );
    for (id, e, status, payload) in [
        (fresh, &env, "completed", &workflow),
        (pending, &env, "pending", &workflow),
        (standalone, &env, "completed", &standalone_payload),
        (foreign, &other, "completed", &workflow),
    ] {
        db.execute("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,status,payload) VALUES($1,$2,'experimental_smtp_email',$3,$4)",&[&id,e,&status,payload]).await.unwrap();
    }
    db.batch_execute(&format!("CREATE ROLE {role} LOGIN; GRANT USAGE ON SCHEMA public TO {role}; GRANT EXECUTE ON FUNCTION public.novu_rust_list_attachment_retention(text,integer,integer),public.novu_rust_scrub_attachment_retention(text,integer,integer,text),public.novu_rust_attachment_retention_audit(text,bigint) TO {role}")).await.unwrap();
    db.execute("INSERT INTO public.novu_rust_attachment_retention_grants(environment_id,database_principal,principal_oid,can_view,can_scrub) SELECT $1,rolname,oid,true,true FROM pg_roles WHERE rolname::text=$2",&[&env,&role]).await.unwrap();
    let (reader, driver) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(driver);
    reader
        .batch_execute(&format!("SET SESSION AUTHORIZATION {role}"))
        .await
        .unwrap();
    assert!(
        reader
            .query(
                "SELECT * FROM public.novu_rust_list_attachment_retention($1,0,100)",
                &[&other]
            )
            .await
            .is_err()
    );
    assert!(
        reader
            .query("SELECT * FROM public.novu_queue_jobs", &[])
            .await
            .is_err()
    );
    let rows = reader
        .query(
            "SELECT * FROM public.novu_rust_list_attachment_retention($1,0,100)",
            &[&env],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    db.batch_execute("CREATE FUNCTION public.smtp_retention_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture'; END $$; CREATE TRIGGER smtp_retention_fail BEFORE INSERT ON public.novu_rust_attachment_retention_audit FOR EACH ROW EXECUTE FUNCTION public.smtp_retention_fail()").await.unwrap();
    assert!(
        reader
            .query(
                "SELECT * FROM public.novu_rust_scrub_attachment_retention($1,0,100,'fixture')",
                &[&env]
            )
            .await
            .is_err()
    );
    assert_eq!(
        reader
            .query(
                "SELECT * FROM public.novu_rust_list_attachment_retention($1,0,100)",
                &[&env]
            )
            .await
            .unwrap()
            .len(),
        2
    );
    db.batch_execute("DROP TRIGGER smtp_retention_fail ON public.novu_rust_attachment_retention_audit; DROP FUNCTION public.smtp_retention_fail()").await.unwrap();
    assert_eq!(
        reader
            .query(
                "SELECT * FROM public.novu_rust_scrub_attachment_retention($1,0,100,'fixture')",
                &[&env]
            )
            .await
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        reader
            .query(
                "SELECT * FROM public.novu_rust_attachment_retention_audit($1,0)",
                &[&env]
            )
            .await
            .unwrap()
            .len(),
        2
    );
    for id in [old, fresh] {
        let row=db.query_one("SELECT attachment_scrubbed_at IS NOT NULL,payload ? 'attachments' FROM public.novu_queue_jobs WHERE id=$1",&[&id]).await.unwrap();
        assert!(row.get::<_, bool>(0));
        assert!(!row.get::<_, bool>(1));
        assert!(
            db.execute(
                "UPDATE public.novu_queue_jobs SET status='pending' WHERE id=$1",
                &[&id]
            )
            .await
            .is_err()
        );
    }
    for id in [pending, standalone, foreign] {
        assert!(db.query_one("SELECT attachment_scrubbed_at IS NULL AND payload ? 'attachments' FROM public.novu_queue_jobs WHERE id=$1",&[&id]).await.unwrap().get::<_,bool>(0));
    }
    db.execute("DELETE FROM public.novu_rust_attachment_retention_grants WHERE environment_id=$1 AND database_principal=$2",&[&env,&role]).await.unwrap();
    assert!(
        reader
            .query(
                "SELECT * FROM public.novu_rust_list_attachment_retention($1,0,100)",
                &[&env]
            )
            .await
            .is_err()
    );
}
