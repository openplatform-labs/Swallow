//! Scoped retention and replay fencing on disposable PostgreSQL.
use std::process::Command;
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;

async fn connect(url: &str) -> Client {
    let (db, driver) = tokio_postgres::connect(url, NoTls).await.unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    db
}

#[tokio::test]
#[ignore = "requires fully migrated disposable PostgreSQL and role creation"]
async fn cleanup_is_scoped_atomic_and_excludes_recoverable_jobs() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let db = connect(&url).await;
    db.batch_execute("INSERT INTO public.novu_rust_environments VALUES ('retention-a','org'),('retention-b','org')").await.unwrap();
    let viewer = format!("retention_view_{}", Uuid::new_v4().simple());
    let scrubber = format!("retention_scrub_{}", Uuid::new_v4().simple());
    db.batch_execute(&format!("CREATE ROLE {viewer} LOGIN; CREATE ROLE {scrubber} LOGIN; GRANT USAGE ON SCHEMA public TO {viewer},{scrubber}; GRANT EXECUTE ON FUNCTION public.novu_rust_list_attachment_retention(text,integer,integer),public.novu_rust_attachment_retention_audit(text,bigint) TO {viewer}; GRANT EXECUTE ON FUNCTION public.novu_rust_scrub_attachment_retention(text,integer,integer,text) TO {scrubber}")).await.unwrap();
    for (role, view, scrub) in [(&viewer, true, false), (&scrubber, false, true)] {
        db.execute("INSERT INTO public.novu_rust_attachment_retention_grants(environment_id,database_principal,principal_oid,can_view,can_scrub) SELECT 'retention-a',rolname,oid,$2,$3 FROM pg_roles WHERE rolname::text=$1",&[role,&view,&scrub]).await.unwrap();
    }
    let parent = Uuid::new_v4();
    let child = Uuid::new_v4();
    let pending = Uuid::new_v4();
    let failed = Uuid::new_v4();
    let foreign = Uuid::new_v4();
    let standalone = Uuid::new_v4();
    db.execute("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,status,payload) VALUES($1,'retention-a','experimental_static_in_app_attachments_v1','completed','{\"emailAttachments\":[{\"file\":[1]}],\"payload\":{\"name\":\"test\"}}'),($2,'retention-a','experimental_resend_email','completed','{\"attachments\":[{\"file\":[2]}],\"workflow_context\":{}}'),($3,'retention-a','experimental_sendgrid_email','pending','{\"attachments\":[{\"file\":[3]}],\"workflow_context\":{}}'),($4,'retention-a','experimental_resend_email','failed','{\"attachments\":[{\"file\":[4]}],\"workflow_context\":{}}'),($5,'retention-b','experimental_static_in_app_attachments_v1','completed','{\"emailAttachments\":[{\"file\":[5]}]}'),($6,'retention-a','experimental_sendgrid_email','completed','{\"attachments\":[{\"file\":[6]}]}')",&[&parent,&child,&pending,&failed,&foreign,&standalone]).await.unwrap();
    let view_url = url.replacen("postgres://postgres@", &format!("postgres://{viewer}@"), 1);
    let scrub_url = url.replacen(
        "postgres://postgres@",
        &format!("postgres://{scrubber}@"),
        1,
    );
    let run = |url: &str, args: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_attachment-retention"))
            .args(args)
            .env("DATABASE_URL", url)
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .output()
            .unwrap()
    };
    let listing = run(&view_url, &["list", "retention-a", "0", "100"]);
    assert!(listing.status.success());
    let values: serde_json::Value = serde_json::from_slice(&listing.stdout).unwrap();
    assert_eq!(values.as_array().unwrap().len(), 2);
    let later = run(&view_url, &["list", "retention-a", "3600", "100"]);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&later.stdout)
            .unwrap()
            .as_array()
            .unwrap()
            .len(),
        0
    );
    assert!(
        !run(&view_url, &["list", "retention-b", "0", "100"])
            .status
            .success()
    );
    assert!(
        !run(
            &view_url,
            &["scrub", "retention-a", "0", "100", "no permission"]
        )
        .status
        .success()
    );
    assert!(
        !run(&scrub_url, &["list", "retention-a", "0", "100"])
            .status
            .success()
    );
    let restricted = connect(&scrub_url).await;
    assert!(
        restricted
            .query("SELECT * FROM public.novu_queue_jobs", &[])
            .await
            .is_err()
    );
    db.batch_execute("CREATE FUNCTION public.test_retention_reject() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture'; END $$; CREATE TRIGGER test_retention_reject BEFORE INSERT ON public.novu_rust_attachment_retention_audit FOR EACH ROW EXECUTE FUNCTION public.test_retention_reject()").await.unwrap();
    assert!(
        !run(&scrub_url, &["scrub", "retention-a", "0", "100", "failure"])
            .status
            .success()
    );
    assert!(
        db.query_one(
            "SELECT payload ? 'emailAttachments' FROM public.novu_queue_jobs WHERE id=$1",
            &[&parent]
        )
        .await
        .unwrap()
        .get::<_, bool>(0)
    );
    db.batch_execute("DROP TRIGGER test_retention_reject ON public.novu_rust_attachment_retention_audit; DROP FUNCTION public.test_retention_reject()").await.unwrap();
    let mut locker = connect(&url).await;
    let tx = locker.transaction().await.unwrap();
    tx.execute(
        "UPDATE public.novu_queue_jobs SET last_error=last_error WHERE id=$1",
        &[&parent],
    )
    .await
    .unwrap();
    let result = run(
        &scrub_url,
        &["scrub", "retention-a", "0", "100", "retention policy"],
    );
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let rows: serde_json::Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(rows.as_array().unwrap().len(), 1);
    assert_eq!(rows[0]["jobId"], child.to_string());
    tx.commit().await.unwrap();
    let a = connect(&scrub_url).await;
    let b = connect(&scrub_url).await;
    let query =
        "SELECT * FROM public.novu_rust_scrub_attachment_retention('retention-a',0,100,'race')";
    let (one, two) = tokio::join!(a.query(query, &[]), b.query(query, &[]));
    assert_eq!(one.unwrap().len() + two.unwrap().len(), 1);
    for id in [parent, child] {
        let row=db.query_one("SELECT attachment_scrubbed_at IS NOT NULL,payload ? 'emailAttachments',payload ? 'attachments' FROM public.novu_queue_jobs WHERE id=$1",&[&id]).await.unwrap();
        assert!(row.get::<_, bool>(0));
        assert!(!row.get::<_, bool>(1));
        assert!(!row.get::<_, bool>(2));
        assert!(
            db.execute(
                "UPDATE public.novu_queue_jobs SET status='pending' WHERE id=$1",
                &[&id]
            )
            .await
            .is_err()
        );
        assert!(db.execute("UPDATE public.novu_queue_jobs SET payload=payload||'{\"attachments\":[1]}'::jsonb WHERE id=$1",&[&id]).await.is_err());
        assert!(
            db.execute(
                "UPDATE public.novu_queue_jobs SET kind='other' WHERE id=$1",
                &[&id]
            )
            .await
            .is_err()
        );
        db.execute(
            "UPDATE public.novu_queue_jobs SET last_error='diagnostic' WHERE id=$1",
            &[&id],
        )
        .await
        .unwrap();
    }
    for id in [pending, failed, foreign, standalone] {
        assert!(
            db.query_one(
                "SELECT attachment_scrubbed_at IS NULL FROM public.novu_queue_jobs WHERE id=$1",
                &[&id]
            )
            .await
            .unwrap()
            .get::<_, bool>(0)
        );
    }
    let audit = run(&view_url, &["audit", "retention-a"]);
    assert!(audit.status.success());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&audit.stdout)
            .unwrap()
            .as_array()
            .unwrap()
            .len(),
        2
    );
    db.execute("DELETE FROM public.novu_rust_attachment_retention_grants WHERE environment_id='retention-a' AND database_principal=$1",&[&scrubber]).await.unwrap();
    assert!(
        !run(&scrub_url, &["scrub", "retention-a", "0", "100", "revoked"])
            .status
            .success()
    );
}
