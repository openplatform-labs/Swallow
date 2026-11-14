//! Mixed-version worker fencing for attachment-bearing workflow jobs.
use novu_postgres_queue::{claim, in_app, requeue_expired};
use std::time::Duration;
use tokio_postgres::NoTls;
use uuid::Uuid;

struct Server(std::process::Child);
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test]
#[ignore = "requires fully migrated disposable PostgreSQL"]
async fn old_workers_cannot_claim_new_or_recovered_attachment_jobs() {
    let (mut db, driver) =
        tokio_postgres::connect(&std::env::var("TEST_DATABASE_URL").unwrap(), NoTls)
            .await
            .unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    db.batch_execute("INSERT INTO public.novu_rust_environments VALUES ('fence-attachment','org')")
        .await
        .unwrap();
    let old = Uuid::new_v4();
    let normal = Uuid::new_v4();
    let malformed = Uuid::new_v4();
    db.execute("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,available_at) VALUES ($1,'fence-attachment',$4,'{\"emailAttachments\":[{\"file\":[1]}]}'::jsonb,now()-interval '2 seconds'),($2,'fence-attachment',$4,'{\"emailAttachments\":[]}'::jsonb,now()-interval '1 second'),($3,'fence-attachment',$4,'{\"emailAttachments\":null}'::jsonb,now())", &[&old,&normal,&malformed,&in_app::JOB_KIND]).await.unwrap();
    let rows = db
        .query(
            "SELECT id,kind FROM public.novu_queue_jobs WHERE environment_id='fence-attachment'",
            &[],
        )
        .await
        .unwrap();
    for row in rows {
        let id: Uuid = row.get(0);
        let kind: String = row.get(1);
        assert_eq!(
            kind,
            if id == normal {
                in_app::JOB_KIND
            } else {
                in_app::ATTACHMENT_JOB_KIND
            }
        );
    }
    // The pre-upgrade claim logic sees the ordinary job only.
    let claimed = claim(&db, in_app::JOB_KIND, Duration::from_secs(60))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claimed.id, normal);
    assert!(
        claim(&db, in_app::JOB_KIND, Duration::from_secs(60))
            .await
            .unwrap()
            .is_none()
    );
    // New worker consumes both kinds in time order; these intentionally invalid jobs fail safely.
    assert!(in_app::process_one(&mut db).await.unwrap());
    assert_eq!(
        db.query_one(
            "SELECT status FROM public.novu_queue_jobs WHERE id=$1",
            &[&old]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "failed"
    );
    assert!(in_app::process_one(&mut db).await.unwrap());
    assert_eq!(
        db.query_one(
            "SELECT status FROM public.novu_queue_jobs WHERE id=$1",
            &[&malformed]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "failed"
    );
    // Previously running legacy jobs are fenced when lease recovery returns them to pending.
    let recovered = Uuid::new_v4();
    let token = Uuid::new_v4();
    db.execute("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,status,lease_token,lease_until) VALUES($1,'fence-attachment',$2,'{\"emailAttachments\":[1]}'::jsonb,'running',$3,now()-interval '1 second')",&[&recovered,&in_app::JOB_KIND,&token]).await.unwrap();
    assert_eq!(requeue_expired(&db, in_app::JOB_KIND).await.unwrap(), 1);
    let row = db
        .query_one(
            "SELECT kind,status FROM public.novu_queue_jobs WHERE id=$1",
            &[&recovered],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>(0), in_app::ATTACHMENT_JOB_KIND);
    assert_eq!(row.get::<_, String>(1), "pending");
    assert!(
        claim(&db, in_app::JOB_KIND, Duration::from_secs(60))
            .await
            .unwrap()
            .is_none()
    );
    let reclaimed = claim(&db, in_app::ATTACHMENT_JOB_KIND, Duration::from_millis(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reclaimed.id, recovered);
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(
        requeue_expired(&db, in_app::ATTACHMENT_JOB_KIND)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        db.query_one(
            "SELECT status FROM public.novu_queue_jobs WHERE id=$1",
            &[&recovered]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "pending"
    );
    let earlier = Uuid::new_v4();
    db.execute("INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,available_at) VALUES($1,'fence-attachment',$2,'{}'::jsonb,now()-interval '30 seconds')", &[&earlier,&in_app::JOB_KIND]).await.unwrap();
    assert!(in_app::process_one(&mut db).await.unwrap());
    assert_eq!(
        db.query_one(
            "SELECT status FROM public.novu_queue_jobs WHERE id=$1",
            &[&earlier]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "failed"
    );
    assert_eq!(
        db.query_one(
            "SELECT status FROM public.novu_queue_jobs WHERE id=$1",
            &[&recovered]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "pending"
    );
    db.execute("UPDATE public.novu_queue_jobs SET status='running',lease_token=gen_random_uuid(),lease_until=now()-interval '1 second' WHERE id=$1",&[&recovered]).await.unwrap();
    let _server = Server(
        std::process::Command::new(env!("CARGO_BIN_EXE_in-app-worker"))
            .env("DATABASE_URL", std::env::var("TEST_DATABASE_URL").unwrap())
            .env("NOVU_POSTGRES_ALLOW_INSECURE", "true")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap(),
    );
    let mut processed = false;
    for _ in 0..100 {
        processed = db
            .query_one(
                "SELECT status='failed' FROM public.novu_queue_jobs WHERE id=$1",
                &[&recovered],
            )
            .await
            .unwrap()
            .get(0);
        if processed {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        processed,
        "worker did not recover and process an expired attachment job"
    );
}
