//! Durable single-attempt SMTP queue. A recorded send is never automatically retried.
use crate::{
    ClaimedJob, email_step_policy, sendgrid::Email, sendgrid_queue, smtp_alert::SmtpAlert,
};
use tokio_postgres::{Client, Error, Transaction};
use uuid::Uuid;
pub const JOB_KIND: &str = "experimental_smtp_email";
async fn claim(client: &Client, environment: &str) -> Result<Option<ClaimedJob>, Error> {
    let token = Uuid::new_v4();
    let row=client.query_opt("WITH candidate AS (SELECT id FROM public.novu_queue_jobs WHERE environment_id=$1 AND kind=$2 AND status='pending' AND available_at<=clock_timestamp() ORDER BY available_at,created_at,id FOR UPDATE SKIP LOCKED LIMIT 1) UPDATE public.novu_queue_jobs j SET status='running',attempts=j.attempts+1,lease_token=$3,lease_until=clock_timestamp()+interval '60 seconds' FROM candidate WHERE j.id=candidate.id RETURNING j.id,j.environment_id,j.kind,j.payload,j.attempts",&[&environment,&JOB_KIND,&token]).await?;
    Ok(row.map(|r| ClaimedJob {
        id: r.get(0),
        environment_id: r.get(1),
        kind: r.get(2),
        payload: r.get(3),
        attempts: r.get(4),
        lease_token: token,
    }))
}
async fn lock_live(tx: &Transaction<'_>, job: &ClaimedJob) -> Result<bool, Error> {
    let row=tx.query_opt("SELECT lease_until FROM public.novu_queue_jobs WHERE id=$1 AND environment_id=$2 AND kind=$3 AND status='running' AND lease_token=$4 FOR UPDATE",&[&job.id,&job.environment_id,&JOB_KIND,&job.lease_token]).await?;
    if row.is_none() {
        return Ok(false);
    }
    Ok(tx
        .query_one(
            "SELECT lease_until>clock_timestamp() FROM public.novu_queue_jobs WHERE id=$1",
            &[&job.id],
        )
        .await?
        .get(0))
}
pub async fn process_one(
    client: &mut Client,
    environment: &str,
    provider: &SmtpAlert,
    policy: email_step_policy::Policy,
) -> Result<bool, Error> {
    process_one_with_transport_guard(client, environment, provider, policy, false).await
}

pub async fn process_one_with_transport_guard(
    client: &mut Client,
    environment: &str,
    provider: &SmtpAlert,
    policy: email_step_policy::Policy,
    transport_guard: bool,
) -> Result<bool, Error> {
    let Some(job) = claim(client, environment).await? else {
        return Ok(false);
    };
    let mut email: Email = match serde_json::from_value(job.payload.clone()) {
        Ok(v) => v,
        Err(_) => {
            crate::fail(client, job.id, job.lease_token, "invalid SMTP job").await?;
            return Ok(true);
        }
    };
    let tx = client.transaction().await?;
    let subscriber = sendgrid_queue::lock_workflow_subscriber(&tx, environment, &email).await?;
    if !lock_live(&tx, &job).await? {
        tx.rollback().await?;
        return Ok(true);
    }
    match email_step_policy::check(&tx, environment, &job, &email, policy).await? {
        email_step_policy::Check::Continue => {}
        email_step_policy::Check::Cancelled => {
            tx.commit().await?;
            return Ok(true);
        }
        email_step_policy::Check::LeaseLost => {
            tx.rollback().await?;
            return Ok(true);
        }
    }
    if !sendgrid_queue::prepare_workflow_email(&tx, environment, &job, &mut email, subscriber)
        .await?
    {
        tx.commit().await?;
        return Ok(true);
    }
    let message = match provider.prepare_workflow(&email) {
        Ok(v) => v,
        Err(_) => {
            tx.execute("UPDATE public.novu_rust_email_messages SET delivery_state='rejected',status='error',error_id='mail_unexpected_error',error_text='unsupported SMTP email' WHERE environment_id=$1 AND email_job_id=$2",&[&environment,&job.id]).await?;
            tx.execute("UPDATE public.novu_queue_jobs SET status='failed',last_error='unsupported SMTP email',lease_token=NULL,lease_until=NULL WHERE id=$1",&[&job.id]).await?;
            tx.commit().await?;
            return Ok(true);
        }
    };
    if !lock_live(&tx, &job).await? {
        tx.rollback().await?;
        return Ok(true);
    }
    if transport_guard {
        tx.query_one("SELECT public.novu_rust_smtp_transport_guard($1,$2,$3,$4)", &[&environment,&job.id,&job.lease_token,&provider.transport_mode()]).await?;
    }
    let inserted=tx.execute("INSERT INTO public.novu_rust_smtp_attempts(job_id,environment_id,state) VALUES ($1,$2,'dispatching') ON CONFLICT(job_id) DO NOTHING",&[&job.id,&environment]).await?;
    if inserted == 0 {
        tx.execute("UPDATE public.novu_queue_jobs SET status='failed',last_error='SMTP attempt already exists',lease_token=NULL,lease_until=NULL WHERE id=$1",&[&job.id]).await?;
        tx.commit().await?;
        return Ok(true);
    }
    tx.execute("UPDATE public.novu_rust_email_messages SET delivery_state='dispatching' WHERE environment_id=$1 AND email_job_id=$2",&[&environment,&job.id]).await?;
    if !lock_live(&tx, &job).await? {
        tx.rollback().await?;
        return Ok(true);
    }
    tx.commit().await?;
    let accepted = provider.deliver_workflow(message).await.is_ok();
    let state = if accepted { "accepted" } else { "unknown" };
    let diagnostic = if accepted {
        None
    } else {
        Some("SMTP delivery outcome unknown")
    };
    let tx = client.transaction().await?;
    if !lock_live(&tx, &job).await? {
        tx.rollback().await?;
        return Ok(true);
    }
    let updated=tx.execute("UPDATE public.novu_rust_smtp_attempts SET state=$2,updated_at=clock_timestamp() WHERE job_id=$1 AND environment_id=$3 AND state='dispatching'",&[&job.id,&state,&environment]).await?;
    if updated == 1 {
        tx.execute("UPDATE public.novu_rust_email_messages SET delivery_state=$3,status=CASE WHEN $3='accepted' THEN 'sent' ELSE 'error' END,error_id=CASE WHEN $3='accepted' THEN NULL ELSE 'mail_unexpected_error' END,error_text=$4 WHERE environment_id=$1 AND email_job_id=$2",&[&environment,&job.id,&state,&diagnostic]).await?;
        tx.execute("UPDATE public.novu_queue_jobs SET status=$2,last_error=$3,lease_token=NULL,lease_until=NULL WHERE id=$1",&[&job.id,&if accepted{"completed"}else{"failed"},&diagnostic]).await?;
    }
    tx.commit().await?;
    Ok(true)
}
pub async fn recover_expired(client: &mut Client, environment: &str) -> Result<usize, Error> {
    let tx = client.transaction().await?;
    let rows=tx.query("SELECT id FROM public.novu_queue_jobs WHERE environment_id=$1 AND kind=$2 AND status='running' AND lease_until<=clock_timestamp() ORDER BY lease_until,id FOR UPDATE SKIP LOCKED LIMIT 100",&[&environment,&JOB_KIND]).await?;
    for row in &rows {
        let id: Uuid = row.get(0);
        let attempted=tx.query_opt("SELECT state FROM public.novu_rust_smtp_attempts WHERE environment_id=$1 AND job_id=$2",&[&environment,&id]).await?.is_some();
        if attempted {
            tx.execute("UPDATE public.novu_rust_smtp_attempts SET state='unknown',updated_at=clock_timestamp() WHERE job_id=$1 AND state='dispatching'",&[&id]).await?;
            tx.execute("UPDATE public.novu_rust_email_messages SET delivery_state='unknown',status='error',error_id='mail_unexpected_error',error_text='SMTP delivery outcome unknown' WHERE environment_id=$1 AND email_job_id=$2",&[&environment,&id]).await?;
            tx.execute("UPDATE public.novu_queue_jobs SET status='failed',last_error='SMTP delivery outcome unknown',lease_token=NULL,lease_until=NULL WHERE id=$1",&[&id]).await?;
        } else {
            tx.execute("UPDATE public.novu_queue_jobs SET status='pending',lease_token=NULL,lease_until=NULL WHERE id=$1",&[&id]).await?;
        }
    }
    tx.commit().await?;
    Ok(rows.len())
}
