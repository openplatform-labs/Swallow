//! Optional checks for workflow email steps immediately before dispatch.
use crate::{ClaimedJob, sendgrid::Email};
use tokio_postgres::{Error, Transaction};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Policy {
    Snapshot,
    Active,
    Unchanged,
}
impl Policy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Snapshot => "snapshot",
            Self::Active => "active",
            Self::Unchanged => "unchanged",
        }
    }
    pub fn from_env() -> Result<Self, &'static str> {
        match std::env::var("SWALLOW_EMAIL_WORKFLOW_STEP_POLICY").as_deref() {
            Ok("snapshot") | Err(std::env::VarError::NotPresent) => Ok(Self::Snapshot),
            Ok("active") => Ok(Self::Active),
            Ok("unchanged") => Ok(Self::Unchanged),
            _ => Err("invalid email workflow step policy"),
        }
    }
}

/// The caller must protect this table and coordinate older workers separately.
pub async fn register(
    db: &tokio_postgres::Client,
    environment: &str,
    policy: Policy,
) -> Result<bool, Error> {
    Ok(db.query_opt("INSERT INTO public.novu_rust_email_workflow_worker_config(environment_id,step_policy) VALUES ($1,$2) ON CONFLICT(environment_id) DO UPDATE SET step_policy=EXCLUDED.step_policy WHERE novu_rust_email_workflow_worker_config.step_policy IS NULL OR novu_rust_email_workflow_worker_config.step_policy=EXCLUDED.step_policy RETURNING 1",&[&environment,&policy.as_str()]).await?.is_some())
}

pub enum Check {
    Continue,
    Cancelled,
    LeaseLost,
}

pub async fn check(
    tx: &Transaction<'_>,
    environment: &str,
    job: &ClaimedJob,
    email: &Email,
    policy: Policy,
) -> Result<Check, Error> {
    if policy == Policy::Snapshot {
        return Ok(Check::Continue);
    }
    let Some(context) = email.workflow_context.as_ref() else {
        return Ok(Check::Continue);
    };
    let workflow = email.workflow_identifier.as_deref().unwrap_or("");
    let Some(id) = context
        .workflow_internal_id
        .as_deref()
        .filter(|id| !id.is_empty())
    else {
        return finish_check(
            tx,
            environment,
            job,
            Some("email workflow identity missing"),
        )
        .await;
    };
    let row=tx.query_opt("SELECT active,has_steps,email_step_active,email_subject,email_html FROM public.novu_rust_workflows WHERE environment_id=$1 AND identifier=$2 AND inbox_metadata->>'id'=$3 FOR SHARE",&[&environment,&workflow,&id]).await?;
    let reason = match row {
        None => Some("email workflow step unavailable"),
        Some(row) if !row.get::<_, bool>(0) || !row.get::<_, bool>(1) || !row.get::<_, bool>(2) => {
            Some("email workflow step disabled")
        }
        Some(row) => {
            let title: Option<String> = row.get(3);
            let content: Option<String> = row.get(4);
            if title.is_none() || content.is_none() {
                Some("email workflow step disabled")
            } else if policy == Policy::Unchanged
                && (title.as_deref() != Some(context.subject_template.as_str())
                    || content.as_deref() != Some(context.html_template.as_str()))
            {
                Some("email workflow step changed")
            } else {
                None
            }
        }
    };
    finish_check(tx, environment, job, reason).await
}

async fn finish_check(
    tx: &Transaction<'_>,
    environment: &str,
    job: &ClaimedJob,
    reason: Option<&str>,
) -> Result<Check, Error> {
    let live=tx.query_opt("SELECT lease_until>clock_timestamp() FROM public.novu_queue_jobs WHERE id=$1 AND environment_id=$2 AND kind=$3 AND status='running' AND lease_token=$4 FOR UPDATE",&[&job.id,&environment,&job.kind,&job.lease_token]).await?.is_some_and(|r|r.get::<_,bool>(0));
    if !live {
        return Ok(Check::LeaseLost);
    }
    if let Some(reason) = reason {
        tx.execute("UPDATE public.novu_rust_email_step_outcomes SET status='canceled',email_job_id=NULL WHERE environment_id=$1 AND email_job_id=$2",&[&environment,&job.id]).await?;
        tx.execute("UPDATE public.novu_queue_jobs SET status='cancelled',last_error=$3,lease_token=NULL,lease_until=NULL WHERE environment_id=$1 AND id=$2",&[&environment,&job.id,&reason]).await?;
        return Ok(Check::Cancelled);
    }
    Ok(Check::Continue)
}
