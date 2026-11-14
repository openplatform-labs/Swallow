//! Optional SMS workflow checks before reserving an external dispatch.
use crate::{ClaimedJob, twilio::Sms};
use tokio_postgres::{Client, Error, Transaction};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Policy {
    Snapshot,
    Active,
    Unchanged,
}
impl Policy {
    pub fn from_env() -> Result<Self, &'static str> {
        match std::env::var("SWALLOW_SMS_WORKFLOW_STEP_POLICY").as_deref() {
            Ok("snapshot") | Err(std::env::VarError::NotPresent) => Ok(Self::Snapshot),
            Ok("active") => Ok(Self::Active),
            Ok("unchanged") => Ok(Self::Unchanged),
            _ => Err("invalid SMS workflow step policy"),
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Snapshot => "snapshot",
            Self::Active => "active",
            Self::Unchanged => "unchanged",
        }
    }
}
pub async fn register(db: &Client, environment: &str, policy: Policy) -> Result<bool, Error> {
    Ok(db.query_opt("INSERT INTO public.novu_rust_sms_workflow_worker_config(environment_id,step_policy) VALUES ($1,$2) ON CONFLICT(environment_id) DO UPDATE SET step_policy=EXCLUDED.step_policy WHERE novu_rust_sms_workflow_worker_config.step_policy IS NULL OR novu_rust_sms_workflow_worker_config.step_policy=EXCLUDED.step_policy RETURNING 1",&[&environment,&policy.as_str()]).await?.is_some())
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
    sms: &Sms,
    policy: Policy,
) -> Result<Check, Error> {
    if policy == Policy::Snapshot {
        return Ok(Check::Continue);
    }
    let Some(ctx) = sms.workflow_context.as_ref() else {
        return Ok(Check::Continue);
    };
    let Some(id) = ctx
        .workflow_internal_id
        .as_deref()
        .filter(|id| !id.is_empty())
    else {
        return finish(tx, environment, job, Some("sms workflow identity missing")).await;
    };
    let row=tx.query_opt("SELECT active,has_steps,sms_step_active,sms_content FROM public.novu_rust_workflows WHERE environment_id=$1 AND identifier=$2 AND inbox_metadata->>'id'=$3 FOR SHARE",&[&environment,&ctx.workflow_identifier,&id]).await?;
    let reason = match row {
        None => Some("sms workflow step unavailable"),
        Some(row) if !row.get::<_, bool>(0) || !row.get::<_, bool>(1) || !row.get::<_, bool>(2) => {
            Some("sms workflow step disabled")
        }
        Some(row) => match row.get::<_, Option<String>>(3) {
            None => Some("sms workflow step disabled"),
            Some(content) if policy == Policy::Unchanged && content != ctx.content_template => {
                Some("sms workflow step changed")
            }
            Some(_) => None,
        },
    };
    finish(tx, environment, job, reason).await
}
async fn finish(
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
        tx.execute("UPDATE public.novu_rust_sms_step_outcomes SET status='canceled',sms_job_id=NULL WHERE environment_id=$1 AND sms_job_id=$2",&[&environment,&job.id]).await?;
        tx.execute("UPDATE public.novu_queue_jobs SET status='cancelled',last_error=$3,lease_token=NULL,lease_until=NULL WHERE environment_id=$1 AND id=$2",&[&environment,&job.id,&reason]).await?;
        return Ok(Check::Cancelled);
    }
    Ok(Check::Continue)
}
