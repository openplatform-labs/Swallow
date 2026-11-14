//! Optional per-target workflow checks before FCM reservation.
use crate::{ClaimedJob, fcm::Push};
use tokio_postgres::{Client, Error, Transaction};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Policy {
    Snapshot,
    Active,
    Unchanged,
}
impl Policy {
    pub fn from_env() -> Result<Self, &'static str> {
        match std::env::var("SWALLOW_PUSH_WORKFLOW_STEP_POLICY").as_deref() {
            Ok("snapshot") | Err(std::env::VarError::NotPresent) => Ok(Self::Snapshot),
            Ok("active") => Ok(Self::Active),
            Ok("unchanged") => Ok(Self::Unchanged),
            _ => Err("invalid push workflow step policy"),
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
    Ok(db.query_opt("INSERT INTO public.novu_rust_push_workflow_worker_config(environment_id,step_policy) VALUES ($1,$2) ON CONFLICT(environment_id) DO UPDATE SET step_policy=EXCLUDED.step_policy WHERE novu_rust_push_workflow_worker_config.step_policy IS NULL OR novu_rust_push_workflow_worker_config.step_policy=EXCLUDED.step_policy RETURNING 1", &[&environment,&policy.as_str()]).await?.is_some())
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
    push: &Push,
    prepared: Option<&serde_json::Value>,
    policy: Policy,
) -> Result<Check, Error> {
    if policy == Policy::Snapshot {
        return Ok(Check::Continue);
    }
    let snapshot = if let Some(c) = push.workflow_context.as_ref() {
        Some((
            Some(c.workflow_identifier.as_str()),
            c.workflow_internal_id.as_deref(),
            Some(c.title_template.as_str()),
            Some(c.content_template.as_str()),
        ))
    } else if push.workflow_guard.is_some()
        || prepared.is_some()
        || push.prepared_messages.is_some()
    {
        let g = push
            .workflow_guard
            .as_ref()
            .or_else(|| prepared.and_then(|c| c.get("workflow_guard")));
        Some((
            g.and_then(|v| v.get("identifier")).and_then(|v| v.as_str()),
            g.and_then(|v| v.get("id")).and_then(|v| v.as_str()),
            g.and_then(|v| v.get("title")).and_then(|v| v.as_str()),
            g.and_then(|v| v.get("content")).and_then(|v| v.as_str()),
        ))
    } else {
        None
    };
    let Some((workflow, id, title, content)) = snapshot else {
        return Ok(Check::Continue);
    };
    let reason = if let (Some(workflow), Some(id), Some(title), Some(content)) = (
        workflow.filter(|x| !x.is_empty()),
        id.filter(|x| !x.is_empty()),
        title,
        content,
    ) {
        let row=tx.query_opt("SELECT active,has_steps,push_step_active,push_title,push_content FROM public.novu_rust_workflows WHERE environment_id=$1 AND identifier=$2 AND inbox_metadata->>'id'=$3 FOR SHARE", &[&environment,&workflow,&id]).await?;
        match row {
            None => Some("push workflow step unavailable"),
            Some(row)
                if !row.get::<_, bool>(0) || !row.get::<_, bool>(1) || !row.get::<_, bool>(2) =>
            {
                Some("push workflow step disabled")
            }
            Some(row) => {
                let current_title: Option<String> = row.get(3);
                let current_content: Option<String> = row.get(4);
                if current_title.is_none() || current_content.is_none() {
                    Some("push workflow step disabled")
                } else if policy == Policy::Unchanged
                    && (current_title.as_deref() != Some(title)
                        || current_content.as_deref() != Some(content))
                {
                    Some("push workflow step changed")
                } else {
                    None
                }
            }
        }
    } else {
        Some("push workflow identity or snapshot missing")
    };
    let live=tx.query_opt("SELECT lease_until>clock_timestamp() FROM public.novu_queue_jobs WHERE id=$1 AND environment_id=$2 AND kind=$3 AND status='running' AND lease_token=$4 FOR UPDATE", &[&job.id,&environment,&job.kind,&job.lease_token]).await?.is_some_and(|r|r.get::<_,bool>(0));
    if !live {
        return Ok(Check::LeaseLost);
    }
    if let Some(reason) = reason {
        tx.execute("UPDATE public.novu_rust_push_messages SET delivery_state='unknown',status='error',error_id='unexpected_push_error',error_text='push delivery partially stopped by workflow policy' WHERE environment_id=$1 AND push_job_id=$2 AND delivery_state='in_progress'", &[&environment,&job.id]).await?;
        tx.execute("UPDATE public.novu_rust_push_step_outcomes SET status='canceled',push_job_id=NULL WHERE environment_id=$1 AND push_job_id=$2", &[&environment,&job.id]).await?;
        tx.execute("UPDATE public.novu_queue_jobs SET status='cancelled',last_error=$3,lease_token=NULL,lease_until=NULL WHERE environment_id=$1 AND id=$2", &[&environment,&job.id,&reason]).await?;
        return Ok(Check::Cancelled);
    }
    Ok(Check::Continue)
}
