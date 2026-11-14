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
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PreferencePolicy {
    Snapshot,
    Recheck,
}
impl PreferencePolicy {
    pub fn from_env() -> Result<Self, &'static str> {
        match std::env::var("SWALLOW_PUSH_PREFERENCE_POLICY").as_deref() {
            Ok("snapshot") | Err(std::env::VarError::NotPresent) => Ok(Self::Snapshot),
            Ok("recheck") => Ok(Self::Recheck),
            _ => Err("invalid push preference policy"),
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Snapshot => "snapshot",
            Self::Recheck => "recheck",
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TargetPolicy {
    Reference,
    Registered,
}
impl TargetPolicy {
    pub fn from_env() -> Result<Self, &'static str> {
        match std::env::var("SWALLOW_PUSH_TARGET_POLICY").as_deref() {
            Ok("reference") | Err(std::env::VarError::NotPresent) => Ok(Self::Reference),
            Ok("registered") => Ok(Self::Registered),
            _ => Err("invalid push target policy"),
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Reference => "reference",
            Self::Registered => "registered",
        }
    }
}
pub async fn register(
    db: &Client,
    environment: &str,
    policy: Policy,
    preference: PreferencePolicy,
    target: TargetPolicy,
) -> Result<bool, Error> {
    Ok(db.query_opt("INSERT INTO public.novu_rust_push_workflow_worker_config(environment_id,step_policy,preference_policy,target_policy) VALUES ($1,$2,$3,$4) ON CONFLICT(environment_id) DO UPDATE SET step_policy=EXCLUDED.step_policy,preference_policy=EXCLUDED.preference_policy,target_policy=EXCLUDED.target_policy WHERE (novu_rust_push_workflow_worker_config.step_policy IS NULL OR novu_rust_push_workflow_worker_config.step_policy=EXCLUDED.step_policy) AND (novu_rust_push_workflow_worker_config.preference_policy IS NULL OR novu_rust_push_workflow_worker_config.preference_policy=EXCLUDED.preference_policy) AND (novu_rust_push_workflow_worker_config.target_policy IS NULL OR novu_rust_push_workflow_worker_config.target_policy=EXCLUDED.target_policy) RETURNING 1", &[&environment,&policy.as_str(),&preference.as_str(),&target.as_str()]).await?.is_some())
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
    finish(tx, environment, job, reason).await
}

/// Caller holds the subscriber row lock before checking current subscriber preferences.
pub async fn check_preference(
    tx: &Transaction<'_>,
    environment: &str,
    job: &ClaimedJob,
    push: &Push,
    prepared: Option<&serde_json::Value>,
    policy: PreferencePolicy,
) -> Result<Check, Error> {
    if policy == PreferencePolicy::Snapshot {
        return Ok(Check::Continue);
    }
    let info = if let Some(c) = push.workflow_context.as_ref() {
        Some((
            Some(c.subscriber_id.as_str()),
            Some(c.workflow_identifier.as_str()),
        ))
    } else if let Some(c) = prepared {
        Some((
            c.get("subscriber_id").and_then(|v| v.as_str()),
            c.get("workflow_identifier").and_then(|v| v.as_str()),
        ))
    } else if push.prepared_messages.is_some() || push.workflow_guard.is_some() {
        Some((None, None))
    } else {
        None
    };
    let Some((subscriber, workflow)) = info else {
        return Ok(Check::Continue);
    };
    let reason = if let (Some(subscriber), Some(workflow)) = (
        subscriber.filter(|x| !x.is_empty()),
        workflow.filter(|x| !x.is_empty()),
    ) {
        let row=tx.query_one("SELECT w.read_only,w.all_enabled,w.push_enabled,(p.channels->>'push')::boolean,(sp.channels->>'push')::boolean FROM (SELECT 1) s LEFT JOIN public.novu_rust_workflow_preferences w ON w.environment_id=$1 AND w.workflow_identifier=$3 LEFT JOIN public.novu_rust_subscriber_preferences p ON p.environment_id=$1 AND p.subscriber_id=$2 LEFT JOIN public.novu_rust_subscriber_workflow_preferences sp ON sp.environment_id=$1 AND sp.subscriber_id=$2 AND sp.workflow_identifier=$3", &[&environment,&subscriber,&workflow]).await?;
        if novu_rust_contracts::preference::in_app_enabled(
            row.get(0),
            row.get(1),
            row.get(2),
            row.get(3),
            row.get(4),
        ) {
            None
        } else {
            Some("push preference recheck disabled")
        }
    } else {
        Some("push preference context missing")
    };
    finish(tx, environment, job, reason).await
}
async fn finish(
    tx: &Transaction<'_>,
    environment: &str,
    job: &ClaimedJob,
    reason: Option<&str>,
) -> Result<Check, Error> {
    let live=tx.query_opt("SELECT lease_until>clock_timestamp() FROM public.novu_queue_jobs WHERE id=$1 AND environment_id=$2 AND kind=$3 AND status='running' AND lease_token=$4 FOR UPDATE", &[&job.id,&environment,&job.kind,&job.lease_token]).await?.is_some_and(|r|r.get::<_,bool>(0));
    if !live {
        return Ok(Check::LeaseLost);
    }
    if let Some(reason) = reason {
        tx.execute("UPDATE public.novu_rust_push_messages SET delivery_state='unknown',status='error',error_id='unexpected_push_error',error_text='push delivery partially stopped by policy' WHERE environment_id=$1 AND push_job_id=$2 AND delivery_state='in_progress'", &[&environment,&job.id]).await?;
        tx.execute("UPDATE public.novu_rust_push_step_outcomes SET status='canceled',push_job_id=NULL WHERE environment_id=$1 AND push_job_id=$2", &[&environment,&job.id]).await?;
        tx.execute("UPDATE public.novu_queue_jobs SET status='cancelled',last_error=$3,lease_token=NULL,lease_until=NULL WHERE environment_id=$1 AND id=$2", &[&environment,&job.id,&reason]).await?;
        return Ok(Check::Cancelled);
    }
    Ok(Check::Continue)
}
