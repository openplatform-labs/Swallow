//! Single-channel subset of Novu's workflow-resource and subscriber-global merge.
pub fn in_app_enabled(
    read_only: Option<bool>,
    all_enabled: Option<bool>,
    workflow_channel: Option<bool>,
    subscriber_channel: Option<bool>,
    subscriber_workflow_channel: Option<bool>,
) -> bool {
    let workflow = workflow_channel.or(all_enabled).unwrap_or(true);
    if read_only.unwrap_or(false) {
        workflow
    } else {
        subscriber_workflow_channel
            .or(subscriber_channel)
            .unwrap_or(workflow)
    }
}
