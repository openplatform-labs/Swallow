//! The stored-workflow trigger gate, after workflow lookup and payload validation.
//! Source: apps/api/src/app/events/usecases/parse-event-request/parse-event-request.usecase.ts.
//! Bridge workflows, authentication, schema validation and queue dispatch are outside this module.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TenantState {
    NotSpecified,
    Missing,
    Found {
        workflow_override_active: Option<bool>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GateResult {
    TenantMissing,
    WorkflowInactive,
    NoWorkflowSteps,
    Dispatch,
}

impl GateResult {
    /// The status values used by TriggerEventStatusEnum in the reference revision.
    /// Dispatch represents permission to continue, not proof that enqueue succeeded.
    pub fn status(self) -> &'static str {
        match self {
            Self::TenantMissing => "no_tenant_found",
            Self::WorkflowInactive => "trigger_not_active",
            Self::NoWorkflowSteps => "no_workflow_steps_defined",
            Self::Dispatch => "processed",
        }
    }
}

/// Apply the stored-workflow guards in the same order as the reference code.
pub fn evaluate(workflow_active: bool, has_steps: bool, tenant: TenantState) -> GateResult {
    let active = match tenant {
        TenantState::Missing => return GateResult::TenantMissing,
        TenantState::NotSpecified => workflow_active,
        TenantState::Found {
            workflow_override_active,
        } => workflow_override_active.unwrap_or(workflow_active),
    };
    if !active {
        return GateResult::WorkflowInactive;
    }
    if !has_steps {
        return GateResult::NoWorkflowSteps;
    }
    GateResult::Dispatch
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stored_workflow_guards_match_reference_precedence_and_statuses() {
        let cases = [
            (
                false,
                false,
                TenantState::Missing,
                GateResult::TenantMissing,
                "no_tenant_found",
            ),
            (
                false,
                false,
                TenantState::NotSpecified,
                GateResult::WorkflowInactive,
                "trigger_not_active",
            ),
            (
                true,
                false,
                TenantState::NotSpecified,
                GateResult::NoWorkflowSteps,
                "no_workflow_steps_defined",
            ),
            (
                true,
                true,
                TenantState::NotSpecified,
                GateResult::Dispatch,
                "processed",
            ),
            (
                false,
                true,
                TenantState::Found {
                    workflow_override_active: Some(true),
                },
                GateResult::Dispatch,
                "processed",
            ),
            (
                true,
                true,
                TenantState::Found {
                    workflow_override_active: Some(false),
                },
                GateResult::WorkflowInactive,
                "trigger_not_active",
            ),
            (
                false,
                true,
                TenantState::Found {
                    workflow_override_active: None,
                },
                GateResult::WorkflowInactive,
                "trigger_not_active",
            ),
        ];
        for (active, steps, tenant, expected, status) in cases {
            let result = evaluate(active, steps, tenant);
            assert_eq!(result, expected);
            assert_eq!(result.status(), status);
        }
    }
}
