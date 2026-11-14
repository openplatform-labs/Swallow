-- Preserve create/delete audit rows and extend the action set for PATCH.
ALTER TABLE public.novu_rust_slack_endpoint_audit DROP CONSTRAINT IF EXISTS novu_rust_slack_endpoint_audit_action_check;
ALTER TABLE public.novu_rust_slack_endpoint_audit ADD CONSTRAINT novu_rust_slack_endpoint_audit_action_check CHECK (action IN ('create','update','delete'));
