-- Experimental chat-step administration. No permission is granted by default.
CREATE TABLE IF NOT EXISTS public.novu_rust_slack_workflow_permissions (
  key_hash text NOT NULL REFERENCES public.novu_rust_api_keys(key_hash) ON DELETE CASCADE,
  permission text NOT NULL CHECK (permission IN ('read','write')),
  PRIMARY KEY(key_hash,permission)
);
CREATE TABLE IF NOT EXISTS public.novu_rust_slack_workflow_audit (
  id uuid PRIMARY KEY,
  environment_id text NOT NULL,
  workflow_identifier text NOT NULL,
  api_key_hash text NOT NULL CHECK (length(api_key_hash)=64),
  user_id text NOT NULL,
  previous_active boolean NOT NULL,
  new_active boolean NOT NULL,
  previous_content_bytes integer,
  new_content_bytes integer,
  recorded_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS novu_rust_slack_workflow_audit_environment_idx ON public.novu_rust_slack_workflow_audit(environment_id,recorded_at DESC,id);
REVOKE ALL ON public.novu_rust_slack_workflow_permissions,public.novu_rust_slack_workflow_audit FROM PUBLIC;
CREATE OR REPLACE FUNCTION public.novu_rust_lock_slack_workflow_grant(p_hash text,p_environment text,p_permission text)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
  IF p_permission NOT IN ('read','write') THEN RETURN false; END IF;
  PERFORM 1 FROM public.novu_rust_api_keys k
    JOIN public.novu_rust_users u ON u.id=k.user_id
    JOIN public.novu_rust_slack_workflow_permissions p ON p.key_hash=k.key_hash
    WHERE k.key_hash=p_hash AND k.environment_id=p_environment AND k.enabled AND p.permission=p_permission
    FOR SHARE OF k,u,p;
  RETURN FOUND;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_lock_slack_workflow_grant(text,text,text) FROM PUBLIC;
