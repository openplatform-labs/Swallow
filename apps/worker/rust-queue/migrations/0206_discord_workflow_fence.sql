-- One administratively selected policy per environment. Enabling waits for
-- in-flight legacy completions that hold the policy row lock.
LOCK TABLE public.novu_rust_environments IN SHARE ROW EXCLUSIVE MODE;
CREATE TABLE public.novu_rust_discord_workflow_policy (
 environment_id text PRIMARY KEY REFERENCES public.novu_rust_environments(id) ON DELETE CASCADE,
 enabled boolean NOT NULL DEFAULT false
);
REVOKE ALL ON public.novu_rust_discord_workflow_policy FROM PUBLIC;
INSERT INTO public.novu_rust_discord_workflow_policy(environment_id) SELECT id FROM public.novu_rust_environments;
CREATE FUNCTION public.novu_rust_discord_workflow_init_policy() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN INSERT INTO public.novu_rust_discord_workflow_policy(environment_id) VALUES(NEW.id); RETURN NEW; END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_discord_workflow_init_policy() FROM PUBLIC;
CREATE TRIGGER novu_rust_discord_workflow_init_policy_trigger AFTER INSERT ON public.novu_rust_environments
 FOR EACH ROW EXECUTE FUNCTION public.novu_rust_discord_workflow_init_policy();

-- Cooperative marker: the caller already owns the live job row and must use
-- the transaction that creates the child jobs and completes the parent.
CREATE FUNCTION public.novu_rust_discord_workflow_guard(p_env text,p_job uuid,p_lease uuid) RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE enabled boolean;
BEGIN
 SELECT p.enabled INTO enabled FROM public.novu_rust_discord_workflow_policy p WHERE p.environment_id=p_env FOR SHARE;
 IF NOT FOUND THEN RAISE EXCEPTION 'Discord workflow policy missing' USING ERRCODE='55000'; END IF;
 PERFORM 1 FROM public.novu_queue_jobs j WHERE j.id=p_job AND j.environment_id=p_env AND j.kind IN ('experimental_static_in_app','experimental_static_in_app_attachments_v1') AND j.status='running' AND j.lease_token=p_lease AND j.lease_until>clock_timestamp();
 IF NOT FOUND THEN RAISE EXCEPTION 'Discord workflow lease lost' USING ERRCODE='55000'; END IF;
 IF enabled THEN PERFORM pg_catalog.set_config('swallow.discord_workflow_fence',p_job::text||':'||p_lease::text,true); END IF;
 RETURN enabled;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_discord_workflow_guard(text,uuid,uuid) FROM PUBLIC;

CREATE FUNCTION public.novu_rust_discord_workflow_fence() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE enabled boolean;
BEGIN
 IF OLD.kind NOT IN ('experimental_static_in_app','experimental_static_in_app_attachments_v1') OR OLD.status<>'running' OR NEW.status NOT IN ('completed','failed') THEN RETURN NEW; END IF;
 SELECT p.enabled INTO enabled FROM public.novu_rust_discord_workflow_policy p WHERE p.environment_id=OLD.environment_id FOR SHARE;
 IF NOT FOUND THEN RAISE EXCEPTION 'Discord workflow policy missing' USING ERRCODE='55000'; END IF;
 IF enabled AND (OLD.lease_token IS NULL OR current_setting('swallow.discord_workflow_fence',true) IS DISTINCT FROM OLD.id::text||':'||OLD.lease_token::text) THEN
  RAISE EXCEPTION 'Discord workflow requires a compatible enabled worker' USING ERRCODE='55000';
 END IF;
 RETURN NEW;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_discord_workflow_fence() FROM PUBLIC;
CREATE TRIGGER novu_rust_discord_workflow_fence_trigger BEFORE UPDATE OF status ON public.novu_queue_jobs
 FOR EACH ROW EXECUTE FUNCTION public.novu_rust_discord_workflow_fence();
