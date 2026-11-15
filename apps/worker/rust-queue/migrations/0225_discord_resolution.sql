-- Dedicated, environment-scoped decision grants; trusted DBAs provision logins.
CREATE TABLE public.novu_rust_discord_resolvers (
 environment_id text NOT NULL REFERENCES public.novu_rust_environments(id),database_principal name NOT NULL,principal_oid oid NOT NULL,
 can_accept boolean NOT NULL DEFAULT false,can_retry boolean NOT NULL DEFAULT false,
 PRIMARY KEY(environment_id,database_principal)
);
CREATE TABLE public.novu_rust_discord_resolutions (
 environment_id text NOT NULL,actor name NOT NULL,actor_oid oid NOT NULL,request_id uuid NOT NULL,
 job_id uuid NOT NULL,attempt_revision uuid,failure_revision bigint NOT NULL,decision text NOT NULL,reason text NOT NULL,
 attempt_state text,http_status integer,retry_after_seconds integer,created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(environment_id,actor_oid,request_id)
);
REVOKE ALL ON public.novu_rust_discord_resolvers,public.novu_rust_discord_resolutions FROM PUBLIC;
CREATE FUNCTION public.novu_rust_resolve_discord(p_env text,p_request uuid,p_job uuid,p_attempt_revision uuid,p_failure_revision bigint,p_decision text,p_reason text)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_actor oid;v_old public.novu_rust_discord_resolutions%ROWTYPE;v_job public.novu_queue_jobs%ROWTYPE;v_attempt public.novu_rust_discord_attempts%ROWTYPE;v_inserted boolean;v_has_attempt boolean;
BEGIN
 IF p_env IS NULL OR length(btrim(p_env))=0 OR p_request IS NULL OR p_job IS NULL OR p_failure_revision IS NULL OR p_failure_revision<0 OR p_decision IS NULL OR p_decision NOT IN ('accepted','retry') OR (p_attempt_revision IS NULL AND p_decision<>'retry') OR p_reason IS NULL OR octet_length(btrim(p_reason)) NOT BETWEEN 1 AND 512 THEN RAISE EXCEPTION 'invalid Discord resolution' USING ERRCODE='22023'; END IF;
 SELECT r.oid INTO v_actor FROM public.novu_rust_discord_resolvers g JOIN pg_catalog.pg_roles r ON r.oid=g.principal_oid AND r.rolname=g.database_principal AND r.rolcanlogin
 WHERE g.environment_id=p_env AND g.database_principal=session_user AND CASE WHEN p_decision='accepted' THEN g.can_accept ELSE g.can_retry END FOR SHARE OF g;
 IF NOT FOUND THEN RAISE EXCEPTION 'Discord resolution permission denied' USING ERRCODE='42501'; END IF;
 INSERT INTO public.novu_rust_discord_resolutions(environment_id,actor,actor_oid,request_id,job_id,attempt_revision,failure_revision,decision,reason)
 VALUES(p_env,session_user,v_actor,p_request,p_job,p_attempt_revision,p_failure_revision,p_decision,p_reason) ON CONFLICT DO NOTHING;
 v_inserted:=FOUND;
 IF NOT v_inserted THEN
  SELECT * INTO v_old FROM public.novu_rust_discord_resolutions WHERE environment_id=p_env AND actor_oid=v_actor AND request_id=p_request;
  IF (v_old.actor,v_old.job_id,v_old.attempt_revision,v_old.failure_revision,v_old.decision,v_old.reason) IS DISTINCT FROM (session_user,p_job,p_attempt_revision,p_failure_revision,p_decision,p_reason) THEN RAISE EXCEPTION 'Discord request conflict' USING ERRCODE='22023'; END IF;
  RETURN false;
 END IF;
 SELECT * INTO v_job FROM public.novu_queue_jobs WHERE environment_id=p_env AND id=p_job AND kind IN ('experimental_discord_webhook','experimental_discord_workflow') FOR UPDATE;
 IF NOT FOUND OR v_job.status<>'failed' OR v_job.failure_revision<>p_failure_revision THEN RAISE EXCEPTION 'Discord job changed or unavailable' USING ERRCODE='22023'; END IF;
 SELECT * INTO v_attempt FROM public.novu_rust_discord_attempts WHERE environment_id=p_env AND job_id=p_job FOR UPDATE;v_has_attempt:=FOUND;
 IF (p_attempt_revision IS NULL AND v_has_attempt) OR (p_attempt_revision IS NOT NULL AND (NOT v_has_attempt OR v_attempt.revision IS DISTINCT FROM p_attempt_revision OR v_attempt.state NOT IN ('unknown','rejected'))) OR (p_decision='accepted' AND v_attempt.state IS DISTINCT FROM 'unknown') THEN RAISE EXCEPTION 'Discord attempt changed or unavailable' USING ERRCODE='22023'; END IF;
 UPDATE public.novu_rust_discord_resolutions SET attempt_state=v_attempt.state,http_status=v_attempt.http_status,retry_after_seconds=v_attempt.retry_after_seconds WHERE environment_id=p_env AND actor_oid=v_actor AND request_id=p_request;
 IF p_decision='accepted' THEN
  UPDATE public.novu_rust_discord_attempts SET state='accepted',updated_at=clock_timestamp() WHERE job_id=p_job AND environment_id=p_env;
  UPDATE public.novu_queue_jobs SET status='completed',last_error=NULL,lease_token=NULL,lease_until=NULL WHERE environment_id=p_env AND id=p_job;
 ELSE
  DELETE FROM public.novu_rust_discord_attempts WHERE job_id=p_job AND environment_id=p_env;
  UPDATE public.novu_queue_jobs SET status='pending',available_at=clock_timestamp(),last_error=NULL,lease_token=NULL,lease_until=NULL WHERE environment_id=p_env AND id=p_job;
 END IF;
 RETURN true;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_resolve_discord(text,uuid,uuid,uuid,bigint,text,text) FROM PUBLIC;
