-- Separate command queue: credentials remain in the environment-specific worker.
CREATE TABLE public.novu_rust_resend_cancel_submitters (
 environment_id text NOT NULL REFERENCES public.novu_rust_environments(id),database_principal name NOT NULL,principal_oid oid NOT NULL,
 PRIMARY KEY(environment_id,database_principal)
);
CREATE TABLE public.novu_rust_resend_cancel_command_workers (
 environment_id text NOT NULL REFERENCES public.novu_rust_environments(id),database_principal name NOT NULL,principal_oid oid NOT NULL,
 PRIMARY KEY(environment_id,database_principal)
);
CREATE TABLE public.novu_rust_resend_cancel_commands (
 environment_id text NOT NULL REFERENCES public.novu_rust_environments(id),request_id uuid NOT NULL,job_id uuid NOT NULL REFERENCES public.novu_queue_jobs(id),
 reason text NOT NULL,submitter name NOT NULL,submitter_oid oid NOT NULL,
 state text NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','dispatching','finished','failed')),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(environment_id,request_id)
);
CREATE UNIQUE INDEX novu_rust_resend_cancel_command_active_job ON public.novu_rust_resend_cancel_commands(environment_id,job_id) WHERE state IN ('pending','dispatching');
CREATE INDEX novu_rust_resend_cancel_command_pending ON public.novu_rust_resend_cancel_commands(environment_id,created_at,request_id) WHERE state='pending';
REVOKE ALL ON public.novu_rust_resend_cancel_submitters,public.novu_rust_resend_cancel_command_workers,public.novu_rust_resend_cancel_commands FROM PUBLIC;
CREATE FUNCTION public.novu_rust_resend_cancel_enqueue(p_env text,p_request uuid,p_job uuid,p_reason text) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_oid oid; v_old public.novu_rust_resend_cancel_commands%ROWTYPE;v_inserted boolean;
BEGIN
 IF p_request IS NULL OR p_job IS NULL OR p_reason IS NULL OR length(btrim(p_reason))=0 OR octet_length(p_reason)>500 OR p_reason ~ '[[:cntrl:]]' THEN RAISE EXCEPTION 'invalid cancellation command' USING ERRCODE='22023'; END IF;
 SELECT r.oid INTO v_oid FROM public.novu_rust_resend_cancel_submitters g JOIN pg_catalog.pg_roles r ON r.oid=g.principal_oid AND r.rolname=g.database_principal AND r.rolcanlogin WHERE g.environment_id=p_env AND g.database_principal=session_user FOR SHARE OF g;
 IF NOT FOUND THEN RAISE EXCEPTION 'cancellation submission denied' USING ERRCODE='42501'; END IF;
 PERFORM 1 FROM public.novu_queue_jobs WHERE environment_id=p_env AND id=p_job;
 IF NOT FOUND THEN RAISE EXCEPTION 'job unavailable' USING ERRCODE='55000'; END IF;
 SELECT * INTO v_old FROM public.novu_rust_resend_cancel_commands WHERE environment_id=p_env AND request_id=p_request;
 IF FOUND THEN
  IF (v_old.job_id,v_old.reason,v_old.submitter,v_old.submitter_oid) IS DISTINCT FROM (p_job,p_reason,session_user,v_oid) THEN RAISE EXCEPTION 'command conflict' USING ERRCODE='23505'; END IF;
  RETURN jsonb_build_object('requestId',p_request,'state',v_old.state,'replayed',true);
 END IF;
 INSERT INTO public.novu_rust_resend_cancel_commands(environment_id,request_id,job_id,reason,submitter,submitter_oid) VALUES(p_env,p_request,p_job,p_reason,session_user,v_oid) ON CONFLICT DO NOTHING RETURNING true INTO v_inserted;
 IF NOT coalesce(v_inserted,false) THEN RAISE EXCEPTION 'command conflict' USING ERRCODE='23505'; END IF;
 RETURN jsonb_build_object('requestId',p_request,'state','pending','replayed',false);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_resend_cancel_enqueue(text,uuid,uuid,text) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_resend_cancel_claim(p_env text) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_cmd public.novu_rust_resend_cancel_commands%ROWTYPE;v_result jsonb;
BEGIN
 PERFORM 1 FROM public.novu_rust_resend_cancel_command_workers g JOIN pg_catalog.pg_roles r ON r.oid=g.principal_oid AND r.rolname=g.database_principal AND r.rolcanlogin WHERE g.environment_id=p_env AND g.database_principal=session_user FOR SHARE OF g;
 IF NOT FOUND THEN RAISE EXCEPTION 'cancellation worker denied' USING ERRCODE='42501'; END IF;
 SELECT * INTO v_cmd FROM public.novu_rust_resend_cancel_commands WHERE environment_id=p_env AND state='pending' ORDER BY created_at,request_id FOR UPDATE SKIP LOCKED LIMIT 1;
 IF NOT FOUND THEN RETURN NULL; END IF;
 BEGIN
  v_result:=public.novu_rust_resend_cancel_reserve(p_env,v_cmd.request_id,v_cmd.job_id,v_cmd.reason);
  IF NOT (v_result->>'fresh')::boolean THEN RAISE EXCEPTION 'already reserved' USING ERRCODE='55000'; END IF;
 EXCEPTION WHEN SQLSTATE '55000' OR SQLSTATE '23505' THEN
  UPDATE public.novu_rust_resend_cancel_commands SET state='failed',updated_at=clock_timestamp() WHERE environment_id=p_env AND request_id=v_cmd.request_id;
  RETURN jsonb_build_object('requestId',v_cmd.request_id,'state','failed');
 END;
 UPDATE public.novu_rust_resend_cancel_commands SET state='dispatching',updated_at=clock_timestamp() WHERE environment_id=p_env AND request_id=v_cmd.request_id;
 RETURN jsonb_build_object('requestId',v_cmd.request_id,'state','dispatching','providerId',v_result->>'providerId');
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_resend_cancel_claim(text) FROM PUBLIC;
-- A confirmed, rejected or manually resolved attempt closes the command; an
-- unknown attempt remains fenced even if the worker died.
CREATE FUNCTION public.novu_rust_resend_cancel_command_finish() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF NEW.state<>'unknown' THEN UPDATE public.novu_rust_resend_cancel_commands SET state='finished',updated_at=clock_timestamp() WHERE environment_id=NEW.environment_id AND request_id=NEW.request_id AND state='dispatching'; END IF;
 RETURN NEW;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_resend_cancel_command_finish() FROM PUBLIC;
CREATE TRIGGER novu_rust_resend_cancel_command_finish AFTER UPDATE OF state ON public.novu_rust_resend_cancel_requests FOR EACH ROW EXECUTE FUNCTION public.novu_rust_resend_cancel_command_finish();
CREATE FUNCTION public.novu_rust_resend_cancel_command_get(p_env text,p_request uuid) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_oid oid;v_result jsonb;
BEGIN
 IF p_request IS NULL THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 SELECT r.oid INTO v_oid FROM public.novu_rust_resend_cancel_submitters g JOIN pg_catalog.pg_roles r ON r.oid=g.principal_oid AND r.rolname=g.database_principal AND r.rolcanlogin WHERE g.environment_id=p_env AND g.database_principal=session_user FOR SHARE OF g;
 IF NOT FOUND THEN RAISE EXCEPTION 'cancellation submission denied' USING ERRCODE='42501'; END IF;
 SELECT jsonb_build_object('requestId',c.request_id,'jobId',c.job_id,'state',c.state,'createdAt',c.created_at::text,'updatedAt',c.updated_at::text,'attemptState',a.state) INTO v_result
 FROM public.novu_rust_resend_cancel_commands c LEFT JOIN public.novu_rust_resend_cancel_requests a ON a.environment_id=c.environment_id AND a.request_id=c.request_id
 WHERE c.environment_id=p_env AND c.request_id=p_request AND c.submitter=session_user AND c.submitter_oid=v_oid;
 RETURN v_result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_resend_cancel_command_get(text,uuid) FROM PUBLIC;
