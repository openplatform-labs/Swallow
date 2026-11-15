-- The unknown marker commits before a cancellation is attempted. Replaying a
-- request never authorizes another provider call.
CREATE TABLE public.novu_rust_resend_cancel_operators (
 environment_id text NOT NULL REFERENCES public.novu_rust_environments(id), database_principal name NOT NULL,
 principal_oid oid NOT NULL, can_cancel boolean NOT NULL DEFAULT false, can_read boolean NOT NULL DEFAULT false,
 PRIMARY KEY(environment_id,database_principal)
);
CREATE TABLE public.novu_rust_resend_cancel_requests (
 environment_id text NOT NULL REFERENCES public.novu_rust_environments(id), request_id uuid NOT NULL,
 job_id uuid NOT NULL REFERENCES public.novu_queue_jobs(id), provider_id text NOT NULL,
 database_principal name NOT NULL, principal_oid oid NOT NULL, reason text NOT NULL,
 state text NOT NULL CHECK(state IN ('unknown','confirmed','rejected')), http_status integer,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(), updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(environment_id,request_id)
);
CREATE UNIQUE INDEX novu_rust_resend_cancel_live_job ON public.novu_rust_resend_cancel_requests(environment_id,job_id) WHERE state IN ('unknown','confirmed');
REVOKE ALL ON public.novu_rust_resend_cancel_operators,public.novu_rust_resend_cancel_requests FROM PUBLIC;
CREATE FUNCTION public.novu_rust_resend_cancel_access(p_env text,p_write boolean) RETURNS oid
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_oid oid;
BEGIN
 SELECT r.oid INTO v_oid FROM public.novu_rust_resend_cancel_operators g JOIN pg_catalog.pg_roles r ON r.rolname=g.database_principal AND r.oid=g.principal_oid AND r.rolcanlogin
 WHERE g.environment_id=p_env AND g.database_principal=session_user AND CASE WHEN p_write THEN g.can_cancel ELSE g.can_read END FOR SHARE OF g;
 IF NOT FOUND THEN RAISE EXCEPTION 'Resend cancellation denied' USING ERRCODE='42501'; END IF;
 RETURN v_oid;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_resend_cancel_access(text,boolean) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_resend_cancel_reserve(p_env text,p_request uuid,p_job uuid,p_reason text) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_oid oid; v_old public.novu_rust_resend_cancel_requests%ROWTYPE; v_provider text; v_inserted boolean;
BEGIN
 IF p_request IS NULL OR p_job IS NULL OR p_reason IS NULL OR length(btrim(p_reason))=0 OR octet_length(p_reason)>500 OR p_reason ~ '[[:cntrl:]]' THEN RAISE EXCEPTION 'invalid cancellation request' USING ERRCODE='22023'; END IF;
 v_oid:=public.novu_rust_resend_cancel_access(p_env,true);
 -- Serializes competing operations using the existing queue job lock.
 PERFORM 1 FROM public.novu_queue_jobs WHERE environment_id=p_env AND id=p_job FOR UPDATE;
 SELECT * INTO v_old FROM public.novu_rust_resend_cancel_requests WHERE environment_id=p_env AND request_id=p_request;
 IF FOUND THEN
  IF (v_old.job_id,v_old.reason,v_old.database_principal,v_old.principal_oid) IS DISTINCT FROM (p_job,p_reason,session_user,v_oid) THEN RAISE EXCEPTION 'request conflict' USING ERRCODE='23505'; END IF;
  RETURN jsonb_build_object('requestId',p_request,'state',v_old.state,'fresh',false);
 END IF;
 SELECT a.provider_message_id INTO v_provider FROM public.novu_queue_jobs j JOIN public.novu_rust_resend_attempts a ON a.environment_id=j.environment_id AND a.job_id=j.id
 WHERE j.environment_id=p_env AND j.id=p_job AND j.kind='experimental_resend_email' AND j.status='completed' AND j.resend_scheduled_at>clock_timestamp()
 AND (j.resend_delivery_result IS NULL OR j.resend_delivery_result='pending') AND a.state='accepted'
 AND a.provider_message_id ~ '^[A-Za-z0-9_-]+$' AND octet_length(a.provider_message_id)<=1024
 AND NOT EXISTS(SELECT 1 FROM public.novu_rust_resend_attempts b WHERE b.environment_id=a.environment_id AND b.provider_message_id=a.provider_message_id AND b.job_id<>a.job_id);
 IF NOT FOUND THEN RAISE EXCEPTION 'scheduled cancellation not eligible' USING ERRCODE='55000'; END IF;
 INSERT INTO public.novu_rust_resend_cancel_requests(environment_id,request_id,job_id,provider_id,database_principal,principal_oid,reason,state)
 VALUES(p_env,p_request,p_job,v_provider,session_user,v_oid,p_reason,'unknown') ON CONFLICT DO NOTHING RETURNING true INTO v_inserted;
 IF NOT coalesce(v_inserted,false) THEN RAISE EXCEPTION 'cancellation request or job conflict' USING ERRCODE='23505'; END IF;
 RETURN jsonb_build_object('requestId',p_request,'state','unknown','fresh',true,'providerId',v_provider);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_resend_cancel_reserve(text,uuid,uuid,text) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_resend_cancel_finish(p_env text,p_request uuid,p_state text,p_status integer) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_oid oid; v_row public.novu_rust_resend_cancel_requests%ROWTYPE;
BEGIN
 IF p_state IS NULL OR p_state NOT IN ('unknown','confirmed','rejected') OR (p_state='rejected' AND (p_status IS NULL OR p_status NOT BETWEEN 400 AND 499)) OR (p_state<>'rejected' AND p_status IS NOT NULL) THEN RAISE EXCEPTION 'invalid cancellation result' USING ERRCODE='22023'; END IF;
 v_oid:=public.novu_rust_resend_cancel_access(p_env,true);
 SELECT * INTO v_row FROM public.novu_rust_resend_cancel_requests WHERE environment_id=p_env AND request_id=p_request FOR UPDATE;
 IF NOT FOUND OR v_row.database_principal<>session_user OR v_row.principal_oid<>v_oid THEN RAISE EXCEPTION 'cancellation request unavailable' USING ERRCODE='42501'; END IF;
 IF v_row.state<>'unknown' THEN
  IF (v_row.state,v_row.http_status) IS DISTINCT FROM (p_state,p_status) THEN RAISE EXCEPTION 'cancellation result conflict' USING ERRCODE='23505'; END IF;
 ELSE
  UPDATE public.novu_rust_resend_cancel_requests SET state=p_state,http_status=p_status,updated_at=clock_timestamp() WHERE environment_id=p_env AND request_id=p_request;
 END IF;
 RETURN jsonb_build_object('requestId',p_request,'state',p_state,'httpStatus',p_status);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_resend_cancel_finish(text,uuid,text,integer) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_resend_cancel_get(p_env text,p_request uuid) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_result jsonb;
BEGIN
 PERFORM public.novu_rust_resend_cancel_access(p_env,false);
 SELECT jsonb_build_object('requestId',request_id,'jobId',job_id,'providerId',provider_id,'databasePrincipal',database_principal,'reason',reason,'state',state,'httpStatus',http_status,'createdAt',created_at::text,'updatedAt',updated_at::text) INTO v_result FROM public.novu_rust_resend_cancel_requests WHERE environment_id=p_env AND request_id=p_request;
 RETURN v_result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_resend_cancel_get(text,uuid) FROM PUBLIC;
