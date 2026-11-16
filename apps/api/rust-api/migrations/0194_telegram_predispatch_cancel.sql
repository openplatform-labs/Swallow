-- Cancellation is separate from Telegram list and retry permissions.
CREATE TABLE public.novu_rust_operator_jwt_telegram_predispatch_cancellers (
 issuer text NOT NULL,subject text NOT NULL,client_id text NOT NULL,environment_id text NOT NULL,
 PRIMARY KEY(issuer,subject,client_id,environment_id),
 FOREIGN KEY(issuer,subject,client_id,environment_id) REFERENCES public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) ON DELETE CASCADE
);
CREATE TABLE public.novu_rust_operator_jwt_telegram_predispatch_cancellations (
 issuer text NOT NULL,subject text NOT NULL,client_id text NOT NULL,environment_id text NOT NULL REFERENCES public.novu_rust_environments(id) ON DELETE CASCADE,
 request_id uuid NOT NULL,job_id uuid NOT NULL,failure_revision bigint NOT NULL,reason text NOT NULL,worker_request_id uuid NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),PRIMARY KEY(issuer,subject,client_id,environment_id,request_id),UNIQUE(environment_id,worker_request_id)
);
REVOKE ALL ON public.novu_rust_operator_jwt_telegram_predispatch_cancellers,public.novu_rust_operator_jwt_telegram_predispatch_cancellations FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_telegram_predispatch_cancel_access(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 PERFORM public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 PERFORM 1 FROM public.novu_rust_operator_jwt_telegram_predispatch_cancellers g WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env FOR SHARE OF g;
 IF NOT FOUND THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 PERFORM 1 FROM public.novu_rust_telegram_predispatch_cancel_grants g JOIN pg_catalog.pg_roles r ON r.oid=g.principal_oid AND r.rolname=g.database_principal AND r.rolcanlogin WHERE g.environment_id=p_env AND g.database_principal=session_user FOR SHARE OF g;
 IF NOT FOUND OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_telegram_predispatch_cancel_access(text,text,text,text,bigint) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_telegram_predispatch_cancel(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_request uuid,p_job uuid,p_revision bigint,p_reason text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE r public.novu_rust_operator_jwt_telegram_predispatch_cancellations%ROWTYPE; worker_request uuid;
BEGIN
 IF p_request IS NULL OR p_job IS NULL OR p_revision IS NULL OR p_revision<0 OR p_reason IS NULL OR octet_length(btrim(p_reason)) NOT BETWEEN 1 AND 512 THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_telegram_predispatch_cancel_access(p_issuer,p_subject,p_client,p_env,p_exp);
 PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(pg_catalog.jsonb_build_array('jwt-telegram-predispatch-cancel',p_issuer,p_subject,p_client,p_env,p_request)::text,0));
 SELECT * INTO r FROM public.novu_rust_operator_jwt_telegram_predispatch_cancellations WHERE issuer=p_issuer AND subject=p_subject AND client_id=p_client AND environment_id=p_env AND request_id=p_request;
 IF FOUND THEN
  IF (r.job_id,r.failure_revision,r.reason) IS DISTINCT FROM (p_job,p_revision,p_reason) THEN RAISE EXCEPTION 'request conflict' USING ERRCODE='P4090'; END IF;
  IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
  RETURN jsonb_build_object('requestId',p_request,'recorded',true,'replayed',true);
 END IF;
 worker_request:=gen_random_uuid();
 PERFORM public.novu_rust_telegram_predispatch_cancel(p_env,worker_request,p_job,p_revision,p_reason);
 INSERT INTO public.novu_rust_operator_jwt_telegram_predispatch_cancellations(issuer,subject,client_id,environment_id,request_id,job_id,failure_revision,reason,worker_request_id) VALUES(p_issuer,p_subject,p_client,p_env,p_request,p_job,p_revision,p_reason,worker_request);
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN jsonb_build_object('requestId',p_request,'recorded',true,'replayed',false);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_telegram_predispatch_cancel(text,text,text,text,bigint,uuid,uuid,bigint,text) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_telegram_predispatch_cancel_request(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_request uuid)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE r public.novu_rust_operator_jwt_telegram_predispatch_cancellations%ROWTYPE;
BEGIN
 IF p_request IS NULL THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_telegram_predispatch_cancel_access(p_issuer,p_subject,p_client,p_env,p_exp);
 SELECT * INTO r FROM public.novu_rust_operator_jwt_telegram_predispatch_cancellations WHERE issuer=p_issuer AND subject=p_subject AND client_id=p_client AND environment_id=p_env AND request_id=p_request;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 IF NOT FOUND THEN RETURN jsonb_build_object('found',false); END IF;
 RETURN jsonb_build_object('found',true,'requestId',r.request_id,'jobId',r.job_id,'failureRevision',r.failure_revision::text,'reason',r.reason,'recorded',true,'createdAt',r.created_at::text);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_telegram_predispatch_cancel_request(text,text,text,text,bigint,uuid) FROM PUBLIC;
