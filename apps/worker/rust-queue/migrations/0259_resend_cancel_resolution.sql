-- Explicit, independently authorized operator assessment of an unknown result.
ALTER TABLE public.novu_rust_resend_cancel_operators ADD COLUMN can_resolve boolean NOT NULL DEFAULT false;
ALTER TABLE public.novu_rust_resend_cancel_requests DROP CONSTRAINT novu_rust_resend_cancel_requests_state_check;
ALTER TABLE public.novu_rust_resend_cancel_requests ADD CONSTRAINT novu_rust_resend_cancel_requests_state_check CHECK(state IN ('unknown','confirmed','rejected','not_cancelled'));
CREATE TABLE public.novu_rust_resend_cancel_resolutions (
 environment_id text NOT NULL, resolution_id uuid NOT NULL, request_id uuid NOT NULL,
 database_principal name NOT NULL, principal_oid oid NOT NULL,
 decision text NOT NULL CHECK(decision IN ('confirmed','not_cancelled')), reason text NOT NULL,
 recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(environment_id,resolution_id), UNIQUE(environment_id,request_id),
 FOREIGN KEY(environment_id,request_id) REFERENCES public.novu_rust_resend_cancel_requests(environment_id,request_id)
);
REVOKE ALL ON public.novu_rust_resend_cancel_resolutions FROM PUBLIC;
CREATE FUNCTION public.novu_rust_resend_cancel_resolve(p_env text,p_resolution uuid,p_request uuid,p_decision text,p_reason text) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_oid oid; v_old public.novu_rust_resend_cancel_resolutions%ROWTYPE; v_state text;
BEGIN
 IF p_resolution IS NULL OR p_request IS NULL OR p_decision IS NULL OR p_decision NOT IN ('confirmed','not_cancelled') OR p_reason IS NULL OR length(btrim(p_reason))=0 OR octet_length(p_reason)>500 OR p_reason ~ '[[:cntrl:]]' THEN RAISE EXCEPTION 'invalid cancellation resolution' USING ERRCODE='22023'; END IF;
 SELECT r.oid INTO v_oid FROM public.novu_rust_resend_cancel_operators g JOIN pg_catalog.pg_roles r ON r.oid=g.principal_oid AND r.rolname=g.database_principal AND r.rolcanlogin
 WHERE g.environment_id=p_env AND g.database_principal=session_user AND g.can_resolve FOR SHARE OF g;
 IF NOT FOUND THEN RAISE EXCEPTION 'cancellation resolution denied' USING ERRCODE='42501'; END IF;
 SELECT state INTO v_state FROM public.novu_rust_resend_cancel_requests WHERE environment_id=p_env AND request_id=p_request FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'cancellation request missing' USING ERRCODE='55000'; END IF;
 SELECT * INTO v_old FROM public.novu_rust_resend_cancel_resolutions WHERE environment_id=p_env AND resolution_id=p_resolution;
 IF FOUND THEN
  IF (v_old.request_id,v_old.decision,v_old.reason,v_old.database_principal,v_old.principal_oid) IS DISTINCT FROM (p_request,p_decision,p_reason,session_user,v_oid) THEN RAISE EXCEPTION 'resolution conflict' USING ERRCODE='23505'; END IF;
  RETURN jsonb_build_object('resolutionId',p_resolution,'requestId',p_request,'decision',v_old.decision,'replayed',true);
 END IF;
 IF v_state<>'unknown' THEN RAISE EXCEPTION 'cancellation is no longer unknown' USING ERRCODE='55000'; END IF;
 INSERT INTO public.novu_rust_resend_cancel_resolutions(environment_id,resolution_id,request_id,database_principal,principal_oid,decision,reason) VALUES(p_env,p_resolution,p_request,session_user,v_oid,p_decision,p_reason);
 UPDATE public.novu_rust_resend_cancel_requests SET state=p_decision,http_status=NULL,updated_at=clock_timestamp() WHERE environment_id=p_env AND request_id=p_request;
 RETURN jsonb_build_object('resolutionId',p_resolution,'requestId',p_request,'decision',p_decision,'replayed',false);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_resend_cancel_resolve(text,uuid,uuid,text,text) FROM PUBLIC;
-- A delayed network response may not override an operator assessment.
CREATE OR REPLACE FUNCTION public.novu_rust_resend_cancel_finish(p_env text,p_request uuid,p_state text,p_status integer) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_oid oid; v_row public.novu_rust_resend_cancel_requests%ROWTYPE;
BEGIN
 IF p_state IS NULL OR p_state NOT IN ('unknown','confirmed','rejected') OR (p_state='rejected' AND (p_status IS NULL OR p_status NOT BETWEEN 400 AND 499)) OR (p_state<>'rejected' AND p_status IS NOT NULL) THEN RAISE EXCEPTION 'invalid cancellation result' USING ERRCODE='22023'; END IF;
 v_oid:=public.novu_rust_resend_cancel_access(p_env,true);
 SELECT * INTO v_row FROM public.novu_rust_resend_cancel_requests WHERE environment_id=p_env AND request_id=p_request FOR UPDATE;
 IF NOT FOUND OR v_row.database_principal<>session_user OR v_row.principal_oid<>v_oid THEN RAISE EXCEPTION 'cancellation request unavailable' USING ERRCODE='42501'; END IF;
 IF EXISTS(SELECT 1 FROM public.novu_rust_resend_cancel_resolutions WHERE environment_id=p_env AND request_id=p_request) THEN RAISE EXCEPTION 'cancellation already resolved by operator' USING ERRCODE='55000'; END IF;
 IF v_row.state<>'unknown' THEN
  IF (v_row.state,v_row.http_status) IS DISTINCT FROM (p_state,p_status) THEN RAISE EXCEPTION 'cancellation result conflict' USING ERRCODE='23505'; END IF;
 ELSE
  UPDATE public.novu_rust_resend_cancel_requests SET state=p_state,http_status=p_status,updated_at=clock_timestamp() WHERE environment_id=p_env AND request_id=p_request;
 END IF;
 RETURN jsonb_build_object('requestId',p_request,'state',p_state,'httpStatus',p_status);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_resend_cancel_finish(text,uuid,text,integer) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_resend_cancel_resolution_get(p_env text,p_resolution uuid) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_result jsonb;
BEGIN
 PERFORM public.novu_rust_resend_cancel_access(p_env,false);
 SELECT jsonb_build_object('resolutionId',resolution_id,'requestId',request_id,'decision',decision,'reason',reason,'databasePrincipal',database_principal,'recordedAt',recorded_at::text) INTO v_result FROM public.novu_rust_resend_cancel_resolutions WHERE environment_id=p_env AND resolution_id=p_resolution;
 RETURN v_result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_resend_cancel_resolution_get(text,uuid) FROM PUBLIC;
