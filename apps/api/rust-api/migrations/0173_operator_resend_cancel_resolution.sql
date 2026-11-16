-- Independently grant each manual assessment. JWT attribution commits with the worker decision.
CREATE TABLE public.novu_rust_operator_jwt_resend_cancel_resolution_grants (
 issuer text NOT NULL,subject text NOT NULL,client_id text NOT NULL,environment_id text NOT NULL,decision text NOT NULL CHECK(decision IN ('confirmed','not_cancelled')),
 PRIMARY KEY(issuer,subject,client_id,environment_id,decision),
 FOREIGN KEY(issuer,subject,client_id,environment_id) REFERENCES public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) ON DELETE CASCADE
);
CREATE TABLE public.novu_rust_operator_jwt_resend_cancel_resolution_audit (
 environment_id text NOT NULL,issuer text NOT NULL,subject text NOT NULL,client_id text NOT NULL,jwt_resolution_id uuid NOT NULL,
 database_resolution_id uuid NOT NULL,request_id uuid NOT NULL,decision text NOT NULL,reason text NOT NULL,
 recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(environment_id,issuer,subject,client_id,jwt_resolution_id),UNIQUE(environment_id,database_resolution_id),
 FOREIGN KEY(environment_id,database_resolution_id) REFERENCES public.novu_rust_resend_cancel_resolutions(environment_id,resolution_id)
);
REVOKE ALL ON public.novu_rust_operator_jwt_resend_cancel_resolution_grants,public.novu_rust_operator_jwt_resend_cancel_resolution_audit FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_resend_cancel_resolve(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_resolution uuid,p_request uuid,p_decision text,p_reason text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_old public.novu_rust_operator_jwt_resend_cancel_resolution_audit%ROWTYPE;v_db_id uuid;
BEGIN
 IF p_resolution IS NULL OR p_request IS NULL OR p_decision IS NULL OR p_decision NOT IN ('confirmed','not_cancelled') OR p_reason IS NULL OR length(btrim(p_reason))=0 OR octet_length(p_reason)>500 OR p_reason ~ '[[:cntrl:]]' THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 PERFORM 1 FROM public.novu_rust_operator_jwt_resend_cancel_resolution_grants g WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env AND g.decision=p_decision FOR SHARE OF g;
 IF NOT FOUND OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 PERFORM 1 FROM public.novu_rust_resend_cancel_operators g JOIN pg_catalog.pg_roles r ON r.oid=g.principal_oid AND r.rolname=g.database_principal AND r.rolcanlogin WHERE g.environment_id=p_env AND g.database_principal=session_user AND g.can_resolve FOR SHARE OF g;
 IF NOT FOUND OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(pg_catalog.jsonb_build_array('resend-cancel-jwt',p_env,p_issuer,p_subject,p_client,p_resolution)::text,0));
 SELECT * INTO v_old FROM public.novu_rust_operator_jwt_resend_cancel_resolution_audit WHERE environment_id=p_env AND issuer=p_issuer AND subject=p_subject AND client_id=p_client AND jwt_resolution_id=p_resolution;
 IF FOUND THEN
  IF (v_old.request_id,v_old.decision,v_old.reason) IS DISTINCT FROM (p_request,p_decision,p_reason) THEN RAISE EXCEPTION 'resolution conflict' USING ERRCODE='P4090'; END IF;
  IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
  RETURN jsonb_build_object('resolutionId',p_resolution,'requestId',p_request,'decision',p_decision,'replayed',true);
 END IF;
 v_db_id:=substr(encode(sha256(convert_to(jsonb_build_array('resend-cancel-resolution-v1',p_env,p_issuer,p_subject,p_client,p_resolution)::text,'UTF8')),'hex'),1,32)::uuid;
 PERFORM public.novu_rust_resend_cancel_resolve(p_env,v_db_id,p_request,p_decision,p_reason);
 INSERT INTO public.novu_rust_operator_jwt_resend_cancel_resolution_audit(environment_id,issuer,subject,client_id,jwt_resolution_id,database_resolution_id,request_id,decision,reason) VALUES(p_env,p_issuer,p_subject,p_client,p_resolution,v_db_id,p_request,p_decision,p_reason);
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN jsonb_build_object('resolutionId',p_resolution,'requestId',p_request,'decision',p_decision,'replayed',false);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_resend_cancel_resolve(text,text,text,text,bigint,uuid,uuid,text,text) FROM PUBLIC;
CREATE OR REPLACE FUNCTION public.novu_rust_operator_jwt_resend_cancel_detail(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_request uuid)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_request jsonb;v_resolution jsonb;
BEGIN
 IF p_request IS NULL THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 PERFORM 1 FROM public.novu_rust_operator_jwt_resend_cancel_detail_grants g WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env FOR SHARE OF g;
 IF NOT FOUND OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 v_request:=public.novu_rust_resend_cancel_get(p_env,p_request);
 SELECT jsonb_build_object('resolutionId',r.resolution_id,'decision',r.decision,'databasePrincipal',r.database_principal,'reason',r.reason,'recordedAt',r.recorded_at::text,'operator',CASE WHEN a.database_resolution_id IS NULL THEN NULL ELSE jsonb_build_object('issuer',a.issuer,'subject',a.subject,'clientId',a.client_id,'resolutionId',a.jwt_resolution_id) END) INTO v_resolution FROM public.novu_rust_resend_cancel_resolutions r LEFT JOIN public.novu_rust_operator_jwt_resend_cancel_resolution_audit a ON a.environment_id=r.environment_id AND a.database_resolution_id=r.resolution_id WHERE r.environment_id=p_env AND r.request_id=p_request;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN jsonb_build_object('request',v_request,'resolution',v_resolution);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_resend_cancel_detail(text,text,text,text,bigint,uuid) FROM PUBLIC;
