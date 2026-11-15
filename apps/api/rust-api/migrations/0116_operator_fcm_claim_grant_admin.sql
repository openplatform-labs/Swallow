-- Extend existing FCM review administrators to unrecorded-claim health and list.
CREATE OR REPLACE FUNCTION public.novu_rust_operator_jwt_fcm_review_grant_input_valid(p_issuer text,p_subject text,p_client text,p_resource text,p_permission text)
RETURNS boolean LANGUAGE sql IMMUTABLE SET search_path=pg_catalog AS $$
 SELECT COALESCE(octet_length(p_issuer) BETWEEN 1 AND 2048 AND octet_length(p_subject) BETWEEN 1 AND 1024 AND octet_length(p_client) BETWEEN 1 AND 1024 AND (p_resource='fcm-partial' AND p_permission IN ('health','recent-list','unreviewed','review','cooldown-health','claim-health','claim-list')),false)
$$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_fcm_review_grant_input_valid(text,text,text,text,text) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.novu_rust_operator_jwt_fcm_review_grant_read(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_target_issuer text,p_target_subject text,p_target_client text,p_resource text,p_permission text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_exists boolean; v_granted boolean;
BEGIN
 IF NOT public.novu_rust_operator_jwt_fcm_review_grant_input_valid(p_target_issuer,p_target_subject,p_target_client,p_resource,p_permission) THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_fcm_review_grant_admin_access(p_issuer,p_subject,p_client,p_env,p_exp,false);
 PERFORM 1 FROM public.novu_rust_operator_jwt_identities i WHERE i.issuer=p_target_issuer AND i.subject=p_target_subject AND i.client_id=p_target_client AND i.environment_id=p_env FOR SHARE OF i;
 v_exists:=FOUND;
 SELECT CASE p_permission WHEN 'health' THEN EXISTS(SELECT 1 FROM public.novu_rust_operator_jwt_fcm_partial_health_grants WHERE issuer=p_target_issuer AND subject=p_target_subject AND client_id=p_target_client AND environment_id=p_env) WHEN 'recent-list' THEN EXISTS(SELECT 1 FROM public.novu_rust_operator_jwt_fcm_partial_jobs_grants WHERE issuer=p_target_issuer AND subject=p_target_subject AND client_id=p_target_client AND environment_id=p_env) WHEN 'unreviewed' THEN EXISTS(SELECT 1 FROM public.novu_rust_operator_jwt_fcm_unreviewed_grants WHERE issuer=p_target_issuer AND subject=p_target_subject AND client_id=p_target_client AND environment_id=p_env) WHEN 'review' THEN EXISTS(SELECT 1 FROM public.novu_rust_operator_jwt_fcm_review_grants WHERE issuer=p_target_issuer AND subject=p_target_subject AND client_id=p_target_client AND environment_id=p_env) WHEN 'cooldown-health' THEN EXISTS(SELECT 1 FROM public.novu_rust_operator_jwt_fcm_cooldown_health_grants WHERE issuer=p_target_issuer AND subject=p_target_subject AND client_id=p_target_client AND environment_id=p_env) WHEN 'claim-health' THEN COALESCE((SELECT can_health FROM public.novu_rust_operator_jwt_fcm_unrecorded_claim_grants WHERE issuer=p_target_issuer AND subject=p_target_subject AND client_id=p_target_client AND environment_id=p_env),false) WHEN 'claim-list' THEN COALESCE((SELECT can_list FROM public.novu_rust_operator_jwt_fcm_unrecorded_claim_grants WHERE issuer=p_target_issuer AND subject=p_target_subject AND client_id=p_target_client AND environment_id=p_env),false) END INTO v_granted;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN jsonb_build_object('targetExists',v_exists,'granted',v_granted);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_fcm_review_grant_read(text,text,text,text,bigint,text,text,text,text,text) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.novu_rust_operator_jwt_fcm_review_grant_change(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_request uuid,p_target_issuer text,p_target_subject text,p_target_client text,p_resource text,p_permission text,p_expected boolean,p_desired boolean,p_reason text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE r public.novu_rust_operator_jwt_fcm_review_grant_requests%ROWTYPE; v_current boolean; v_changed boolean;
BEGIN
 IF p_request IS NULL OR NOT public.novu_rust_operator_jwt_fcm_review_grant_input_valid(p_target_issuer,p_target_subject,p_target_client,p_resource,p_permission) OR p_expected IS NULL OR p_desired IS NULL OR p_reason IS NULL OR octet_length(btrim(p_reason)) NOT BETWEEN 1 AND 1000 THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_fcm_review_grant_admin_access(p_issuer,p_subject,p_client,p_env,p_exp,true);
 IF (p_issuer,p_subject)=(p_target_issuer,p_target_subject) THEN RAISE EXCEPTION 'self change denied' USING ERRCODE='42501'; END IF;
 PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(pg_catalog.jsonb_build_array('jwt-fcm-partial-grant-request',p_issuer,p_subject,p_client,p_env,p_request)::text,0));
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 SELECT * INTO r FROM public.novu_rust_operator_jwt_fcm_review_grant_requests WHERE issuer=p_issuer AND subject=p_subject AND client_id=p_client AND environment_id=p_env AND request_id=p_request;
 IF FOUND THEN
  IF (r.target_issuer,r.target_subject,r.target_client,r.resource,r.permission,r.expected,r.desired,r.reason) IS DISTINCT FROM (p_target_issuer,p_target_subject,p_target_client,p_resource,p_permission,p_expected,p_desired,p_reason) THEN RAISE EXCEPTION 'request conflict' USING ERRCODE='P4090'; END IF;
  IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
  RETURN jsonb_build_object('changed',r.changed,'replayed',true,'requestId',p_request);
 END IF;
 PERFORM 1 FROM public.novu_rust_operator_jwt_identities i WHERE i.issuer=p_target_issuer AND i.subject=p_target_subject AND i.client_id=p_target_client AND i.environment_id=p_env FOR UPDATE OF i;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 IF NOT FOUND THEN RAISE EXCEPTION 'target missing' USING ERRCODE='22023'; END IF;
 SELECT CASE p_permission WHEN 'health' THEN EXISTS(SELECT 1 FROM public.novu_rust_operator_jwt_fcm_partial_health_grants WHERE issuer=p_target_issuer AND subject=p_target_subject AND client_id=p_target_client AND environment_id=p_env) WHEN 'recent-list' THEN EXISTS(SELECT 1 FROM public.novu_rust_operator_jwt_fcm_partial_jobs_grants WHERE issuer=p_target_issuer AND subject=p_target_subject AND client_id=p_target_client AND environment_id=p_env) WHEN 'unreviewed' THEN EXISTS(SELECT 1 FROM public.novu_rust_operator_jwt_fcm_unreviewed_grants WHERE issuer=p_target_issuer AND subject=p_target_subject AND client_id=p_target_client AND environment_id=p_env) WHEN 'review' THEN EXISTS(SELECT 1 FROM public.novu_rust_operator_jwt_fcm_review_grants WHERE issuer=p_target_issuer AND subject=p_target_subject AND client_id=p_target_client AND environment_id=p_env) WHEN 'cooldown-health' THEN EXISTS(SELECT 1 FROM public.novu_rust_operator_jwt_fcm_cooldown_health_grants WHERE issuer=p_target_issuer AND subject=p_target_subject AND client_id=p_target_client AND environment_id=p_env) WHEN 'claim-health' THEN COALESCE((SELECT can_health FROM public.novu_rust_operator_jwt_fcm_unrecorded_claim_grants WHERE issuer=p_target_issuer AND subject=p_target_subject AND client_id=p_target_client AND environment_id=p_env),false) WHEN 'claim-list' THEN COALESCE((SELECT can_list FROM public.novu_rust_operator_jwt_fcm_unrecorded_claim_grants WHERE issuer=p_target_issuer AND subject=p_target_subject AND client_id=p_target_client AND environment_id=p_env),false) END INTO v_current;
 IF v_current IS DISTINCT FROM p_expected THEN RAISE EXCEPTION 'stale grant' USING ERRCODE='P4090'; END IF;
 v_changed:=v_current IS DISTINCT FROM p_desired;
 IF v_changed THEN
  IF p_desired THEN
   IF p_permission='health' THEN INSERT INTO public.novu_rust_operator_jwt_fcm_partial_health_grants(issuer,subject,client_id,environment_id) VALUES(p_target_issuer,p_target_subject,p_target_client,p_env);
   ELSIF p_permission='recent-list' THEN INSERT INTO public.novu_rust_operator_jwt_fcm_partial_jobs_grants(issuer,subject,client_id,environment_id) VALUES(p_target_issuer,p_target_subject,p_target_client,p_env);
   ELSIF p_permission='unreviewed' THEN INSERT INTO public.novu_rust_operator_jwt_fcm_unreviewed_grants(issuer,subject,client_id,environment_id) VALUES(p_target_issuer,p_target_subject,p_target_client,p_env);
   ELSIF p_permission='review' THEN INSERT INTO public.novu_rust_operator_jwt_fcm_review_grants(issuer,subject,client_id,environment_id) VALUES(p_target_issuer,p_target_subject,p_target_client,p_env);
   ELSIF p_permission='cooldown-health' THEN INSERT INTO public.novu_rust_operator_jwt_fcm_cooldown_health_grants(issuer,subject,client_id,environment_id) VALUES(p_target_issuer,p_target_subject,p_target_client,p_env);
   ELSIF p_permission IN ('claim-health','claim-list') THEN INSERT INTO public.novu_rust_operator_jwt_fcm_unrecorded_claim_grants(issuer,subject,client_id,environment_id,can_health,can_list) VALUES(p_target_issuer,p_target_subject,p_target_client,p_env,p_permission='claim-health',p_permission='claim-list') ON CONFLICT(issuer,subject,client_id,environment_id) DO UPDATE SET can_health=public.novu_rust_operator_jwt_fcm_unrecorded_claim_grants.can_health OR EXCLUDED.can_health,can_list=public.novu_rust_operator_jwt_fcm_unrecorded_claim_grants.can_list OR EXCLUDED.can_list;
   END IF;
  ELSE
   IF p_permission='health' THEN DELETE FROM public.novu_rust_operator_jwt_fcm_partial_health_grants WHERE issuer=p_target_issuer AND subject=p_target_subject AND client_id=p_target_client AND environment_id=p_env;
   ELSIF p_permission='recent-list' THEN DELETE FROM public.novu_rust_operator_jwt_fcm_partial_jobs_grants WHERE issuer=p_target_issuer AND subject=p_target_subject AND client_id=p_target_client AND environment_id=p_env;
   ELSIF p_permission='unreviewed' THEN DELETE FROM public.novu_rust_operator_jwt_fcm_unreviewed_grants WHERE issuer=p_target_issuer AND subject=p_target_subject AND client_id=p_target_client AND environment_id=p_env;
   ELSIF p_permission='review' THEN DELETE FROM public.novu_rust_operator_jwt_fcm_review_grants WHERE issuer=p_target_issuer AND subject=p_target_subject AND client_id=p_target_client AND environment_id=p_env;
   ELSIF p_permission='cooldown-health' THEN DELETE FROM public.novu_rust_operator_jwt_fcm_cooldown_health_grants WHERE issuer=p_target_issuer AND subject=p_target_subject AND client_id=p_target_client AND environment_id=p_env;
   ELSIF p_permission IN ('claim-health','claim-list') THEN UPDATE public.novu_rust_operator_jwt_fcm_unrecorded_claim_grants SET can_health=CASE WHEN p_permission='claim-health' THEN false ELSE can_health END,can_list=CASE WHEN p_permission='claim-list' THEN false ELSE can_list END WHERE issuer=p_target_issuer AND subject=p_target_subject AND client_id=p_target_client AND environment_id=p_env;
   END IF;
  END IF;
  INSERT INTO public.novu_rust_operator_jwt_fcm_review_grant_audit(environment_id,actor_issuer,actor_subject,actor_client,database_principal,request_id,target_issuer,target_subject,target_client,resource,permission,previous,current,reason) VALUES(p_env,p_issuer,p_subject,p_client,session_user,p_request,p_target_issuer,p_target_subject,p_target_client,p_resource,p_permission,p_expected,p_desired,p_reason);
 END IF;
 INSERT INTO public.novu_rust_operator_jwt_fcm_review_grant_requests(issuer,subject,client_id,environment_id,request_id,target_issuer,target_subject,target_client,resource,permission,expected,desired,reason,changed) VALUES(p_issuer,p_subject,p_client,p_env,p_request,p_target_issuer,p_target_subject,p_target_client,p_resource,p_permission,p_expected,p_desired,p_reason,v_changed);
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN jsonb_build_object('changed',v_changed,'replayed',false,'requestId',p_request);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_fcm_review_grant_change(text,text,text,text,bigint,uuid,text,text,text,text,text,boolean,boolean,text) FROM PUBLIC;
