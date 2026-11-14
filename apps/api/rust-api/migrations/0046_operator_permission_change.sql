-- The DBA must explicitly opt each JWT subject into permission changes.
ALTER TABLE public.novu_rust_operator_jwt_permission_review_grants ADD COLUMN permission_write boolean NOT NULL DEFAULT false;
ALTER TABLE public.novu_rust_operator_jwt_permission_audit ADD COLUMN jwt_issuer text, ADD COLUMN jwt_subject text, ADD COLUMN jwt_client_id text, ADD COLUMN request_id uuid;
CREATE TABLE public.novu_rust_operator_jwt_permission_change_requests (
 issuer text NOT NULL, subject text NOT NULL, client_id text NOT NULL, environment_id text NOT NULL,
 request_id uuid NOT NULL, target_issuer text NOT NULL, target_subject text NOT NULL, target_client text NOT NULL,
 expected jsonb NOT NULL, desired jsonb NOT NULL, reason text NOT NULL, changed boolean NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(issuer,subject,client_id,environment_id,request_id)
);
REVOKE ALL ON public.novu_rust_operator_jwt_permission_change_requests FROM PUBLIC;

CREATE FUNCTION public.novu_rust_operator_jwt_permission_write_access(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 PERFORM public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 PERFORM 1 FROM public.novu_rust_operator_jwt_permission_review_grants g WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env AND g.permission_write FOR SHARE OF g;
 IF NOT FOUND OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_permission_write_access(text,text,text,text,bigint) FROM PUBLIC;

CREATE FUNCTION public.novu_rust_operator_jwt_change_permissions(
 p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_request uuid,
 p_target_issuer text,p_target_subject text,p_target_client text,p_expected jsonb,p_desired jsonb,p_reason text
) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_existing public.novu_rust_operator_jwt_permission_change_requests%ROWTYPE; v_old public.novu_rust_operator_jwt_identities%ROWTYPE; v_new public.novu_rust_operator_jwt_identities%ROWTYPE; v_old_permissions jsonb; v_exists boolean; v_changed boolean;
BEGIN
 IF p_request IS NULL OR p_target_issuer IS NULL OR octet_length(p_target_issuer) NOT BETWEEN 1 AND 2048 OR p_target_subject IS NULL OR octet_length(p_target_subject) NOT BETWEEN 1 AND 1024 OR p_target_client IS NULL OR octet_length(p_target_client) NOT BETWEEN 1 AND 1024 OR p_reason IS NULL OR octet_length(p_reason) NOT BETWEEN 1 AND 1000 OR btrim(p_reason)='' OR p_expected IS NULL OR p_desired IS NULL THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 IF jsonb_typeof(p_desired)<>'object' OR (SELECT count(*) FROM jsonb_object_keys(p_desired))<>5 OR EXISTS(SELECT 1 FROM unnest(ARRAY['enabled','subscriberRead','alertRead','alertRetry','alertAccepted']) k WHERE jsonb_typeof(p_desired->k) IS DISTINCT FROM 'boolean') THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 IF p_expected<>'null'::jsonb THEN
  IF jsonb_typeof(p_expected)<>'object' OR (SELECT count(*) FROM jsonb_object_keys(p_expected))<>5 OR EXISTS(SELECT 1 FROM unnest(ARRAY['enabled','subscriberRead','alertRead','alertRetry','alertAccepted']) k WHERE jsonb_typeof(p_expected->k) IS DISTINCT FROM 'boolean') THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 END IF;
 PERFORM public.novu_rust_operator_jwt_permission_write_access(p_issuer,p_subject,p_client,p_env,p_exp);
 -- Changing one's own identity can create lock upgrades and self-escalation.
 IF (p_issuer,p_subject,p_client)=(p_target_issuer,p_target_subject,p_target_client) THEN RAISE EXCEPTION 'self change denied' USING ERRCODE='42501'; END IF;
 -- Serialize a request id before testing the target; permits safe exact replay.
 PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(pg_catalog.jsonb_build_array('jwt-permission-request',p_issuer,p_subject,p_client,p_env,p_request)::text,0));
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 SELECT * INTO v_existing FROM public.novu_rust_operator_jwt_permission_change_requests r WHERE r.issuer=p_issuer AND r.subject=p_subject AND r.client_id=p_client AND r.environment_id=p_env AND r.request_id=p_request;
 IF FOUND THEN
  IF (v_existing.target_issuer,v_existing.target_subject,v_existing.target_client,v_existing.expected,v_existing.desired,v_existing.reason) IS DISTINCT FROM (p_target_issuer,p_target_subject,p_target_client,p_expected,p_desired,p_reason) THEN RAISE EXCEPTION 'request conflict' USING ERRCODE='22023'; END IF;
  RETURN jsonb_build_object('changed',v_existing.changed,'replayed',true);
 END IF;
 -- Same advisory key as the DB administrative function protects first registration.
 PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(pg_catalog.jsonb_build_array(p_env,p_target_issuer,p_target_subject,p_target_client)::text,0));
 SELECT * INTO v_old FROM public.novu_rust_operator_jwt_identities i WHERE i.environment_id=p_env AND i.issuer=p_target_issuer AND i.subject=p_target_subject AND i.client_id=p_target_client FOR UPDATE;
 v_exists:=FOUND;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 v_old_permissions:=CASE WHEN v_exists THEN jsonb_build_object('enabled',v_old.enabled,'subscriberRead',v_old.subscriber_read,'alertRead',v_old.alert_read,'alertRetry',v_old.alert_retry,'alertAccepted',v_old.alert_accepted) ELSE 'null'::jsonb END;
 IF v_old_permissions IS DISTINCT FROM p_expected THEN RAISE EXCEPTION 'stale permission state' USING ERRCODE='22023'; END IF;
 v_changed:=v_old_permissions IS DISTINCT FROM p_desired;
 IF v_changed THEN
  IF v_exists THEN
   UPDATE public.novu_rust_operator_jwt_identities SET enabled=(p_desired->>'enabled')::boolean,subscriber_read=(p_desired->>'subscriberRead')::boolean,alert_read=(p_desired->>'alertRead')::boolean,alert_retry=(p_desired->>'alertRetry')::boolean,alert_accepted=(p_desired->>'alertAccepted')::boolean WHERE environment_id=p_env AND issuer=p_target_issuer AND subject=p_target_subject AND client_id=p_target_client RETURNING * INTO v_new;
  ELSE
   INSERT INTO public.novu_rust_operator_jwt_identities(environment_id,issuer,subject,client_id,enabled,subscriber_read,alert_read,alert_retry,alert_accepted) VALUES(p_env,p_target_issuer,p_target_subject,p_target_client,(p_desired->>'enabled')::boolean,(p_desired->>'subscriberRead')::boolean,(p_desired->>'alertRead')::boolean,(p_desired->>'alertRetry')::boolean,(p_desired->>'alertAccepted')::boolean) RETURNING * INTO v_new;
  END IF;
  INSERT INTO public.novu_rust_operator_jwt_permission_audit(environment_id,issuer,subject,client_id,actor,actor_oid,reason,previous,current,jwt_issuer,jwt_subject,jwt_client_id,request_id)
  VALUES(p_env,p_target_issuer,p_target_subject,p_target_client,session_user,(SELECT oid FROM pg_roles WHERE rolname=session_user),p_reason,CASE WHEN v_exists THEN to_jsonb(v_old) ELSE NULL END,to_jsonb(v_new),p_issuer,p_subject,p_client,p_request);
 END IF;
 INSERT INTO public.novu_rust_operator_jwt_permission_change_requests(issuer,subject,client_id,environment_id,request_id,target_issuer,target_subject,target_client,expected,desired,reason,changed) VALUES(p_issuer,p_subject,p_client,p_env,p_request,p_target_issuer,p_target_subject,p_target_client,p_expected,p_desired,p_reason,v_changed);
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN jsonb_build_object('changed',v_changed,'replayed',false);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_change_permissions(text,text,text,text,bigint,uuid,text,text,text,jsonb,jsonb,text) FROM PUBLIC;

CREATE FUNCTION public.novu_rust_operator_jwt_permission_change_request(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_request uuid)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_request public.novu_rust_operator_jwt_permission_change_requests%ROWTYPE;
BEGIN
 IF p_request IS NULL THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_permission_write_access(p_issuer,p_subject,p_client,p_env,p_exp);
 SELECT * INTO v_request FROM public.novu_rust_operator_jwt_permission_change_requests r WHERE r.issuer=p_issuer AND r.subject=p_subject AND r.client_id=p_client AND r.environment_id=p_env AND r.request_id=p_request;
 IF NOT FOUND THEN RETURN jsonb_build_object('found',false); END IF;
 RETURN jsonb_build_object('found',true,'requestId',v_request.request_id,'targetIssuer',v_request.target_issuer,'targetSubject',v_request.target_subject,'targetClientId',v_request.target_client,'expected',v_request.expected,'desired',v_request.desired,'reason',v_request.reason,'changed',v_request.changed,'createdAt',v_request.created_at);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_permission_change_request(text,text,text,text,bigint,uuid) FROM PUBLIC;

-- Preserve the review contract while attributing HTTP changes to their JWT actor.
CREATE OR REPLACE FUNCTION public.novu_rust_operator_jwt_review_audit(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_before bigint)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_result jsonb;
BEGIN
 IF p_before IS NOT NULL AND p_before<1 THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_review_access(p_issuer,p_subject,p_client,p_env,p_exp,true);
 WITH candidates AS MATERIALIZED (
  SELECT id,recorded_at,issuer,subject,client_id,actor,actor_oid,reason,previous,current,jwt_issuer,jwt_subject,jwt_client_id,request_id
  FROM public.novu_rust_operator_jwt_permission_audit
  WHERE environment_id=p_env AND (p_before IS NULL OR id<p_before)
  ORDER BY id DESC LIMIT 51
 ), page AS MATERIALIZED (SELECT * FROM candidates ORDER BY id DESC LIMIT 50)
 SELECT pg_catalog.jsonb_build_object('data',coalesce((SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
  'id',id::text,'recordedAt',recorded_at,'issuer',issuer,'subject',subject,'clientId',client_id,
  'actor',actor,'actorOid',actor_oid,'reason',reason,'previous',previous,'current',current,'jwtIssuer',jwt_issuer,'jwtSubject',jwt_subject,'jwtClientId',jwt_client_id,'requestId',request_id
 ) ORDER BY id DESC) FROM page),'[]'::jsonb),
 'next',CASE WHEN (SELECT count(*) FROM candidates)>50 THEN (SELECT min(id)::text FROM page) ELSE NULL END) INTO v_result;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN v_result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_review_audit(text,text,text,text,bigint,bigint) FROM PUBLIC;
