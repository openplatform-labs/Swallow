-- Deletion is independently authorized and fenced to a subscriber generation/profile revision.
CREATE TABLE public.novu_rust_operator_jwt_subscriber_delete_grants (
 issuer text NOT NULL,subject text NOT NULL,client_id text NOT NULL,environment_id text NOT NULL,
 PRIMARY KEY(issuer,subject,client_id,environment_id),
 FOREIGN KEY(issuer,subject,client_id,environment_id) REFERENCES public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) ON DELETE CASCADE
);
CREATE TABLE public.novu_rust_operator_jwt_subscriber_delete_requests (
 issuer text NOT NULL,subject text NOT NULL,client_id text NOT NULL,environment_id text NOT NULL,request_id uuid NOT NULL,
 subscriber_id text NOT NULL,generation uuid NOT NULL,revision bigint NOT NULL,reason text NOT NULL,database_principal name NOT NULL,created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(issuer,subject,client_id,environment_id,request_id)
);
REVOKE ALL ON public.novu_rust_operator_jwt_subscriber_delete_grants,public.novu_rust_operator_jwt_subscriber_delete_requests FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_subscriber_delete_access(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 PERFORM public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 PERFORM 1 FROM public.novu_rust_operator_jwt_subscriber_delete_grants g WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env FOR SHARE OF g;
 IF NOT FOUND OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_subscriber_delete_access(text,text,text,text,bigint) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_subscriber_delete(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_request uuid,p_subscriber text,p_generation uuid,p_revision bigint,p_reason text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE r public.novu_rust_operator_jwt_subscriber_delete_requests%ROWTYPE; s public.novu_rust_subscribers%ROWTYPE;
BEGIN
 IF p_request IS NULL OR p_subscriber IS NULL OR octet_length(p_subscriber) NOT BETWEEN 1 AND 512 OR p_generation IS NULL OR p_revision IS NULL OR p_revision<0 OR p_reason IS NULL OR octet_length(btrim(p_reason)) NOT BETWEEN 1 AND 1000 THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_subscriber_delete_access(p_issuer,p_subject,p_client,p_env,p_exp);
 PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(pg_catalog.jsonb_build_array('jwt-subscriber-delete',p_issuer,p_subject,p_client,p_env,p_request)::text,0));
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 SELECT * INTO r FROM public.novu_rust_operator_jwt_subscriber_delete_requests WHERE issuer=p_issuer AND subject=p_subject AND client_id=p_client AND environment_id=p_env AND request_id=p_request;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 IF FOUND THEN
  IF (r.subscriber_id,r.generation,r.revision,r.reason) IS DISTINCT FROM (p_subscriber,p_generation,p_revision,btrim(p_reason)) THEN RAISE EXCEPTION 'request conflict' USING ERRCODE='P4090'; END IF;
  RETURN jsonb_build_object('requestId',p_request,'subscriberId',r.subscriber_id,'generation',r.generation,'revision',r.revision::text,'replayed',true);
 END IF;
 SELECT * INTO s FROM public.novu_rust_subscribers WHERE environment_id=p_env AND subscriber_id=p_subscriber FOR UPDATE;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 IF NOT FOUND OR s.management_generation<>p_generation OR s.profile_revision<>p_revision THEN RAISE EXCEPTION 'stale subscriber' USING ERRCODE='P4090'; END IF;
 DELETE FROM public.novu_rust_in_app_messages WHERE environment_id=p_env AND subscriber_id=p_subscriber AND subscriber_internal_id=s.internal_id;
 DELETE FROM public.novu_rust_subscription_preferences WHERE environment_id=p_env AND subscriber_internal_id=s.internal_id;
 DELETE FROM public.novu_rust_email_messages WHERE environment_id=p_env AND subscriber_internal_id=s.internal_id;
 DELETE FROM public.novu_rust_sms_messages WHERE environment_id=p_env AND subscriber_internal_id=s.internal_id;
 DELETE FROM public.novu_rust_push_messages WHERE environment_id=p_env AND subscriber_internal_id=s.internal_id;
 DELETE FROM public.novu_rust_subscribers WHERE environment_id=p_env AND subscriber_id=p_subscriber AND internal_id=s.internal_id;
 INSERT INTO public.novu_rust_operator_jwt_subscriber_delete_requests(issuer,subject,client_id,environment_id,request_id,subscriber_id,generation,revision,reason,database_principal) VALUES(p_issuer,p_subject,p_client,p_env,p_request,p_subscriber,p_generation,p_revision,btrim(p_reason),session_user);
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN jsonb_build_object('requestId',p_request,'subscriberId',p_subscriber,'generation',p_generation,'revision',p_revision::text,'replayed',false);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_subscriber_delete(text,text,text,text,bigint,uuid,text,uuid,bigint,text) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_subscriber_delete_request_read(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_request uuid)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE r public.novu_rust_operator_jwt_subscriber_delete_requests%ROWTYPE;
BEGIN
 IF p_request IS NULL THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_subscriber_delete_access(p_issuer,p_subject,p_client,p_env,p_exp);
 SELECT * INTO r FROM public.novu_rust_operator_jwt_subscriber_delete_requests WHERE issuer=p_issuer AND subject=p_subject AND client_id=p_client AND environment_id=p_env AND request_id=p_request;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 IF NOT FOUND THEN RETURN jsonb_build_object('found',false); END IF;
 RETURN jsonb_build_object('found',true,'requestId',r.request_id,'subscriberId',r.subscriber_id,'generation',r.generation,'revision',r.revision::text,'reason',r.reason,'createdAt',r.created_at);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_subscriber_delete_request_read(text,text,text,text,bigint,uuid) FROM PUBLIC;
