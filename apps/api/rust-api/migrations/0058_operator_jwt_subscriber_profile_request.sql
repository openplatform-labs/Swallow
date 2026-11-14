-- Deduplicate profile updates without storing personal profile values again.
CREATE TABLE public.novu_rust_operator_jwt_subscriber_profile_requests (
 issuer text NOT NULL,subject text NOT NULL,client_id text NOT NULL,environment_id text NOT NULL,request_id uuid NOT NULL,
 subscriber_id text NOT NULL,generation uuid NOT NULL,previous_revision bigint NOT NULL,new_revision bigint NOT NULL,changed boolean NOT NULL,
 request_digest bytea NOT NULL CHECK(octet_length(request_digest)=32),fields text[] NOT NULL,reason text NOT NULL,created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(issuer,subject,client_id,environment_id,request_id)
);
REVOKE ALL ON public.novu_rust_operator_jwt_subscriber_profile_requests FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_subscriber_profile_write_v2(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_request uuid,p_subscriber text,p_generation uuid,p_revision bigint,p_profile jsonb,p_reason text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE r public.novu_rust_operator_jwt_subscriber_profile_requests%ROWTYPE; v_digest bytea; v_result jsonb; v_fields text[];
BEGIN
 IF p_request IS NULL OR p_subscriber IS NULL OR octet_length(p_subscriber) NOT BETWEEN 1 AND 512 OR p_generation IS NULL OR p_revision IS NULL OR p_revision<0 OR p_profile IS NULL OR jsonb_typeof(p_profile)<>'object' OR p_reason IS NULL OR octet_length(btrim(p_reason)) NOT BETWEEN 1 AND 1000 THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 SELECT array_agg(key ORDER BY key) INTO v_fields FROM jsonb_object_keys(p_profile) key;
 IF v_fields IS NULL OR NOT v_fields <@ ARRAY['firstName','lastName','email','phone']::text[] OR EXISTS(SELECT 1 FROM jsonb_each(p_profile) e WHERE jsonb_typeof(e.value) NOT IN ('string','null') OR octet_length(e.value::text)>8192) THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_subscriber_profile_access(p_issuer,p_subject,p_client,p_env,p_exp,'write');
 v_digest:=pg_catalog.sha256(pg_catalog.convert_to(pg_catalog.jsonb_build_array(p_subscriber,p_generation,p_revision,p_profile,p_reason)::text,'UTF8'));
 PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(pg_catalog.jsonb_build_array('jwt-subscriber-profile-request',p_issuer,p_subject,p_client,p_env,p_request)::text,0));
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 SELECT * INTO r FROM public.novu_rust_operator_jwt_subscriber_profile_requests WHERE issuer=p_issuer AND subject=p_subject AND client_id=p_client AND environment_id=p_env AND request_id=p_request;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 IF FOUND THEN
  IF r.request_digest IS DISTINCT FROM v_digest THEN RAISE EXCEPTION 'request conflict' USING ERRCODE='P4090'; END IF;
  RETURN jsonb_build_object('requestId',p_request,'subscriberId',r.subscriber_id,'generation',r.generation,'revision',r.new_revision::text,'changed',r.changed,'replayed',true);
 END IF;
 v_result:=public.novu_rust_operator_jwt_subscriber_profile_write(p_issuer,p_subject,p_client,p_env,p_exp,p_subscriber,p_generation,p_revision,p_profile,p_reason);
 INSERT INTO public.novu_rust_operator_jwt_subscriber_profile_requests(issuer,subject,client_id,environment_id,request_id,subscriber_id,generation,previous_revision,new_revision,changed,request_digest,fields,reason) VALUES(p_issuer,p_subject,p_client,p_env,p_request,p_subscriber,p_generation,p_revision,(v_result->>'revision')::bigint,(v_result->>'changed')::boolean,v_digest,v_fields,btrim(p_reason));
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN v_result||jsonb_build_object('requestId',p_request,'replayed',false);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_subscriber_profile_write_v2(text,text,text,text,bigint,uuid,text,uuid,bigint,jsonb,text) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_subscriber_profile_request_read(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_request uuid)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE r public.novu_rust_operator_jwt_subscriber_profile_requests%ROWTYPE;
BEGIN
 IF p_request IS NULL THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_subscriber_profile_access(p_issuer,p_subject,p_client,p_env,p_exp,'write');
 SELECT * INTO r FROM public.novu_rust_operator_jwt_subscriber_profile_requests WHERE issuer=p_issuer AND subject=p_subject AND client_id=p_client AND environment_id=p_env AND request_id=p_request;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 IF NOT FOUND THEN RETURN jsonb_build_object('found',false); END IF;
 RETURN jsonb_build_object('found',true,'requestId',r.request_id,'subscriberId',r.subscriber_id,'generation',r.generation,'previousRevision',r.previous_revision::text,'revision',r.new_revision::text,'changed',r.changed,'fields',r.fields,'reason',r.reason,'createdAt',r.created_at);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_subscriber_profile_request_read(text,text,text,text,bigint,uuid) FROM PUBLIC;
