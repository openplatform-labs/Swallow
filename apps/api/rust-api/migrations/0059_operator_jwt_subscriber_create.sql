-- Independent create permission and idempotent, audited request receipts.
CREATE TABLE public.novu_rust_operator_jwt_subscriber_create_grants (
 issuer text NOT NULL,subject text NOT NULL,client_id text NOT NULL,environment_id text NOT NULL,
 PRIMARY KEY(issuer,subject,client_id,environment_id),
 FOREIGN KEY(issuer,subject,client_id,environment_id) REFERENCES public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) ON DELETE CASCADE
);
CREATE TABLE public.novu_rust_operator_jwt_subscriber_create_requests (
 issuer text NOT NULL,subject text NOT NULL,client_id text NOT NULL,environment_id text NOT NULL,request_id uuid NOT NULL,
 subscriber_id text NOT NULL,generation uuid NOT NULL,request_digest bytea NOT NULL CHECK(octet_length(request_digest)=32),
 fields text[] NOT NULL,reason text NOT NULL,database_principal name NOT NULL,created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(issuer,subject,client_id,environment_id,request_id)
);
REVOKE ALL ON public.novu_rust_operator_jwt_subscriber_create_grants,public.novu_rust_operator_jwt_subscriber_create_requests FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_subscriber_create_access(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 PERFORM public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 PERFORM 1 FROM public.novu_rust_operator_jwt_subscriber_create_grants g WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env FOR SHARE OF g;
 IF NOT FOUND OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_subscriber_create_access(text,text,text,text,bigint) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_subscriber_create(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_request uuid,p_subscriber text,p_profile jsonb,p_reason text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE r public.novu_rust_operator_jwt_subscriber_create_requests%ROWTYPE; v_digest bytea;v_fields text[];v_generation uuid;
BEGIN
 IF p_request IS NULL OR p_subscriber IS NULL OR octet_length(p_subscriber) NOT BETWEEN 1 AND 512 OR btrim(p_subscriber)<>p_subscriber OR p_reason IS NULL OR octet_length(btrim(p_reason)) NOT BETWEEN 1 AND 1000 OR p_profile IS NULL OR jsonb_typeof(p_profile)<>'object' THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 SELECT coalesce(array_agg(key ORDER BY key),ARRAY[]::text[]) INTO v_fields FROM jsonb_object_keys(p_profile) key;
 IF NOT v_fields <@ ARRAY['firstName','lastName','email','phone']::text[] OR EXISTS(SELECT 1 FROM jsonb_each(p_profile) e WHERE jsonb_typeof(e.value) NOT IN ('string','null') OR octet_length(e.value::text)>8192) THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_subscriber_create_access(p_issuer,p_subject,p_client,p_env,p_exp);
 v_digest:=pg_catalog.sha256(pg_catalog.convert_to(pg_catalog.jsonb_build_array(p_subscriber,p_profile,p_reason)::text,'UTF8'));
 PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(pg_catalog.jsonb_build_array('jwt-subscriber-create',p_issuer,p_subject,p_client,p_env,p_request)::text,0));
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 SELECT * INTO r FROM public.novu_rust_operator_jwt_subscriber_create_requests WHERE issuer=p_issuer AND subject=p_subject AND client_id=p_client AND environment_id=p_env AND request_id=p_request;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 IF FOUND THEN
  IF r.request_digest IS DISTINCT FROM v_digest THEN RAISE EXCEPTION 'request conflict' USING ERRCODE='P4090'; END IF;
  RETURN jsonb_build_object('requestId',p_request,'subscriberId',r.subscriber_id,'generation',r.generation,'replayed',true);
 END IF;
 INSERT INTO public.novu_rust_subscribers(environment_id,subscriber_id,internal_id,profile) VALUES(p_env,p_subscriber,(SELECT lpad(to_hex(floor(extract(epoch FROM clock_timestamp()))::bigint),8,'0') || prefix || lpad(to_hex(nextval('public.novu_rust_objectid_counter'::regclass)),6,'0') FROM public.novu_rust_objectid_identity WHERE singleton),p_profile||'{"data":{}}'::jsonb) ON CONFLICT(environment_id,subscriber_id) DO NOTHING RETURNING management_generation INTO v_generation;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 IF NOT FOUND THEN RAISE EXCEPTION 'subscriber exists' USING ERRCODE='P4090'; END IF;
 INSERT INTO public.novu_rust_operator_jwt_subscriber_create_requests(issuer,subject,client_id,environment_id,request_id,subscriber_id,generation,request_digest,fields,reason,database_principal) VALUES(p_issuer,p_subject,p_client,p_env,p_request,p_subscriber,v_generation,v_digest,v_fields,btrim(p_reason),session_user);
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN jsonb_build_object('requestId',p_request,'subscriberId',p_subscriber,'generation',v_generation,'replayed',false);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_subscriber_create(text,text,text,text,bigint,uuid,text,jsonb,text) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_subscriber_create_request_read(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_request uuid)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE r public.novu_rust_operator_jwt_subscriber_create_requests%ROWTYPE;
BEGIN
 IF p_request IS NULL THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_subscriber_create_access(p_issuer,p_subject,p_client,p_env,p_exp);
 SELECT * INTO r FROM public.novu_rust_operator_jwt_subscriber_create_requests WHERE issuer=p_issuer AND subject=p_subject AND client_id=p_client AND environment_id=p_env AND request_id=p_request;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 IF NOT FOUND THEN RETURN jsonb_build_object('found',false); END IF;
 RETURN jsonb_build_object('found',true,'requestId',r.request_id,'subscriberId',r.subscriber_id,'generation',r.generation,'fields',r.fields,'reason',r.reason,'createdAt',r.created_at);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_subscriber_create_request_read(text,text,text,text,bigint,uuid) FROM PUBLIC;
