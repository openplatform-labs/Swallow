-- Enqueue authorization is independent of the worker and review permissions.
CREATE TABLE public.novu_rust_telegram_enqueue_grants (
 environment_id text NOT NULL REFERENCES public.novu_rust_environments(id),destination_id text NOT NULL,
 database_principal name NOT NULL,principal_oid oid NOT NULL,
 PRIMARY KEY(environment_id,destination_id,database_principal)
);
CREATE TABLE public.novu_rust_telegram_enqueue_requests (
 environment_id text NOT NULL REFERENCES public.novu_rust_environments(id),request_id uuid NOT NULL,
 destination_id text NOT NULL,job_id uuid NOT NULL,payload_hash text NOT NULL,
 database_principal name NOT NULL,principal_oid oid NOT NULL,created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(environment_id,request_id)
);
REVOKE ALL ON public.novu_rust_telegram_enqueue_grants,public.novu_rust_telegram_enqueue_requests FROM PUBLIC;
CREATE FUNCTION public.novu_rust_telegram_enqueue(p_env text,p_request uuid,p_payload jsonb) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_dest text;v_oid oid;v_hash text;v_job uuid;r public.novu_rust_telegram_enqueue_requests%ROWTYPE;
BEGIN
 IF p_env IS NULL OR length(btrim(p_env))=0 OR p_request IS NULL OR p_payload IS NULL OR jsonb_typeof(p_payload)<>'object' OR octet_length(p_payload::text)>131072 THEN RAISE EXCEPTION 'invalid Telegram enqueue request' USING ERRCODE='22023'; END IF;
 v_dest:=p_payload->>'destination';
 IF v_dest IS NULL OR v_dest !~ '^[A-Za-z0-9._-]{1,64}$' OR jsonb_typeof(p_payload->'destination') IS DISTINCT FROM 'string' OR
    jsonb_typeof(p_payload->'chatId') IS DISTINCT FROM 'string' OR octet_length(p_payload->>'chatId') NOT BETWEEN 1 AND 256 OR position(chr(10) in (p_payload->>'chatId'))>0 OR position(chr(13) in (p_payload->>'chatId'))>0 OR
    jsonb_typeof(p_payload->'text') IS DISTINCT FROM 'string' OR octet_length(p_payload->>'text')=0 OR
    (p_payload ? 'passthrough' AND jsonb_typeof(p_payload->'passthrough') IS DISTINCT FROM 'object') OR
    EXISTS(SELECT 1 FROM jsonb_object_keys(p_payload) k WHERE k NOT IN ('destination','chatId','text','passthrough')) THEN RAISE EXCEPTION 'invalid Telegram enqueue request' USING ERRCODE='22023'; END IF;
 IF p_payload ? 'passthrough' AND (SELECT count(*) FROM jsonb_object_keys(p_payload->'passthrough'))>100 THEN RAISE EXCEPTION 'invalid Telegram enqueue request' USING ERRCODE='22023'; END IF;
 SELECT g.principal_oid INTO v_oid FROM public.novu_rust_telegram_enqueue_grants g JOIN pg_catalog.pg_roles role_entry ON role_entry.oid=g.principal_oid AND role_entry.rolname=g.database_principal AND role_entry.rolcanlogin
 WHERE g.environment_id=p_env AND g.destination_id=v_dest AND g.database_principal=session_user FOR SHARE OF g;
 IF NOT FOUND THEN RAISE EXCEPTION 'Telegram enqueue denied' USING ERRCODE='42501'; END IF;
 v_hash:=encode(sha256(convert_to(p_payload::text,'UTF8')),'hex');
 PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(pg_catalog.jsonb_build_array('telegram-enqueue',p_env,p_request)::text,0));
 SELECT * INTO r FROM public.novu_rust_telegram_enqueue_requests WHERE environment_id=p_env AND request_id=p_request;
 IF FOUND THEN
  IF (r.destination_id,r.payload_hash,r.database_principal,r.principal_oid) IS DISTINCT FROM (v_dest,v_hash,session_user,v_oid) THEN RAISE EXCEPTION 'Telegram enqueue request conflict' USING ERRCODE='P4090'; END IF;
  RETURN jsonb_build_object('jobId',r.job_id,'requestId',p_request,'replayed',true);
 END IF;
 v_job:=gen_random_uuid();
 INSERT INTO public.novu_rust_telegram_enqueue_requests(environment_id,request_id,destination_id,job_id,payload_hash,database_principal,principal_oid) VALUES(p_env,p_request,v_dest,v_job,v_hash,session_user,v_oid);
 INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload,available_at) VALUES(v_job,p_env,'experimental_telegram',p_payload,clock_timestamp());
 RETURN jsonb_build_object('jobId',v_job,'requestId',p_request,'replayed',false);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_telegram_enqueue(text,uuid,jsonb) FROM PUBLIC;
