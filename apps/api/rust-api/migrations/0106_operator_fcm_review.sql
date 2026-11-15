-- Separate list and review grants; idempotent request identity is scoped to JWT and environment.
CREATE TABLE public.novu_rust_operator_jwt_fcm_unreviewed_grants (
 issuer text NOT NULL,subject text NOT NULL,client_id text NOT NULL,environment_id text NOT NULL,
 PRIMARY KEY(issuer,subject,client_id,environment_id),
 FOREIGN KEY(issuer,subject,client_id,environment_id) REFERENCES public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) ON DELETE CASCADE
);
CREATE TABLE public.novu_rust_operator_jwt_fcm_review_grants (
 issuer text NOT NULL,subject text NOT NULL,client_id text NOT NULL,environment_id text NOT NULL,
 PRIMARY KEY(issuer,subject,client_id,environment_id),
 FOREIGN KEY(issuer,subject,client_id,environment_id) REFERENCES public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) ON DELETE CASCADE
);
CREATE TABLE public.novu_rust_operator_jwt_fcm_review_requests (
 issuer text NOT NULL,subject text NOT NULL,client_id text NOT NULL,environment_id text NOT NULL,request_id uuid NOT NULL,
 job_id uuid NOT NULL,revision text NOT NULL,reason text NOT NULL,recorded boolean,created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(issuer,subject,client_id,environment_id,request_id)
);
REVOKE ALL ON public.novu_rust_operator_jwt_fcm_unreviewed_grants,public.novu_rust_operator_jwt_fcm_review_grants,public.novu_rust_operator_jwt_fcm_review_requests FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_fcm_unreviewed(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_after uuid)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE result jsonb;
BEGIN
 PERFORM public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 PERFORM 1 FROM public.novu_rust_operator_jwt_fcm_unreviewed_grants g WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env FOR SHARE OF g;
 IF NOT FOUND OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 WITH candidates AS MATERIALIZED (SELECT * FROM public.novu_rust_fcm_partial_unreviewed(p_env,p_after) ORDER BY job_id LIMIT 21),page AS MATERIALIZED (SELECT * FROM candidates ORDER BY job_id LIMIT 20)
 SELECT jsonb_build_object('data',coalesce((SELECT jsonb_agg(jsonb_build_object('jobId',job_id,'acceptedCount',accepted,'rejectedCount',rejected,'revision',revision) ORDER BY job_id) FROM page),'[]'::jsonb),
 'hasMore',(SELECT count(*)>20 FROM candidates),'next',CASE WHEN (SELECT count(*)>20 FROM candidates) THEN (SELECT job_id::text FROM page ORDER BY job_id DESC LIMIT 1) ELSE NULL END) INTO result;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_fcm_unreviewed(text,text,text,text,bigint,uuid) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_fcm_review(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_request uuid,p_job uuid,p_revision text,p_reason text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE n integer;r public.novu_rust_operator_jwt_fcm_review_requests%ROWTYPE;operator_id text;v_recorded boolean;
BEGIN
 IF p_request IS NULL OR p_job IS NULL OR p_revision IS NULL OR p_revision !~ '^[0-9a-f]{64}$' OR p_reason IS NULL OR length(btrim(p_reason))=0 OR octet_length(p_reason)>512 THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 PERFORM 1 FROM public.novu_rust_operator_jwt_fcm_review_grants g WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env FOR SHARE OF g;
 IF NOT FOUND OR NOT public.novu_rust_lock_operator_grant(p_env,'queue_ack') OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 INSERT INTO public.novu_rust_operator_jwt_fcm_review_requests(issuer,subject,client_id,environment_id,request_id,job_id,revision,reason) VALUES(p_issuer,p_subject,p_client,p_env,p_request,p_job,p_revision,p_reason) ON CONFLICT DO NOTHING;
 GET DIAGNOSTICS n=ROW_COUNT;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 IF n=0 THEN
  SELECT * INTO r FROM public.novu_rust_operator_jwt_fcm_review_requests WHERE issuer=p_issuer AND subject=p_subject AND client_id=p_client AND environment_id=p_env AND request_id=p_request;
  IF NOT FOUND OR (r.job_id,r.revision,r.reason) IS DISTINCT FROM (p_job,p_revision,p_reason) OR r.recorded IS NULL THEN RAISE EXCEPTION 'request id conflict' USING ERRCODE='P4090'; END IF;
  IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
  RETURN jsonb_build_object('recorded',r.recorded,'replayed',true);
 END IF;
 operator_id:='jwt:'||encode(sha256(convert_to(jsonb_build_array(p_issuer,p_subject,p_client,p_env)::text,'UTF8')),'hex');
 v_recorded:=public.novu_rust_fcm_partial_ack(p_env,p_job,p_revision,operator_id,p_reason);
 UPDATE public.novu_rust_operator_jwt_fcm_review_requests SET recorded=v_recorded WHERE issuer=p_issuer AND subject=p_subject AND client_id=p_client AND environment_id=p_env AND request_id=p_request;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN jsonb_build_object('recorded',v_recorded,'replayed',false);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_fcm_review(text,text,text,text,bigint,uuid,uuid,text,text) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_fcm_review_request(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_request uuid)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE r public.novu_rust_operator_jwt_fcm_review_requests%ROWTYPE;
BEGIN
 IF p_request IS NULL THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 PERFORM 1 FROM public.novu_rust_operator_jwt_fcm_review_grants g WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env FOR SHARE OF g;
 IF NOT FOUND OR NOT public.novu_rust_lock_operator_grant(p_env,'queue_ack') OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 SELECT * INTO r FROM public.novu_rust_operator_jwt_fcm_review_requests WHERE issuer=p_issuer AND subject=p_subject AND client_id=p_client AND environment_id=p_env AND request_id=p_request;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 IF NOT FOUND THEN RETURN jsonb_build_object('found',false); END IF;
 RETURN jsonb_build_object('found',true,'requestId',r.request_id,'jobId',r.job_id,'revision',r.revision,'reason',r.reason,'recorded',r.recorded,'createdAt',r.created_at::text);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_fcm_review_request(text,text,text,text,bigint,uuid) FROM PUBLIC;
