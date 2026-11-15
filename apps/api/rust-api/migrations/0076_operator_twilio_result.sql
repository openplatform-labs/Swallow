-- Independent, environment-scoped SMS result and listing permissions.
CREATE TABLE public.novu_rust_operator_jwt_twilio_result_grants (
 issuer text NOT NULL,subject text NOT NULL,client_id text NOT NULL,environment_id text NOT NULL,
 PRIMARY KEY(issuer,subject,client_id,environment_id),
 FOREIGN KEY(issuer,subject,client_id,environment_id) REFERENCES public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) ON DELETE CASCADE
);
CREATE TABLE public.novu_rust_operator_jwt_twilio_jobs_grants (
 issuer text NOT NULL,subject text NOT NULL,client_id text NOT NULL,environment_id text NOT NULL,
 PRIMARY KEY(issuer,subject,client_id,environment_id),
 FOREIGN KEY(issuer,subject,client_id,environment_id) REFERENCES public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) ON DELETE CASCADE
);
REVOKE ALL ON public.novu_rust_operator_jwt_twilio_result_grants,public.novu_rust_operator_jwt_twilio_jobs_grants FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_twilio_result(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_job uuid)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE result jsonb;
BEGIN
 PERFORM public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 PERFORM 1 FROM public.novu_rust_operator_jwt_twilio_result_grants g WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env FOR SHARE OF g;
 IF NOT FOUND OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 IF p_job IS NULL THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 SELECT jsonb_build_object('jobId',j.id,'jobStatus',j.status,'result',CASE WHEN v.ambiguous THEN 'unknown' ELSE coalesce(j.twilio_delivery_result,'unknown') END,
  'providerIdAmbiguous',v.ambiguous,'attemptState',a.state,'providerIdKnown',coalesce(length(a.provider_message_id)>0,false),
  'lastCompletedScanAt',(SELECT h.last_completed_at::text FROM public.novu_rust_twilio_projection_heartbeat h WHERE h.environment_id=p_env)) INTO result
 FROM public.novu_queue_jobs j LEFT JOIN public.novu_rust_twilio_attempts a ON a.environment_id=j.environment_id AND a.job_id=j.id
 CROSS JOIN LATERAL (SELECT EXISTS(SELECT 1 FROM public.novu_rust_twilio_attempts b WHERE b.environment_id=j.environment_id AND b.provider_message_id=a.provider_message_id AND b.job_id<>a.job_id AND a.provider_message_id IS NOT NULL AND a.provider_message_id<>'') AS ambiguous) v
 WHERE j.id=p_job AND j.environment_id=p_env AND j.kind='experimental_twilio_sms';
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 IF result IS NULL THEN RAISE EXCEPTION 'channel job not found' USING ERRCODE='22023'; END IF;
 RETURN result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_twilio_result(text,text,text,text,bigint,uuid) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_twilio_jobs(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_after uuid)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE result jsonb;
BEGIN
 PERFORM public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 PERFORM 1 FROM public.novu_rust_operator_jwt_twilio_result_grants g WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env FOR SHARE OF g;
 IF NOT FOUND OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 PERFORM 1 FROM public.novu_rust_operator_jwt_twilio_jobs_grants g WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env FOR SHARE OF g;
 IF NOT FOUND OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 WITH candidates AS MATERIALIZED (SELECT j.id FROM public.novu_queue_jobs j WHERE j.environment_id=p_env AND j.kind='experimental_twilio_sms' AND (p_after IS NULL OR j.id>p_after) ORDER BY j.id LIMIT 21),
 page AS MATERIALIZED (SELECT id FROM candidates ORDER BY id LIMIT 20)
 SELECT jsonb_build_object('data',coalesce((SELECT jsonb_agg(public.novu_rust_operator_jwt_twilio_result(p_issuer,p_subject,p_client,p_env,p_exp,id) ORDER BY id) FROM page),'[]'::jsonb),
  'hasMore',(SELECT count(*)>20 FROM candidates),'next',CASE WHEN (SELECT count(*)>20 FROM candidates) THEN (SELECT id::text FROM page ORDER BY id DESC LIMIT 1) ELSE NULL END) INTO result;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_twilio_jobs(text,text,text,text,bigint,uuid) FROM PUBLIC;
