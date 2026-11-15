-- List recent partial FCM jobs under independently provisioned JWT and DB rights.
CREATE TABLE public.novu_rust_operator_jwt_fcm_partial_jobs_grants (
 issuer text NOT NULL,subject text NOT NULL,client_id text NOT NULL,environment_id text NOT NULL,
 PRIMARY KEY(issuer,subject,client_id,environment_id),
 FOREIGN KEY(issuer,subject,client_id,environment_id) REFERENCES public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) ON DELETE CASCADE
);
REVOKE ALL ON public.novu_rust_operator_jwt_fcm_partial_jobs_grants FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_fcm_partial_jobs(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_window integer,p_after uuid)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE result jsonb;
BEGIN
 IF p_window IS NULL OR p_window NOT BETWEEN 1 AND 604800 THEN RAISE EXCEPTION 'invalid threshold' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 PERFORM 1 FROM public.novu_rust_operator_jwt_fcm_partial_health_grants g WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env FOR SHARE OF g;
 IF NOT FOUND OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 PERFORM 1 FROM public.novu_rust_operator_jwt_fcm_partial_jobs_grants g WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env FOR SHARE OF g;
 IF NOT FOUND OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 PERFORM public.novu_rust_assert_alert_view(p_env);
 WITH candidates AS MATERIALIZED (
  SELECT j.id FROM public.novu_queue_jobs j
  WHERE j.environment_id=p_env AND j.kind='experimental_fcm_push' AND j.status='completed' AND (p_after IS NULL OR j.id>p_after)
   AND EXISTS(SELECT 1 FROM public.novu_rust_fcm_attempts a WHERE a.environment_id=p_env AND a.job_id=j.id AND a.state='accepted')
   AND EXISTS(SELECT 1 FROM public.novu_rust_fcm_attempts a WHERE a.environment_id=p_env AND a.job_id=j.id AND a.state='rejected' AND a.updated_at>=statement_timestamp()-p_window*interval '1 second')
  ORDER BY j.id LIMIT 21
 ), page AS MATERIALIZED (SELECT id FROM candidates ORDER BY id LIMIT 20)
 SELECT jsonb_build_object('data',coalesce((SELECT jsonb_agg(jsonb_build_object('jobId',p.id,'acceptedCount',(SELECT count(*) FROM public.novu_rust_fcm_attempts a WHERE a.environment_id=p_env AND a.job_id=p.id AND a.state='accepted'),'rejectedCount',(SELECT count(*) FROM public.novu_rust_fcm_attempts a WHERE a.environment_id=p_env AND a.job_id=p.id AND a.state='rejected')) ORDER BY p.id) FROM page p),'[]'::jsonb),
 'hasMore',(SELECT count(*)>20 FROM candidates),'next',CASE WHEN (SELECT count(*)>20 FROM candidates) THEN (SELECT id::text FROM page ORDER BY id DESC LIMIT 1) ELSE NULL END) INTO result;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_fcm_partial_jobs(text,text,text,text,bigint,integer,uuid) FROM PUBLIC;
