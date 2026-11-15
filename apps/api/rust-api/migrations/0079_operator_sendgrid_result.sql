-- Independent operator access to non-identifying SendGrid projection counts.
CREATE TABLE public.novu_rust_operator_jwt_sendgrid_result_grants (
 issuer text NOT NULL,subject text NOT NULL,client_id text NOT NULL,environment_id text NOT NULL,
 PRIMARY KEY(issuer,subject,client_id,environment_id),
 FOREIGN KEY(issuer,subject,client_id,environment_id) REFERENCES public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) ON DELETE CASCADE
);
CREATE TABLE public.novu_rust_operator_jwt_sendgrid_jobs_grants (
 issuer text NOT NULL,subject text NOT NULL,client_id text NOT NULL,environment_id text NOT NULL,
 PRIMARY KEY(issuer,subject,client_id,environment_id),
 FOREIGN KEY(issuer,subject,client_id,environment_id) REFERENCES public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) ON DELETE CASCADE
);
REVOKE ALL ON public.novu_rust_operator_jwt_sendgrid_result_grants,public.novu_rust_operator_jwt_sendgrid_jobs_grants FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_sendgrid_result(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_job uuid)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE result jsonb;
BEGIN
 PERFORM public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 PERFORM 1 FROM public.novu_rust_operator_jwt_sendgrid_result_grants g WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env FOR SHARE OF g;
 IF NOT FOUND OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 IF p_job IS NULL THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 SELECT jsonb_build_object('jobId',j.id,'jobStatus',j.status,'attemptState',a.state,'dispatchTracked',a.dispatch_id IS NOT NULL,
 'result',CASE WHEN a.state IS DISTINCT FROM 'accepted' OR a.dispatch_id IS NULL OR r.total=0 THEN 'unknown' ELSE j.sendgrid_delivery_result END,
 'recipientCount',r.total,'deliveredCount',r.delivered,'failedCount',r.failed,'pendingCount',r.pending,'unknownCount',r.unknown,'conflictingCount',r.conflicting,
 'lastCompletedScanAt',(SELECT h.last_completed_at::text FROM public.novu_rust_sendgrid_projection_heartbeat h WHERE h.environment_id=p_env)) INTO result
 FROM public.novu_queue_jobs j LEFT JOIN public.novu_rust_sendgrid_attempts a ON a.environment_id=j.environment_id AND a.job_id=j.id
 CROSS JOIN LATERAL (SELECT count(*) AS total,count(*) FILTER(WHERE s.delivery_result='delivered') AS delivered,count(*) FILTER(WHERE s.delivery_result='failed') AS failed,count(*) FILTER(WHERE s.delivery_result='pending') AS pending,count(*) FILTER(WHERE s.delivery_result='unknown') AS unknown,count(*) FILTER(WHERE s.delivery_result='conflicting') AS conflicting FROM public.novu_rust_sendgrid_recipients s WHERE s.environment_id=j.environment_id AND s.dispatch_id=a.dispatch_id) r
 WHERE j.id=p_job AND j.environment_id=p_env AND j.kind='experimental_sendgrid_email';
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 IF result IS NULL THEN RAISE EXCEPTION 'channel job not found' USING ERRCODE='22023'; END IF;
 RETURN result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_sendgrid_result(text,text,text,text,bigint,uuid) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_sendgrid_jobs(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_after uuid)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE result jsonb;
BEGIN
 PERFORM public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 PERFORM 1 FROM public.novu_rust_operator_jwt_sendgrid_result_grants g WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env FOR SHARE OF g;
 IF NOT FOUND OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 PERFORM 1 FROM public.novu_rust_operator_jwt_sendgrid_jobs_grants g WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env FOR SHARE OF g;
 IF NOT FOUND OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 WITH candidates AS MATERIALIZED (SELECT j.id FROM public.novu_queue_jobs j WHERE j.environment_id=p_env AND j.kind='experimental_sendgrid_email' AND (p_after IS NULL OR j.id>p_after) ORDER BY j.id LIMIT 21),page AS MATERIALIZED (SELECT id FROM candidates ORDER BY id LIMIT 20)
 SELECT jsonb_build_object('data',coalesce((SELECT jsonb_agg(public.novu_rust_operator_jwt_sendgrid_result(p_issuer,p_subject,p_client,p_env,p_exp,id) ORDER BY id) FROM page),'[]'::jsonb),
 'hasMore',(SELECT count(*)>20 FROM candidates),'next',CASE WHEN (SELECT count(*)>20 FROM candidates) THEN (SELECT id::text FROM page ORDER BY id DESC LIMIT 1) ELSE NULL END) INTO result;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_sendgrid_jobs(text,text,text,text,bigint,uuid) FROM PUBLIC;
