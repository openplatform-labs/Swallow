-- Apply worker 0184 before this migration. Existing grants remain unchanged.
CREATE OR REPLACE FUNCTION public.novu_rust_operator_jwt_fcm_result(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_job uuid)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE result jsonb;
BEGIN
 PERFORM public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 PERFORM 1 FROM public.novu_rust_operator_jwt_fcm_result_grants g WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env FOR SHARE OF g;
 IF NOT FOUND OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 IF p_job IS NULL THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 SELECT jsonb_build_object('jobId',j.id,'jobStatus',j.status,'attemptCount',count(a.job_id),
 'acceptedCount',count(a.job_id) FILTER(WHERE a.state='accepted'),'rejectedCount',count(a.job_id) FILTER(WHERE a.state='rejected'),
 'unknownCount',count(a.job_id) FILTER(WHERE a.state='unknown'),'dispatchingCount',count(a.job_id) FILTER(WHERE a.state='dispatching'),
 'skippedCount',count(a.job_id) FILTER(WHERE a.state='skipped'),'operatorAcceptedCount',count(a.job_id) FILTER(WHERE a.operator_resolution='accepted'),
 'operatorNotAcceptedCount',count(a.job_id) FILTER(WHERE a.operator_resolution='not_accepted'),
 'rateLimitedCount',count(a.job_id) FILTER(WHERE a.http_status=429),
 'rateLimitAdviceCount',count(a.retry_not_before) FILTER(WHERE a.http_status=429),
 'rateLimitNotBefore',max(a.retry_not_before) FILTER(WHERE a.http_status=429)) INTO result
 FROM public.novu_queue_jobs j LEFT JOIN public.novu_rust_fcm_attempts a ON a.environment_id=j.environment_id AND a.job_id=j.id
 WHERE j.id=p_job AND j.environment_id=p_env AND j.kind='experimental_fcm_push' GROUP BY j.id,j.status;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 IF result IS NULL THEN RAISE EXCEPTION 'channel job not found' USING ERRCODE='22023'; END IF;
 RETURN result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_fcm_result(text,text,text,text,bigint,uuid) FROM PUBLIC;
