-- Diagnostic snapshots only: no payload, subscriber, device or credentials.
CREATE FUNCTION public.novu_rust_fcm_unrecorded_claim_list(p_env text,p_min_gap integer,p_grace integer,p_after uuid)
RETURNS TABLE(job_id uuid,status text,available_at text,claims integer,recorded_attempts bigint,unrecorded_claims bigint)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF p_min_gap IS NULL OR p_min_gap NOT BETWEEN 2 AND 100000 OR p_grace IS NULL OR p_grace NOT BETWEEN 1 AND 86400 THEN
  RAISE EXCEPTION 'invalid FCM claim health thresholds' USING ERRCODE='22023';
 END IF;
 PERFORM public.novu_rust_assert_alert_view(p_env);
 RETURN QUERY SELECT j.id,j.status,j.available_at::text,j.attempts,a.n,j.attempts-a.n
 FROM public.novu_queue_jobs j
 CROSS JOIN LATERAL (SELECT count(*) AS n FROM public.novu_rust_fcm_attempts a WHERE a.job_id=j.id AND a.environment_id=j.environment_id) a
 WHERE j.environment_id=p_env AND j.kind='experimental_fcm_push' AND j.status IN ('pending','running')
   AND j.available_at<=statement_timestamp()-p_grace*interval '1 second' AND j.attempts>=p_min_gap
   AND j.attempts-a.n>=p_min_gap AND (p_after IS NULL OR j.id>p_after)
 ORDER BY j.id LIMIT 101;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_fcm_unrecorded_claim_list(text,integer,integer,uuid) FROM PUBLIC;
