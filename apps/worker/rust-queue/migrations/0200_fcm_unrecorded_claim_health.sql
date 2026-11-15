-- A diagnostic count, not proof of a version fence failure.
CREATE FUNCTION public.novu_rust_fcm_unrecorded_claim_health(p_env text,p_min_gap integer,p_grace integer)
RETURNS bigint LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE result bigint;
BEGIN
 IF p_min_gap NOT BETWEEN 2 AND 100000 OR p_grace NOT BETWEEN 1 AND 86400 OR p_min_gap IS NULL OR p_grace IS NULL THEN
  RAISE EXCEPTION 'invalid FCM claim health thresholds' USING ERRCODE='22023';
 END IF;
 PERFORM public.novu_rust_assert_alert_view(p_env);
 SELECT count(*) INTO result FROM public.novu_queue_jobs j
 WHERE j.environment_id=p_env AND j.kind='experimental_fcm_push' AND j.status IN ('pending','running')
   AND j.available_at<=statement_timestamp()-p_grace*interval '1 second'
   AND j.attempts >= p_min_gap
   AND j.attempts - (SELECT count(*) FROM public.novu_rust_fcm_attempts a WHERE a.job_id=j.id AND a.environment_id=j.environment_id) >= p_min_gap;
 RETURN result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_fcm_unrecorded_claim_health(text,integer,integer) FROM PUBLIC;
