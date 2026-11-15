-- Recent completed jobs with at least one accepted and one rejected FCM target.
CREATE INDEX novu_rust_fcm_rejected_health_idx ON public.novu_rust_fcm_attempts(environment_id,updated_at,job_id) WHERE state='rejected';
CREATE FUNCTION public.novu_rust_fcm_partial_health(p_env text,p_window integer)
RETURNS bigint LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE result bigint;
BEGIN
 IF p_window IS NULL OR p_window NOT BETWEEN 1 AND 604800 THEN RAISE EXCEPTION 'invalid window' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_assert_alert_view(p_env);
 SELECT count(DISTINCT j.id) INTO result FROM public.novu_rust_fcm_attempts a
 JOIN public.novu_queue_jobs j ON j.id=a.job_id AND j.environment_id=a.environment_id
 WHERE a.environment_id=p_env AND a.state='rejected' AND a.updated_at>=statement_timestamp()-p_window*interval '1 second'
 AND j.kind='experimental_fcm_push' AND j.status='completed'
 AND EXISTS (SELECT 1 FROM public.novu_rust_fcm_attempts b WHERE b.environment_id=p_env AND b.job_id=j.id AND b.state='accepted');
 RETURN result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_fcm_partial_health(text,integer) FROM PUBLIC;
