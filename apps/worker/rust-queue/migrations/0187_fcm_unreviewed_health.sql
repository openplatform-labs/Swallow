-- Count unreviewed snapshots without a time window or the list's 100-row limit.
CREATE INDEX novu_rust_fcm_completed_review_idx ON public.novu_queue_jobs(environment_id,id) WHERE kind='experimental_fcm_push' AND status='completed';
CREATE FUNCTION public.novu_rust_fcm_unreviewed_health(p_env text)
RETURNS bigint LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_count bigint;
BEGIN
 PERFORM public.novu_rust_assert_alert_view(p_env);
 SELECT count(*) INTO v_count FROM public.novu_queue_jobs j
 CROSS JOIN LATERAL public.novu_rust_fcm_partial_snapshot(j.environment_id,j.id) s
 WHERE j.environment_id=p_env AND j.kind='experimental_fcm_push' AND j.status='completed'
 AND s.accepted>0 AND s.rejected>0
 AND NOT EXISTS (SELECT 1 FROM public.novu_rust_fcm_partial_reviews r WHERE r.environment_id=p_env AND r.job_id=j.id AND r.revision=s.revision);
 RETURN v_count;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_fcm_unreviewed_health(text) FROM PUBLIC;
