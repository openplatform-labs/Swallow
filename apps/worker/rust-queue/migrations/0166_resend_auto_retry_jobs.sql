-- Environment-scoped, payload-free pages for investigating retry health.
CREATE INDEX IF NOT EXISTS novu_queue_resend_auto_attention_idx ON public.novu_queue_jobs(environment_id,id)
 WHERE kind='experimental_resend_email' AND status IN ('pending','failed');
CREATE FUNCTION public.novu_rust_resend_auto_retry_jobs(p_environment text,p_grace integer,p_filter text,p_after uuid)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_result jsonb;
BEGIN
 IF p_grace IS NULL OR p_grace NOT BETWEEN 1 AND 86400 OR p_filter IS NULL OR p_filter NOT IN ('rate-limited','exhausted','guard-deferred','overdue-auto') THEN RAISE EXCEPTION 'invalid filter' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_assert_alert_view(p_environment);
 WITH candidates AS MATERIALIZED (
  SELECT j.id,j.status,j.available_at,a.state AS attempt_state,a.http_status,a.updated_at AS attempt_updated_at
  FROM public.novu_queue_jobs j
  LEFT JOIN public.novu_rust_resend_attempts a ON a.job_id=j.id AND a.environment_id=j.environment_id
  LEFT JOIN public.novu_rust_resend_auto_retry_policy p ON p.environment_id=j.environment_id
  LEFT JOIN LATERAL (SELECT r.resend_auto_retry FROM public.novu_rust_delivery_reconciliations r WHERE r.environment_id=j.environment_id AND r.job_id=j.id AND r.target_index IS NULL AND r.decision='retry' ORDER BY r.decision_order DESC LIMIT 1) last_retry ON true
  WHERE j.environment_id=p_environment AND j.kind='experimental_resend_email' AND j.status IN ('pending','failed') AND (p_after IS NULL OR j.id>p_after)
   AND CASE p_filter
    WHEN 'rate-limited' THEN j.status='failed' AND a.state='rejected' AND a.http_status=429 AND a.updated_at<=statement_timestamp()-p_grace*interval '1 second'
    WHEN 'exhausted' THEN j.status='failed' AND a.state='rejected' AND a.http_status=429 AND a.updated_at<=statement_timestamp()-p_grace*interval '1 second' AND p.enabled AND a.created_at>=p.not_before AND (SELECT count(*) FROM public.novu_rust_delivery_reconciliations r WHERE r.environment_id=p_environment AND r.job_id=j.id AND r.target_index IS NULL AND r.decision='retry')>=p.max_retries
    WHEN 'guard-deferred' THEN j.status='pending' AND j.last_error='Resend automatic retry requires evidence guards'
    WHEN 'overdue-auto' THEN j.status='pending' AND j.available_at<=statement_timestamp()-p_grace*interval '1 second' AND coalesce(last_retry.resend_auto_retry,false)
   END
  ORDER BY j.id LIMIT 21
 ), page AS MATERIALIZED (SELECT * FROM candidates ORDER BY id LIMIT 20)
 SELECT jsonb_build_object('data',coalesce((SELECT jsonb_agg(jsonb_build_object('jobId',id,'jobStatus',status,'availableAt',available_at::text,'attemptState',attempt_state,'httpStatus',http_status,'attemptUpdatedAt',attempt_updated_at::text) ORDER BY id) FROM page),'[]'::jsonb),
  'hasMore',(SELECT count(*)>20 FROM candidates),'next',CASE WHEN (SELECT count(*)>20 FROM candidates) THEN (SELECT id::text FROM page ORDER BY id DESC LIMIT 1) ELSE NULL END) INTO v_result;
 RETURN v_result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_resend_auto_retry_jobs(text,integer,text,uuid) FROM PUBLIC;
