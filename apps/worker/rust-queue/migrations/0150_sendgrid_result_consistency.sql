-- Preserve existing grants while suppressing contradictory terminal results.
-- Expose current SendGrid projection without exposing payloads, addresses or hashes.
CREATE OR REPLACE FUNCTION public.novu_rust_sendgrid_result(p_environment text,p_job uuid)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_result jsonb; v_count bigint;
BEGIN
  PERFORM public.novu_rust_assert_alert_view(p_environment);
  IF p_job IS NULL THEN RAISE EXCEPTION 'job required' USING ERRCODE='22023'; END IF;
  SELECT jsonb_build_object(
    'jobId',j.id,'jobStatus',j.status,'result',CASE WHEN a.state IS DISTINCT FROM 'accepted' OR a.dispatch_id IS NULL OR coalesce(r.recipient_count,0)=0 OR (j.sendgrid_delivery_result='delivered' AND r.all_delivered IS NOT TRUE) OR (j.sendgrid_delivery_result='failed' AND r.all_failed IS NOT TRUE) THEN 'unknown' ELSE j.sendgrid_delivery_result END,
    'attemptState',a.state,'dispatchId',a.dispatch_id,
    'recipients',COALESCE(r.recipients,'[]'::jsonb),
    'lastCompletedScanAt',(SELECT h.last_completed_at::text FROM public.novu_rust_sendgrid_projection_heartbeat h WHERE h.environment_id=p_environment)),
    COALESCE(r.recipient_count,0)
  INTO v_result,v_count
  FROM public.novu_queue_jobs j
  LEFT JOIN public.novu_rust_sendgrid_attempts a ON a.environment_id=j.environment_id AND a.job_id=j.id
  LEFT JOIN LATERAL (
    SELECT jsonb_agg(jsonb_build_object('kind',s.recipient_kind,'index',s.recipient_index,'result',s.delivery_result)
                     ORDER BY s.recipient_kind,s.recipient_index) AS recipients,count(*) AS recipient_count, bool_and(s.delivery_result='delivered') AS all_delivered, bool_and(s.delivery_result='failed') AS all_failed
    FROM (SELECT recipient_kind,recipient_index,delivery_result FROM public.novu_rust_sendgrid_recipients
          WHERE environment_id=p_environment AND dispatch_id=a.dispatch_id ORDER BY recipient_kind,recipient_index LIMIT 1001) s
  ) r ON true
  WHERE j.id=p_job AND j.environment_id=p_environment AND j.kind='experimental_sendgrid_email';
  IF v_result IS NULL THEN RAISE EXCEPTION 'SendGrid job not found' USING ERRCODE='22023'; END IF;
  IF v_count>1000 THEN RAISE EXCEPTION 'recipient snapshot exceeds limit' USING ERRCODE='22023'; END IF;
  RETURN v_result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_sendgrid_result(text,uuid) FROM PUBLIC;
