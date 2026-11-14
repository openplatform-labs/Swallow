-- Bounded operator view; excludes queue payloads, addresses, credentials and diagnostics.
CREATE OR REPLACE FUNCTION public.novu_rust_list_unknown_attempts(p_environment text,p_after uuid,p_target integer)
RETURNS TABLE(job_id uuid,kind text,job_status text,target_index integer,http_status integer,updated_at text)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
  PERFORM public.novu_rust_assert_alert_view(p_environment);
  IF p_target IS NULL OR p_target < -1 OR (p_after IS NULL AND p_target<>-1) THEN
    RAISE EXCEPTION 'invalid cursor' USING ERRCODE='22023';
  END IF;
  RETURN QUERY SELECT j.id,j.kind,j.status,a.target_index,a.http_status,a.updated_at::text
    FROM public.novu_queue_jobs j JOIN (
      SELECT x.job_id,x.environment_id,NULL::integer AS target_index,x.http_status,x.updated_at FROM public.novu_rust_sendgrid_attempts x WHERE x.state='unknown' AND x.environment_id=p_environment
      UNION ALL SELECT x.job_id,x.environment_id,NULL::integer,x.http_status,x.updated_at FROM public.novu_rust_resend_attempts x WHERE x.state='unknown' AND x.environment_id=p_environment
      UNION ALL SELECT x.job_id,x.environment_id,NULL::integer,x.http_status,x.updated_at FROM public.novu_rust_twilio_attempts x WHERE x.state='unknown' AND x.environment_id=p_environment
      UNION ALL SELECT x.job_id,x.environment_id,x.target_index,x.http_status,x.updated_at FROM public.novu_rust_fcm_attempts x WHERE x.state='unknown' AND x.environment_id=p_environment
    ) a ON a.job_id=j.id AND a.environment_id=j.environment_id
    WHERE j.environment_id=p_environment AND (p_after IS NULL OR (j.id,coalesce(a.target_index,-1))>(p_after,p_target))
    ORDER BY j.id,coalesce(a.target_index,-1) LIMIT 100;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_list_unknown_attempts(text,uuid,integer) FROM PUBLIC;
