-- A bounded hint is advisory only; it never schedules a retry.
ALTER TABLE public.novu_rust_telegram_attempts ADD COLUMN retry_after_seconds integer
 CHECK (retry_after_seconds IS NULL OR (state='rejected' AND http_status=429 AND retry_after_seconds BETWEEN 0 AND 86400));

CREATE FUNCTION public.novu_rust_telegram_worker_finish_rate_limit(p_env text,p_dest text,p_job uuid,p_token uuid,p_retry integer)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE done boolean;
BEGIN
 IF p_retry IS NULL OR p_retry NOT BETWEEN 0 AND 86400 THEN RAISE EXCEPTION 'invalid Telegram retry hint' USING ERRCODE='22023'; END IF;
 done:=public.novu_rust_telegram_worker_finish(p_env,p_dest,p_job,p_token,'rejected',429,NULL,NULL);
 IF done THEN
  UPDATE public.novu_rust_telegram_attempts SET retry_after_seconds=p_retry WHERE job_id=p_job AND environment_id=p_env;
 END IF;
 RETURN done;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_telegram_worker_finish_rate_limit(text,text,uuid,uuid,integer) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.novu_rust_telegram_attention(p_env text,p_after uuid) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE result jsonb;
BEGIN
 PERFORM public.novu_rust_telegram_operator_access(p_env,'view');
 WITH candidates AS MATERIALIZED (
  SELECT j.id,j.kind,j.status,j.attempts,a.attempt_id,a.state,a.http_status,a.retry_after_seconds,a.updated_at FROM public.novu_queue_jobs j
  JOIN public.novu_rust_telegram_attempts a ON a.job_id=j.id AND a.environment_id=j.environment_id
  WHERE j.environment_id=p_env AND j.kind IN ('experimental_telegram','experimental_telegram_workflow') AND j.status='failed' AND a.state IN ('unknown','rejected') AND (p_after IS NULL OR j.id>p_after)
  ORDER BY j.id LIMIT 51
 ), page AS MATERIALIZED (SELECT * FROM candidates ORDER BY id LIMIT 50)
 SELECT jsonb_build_object('data',COALESCE((SELECT jsonb_agg(jsonb_build_object('jobId',id,'jobKind',kind,'jobStatus',status,'attempts',attempts,'attemptId',attempt_id,'state',state,'httpStatus',http_status,'retryAfterSeconds',retry_after_seconds,'updatedAt',updated_at) ORDER BY id) FROM page),'[]'::jsonb),'hasMore',(SELECT count(*)>50 FROM candidates),'next',CASE WHEN (SELECT count(*)>50 FROM candidates) THEN (SELECT id FROM page ORDER BY id DESC LIMIT 1) ELSE NULL END) INTO result;
 RETURN result;
END; $$;
