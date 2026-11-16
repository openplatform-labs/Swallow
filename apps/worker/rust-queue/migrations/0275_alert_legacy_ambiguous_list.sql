-- Only currently blocked attempts are actionable; old resolved attempts remain in history.
CREATE FUNCTION public.novu_rust_alert_legacy_ambiguous(p_environment text,p_after_channel text,p_after_target text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE result jsonb;
BEGIN
 IF p_environment IS NULL OR length(btrim(p_environment))=0 OR
    ((p_after_channel IS NULL)<>(p_after_target IS NULL)) OR
    (p_after_channel IS NOT NULL AND (p_after_channel NOT IN ('slack','email') OR octet_length(p_after_target) NOT BETWEEN 1 AND 64 OR p_after_target !~ '^[A-Za-z0-9._-]+$')) THEN
  RAISE EXCEPTION 'invalid alert legacy cursor' USING ERRCODE='22023';
 END IF;
 IF NOT public.novu_rust_lock_operator_grant(p_environment,'alert_view') THEN RAISE EXCEPTION 'alert view permission denied' USING ERRCODE='42501'; END IF;
 IF NOT EXISTS(SELECT 1 FROM public.novu_rust_environments e WHERE e.id=p_environment) THEN RAISE EXCEPTION 'environment not found' USING ERRCODE='22023'; END IF;
 WITH candidates AS MATERIALIZED (
  SELECT s.channel,s.target_id,s.attempt_id,a.http_status,a.started_at
  FROM public.novu_rust_alert_delivery_slots s JOIN public.novu_rust_alert_delivery_attempts a
   ON a.id=s.attempt_id AND a.environment_id=s.environment_id AND a.channel=s.channel AND a.target_id=s.target_id
  WHERE s.environment_id=p_environment AND s.state='rejected' AND a.state='rejected' AND a.http_status IN (408,409)
   AND (p_after_channel IS NULL OR (s.channel COLLATE "C",s.target_id COLLATE "C")>(p_after_channel COLLATE "C",p_after_target COLLATE "C"))
  ORDER BY s.channel COLLATE "C",s.target_id COLLATE "C" LIMIT 51
 ), page AS MATERIALIZED (SELECT * FROM candidates ORDER BY channel COLLATE "C",target_id COLLATE "C" LIMIT 50)
 SELECT jsonb_build_object('data',COALESCE((SELECT jsonb_agg(jsonb_build_object('channel',channel,'targetId',target_id,'attemptId',attempt_id,'httpStatus',http_status,'startedAt',started_at::text) ORDER BY channel COLLATE "C",target_id COLLATE "C") FROM page),'[]'::jsonb),'hasMore',(SELECT count(*)>50 FROM candidates),'next',(SELECT CASE WHEN (SELECT count(*)>50 FROM candidates) THEN jsonb_build_object('channel',channel,'targetId',target_id) ELSE NULL END FROM page ORDER BY channel COLLATE "C" DESC,target_id COLLATE "C" DESC LIMIT 1)) INTO result;
 RETURN result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_alert_legacy_ambiguous(text,text,text) FROM PUBLIC;
