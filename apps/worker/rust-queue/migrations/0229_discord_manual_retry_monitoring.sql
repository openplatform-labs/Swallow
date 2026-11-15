-- Existing 429 retry diagnostics now include operator decisions. No automatic-retry budget is consumed.
ALTER TABLE public.novu_rust_discord_resolutions ADD COLUMN retry_destination_id text, ADD COLUMN retry_scheduled_delay_seconds integer, ADD COLUMN retry_scheduled_at timestamptz;
CREATE INDEX novu_rust_discord_resolution_retry_job_idx ON public.novu_rust_discord_resolutions(environment_id,job_id,created_at DESC) WHERE decision IN ('retry','retry_after') AND http_status=429 AND attempt_state='rejected';
CREATE OR REPLACE FUNCTION public.novu_rust_resolve_discord(p_env text,p_request uuid,p_job uuid,p_attempt_revision uuid,p_failure_revision bigint,p_decision text,p_reason text)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_actor oid;v_old public.novu_rust_discord_resolutions%ROWTYPE;v_job public.novu_queue_jobs%ROWTYPE;v_attempt public.novu_rust_discord_attempts%ROWTYPE;v_inserted boolean;v_has_attempt boolean;v_not_before timestamptz;v_scheduled_at timestamptz;v_available_at timestamptz;v_destination text;
BEGIN
 IF p_env IS NULL OR length(btrim(p_env))=0 OR p_request IS NULL OR p_job IS NULL OR p_failure_revision IS NULL OR p_failure_revision<0 OR p_decision IS NULL OR p_decision NOT IN ('accepted','retry','retry_after') OR (p_attempt_revision IS NULL AND p_decision<>'retry') OR p_reason IS NULL OR octet_length(btrim(p_reason)) NOT BETWEEN 1 AND 512 THEN RAISE EXCEPTION 'invalid Discord resolution' USING ERRCODE='22023'; END IF;
 SELECT r.oid INTO v_actor FROM public.novu_rust_discord_resolvers g JOIN pg_catalog.pg_roles r ON r.oid=g.principal_oid AND r.rolname=g.database_principal AND r.rolcanlogin
 WHERE g.environment_id=p_env AND g.database_principal=session_user AND CASE WHEN p_decision='accepted' THEN g.can_accept ELSE g.can_retry END FOR SHARE OF g;
 IF NOT FOUND THEN RAISE EXCEPTION 'Discord resolution permission denied' USING ERRCODE='42501'; END IF;
 INSERT INTO public.novu_rust_discord_resolutions(environment_id,actor,actor_oid,request_id,job_id,attempt_revision,failure_revision,decision,reason)
 VALUES(p_env,session_user,v_actor,p_request,p_job,p_attempt_revision,p_failure_revision,p_decision,p_reason) ON CONFLICT DO NOTHING;
 v_inserted:=FOUND;
 IF NOT v_inserted THEN
  SELECT * INTO v_old FROM public.novu_rust_discord_resolutions WHERE environment_id=p_env AND actor_oid=v_actor AND request_id=p_request;
  IF (v_old.actor,v_old.job_id,v_old.attempt_revision,v_old.failure_revision,v_old.decision,v_old.reason) IS DISTINCT FROM (session_user,p_job,p_attempt_revision,p_failure_revision,p_decision,p_reason) THEN RAISE EXCEPTION 'Discord request conflict' USING ERRCODE='22023'; END IF;
  RETURN false;
 END IF;
 SELECT * INTO v_job FROM public.novu_queue_jobs WHERE environment_id=p_env AND id=p_job AND kind IN ('experimental_discord_webhook','experimental_discord_workflow') FOR UPDATE;
 IF NOT FOUND OR v_job.status<>'failed' OR v_job.failure_revision<>p_failure_revision THEN RAISE EXCEPTION 'Discord job changed or unavailable' USING ERRCODE='22023'; END IF;
 SELECT * INTO v_attempt FROM public.novu_rust_discord_attempts WHERE environment_id=p_env AND job_id=p_job FOR UPDATE;v_has_attempt:=FOUND;
 IF (p_attempt_revision IS NULL AND v_has_attempt) OR (p_attempt_revision IS NOT NULL AND (NOT v_has_attempt OR v_attempt.revision IS DISTINCT FROM p_attempt_revision OR v_attempt.state NOT IN ('unknown','rejected'))) OR (p_decision='accepted' AND v_attempt.state IS DISTINCT FROM 'unknown') THEN RAISE EXCEPTION 'Discord attempt changed or unavailable' USING ERRCODE='22023'; END IF;
 IF p_decision='retry_after' THEN
  IF NOT v_has_attempt OR v_attempt.state<>'rejected' OR v_attempt.http_status IS DISTINCT FROM 429 OR v_attempt.retry_after_seconds IS NULL OR v_attempt.retry_after_seconds NOT BETWEEN 0 AND 86400 THEN RAISE EXCEPTION 'Discord retry guidance unavailable' USING ERRCODE='22023'; END IF;
  v_not_before:=v_attempt.updated_at+make_interval(secs=>v_attempt.retry_after_seconds::double precision);
 END IF;
 UPDATE public.novu_rust_discord_resolutions SET attempt_state=v_attempt.state,http_status=v_attempt.http_status,retry_after_seconds=v_attempt.retry_after_seconds,retry_not_before=v_not_before WHERE environment_id=p_env AND actor_oid=v_actor AND request_id=p_request;
 IF p_decision='accepted' THEN
  UPDATE public.novu_rust_discord_attempts SET state='accepted',updated_at=clock_timestamp() WHERE job_id=p_job AND environment_id=p_env;
  UPDATE public.novu_queue_jobs SET status='completed',last_error=NULL,lease_token=NULL,lease_until=NULL WHERE environment_id=p_env AND id=p_job;
 ELSE
  DELETE FROM public.novu_rust_discord_attempts WHERE job_id=p_job AND environment_id=p_env;
  v_scheduled_at:=clock_timestamp();
  v_available_at:=CASE WHEN p_decision='retry_after' THEN greatest(v_scheduled_at,v_not_before) ELSE v_scheduled_at END;
  UPDATE public.novu_queue_jobs SET status='pending',available_at=v_available_at,last_error=NULL,lease_token=NULL,lease_until=NULL WHERE environment_id=p_env AND id=p_job;
  IF v_attempt.state='rejected' AND v_attempt.http_status=429 THEN
   v_destination:=CASE WHEN v_job.payload->>'destination' ~ '^[A-Za-z0-9._-]{1,64}$' THEN v_job.payload->>'destination' ELSE NULL END;
   UPDATE public.novu_rust_discord_resolutions SET retry_destination_id=v_destination,retry_scheduled_at=v_scheduled_at,retry_scheduled_delay_seconds=greatest(0,least(86400,ceil(extract(epoch FROM v_available_at-v_scheduled_at))::integer)) WHERE environment_id=p_env AND actor_oid=v_actor AND request_id=p_request;
  END IF;
 END IF;
 RETURN true;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_resolve_discord(text,uuid,uuid,uuid,bigint,text,text) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.novu_rust_discord_retry_health(p_env text,p_grace integer)
RETURNS TABLE(pending bigint,overdue bigint)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF p_grace IS NULL OR p_grace NOT BETWEEN 1 AND 86400 THEN RAISE EXCEPTION 'invalid retry grace' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_assert_alert_view(p_env);
 RETURN QUERY SELECT count(*),count(*) FILTER (WHERE j.available_at<=statement_timestamp()-make_interval(secs=>p_grace::double precision))
 FROM public.novu_queue_jobs j
 WHERE j.environment_id=p_env AND j.kind IN ('experimental_discord_webhook','experimental_discord_workflow') AND j.status='pending'
 AND (EXISTS(SELECT 1 FROM public.novu_rust_discord_retry_history h WHERE h.job_id=j.id AND h.environment_id=p_env)
   OR EXISTS(SELECT 1 FROM public.novu_rust_discord_resolutions r WHERE r.environment_id=p_env AND r.job_id=j.id AND r.decision IN ('retry','retry_after') AND r.attempt_state='rejected' AND r.http_status=429));
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_discord_retry_health(text,integer) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.novu_rust_discord_retry_list(p_env text,p_grace integer,p_overdue_only boolean,p_after uuid)
RETURNS TABLE(job_id uuid,kind text,destination_id text,available_at text,attempts integer,retry_count bigint,last_delay_seconds integer,last_retry_after_seconds integer,overdue boolean)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF p_grace IS NULL OR p_grace NOT BETWEEN 1 AND 86400 OR p_overdue_only IS NULL THEN RAISE EXCEPTION 'invalid retry list request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_assert_alert_view(p_env);
 RETURN QUERY SELECT j.id,j.kind,h.destination_id,j.available_at::text,j.attempts,h.retry_count,h.delay_seconds,h.retry_after_seconds,
  j.available_at<=statement_timestamp()-make_interval(secs=>p_grace::double precision)
 FROM public.novu_queue_jobs j JOIN LATERAL (
  SELECT count(*) OVER () AS retry_count,e.destination_id,e.delay_seconds,e.retry_after_seconds FROM (
   SELECT r.destination_id,r.scheduled_delay_seconds AS delay_seconds,r.retry_after_seconds,r.recorded_at AS event_at,'auto:'||lpad(r.id::text,20,'0') AS event_id
   FROM public.novu_rust_discord_retry_history r WHERE r.job_id=j.id AND r.environment_id=p_env
   UNION ALL
   SELECT coalesce(r.retry_destination_id,'unknown'),coalesce(r.retry_scheduled_delay_seconds,greatest(0,least(86400,ceil(extract(epoch FROM coalesce(r.retry_not_before,r.created_at)-r.created_at))::integer))),r.retry_after_seconds,coalesce(r.retry_scheduled_at,r.created_at),'manual:'||r.actor_oid::text||':'||r.request_id::text
   FROM public.novu_rust_discord_resolutions r WHERE r.environment_id=p_env AND r.job_id=j.id AND r.decision IN ('retry','retry_after') AND r.attempt_state='rejected' AND r.http_status=429
  ) e ORDER BY e.event_at DESC,e.event_id DESC LIMIT 1
 ) h ON true
 WHERE j.environment_id=p_env AND j.kind IN ('experimental_discord_webhook','experimental_discord_workflow') AND j.status='pending'
 AND (p_after IS NULL OR j.id>p_after)
 AND (NOT p_overdue_only OR j.available_at<=statement_timestamp()-make_interval(secs=>p_grace::double precision))
 ORDER BY j.id LIMIT 101;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_discord_retry_list(text,integer,boolean,uuid) FROM PUBLIC;
