-- Sandbox success is a provider validation, never a delivered email.
ALTER TABLE public.novu_rust_sendgrid_attempts DROP CONSTRAINT IF EXISTS novu_rust_sendgrid_attempts_state_check;
ALTER TABLE public.novu_rust_sendgrid_attempts ADD CONSTRAINT novu_rust_sendgrid_attempts_state_check CHECK (state IN ('dispatching','accepted','rejected','unknown','validated'));
ALTER TABLE public.novu_rust_email_messages DROP CONSTRAINT IF EXISTS novu_rust_email_delivery_state_check;
ALTER TABLE public.novu_rust_email_messages ADD CONSTRAINT novu_rust_email_delivery_state_check CHECK (delivery_state IN ('not_attempted','dispatching','in_progress','accepted','rejected','unknown','validated'));
CREATE FUNCTION public.novu_rust_sendgrid_sandbox_guard() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE sandbox boolean;
BEGIN
  SELECT (payload->'sandbox_mode' = 'true'::jsonb) INTO sandbox FROM public.novu_queue_jobs WHERE id=NEW.job_id AND environment_id=NEW.environment_id;
  IF coalesce(sandbox,false) AND (NEW.state='accepted' OR NEW.dispatch_id IS NOT NULL) THEN
    RAISE EXCEPTION 'sandbox cannot be accepted for delivery or tracked as a dispatch' USING ERRCODE='23514';
  END IF;
  IF NEW.state='validated' AND (sandbox IS DISTINCT FROM true OR NEW.http_status IS DISTINCT FROM 200 OR NEW.dispatch_id IS NOT NULL) THEN
    RAISE EXCEPTION 'invalid sandbox validation' USING ERRCODE='23514';
  END IF;
  RETURN NEW;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_sendgrid_sandbox_guard() FROM PUBLIC;
CREATE TRIGGER novu_rust_sendgrid_sandbox_guard BEFORE INSERT OR UPDATE ON public.novu_rust_sendgrid_attempts FOR EACH ROW EXECUTE FUNCTION public.novu_rust_sendgrid_sandbox_guard();
CREATE FUNCTION public.novu_rust_sendgrid_sandbox_payload_guard() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
  IF OLD.kind='experimental_sendgrid_email' AND NEW.payload->'sandbox_mode' IS DISTINCT FROM OLD.payload->'sandbox_mode'
     AND EXISTS (SELECT 1 FROM public.novu_rust_sendgrid_attempts a WHERE a.job_id=OLD.id AND a.environment_id=OLD.environment_id) THEN
    RAISE EXCEPTION 'cannot change sandbox mode after an attempt' USING ERRCODE='23514';
  END IF;
  RETURN NEW;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_sendgrid_sandbox_payload_guard() FROM PUBLIC;
CREATE TRIGGER novu_rust_sendgrid_sandbox_payload_guard BEFORE UPDATE OF payload ON public.novu_queue_jobs FOR EACH ROW EXECUTE FUNCTION public.novu_rust_sendgrid_sandbox_payload_guard();
-- Preserve existing grants while refusing inconsistent delivery projections.
CREATE OR REPLACE FUNCTION public.novu_rust_sendgrid_health(p_environment text,p_grace integer,p_heartbeat integer)
RETURNS TABLE(overdue bigint,eligible bigint,last_projection text,stale boolean)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
  IF p_grace IS NULL OR p_grace NOT BETWEEN 1 AND 86400 OR p_heartbeat IS NULL OR p_heartbeat NOT BETWEEN 1 AND 86400 THEN RAISE EXCEPTION 'invalid threshold' USING ERRCODE='22023'; END IF;
  PERFORM public.novu_rust_assert_alert_view(p_environment);
  RETURN QUERY SELECT count(*) FILTER(WHERE (j.sendgrid_delivery_result IS DISTINCT FROM 'delivered' OR NOT EXISTS (SELECT 1 FROM public.novu_rust_sendgrid_attempts a WHERE a.environment_id=j.environment_id AND a.job_id=j.id AND a.state='accepted' AND a.dispatch_id IS NOT NULL AND EXISTS (SELECT 1 FROM public.novu_rust_sendgrid_recipients r WHERE r.environment_id=a.environment_id AND r.dispatch_id=a.dispatch_id) AND NOT EXISTS (SELECT 1 FROM public.novu_rust_sendgrid_recipients r WHERE r.environment_id=a.environment_id AND r.dispatch_id=a.dispatch_id AND r.delivery_result IS DISTINCT FROM 'delivered')))),count(*),
    (SELECT h.last_completed_at::text FROM public.novu_rust_sendgrid_projection_heartbeat h WHERE h.environment_id=p_environment),
    coalesce((SELECT h.last_completed_at<statement_timestamp()-p_heartbeat*interval '1 second' FROM public.novu_rust_sendgrid_projection_heartbeat h WHERE h.environment_id=p_environment),true)
    FROM public.novu_queue_jobs j WHERE j.environment_id=p_environment AND j.kind='experimental_sendgrid_email' AND j.status<>'cancelled' AND NOT EXISTS (SELECT 1 FROM public.novu_rust_sendgrid_attempts a WHERE a.environment_id=j.environment_id AND a.job_id=j.id AND a.state='validated') AND j.available_at<=statement_timestamp()-p_grace*interval '1 second';
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_sendgrid_health(text,integer,integer) FROM PUBLIC;
