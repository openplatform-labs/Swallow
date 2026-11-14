-- Invalidate current Twilio projection when new or newly linked evidence arrives.
CREATE OR REPLACE FUNCTION public.novu_rust_invalidate_twilio_insert()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
  UPDATE public.novu_queue_jobs j SET twilio_delivery_result='unknown'
  WHERE j.kind='experimental_twilio_sms' AND EXISTS (
    SELECT 1 FROM new_twilio_events e
    JOIN public.novu_rust_twilio_attempts a ON a.environment_id=e.environment_id AND a.job_id=e.job_id AND a.dispatch_id=e.dispatch_id
    WHERE j.id=a.job_id AND j.environment_id=a.environment_id );
  UPDATE public.novu_rust_sms_messages m SET twilio_delivery_result='unknown'
  WHERE m.twilio_delivery_result IS DISTINCT FROM 'unknown' AND EXISTS (
    SELECT 1 FROM new_twilio_events e
    JOIN public.novu_rust_twilio_attempts a ON a.environment_id=e.environment_id AND a.job_id=e.job_id AND a.dispatch_id=e.dispatch_id
    WHERE m.sms_job_id=a.job_id AND m.environment_id=a.environment_id );
  RETURN NULL;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_invalidate_twilio_insert() FROM PUBLIC;
DROP TRIGGER IF EXISTS novu_rust_twilio_projection_invalidate_insert ON public.novu_rust_twilio_delivery_events;
CREATE TRIGGER novu_rust_twilio_projection_invalidate_insert AFTER INSERT ON public.novu_rust_twilio_delivery_events
  REFERENCING NEW TABLE AS new_twilio_events FOR EACH STATEMENT EXECUTE FUNCTION public.novu_rust_invalidate_twilio_insert();
CREATE OR REPLACE FUNCTION public.novu_rust_invalidate_twilio_update()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
  UPDATE public.novu_queue_jobs j SET twilio_delivery_result='unknown'
  WHERE j.kind='experimental_twilio_sms' AND EXISTS (
    SELECT 1 FROM new_twilio_events e JOIN old_twilio_events previous ON (previous.environment_id,previous.account_sid,previous.message_sid,previous.status,previous.error_code)=(e.environment_id,e.account_sid,e.message_sid,e.status,e.error_code)
    JOIN public.novu_rust_twilio_attempts a ON a.environment_id=e.environment_id AND a.job_id=e.job_id AND a.dispatch_id=e.dispatch_id
    WHERE j.id=a.job_id AND j.environment_id=a.environment_id AND (previous.job_id IS DISTINCT FROM e.job_id OR previous.dispatch_id IS DISTINCT FROM e.dispatch_id));
  UPDATE public.novu_rust_sms_messages m SET twilio_delivery_result='unknown'
  WHERE m.twilio_delivery_result IS DISTINCT FROM 'unknown' AND EXISTS (
    SELECT 1 FROM new_twilio_events e JOIN old_twilio_events previous ON (previous.environment_id,previous.account_sid,previous.message_sid,previous.status,previous.error_code)=(e.environment_id,e.account_sid,e.message_sid,e.status,e.error_code)
    JOIN public.novu_rust_twilio_attempts a ON a.environment_id=e.environment_id AND a.job_id=e.job_id AND a.dispatch_id=e.dispatch_id
    WHERE m.sms_job_id=a.job_id AND m.environment_id=a.environment_id AND (previous.job_id IS DISTINCT FROM e.job_id OR previous.dispatch_id IS DISTINCT FROM e.dispatch_id));
  RETURN NULL;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_invalidate_twilio_update() FROM PUBLIC;
DROP TRIGGER IF EXISTS novu_rust_twilio_projection_invalidate_update ON public.novu_rust_twilio_delivery_events;
CREATE TRIGGER novu_rust_twilio_projection_invalidate_update AFTER UPDATE ON public.novu_rust_twilio_delivery_events
  REFERENCING OLD TABLE AS old_twilio_events NEW TABLE AS new_twilio_events FOR EACH STATEMENT EXECUTE FUNCTION public.novu_rust_invalidate_twilio_update();
