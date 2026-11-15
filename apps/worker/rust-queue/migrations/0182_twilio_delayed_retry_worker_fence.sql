-- Block legacy dispatching attempts for jobs that require a late-callback check.
-- This is a cooperative version fence, not a defense against privileged SQL.
CREATE FUNCTION public.novu_rust_twilio_delayed_retry_fence()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE delayed boolean; lease uuid;
BEGIN
 IF NEW.state <> 'dispatching' THEN RETURN NEW; END IF;
 SELECT j.twilio_delayed_retry,j.lease_token INTO delayed,lease FROM public.novu_queue_jobs j
 WHERE j.id=NEW.job_id AND j.environment_id=NEW.environment_id AND j.kind='experimental_twilio_sms';
 IF coalesce(delayed,false) AND (lease IS NULL OR current_setting('swallow.twilio_delayed_retry_fence',true) IS DISTINCT FROM NEW.job_id::text||':'||lease::text) THEN
  RAISE EXCEPTION 'Twilio delayed retry requires a compatible worker' USING ERRCODE='55000';
 END IF;
 RETURN NEW;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_twilio_delayed_retry_fence() FROM PUBLIC;
CREATE TRIGGER novu_rust_twilio_delayed_retry_fence BEFORE INSERT ON public.novu_rust_twilio_attempts
FOR EACH ROW EXECUTE FUNCTION public.novu_rust_twilio_delayed_retry_fence();
