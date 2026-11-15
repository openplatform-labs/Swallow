-- Cooperative compatibility fence; privileged SQL can bypass this protection.
CREATE FUNCTION public.novu_rust_fcm_exponential_version_fence()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE policy text; lease uuid;
BEGIN
 IF NEW.state <> 'dispatching' THEN RETURN NEW; END IF;
 SELECT multicast_503_policy INTO policy FROM public.novu_rust_push_workflow_worker_config
 WHERE environment_id=NEW.environment_id FOR SHARE;
 IF policy='exponential' THEN
  SELECT lease_token INTO lease FROM public.novu_queue_jobs
   WHERE id=NEW.job_id AND environment_id=NEW.environment_id AND kind='experimental_fcm_push' AND status='running';
  IF lease IS NULL OR current_setting('swallow.fcm_exponential_fence',true) IS DISTINCT FROM NEW.job_id::text||':'||lease::text THEN
   RAISE EXCEPTION 'FCM exponential policy requires a compatible worker' USING ERRCODE='55000';
  END IF;
 END IF;
 RETURN NEW;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_fcm_exponential_version_fence() FROM PUBLIC;
CREATE TRIGGER novu_rust_fcm_exponential_version_fence BEFORE INSERT ON public.novu_rust_fcm_attempts
FOR EACH ROW EXECUTE FUNCTION public.novu_rust_fcm_exponential_version_fence();
