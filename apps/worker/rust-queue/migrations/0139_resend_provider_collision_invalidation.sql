-- Invalidate already auto-correlated projections when another attempt or a
-- retry audit begins claiming the same provider ID in the same environment.
CREATE OR REPLACE FUNCTION public.novu_rust_invalidate_auto_resend_provider(
  p_environment text,p_provider_id text,p_source_job uuid,p_exclude_source boolean)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
  IF p_provider_id IS NULL OR p_provider_id='' THEN RETURN; END IF;
  PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('resend-provider/'||length(p_environment)::text||'/'||p_environment||'/'||p_provider_id,0));
  PERFORM j.id FROM public.novu_queue_jobs j JOIN public.novu_rust_resend_auto_correlations c
    ON c.job_id=j.id AND c.environment_id=j.environment_id
    WHERE c.environment_id=p_environment AND c.provider_message_id=p_provider_id
      AND (NOT p_exclude_source OR c.job_id<>p_source_job)
    ORDER BY j.id FOR UPDATE OF j;
  UPDATE public.novu_queue_jobs j SET resend_delivery_result='unknown'
    FROM public.novu_rust_resend_auto_correlations c
    WHERE c.job_id=j.id AND c.environment_id=j.environment_id
      AND c.environment_id=p_environment AND c.provider_message_id=p_provider_id
      AND (NOT p_exclude_source OR c.job_id<>p_source_job);
  UPDATE public.novu_rust_email_messages m SET resend_delivery_result='unknown'
    FROM public.novu_rust_resend_auto_correlations c
    WHERE c.job_id=m.email_job_id AND c.environment_id=m.environment_id
      AND c.environment_id=p_environment AND c.provider_message_id=p_provider_id
      AND (NOT p_exclude_source OR c.job_id<>p_source_job);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_invalidate_auto_resend_provider(text,text,uuid,boolean) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.novu_rust_auto_resend_attempt_collision()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
  IF NEW.provider_message_id IS NOT NULL AND NEW.provider_message_id<>'' THEN
    PERFORM public.novu_rust_invalidate_auto_resend_provider(NEW.environment_id,NEW.provider_message_id,NEW.job_id,true);
  END IF;
  RETURN NULL;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_auto_resend_attempt_collision() FROM PUBLIC;
DROP TRIGGER IF EXISTS novu_rust_auto_resend_attempt_collision ON public.novu_rust_resend_attempts;
CREATE TRIGGER novu_rust_auto_resend_attempt_collision AFTER INSERT OR UPDATE OF provider_message_id,environment_id,job_id
  ON public.novu_rust_resend_attempts FOR EACH ROW EXECUTE FUNCTION public.novu_rust_auto_resend_attempt_collision();

CREATE OR REPLACE FUNCTION public.novu_rust_auto_resend_retry_collision()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
  IF NEW.decision='retry' AND NEW.target_index IS NULL AND NEW.retry_provider_message_id IS NOT NULL AND NEW.retry_provider_message_id<>'' THEN
    PERFORM public.novu_rust_invalidate_auto_resend_provider(NEW.environment_id,NEW.retry_provider_message_id,NEW.job_id,false);
  END IF;
  RETURN NULL;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_auto_resend_retry_collision() FROM PUBLIC;
DROP TRIGGER IF EXISTS novu_rust_auto_resend_retry_collision ON public.novu_rust_delivery_reconciliations;
CREATE TRIGGER novu_rust_auto_resend_retry_collision AFTER INSERT OR UPDATE OF retry_provider_message_id,environment_id,job_id,decision,target_index
  ON public.novu_rust_delivery_reconciliations FOR EACH ROW EXECUTE FUNCTION public.novu_rust_auto_resend_retry_collision();

-- Serialize each affected tag and provider ID before looking up auto audits.
-- This includes events arriving before an auto-correlation audit commits.
CREATE OR REPLACE FUNCTION public.novu_rust_invalidate_auto_resend_insert()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_env text; v_tag uuid; v_id text;
BEGIN
  FOR v_env,v_tag IN SELECT DISTINCT e.environment_id,e.correlation_id FROM inserted_auto_resend_events e WHERE e.correlation_id IS NOT NULL ORDER BY 1,2 LOOP
    PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('resend-tag/'||length(v_env)::text||'/'||v_env||'/'||v_tag::text,0));
  END LOOP;
  FOR v_env,v_id IN SELECT DISTINCT e.environment_id,e.email_id FROM inserted_auto_resend_events e ORDER BY 1,2 LOOP
    PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('resend-provider/'||length(v_env)::text||'/'||v_env||'/'||v_id,0));
  END LOOP;
  PERFORM j.id FROM public.novu_queue_jobs j WHERE EXISTS (
    SELECT 1 FROM public.novu_rust_resend_auto_correlations c JOIN inserted_auto_resend_events e
      ON e.environment_id=c.environment_id AND (e.email_id=c.provider_message_id OR e.correlation_id=c.correlation_id)
    WHERE c.job_id=j.id AND c.environment_id=j.environment_id) ORDER BY j.id FOR UPDATE;
  UPDATE public.novu_queue_jobs j SET resend_delivery_result='unknown' WHERE EXISTS (
    SELECT 1 FROM public.novu_rust_resend_auto_correlations c JOIN inserted_auto_resend_events e
      ON e.environment_id=c.environment_id AND (e.email_id=c.provider_message_id OR e.correlation_id=c.correlation_id)
    WHERE c.job_id=j.id AND c.environment_id=j.environment_id);
  UPDATE public.novu_rust_email_messages m SET resend_delivery_result='unknown' WHERE EXISTS (
    SELECT 1 FROM public.novu_rust_resend_auto_correlations c JOIN inserted_auto_resend_events e
      ON e.environment_id=c.environment_id AND (e.email_id=c.provider_message_id OR e.correlation_id=c.correlation_id)
    WHERE c.job_id=m.email_job_id AND c.environment_id=m.environment_id);
  RETURN NULL;
END; $$;

CREATE OR REPLACE FUNCTION public.novu_rust_invalidate_auto_resend_delete()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_env text; v_tag uuid; v_id text;
BEGIN
  FOR v_env,v_tag IN SELECT DISTINCT e.environment_id,e.correlation_id FROM deleted_auto_resend_events e WHERE e.correlation_id IS NOT NULL ORDER BY 1,2 LOOP
    PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('resend-tag/'||length(v_env)::text||'/'||v_env||'/'||v_tag::text,0));
  END LOOP;
  FOR v_env,v_id IN SELECT DISTINCT e.environment_id,e.email_id FROM deleted_auto_resend_events e ORDER BY 1,2 LOOP
    PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('resend-provider/'||length(v_env)::text||'/'||v_env||'/'||v_id,0));
  END LOOP;
  PERFORM j.id FROM public.novu_queue_jobs j WHERE EXISTS (
    SELECT 1 FROM public.novu_rust_resend_auto_correlations c JOIN deleted_auto_resend_events e
      ON e.environment_id=c.environment_id AND (e.email_id=c.provider_message_id OR e.correlation_id=c.correlation_id)
    WHERE c.job_id=j.id AND c.environment_id=j.environment_id) ORDER BY j.id FOR UPDATE;
  UPDATE public.novu_queue_jobs j SET resend_delivery_result='unknown' WHERE EXISTS (
    SELECT 1 FROM public.novu_rust_resend_auto_correlations c JOIN deleted_auto_resend_events e
      ON e.environment_id=c.environment_id AND (e.email_id=c.provider_message_id OR e.correlation_id=c.correlation_id)
    WHERE c.job_id=j.id AND c.environment_id=j.environment_id);
  UPDATE public.novu_rust_email_messages m SET resend_delivery_result='unknown' WHERE EXISTS (
    SELECT 1 FROM public.novu_rust_resend_auto_correlations c JOIN deleted_auto_resend_events e
      ON e.environment_id=c.environment_id AND (e.email_id=c.provider_message_id OR e.correlation_id=c.correlation_id)
    WHERE c.job_id=m.email_job_id AND c.environment_id=m.environment_id);
  RETURN NULL;
END; $$;
