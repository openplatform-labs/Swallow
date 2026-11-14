-- Preserve the deleted attempt's tag and refuse retry if matching callback
-- evidence is already committed. Both direct and scoped paths insert audit
-- under the job lock before deleting the attempt.
ALTER TABLE public.novu_rust_delivery_reconciliations
  ADD COLUMN IF NOT EXISTS retry_correlation_id uuid;
CREATE INDEX IF NOT EXISTS novu_rust_retry_correlation_lookup_idx
  ON public.novu_rust_delivery_reconciliations(environment_id,retry_correlation_id,job_id)
  WHERE decision='retry' AND target_index IS NULL AND retry_correlation_id IS NOT NULL;

CREATE OR REPLACE FUNCTION public.novu_rust_capture_retry_correlation()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_tag uuid;
BEGIN
  -- Do not allow a caller-supplied correlation to manufacture an audit link.
  NEW.retry_correlation_id := NULL;
  IF NEW.decision='retry' AND NEW.target_index IS NULL THEN
    SELECT a.correlation_id INTO v_tag
      FROM public.novu_queue_jobs j
      JOIN public.novu_rust_resend_attempts a
        ON a.job_id=j.id AND a.environment_id=j.environment_id
      WHERE j.id=NEW.job_id AND j.environment_id=NEW.environment_id
        AND j.kind='experimental_resend_email' AND a.state='unknown';
    IF v_tag IS NOT NULL THEN
      IF EXISTS(SELECT 1 FROM public.novu_rust_resend_delivery_events e
        WHERE e.environment_id=NEW.environment_id AND e.correlation_id=v_tag) THEN
        RAISE EXCEPTION 'Resend retry refused: authenticated tagged callback already exists' USING ERRCODE='22023';
      END IF;
      NEW.retry_correlation_id := v_tag;
    END IF;
  END IF;
  RETURN NEW;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_capture_retry_correlation() FROM PUBLIC;
DROP TRIGGER IF EXISTS novu_rust_retry_correlation_capture ON public.novu_rust_delivery_reconciliations;
CREATE TRIGGER novu_rust_retry_correlation_capture
  BEFORE INSERT ON public.novu_rust_delivery_reconciliations
  FOR EACH ROW EXECUTE FUNCTION public.novu_rust_capture_retry_correlation();
