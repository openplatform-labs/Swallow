-- Expose only the active-key count to authorized environment operators.
CREATE OR REPLACE FUNCTION public.novu_rust_sendgrid_webhook_key_count(p_environment text)
RETURNS bigint LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE n bigint;
BEGIN
  IF p_environment IS NULL OR length(btrim(p_environment))=0 THEN
    RAISE EXCEPTION 'environment required' USING ERRCODE='22023';
  END IF;
  IF NOT public.novu_rust_lock_operator_grant(p_environment,'alert_view') THEN
    RAISE EXCEPTION 'alert view permission denied' USING ERRCODE='42501';
  END IF;
  IF NOT EXISTS(SELECT 1 FROM public.novu_rust_environments e WHERE e.id=p_environment) THEN
    RAISE EXCEPTION 'environment not found' USING ERRCODE='22023';
  END IF;
  SELECT count(*) INTO n FROM public.novu_rust_sendgrid_webhook_keys k
    WHERE k.environment_id=p_environment AND k.active;
  RETURN n;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_sendgrid_webhook_key_count(text) FROM PUBLIC;
