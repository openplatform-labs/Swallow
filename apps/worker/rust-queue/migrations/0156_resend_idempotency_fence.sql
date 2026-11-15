-- Lock the policy through dispatch preparation and reject stale worker settings.
CREATE OR REPLACE FUNCTION public.novu_rust_check_resend_idempotency_scope(p_environment text, p_scope text)
RETURNS void LANGUAGE plpgsql SECURITY INVOKER SET search_path = pg_catalog, public AS $$
DECLARE v_scope text;
BEGIN
  SELECT scope INTO v_scope FROM public.novu_rust_resend_idempotency_policy
    WHERE environment_id=p_environment FOR SHARE;
  IF v_scope IS DISTINCT FROM p_scope THEN
    RAISE EXCEPTION 'Resend idempotency policy mismatch' USING ERRCODE = '55000';
  END IF;
END;
$$;
REVOKE ALL ON FUNCTION public.novu_rust_check_resend_idempotency_scope(text,text) FROM PUBLIC;
