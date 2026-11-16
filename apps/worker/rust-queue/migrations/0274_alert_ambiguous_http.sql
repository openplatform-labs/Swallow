-- Ambiguous HTTP statuses must never be recorded as definitive rejection.
CREATE OR REPLACE FUNCTION public.novu_rust_finish_alert(
    p_environment text,p_channel text,p_target text,p_attempt uuid,p_state text,p_http_status integer
) RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_changed integer;
BEGIN
  IF p_environment IS NULL OR p_channel IS NULL OR p_target IS NULL OR p_attempt IS NULL OR p_state IS NULL OR p_state NOT IN ('accepted','rejected','unknown')
     OR (p_http_status IS NOT NULL AND (p_state<>'rejected' OR (p_http_status NOT BETWEEN 400 AND 499 OR p_http_status IN (408,409)))) THEN
    RAISE EXCEPTION 'invalid alert outcome' USING ERRCODE='22023';
  END IF;
  UPDATE public.novu_rust_alert_delivery_slots s SET state=p_state,lease_until=NULL
    WHERE s.environment_id=p_environment AND s.channel=p_channel AND s.target_id=p_target AND s.attempt_id=p_attempt
      AND s.state='dispatching' AND s.lease_until>clock_timestamp()
      AND EXISTS(SELECT 1 FROM public.novu_rust_alert_delivery_attempts a JOIN pg_catalog.pg_roles r ON r.oid=a.sender_oid AND r.rolname=session_user AND r.rolcanlogin
        WHERE a.id=p_attempt AND a.environment_id=p_environment AND a.channel=p_channel AND a.target_id=p_target AND a.state='dispatching' AND a.sender_session_user=session_user);
  GET DIAGNOSTICS v_changed=ROW_COUNT;
  IF v_changed=1 THEN
    UPDATE public.novu_rust_alert_delivery_attempts SET state=p_state,http_status=p_http_status,finished_at=clock_timestamp() WHERE id=p_attempt AND state='dispatching';
    IF NOT FOUND THEN RAISE EXCEPTION 'alert attempt disappeared' USING ERRCODE='40001'; END IF;
  END IF;
  RETURN v_changed=1;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_finish_alert(text,text,text,uuid,text,integer) FROM PUBLIC;
