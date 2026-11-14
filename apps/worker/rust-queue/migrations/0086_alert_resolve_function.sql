-- Permit audited alert resolution without direct access to alert tables.
CREATE OR REPLACE FUNCTION public.novu_rust_resolve_alert(
    p_environment text,p_channel text,p_target text,p_decision text,p_operator text,p_reason text
) RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_state text; v_attempt uuid; v_new_state text;
BEGIN
  IF p_environment IS NULL OR length(btrim(p_environment))=0
     OR p_channel IS NULL OR p_channel NOT IN ('slack','email')
     OR p_target IS NULL OR length(p_target)=0 OR octet_length(p_target)>64
     OR p_decision IS NULL OR p_decision NOT IN ('retry','accepted')
     OR p_operator IS NULL OR length(btrim(p_operator))=0 OR octet_length(p_operator)>128
     OR p_reason IS NULL OR length(btrim(p_reason))=0 OR octet_length(p_reason)>512 THEN
    RAISE EXCEPTION 'invalid alert resolution' USING ERRCODE='22023';
  END IF;
  IF NOT public.novu_rust_lock_operator_grant(p_environment,'alert_' || p_decision) THEN
    RAISE EXCEPTION 'alert resolution permission denied' USING ERRCODE='42501';
  END IF;
  SELECT s.state,s.attempt_id INTO v_state,v_attempt FROM public.novu_rust_alert_delivery_slots s
    WHERE s.environment_id=p_environment AND s.channel=p_channel AND s.target_id=p_target FOR UPDATE OF s;
  IF NOT FOUND THEN RAISE EXCEPTION 'alert destination not found' USING ERRCODE='22023'; END IF;
  IF v_state NOT IN ('unknown','rejected') OR (p_decision='accepted' AND v_state<>'unknown') OR v_attempt IS NULL THEN
    RAISE EXCEPTION 'alert outcome is not eligible for this decision' USING ERRCODE='22023';
  END IF;
  INSERT INTO public.novu_rust_alert_delivery_resolutions
    (id,environment_id,channel,target_id,attempt_id,previous_state,decision,operator_name,reason,database_user,database_session_user)
    VALUES (gen_random_uuid(),p_environment,p_channel,p_target,v_attempt,v_state,p_decision,p_operator,p_reason,session_user,session_user);
  v_new_state := CASE WHEN p_decision='retry' THEN 'idle' ELSE 'accepted' END;
  UPDATE public.novu_rust_alert_delivery_slots SET state=v_new_state,lease_until=NULL,
    last_started_at=CASE WHEN v_new_state='accepted' THEN clock_timestamp() ELSE last_started_at END
    WHERE environment_id=p_environment AND channel=p_channel AND target_id=p_target;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_resolve_alert(text,text,text,text,text,text) FROM PUBLIC;
