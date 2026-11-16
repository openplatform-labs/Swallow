-- Prevent a decision based on an old observation from resolving a later attempt.
CREATE OR REPLACE FUNCTION public.novu_rust_resolve_alert_v2(
 p_environment text,p_channel text,p_target text,p_attempt uuid,p_decision text,p_operator text,p_reason text
) RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_attempt uuid; v_state text;
BEGIN
 IF p_environment IS NULL OR length(btrim(p_environment))=0 OR p_channel IS NULL OR p_channel NOT IN ('slack','email')
 OR p_target IS NULL OR length(p_target)=0 OR octet_length(p_target)>64 OR p_attempt IS NULL
 OR p_decision IS NULL OR p_decision NOT IN ('retry','accepted')
 OR p_operator IS NULL OR length(btrim(p_operator))=0 OR octet_length(p_operator)>128
 OR p_reason IS NULL OR length(btrim(p_reason))=0 OR octet_length(p_reason)>512 THEN
  RAISE EXCEPTION 'invalid alert resolution' USING ERRCODE='22023';
 END IF;
 IF NOT public.novu_rust_lock_operator_grant(p_environment,'alert_' || p_decision) THEN
  RAISE EXCEPTION 'alert resolution permission denied' USING ERRCODE='42501';
 END IF;
 SELECT s.attempt_id,s.state INTO v_attempt,v_state FROM public.novu_rust_alert_delivery_slots s
 WHERE s.environment_id=p_environment AND s.channel=p_channel AND s.target_id=p_target FOR UPDATE OF s;
 IF NOT FOUND OR v_attempt IS DISTINCT FROM p_attempt THEN
  RAISE EXCEPTION 'alert attempt changed' USING ERRCODE='22023';
 END IF;
 -- Only the attempt-fenced API can assert acceptance for historically ambiguous 408/409.
 IF p_decision='accepted' AND v_state='rejected' AND EXISTS(
  SELECT 1 FROM public.novu_rust_alert_delivery_attempts a
  WHERE a.id=v_attempt AND a.environment_id=p_environment AND a.channel=p_channel AND a.target_id=p_target AND a.state='rejected' AND a.http_status IN (408,409)
 ) THEN
  INSERT INTO public.novu_rust_alert_delivery_resolutions(id,environment_id,channel,target_id,attempt_id,previous_state,decision,operator_name,reason,database_user,database_session_user)
   VALUES(gen_random_uuid(),p_environment,p_channel,p_target,v_attempt,v_state,p_decision,p_operator,p_reason,session_user,session_user);
  UPDATE public.novu_rust_alert_delivery_slots SET state='accepted',lease_until=NULL,last_started_at=clock_timestamp()
   WHERE environment_id=p_environment AND channel=p_channel AND target_id=p_target;
  RETURN;
 END IF;
 PERFORM public.novu_rust_resolve_alert(p_environment,p_channel,p_target,p_decision,p_operator,p_reason);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_resolve_alert_v2(text,text,text,uuid,text,text,text) FROM PUBLIC;
