-- Bind new alert dispatch attempts to the DB session identity that reserved them.
ALTER TABLE public.novu_rust_alert_delivery_attempts ADD COLUMN IF NOT EXISTS sender_session_user text;
ALTER TABLE public.novu_rust_alert_delivery_attempts ADD COLUMN IF NOT EXISTS sender_oid oid;

CREATE OR REPLACE FUNCTION public.novu_rust_reserve_alert(
    p_environment text,p_channel text,p_target text,p_cooldown integer,p_condition_mask integer
) RETURNS TABLE(result text,attempt_id uuid) LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_state text; v_id uuid; v_expired boolean; v_cooling boolean; v_mask integer; v_sender oid;
BEGIN
  IF p_environment IS NULL OR length(btrim(p_environment))=0 OR p_channel IS NULL OR p_channel NOT IN ('slack','email')
     OR p_target IS NULL OR octet_length(p_target) NOT BETWEEN 1 AND 64 OR p_target !~ '^[A-Za-z0-9._-]+$'
     OR p_cooldown IS NULL OR p_cooldown NOT BETWEEN 60 AND 86400 OR (p_condition_mask IS NOT NULL AND p_condition_mask<=0) THEN
    RAISE EXCEPTION 'invalid alert destination or cooldown' USING ERRCODE='22023';
  END IF;
  IF NOT public.novu_rust_lock_operator_grant(p_environment,'alert_send') THEN
    RAISE EXCEPTION 'alert send permission denied' USING ERRCODE='42501';
  END IF;
  IF NOT EXISTS(SELECT 1 FROM public.novu_rust_environments e WHERE e.id=p_environment) THEN RAISE EXCEPTION 'environment not found' USING ERRCODE='22023'; END IF;
  SELECT oid INTO v_sender FROM pg_catalog.pg_roles WHERE rolname=session_user AND rolcanlogin;
  INSERT INTO public.novu_rust_alert_delivery_slots(environment_id,channel,target_id,state) VALUES(p_environment,p_channel,p_target,'idle') ON CONFLICT DO NOTHING;
  SELECT s.state,s.attempt_id,s.lease_until<=clock_timestamp(),s.last_started_at>clock_timestamp()-p_cooldown*interval '1 second',s.last_condition_mask
    INTO v_state,v_id,v_expired,v_cooling,v_mask FROM public.novu_rust_alert_delivery_slots s
    WHERE s.environment_id=p_environment AND s.channel=p_channel AND s.target_id=p_target FOR UPDATE OF s;
  IF v_state='dispatching' AND v_expired THEN
    UPDATE public.novu_rust_alert_delivery_attempts SET state='unknown',finished_at=clock_timestamp() WHERE id=v_id AND state='dispatching';
    UPDATE public.novu_rust_alert_delivery_slots SET state='unknown',lease_until=NULL WHERE environment_id=p_environment AND channel=p_channel AND target_id=p_target;
    RETURN QUERY SELECT 'blocked'::text,NULL::uuid; RETURN;
  ELSIF v_state='dispatching' THEN RETURN QUERY SELECT 'inflight'::text,NULL::uuid; RETURN;
  ELSIF v_state IN ('unknown','rejected') THEN RETURN QUERY SELECT 'blocked'::text,NULL::uuid; RETURN;
  ELSIF v_state='accepted' AND v_cooling AND (p_condition_mask IS NULL OR (p_condition_mask & ~coalesce(v_mask,0))=0) THEN
    RETURN QUERY SELECT 'cooldown'::text,NULL::uuid; RETURN;
  END IF;
  v_id:=gen_random_uuid();
  UPDATE public.novu_rust_alert_delivery_slots SET state='dispatching',attempt_id=v_id,last_started_at=clock_timestamp(),lease_until=clock_timestamp()+interval '60 seconds',last_condition_mask=CASE WHEN p_condition_mask IS NULL THEN last_condition_mask ELSE p_condition_mask END WHERE environment_id=p_environment AND channel=p_channel AND target_id=p_target;
  INSERT INTO public.novu_rust_alert_delivery_attempts(id,environment_id,channel,target_id,state,sender_session_user,sender_oid) VALUES(v_id,p_environment,p_channel,p_target,'dispatching',session_user,v_sender);
  RETURN QUERY SELECT 'claimed'::text,v_id;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_reserve_alert(text,text,text,integer,integer) FROM PUBLIC;

-- The original login may complete its own in-flight request even if alert_send
-- was revoked after reservation; this avoids discarding an already-known result.
CREATE OR REPLACE FUNCTION public.novu_rust_finish_alert(
    p_environment text,p_channel text,p_target text,p_attempt uuid,p_state text,p_http_status integer
) RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_changed integer;
BEGIN
  IF p_environment IS NULL OR p_channel IS NULL OR p_target IS NULL OR p_attempt IS NULL OR p_state IS NULL OR p_state NOT IN ('accepted','rejected','unknown')
     OR (p_http_status IS NOT NULL AND (p_state<>'rejected' OR p_http_status NOT BETWEEN 400 AND 499)) THEN
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
