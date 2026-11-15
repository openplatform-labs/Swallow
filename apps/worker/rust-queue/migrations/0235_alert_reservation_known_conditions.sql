CREATE FUNCTION public.novu_rust_reserve_alert_v3(p_environment text,p_channel text,p_target text,p_cooldown integer,p_low bigint,p_known_low bigint,p_high bigint,p_known_high bigint)
RETURNS TABLE(result text,attempt_id uuid) LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_state text;v_id uuid;v_expired boolean;v_cooling boolean;v_low bigint;v_high bigint;v_sender oid;
BEGIN
 IF p_environment IS NULL OR length(btrim(p_environment))=0 OR p_channel IS NULL OR p_channel NOT IN ('slack','email') OR p_target IS NULL OR octet_length(p_target) NOT BETWEEN 1 AND 64 OR p_target !~ '^[A-Za-z0-9._-]+$' OR p_cooldown IS NULL OR p_cooldown NOT BETWEEN 60 AND 86400 OR p_low IS NULL OR p_known_low IS NULL OR p_high IS NULL OR p_known_high IS NULL OR (p_low & ~p_known_low)<>0 OR (p_high & ~p_known_high)<>0 OR (p_low=0 AND p_high=0) THEN RAISE EXCEPTION 'invalid alert reservation' USING ERRCODE='22023'; END IF;
 IF NOT public.novu_rust_lock_operator_grant(p_environment,'alert_send') THEN RAISE EXCEPTION 'alert send permission denied' USING ERRCODE='42501'; END IF;
 IF NOT EXISTS(SELECT 1 FROM public.novu_rust_environments WHERE id=p_environment) THEN RAISE EXCEPTION 'environment not found' USING ERRCODE='22023'; END IF;
 SELECT oid INTO v_sender FROM pg_catalog.pg_roles WHERE rolname=session_user AND rolcanlogin;
 INSERT INTO public.novu_rust_alert_delivery_slots(environment_id,channel,target_id,state) VALUES(p_environment,p_channel,p_target,'idle') ON CONFLICT DO NOTHING;
 SELECT s.state,s.attempt_id,s.lease_until<=clock_timestamp(),s.last_started_at>clock_timestamp()-p_cooldown*interval '1 second',s.last_condition_mask_v2,s.last_condition_mask_v3_high INTO v_state,v_id,v_expired,v_cooling,v_low,v_high FROM public.novu_rust_alert_delivery_slots s WHERE s.environment_id=p_environment AND s.channel=p_channel AND s.target_id=p_target FOR UPDATE OF s;
 IF v_state='dispatching' AND v_expired THEN
  UPDATE public.novu_rust_alert_delivery_attempts SET state='unknown',finished_at=clock_timestamp() WHERE id=v_id AND state='dispatching';
  UPDATE public.novu_rust_alert_delivery_slots SET state='unknown',lease_until=NULL WHERE environment_id=p_environment AND channel=p_channel AND target_id=p_target;
  RETURN QUERY SELECT 'blocked'::text,NULL::uuid;RETURN;
 ELSIF v_state='dispatching' THEN RETURN QUERY SELECT 'inflight'::text,NULL::uuid;RETURN;
 ELSIF v_state IN ('unknown','rejected') THEN RETURN QUERY SELECT 'blocked'::text,NULL::uuid;RETURN; END IF;
 IF NOT EXISTS(SELECT 1 FROM public.novu_rust_alert_v3_targets WHERE environment_id=p_environment AND channel=p_channel AND target_id=p_target) THEN
  IF v_state='accepted' AND v_low IS NULL THEN RAISE EXCEPTION 'v3 cannot adopt target without v2 condition history' USING ERRCODE='22023'; END IF;
  IF NOT EXISTS(SELECT 1 FROM public.novu_rust_alert_v2_targets WHERE environment_id=p_environment AND channel=p_channel AND target_id=p_target) THEN
   IF v_state<>'idle' OR EXISTS(SELECT 1 FROM public.novu_rust_alert_delivery_slots WHERE environment_id=p_environment AND channel=p_channel AND target_id=p_target AND last_condition_mask IS NOT NULL) THEN RAISE EXCEPTION 'v3 requires an unused or v2 alert target' USING ERRCODE='22023'; END IF;
   INSERT INTO public.novu_rust_alert_v2_targets VALUES(p_environment,p_channel,p_target);
  END IF;
  INSERT INTO public.novu_rust_alert_v3_targets VALUES(p_environment,p_channel,p_target);
  v_high:=0;
 END IF;
 IF v_state='accepted' AND v_cooling AND (p_low & ~coalesce(v_low,0))=0 AND (p_high & ~coalesce(v_high,0))=0 THEN RETURN QUERY SELECT 'cooldown'::text,NULL::uuid;RETURN; END IF;
 v_id:=gen_random_uuid();
 UPDATE public.novu_rust_alert_delivery_slots SET state='dispatching',attempt_id=v_id,last_started_at=clock_timestamp(),lease_until=clock_timestamp()+interval '60 seconds',last_condition_mask_v2=(coalesce(v_low,0) & ~p_known_low) | p_low,last_condition_mask_v3_high=(coalesce(v_high,0) & ~p_known_high) | p_high WHERE environment_id=p_environment AND channel=p_channel AND target_id=p_target;
 INSERT INTO public.novu_rust_alert_delivery_attempts(id,environment_id,channel,target_id,state,sender_session_user,sender_oid) VALUES(v_id,p_environment,p_channel,p_target,'dispatching',session_user,v_sender);
 RETURN QUERY SELECT 'claimed'::text,v_id;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_reserve_alert_v3(text,text,text,integer,bigint,bigint,bigint,bigint) FROM PUBLIC;

-- Older senders cannot supply observation coverage. Reject them rather than
-- let a mixed deployment silently erase unknown-condition history.
CREATE OR REPLACE FUNCTION public.novu_rust_reserve_alert_v3(p_environment text,p_channel text,p_target text,p_cooldown integer,p_low bigint,p_high bigint)
RETURNS TABLE(result text,attempt_id uuid) LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 RAISE EXCEPTION 'v3 alert sender must be upgraded for migration 0235' USING ERRCODE='55000';
END; $$;
