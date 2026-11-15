-- Two signed 64-bit words, with sticky target version fencing.
ALTER TABLE public.novu_rust_alert_delivery_slots ADD COLUMN last_condition_mask_v3_high bigint;
CREATE TABLE public.novu_rust_alert_v3_targets (
 environment_id text NOT NULL REFERENCES public.novu_rust_environments(id) ON DELETE CASCADE,
 channel text NOT NULL CHECK(channel IN ('slack','email')),target_id text NOT NULL,
 PRIMARY KEY(environment_id,channel,target_id)
);
REVOKE ALL ON public.novu_rust_alert_v3_targets FROM PUBLIC;
CREATE OR REPLACE FUNCTION public.novu_rust_reserve_alert_v2(
    p_environment text,p_channel text,p_target text,p_cooldown integer,p_condition_mask bigint
) RETURNS TABLE(result text,attempt_id uuid) LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_state text; v_id uuid; v_expired boolean; v_cooling boolean; v_mask bigint; v_sender oid;
BEGIN
  IF p_environment IS NULL OR length(btrim(p_environment))=0 OR p_channel IS NULL OR p_channel NOT IN ('slack','email')
     OR p_target IS NULL OR octet_length(p_target) NOT BETWEEN 1 AND 64 OR p_target !~ '^[A-Za-z0-9._-]+$'
     OR p_cooldown IS NULL OR p_cooldown NOT BETWEEN 60 AND 86400 OR (p_condition_mask IS NOT NULL AND p_condition_mask=0) THEN
    RAISE EXCEPTION 'invalid alert destination or cooldown' USING ERRCODE='22023';
  END IF;
  IF NOT public.novu_rust_lock_operator_grant(p_environment,'alert_send') THEN
    RAISE EXCEPTION 'alert send permission denied' USING ERRCODE='42501';
  END IF;
  IF NOT EXISTS(SELECT 1 FROM public.novu_rust_environments e WHERE e.id=p_environment) THEN RAISE EXCEPTION 'environment not found' USING ERRCODE='22023'; END IF;
  SELECT oid INTO v_sender FROM pg_catalog.pg_roles WHERE rolname=session_user AND rolcanlogin;
  INSERT INTO public.novu_rust_alert_delivery_slots(environment_id,channel,target_id,state) VALUES(p_environment,p_channel,p_target,'idle') ON CONFLICT DO NOTHING;
  SELECT s.state,s.attempt_id,s.lease_until<=clock_timestamp(),s.last_started_at>clock_timestamp()-p_cooldown*interval '1 second',s.last_condition_mask_v2
    INTO v_state,v_id,v_expired,v_cooling,v_mask FROM public.novu_rust_alert_delivery_slots s
    WHERE s.environment_id=p_environment AND s.channel=p_channel AND s.target_id=p_target FOR UPDATE OF s;
  IF EXISTS(SELECT 1 FROM public.novu_rust_alert_v3_targets t WHERE t.environment_id=p_environment AND t.channel=p_channel AND t.target_id=p_target) THEN RAISE EXCEPTION 'alert target uses v3 conditions' USING ERRCODE='22023'; END IF;
  IF v_state='dispatching' AND v_expired THEN
    UPDATE public.novu_rust_alert_delivery_attempts SET state='unknown',finished_at=clock_timestamp() WHERE id=v_id AND state='dispatching';
    UPDATE public.novu_rust_alert_delivery_slots SET state='unknown',lease_until=NULL WHERE environment_id=p_environment AND channel=p_channel AND target_id=p_target;
    RETURN QUERY SELECT 'blocked'::text,NULL::uuid; RETURN;
  ELSIF v_state='dispatching' THEN RETURN QUERY SELECT 'inflight'::text,NULL::uuid; RETURN;
  ELSIF v_state IN ('unknown','rejected') THEN RETURN QUERY SELECT 'blocked'::text,NULL::uuid; RETURN;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM public.novu_rust_alert_v2_targets t WHERE t.environment_id=p_environment AND t.channel=p_channel AND t.target_id=p_target) THEN
    IF v_state<>'idle' OR EXISTS (SELECT 1 FROM public.novu_rust_alert_delivery_slots s WHERE s.environment_id=p_environment AND s.channel=p_channel AND s.target_id=p_target AND s.last_condition_mask IS NOT NULL) THEN
      RAISE EXCEPTION 'v2 requires an unused alert target' USING ERRCODE='22023';
    END IF;
    INSERT INTO public.novu_rust_alert_v2_targets(environment_id,channel,target_id) VALUES(p_environment,p_channel,p_target);
  END IF;
  IF v_state='accepted' AND v_cooling AND (p_condition_mask IS NULL OR (p_condition_mask & ~coalesce(v_mask,0))=0) THEN
    RETURN QUERY SELECT 'cooldown'::text,NULL::uuid; RETURN;
  END IF;
  v_id:=gen_random_uuid();
  UPDATE public.novu_rust_alert_delivery_slots SET state='dispatching',attempt_id=v_id,last_started_at=clock_timestamp(),lease_until=clock_timestamp()+interval '60 seconds',last_condition_mask_v2=CASE WHEN p_condition_mask IS NULL THEN last_condition_mask_v2 ELSE p_condition_mask END WHERE environment_id=p_environment AND channel=p_channel AND target_id=p_target;
  INSERT INTO public.novu_rust_alert_delivery_attempts(id,environment_id,channel,target_id,state,sender_session_user,sender_oid) VALUES(v_id,p_environment,p_channel,p_target,'dispatching',session_user,v_sender);
  RETURN QUERY SELECT 'claimed'::text,v_id;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_reserve_alert_v2(text,text,text,integer,bigint) FROM PUBLIC;


-- Remove confirmed recovered conditions from the last accepted cooldown mask.
-- Unknown or disabled checks are excluded from p_known by the caller.
CREATE OR REPLACE FUNCTION public.novu_rust_observe_alert_conditions_v2(
 p_environment text,p_channel text,p_target text,p_active bigint,p_known bigint
) RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF p_environment IS NULL OR length(btrim(p_environment))=0 OR p_channel IS NULL OR p_channel NOT IN ('slack','email')
 OR p_target IS NULL OR octet_length(p_target) NOT BETWEEN 1 AND 64 OR p_target !~ '^[A-Za-z0-9._-]+$'
 OR p_active IS NULL OR p_known IS NULL OR (p_active & ~p_known)<>0 THEN
   RAISE EXCEPTION 'invalid alert observation' USING ERRCODE='22023';
 END IF;
 IF NOT public.novu_rust_lock_operator_grant(p_environment,'alert_send') THEN RAISE EXCEPTION 'alert send permission denied' USING ERRCODE='42501'; END IF;
 IF NOT EXISTS(SELECT 1 FROM public.novu_rust_environments e WHERE e.id=p_environment) THEN RAISE EXCEPTION 'environment not found' USING ERRCODE='22023'; END IF;
 PERFORM 1 FROM public.novu_rust_alert_delivery_slots s WHERE s.environment_id=p_environment AND s.channel=p_channel AND s.target_id=p_target FOR UPDATE;
 IF EXISTS(SELECT 1 FROM public.novu_rust_alert_v3_targets t WHERE t.environment_id=p_environment AND t.channel=p_channel AND t.target_id=p_target) THEN RAISE EXCEPTION 'alert target uses v3 conditions' USING ERRCODE='22023'; END IF;
 UPDATE public.novu_rust_alert_delivery_slots s SET last_condition_mask_v2=s.last_condition_mask_v2 & (~p_known | p_active)
 WHERE s.environment_id=p_environment AND s.channel=p_channel AND s.target_id=p_target AND s.state='accepted' AND s.last_condition_mask_v2 IS NOT NULL;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_observe_alert_conditions_v2(text,text,text,bigint,bigint) FROM PUBLIC;

-- The low word reuses v2's bit assignments. First adoption preserves a v2
-- slot's low word; a legacy-only used target must use a new target ID.
CREATE FUNCTION public.novu_rust_reserve_alert_v3(p_environment text,p_channel text,p_target text,p_cooldown integer,p_low bigint,p_high bigint)
RETURNS TABLE(result text,attempt_id uuid) LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_state text;v_id uuid;v_expired boolean;v_cooling boolean;v_low bigint;v_high bigint;v_sender oid;
BEGIN
 IF p_environment IS NULL OR length(btrim(p_environment))=0 OR p_channel IS NULL OR p_channel NOT IN ('slack','email') OR p_target IS NULL OR octet_length(p_target) NOT BETWEEN 1 AND 64 OR p_target !~ '^[A-Za-z0-9._-]+$' OR p_cooldown IS NULL OR p_cooldown NOT BETWEEN 60 AND 86400 OR p_low IS NULL OR p_high IS NULL OR (p_low=0 AND p_high=0) THEN RAISE EXCEPTION 'invalid alert reservation' USING ERRCODE='22023'; END IF;
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
 UPDATE public.novu_rust_alert_delivery_slots SET state='dispatching',attempt_id=v_id,last_started_at=clock_timestamp(),lease_until=clock_timestamp()+interval '60 seconds',last_condition_mask_v2=p_low,last_condition_mask_v3_high=p_high WHERE environment_id=p_environment AND channel=p_channel AND target_id=p_target;
 INSERT INTO public.novu_rust_alert_delivery_attempts(id,environment_id,channel,target_id,state,sender_session_user,sender_oid) VALUES(v_id,p_environment,p_channel,p_target,'dispatching',session_user,v_sender);
 RETURN QUERY SELECT 'claimed'::text,v_id;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_reserve_alert_v3(text,text,text,integer,bigint,bigint) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_observe_alert_conditions_v3(p_environment text,p_channel text,p_target text,p_active_low bigint,p_known_low bigint,p_active_high bigint,p_known_high bigint)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF p_environment IS NULL OR length(btrim(p_environment))=0 OR p_channel IS NULL OR p_channel NOT IN ('slack','email') OR p_target IS NULL OR octet_length(p_target) NOT BETWEEN 1 AND 64 OR p_target !~ '^[A-Za-z0-9._-]+$' OR p_active_low IS NULL OR p_known_low IS NULL OR p_active_high IS NULL OR p_known_high IS NULL OR (p_active_low & ~p_known_low)<>0 OR (p_active_high & ~p_known_high)<>0 THEN RAISE EXCEPTION 'invalid alert observation' USING ERRCODE='22023'; END IF;
 IF NOT public.novu_rust_lock_operator_grant(p_environment,'alert_send') THEN RAISE EXCEPTION 'alert send permission denied' USING ERRCODE='42501'; END IF;
 IF NOT EXISTS(SELECT 1 FROM public.novu_rust_environments WHERE id=p_environment) THEN RAISE EXCEPTION 'environment not found' USING ERRCODE='22023'; END IF;
 PERFORM 1 FROM public.novu_rust_alert_delivery_slots s WHERE s.environment_id=p_environment AND s.channel=p_channel AND s.target_id=p_target FOR UPDATE;
 IF EXISTS(SELECT 1 FROM public.novu_rust_alert_delivery_slots s WHERE s.environment_id=p_environment AND s.channel=p_channel AND s.target_id=p_target AND (s.state<>'idle' OR s.last_condition_mask IS NOT NULL) AND NOT EXISTS(SELECT 1 FROM public.novu_rust_alert_v2_targets t WHERE t.environment_id=s.environment_id AND t.channel=s.channel AND t.target_id=s.target_id)) THEN RAISE EXCEPTION 'v3 requires an unused or v2 alert target' USING ERRCODE='22023'; END IF;
 -- A healthy observation must also adopt a quiescent v2 target; otherwise a
 -- recovered v2 condition could remain latched until the next reservation.
 IF EXISTS(SELECT 1 FROM public.novu_rust_alert_delivery_slots s JOIN public.novu_rust_alert_v2_targets t USING(environment_id,channel,target_id) WHERE s.environment_id=p_environment AND s.channel=p_channel AND s.target_id=p_target AND s.state='accepted' AND s.last_condition_mask_v2 IS NOT NULL) THEN
  INSERT INTO public.novu_rust_alert_v3_targets VALUES(p_environment,p_channel,p_target) ON CONFLICT DO NOTHING;
  UPDATE public.novu_rust_alert_delivery_slots SET last_condition_mask_v3_high=coalesce(last_condition_mask_v3_high,0) WHERE environment_id=p_environment AND channel=p_channel AND target_id=p_target;
 END IF;
 UPDATE public.novu_rust_alert_delivery_slots s SET last_condition_mask_v2=s.last_condition_mask_v2 & (~p_known_low | p_active_low),last_condition_mask_v3_high=s.last_condition_mask_v3_high & (~p_known_high | p_active_high)
 WHERE s.environment_id=p_environment AND s.channel=p_channel AND s.target_id=p_target AND s.state='accepted' AND EXISTS(SELECT 1 FROM public.novu_rust_alert_v3_targets t WHERE t.environment_id=s.environment_id AND t.channel=s.channel AND t.target_id=s.target_id);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_observe_alert_conditions_v3(text,text,text,bigint,bigint,bigint,bigint) FROM PUBLIC;
