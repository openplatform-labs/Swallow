-- The event which most recently extended the currently stored deadline.
ALTER TABLE public.novu_rust_fcm_cooldown_policies
 ADD COLUMN deadline_source integer CHECK (deadline_source IN (429,503)),
 ADD COLUMN deadline_recorded_at timestamptz;

CREATE OR REPLACE FUNCTION public.novu_rust_fcm_cooldown_record(p_env text,p_seconds integer) RETURNS text
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_until timestamptz; v_now timestamptz;
BEGIN
 IF p_seconds IS NOT NULL AND p_seconds NOT BETWEEN 0 AND 86400 THEN RAISE EXCEPTION 'invalid FCM cooldown' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_fcm_cooldown_access(p_env);
 PERFORM 1 FROM public.novu_rust_fcm_cooldown_policies WHERE environment_id=p_env FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'FCM cooldown policy missing' USING ERRCODE='42501'; END IF;
 v_now := clock_timestamp();
 UPDATE public.novu_rust_fcm_cooldown_policies SET
  deadline_source=CASE WHEN v_now+make_interval(secs=>greatest(60,coalesce(p_seconds,fallback_seconds))::double precision)>not_before THEN NULL ELSE deadline_source END,
  deadline_recorded_at=CASE WHEN v_now+make_interval(secs=>greatest(60,coalesce(p_seconds,fallback_seconds))::double precision)>not_before THEN v_now ELSE deadline_recorded_at END,
  not_before=greatest(not_before,v_now+make_interval(secs=>greatest(60,coalesce(p_seconds,fallback_seconds))::double precision))
 WHERE environment_id=p_env RETURNING not_before INTO v_until;
 IF NOT FOUND THEN RAISE EXCEPTION 'FCM cooldown policy missing' USING ERRCODE='42501'; END IF;
 RETURN v_until::text;
END; $$;

CREATE FUNCTION public.novu_rust_fcm_cooldown_record_429(p_env text,p_seconds integer) RETURNS text
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_until timestamptz; v_now timestamptz;
BEGIN
 IF p_seconds IS NOT NULL AND p_seconds NOT BETWEEN 0 AND 86400 THEN RAISE EXCEPTION 'invalid FCM cooldown' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_fcm_cooldown_access(p_env);
 PERFORM 1 FROM public.novu_rust_fcm_cooldown_policies WHERE environment_id=p_env FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'FCM cooldown policy missing' USING ERRCODE='42501'; END IF;
 v_now := clock_timestamp();
 UPDATE public.novu_rust_fcm_cooldown_policies SET
  deadline_source=CASE WHEN v_now+make_interval(secs=>greatest(60,coalesce(p_seconds,fallback_seconds))::double precision)>not_before THEN 429 ELSE deadline_source END,
  deadline_recorded_at=CASE WHEN v_now+make_interval(secs=>greatest(60,coalesce(p_seconds,fallback_seconds))::double precision)>not_before THEN v_now ELSE deadline_recorded_at END,
  not_before=greatest(not_before,v_now+make_interval(secs=>greatest(60,coalesce(p_seconds,fallback_seconds))::double precision))
 WHERE environment_id=p_env RETURNING not_before INTO v_until;
 IF NOT FOUND THEN RAISE EXCEPTION 'FCM cooldown policy missing' USING ERRCODE='42501'; END IF;
 RETURN v_until::text;
END; $$;

REVOKE ALL ON FUNCTION public.novu_rust_fcm_cooldown_record_429(text,integer) FROM PUBLIC;

CREATE FUNCTION public.novu_rust_fcm_cooldown_record_503(p_env text,p_seconds integer) RETURNS text
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_until timestamptz; v_now timestamptz;
BEGIN
 IF p_seconds IS NOT NULL AND p_seconds NOT BETWEEN 0 AND 86400 THEN RAISE EXCEPTION 'invalid FCM cooldown' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_fcm_cooldown_access(p_env);
 PERFORM 1 FROM public.novu_rust_fcm_cooldown_policies WHERE environment_id=p_env FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'FCM cooldown policy missing' USING ERRCODE='42501'; END IF;
 v_now := clock_timestamp();
 UPDATE public.novu_rust_fcm_cooldown_policies SET
  deadline_source=CASE WHEN v_now+make_interval(secs=>greatest(60,coalesce(p_seconds,fallback_seconds))::double precision)>not_before THEN 503 ELSE deadline_source END,
  deadline_recorded_at=CASE WHEN v_now+make_interval(secs=>greatest(60,coalesce(p_seconds,fallback_seconds))::double precision)>not_before THEN v_now ELSE deadline_recorded_at END,
  not_before=greatest(not_before,v_now+make_interval(secs=>greatest(60,coalesce(p_seconds,fallback_seconds))::double precision))
 WHERE environment_id=p_env RETURNING not_before INTO v_until;
 IF NOT FOUND THEN RAISE EXCEPTION 'FCM cooldown policy missing' USING ERRCODE='42501'; END IF;
 RETURN v_until::text;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_fcm_cooldown_record_503(text,integer) FROM PUBLIC;
