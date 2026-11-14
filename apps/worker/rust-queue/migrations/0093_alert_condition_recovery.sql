-- Remove confirmed recovered conditions from the last accepted cooldown mask.
-- Unknown or disabled checks are excluded from p_known by the caller.
CREATE OR REPLACE FUNCTION public.novu_rust_observe_alert_conditions(
 p_environment text,p_channel text,p_target text,p_active integer,p_known integer
) RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF p_environment IS NULL OR length(btrim(p_environment))=0 OR p_channel IS NULL OR p_channel NOT IN ('slack','email')
 OR p_target IS NULL OR octet_length(p_target) NOT BETWEEN 1 AND 64 OR p_target !~ '^[A-Za-z0-9._-]+$'
 OR p_active IS NULL OR p_known IS NULL OR p_active<0 OR p_known<0 OR (p_active & ~p_known)<>0 THEN
   RAISE EXCEPTION 'invalid alert observation' USING ERRCODE='22023';
 END IF;
 IF NOT public.novu_rust_lock_operator_grant(p_environment,'alert_send') THEN RAISE EXCEPTION 'alert send permission denied' USING ERRCODE='42501'; END IF;
 IF NOT EXISTS(SELECT 1 FROM public.novu_rust_environments e WHERE e.id=p_environment) THEN RAISE EXCEPTION 'environment not found' USING ERRCODE='22023'; END IF;
 UPDATE public.novu_rust_alert_delivery_slots s SET last_condition_mask=s.last_condition_mask & (~p_known | p_active)
 WHERE s.environment_id=p_environment AND s.channel=p_channel AND s.target_id=p_target AND s.state='accepted' AND s.last_condition_mask IS NOT NULL;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_observe_alert_conditions(text,text,text,integer,integer) FROM PUBLIC;
