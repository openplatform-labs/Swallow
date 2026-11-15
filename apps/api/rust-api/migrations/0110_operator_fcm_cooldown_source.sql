CREATE OR REPLACE FUNCTION public.novu_rust_operator_jwt_fcm_cooldown_health(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_grace integer)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_result jsonb;
BEGIN
 IF p_grace IS NULL OR p_grace NOT BETWEEN 1 AND 86400 THEN RAISE EXCEPTION 'invalid threshold' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 PERFORM 1 FROM public.novu_rust_operator_jwt_fcm_cooldown_health_grants g
  WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env FOR SHARE OF g;
 IF NOT FOUND OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 SELECT jsonb_build_object('active',h.active,'notBefore',h.not_before,'fallbackSeconds',h.fallback_seconds,'deferred',h.deferred,'overdue',h.overdue,'deadlineSource',CASE WHEN h.active THEN p.deadline_source ELSE NULL END,'deadlineRecordedAt',CASE WHEN h.active AND p.deadline_recorded_at IS NOT NULL THEN p.deadline_recorded_at::text ELSE NULL END)
  INTO v_result FROM public.novu_rust_fcm_cooldown_health(p_env,p_grace) h LEFT JOIN public.novu_rust_fcm_cooldown_policies p ON p.environment_id=p_env;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN v_result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_fcm_cooldown_health(text,text,text,text,bigint,integer) FROM PUBLIC;
