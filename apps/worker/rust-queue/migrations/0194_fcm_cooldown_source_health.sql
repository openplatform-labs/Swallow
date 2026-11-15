-- Optional detailed monitor, independently granted to alert senders.
CREATE FUNCTION public.novu_rust_fcm_cooldown_health_detail(p_env text,p_grace integer)
RETURNS TABLE(active boolean,not_before text,fallback_seconds integer,deferred bigint,overdue bigint,deadline_source integer)
LANGUAGE sql SECURITY DEFINER SET search_path=pg_catalog AS $$
 SELECT h.active,h.not_before,h.fallback_seconds,h.deferred,h.overdue,
        CASE WHEN h.active THEN p.deadline_source ELSE NULL END
 FROM public.novu_rust_fcm_cooldown_health(p_env,p_grace) h
 LEFT JOIN public.novu_rust_fcm_cooldown_policies p ON p.environment_id=p_env
$$;
REVOKE ALL ON FUNCTION public.novu_rust_fcm_cooldown_health_detail(text,integer) FROM PUBLIC;
