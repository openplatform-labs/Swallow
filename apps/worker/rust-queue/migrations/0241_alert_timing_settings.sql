-- Non-secret timing settings; target credentials remain in local files.
CREATE TABLE public.novu_rust_alert_timing_grants (
 environment_id text NOT NULL REFERENCES public.novu_rust_environments(id) ON DELETE CASCADE,
 database_principal name NOT NULL, principal_oid oid NOT NULL,
 can_read boolean NOT NULL DEFAULT false, can_write boolean NOT NULL DEFAULT false,
 PRIMARY KEY(environment_id,database_principal)
);
CREATE TABLE public.novu_rust_alert_timing_settings (
 environment_id text NOT NULL REFERENCES public.novu_rust_environments(id) ON DELETE CASCADE,
 channel text NOT NULL CHECK(channel IN ('slack','email')),
 target_id text NOT NULL CHECK(target_id ~ '^[A-Za-z0-9._-]{1,64}$'),
 revision bigint NOT NULL CHECK(revision>0), grace integer CHECK(grace BETWEEN 1 AND 86400),
 heartbeat integer CHECK(heartbeat BETWEEN 1 AND 86400), cooldown integer CHECK(cooldown BETWEEN 60 AND 86400),
 PRIMARY KEY(environment_id,channel,target_id)
);
CREATE TABLE public.novu_rust_alert_timing_requests (
 environment_id text NOT NULL, actor name NOT NULL, actor_oid oid NOT NULL, request_id uuid NOT NULL,
 channel text NOT NULL, target_id text NOT NULL, expected_revision bigint NOT NULL,
 grace integer, heartbeat integer, cooldown integer, reason text NOT NULL, result jsonb NOT NULL,
 PRIMARY KEY(environment_id,actor_oid,request_id)
);
CREATE TABLE public.novu_rust_alert_timing_audit (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, environment_id text NOT NULL, actor name NOT NULL,
 actor_oid oid NOT NULL, request_id uuid NOT NULL, channel text NOT NULL, target_id text NOT NULL,
 previous jsonb NOT NULL, current jsonb NOT NULL, reason text NOT NULL, recorded_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
REVOKE ALL ON public.novu_rust_alert_timing_grants,public.novu_rust_alert_timing_settings,public.novu_rust_alert_timing_requests,public.novu_rust_alert_timing_audit FROM PUBLIC;
CREATE FUNCTION public.novu_rust_alert_timing_access(p_env text,p_write boolean) RETURNS oid
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_oid oid;
BEGIN
 IF p_env IS NULL OR length(btrim(p_env))=0 OR p_write IS NULL THEN RAISE EXCEPTION 'invalid timing request' USING ERRCODE='22023'; END IF;
 SELECT r.oid INTO v_oid FROM public.novu_rust_alert_timing_grants g JOIN pg_catalog.pg_roles r ON r.rolname=g.database_principal AND r.oid=g.principal_oid AND r.rolcanlogin
 WHERE g.environment_id=p_env AND g.database_principal=session_user AND CASE WHEN p_write THEN g.can_write ELSE g.can_read END FOR SHARE OF g;
 IF NOT FOUND THEN RAISE EXCEPTION 'timing access denied' USING ERRCODE='42501'; END IF;
 RETURN v_oid;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_alert_timing_access(text,boolean) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_alert_timing_list(p_env text) RETURNS TABLE(channel text,target_id text,revision bigint,grace integer,heartbeat integer,cooldown integer)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 PERFORM public.novu_rust_alert_timing_access(p_env,false);
 RETURN QUERY SELECT s.channel,s.target_id,s.revision,s.grace,s.heartbeat,s.cooldown FROM public.novu_rust_alert_timing_settings s WHERE s.environment_id=p_env ORDER BY s.channel COLLATE "C",s.target_id COLLATE "C" LIMIT 1025;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_alert_timing_list(text) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_alert_timing_set(p_env text,p_channel text,p_target text,p_expected bigint,p_grace integer,p_heartbeat integer,p_cooldown integer,p_request uuid,p_reason text) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_oid oid; v_old public.novu_rust_alert_timing_requests%ROWTYPE; v_revision bigint; v_previous jsonb; v_current jsonb;
BEGIN
 IF p_channel IS NULL OR p_channel NOT IN ('slack','email') OR p_target IS NULL OR p_target !~ '^[A-Za-z0-9._-]{1,64}$' OR p_expected IS NULL OR p_expected<0 OR p_request IS NULL OR p_reason IS NULL OR octet_length(btrim(p_reason)) NOT BETWEEN 1 AND 512 OR (p_grace IS NOT NULL AND p_grace NOT BETWEEN 1 AND 86400) OR (p_heartbeat IS NOT NULL AND p_heartbeat NOT BETWEEN 1 AND 86400) OR (p_cooldown IS NOT NULL AND p_cooldown NOT BETWEEN 60 AND 86400) THEN RAISE EXCEPTION 'invalid timing request' USING ERRCODE='22023'; END IF;
 v_oid:=public.novu_rust_alert_timing_access(p_env,true);
 -- Serialize initial creation and duplicate requests as well as revision updates.
 PERFORM 1 FROM public.novu_rust_environments WHERE id=p_env FOR UPDATE;
 SELECT * INTO v_old FROM public.novu_rust_alert_timing_requests WHERE environment_id=p_env AND actor_oid=v_oid AND request_id=p_request;
 IF FOUND THEN
  IF (v_old.actor,v_old.channel,v_old.target_id,v_old.expected_revision,v_old.grace,v_old.heartbeat,v_old.cooldown,v_old.reason) IS DISTINCT FROM (session_user,p_channel,p_target,p_expected,p_grace,p_heartbeat,p_cooldown,p_reason) THEN RAISE EXCEPTION 'timing request conflict' USING ERRCODE='23505'; END IF;
  RETURN jsonb_build_object('replayed',true,'settings',v_old.result);
 END IF;
 SELECT revision,jsonb_build_object('revision',revision::text,'grace',grace,'heartbeat',heartbeat,'cooldown',cooldown) INTO v_revision,v_previous FROM public.novu_rust_alert_timing_settings WHERE environment_id=p_env AND channel=p_channel AND target_id=p_target FOR UPDATE;
 v_revision:=coalesce(v_revision,0);v_previous:=coalesce(v_previous,jsonb_build_object('revision','0','grace',NULL,'heartbeat',NULL,'cooldown',NULL));
 IF v_revision<>p_expected THEN RAISE EXCEPTION 'stale timing revision' USING ERRCODE='40001'; END IF;
 INSERT INTO public.novu_rust_alert_timing_settings(environment_id,channel,target_id,revision,grace,heartbeat,cooldown) VALUES(p_env,p_channel,p_target,v_revision+1,p_grace,p_heartbeat,p_cooldown) ON CONFLICT(environment_id,channel,target_id) DO UPDATE SET revision=EXCLUDED.revision,grace=EXCLUDED.grace,heartbeat=EXCLUDED.heartbeat,cooldown=EXCLUDED.cooldown;
 v_current:=jsonb_build_object('revision',(v_revision+1)::text,'grace',p_grace,'heartbeat',p_heartbeat,'cooldown',p_cooldown);
 INSERT INTO public.novu_rust_alert_timing_requests VALUES(p_env,session_user,v_oid,p_request,p_channel,p_target,p_expected,p_grace,p_heartbeat,p_cooldown,p_reason,v_current);
 INSERT INTO public.novu_rust_alert_timing_audit(environment_id,actor,actor_oid,request_id,channel,target_id,previous,current,reason) VALUES(p_env,session_user,v_oid,p_request,p_channel,p_target,v_previous,v_current,p_reason);
 RETURN jsonb_build_object('replayed',false,'settings',v_current);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_alert_timing_set(text,text,text,bigint,integer,integer,integer,uuid,text) FROM PUBLIC;
