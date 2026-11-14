-- Decision permissions are separate from alert visibility and from one another.
CREATE TABLE public.novu_rust_console_alert_decision_grants (
 credential_id uuid NOT NULL REFERENCES public.novu_rust_console_credentials(credential_id) ON DELETE CASCADE,
 decision text NOT NULL CHECK (decision IN ('retry','accepted')),
 PRIMARY KEY(credential_id,decision)
);
CREATE TABLE public.novu_rust_console_alert_decision_grant_audit (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, environment_id text NOT NULL, credential_id uuid NOT NULL,
 decision text NOT NULL, action text NOT NULL, actor name NOT NULL, actor_oid oid NOT NULL,
 reason text NOT NULL, created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX novu_rust_console_decision_grant_audit_env_idx ON public.novu_rust_console_alert_decision_grant_audit(environment_id,id);
CREATE TABLE public.novu_rust_console_alert_decision_audit (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, environment_id text NOT NULL, credential_id uuid NOT NULL,
 channel text NOT NULL, target_id text NOT NULL, attempt_id uuid NOT NULL, decision text NOT NULL,
 reason text NOT NULL, database_session_user name NOT NULL, created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX novu_rust_console_decision_audit_env_idx ON public.novu_rust_console_alert_decision_audit(environment_id,id);
REVOKE ALL ON public.novu_rust_console_alert_decision_grants,public.novu_rust_console_alert_decision_grant_audit,public.novu_rust_console_alert_decision_audit FROM PUBLIC;

CREATE FUNCTION public.novu_rust_manage_console_alert_decision(p_env text,p_id uuid,p_decision text,p_action text,p_reason text)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_actor oid; v_enabled boolean; v_expires timestamptz; v_changed integer;
BEGIN
 IF p_id IS NULL OR p_decision IS NULL OR p_decision NOT IN ('retry','accepted') OR p_action IS NULL OR p_action NOT IN ('grant','revoke')
 OR p_reason IS NULL OR length(btrim(p_reason))=0 OR octet_length(p_reason)>512 THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 v_actor:=public.novu_rust_lock_console_admin(p_env);
 SELECT c.enabled,c.expires_at INTO v_enabled,v_expires FROM public.novu_rust_console_credentials c WHERE c.environment_id=p_env AND c.credential_id=p_id FOR UPDATE OF c;
 IF NOT FOUND THEN RAISE EXCEPTION 'credential not found' USING ERRCODE='22023'; END IF;
 IF p_action='grant' THEN
  IF NOT v_enabled OR v_expires<=clock_timestamp() THEN RAISE EXCEPTION 'inactive credential' USING ERRCODE='22023'; END IF;
  INSERT INTO public.novu_rust_console_alert_decision_grants(credential_id,decision) VALUES(p_id,p_decision) ON CONFLICT DO NOTHING;
 ELSE
  DELETE FROM public.novu_rust_console_alert_decision_grants WHERE credential_id=p_id AND decision=p_decision;
 END IF;
 GET DIAGNOSTICS v_changed=ROW_COUNT;
 IF v_changed=1 THEN INSERT INTO public.novu_rust_console_alert_decision_grant_audit(environment_id,credential_id,decision,action,actor,actor_oid,reason) VALUES(p_env,p_id,p_decision,p_action,session_user,v_actor,p_reason); END IF;
 RETURN v_changed=1;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_manage_console_alert_decision(text,uuid,text,text,text) FROM PUBLIC;

CREATE FUNCTION public.novu_rust_list_console_alert_decisions(p_env text,p_after uuid)
RETURNS TABLE(credential_id uuid,retry boolean,accepted boolean,enabled boolean,expires_at text)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 PERFORM public.novu_rust_lock_console_admin(p_env);
 RETURN QUERY SELECT c.credential_id,
 EXISTS(SELECT 1 FROM public.novu_rust_console_alert_decision_grants g WHERE g.credential_id=c.credential_id AND g.decision='retry'),
 EXISTS(SELECT 1 FROM public.novu_rust_console_alert_decision_grants g WHERE g.credential_id=c.credential_id AND g.decision='accepted'),
 c.enabled,c.expires_at::text FROM public.novu_rust_console_credentials c
 WHERE c.environment_id=p_env AND (p_after IS NULL OR c.credential_id>p_after)
 ORDER BY c.credential_id LIMIT 100;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_list_console_alert_decisions(text,uuid) FROM PUBLIC;

CREATE FUNCTION public.novu_rust_list_console_decision_audit(p_env text,p_after bigint,p_kind text)
RETURNS TABLE(id text,credential_id uuid,details jsonb,created_at text)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF p_after IS NULL OR p_after<0 OR p_kind IS NULL OR p_kind NOT IN ('grant','resolution') THEN RAISE EXCEPTION 'invalid cursor' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_lock_console_admin(p_env);
 IF p_kind='grant' THEN
  RETURN QUERY SELECT a.id::text,a.credential_id,jsonb_build_object('decision',a.decision,'action',a.action,'actor',a.actor,'reason',a.reason),a.created_at::text FROM public.novu_rust_console_alert_decision_grant_audit a WHERE a.environment_id=p_env AND a.id>p_after ORDER BY a.id LIMIT 100;
 ELSE
  RETURN QUERY SELECT a.id::text,a.credential_id,jsonb_build_object('channel',a.channel,'targetId',a.target_id,'attemptId',a.attempt_id,'decision',a.decision,'reason',a.reason,'databaseSessionUser',a.database_session_user),a.created_at::text FROM public.novu_rust_console_alert_decision_audit a WHERE a.environment_id=p_env AND a.id>p_after ORDER BY a.id LIMIT 100;
 END IF;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_list_console_decision_audit(text,bigint,text) FROM PUBLIC;

CREATE FUNCTION public.novu_rust_console_resolve_alert(p_hash text,p_channel text,p_target text,p_attempt uuid,p_decision text,p_reason text)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_env text; v_id uuid; v_expires timestamptz;
BEGIN
 IF p_hash IS NULL OR p_hash !~ '^[0-9a-f]{64}$' OR p_decision IS NULL OR p_decision NOT IN ('retry','accepted') THEN RAISE EXCEPTION 'invalid credential' USING ERRCODE='42501'; END IF;
 SELECT c.environment_id,c.credential_id,c.expires_at INTO v_env,v_id,v_expires FROM public.novu_rust_console_credentials c
 WHERE c.token_hash=p_hash AND c.enabled AND c.expires_at>clock_timestamp() FOR SHARE OF c;
 IF NOT FOUND OR v_expires<=clock_timestamp() THEN RAISE EXCEPTION 'invalid credential' USING ERRCODE='42501'; END IF;
 PERFORM 1 FROM public.novu_rust_console_alert_decision_grants g WHERE g.credential_id=v_id AND g.decision=p_decision FOR SHARE OF g;
 IF NOT FOUND THEN RAISE EXCEPTION 'invalid credential' USING ERRCODE='42501'; END IF;
 -- Match the worker's grant-before-slot order, and recheck expiry after waiting.
 IF NOT public.novu_rust_lock_operator_grant(v_env,'alert_' || p_decision) THEN RAISE EXCEPTION 'invalid credential' USING ERRCODE='42501'; END IF;
 PERFORM 1 FROM public.novu_rust_alert_delivery_slots s WHERE s.environment_id=v_env AND s.channel=p_channel AND s.target_id=p_target FOR UPDATE OF s;
 IF v_expires<=clock_timestamp() THEN RAISE EXCEPTION 'invalid credential' USING ERRCODE='42501'; END IF;
 PERFORM public.novu_rust_resolve_alert_v2(v_env,p_channel,p_target,p_attempt,p_decision,'console:' || v_id::text,p_reason);
 INSERT INTO public.novu_rust_console_alert_decision_audit(environment_id,credential_id,channel,target_id,attempt_id,decision,reason,database_session_user)
 VALUES(v_env,v_id,p_channel,p_target,p_attempt,p_decision,p_reason,session_user);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_console_resolve_alert(text,text,text,uuid,text,text) FROM PUBLIC;
