-- Environment administrator management and audit of optional alert visibility.
CREATE TABLE public.novu_rust_console_alert_grant_audit (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 environment_id text NOT NULL,
 credential_id uuid NOT NULL,
 action text NOT NULL CHECK (action IN ('grant','revoke')),
 actor name NOT NULL,
 actor_oid oid NOT NULL,
 reason text NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX novu_rust_console_alert_grant_audit_env_idx ON public.novu_rust_console_alert_grant_audit(environment_id,id);
REVOKE ALL ON public.novu_rust_console_alert_grant_audit FROM PUBLIC;

CREATE FUNCTION public.novu_rust_manage_console_alert_grant(p_env text,p_id uuid,p_action text,p_reason text) RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_actor oid; v_enabled boolean; v_expires timestamptz; v_changed integer;
BEGIN
 IF p_env IS NULL OR length(btrim(p_env))=0 OR p_id IS NULL OR p_action IS NULL OR p_action NOT IN ('grant','revoke')
 OR p_reason IS NULL OR length(btrim(p_reason))=0 OR octet_length(p_reason)>512 THEN
  RAISE EXCEPTION 'invalid request' USING ERRCODE='22023';
 END IF;
 v_actor:=public.novu_rust_lock_console_admin(p_env);
 SELECT c.enabled,c.expires_at INTO v_enabled,v_expires FROM public.novu_rust_console_credentials c
  WHERE c.environment_id=p_env AND c.credential_id=p_id FOR UPDATE OF c;
 IF NOT FOUND THEN RAISE EXCEPTION 'credential not found' USING ERRCODE='22023'; END IF;
 IF p_action='grant' THEN
  IF NOT v_enabled OR v_expires<=clock_timestamp() THEN RAISE EXCEPTION 'inactive credential' USING ERRCODE='22023'; END IF;
  INSERT INTO public.novu_rust_console_alert_grants(credential_id) VALUES(p_id) ON CONFLICT DO NOTHING;
 ELSE
  DELETE FROM public.novu_rust_console_alert_grants WHERE credential_id=p_id;
 END IF;
 GET DIAGNOSTICS v_changed=ROW_COUNT;
 IF v_changed=1 THEN
  INSERT INTO public.novu_rust_console_alert_grant_audit(environment_id,credential_id,action,actor,actor_oid,reason)
   VALUES(p_env,p_id,p_action,session_user,v_actor,p_reason);
 END IF;
 RETURN v_changed=1;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_manage_console_alert_grant(text,uuid,text,text) FROM PUBLIC;

CREATE FUNCTION public.novu_rust_list_console_alert_grants(p_env text,p_after uuid)
RETURNS TABLE(credential_id uuid,enabled boolean,expires_at text) LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 PERFORM public.novu_rust_lock_console_admin(p_env);
 RETURN QUERY SELECT c.credential_id,c.enabled,c.expires_at::text FROM public.novu_rust_console_credentials c
 JOIN public.novu_rust_console_alert_grants g ON g.credential_id=c.credential_id
 WHERE c.environment_id=p_env AND (p_after IS NULL OR c.credential_id>p_after)
 ORDER BY c.credential_id LIMIT 100;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_list_console_alert_grants(text,uuid) FROM PUBLIC;

CREATE FUNCTION public.novu_rust_list_console_alert_grant_audit(p_env text,p_after bigint)
RETURNS TABLE(id text,credential_id uuid,action text,actor text,reason text,created_at text)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF p_after IS NULL OR p_after<0 THEN RAISE EXCEPTION 'invalid cursor' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_lock_console_admin(p_env);
 RETURN QUERY SELECT a.id::text,a.credential_id,a.action,a.actor::text,a.reason,a.created_at::text
 FROM public.novu_rust_console_alert_grant_audit a WHERE a.environment_id=p_env AND a.id>p_after ORDER BY a.id LIMIT 100;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_list_console_alert_grant_audit(text,bigint) FROM PUBLIC;
