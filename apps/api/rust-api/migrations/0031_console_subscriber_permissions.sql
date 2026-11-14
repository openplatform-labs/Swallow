-- Existing credentials and the legacy issuance function retain subscriber access.
ALTER TABLE public.novu_rust_console_credentials ADD COLUMN subscriber_read boolean NOT NULL DEFAULT true;
CREATE OR REPLACE FUNCTION public.novu_rust_console_subscribers(p_hash text,p_name text,p_after text,p_before text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_env text; v_expires timestamptz; v_result jsonb;
BEGIN
 IF p_hash IS NULL OR p_hash !~ '^[0-9a-f]{64}$' OR (p_name IS NOT NULL AND octet_length(p_name)>512)
 OR (p_after IS NOT NULL AND (octet_length(p_after)>512 OR p_after=''))
 OR (p_before IS NOT NULL AND (octet_length(p_before)>512 OR p_before=''))
 OR (p_after IS NOT NULL AND p_before IS NOT NULL) THEN
  RAISE EXCEPTION 'invalid request' USING ERRCODE='22023';
 END IF;
 SELECT c.environment_id,c.expires_at INTO v_env,v_expires FROM public.novu_rust_console_credentials c
 WHERE c.token_hash=p_hash AND c.enabled AND c.subscriber_read AND c.expires_at>clock_timestamp() FOR SHARE OF c;
 IF NOT FOUND OR v_expires<=clock_timestamp() THEN RAISE EXCEPTION 'invalid credential' USING ERRCODE='42501'; END IF;
 WITH filtered AS MATERIALIZED (
   SELECT s.subscriber_id,s.profile FROM public.novu_rust_subscribers s
   WHERE s.environment_id=v_env AND (p_name IS NULL OR position(lower(p_name) in lower(trim(coalesce(s.profile->>'firstName','') || ' ' || coalesce(s.profile->>'lastName',''))))>0)
 ), candidates AS MATERIALIZED (
   SELECT * FROM filtered WHERE (p_after IS NULL OR subscriber_id COLLATE "C">p_after COLLATE "C") AND (p_before IS NULL OR subscriber_id COLLATE "C"<p_before COLLATE "C")
   ORDER BY CASE WHEN p_before IS NULL THEN subscriber_id END COLLATE "C" ASC,CASE WHEN p_before IS NOT NULL THEN subscriber_id END COLLATE "C" DESC LIMIT 20
 ), bounds AS (
   SELECT min(subscriber_id COLLATE "C") AS first_id,max(subscriber_id COLLATE "C") AS last_id FROM candidates
 )
 SELECT jsonb_build_object('data',coalesce((SELECT jsonb_agg(jsonb_build_object('subscriberId',subscriber_id,'firstName',profile->>'firstName','lastName',profile->>'lastName','email',profile->>'email') ORDER BY subscriber_id COLLATE "C") FROM candidates),'[]'::jsonb),
  'previous',CASE WHEN EXISTS(SELECT 1 FROM filtered f WHERE f.subscriber_id COLLATE "C"<b.first_id COLLATE "C") THEN b.first_id ELSE NULL END,
  'next',CASE WHEN EXISTS(SELECT 1 FROM filtered f WHERE f.subscriber_id COLLATE "C">b.last_id COLLATE "C") THEN b.last_id ELSE NULL END) INTO v_result FROM bounds b;
 RETURN v_result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_console_subscribers(text,text,text,text) FROM PUBLIC;

CREATE TABLE public.novu_rust_console_subscriber_scope_audit (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 environment_id text NOT NULL,
 credential_id uuid NOT NULL,
 action text NOT NULL CHECK (action IN ('initial_grant','initial_none','grant','revoke')),
 actor name NOT NULL,
 actor_oid oid NOT NULL,
 reason text NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX novu_rust_console_subscriber_scope_audit_env_idx ON public.novu_rust_console_subscriber_scope_audit(environment_id,id);
REVOKE ALL ON public.novu_rust_console_subscriber_scope_audit FROM PUBLIC;

CREATE FUNCTION public.novu_rust_issue_console_credential_scoped(p_env text,p_hash text,p_expiry timestamptz,p_subscriber boolean,p_reason text) RETURNS uuid
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_id uuid; v_actor oid;
BEGIN
 IF p_subscriber IS NULL THEN RAISE EXCEPTION 'subscriber scope required' USING ERRCODE='22023'; END IF;
 v_actor:=public.novu_rust_lock_console_admin(p_env);
 v_id:=public.novu_rust_issue_console_credential(p_env,p_hash,p_expiry,p_reason);
 UPDATE public.novu_rust_console_credentials SET subscriber_read=p_subscriber WHERE credential_id=v_id AND environment_id=p_env;
 INSERT INTO public.novu_rust_console_subscriber_scope_audit(environment_id,credential_id,action,actor,actor_oid,reason)
 VALUES(p_env,v_id,CASE WHEN p_subscriber THEN 'initial_grant' ELSE 'initial_none' END,session_user,v_actor,p_reason);
 RETURN v_id;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_issue_console_credential_scoped(text,text,timestamptz,boolean,text) FROM PUBLIC;

CREATE FUNCTION public.novu_rust_manage_console_subscriber_scope(p_env text,p_id uuid,p_action text,p_reason text) RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_actor oid; v_enabled boolean; v_expires timestamptz; v_current boolean; v_new boolean;
BEGIN
 IF p_env IS NULL OR length(btrim(p_env))=0 OR p_id IS NULL OR p_action IS NULL OR p_action NOT IN ('grant','revoke')
 OR p_reason IS NULL OR length(btrim(p_reason))=0 OR octet_length(p_reason)>512 THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 v_actor:=public.novu_rust_lock_console_admin(p_env);
 SELECT c.enabled,c.expires_at,c.subscriber_read INTO v_enabled,v_expires,v_current FROM public.novu_rust_console_credentials c WHERE c.environment_id=p_env AND c.credential_id=p_id FOR UPDATE OF c;
 IF NOT FOUND THEN RAISE EXCEPTION 'credential not found' USING ERRCODE='22023'; END IF;
 v_new:=p_action='grant';
 IF v_new AND (NOT v_enabled OR v_expires<=clock_timestamp()) THEN RAISE EXCEPTION 'inactive credential' USING ERRCODE='22023'; END IF;
 IF v_current=v_new THEN RETURN false; END IF;
 UPDATE public.novu_rust_console_credentials SET subscriber_read=v_new WHERE environment_id=p_env AND credential_id=p_id;
 INSERT INTO public.novu_rust_console_subscriber_scope_audit(environment_id,credential_id,action,actor,actor_oid,reason) VALUES(p_env,p_id,p_action,session_user,v_actor,p_reason);
 RETURN true;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_manage_console_subscriber_scope(text,uuid,text,text) FROM PUBLIC;

CREATE FUNCTION public.novu_rust_list_console_subscriber_scopes(p_env text,p_after uuid)
RETURNS TABLE(credential_id uuid,enabled boolean,expires_at text,subscriber_read boolean)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 PERFORM public.novu_rust_lock_console_admin(p_env);
 RETURN QUERY SELECT c.credential_id,c.enabled,c.expires_at::text,c.subscriber_read FROM public.novu_rust_console_credentials c WHERE c.environment_id=p_env AND (p_after IS NULL OR c.credential_id>p_after) ORDER BY c.credential_id LIMIT 100;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_list_console_subscriber_scopes(text,uuid) FROM PUBLIC;

CREATE FUNCTION public.novu_rust_list_console_subscriber_scope_audit(p_env text,p_after bigint)
RETURNS TABLE(id text,credential_id uuid,action text,actor text,reason text,created_at text)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF p_after IS NULL OR p_after<0 THEN RAISE EXCEPTION 'invalid cursor' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_lock_console_admin(p_env);
 RETURN QUERY SELECT a.id::text,a.credential_id,a.action,a.actor::text,a.reason,a.created_at::text FROM public.novu_rust_console_subscriber_scope_audit a WHERE a.environment_id=p_env AND a.id>p_after ORDER BY a.id LIMIT 100;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_list_console_subscriber_scope_audit(text,bigint) FROM PUBLIC;
