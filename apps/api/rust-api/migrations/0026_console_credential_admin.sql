-- Environment-scoped, database-login-bound management of console credentials.
ALTER TABLE public.novu_rust_console_credentials ADD COLUMN IF NOT EXISTS credential_id uuid DEFAULT gen_random_uuid();
UPDATE public.novu_rust_console_credentials SET credential_id=gen_random_uuid() WHERE credential_id IS NULL;
ALTER TABLE public.novu_rust_console_credentials ALTER COLUMN credential_id SET NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS novu_rust_console_credential_id_idx ON public.novu_rust_console_credentials(credential_id);
CREATE INDEX IF NOT EXISTS novu_rust_console_credential_env_idx ON public.novu_rust_console_credentials(environment_id,credential_id);
CREATE TABLE IF NOT EXISTS public.novu_rust_console_credential_admins (
 environment_id text NOT NULL REFERENCES public.novu_rust_environments(id),
 database_principal name NOT NULL,
 principal_oid oid NOT NULL,
 PRIMARY KEY(environment_id,database_principal)
);
CREATE TABLE IF NOT EXISTS public.novu_rust_console_credential_audit (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 environment_id text NOT NULL,
 credential_id uuid NOT NULL,
 action text NOT NULL CHECK (action IN ('issue','revoke')),
 actor name NOT NULL,
 actor_oid oid NOT NULL,
 reason text NOT NULL,
 expires_at timestamptz NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS novu_rust_console_credential_audit_env_idx ON public.novu_rust_console_credential_audit(environment_id,id);
REVOKE ALL ON public.novu_rust_console_credential_admins,public.novu_rust_console_credential_audit FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.novu_rust_lock_console_admin(p_env text) RETURNS oid
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_actor oid;
BEGIN
 SELECT oid INTO v_actor FROM pg_catalog.pg_roles WHERE rolname=session_user AND rolcanlogin;
 PERFORM 1 FROM public.novu_rust_console_credential_admins a WHERE a.environment_id=p_env AND a.database_principal=session_user AND a.principal_oid=v_actor FOR SHARE OF a;
 IF NOT FOUND THEN RAISE EXCEPTION 'console administration denied' USING ERRCODE='42501'; END IF;
 RETURN v_actor;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_lock_console_admin(text) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.novu_rust_issue_console_credential(p_env text,p_hash text,p_expiry timestamptz,p_reason text) RETURNS uuid
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_actor oid; v_id uuid;
BEGIN
 IF p_env IS NULL OR length(btrim(p_env))=0 OR p_hash IS NULL OR p_hash !~ '^[0-9a-f]{64}$'
 OR p_expiry IS NULL OR NOT isfinite(p_expiry) OR p_expiry<=clock_timestamp() OR p_expiry>clock_timestamp()+interval '30 days'
 OR p_reason IS NULL OR length(btrim(p_reason))=0 OR octet_length(p_reason)>512 THEN RAISE EXCEPTION 'invalid console issue request' USING ERRCODE='22023'; END IF;
 v_actor:=public.novu_rust_lock_console_admin(p_env);
 INSERT INTO public.novu_rust_console_credentials(token_hash,environment_id,expires_at) VALUES(p_hash,p_env,p_expiry) RETURNING credential_id INTO v_id;
 INSERT INTO public.novu_rust_console_credential_audit(environment_id,credential_id,action,actor,actor_oid,reason,expires_at) VALUES(p_env,v_id,'issue',session_user,v_actor,p_reason,p_expiry);
 RETURN v_id;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_issue_console_credential(text,text,timestamptz,text) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.novu_rust_revoke_console_credential(p_env text,p_id uuid,p_reason text) RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_actor oid; v_expiry timestamptz; v_enabled boolean;
BEGIN
 IF p_env IS NULL OR length(btrim(p_env))=0 OR p_id IS NULL OR p_reason IS NULL OR length(btrim(p_reason))=0 OR octet_length(p_reason)>512 THEN RAISE EXCEPTION 'invalid console revoke request' USING ERRCODE='22023'; END IF;
 v_actor:=public.novu_rust_lock_console_admin(p_env);
 SELECT c.enabled,c.expires_at INTO v_enabled,v_expiry FROM public.novu_rust_console_credentials c WHERE c.environment_id=p_env AND c.credential_id=p_id FOR UPDATE OF c;
 IF NOT FOUND THEN RAISE EXCEPTION 'credential not found' USING ERRCODE='22023'; END IF;
 IF NOT v_enabled THEN RETURN false; END IF;
 UPDATE public.novu_rust_console_credentials SET enabled=false WHERE environment_id=p_env AND credential_id=p_id;
 INSERT INTO public.novu_rust_console_credential_audit(environment_id,credential_id,action,actor,actor_oid,reason,expires_at) VALUES(p_env,p_id,'revoke',session_user,v_actor,p_reason,v_expiry);
 RETURN true;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_revoke_console_credential(text,uuid,text) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.novu_rust_list_console_credentials(p_env text,p_after uuid)
RETURNS TABLE(credential_id uuid,enabled boolean,expires_at text,created_at text)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 PERFORM public.novu_rust_lock_console_admin(p_env);
 RETURN QUERY SELECT c.credential_id,c.enabled,c.expires_at::text,c.created_at::text FROM public.novu_rust_console_credentials c WHERE c.environment_id=p_env AND (p_after IS NULL OR c.credential_id>p_after) ORDER BY c.credential_id LIMIT 100;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_list_console_credentials(text,uuid) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.novu_rust_list_console_credential_audit(p_env text,p_after bigint)
RETURNS TABLE(id text,credential_id uuid,action text,actor text,reason text,expires_at text,created_at text)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF p_after IS NULL OR p_after<0 THEN RAISE EXCEPTION 'invalid cursor' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_lock_console_admin(p_env);
 RETURN QUERY SELECT a.id::text,a.credential_id,a.action,a.actor::text,a.reason,a.expires_at::text,a.created_at::text FROM public.novu_rust_console_credential_audit a WHERE a.environment_id=p_env AND a.id>p_after ORDER BY a.id LIMIT 100;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_list_console_credential_audit(text,bigint) FROM PUBLIC;
