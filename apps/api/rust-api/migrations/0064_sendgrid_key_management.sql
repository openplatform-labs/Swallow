-- Experimental, separately granted SendGrid verification-key management.
CREATE TABLE IF NOT EXISTS public.novu_rust_sendgrid_key_permissions (
  key_hash text NOT NULL REFERENCES public.novu_rust_api_keys(key_hash) ON DELETE CASCADE,
  permission text NOT NULL CHECK(permission IN ('read','write')),
  PRIMARY KEY(key_hash,permission)
);
CREATE TABLE IF NOT EXISTS public.novu_rust_sendgrid_key_audit (
  id uuid PRIMARY KEY, environment_id text NOT NULL, key_id text NOT NULL,
  action text NOT NULL CHECK(action IN ('create','delete')),
  key_hash text NOT NULL CHECK(length(key_hash)=64), user_id text NOT NULL,
  recorded_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
REVOKE ALL ON public.novu_rust_sendgrid_key_permissions,public.novu_rust_sendgrid_key_audit FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.novu_rust_lock_sendgrid_key_grant(p_hash text,p_environment text,p_permission text)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
  IF p_permission NOT IN ('read','write') THEN RETURN false; END IF;
  PERFORM 1 FROM public.novu_rust_api_keys k JOIN public.novu_rust_users u ON u.id=k.user_id
    JOIN public.novu_rust_sendgrid_key_permissions p ON p.key_hash=k.key_hash
    WHERE k.key_hash=p_hash AND k.environment_id=p_environment AND k.enabled AND p.permission=p_permission
    FOR SHARE OF k,u,p;
  RETURN FOUND;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_lock_sendgrid_key_grant(text,text,text) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.novu_rust_sendgrid_keys_list(p_hash text,p_environment text,p_after text)
RETURNS TABLE(key_id text,active boolean) LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
  IF NOT public.novu_rust_lock_sendgrid_key_grant(p_hash,p_environment,'read') THEN
    RAISE EXCEPTION 'permission denied' USING ERRCODE='42501';
  END IF;
  RETURN QUERY SELECT k.key_id,k.active FROM public.novu_rust_sendgrid_webhook_keys k
    WHERE k.environment_id=p_environment AND (p_after IS NULL OR k.key_id COLLATE "C">p_after COLLATE "C")
    ORDER BY k.key_id COLLATE "C" LIMIT 101;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_sendgrid_keys_list(text,text,text) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.novu_rust_sendgrid_key_change(p_hash text,p_environment text,p_action text,p_id text,p_public text,p_audit uuid)
RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE n bigint; actor text;
BEGIN
  IF p_action IS NULL OR p_action NOT IN ('create','delete') OR p_id IS NULL OR length(p_id) NOT BETWEEN 1 AND 128
    OR p_id !~ '^[A-Za-z0-9_-][A-Za-z0-9._-]*$' OR p_audit IS NULL
    OR (p_action='create' AND (p_public IS NULL OR length(p_public)>1024 OR p_public !~ '^[A-Za-z0-9+/]+={0,2}$')) THEN
    RAISE EXCEPTION 'invalid request' USING ERRCODE='22023';
  END IF;
  IF NOT public.novu_rust_lock_sendgrid_key_grant(p_hash,p_environment,'write') THEN
    RAISE EXCEPTION 'permission denied' USING ERRCODE='42501';
  END IF;
  PERFORM 1 FROM public.novu_rust_environments WHERE id=p_environment FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'environment not found' USING ERRCODE='22023'; END IF;
  IF p_action='create' THEN
    SELECT count(*) INTO n FROM public.novu_rust_sendgrid_webhook_keys WHERE environment_id=p_environment AND active;
    IF n>=10 THEN RETURN 'limit'; END IF;
    BEGIN
      INSERT INTO public.novu_rust_sendgrid_webhook_keys(environment_id,key_id,public_key_base64) VALUES (p_environment,p_id,p_public);
    EXCEPTION WHEN unique_violation THEN RETURN 'conflict'; END;
  ELSE
    DELETE FROM public.novu_rust_sendgrid_webhook_keys WHERE environment_id=p_environment AND key_id=p_id;
    IF NOT FOUND THEN RETURN 'missing'; END IF;
  END IF;
  SELECT user_id INTO actor FROM public.novu_rust_api_keys WHERE key_hash=p_hash;
  INSERT INTO public.novu_rust_sendgrid_key_audit(id,environment_id,key_id,action,key_hash,user_id)
    VALUES (p_audit,p_environment,p_id,p_action,p_hash,actor);
  RETURN 'ok';
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_sendgrid_key_change(text,text,text,text,text,uuid) FROM PUBLIC;
