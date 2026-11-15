-- Preserve key identity while allowing an explicitly authorized rotation.
ALTER TABLE public.novu_rust_sendgrid_key_audit DROP CONSTRAINT IF EXISTS novu_rust_sendgrid_key_audit_action_check;
ALTER TABLE public.novu_rust_sendgrid_key_audit ADD CONSTRAINT novu_rust_sendgrid_key_audit_action_check CHECK(action IN ('create','delete','activate','deactivate'));
CREATE OR REPLACE FUNCTION public.novu_rust_sendgrid_key_set_active(p_hash text,p_environment text,p_id text,p_active boolean,p_public text,p_audit uuid)
RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE old_active boolean; old_public text; n bigint; actor text;
BEGIN
  IF p_id IS NULL OR length(p_id) NOT BETWEEN 1 AND 128 OR p_id !~ '^[A-Za-z0-9_-][A-Za-z0-9._-]*$'
    OR p_active IS NULL OR p_audit IS NULL OR (p_active AND (p_public IS NULL OR length(p_public)>1024 OR p_public !~ '^[A-Za-z0-9+/]+={0,2}$')) THEN
    RAISE EXCEPTION 'invalid request' USING ERRCODE='22023';
  END IF;
  IF NOT public.novu_rust_lock_sendgrid_key_grant(p_hash,p_environment,'write') THEN
    RAISE EXCEPTION 'permission denied' USING ERRCODE='42501';
  END IF;
  PERFORM 1 FROM public.novu_rust_environments WHERE id=p_environment FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'environment not found' USING ERRCODE='22023'; END IF;
  SELECT k.active,k.public_key_base64 INTO old_active,old_public FROM public.novu_rust_sendgrid_webhook_keys k
    WHERE k.environment_id=p_environment AND k.key_id=p_id FOR UPDATE;
  IF NOT FOUND THEN RETURN 'missing'; END IF;
  IF p_active AND p_public IS DISTINCT FROM old_public THEN RETURN 'conflict'; END IF;
  IF old_active=p_active THEN RETURN 'unchanged'; END IF;
  IF p_active THEN
    SELECT count(*) INTO n FROM public.novu_rust_sendgrid_webhook_keys WHERE environment_id=p_environment AND active;
    IF n>=10 THEN RETURN 'limit'; END IF;
  END IF;
  BEGIN
    UPDATE public.novu_rust_sendgrid_webhook_keys SET active=p_active WHERE environment_id=p_environment AND key_id=p_id;
  EXCEPTION WHEN unique_violation THEN RETURN 'conflict'; END;
  SELECT user_id INTO actor FROM public.novu_rust_api_keys WHERE key_hash=p_hash;
  INSERT INTO public.novu_rust_sendgrid_key_audit(id,environment_id,key_id,action,key_hash,user_id)
    VALUES (p_audit,p_environment,p_id,CASE WHEN p_active THEN 'activate' ELSE 'deactivate' END,p_hash,actor);
  RETURN 'ok';
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_sendgrid_key_set_active(text,text,text,boolean,text,uuid) FROM PUBLIC;
