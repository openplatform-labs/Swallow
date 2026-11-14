-- Optional cooperative guard: only workers that invoke the claim function
-- participate. The session advisory lock is released on disconnect.
CREATE TABLE IF NOT EXISTS public.novu_rust_fcm_routing_config (
  environment_id text PRIMARY KEY REFERENCES public.novu_rust_environments(id),
  routing_hash text NOT NULL CHECK (routing_hash ~ '^[0-9a-f]{64}$'),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS public.novu_rust_fcm_routing_audit (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  environment_id text NOT NULL,
  old_hash text NOT NULL,
  actor text NOT NULL,
  actor_oid oid NOT NULL,
  reason text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS public.novu_rust_fcm_routing_workers (
  environment_id text NOT NULL REFERENCES public.novu_rust_environments(id),
  database_principal text NOT NULL,
  principal_oid oid NOT NULL,
  PRIMARY KEY(environment_id,database_principal)
);
CREATE OR REPLACE FUNCTION public.novu_rust_claim_fcm_routing(p_environment text,p_hash text)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE lock_key bigint;
BEGIN
  IF p_environment IS NULL OR p_environment='' OR p_hash IS NULL OR p_hash !~ '^[0-9a-f]{64}$' THEN
    RAISE EXCEPTION 'invalid FCM routing configuration' USING ERRCODE='22023';
  END IF;
  PERFORM 1 FROM public.novu_rust_fcm_routing_workers w
    WHERE w.environment_id=p_environment AND w.database_principal=session_user
      AND w.principal_oid=(SELECT oid FROM pg_catalog.pg_roles WHERE rolname=session_user AND rolcanlogin)
    FOR SHARE OF w;
  IF NOT FOUND THEN RAISE EXCEPTION 'FCM routing claim denied' USING ERRCODE='42501'; END IF;
  lock_key := pg_catalog.hashtextextended('swallow-fcm-routing:'||p_environment, 82991);
  PERFORM pg_catalog.pg_advisory_lock_shared(lock_key);
  INSERT INTO public.novu_rust_fcm_routing_config(environment_id,routing_hash)
    VALUES(p_environment,p_hash)
    ON CONFLICT(environment_id) DO UPDATE SET routing_hash=EXCLUDED.routing_hash
      WHERE public.novu_rust_fcm_routing_config.routing_hash=EXCLUDED.routing_hash;
  IF NOT FOUND THEN
    PERFORM pg_catalog.pg_advisory_unlock_shared(lock_key);
    RETURN false;
  END IF;
  RETURN true;
END;
$$;
CREATE OR REPLACE FUNCTION public.novu_rust_reset_fcm_routing(p_environment text,p_reason text)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE lock_key bigint; old_hash text; actor_id oid;
BEGIN
  IF p_environment IS NULL OR p_environment='' OR p_reason IS NULL OR length(btrim(p_reason))=0 OR length(p_reason)>1000 THEN
    RAISE EXCEPTION 'invalid FCM routing reset' USING ERRCODE='22023';
  END IF;
  SELECT oid INTO actor_id FROM pg_catalog.pg_roles WHERE rolname=session_user AND rolcanlogin;
  PERFORM 1 FROM public.novu_rust_operator_grant_admins a
    WHERE a.environment_id=p_environment AND a.database_principal=session_user AND a.principal_oid=actor_id FOR SHARE OF a;
  IF NOT FOUND THEN RAISE EXCEPTION 'FCM routing reset denied' USING ERRCODE='42501'; END IF;
  lock_key := pg_catalog.hashtextextended('swallow-fcm-routing:'||p_environment, 82991);
  IF NOT pg_catalog.pg_try_advisory_xact_lock(lock_key) THEN RETURN false; END IF;
  DELETE FROM public.novu_rust_fcm_routing_config WHERE environment_id=p_environment RETURNING routing_hash INTO old_hash;
  IF NOT FOUND THEN RETURN false; END IF;
  INSERT INTO public.novu_rust_fcm_routing_audit(environment_id,old_hash,actor,actor_oid,reason)
    VALUES(p_environment,old_hash,session_user,actor_id,p_reason);
  RETURN true;
END;
$$;
REVOKE ALL ON public.novu_rust_fcm_routing_config,public.novu_rust_fcm_routing_audit,public.novu_rust_fcm_routing_workers FROM PUBLIC;
REVOKE ALL ON FUNCTION public.novu_rust_claim_fcm_routing(text,text),public.novu_rust_reset_fcm_routing(text,text) FROM PUBLIC;
