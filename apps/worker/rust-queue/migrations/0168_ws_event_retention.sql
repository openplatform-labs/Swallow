-- Explicit, environment-scoped maintenance. No automatic deletion is enabled.
-- This index takes a table lock while built; schedule for the actual table size.
CREATE INDEX novu_rust_ws_event_retention_idx ON public.novu_rust_ws_events(environment_id,created_at,id);
CREATE TABLE public.novu_rust_ws_retention_grants (
 environment_id text NOT NULL, database_principal name NOT NULL, principal_oid oid NOT NULL,
 can_preview boolean NOT NULL DEFAULT false, can_delete boolean NOT NULL DEFAULT false,
 PRIMARY KEY(environment_id,database_principal)
);
CREATE TABLE public.novu_rust_ws_retention_audit (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(), environment_id text NOT NULL,
 database_principal name NOT NULL, principal_oid oid NOT NULL,
 cutoff timestamptz NOT NULL, batch_limit integer NOT NULL, deleted_count integer NOT NULL,
 reason text NOT NULL, recorded_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
REVOKE ALL ON public.novu_rust_ws_retention_grants, public.novu_rust_ws_retention_audit FROM PUBLIC;
CREATE FUNCTION public.novu_rust_ws_retention_batch(p_env text,p_cutoff timestamptz,p_limit integer,p_delete boolean,p_reason text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_oid oid; v_count integer; v_oldest timestamptz; v_newest timestamptz;
BEGIN
 IF p_env IS NULL OR octet_length(p_env) NOT BETWEEN 1 AND 512 OR p_cutoff IS NULL OR NOT isfinite(p_cutoff) OR p_cutoff>=clock_timestamp() OR p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 1000 OR p_delete IS NULL OR (p_delete AND (p_reason IS NULL OR octet_length(btrim(p_reason)) NOT BETWEEN 1 AND 512)) THEN
  RAISE EXCEPTION 'invalid retention parameters' USING ERRCODE='22023';
 END IF;
 SELECT oid INTO v_oid FROM pg_catalog.pg_roles WHERE rolname=session_user;
 PERFORM 1 FROM public.novu_rust_ws_retention_grants g WHERE g.environment_id=p_env AND g.database_principal=session_user AND g.principal_oid=v_oid AND CASE WHEN p_delete THEN g.can_delete ELSE g.can_preview END FOR SHARE OF g;
 IF NOT FOUND THEN RAISE EXCEPTION 'retention permission denied' USING ERRCODE='42501'; END IF;
 IF p_delete THEN
  WITH candidates AS MATERIALIZED (
   SELECT id FROM public.novu_rust_ws_events WHERE environment_id=p_env AND created_at<p_cutoff ORDER BY created_at,id FOR UPDATE SKIP LOCKED LIMIT p_limit
  ), removed AS (
   DELETE FROM public.novu_rust_ws_events e USING candidates c WHERE e.id=c.id RETURNING e.created_at
  ) SELECT count(*)::integer,min(created_at),max(created_at) INTO v_count,v_oldest,v_newest FROM removed;
  INSERT INTO public.novu_rust_ws_retention_audit(environment_id,database_principal,principal_oid,cutoff,batch_limit,deleted_count,reason) VALUES(p_env,session_user,v_oid,p_cutoff,p_limit,v_count,p_reason);
 ELSE
  SELECT count(*)::integer,min(created_at),max(created_at) INTO v_count,v_oldest,v_newest FROM
   (SELECT created_at FROM public.novu_rust_ws_events WHERE environment_id=p_env AND created_at<p_cutoff ORDER BY created_at,id LIMIT p_limit) candidates;
 END IF;
 RETURN jsonb_build_object('environmentId',p_env,'cutoff',p_cutoff::text,'batchLimit',p_limit,'deleted',p_delete,'count',v_count,'oldest',v_oldest::text,'newest',v_newest::text);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_ws_retention_batch(text,timestamptz,integer,boolean,text) FROM PUBLIC;
