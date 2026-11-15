-- Optional idempotent maintenance; existing preview and legacy delete remain available.
ALTER TABLE public.novu_rust_ws_retention_audit ADD COLUMN request_id uuid;
-- Enforce the documented full-byte limit for new legacy and v2 audit entries.
ALTER TABLE public.novu_rust_ws_retention_audit ADD CONSTRAINT novu_rust_ws_retention_reason_size CHECK (octet_length(reason)<=512) NOT VALID;
CREATE TABLE public.novu_rust_ws_retention_requests (
 environment_id text NOT NULL, database_principal name NOT NULL, principal_oid oid NOT NULL, request_id uuid NOT NULL,
 cutoff timestamptz NOT NULL, batch_limit integer NOT NULL, reason text NOT NULL, result jsonb,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(environment_id,database_principal,principal_oid,request_id)
);
REVOKE ALL ON public.novu_rust_ws_retention_requests FROM PUBLIC;
CREATE FUNCTION public.novu_rust_ws_retention_delete_v2(p_env text,p_cutoff timestamptz,p_limit integer,p_request uuid,p_reason text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_oid oid; v_count integer; v_oldest timestamptz; v_newest timestamptz; v_inserted integer; r public.novu_rust_ws_retention_requests%ROWTYPE; v_result jsonb;
BEGIN
 IF p_env IS NULL OR octet_length(p_env) NOT BETWEEN 1 AND 512 OR p_cutoff IS NULL OR NOT isfinite(p_cutoff) OR p_cutoff>=clock_timestamp() OR p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 1000 OR p_request IS NULL OR p_reason IS NULL OR octet_length(p_reason)>512 OR length(btrim(p_reason))=0 THEN
  RAISE EXCEPTION 'invalid retention parameters' USING ERRCODE='22023';
 END IF;
 SELECT oid INTO v_oid FROM pg_catalog.pg_roles WHERE rolname=session_user;
 PERFORM 1 FROM public.novu_rust_ws_retention_grants g WHERE g.environment_id=p_env AND g.database_principal=session_user AND g.principal_oid=v_oid AND g.can_delete FOR SHARE OF g;
 IF NOT FOUND THEN RAISE EXCEPTION 'retention permission denied' USING ERRCODE='42501'; END IF;
 INSERT INTO public.novu_rust_ws_retention_requests(environment_id,database_principal,principal_oid,request_id,cutoff,batch_limit,reason)
 VALUES(p_env,session_user,v_oid,p_request,p_cutoff,p_limit,p_reason) ON CONFLICT DO NOTHING;
 GET DIAGNOSTICS v_inserted=ROW_COUNT;
 IF v_inserted=0 THEN
  SELECT * INTO r FROM public.novu_rust_ws_retention_requests WHERE environment_id=p_env AND database_principal=session_user AND principal_oid=v_oid AND request_id=p_request;
  IF NOT FOUND OR (r.cutoff,r.batch_limit,r.reason) IS DISTINCT FROM (p_cutoff,p_limit,p_reason) OR r.result IS NULL THEN RAISE EXCEPTION 'request conflict' USING ERRCODE='P4090'; END IF;
  RETURN r.result || jsonb_build_object('replayed',true);
 END IF;
 WITH candidates AS MATERIALIZED (
  SELECT id FROM public.novu_rust_ws_events WHERE environment_id=p_env AND created_at<p_cutoff ORDER BY created_at,id FOR UPDATE SKIP LOCKED LIMIT p_limit
 ), removed AS (
  DELETE FROM public.novu_rust_ws_events e USING candidates c WHERE e.id=c.id RETURNING e.created_at
 ) SELECT count(*)::integer,min(created_at),max(created_at) INTO v_count,v_oldest,v_newest FROM removed;
 INSERT INTO public.novu_rust_ws_retention_audit(environment_id,database_principal,principal_oid,cutoff,batch_limit,deleted_count,reason,request_id) VALUES(p_env,session_user,v_oid,p_cutoff,p_limit,v_count,p_reason,p_request);
 v_result:=jsonb_build_object('environmentId',p_env,'requestId',p_request,'cutoff',p_cutoff::text,'batchLimit',p_limit,'count',v_count,'oldest',v_oldest::text,'newest',v_newest::text);
 UPDATE public.novu_rust_ws_retention_requests SET result=v_result WHERE environment_id=p_env AND database_principal=session_user AND principal_oid=v_oid AND request_id=p_request;
 RETURN v_result || jsonb_build_object('replayed',false);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_ws_retention_delete_v2(text,timestamptz,integer,uuid,text) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_ws_retention_request(p_env text,p_request uuid)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_oid oid; r public.novu_rust_ws_retention_requests%ROWTYPE;
BEGIN
 IF p_env IS NULL OR octet_length(p_env) NOT BETWEEN 1 AND 512 OR p_request IS NULL THEN RAISE EXCEPTION 'invalid retention parameters' USING ERRCODE='22023'; END IF;
 SELECT oid INTO v_oid FROM pg_catalog.pg_roles WHERE rolname=session_user;
 PERFORM 1 FROM public.novu_rust_ws_retention_grants g WHERE g.environment_id=p_env AND g.database_principal=session_user AND g.principal_oid=v_oid AND g.can_delete FOR SHARE OF g;
 IF NOT FOUND THEN RAISE EXCEPTION 'retention permission denied' USING ERRCODE='42501'; END IF;
 SELECT * INTO r FROM public.novu_rust_ws_retention_requests WHERE environment_id=p_env AND database_principal=session_user AND principal_oid=v_oid AND request_id=p_request;
 IF NOT FOUND THEN RETURN jsonb_build_object('found',false); END IF;
 RETURN r.result || jsonb_build_object('found',true,'reason',r.reason,'createdAt',r.created_at::text);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_ws_retention_request(text,uuid) FROM PUBLIC;
