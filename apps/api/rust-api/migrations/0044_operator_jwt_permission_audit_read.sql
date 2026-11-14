-- Separate EXECUTE permission permits environment-scoped audit review without table access.
CREATE INDEX novu_rust_operator_jwt_permission_audit_env_id_idx
 ON public.novu_rust_operator_jwt_permission_audit(environment_id,id DESC);
CREATE FUNCTION public.novu_rust_operator_jwt_permission_audit_page(p_env text,p_before bigint)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_actor oid; v_result jsonb;
BEGIN
 IF p_env IS NULL OR octet_length(p_env) NOT BETWEEN 1 AND 512 OR (p_before IS NOT NULL AND p_before<1) THEN
  RAISE EXCEPTION 'invalid audit lookup' USING ERRCODE='22023';
 END IF;
 SELECT oid INTO v_actor FROM pg_catalog.pg_roles WHERE rolname=session_user AND rolcanlogin;
 PERFORM 1 FROM public.novu_rust_operator_jwt_admin_grants g
 WHERE g.environment_id=p_env AND g.database_principal=session_user AND g.principal_oid=v_actor FOR SHARE OF g;
 IF NOT FOUND THEN RAISE EXCEPTION 'permission audit denied' USING ERRCODE='42501'; END IF;
 WITH candidates AS MATERIALIZED (
  SELECT id,recorded_at,issuer,subject,client_id,actor,actor_oid,reason,previous,current
  FROM public.novu_rust_operator_jwt_permission_audit
  WHERE environment_id=p_env AND (p_before IS NULL OR id<p_before)
  ORDER BY id DESC LIMIT 51
 ), page AS MATERIALIZED (SELECT * FROM candidates ORDER BY id DESC LIMIT 50)
 SELECT pg_catalog.jsonb_build_object(
  'data',coalesce((SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
   'id',id,'recordedAt',recorded_at,'issuer',issuer,'subject',subject,'clientId',client_id,
   'actor',actor,'actorOid',actor_oid,'reason',reason,'previous',previous,'current',current
  ) ORDER BY id DESC) FROM page),'[]'::jsonb),
  'next',CASE WHEN (SELECT count(*) FROM candidates)>50 THEN (SELECT min(id) FROM page) ELSE NULL END
 ) INTO v_result;
 RETURN v_result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_permission_audit_page(text,bigint) FROM PUBLIC;
