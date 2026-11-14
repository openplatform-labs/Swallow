-- Optional, environment-scoped administration of JWT identity permissions.
-- Trust registration and EXECUTE grants are made by a separate DBA.
CREATE TABLE public.novu_rust_operator_jwt_admin_grants (
 environment_id text NOT NULL REFERENCES public.novu_rust_environments(id),
 database_principal name NOT NULL,
 principal_oid oid NOT NULL,
 PRIMARY KEY(environment_id,database_principal)
);
CREATE TABLE public.novu_rust_operator_jwt_permission_audit (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 environment_id text NOT NULL,
 issuer text NOT NULL,
 subject text NOT NULL,
 client_id text NOT NULL,
 actor name NOT NULL,
 actor_oid oid NOT NULL,
 reason text NOT NULL,
 previous jsonb,
 current jsonb NOT NULL
);
REVOKE ALL ON public.novu_rust_operator_jwt_admin_grants,public.novu_rust_operator_jwt_permission_audit FROM PUBLIC;

CREATE FUNCTION public.novu_rust_operator_jwt_set_permissions(
 p_env text,p_issuer text,p_subject text,p_client text,
 p_enabled boolean,p_subscriber_read boolean,p_alert_read boolean,p_alert_retry boolean,p_alert_accepted boolean,p_reason text
) RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_actor oid; v_old public.novu_rust_operator_jwt_identities%ROWTYPE; v_new public.novu_rust_operator_jwt_identities%ROWTYPE; v_exists boolean;
BEGIN
 IF p_env IS NULL OR p_issuer IS NULL OR p_subject IS NULL OR p_client IS NULL OR
    octet_length(p_env) NOT BETWEEN 1 AND 512 OR octet_length(p_issuer) NOT BETWEEN 1 AND 2048 OR
    octet_length(p_subject) NOT BETWEEN 1 AND 1024 OR octet_length(p_client) NOT BETWEEN 1 AND 1024 OR
    p_enabled IS NULL OR p_subscriber_read IS NULL OR p_alert_read IS NULL OR p_alert_retry IS NULL OR p_alert_accepted IS NULL OR
    p_reason IS NULL OR octet_length(p_reason) NOT BETWEEN 1 AND 1000 OR btrim(p_reason)='' THEN
  RAISE EXCEPTION 'invalid permission change' USING ERRCODE='22023';
 END IF;
 SELECT oid INTO v_actor FROM pg_catalog.pg_roles WHERE rolname=session_user AND rolcanlogin;
 PERFORM 1 FROM public.novu_rust_operator_jwt_admin_grants g WHERE g.environment_id=p_env AND g.database_principal=session_user AND g.principal_oid=v_actor FOR SHARE OF g;
 IF NOT FOUND THEN RAISE EXCEPTION 'permission administration denied' USING ERRCODE='42501'; END IF;
 -- Serialize cooperating first registrations as well as updates to an existing row.
 PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(pg_catalog.jsonb_build_array(p_env,p_issuer,p_subject,p_client)::text,0));
 SELECT * INTO v_old FROM public.novu_rust_operator_jwt_identities i
 WHERE i.environment_id=p_env AND i.issuer=p_issuer AND i.subject=p_subject AND i.client_id=p_client FOR UPDATE;
 v_exists:=FOUND;
 IF v_exists AND (v_old.enabled,v_old.subscriber_read,v_old.alert_read,v_old.alert_retry,v_old.alert_accepted)
   IS NOT DISTINCT FROM (p_enabled,p_subscriber_read,p_alert_read,p_alert_retry,p_alert_accepted) THEN RETURN false; END IF;
 IF v_exists THEN
  UPDATE public.novu_rust_operator_jwt_identities SET enabled=p_enabled,subscriber_read=p_subscriber_read,alert_read=p_alert_read,alert_retry=p_alert_retry,alert_accepted=p_alert_accepted
  WHERE environment_id=p_env AND issuer=p_issuer AND subject=p_subject AND client_id=p_client RETURNING * INTO v_new;
 ELSE
  INSERT INTO public.novu_rust_operator_jwt_identities(environment_id,issuer,subject,client_id,enabled,subscriber_read,alert_read,alert_retry,alert_accepted)
  VALUES(p_env,p_issuer,p_subject,p_client,p_enabled,p_subscriber_read,p_alert_read,p_alert_retry,p_alert_accepted) RETURNING * INTO v_new;
 END IF;
 INSERT INTO public.novu_rust_operator_jwt_permission_audit(environment_id,issuer,subject,client_id,actor,actor_oid,reason,previous,current)
 VALUES(p_env,p_issuer,p_subject,p_client,session_user,v_actor,p_reason,CASE WHEN v_exists THEN pg_catalog.to_jsonb(v_old) ELSE NULL END,pg_catalog.to_jsonb(v_new));
 RETURN true;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_set_permissions(text,text,text,text,boolean,boolean,boolean,boolean,boolean,text) FROM PUBLIC;

CREATE FUNCTION public.novu_rust_operator_jwt_get_permissions(p_env text,p_issuer text,p_subject text,p_client text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_actor oid; v_identity public.novu_rust_operator_jwt_identities%ROWTYPE;
BEGIN
 IF p_env IS NULL OR p_issuer IS NULL OR p_subject IS NULL OR p_client IS NULL OR
    octet_length(p_env) NOT BETWEEN 1 AND 512 OR octet_length(p_issuer) NOT BETWEEN 1 AND 2048 OR
    octet_length(p_subject) NOT BETWEEN 1 AND 1024 OR octet_length(p_client) NOT BETWEEN 1 AND 1024 THEN
  RAISE EXCEPTION 'invalid permission lookup' USING ERRCODE='22023';
 END IF;
 SELECT oid INTO v_actor FROM pg_catalog.pg_roles WHERE rolname=session_user AND rolcanlogin;
 PERFORM 1 FROM public.novu_rust_operator_jwt_admin_grants g WHERE g.environment_id=p_env AND g.database_principal=session_user AND g.principal_oid=v_actor FOR SHARE OF g;
 IF NOT FOUND THEN RAISE EXCEPTION 'permission administration denied' USING ERRCODE='42501'; END IF;
 SELECT * INTO v_identity FROM public.novu_rust_operator_jwt_identities i
 WHERE i.environment_id=p_env AND i.issuer=p_issuer AND i.subject=p_subject AND i.client_id=p_client FOR SHARE;
 IF NOT FOUND THEN RETURN NULL; END IF;
 RETURN pg_catalog.to_jsonb(v_identity);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_get_permissions(text,text,text,text) FROM PUBLIC;
