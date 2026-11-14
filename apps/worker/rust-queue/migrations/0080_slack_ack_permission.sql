ALTER TABLE public.novu_rust_delivery_operator_grants DROP CONSTRAINT IF EXISTS novu_rust_delivery_operator_grants_decision_check;
ALTER TABLE public.novu_rust_delivery_operator_grants ADD CONSTRAINT novu_rust_delivery_operator_grants_decision_check CHECK (decision IN ('retry','accepted','alert_send','alert_retry','alert_accepted','slack_retry','slack_accepted','slack_ack'));
CREATE OR REPLACE FUNCTION public.novu_rust_lock_operator_grant(p_environment text,p_decision text)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
BEGIN
    IF p_decision NOT IN ('retry','accepted','alert_send','alert_retry','alert_accepted','slack_retry','slack_accepted','slack_ack') THEN RETURN false; END IF;
    PERFORM 1 FROM public.novu_rust_delivery_operator_grants g
      WHERE g.environment_id=p_environment AND g.database_principal=session_user AND g.decision=p_decision
        AND g.principal_oid=(SELECT r.oid FROM pg_catalog.pg_roles r WHERE r.rolname=session_user AND r.rolcanlogin)
      FOR SHARE OF g;
    RETURN FOUND;
END;
$$;
REVOKE ALL ON FUNCTION public.novu_rust_lock_operator_grant(text,text) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.novu_rust_manage_operator_grant(
    p_environment text,p_principal text,p_permission text,p_action text,p_reason text
) RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE changed integer; actor_id oid; target_id oid;
BEGIN
    IF p_environment IS NULL OR p_principal IS NULL OR p_permission IS NULL OR p_action IS NULL OR p_reason IS NULL
      OR p_action NOT IN ('grant','revoke')
      OR p_permission NOT IN ('retry','accepted','alert_send','alert_retry','alert_accepted','slack_retry','slack_accepted','slack_ack')
      OR length(btrim(p_reason))=0 OR octet_length(p_reason)>512
      OR octet_length(p_principal)>63 OR length(p_principal)=0 THEN
      RAISE EXCEPTION 'invalid operator grant request' USING ERRCODE='22023';
    END IF;
    SELECT oid INTO actor_id FROM pg_catalog.pg_roles WHERE rolname=session_user AND rolcanlogin;
    PERFORM 1 FROM public.novu_rust_operator_grant_admins
      WHERE environment_id=p_environment AND database_principal=session_user AND principal_oid=actor_id FOR SHARE;
    IF NOT FOUND THEN RAISE EXCEPTION 'operator grant administration denied' USING ERRCODE='42501'; END IF;
    SELECT oid INTO target_id FROM pg_catalog.pg_roles WHERE rolname=p_principal AND rolcanlogin;
    IF p_action='grant' THEN
        IF target_id IS NULL THEN RAISE EXCEPTION 'operator login not found' USING ERRCODE='22023'; END IF;
        INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision,principal_oid)
          VALUES (p_environment,p_principal,p_permission,target_id)
          ON CONFLICT (environment_id,database_principal,decision) DO UPDATE SET principal_oid=EXCLUDED.principal_oid
          WHERE public.novu_rust_delivery_operator_grants.principal_oid IS DISTINCT FROM EXCLUDED.principal_oid;
    ELSE
        DELETE FROM public.novu_rust_delivery_operator_grants
          WHERE environment_id=p_environment AND database_principal=p_principal AND decision=p_permission
          RETURNING principal_oid INTO target_id;
    END IF;
    GET DIAGNOSTICS changed = ROW_COUNT;
    IF changed=1 THEN
      INSERT INTO public.novu_rust_operator_grant_audit(environment_id,actor_session_user,target_session_user,permission,action,reason,actor_oid,target_oid)
        VALUES (p_environment,session_user,p_principal,p_permission,p_action,p_reason,actor_id,target_id);
    END IF;
    RETURN changed=1;
END;
$$;
REVOKE ALL ON FUNCTION public.novu_rust_manage_operator_grant(text,text,text,text,text) FROM PUBLIC;
