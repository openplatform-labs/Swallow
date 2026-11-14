-- NULL is unrestricted for backwards compatibility; empty arrays deny all.
-- Changing a scope locks the same administrator row used by delegation.
ALTER TABLE public.novu_rust_operator_grant_admins ADD COLUMN IF NOT EXISTS grant_scope text[];
ALTER TABLE public.novu_rust_operator_grant_admins ADD COLUMN IF NOT EXISTS revoke_scope text[];
DO $$ BEGIN
 IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_constraint WHERE conrelid='public.novu_rust_operator_grant_admins'::regclass AND conname='novu_rust_operator_admin_scope_check') THEN
  ALTER TABLE public.novu_rust_operator_grant_admins ADD CONSTRAINT novu_rust_operator_admin_scope_check CHECK (
    (grant_scope IS NULL OR (coalesce(array_ndims(grant_scope),1)=1 AND array_position(grant_scope,NULL) IS NULL AND grant_scope <@ ARRAY['retry','accepted','alert_send','alert_retry','alert_accepted','slack_retry','slack_accepted','slack_ack','slack_view','alert_view','queue_ack']::text[])) AND
    (revoke_scope IS NULL OR (coalesce(array_ndims(revoke_scope),1)=1 AND array_position(revoke_scope,NULL) IS NULL AND revoke_scope <@ ARRAY['retry','accepted','alert_send','alert_retry','alert_accepted','slack_retry','slack_accepted','slack_ack','slack_view','alert_view','queue_ack']::text[]))
  );
 END IF;
END $$;
CREATE OR REPLACE FUNCTION public.novu_rust_manage_operator_grant(
    p_environment text,p_principal text,p_permission text,p_action text,p_reason text
) RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE changed integer; actor_id oid; target_id oid; allowed_grants text[]; allowed_revokes text[];
BEGIN
    IF p_environment IS NULL OR p_principal IS NULL OR p_permission IS NULL OR p_action IS NULL OR p_reason IS NULL
      OR p_action NOT IN ('grant','revoke')
      OR p_permission NOT IN ('retry','accepted','alert_send','alert_retry','alert_accepted','slack_retry','slack_accepted','slack_ack','slack_view','alert_view','queue_ack')
      OR length(btrim(p_reason))=0 OR octet_length(p_reason)>512
      OR octet_length(p_principal)>63 OR length(p_principal)=0 THEN
      RAISE EXCEPTION 'invalid operator grant request' USING ERRCODE='22023';
    END IF;
    SELECT oid INTO actor_id FROM pg_catalog.pg_roles WHERE rolname=session_user AND rolcanlogin;
    SELECT grant_scope,revoke_scope INTO allowed_grants,allowed_revokes FROM public.novu_rust_operator_grant_admins
      WHERE environment_id=p_environment AND database_principal=session_user AND principal_oid=actor_id FOR SHARE;
    IF NOT FOUND THEN RAISE EXCEPTION 'operator grant administration denied' USING ERRCODE='42501'; END IF;
    IF (p_action='grant' AND allowed_grants IS NOT NULL AND NOT coalesce(p_permission=ANY(allowed_grants),false))
       OR (p_action='revoke' AND allowed_revokes IS NOT NULL AND NOT coalesce(p_permission=ANY(allowed_revokes),false)) THEN
      RAISE EXCEPTION 'operator permission outside delegation scope' USING ERRCODE='42501';
    END IF;
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

CREATE OR REPLACE FUNCTION public.novu_rust_current_operator_admin_scope(p_environment text)
RETURNS TABLE(grant_scope text[], revoke_scope text[])
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 RETURN QUERY SELECT a.grant_scope,a.revoke_scope FROM public.novu_rust_operator_grant_admins a
 WHERE a.environment_id=p_environment AND a.database_principal=session_user
 AND a.principal_oid=(SELECT r.oid FROM pg_catalog.pg_roles r WHERE r.rolname=session_user AND r.rolcanlogin)
 FOR SHARE OF a;
 IF NOT FOUND THEN RAISE EXCEPTION 'operator grant administration denied' USING ERRCODE='42501'; END IF;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_current_operator_admin_scope(text) FROM PUBLIC;
