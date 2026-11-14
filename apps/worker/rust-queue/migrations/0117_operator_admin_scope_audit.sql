-- Scope administrators are separately bootstrapped by a trusted DBA.
CREATE TABLE IF NOT EXISTS public.novu_rust_operator_scope_admins (
 environment_id text NOT NULL REFERENCES public.novu_rust_environments(id),
 database_principal text NOT NULL,
 principal_oid oid NOT NULL,
 PRIMARY KEY(environment_id,database_principal)
);
CREATE TABLE IF NOT EXISTS public.novu_rust_operator_scope_audit (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 environment_id text NOT NULL,
 actor_session_user text NOT NULL,
 actor_oid oid NOT NULL,
 target_session_user text NOT NULL,
 target_oid oid NOT NULL,
 previous_grant_scope text[], previous_revoke_scope text[],
 grant_scope text[], revoke_scope text[],
 reason text NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS novu_rust_operator_scope_audit_environment_id_idx
 ON public.novu_rust_operator_scope_audit(environment_id,id);
REVOKE ALL ON public.novu_rust_operator_scope_admins,public.novu_rust_operator_scope_audit FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.novu_rust_set_operator_admin_scope(
 p_environment text,p_target text,p_grant_scope text[],p_revoke_scope text[],p_reason text
) RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE actor_id oid; target_id oid; old_grant text[]; old_revoke text[];
BEGIN
 IF p_environment IS NULL OR length(btrim(p_environment))=0 OR p_target IS NULL
   OR octet_length(p_target) NOT BETWEEN 1 AND 63 OR p_reason IS NULL
   OR length(btrim(p_reason))=0 OR octet_length(p_reason)>512 THEN
   RAISE EXCEPTION 'invalid operator scope request' USING ERRCODE='22023';
 END IF;
 SELECT r.oid INTO actor_id FROM pg_catalog.pg_roles r WHERE r.rolname=session_user AND r.rolcanlogin;
 PERFORM 1 FROM public.novu_rust_operator_scope_admins a
 WHERE a.environment_id=p_environment AND a.database_principal=session_user AND a.principal_oid=actor_id FOR SHARE OF a;
 IF NOT FOUND THEN RAISE EXCEPTION 'operator scope administration denied' USING ERRCODE='42501'; END IF;
 SELECT r.oid INTO target_id FROM pg_catalog.pg_roles r WHERE r.rolname=p_target AND r.rolcanlogin;
 SELECT a.grant_scope,a.revoke_scope INTO old_grant,old_revoke FROM public.novu_rust_operator_grant_admins a
 WHERE a.environment_id=p_environment AND a.database_principal=p_target AND a.principal_oid=target_id FOR UPDATE OF a;
 IF NOT FOUND THEN RAISE EXCEPTION 'current operator grant administrator not found' USING ERRCODE='22023'; END IF;
 IF old_grant IS NOT DISTINCT FROM p_grant_scope AND old_revoke IS NOT DISTINCT FROM p_revoke_scope THEN RETURN false; END IF;
 UPDATE public.novu_rust_operator_grant_admins SET grant_scope=p_grant_scope,revoke_scope=p_revoke_scope
 WHERE environment_id=p_environment AND database_principal=p_target;
 INSERT INTO public.novu_rust_operator_scope_audit(environment_id,actor_session_user,actor_oid,target_session_user,target_oid,previous_grant_scope,previous_revoke_scope,grant_scope,revoke_scope,reason)
 VALUES(p_environment,session_user,actor_id,p_target,target_id,old_grant,old_revoke,p_grant_scope,p_revoke_scope,p_reason);
 RETURN true;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_set_operator_admin_scope(text,text,text[],text[],text) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.novu_rust_list_operator_scope_audit(p_environment text,p_after bigint)
RETURNS TABLE(audit_id bigint,actor text,target text,previous_grant_scope text[],previous_revoke_scope text[],grant_scope text[],revoke_scope text[],reason text,created_at text)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF p_environment IS NULL OR length(btrim(p_environment))=0 OR (p_after IS NOT NULL AND p_after<0) THEN
   RAISE EXCEPTION 'invalid operator scope audit cursor' USING ERRCODE='22023';
 END IF;
 PERFORM 1 FROM public.novu_rust_operator_scope_admins a
 WHERE a.environment_id=p_environment AND a.database_principal=session_user
 AND a.principal_oid=(SELECT r.oid FROM pg_catalog.pg_roles r WHERE r.rolname=session_user AND r.rolcanlogin) FOR SHARE OF a;
 IF NOT FOUND THEN RAISE EXCEPTION 'operator scope administration denied' USING ERRCODE='42501'; END IF;
 RETURN QUERY SELECT a.id,a.actor_session_user,a.target_session_user,a.previous_grant_scope,a.previous_revoke_scope,a.grant_scope,a.revoke_scope,a.reason,a.created_at::text
 FROM public.novu_rust_operator_scope_audit a WHERE a.environment_id=p_environment AND (p_after IS NULL OR a.id>p_after)
 ORDER BY a.id LIMIT 100;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_list_operator_scope_audit(text,bigint) FROM PUBLIC;
