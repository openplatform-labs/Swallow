-- Install as a trusted owner; bootstrap administrators separately.
CREATE TABLE IF NOT EXISTS public.novu_rust_operator_grant_admins (
    environment_id text NOT NULL REFERENCES public.novu_rust_environments(id),
    database_principal text NOT NULL,
    PRIMARY KEY (environment_id, database_principal)
);
CREATE TABLE IF NOT EXISTS public.novu_rust_operator_grant_audit (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    environment_id text NOT NULL,
    actor_session_user text NOT NULL,
    target_session_user text NOT NULL,
    permission text NOT NULL,
    action text NOT NULL CHECK (action IN ('grant','revoke')),
    reason text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
REVOKE ALL ON public.novu_rust_operator_grant_admins, public.novu_rust_operator_grant_audit FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.novu_rust_manage_operator_grant(
    p_environment text, p_principal text, p_permission text, p_action text, p_reason text
) RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE changed integer;
BEGIN
    IF p_environment IS NULL OR p_principal IS NULL OR p_permission IS NULL OR p_action IS NULL OR p_reason IS NULL
        OR p_action NOT IN ('grant','revoke')
        OR p_permission NOT IN ('retry','accepted','alert_send','alert_retry','alert_accepted','slack_retry','slack_accepted')
        OR length(btrim(p_reason))=0 OR octet_length(p_reason)>512
        OR octet_length(p_principal)>63 OR length(p_principal)=0 THEN
        RAISE EXCEPTION 'invalid operator grant request' USING ERRCODE='22023';
    END IF;
    PERFORM 1 FROM public.novu_rust_operator_grant_admins
        WHERE environment_id=p_environment AND database_principal=session_user FOR SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'operator grant administration denied' USING ERRCODE='42501';
    END IF;
    IF p_action='grant' THEN
        IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname=p_principal AND rolcanlogin) THEN
            RAISE EXCEPTION 'operator login not found' USING ERRCODE='22023';
        END IF;
        INSERT INTO public.novu_rust_delivery_operator_grants(environment_id,database_principal,decision)
            VALUES (p_environment,p_principal,p_permission) ON CONFLICT DO NOTHING;
    ELSE
        DELETE FROM public.novu_rust_delivery_operator_grants
            WHERE environment_id=p_environment AND database_principal=p_principal AND decision=p_permission;
    END IF;
    GET DIAGNOSTICS changed = ROW_COUNT;
    IF changed=1 THEN
        INSERT INTO public.novu_rust_operator_grant_audit(environment_id,actor_session_user,target_session_user,permission,action,reason)
            VALUES (p_environment,session_user,p_principal,p_permission,p_action,p_reason);
    END IF;
    RETURN changed=1;
END;
$$;
REVOKE ALL ON FUNCTION public.novu_rust_manage_operator_grant(text,text,text,text,text) FROM PUBLIC;
