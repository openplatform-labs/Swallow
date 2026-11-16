CREATE TABLE public.novu_rust_resend_cancel_command_reviewers (
 environment_id text NOT NULL REFERENCES public.novu_rust_environments(id),database_principal name NOT NULL,principal_oid oid NOT NULL,
 PRIMARY KEY(environment_id,database_principal)
);
CREATE TABLE public.novu_rust_resend_cancel_command_reviews (
 environment_id text NOT NULL,request_id uuid NOT NULL,failed_updated_at timestamptz NOT NULL,database_principal name NOT NULL,principal_oid oid NOT NULL,
 reason text NOT NULL,recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(environment_id,request_id,failed_updated_at),FOREIGN KEY(environment_id,request_id) REFERENCES public.novu_rust_resend_cancel_commands(environment_id,request_id)
);
CREATE INDEX novu_rust_resend_cancel_command_failed ON public.novu_rust_resend_cancel_commands(environment_id,request_id) WHERE state='failed';
REVOKE ALL ON public.novu_rust_resend_cancel_command_reviewers,public.novu_rust_resend_cancel_command_reviews FROM PUBLIC;
CREATE FUNCTION public.novu_rust_resend_cancel_command_health(p_env text,p_grace integer) RETURNS TABLE(pending bigint,overdue bigint,unreviewed bigint)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF p_grace IS NULL OR p_grace NOT BETWEEN 1 AND 86400 THEN RAISE EXCEPTION 'invalid grace' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_assert_alert_view(p_env);
 RETURN QUERY SELECT count(*) FILTER(WHERE c.state='pending'),count(*) FILTER(WHERE c.state='pending' AND c.created_at<=statement_timestamp()-p_grace*interval '1 second'),count(*) FILTER(WHERE c.state='failed' AND NOT EXISTS(SELECT 1 FROM public.novu_rust_resend_cancel_command_reviews r WHERE r.environment_id=c.environment_id AND r.request_id=c.request_id AND r.failed_updated_at=c.updated_at)) FROM public.novu_rust_resend_cancel_commands c WHERE c.environment_id=p_env AND c.state IN ('pending','failed');
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_resend_cancel_command_health(text,integer) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_resend_cancel_command_failed_list(p_env text,p_after uuid) RETURNS TABLE(request_id uuid,job_id uuid,failed_at text)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 PERFORM public.novu_rust_assert_alert_view(p_env);
 RETURN QUERY SELECT c.request_id,c.job_id,c.updated_at::text FROM public.novu_rust_resend_cancel_commands c WHERE c.environment_id=p_env AND c.state='failed' AND (p_after IS NULL OR c.request_id>p_after) AND NOT EXISTS(SELECT 1 FROM public.novu_rust_resend_cancel_command_reviews r WHERE r.environment_id=c.environment_id AND r.request_id=c.request_id AND r.failed_updated_at=c.updated_at) ORDER BY c.request_id LIMIT 51;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_resend_cancel_command_failed_list(text,uuid) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_resend_cancel_command_ack(p_env text,p_request uuid,p_expected timestamptz,p_reason text) RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_oid oid;v_updated timestamptz;v_inserted boolean;
BEGIN
 IF p_request IS NULL OR p_expected IS NULL OR p_reason IS NULL OR length(btrim(p_reason))=0 OR octet_length(p_reason)>500 OR p_reason ~ '[[:cntrl:]]' THEN RAISE EXCEPTION 'invalid review' USING ERRCODE='22023'; END IF;
 SELECT r.oid INTO v_oid FROM public.novu_rust_resend_cancel_command_reviewers g JOIN pg_catalog.pg_roles r ON r.oid=g.principal_oid AND r.rolname=g.database_principal AND r.rolcanlogin WHERE g.environment_id=p_env AND g.database_principal=session_user FOR SHARE OF g;
 IF NOT FOUND THEN RAISE EXCEPTION 'review denied' USING ERRCODE='42501'; END IF;
 SELECT updated_at INTO v_updated FROM public.novu_rust_resend_cancel_commands WHERE environment_id=p_env AND request_id=p_request AND state='failed' FOR UPDATE;
 IF NOT FOUND OR v_updated IS DISTINCT FROM p_expected THEN RAISE EXCEPTION 'failure changed' USING ERRCODE='55000'; END IF;
 INSERT INTO public.novu_rust_resend_cancel_command_reviews(environment_id,request_id,failed_updated_at,database_principal,principal_oid,reason) VALUES(p_env,p_request,v_updated,session_user,v_oid,p_reason) ON CONFLICT DO NOTHING RETURNING true INTO v_inserted;
 RETURN coalesce(v_inserted,false);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_resend_cancel_command_ack(text,uuid,timestamptz,text) FROM PUBLIC;
