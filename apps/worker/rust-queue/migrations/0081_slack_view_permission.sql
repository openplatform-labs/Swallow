ALTER TABLE public.novu_rust_delivery_operator_grants DROP CONSTRAINT IF EXISTS novu_rust_delivery_operator_grants_decision_check;
ALTER TABLE public.novu_rust_delivery_operator_grants ADD CONSTRAINT novu_rust_delivery_operator_grants_decision_check CHECK (decision IN ('retry','accepted','alert_send','alert_retry','alert_accepted','slack_retry','slack_accepted','slack_ack','slack_view'));
CREATE OR REPLACE FUNCTION public.novu_rust_lock_operator_grant(p_environment text,p_decision text)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
BEGIN
    IF p_decision NOT IN ('retry','accepted','alert_send','alert_retry','alert_accepted','slack_retry','slack_accepted','slack_ack','slack_view') THEN RETURN false; END IF;
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
      OR p_permission NOT IN ('retry','accepted','alert_send','alert_retry','alert_accepted','slack_retry','slack_accepted','slack_ack','slack_view')
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

-- This function exposes only bounded non-payload metadata for an authorized environment.
CREATE OR REPLACE FUNCTION public.novu_rust_list_slack_attention(p_environment text,p_after uuid)
RETURNS TABLE(job_id uuid,kind text,job_status text,attempts integer,http_status integer,category text)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
BEGIN
  IF p_environment IS NULL OR length(btrim(p_environment))=0 THEN
    RAISE EXCEPTION 'environment required' USING ERRCODE='22023';
  END IF;
  IF NOT public.novu_rust_lock_operator_grant(p_environment,'slack_view') THEN
    RAISE EXCEPTION 'Slack view permission denied' USING ERRCODE='42501';
  END IF;
  IF NOT EXISTS(SELECT 1 FROM public.novu_rust_environments e WHERE e.id=p_environment) THEN
    RAISE EXCEPTION 'environment not found' USING ERRCODE='22023';
  END IF;
  RETURN QUERY
    SELECT j.id,j.kind,j.status,j.attempts,a.http_status,
      CASE WHEN a.state='unknown' THEN 'unknownAttempt'
        WHEN a.state='rejected' THEN 'rejectedAttempt'
        WHEN a.state='dispatching' AND (j.status<>'running' OR j.lease_until<=statement_timestamp()) THEN 'strandedDispatch'
        WHEN j.status='failed' THEN 'preDispatchFailure' ELSE 'missingEndpoint' END
    FROM public.novu_queue_jobs j
    LEFT JOIN public.novu_rust_slack_attempts a ON a.job_id=j.id AND a.environment_id=j.environment_id
    WHERE j.environment_id=p_environment AND (p_after IS NULL OR j.id>p_after) AND (
      (j.kind IN ('experimental_slack_chat','experimental_slack_webhook','experimental_slack_workflow_delivery') AND
        (a.state IN ('unknown','rejected') OR (a.state='dispatching' AND (j.status<>'running' OR j.lease_until<=statement_timestamp()))))
      OR (j.kind IN ('experimental_slack_workflow_fanout','experimental_slack_workflow_delivery') AND j.status='failed' AND j.last_error IS NOT NULL
        AND NOT EXISTS(SELECT 1 FROM public.novu_rust_slack_attempts x WHERE x.job_id=j.id)
        AND NOT EXISTS(SELECT 1 FROM public.novu_rust_slack_workflow_acknowledgements z WHERE z.environment_id=j.environment_id AND z.job_id=j.id AND z.job_attempts=j.attempts AND z.job_status=j.status AND z.job_error=j.last_error))
      OR (j.kind='experimental_slack_workflow_fanout' AND j.status='cancelled' AND j.last_error='no Slack endpoints'
        AND NOT EXISTS(SELECT 1 FROM public.novu_rust_slack_workflow_acknowledgements z WHERE z.environment_id=j.environment_id AND z.job_id=j.id AND z.job_attempts=j.attempts AND z.job_status=j.status AND z.job_error=j.last_error))
    ) ORDER BY j.id LIMIT 100;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_list_slack_attention(text,uuid) FROM PUBLIC;
