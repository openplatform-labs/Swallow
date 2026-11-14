-- Issue both independently chosen read scopes in the same transaction.
CREATE FUNCTION public.novu_rust_issue_console_credential_permissions(
 p_env text,p_hash text,p_expiry timestamptz,p_subscriber boolean,p_alert boolean,p_reason text
) RETURNS uuid LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_id uuid;
BEGIN
 IF p_subscriber IS NULL OR p_alert IS NULL THEN
  RAISE EXCEPTION 'permissions required' USING ERRCODE='22023';
 END IF;
 v_id:=public.novu_rust_issue_console_credential_scoped(p_env,p_hash,p_expiry,p_subscriber,p_reason);
 IF p_alert THEN
  PERFORM public.novu_rust_manage_console_alert_grant(p_env,v_id,'grant',p_reason);
 END IF;
 RETURN v_id;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_issue_console_credential_permissions(text,text,timestamptz,boolean,boolean,text) FROM PUBLIC;
