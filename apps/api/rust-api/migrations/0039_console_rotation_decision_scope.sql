-- Explicitly selected decision scopes are copied in the rotation transaction.
CREATE FUNCTION public.novu_rust_rotate_console_credential_scoped(
 p_env text,p_old uuid,p_hash text,p_expiry timestamptz,p_grace integer,p_scope text,p_reason text
) RETURNS TABLE(new_credential_id uuid,old_valid_until text)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_retry boolean; v_accepted boolean; v_copy_retry boolean; v_copy_accepted boolean; v_new uuid; v_until text;
BEGIN
 IF p_old IS NULL OR p_grace IS NULL OR p_grace NOT BETWEEN 0 AND 86400 OR p_scope IS NULL OR p_scope NOT IN ('none','copy','retry','accepted','both') THEN
  RAISE EXCEPTION 'invalid rotation request' USING ERRCODE='22023';
 END IF;
 PERFORM public.novu_rust_lock_console_admin(p_env);
 -- Every supported grant change locks the credential first; retain that lock through rotation.
 PERFORM 1 FROM public.novu_rust_console_credentials c WHERE c.environment_id=p_env AND c.credential_id=p_old FOR UPDATE OF c;
 IF NOT FOUND THEN RAISE EXCEPTION 'credential not found' USING ERRCODE='22023'; END IF;
 SELECT EXISTS(SELECT 1 FROM public.novu_rust_console_alert_decision_grants g WHERE g.credential_id=p_old AND g.decision='retry'),
        EXISTS(SELECT 1 FROM public.novu_rust_console_alert_decision_grants g WHERE g.credential_id=p_old AND g.decision='accepted') INTO v_retry,v_accepted;
 v_copy_retry:=p_scope IN ('retry','both') OR (p_scope='copy' AND v_retry);
 v_copy_accepted:=p_scope IN ('accepted','both') OR (p_scope='copy' AND v_accepted);
 IF (v_copy_retry AND NOT v_retry) OR (v_copy_accepted AND NOT v_accepted) THEN
  RAISE EXCEPTION 'requested decision scope not held by original credential' USING ERRCODE='22023';
 END IF;
 IF p_grace=0 THEN
  v_new:=public.novu_rust_rotate_console_credential(p_env,p_old,p_hash,p_expiry,p_reason);
 ELSE
  SELECT r.new_credential_id,r.old_valid_until INTO v_new,v_until FROM public.novu_rust_rotate_console_credential_overlap(p_env,p_old,p_hash,p_expiry,p_grace,p_reason) r;
 END IF;
 IF v_copy_retry THEN PERFORM public.novu_rust_manage_console_alert_decision(p_env,v_new,'retry','grant',p_reason); END IF;
 IF v_copy_accepted THEN PERFORM public.novu_rust_manage_console_alert_decision(p_env,v_new,'accepted','grant',p_reason); END IF;
 RETURN QUERY SELECT v_new,v_until;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_rotate_console_credential_scoped(text,uuid,text,timestamptz,integer,text,text) FROM PUBLIC;
