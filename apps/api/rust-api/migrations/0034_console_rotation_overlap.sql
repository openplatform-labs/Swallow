-- A credential can be rotated only once, including during an overlap window.
CREATE UNIQUE INDEX novu_rust_console_rotation_old_id_unique ON public.novu_rust_console_rotation_audit(old_credential_id);
ALTER TABLE public.novu_rust_console_rotation_audit ADD COLUMN old_valid_until timestamptz;

CREATE FUNCTION public.novu_rust_rotate_console_credential_overlap(p_env text,p_old uuid,p_hash text,p_expiry timestamptz,p_grace integer,p_reason text)
RETURNS TABLE(new_credential_id uuid,old_valid_until text) LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_actor oid; v_enabled boolean; v_expires timestamptz; v_subscriber boolean; v_alert boolean; v_new uuid; v_deadline timestamptz;
BEGIN
 IF p_old IS NULL OR p_grace IS NULL OR p_grace NOT BETWEEN 1 AND 86400 THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 v_actor:=public.novu_rust_lock_console_admin(p_env);
 SELECT c.enabled,c.expires_at,c.subscriber_read INTO v_enabled,v_expires,v_subscriber FROM public.novu_rust_console_credentials c
 WHERE c.environment_id=p_env AND c.credential_id=p_old FOR UPDATE OF c;
 IF NOT FOUND OR NOT v_enabled OR v_expires<=clock_timestamp() OR EXISTS(SELECT 1 FROM public.novu_rust_console_rotation_audit a WHERE a.old_credential_id=p_old) THEN
  RAISE EXCEPTION 'inactive or already rotated credential' USING ERRCODE='22023';
 END IF;
 SELECT EXISTS(SELECT 1 FROM public.novu_rust_console_alert_grants g WHERE g.credential_id=p_old) INTO v_alert;
 v_new:=public.novu_rust_issue_console_credential_permissions(p_env,p_hash,p_expiry,v_subscriber,v_alert,p_reason);
 v_deadline:=least(v_expires,clock_timestamp()+(p_grace * interval '1 second'));
 IF v_deadline<=clock_timestamp() THEN RAISE EXCEPTION 'inactive credential' USING ERRCODE='22023'; END IF;
 UPDATE public.novu_rust_console_credentials SET expires_at=v_deadline WHERE environment_id=p_env AND credential_id=p_old;
 INSERT INTO public.novu_rust_console_rotation_audit(environment_id,old_credential_id,new_credential_id,actor,actor_oid,reason,old_valid_until)
 VALUES(p_env,p_old,v_new,session_user,v_actor,p_reason,v_deadline);
 RETURN QUERY SELECT v_new,v_deadline::text;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_rotate_console_credential_overlap(text,uuid,text,timestamptz,integer,text) FROM PUBLIC;

CREATE FUNCTION public.novu_rust_list_console_rotation_audit_v2(p_env text,p_after bigint)
RETURNS TABLE(id text,old_credential_id uuid,new_credential_id uuid,actor text,reason text,created_at text,old_valid_until text)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF p_after IS NULL OR p_after<0 THEN RAISE EXCEPTION 'invalid cursor' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_lock_console_admin(p_env);
 RETURN QUERY SELECT a.id::text,a.old_credential_id,a.new_credential_id,a.actor::text,a.reason,a.created_at::text,a.old_valid_until::text
 FROM public.novu_rust_console_rotation_audit a WHERE a.environment_id=p_env AND a.id>p_after ORDER BY a.id LIMIT 100;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_list_console_rotation_audit_v2(text,bigint) FROM PUBLIC;
