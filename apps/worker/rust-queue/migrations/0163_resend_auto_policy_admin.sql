-- Separate administration rights from dispatch and from reconciliation.
ALTER TABLE public.novu_rust_resend_auto_retry_policy ADD COLUMN IF NOT EXISTS revision bigint NOT NULL DEFAULT 0 CHECK (revision >= 0);
CREATE TABLE public.novu_rust_resend_auto_policy_admins (
 environment_id text NOT NULL REFERENCES public.novu_rust_environments(id),
 database_principal text NOT NULL,
 principal_oid oid NOT NULL,
 can_read boolean NOT NULL DEFAULT false,
 can_write boolean NOT NULL DEFAULT false,
 can_audit boolean NOT NULL DEFAULT false,
 PRIMARY KEY (environment_id,database_principal)
);
REVOKE ALL ON public.novu_rust_resend_auto_policy_admins FROM PUBLIC;
CREATE TABLE public.novu_rust_resend_auto_policy_audit (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 environment_id text NOT NULL REFERENCES public.novu_rust_environments(id),
 database_principal text NOT NULL,
 previous_policy jsonb,
 current_policy jsonb NOT NULL,
 reason text NOT NULL CHECK (octet_length(reason) BETWEEN 1 AND 512),
 recorded_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX novu_rust_resend_auto_policy_audit_env_idx ON public.novu_rust_resend_auto_policy_audit(environment_id,id DESC);
REVOKE ALL ON public.novu_rust_resend_auto_policy_audit FROM PUBLIC;

CREATE FUNCTION public.novu_rust_resend_auto_policy_access(p_environment text,p_access text)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF p_environment IS NULL OR length(p_environment)=0 OR p_access IS NULL OR p_access NOT IN ('read','write','audit') THEN
  RAISE EXCEPTION 'invalid policy parameters' USING ERRCODE='22023';
 END IF;
 PERFORM 1 FROM public.novu_rust_resend_auto_policy_admins g
 WHERE g.environment_id=p_environment AND g.database_principal=session_user
  AND g.principal_oid=(SELECT oid FROM pg_catalog.pg_roles WHERE rolname=session_user AND rolcanlogin)
  AND CASE p_access WHEN 'read' THEN g.can_read WHEN 'write' THEN g.can_write ELSE g.can_audit END
 FOR SHARE OF g;
 IF NOT FOUND THEN RAISE EXCEPTION 'policy access denied' USING ERRCODE='42501'; END IF;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_resend_auto_policy_access(text,text) FROM PUBLIC;

CREATE FUNCTION public.novu_rust_resend_auto_policy_read(p_environment text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_result jsonb;
BEGIN
 PERFORM public.novu_rust_resend_auto_policy_access(p_environment,'read');
 SELECT jsonb_build_object('exists',true,'enabled',enabled,'maxRetries',max_retries,'notBefore',not_before::text,'revision',revision::text)
  INTO v_result FROM public.novu_rust_resend_auto_retry_policy WHERE environment_id=p_environment;
 RETURN coalesce(v_result,jsonb_build_object('exists',false));
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_resend_auto_policy_read(text) FROM PUBLIC;

CREATE FUNCTION public.novu_rust_resend_auto_policy_set(
 p_environment text,p_expected bigint,p_enabled boolean,p_max integer,p_cutoff text,p_reason text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_old public.novu_rust_resend_auto_retry_policy%ROWTYPE;
 v_new public.novu_rust_resend_auto_retry_policy%ROWTYPE; v_previous jsonb; v_current jsonb;
BEGIN
 IF p_expected IS NULL OR p_expected < -1 OR p_enabled IS NULL OR p_max IS NULL OR p_max NOT BETWEEN 1 AND 10
  OR p_cutoff IS NULL OR p_cutoff NOT IN ('now','preserve') OR p_reason IS NULL
  OR length(btrim(p_reason))=0 OR octet_length(p_reason)>512 THEN
  RAISE EXCEPTION 'invalid policy parameters' USING ERRCODE='22023';
 END IF;
 PERFORM public.novu_rust_resend_auto_policy_access(p_environment,'write');
 SELECT * INTO v_old FROM public.novu_rust_resend_auto_retry_policy WHERE environment_id=p_environment FOR UPDATE;
 IF FOUND THEN
  IF p_expected<>v_old.revision THEN RAISE EXCEPTION 'stale policy revision' USING ERRCODE='40001'; END IF;
  v_previous := jsonb_build_object('enabled',v_old.enabled,'maxRetries',v_old.max_retries,'notBefore',v_old.not_before::text,'revision',v_old.revision::text);
  UPDATE public.novu_rust_resend_auto_retry_policy SET enabled=p_enabled,max_retries=p_max,
   not_before=CASE WHEN p_cutoff='now' THEN clock_timestamp() ELSE v_old.not_before END, revision=revision+1
   WHERE environment_id=p_environment RETURNING * INTO v_new;
 ELSE
  IF p_expected<>-1 OR p_cutoff<>'now' THEN RAISE EXCEPTION 'policy absent or stale' USING ERRCODE='40001'; END IF;
  INSERT INTO public.novu_rust_resend_auto_retry_policy(environment_id,enabled,max_retries,not_before,revision)
   VALUES(p_environment,p_enabled,p_max,clock_timestamp(),0) ON CONFLICT(environment_id) DO NOTHING RETURNING * INTO v_new;
  IF NOT FOUND THEN RAISE EXCEPTION 'policy created concurrently' USING ERRCODE='40001'; END IF;
 END IF;
 v_current := jsonb_build_object('enabled',v_new.enabled,'maxRetries',v_new.max_retries,'notBefore',v_new.not_before::text,'revision',v_new.revision::text);
 INSERT INTO public.novu_rust_resend_auto_policy_audit(environment_id,database_principal,previous_policy,current_policy,reason)
  VALUES(p_environment,session_user,v_previous,v_current,p_reason);
 RETURN v_current;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_resend_auto_policy_set(text,bigint,boolean,integer,text,text) FROM PUBLIC;

CREATE FUNCTION public.novu_rust_resend_auto_policy_audit_read(p_environment text,p_before bigint)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_result jsonb;
BEGIN
 IF p_before IS NOT NULL AND p_before<=0 THEN RAISE EXCEPTION 'invalid cursor' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_resend_auto_policy_access(p_environment,'audit');
 WITH candidates AS MATERIALIZED (SELECT * FROM public.novu_rust_resend_auto_policy_audit
  WHERE environment_id=p_environment AND (p_before IS NULL OR id<p_before) ORDER BY id DESC LIMIT 51),
 page AS MATERIALIZED (SELECT * FROM candidates ORDER BY id DESC LIMIT 50)
 SELECT jsonb_build_object('data',coalesce((SELECT jsonb_agg(jsonb_build_object('id',id::text,'databasePrincipal',database_principal,'previous',previous_policy,'current',current_policy,'reason',reason,'recordedAt',recorded_at::text) ORDER BY id DESC) FROM page),'[]'::jsonb),
  'next',CASE WHEN (SELECT count(*) FROM candidates)>50 THEN (SELECT min(id)::text FROM page) ELSE NULL END) INTO v_result;
 RETURN v_result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_resend_auto_policy_audit_read(text,bigint) FROM PUBLIC;
