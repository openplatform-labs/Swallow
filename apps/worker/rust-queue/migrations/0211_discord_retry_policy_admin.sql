-- Policy revisions ignore identical worker startup registrations.
ALTER TABLE public.novu_rust_discord_retry_policy ADD COLUMN revision bigint NOT NULL DEFAULT 0 CHECK(revision>=0);
CREATE FUNCTION public.novu_rust_discord_retry_policy_revision() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
 IF (NEW.mode,NEW.max_retries,NEW.fallback_seconds) IS DISTINCT FROM (OLD.mode,OLD.max_retries,OLD.fallback_seconds) THEN NEW.revision:=OLD.revision+1; ELSE NEW.revision:=OLD.revision; END IF;
 RETURN NEW;
END; $$;
CREATE TRIGGER novu_rust_discord_retry_policy_revision BEFORE UPDATE ON public.novu_rust_discord_retry_policy FOR EACH ROW EXECUTE FUNCTION public.novu_rust_discord_retry_policy_revision();
CREATE TABLE public.novu_rust_discord_retry_policy_admins (
 environment_id text NOT NULL REFERENCES public.novu_rust_environments(id) ON DELETE CASCADE,destination_id text NOT NULL,
 database_principal name NOT NULL,principal_oid oid NOT NULL,
 can_read boolean NOT NULL DEFAULT false,can_write boolean NOT NULL DEFAULT false,can_audit boolean NOT NULL DEFAULT false,
 PRIMARY KEY(environment_id,destination_id,database_principal)
);
CREATE TABLE public.novu_rust_discord_retry_policy_requests (
 environment_id text NOT NULL REFERENCES public.novu_rust_environments(id) ON DELETE CASCADE,destination_id text NOT NULL,
 database_principal name NOT NULL,principal_oid oid NOT NULL,request_id uuid NOT NULL,expected_revision bigint NOT NULL,
 mode text NOT NULL,max_retries integer NOT NULL,fallback_seconds integer NOT NULL,reason text NOT NULL,result jsonb,recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(environment_id,destination_id,database_principal,principal_oid,request_id)
);
-- Audit is retained across environment deletion; a reused ID can show earlier records.
CREATE TABLE public.novu_rust_discord_retry_policy_audit (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,environment_id text NOT NULL,destination_id text NOT NULL,
 database_principal name NOT NULL,principal_oid oid NOT NULL,request_id uuid NOT NULL,
 previous_policy jsonb NOT NULL,current_policy jsonb NOT NULL,reason text NOT NULL,recorded_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX novu_rust_discord_retry_policy_audit_idx ON public.novu_rust_discord_retry_policy_audit(environment_id,destination_id,id DESC);
REVOKE ALL ON public.novu_rust_discord_retry_policy_admins,public.novu_rust_discord_retry_policy_requests,public.novu_rust_discord_retry_policy_audit FROM PUBLIC;
CREATE FUNCTION public.novu_rust_discord_retry_policy_access(p_env text,p_destination text,p_access text) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF p_env IS NULL OR length(p_env)=0 OR p_destination IS NULL OR p_destination !~ '^[A-Za-z0-9._-]{1,64}$' OR p_access IS NULL OR p_access NOT IN ('read','write','audit') THEN RAISE EXCEPTION 'invalid policy request' USING ERRCODE='22023'; END IF;
 PERFORM 1 FROM public.novu_rust_discord_retry_policy_admins g JOIN pg_catalog.pg_roles r ON r.rolname=g.database_principal AND r.oid=g.principal_oid AND r.rolcanlogin
 WHERE g.environment_id=p_env AND g.destination_id=p_destination AND g.database_principal=session_user AND CASE p_access WHEN 'read' THEN g.can_read WHEN 'write' THEN g.can_write ELSE g.can_audit END FOR SHARE OF g;
 IF NOT FOUND THEN RAISE EXCEPTION 'policy access denied' USING ERRCODE='42501'; END IF;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_discord_retry_policy_access(text,text,text) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_discord_retry_policy_read(p_env text,p_destination text) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE result jsonb;
BEGIN
 PERFORM public.novu_rust_discord_retry_policy_access(p_env,p_destination,'read');
 SELECT jsonb_build_object('mode',mode,'maxRetries',max_retries,'fallbackSeconds',fallback_seconds,'revision',revision::text) INTO result FROM public.novu_rust_discord_retry_policy WHERE environment_id=p_env AND destination_id=p_destination;
 IF NOT FOUND THEN RAISE EXCEPTION 'policy missing' USING ERRCODE='55000'; END IF;
 RETURN result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_discord_retry_policy_read(text,text) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_discord_retry_policy_set(p_env text,p_destination text,p_request uuid,p_expected bigint,p_mode text,p_max integer,p_fallback integer,p_reason text) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_oid oid;v_inserted boolean;v_req public.novu_rust_discord_retry_policy_requests%ROWTYPE;v_old public.novu_rust_discord_retry_policy%ROWTYPE;v_new public.novu_rust_discord_retry_policy%ROWTYPE;v_previous jsonb;v_result jsonb;
BEGIN
 IF p_request IS NULL OR p_expected IS NULL OR p_expected<0 OR p_mode IS NULL OR p_mode NOT IN ('off','known','fallback') OR p_max IS NULL OR p_max NOT BETWEEN 0 AND 10 OR p_fallback IS NULL OR p_fallback NOT BETWEEN 1 AND 86400 OR p_reason IS NULL OR octet_length(p_reason)>512 OR octet_length(btrim(p_reason))=0 THEN RAISE EXCEPTION 'invalid policy request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_discord_retry_policy_access(p_env,p_destination,'write');
 SELECT oid INTO v_oid FROM pg_catalog.pg_roles WHERE rolname=session_user AND rolcanlogin;
 INSERT INTO public.novu_rust_discord_retry_policy_requests(environment_id,destination_id,database_principal,principal_oid,request_id,expected_revision,mode,max_retries,fallback_seconds,reason) VALUES(p_env,p_destination,session_user,v_oid,p_request,p_expected,p_mode,p_max,p_fallback,p_reason) ON CONFLICT DO NOTHING RETURNING true INTO v_inserted;
 IF NOT coalesce(v_inserted,false) THEN
  SELECT * INTO v_req FROM public.novu_rust_discord_retry_policy_requests WHERE environment_id=p_env AND destination_id=p_destination AND database_principal=session_user AND principal_oid=v_oid AND request_id=p_request;
  IF NOT FOUND OR v_req.result IS NULL THEN RAISE EXCEPTION 'request unavailable' USING ERRCODE='55000'; END IF;
  IF (v_req.expected_revision,v_req.mode,v_req.max_retries,v_req.fallback_seconds,v_req.reason) IS DISTINCT FROM (p_expected,p_mode,p_max,p_fallback,p_reason) THEN RAISE EXCEPTION 'request conflict' USING ERRCODE='23505'; END IF;
  RETURN jsonb_build_object('requestId',p_request,'replayed',true,'policy',v_req.result);
 END IF;
 SELECT * INTO v_old FROM public.novu_rust_discord_retry_policy WHERE environment_id=p_env AND destination_id=p_destination FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'policy missing' USING ERRCODE='55000'; END IF;
 IF v_old.revision<>p_expected THEN RAISE EXCEPTION 'stale policy revision' USING ERRCODE='40001'; END IF;
 v_previous:=jsonb_build_object('mode',v_old.mode,'maxRetries',v_old.max_retries,'fallbackSeconds',v_old.fallback_seconds,'revision',v_old.revision::text);
 UPDATE public.novu_rust_discord_retry_policy SET mode=p_mode,max_retries=p_max,fallback_seconds=p_fallback WHERE environment_id=p_env AND destination_id=p_destination RETURNING * INTO v_new;
 v_result:=jsonb_build_object('mode',v_new.mode,'maxRetries',v_new.max_retries,'fallbackSeconds',v_new.fallback_seconds,'revision',v_new.revision::text);
 INSERT INTO public.novu_rust_discord_retry_policy_audit(environment_id,destination_id,database_principal,principal_oid,request_id,previous_policy,current_policy,reason) VALUES(p_env,p_destination,session_user,v_oid,p_request,v_previous,v_result,p_reason);
 UPDATE public.novu_rust_discord_retry_policy_requests SET result=v_result WHERE environment_id=p_env AND destination_id=p_destination AND database_principal=session_user AND principal_oid=v_oid AND request_id=p_request;
 RETURN jsonb_build_object('requestId',p_request,'replayed',false,'policy',v_result);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_discord_retry_policy_set(text,text,uuid,bigint,text,integer,integer,text) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_discord_retry_policy_request(p_env text,p_destination text,p_request uuid) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_row public.novu_rust_discord_retry_policy_requests%ROWTYPE;
BEGIN
 IF p_request IS NULL THEN RAISE EXCEPTION 'invalid policy request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_discord_retry_policy_access(p_env,p_destination,'write');
 SELECT * INTO v_row FROM public.novu_rust_discord_retry_policy_requests WHERE environment_id=p_env AND destination_id=p_destination AND database_principal=session_user AND principal_oid=(SELECT oid FROM pg_catalog.pg_roles WHERE rolname=session_user AND rolcanlogin) AND request_id=p_request;
 IF NOT FOUND THEN RETURN jsonb_build_object('found',false); END IF;
 RETURN jsonb_build_object('found',true,'requestId',v_row.request_id,'expectedRevision',v_row.expected_revision::text,'mode',v_row.mode,'maxRetries',v_row.max_retries,'fallbackSeconds',v_row.fallback_seconds,'reason',v_row.reason,'policy',v_row.result,'recordedAt',v_row.recorded_at::text);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_discord_retry_policy_request(text,text,uuid) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_discord_retry_policy_audit_read(p_env text,p_destination text,p_before bigint) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE result jsonb;
BEGIN
 IF p_before IS NOT NULL AND p_before<=0 THEN RAISE EXCEPTION 'invalid cursor' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_discord_retry_policy_access(p_env,p_destination,'audit');
 WITH candidates AS MATERIALIZED (SELECT * FROM public.novu_rust_discord_retry_policy_audit WHERE environment_id=p_env AND destination_id=p_destination AND (p_before IS NULL OR id<p_before) ORDER BY id DESC LIMIT 51),page AS MATERIALIZED (SELECT * FROM candidates ORDER BY id DESC LIMIT 50)
 SELECT jsonb_build_object('data',coalesce((SELECT jsonb_agg(jsonb_build_object('id',id::text,'requestId',request_id,'databasePrincipal',database_principal,'previous',previous_policy,'current',current_policy,'reason',reason,'recordedAt',recorded_at::text) ORDER BY id DESC) FROM page),'[]'::jsonb),'next',CASE WHEN (SELECT count(*) FROM candidates)>50 THEN (SELECT min(id)::text FROM page) ELSE NULL END) INTO result;
 RETURN result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_discord_retry_policy_audit_read(text,text,bigint) FROM PUBLIC;
