-- Permissions are separate from the existing retry policy grants.
CREATE TABLE public.novu_rust_discord_cooldown_admins (
 environment_id text NOT NULL REFERENCES public.novu_rust_environments(id) ON DELETE CASCADE,
 destination_id text NOT NULL, database_principal name NOT NULL, principal_oid oid NOT NULL,
 can_read boolean NOT NULL DEFAULT false, can_write boolean NOT NULL DEFAULT false, can_audit boolean NOT NULL DEFAULT false,
 PRIMARY KEY(environment_id,destination_id,database_principal)
);
CREATE TABLE public.novu_rust_discord_cooldown_revisions (
 environment_id text NOT NULL, destination_id text NOT NULL, revision bigint NOT NULL DEFAULT 0 CHECK(revision>=0),
 PRIMARY KEY(environment_id,destination_id),
 FOREIGN KEY(environment_id,destination_id) REFERENCES public.novu_rust_discord_destinations(environment_id,destination_id)
);
CREATE TABLE public.novu_rust_discord_cooldown_requests (
 environment_id text NOT NULL REFERENCES public.novu_rust_environments(id) ON DELETE CASCADE,
 destination_id text NOT NULL, database_principal name NOT NULL, principal_oid oid NOT NULL, request_id uuid NOT NULL,
 expected_revision bigint NOT NULL, enabled boolean NOT NULL, reason text NOT NULL, result jsonb,
 recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(environment_id,destination_id,database_principal,principal_oid,request_id)
);
-- Retained independently of environment deletion for operational investigation.
CREATE TABLE public.novu_rust_discord_cooldown_audit (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, environment_id text NOT NULL, destination_id text NOT NULL,
 database_principal name NOT NULL, principal_oid oid NOT NULL, request_id uuid NOT NULL,
 previous_policy jsonb NOT NULL, current_policy jsonb NOT NULL, reason text NOT NULL,
 recorded_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX novu_rust_discord_cooldown_audit_idx ON public.novu_rust_discord_cooldown_audit(environment_id,destination_id,id DESC);
REVOKE ALL ON public.novu_rust_discord_cooldown_admins,public.novu_rust_discord_cooldown_revisions,public.novu_rust_discord_cooldown_requests,public.novu_rust_discord_cooldown_audit FROM PUBLIC;

CREATE FUNCTION public.novu_rust_discord_cooldown_access(p_env text,p_destination text,p_access text) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF p_env IS NULL OR length(p_env)=0 OR p_destination IS NULL OR p_destination !~ '^[A-Za-z0-9._-]{1,64}$' OR p_access IS NULL OR p_access NOT IN ('read','write','audit') THEN RAISE EXCEPTION 'invalid cooldown request' USING ERRCODE='22023'; END IF;
 PERFORM 1 FROM public.novu_rust_discord_cooldown_admins g JOIN pg_catalog.pg_roles r ON r.rolname=g.database_principal AND r.oid=g.principal_oid AND r.rolcanlogin
 WHERE g.environment_id=p_env AND g.destination_id=p_destination AND g.database_principal=session_user AND CASE p_access WHEN 'read' THEN g.can_read WHEN 'write' THEN g.can_write ELSE g.can_audit END FOR SHARE OF g;
 IF NOT FOUND THEN RAISE EXCEPTION 'cooldown access denied' USING ERRCODE='42501'; END IF;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_discord_cooldown_access(text,text,text) FROM PUBLIC;

CREATE FUNCTION public.novu_rust_discord_cooldown_read(p_env text,p_destination text) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_result jsonb;
BEGIN
 PERFORM public.novu_rust_discord_cooldown_access(p_env,p_destination,'read');
 SELECT jsonb_build_object('enabled',c.environment_id IS NOT NULL,'revision',coalesce(r.revision,0)::text,'notBefore',CASE WHEN c.not_before > '-infinity'::timestamptz THEN c.not_before::text ELSE NULL END)
 INTO v_result FROM public.novu_rust_discord_destinations d
 LEFT JOIN public.novu_rust_discord_shared_cooldown c USING(environment_id,destination_id)
 LEFT JOIN public.novu_rust_discord_cooldown_revisions r USING(environment_id,destination_id)
 WHERE d.environment_id=p_env AND d.destination_id=p_destination;
 IF NOT FOUND THEN RAISE EXCEPTION 'destination missing' USING ERRCODE='55000'; END IF;
 RETURN v_result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_discord_cooldown_read(text,text) FROM PUBLIC;

CREATE FUNCTION public.novu_rust_discord_cooldown_set(p_env text,p_destination text,p_request uuid,p_expected bigint,p_enabled boolean,p_reason text) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_oid oid; v_inserted boolean; v_req public.novu_rust_discord_cooldown_requests%ROWTYPE; v_revision bigint; v_enabled boolean; v_old jsonb; v_new jsonb;
BEGIN
 IF p_request IS NULL OR p_expected IS NULL OR p_expected<0 OR p_enabled IS NULL OR p_reason IS NULL OR octet_length(p_reason)>512 OR octet_length(btrim(p_reason))=0 THEN RAISE EXCEPTION 'invalid cooldown request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_discord_cooldown_access(p_env,p_destination,'write');
 SELECT oid INTO v_oid FROM pg_catalog.pg_roles WHERE rolname=session_user AND rolcanlogin;
 INSERT INTO public.novu_rust_discord_cooldown_requests(environment_id,destination_id,database_principal,principal_oid,request_id,expected_revision,enabled,reason) VALUES(p_env,p_destination,session_user,v_oid,p_request,p_expected,p_enabled,p_reason) ON CONFLICT DO NOTHING RETURNING true INTO v_inserted;
 IF NOT coalesce(v_inserted,false) THEN
  SELECT * INTO v_req FROM public.novu_rust_discord_cooldown_requests WHERE environment_id=p_env AND destination_id=p_destination AND database_principal=session_user AND principal_oid=v_oid AND request_id=p_request;
  IF NOT FOUND OR v_req.result IS NULL THEN RAISE EXCEPTION 'request unavailable' USING ERRCODE='55000'; END IF;
  IF (v_req.expected_revision,v_req.enabled,v_req.reason) IS DISTINCT FROM (p_expected,p_enabled,p_reason) THEN RAISE EXCEPTION 'request conflict' USING ERRCODE='23505'; END IF;
  RETURN jsonb_build_object('requestId',p_request,'replayed',true,'policy',v_req.result);
 END IF;
 -- Serialize all configuration changes, including creation of the first revision.
 PERFORM 1 FROM public.novu_rust_discord_destinations WHERE environment_id=p_env AND destination_id=p_destination FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'destination missing' USING ERRCODE='55000'; END IF;
 INSERT INTO public.novu_rust_discord_cooldown_revisions(environment_id,destination_id) VALUES(p_env,p_destination) ON CONFLICT DO NOTHING;
 SELECT revision INTO v_revision FROM public.novu_rust_discord_cooldown_revisions WHERE environment_id=p_env AND destination_id=p_destination FOR UPDATE;
 IF v_revision<>p_expected THEN RAISE EXCEPTION 'stale cooldown revision' USING ERRCODE='40001'; END IF;
 SELECT EXISTS(SELECT 1 FROM public.novu_rust_discord_shared_cooldown WHERE environment_id=p_env AND destination_id=p_destination) INTO v_enabled;
 v_old:=jsonb_build_object('enabled',v_enabled,'revision',v_revision::text);
 IF v_enabled IS DISTINCT FROM p_enabled THEN
  IF p_enabled THEN INSERT INTO public.novu_rust_discord_shared_cooldown(environment_id,destination_id) VALUES(p_env,p_destination);
  ELSE DELETE FROM public.novu_rust_discord_shared_cooldown WHERE environment_id=p_env AND destination_id=p_destination; END IF;
  UPDATE public.novu_rust_discord_cooldown_revisions SET revision=revision+1 WHERE environment_id=p_env AND destination_id=p_destination RETURNING revision INTO v_revision;
 END IF;
 v_new:=jsonb_build_object('enabled',p_enabled,'revision',v_revision::text);
 INSERT INTO public.novu_rust_discord_cooldown_audit(environment_id,destination_id,database_principal,principal_oid,request_id,previous_policy,current_policy,reason) VALUES(p_env,p_destination,session_user,v_oid,p_request,v_old,v_new,p_reason);
 UPDATE public.novu_rust_discord_cooldown_requests SET result=v_new WHERE environment_id=p_env AND destination_id=p_destination AND database_principal=session_user AND principal_oid=v_oid AND request_id=p_request;
 RETURN jsonb_build_object('requestId',p_request,'replayed',false,'policy',v_new);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_discord_cooldown_set(text,text,uuid,bigint,boolean,text) FROM PUBLIC;

CREATE FUNCTION public.novu_rust_discord_cooldown_request(p_env text,p_destination text,p_request uuid) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_row public.novu_rust_discord_cooldown_requests%ROWTYPE;
BEGIN
 IF p_request IS NULL THEN RAISE EXCEPTION 'invalid cooldown request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_discord_cooldown_access(p_env,p_destination,'write');
 SELECT * INTO v_row FROM public.novu_rust_discord_cooldown_requests WHERE environment_id=p_env AND destination_id=p_destination AND database_principal=session_user AND principal_oid=(SELECT oid FROM pg_catalog.pg_roles WHERE rolname=session_user AND rolcanlogin) AND request_id=p_request;
 IF NOT FOUND THEN RETURN jsonb_build_object('found',false); END IF;
 RETURN jsonb_build_object('found',true,'requestId',v_row.request_id,'expectedRevision',v_row.expected_revision::text,'enabled',v_row.enabled,'reason',v_row.reason,'policy',v_row.result,'recordedAt',v_row.recorded_at::text);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_discord_cooldown_request(text,text,uuid) FROM PUBLIC;

CREATE FUNCTION public.novu_rust_discord_cooldown_audit_read(p_env text,p_destination text,p_before bigint) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_result jsonb;
BEGIN
 IF p_before IS NOT NULL AND p_before<=0 THEN RAISE EXCEPTION 'invalid cursor' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_discord_cooldown_access(p_env,p_destination,'audit');
 WITH candidates AS MATERIALIZED (SELECT * FROM public.novu_rust_discord_cooldown_audit WHERE environment_id=p_env AND destination_id=p_destination AND (p_before IS NULL OR id<p_before) ORDER BY id DESC LIMIT 51), page AS MATERIALIZED (SELECT * FROM candidates ORDER BY id DESC LIMIT 50)
 SELECT jsonb_build_object('data',coalesce((SELECT jsonb_agg(jsonb_build_object('id',id::text,'requestId',request_id,'databasePrincipal',database_principal,'previous',previous_policy,'current',current_policy,'reason',reason,'recordedAt',recorded_at::text) ORDER BY id DESC) FROM page),'[]'::jsonb),'next',CASE WHEN (SELECT count(*) FROM candidates)>50 THEN (SELECT min(id)::text FROM page) ELSE NULL END) INTO v_result;
 RETURN v_result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_discord_cooldown_audit_read(text,text,bigint) FROM PUBLIC;
