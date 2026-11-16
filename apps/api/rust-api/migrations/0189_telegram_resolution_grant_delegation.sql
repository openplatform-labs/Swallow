-- Dedicated Telegram resolution grant administration; existing channel admins are not automatically authorized.
-- Provision the independent administrators through a trusted DBA.
CREATE TABLE public.novu_rust_operator_jwt_telegram_resolution_grant_admins (
 issuer text NOT NULL,subject text NOT NULL,client_id text NOT NULL,environment_id text NOT NULL,
 can_read boolean NOT NULL DEFAULT false,can_write boolean NOT NULL DEFAULT false,
 PRIMARY KEY(issuer,subject,client_id,environment_id),
 FOREIGN KEY(issuer,subject,client_id,environment_id) REFERENCES public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) ON DELETE CASCADE
);
CREATE TABLE public.novu_rust_operator_jwt_telegram_resolution_grant_requests (
 issuer text NOT NULL,subject text NOT NULL,client_id text NOT NULL,environment_id text NOT NULL REFERENCES public.novu_rust_environments(id) ON DELETE CASCADE,request_id uuid NOT NULL,
 target_issuer text NOT NULL,target_subject text NOT NULL,target_client text NOT NULL,resource text NOT NULL,permission text NOT NULL,
 expected boolean NOT NULL,desired boolean NOT NULL,reason text NOT NULL,changed boolean NOT NULL,created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(issuer,subject,client_id,environment_id,request_id)
);
CREATE TABLE public.novu_rust_operator_jwt_telegram_resolution_grant_audit (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(),environment_id text NOT NULL,actor_issuer text NOT NULL,actor_subject text NOT NULL,actor_client text NOT NULL,
 database_principal name NOT NULL,request_id uuid NOT NULL,target_issuer text NOT NULL,target_subject text NOT NULL,target_client text NOT NULL,
 resource text NOT NULL,permission text NOT NULL,previous boolean NOT NULL,current boolean NOT NULL,reason text NOT NULL,recorded_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
REVOKE ALL ON public.novu_rust_operator_jwt_telegram_resolution_grant_admins,public.novu_rust_operator_jwt_telegram_resolution_grant_requests,public.novu_rust_operator_jwt_telegram_resolution_grant_audit FROM PUBLIC;

CREATE FUNCTION public.novu_rust_operator_jwt_telegram_resolution_grant_admin_access(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_write boolean)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF p_write IS NULL THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 PERFORM 1 FROM public.novu_rust_operator_jwt_telegram_resolution_grant_admins g WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env AND CASE WHEN p_write THEN g.can_write ELSE g.can_read END FOR SHARE OF g;
 IF NOT FOUND OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_telegram_resolution_grant_admin_access(text,text,text,text,bigint,boolean) FROM PUBLIC;

CREATE FUNCTION public.novu_rust_operator_jwt_telegram_resolution_grant_input_valid(p_issuer text,p_subject text,p_client text,p_resource text,p_permission text)
RETURNS boolean LANGUAGE sql IMMUTABLE SET search_path=pg_catalog AS $$
 SELECT COALESCE(octet_length(p_issuer) BETWEEN 1 AND 2048 AND octet_length(p_subject) BETWEEN 1 AND 1024 AND octet_length(p_client) BETWEEN 1 AND 1024 AND (p_resource='telegram-resolution' AND p_permission IN ('list','retry','accepted')),false)
$$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_telegram_resolution_grant_input_valid(text,text,text,text,text) FROM PUBLIC;

CREATE FUNCTION public.novu_rust_operator_jwt_telegram_resolution_grant_read(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_target_issuer text,p_target_subject text,p_target_client text,p_resource text,p_permission text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_exists boolean; v_granted boolean;
BEGIN
 IF NOT public.novu_rust_operator_jwt_telegram_resolution_grant_input_valid(p_target_issuer,p_target_subject,p_target_client,p_resource,p_permission) THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_telegram_resolution_grant_admin_access(p_issuer,p_subject,p_client,p_env,p_exp,false);
 PERFORM 1 FROM public.novu_rust_operator_jwt_identities i WHERE i.issuer=p_target_issuer AND i.subject=p_target_subject AND i.client_id=p_target_client AND i.environment_id=p_env FOR SHARE OF i;
 v_exists:=FOUND;
 SELECT COALESCE((SELECT CASE p_permission WHEN 'list' THEN g.can_list WHEN 'retry' THEN g.can_retry ELSE g.can_accept END FROM public.novu_rust_operator_jwt_telegram_resolution_grants g WHERE g.issuer=p_target_issuer AND g.subject=p_target_subject AND g.client_id=p_target_client AND g.environment_id=p_env),false) INTO v_granted;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN jsonb_build_object('targetExists',v_exists,'granted',v_granted);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_telegram_resolution_grant_read(text,text,text,text,bigint,text,text,text,text,text) FROM PUBLIC;

CREATE FUNCTION public.novu_rust_operator_jwt_telegram_resolution_grant_change(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_request uuid,p_target_issuer text,p_target_subject text,p_target_client text,p_resource text,p_permission text,p_expected boolean,p_desired boolean,p_reason text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE r public.novu_rust_operator_jwt_telegram_resolution_grant_requests%ROWTYPE; v_current boolean; v_changed boolean;
BEGIN
 IF p_request IS NULL OR NOT public.novu_rust_operator_jwt_telegram_resolution_grant_input_valid(p_target_issuer,p_target_subject,p_target_client,p_resource,p_permission) OR p_expected IS NULL OR p_desired IS NULL OR p_reason IS NULL OR octet_length(btrim(p_reason)) NOT BETWEEN 1 AND 1000 THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_telegram_resolution_grant_admin_access(p_issuer,p_subject,p_client,p_env,p_exp,true);
 IF (p_issuer,p_subject)=(p_target_issuer,p_target_subject) THEN RAISE EXCEPTION 'self change denied' USING ERRCODE='42501'; END IF;
 PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(pg_catalog.jsonb_build_array('jwt-telegram-resolution-grant-request',p_issuer,p_subject,p_client,p_env,p_request)::text,0));
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 SELECT * INTO r FROM public.novu_rust_operator_jwt_telegram_resolution_grant_requests WHERE issuer=p_issuer AND subject=p_subject AND client_id=p_client AND environment_id=p_env AND request_id=p_request;
 IF FOUND THEN
  IF (r.target_issuer,r.target_subject,r.target_client,r.resource,r.permission,r.expected,r.desired,r.reason) IS DISTINCT FROM (p_target_issuer,p_target_subject,p_target_client,p_resource,p_permission,p_expected,p_desired,p_reason) THEN RAISE EXCEPTION 'request conflict' USING ERRCODE='P4090'; END IF;
  IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
  RETURN jsonb_build_object('changed',r.changed,'replayed',true,'requestId',p_request);
 END IF;
 PERFORM 1 FROM public.novu_rust_operator_jwt_identities i WHERE i.issuer=p_target_issuer AND i.subject=p_target_subject AND i.client_id=p_target_client AND i.environment_id=p_env FOR UPDATE OF i;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 IF NOT FOUND THEN RAISE EXCEPTION 'target missing' USING ERRCODE='22023'; END IF;
 SELECT COALESCE((SELECT CASE p_permission WHEN 'list' THEN g.can_list WHEN 'retry' THEN g.can_retry ELSE g.can_accept END FROM public.novu_rust_operator_jwt_telegram_resolution_grants g WHERE g.issuer=p_target_issuer AND g.subject=p_target_subject AND g.client_id=p_target_client AND g.environment_id=p_env),false) INTO v_current;
 IF v_current IS DISTINCT FROM p_expected THEN RAISE EXCEPTION 'stale grant' USING ERRCODE='P4090'; END IF;
 v_changed:=v_current IS DISTINCT FROM p_desired;
 IF v_changed THEN
  INSERT INTO public.novu_rust_operator_jwt_telegram_resolution_grants(issuer,subject,client_id,environment_id,can_list,can_retry,can_accept)
  VALUES(p_target_issuer,p_target_subject,p_target_client,p_env,p_permission='list' AND p_desired,p_permission='retry' AND p_desired,p_permission='accepted' AND p_desired)
  ON CONFLICT(issuer,subject,client_id,environment_id) DO UPDATE SET
    can_list=CASE WHEN p_permission='list' THEN p_desired ELSE public.novu_rust_operator_jwt_telegram_resolution_grants.can_list END,
    can_retry=CASE WHEN p_permission='retry' THEN p_desired ELSE public.novu_rust_operator_jwt_telegram_resolution_grants.can_retry END,
    can_accept=CASE WHEN p_permission='accepted' THEN p_desired ELSE public.novu_rust_operator_jwt_telegram_resolution_grants.can_accept END;
  INSERT INTO public.novu_rust_operator_jwt_telegram_resolution_grant_audit(environment_id,actor_issuer,actor_subject,actor_client,database_principal,request_id,target_issuer,target_subject,target_client,resource,permission,previous,current,reason) VALUES(p_env,p_issuer,p_subject,p_client,session_user,p_request,p_target_issuer,p_target_subject,p_target_client,p_resource,p_permission,p_expected,p_desired,p_reason);
 END IF;
 INSERT INTO public.novu_rust_operator_jwt_telegram_resolution_grant_requests(issuer,subject,client_id,environment_id,request_id,target_issuer,target_subject,target_client,resource,permission,expected,desired,reason,changed) VALUES(p_issuer,p_subject,p_client,p_env,p_request,p_target_issuer,p_target_subject,p_target_client,p_resource,p_permission,p_expected,p_desired,p_reason,v_changed);
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN jsonb_build_object('changed',v_changed,'replayed',false,'requestId',p_request);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_telegram_resolution_grant_change(text,text,text,text,bigint,uuid,text,text,text,text,text,boolean,boolean,text) FROM PUBLIC;

CREATE FUNCTION public.novu_rust_operator_jwt_telegram_resolution_grant_request(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_request uuid)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE r public.novu_rust_operator_jwt_telegram_resolution_grant_requests%ROWTYPE;
BEGIN
 IF p_request IS NULL THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_telegram_resolution_grant_admin_access(p_issuer,p_subject,p_client,p_env,p_exp,true);
 SELECT * INTO r FROM public.novu_rust_operator_jwt_telegram_resolution_grant_requests WHERE issuer=p_issuer AND subject=p_subject AND client_id=p_client AND environment_id=p_env AND request_id=p_request;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 IF NOT FOUND THEN RETURN jsonb_build_object('found',false); END IF;
 RETURN jsonb_build_object('found',true,'requestId',r.request_id,'targetIssuer',r.target_issuer,'targetSubject',r.target_subject,'targetClientId',r.target_client,'resource',r.resource,'permission',r.permission,'expected',r.expected,'desired',r.desired,'reason',r.reason,'changed',r.changed,'createdAt',r.created_at);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_telegram_resolution_grant_request(text,text,text,text,bigint,uuid) FROM PUBLIC;

-- Viewing delegated grant history is independently provisioned by the DB administrator.
CREATE TABLE public.novu_rust_operator_jwt_telegram_resolution_grant_audit_viewers (
 issuer text NOT NULL,subject text NOT NULL,client_id text NOT NULL,environment_id text NOT NULL,
 PRIMARY KEY(issuer,subject,client_id,environment_id),
 FOREIGN KEY(issuer,subject,client_id,environment_id) REFERENCES public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) ON DELETE CASCADE
);
REVOKE ALL ON public.novu_rust_operator_jwt_telegram_resolution_grant_audit_viewers FROM PUBLIC;
CREATE INDEX novu_rust_operator_jwt_telegram_resolution_grant_audit_page_idx ON public.novu_rust_operator_jwt_telegram_resolution_grant_audit(environment_id,id);
CREATE FUNCTION public.novu_rust_operator_jwt_telegram_resolution_grant_audit_read(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_after uuid)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_result jsonb;
BEGIN
 PERFORM public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 PERFORM 1 FROM public.novu_rust_operator_jwt_telegram_resolution_grant_audit_viewers g WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env FOR SHARE OF g;
 IF NOT FOUND OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 WITH candidates AS MATERIALIZED (
  SELECT id,actor_issuer,actor_subject,actor_client,database_principal,request_id,target_issuer,target_subject,target_client,resource,permission,previous,current,reason,recorded_at
  FROM public.novu_rust_operator_jwt_telegram_resolution_grant_audit WHERE environment_id=p_env AND (p_after IS NULL OR id>p_after) ORDER BY id LIMIT 51
 ), page AS MATERIALIZED (SELECT * FROM candidates ORDER BY id LIMIT 50)
 SELECT jsonb_build_object('data',coalesce((SELECT jsonb_agg(jsonb_build_object('id',id,'actorIssuer',actor_issuer,'actorSubject',actor_subject,'actorClientId',actor_client,'databasePrincipal',database_principal,'requestId',request_id,'targetIssuer',target_issuer,'targetSubject',target_subject,'targetClientId',target_client,'resource',resource,'permission',permission,'previous',previous,'current',current,'reason',reason,'recordedAt',recorded_at::text) ORDER BY id) FROM page),'[]'::jsonb),'hasMore',(SELECT count(*)>50 FROM candidates),'next',CASE WHEN (SELECT count(*)>50 FROM candidates) THEN (SELECT id::text FROM page ORDER BY id DESC LIMIT 1) ELSE NULL END) INTO v_result;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN v_result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_telegram_resolution_grant_audit_read(text,text,text,text,bigint,uuid) FROM PUBLIC;
