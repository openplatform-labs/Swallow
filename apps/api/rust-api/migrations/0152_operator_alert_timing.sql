-- Target-specific JWT grants in addition to the worker DB login's timing grants.
CREATE TABLE public.novu_rust_operator_jwt_alert_timing_grants (
 issuer text NOT NULL,subject text NOT NULL,client_id text NOT NULL,environment_id text NOT NULL,
 channel text NOT NULL CHECK(channel IN ('slack','email')),target_id text NOT NULL CHECK(target_id ~ '^[A-Za-z0-9._-]{1,64}$'),
 can_read boolean NOT NULL DEFAULT false,can_write boolean NOT NULL DEFAULT false,can_audit boolean NOT NULL DEFAULT false,
 PRIMARY KEY(issuer,subject,client_id,environment_id,channel,target_id),
 FOREIGN KEY(issuer,subject,client_id,environment_id) REFERENCES public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) ON DELETE CASCADE
);
CREATE TABLE public.novu_rust_operator_jwt_alert_timing_requests (
 issuer text NOT NULL,subject text NOT NULL,client_id text NOT NULL,environment_id text NOT NULL,channel text NOT NULL,target_id text NOT NULL,request_id uuid NOT NULL,
 expected_revision bigint NOT NULL,grace integer,heartbeat integer,cooldown integer,reason text NOT NULL,settings jsonb NOT NULL,created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(issuer,subject,client_id,environment_id,channel,target_id,request_id)
);
CREATE TABLE public.novu_rust_operator_jwt_alert_timing_audit (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(),environment_id text NOT NULL,channel text NOT NULL,target_id text NOT NULL,
 actor_issuer text NOT NULL,actor_subject text NOT NULL,actor_client text NOT NULL,database_principal name NOT NULL,
 request_id uuid NOT NULL,previous jsonb NOT NULL,current jsonb NOT NULL,reason text NOT NULL,recorded_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX novu_rust_jwt_alert_timing_audit_idx ON public.novu_rust_operator_jwt_alert_timing_audit(environment_id,channel,target_id,id);
REVOKE ALL ON public.novu_rust_operator_jwt_alert_timing_grants,public.novu_rust_operator_jwt_alert_timing_requests,public.novu_rust_operator_jwt_alert_timing_audit FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_alert_timing_access(i text,s text,c text,e text,x bigint,ch text,t text,a text) RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF ch IS NULL OR ch NOT IN ('slack','email') OR t IS NULL OR t !~ '^[A-Za-z0-9._-]{1,64}$' OR a IS NULL OR a NOT IN ('read','write','audit') THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_self(i,s,c,e,x);
 PERFORM 1 FROM public.novu_rust_operator_jwt_alert_timing_grants g WHERE g.issuer=i AND g.subject=s AND g.client_id=c AND g.environment_id=e AND g.channel=ch AND g.target_id=t AND CASE a WHEN 'read' THEN g.can_read WHEN 'write' THEN g.can_write ELSE g.can_audit END FOR SHARE OF g;
 IF NOT FOUND OR to_timestamp(x::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 PERFORM public.novu_rust_alert_timing_access(e,a='write');
 IF to_timestamp(x::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_alert_timing_access(text,text,text,text,bigint,text,text,text) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_alert_timing_read(i text,s text,c text,e text,x bigint,ch text,t text) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v jsonb;
BEGIN
 PERFORM public.novu_rust_operator_jwt_alert_timing_access(i,s,c,e,x,ch,t,'read');
 SELECT jsonb_build_object('revision',revision::text,'grace',grace,'heartbeat',heartbeat,'cooldown',cooldown) INTO v FROM public.novu_rust_alert_timing_settings WHERE environment_id=e AND channel=ch AND target_id=t;
 IF to_timestamp(x::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN coalesce(v,jsonb_build_object('revision','0','grace',NULL,'heartbeat',NULL,'cooldown',NULL));
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_alert_timing_read(text,text,text,text,bigint,text,text) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_alert_timing_change(i text,s text,c text,e text,x bigint,ch text,t text,req uuid,expected bigint,g integer,h integer,cd integer,why text) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE r public.novu_rust_operator_jwt_alert_timing_requests%ROWTYPE; worker_req uuid; result jsonb; prev jsonb;
BEGIN
 IF req IS NULL OR expected IS NULL OR expected<0 OR why IS NULL OR octet_length(btrim(why)) NOT BETWEEN 1 AND 512 OR octet_length(why)>512 OR (g IS NOT NULL AND g NOT BETWEEN 1 AND 86400) OR (h IS NOT NULL AND h NOT BETWEEN 1 AND 86400) OR (cd IS NOT NULL AND cd NOT BETWEEN 60 AND 86400) THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_alert_timing_access(i,s,c,e,x,ch,t,'write');
 PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(pg_catalog.jsonb_build_array('jwt-alert-timing',i,s,c,e,ch,t,req)::text,0));
 IF to_timestamp(x::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 SELECT * INTO r FROM public.novu_rust_operator_jwt_alert_timing_requests WHERE issuer=i AND subject=s AND client_id=c AND environment_id=e AND channel=ch AND target_id=t AND request_id=req;
 IF FOUND THEN
  IF (r.expected_revision,r.grace,r.heartbeat,r.cooldown,r.reason) IS DISTINCT FROM (expected,g,h,cd,why) THEN RAISE EXCEPTION 'request conflict' USING ERRCODE='P4090'; END IF;
  IF to_timestamp(x::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
  RETURN jsonb_build_object('requestId',req,'replayed',true,'settings',r.settings);
 END IF;
 worker_req:=gen_random_uuid();
 BEGIN result:=public.novu_rust_alert_timing_set(e,ch,t,expected,g,h,cd,worker_req,why);
 EXCEPTION WHEN SQLSTATE '40001' THEN RAISE EXCEPTION 'stale revision' USING ERRCODE='P4090'; END;
 SELECT previous INTO prev FROM public.novu_rust_alert_timing_audit WHERE environment_id=e AND actor=session_user AND request_id=worker_req;
 IF NOT FOUND THEN RAISE EXCEPTION 'missing audit' USING ERRCODE='55000'; END IF;
 INSERT INTO public.novu_rust_operator_jwt_alert_timing_audit(environment_id,channel,target_id,actor_issuer,actor_subject,actor_client,database_principal,request_id,previous,current,reason) VALUES(e,ch,t,i,s,c,session_user,req,prev,result->'settings',why);
 INSERT INTO public.novu_rust_operator_jwt_alert_timing_requests(issuer,subject,client_id,environment_id,channel,target_id,request_id,expected_revision,grace,heartbeat,cooldown,reason,settings) VALUES(i,s,c,e,ch,t,req,expected,g,h,cd,why,result->'settings');
 IF to_timestamp(x::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN jsonb_build_object('requestId',req,'replayed',false,'settings',result->'settings');
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_alert_timing_change(text,text,text,text,bigint,text,text,uuid,bigint,integer,integer,integer,text) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_alert_timing_request(i text,s text,c text,e text,x bigint,ch text,t text,req uuid) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE r public.novu_rust_operator_jwt_alert_timing_requests%ROWTYPE;
BEGIN
 IF req IS NULL THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_alert_timing_access(i,s,c,e,x,ch,t,'write');
 SELECT * INTO r FROM public.novu_rust_operator_jwt_alert_timing_requests WHERE issuer=i AND subject=s AND client_id=c AND environment_id=e AND channel=ch AND target_id=t AND request_id=req;
 IF to_timestamp(x::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 IF NOT FOUND THEN RETURN jsonb_build_object('found',false); END IF;
 RETURN jsonb_build_object('found',true,'requestId',r.request_id,'expectedRevision',r.expected_revision::text,'grace',r.grace,'heartbeat',r.heartbeat,'cooldown',r.cooldown,'reason',r.reason,'settings',r.settings,'createdAt',r.created_at::text);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_alert_timing_request(text,text,text,text,bigint,text,text,uuid) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_alert_timing_audit(i text,s text,c text,e text,x bigint,ch text,t text,after_id uuid) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v jsonb;
BEGIN
 PERFORM public.novu_rust_operator_jwt_alert_timing_access(i,s,c,e,x,ch,t,'audit');
 WITH candidates AS MATERIALIZED (SELECT * FROM public.novu_rust_operator_jwt_alert_timing_audit WHERE environment_id=e AND channel=ch AND target_id=t AND (after_id IS NULL OR id>after_id) ORDER BY id LIMIT 51),page AS MATERIALIZED (SELECT * FROM candidates ORDER BY id LIMIT 50)
 SELECT jsonb_build_object('data',coalesce((SELECT jsonb_agg(jsonb_build_object('id',id,'actorIssuer',actor_issuer,'actorSubject',actor_subject,'actorClientId',actor_client,'databasePrincipal',database_principal,'requestId',request_id,'previous',previous,'current',current,'reason',reason,'recordedAt',recorded_at::text) ORDER BY id) FROM page),'[]'::jsonb),'hasMore',(SELECT count(*)>50 FROM candidates),'next',CASE WHEN (SELECT count(*)>50 FROM candidates) THEN (SELECT id::text FROM page ORDER BY id DESC LIMIT 1) ELSE NULL END) INTO v;
 IF to_timestamp(x::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN v;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_alert_timing_audit(text,text,text,text,bigint,text,text,uuid) FROM PUBLIC;
