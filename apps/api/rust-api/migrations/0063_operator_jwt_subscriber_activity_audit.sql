-- Independently scoped activity review. Existing create/delete receipts receive UUID page keys.
ALTER TABLE public.novu_rust_operator_jwt_subscriber_create_requests ADD COLUMN audit_id uuid NOT NULL DEFAULT gen_random_uuid();
ALTER TABLE public.novu_rust_operator_jwt_subscriber_delete_requests ADD COLUMN audit_id uuid NOT NULL DEFAULT gen_random_uuid();
CREATE UNIQUE INDEX novu_rust_operator_jwt_subscriber_create_audit_page_idx ON public.novu_rust_operator_jwt_subscriber_create_requests(environment_id,audit_id);
CREATE UNIQUE INDEX novu_rust_operator_jwt_subscriber_delete_audit_page_idx ON public.novu_rust_operator_jwt_subscriber_delete_requests(environment_id,audit_id);
CREATE INDEX novu_rust_operator_jwt_subscriber_profile_audit_page_idx ON public.novu_rust_operator_jwt_subscriber_profile_audit(environment_id,id);
CREATE TABLE public.novu_rust_operator_jwt_subscriber_activity_viewers (
 issuer text NOT NULL,subject text NOT NULL,client_id text NOT NULL,environment_id text NOT NULL,kind text NOT NULL CHECK(kind IN ('create','profile','delete')),
 PRIMARY KEY(issuer,subject,client_id,environment_id,kind),
 FOREIGN KEY(issuer,subject,client_id,environment_id) REFERENCES public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) ON DELETE CASCADE
);
REVOKE ALL ON public.novu_rust_operator_jwt_subscriber_activity_viewers FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_subscriber_activity_read(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_kind text,p_after uuid)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_result jsonb;
BEGIN
 IF p_kind IS NULL OR p_kind NOT IN ('create','profile','delete') THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 PERFORM 1 FROM public.novu_rust_operator_jwt_subscriber_activity_viewers g WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env AND g.kind=p_kind FOR SHARE OF g;
 IF NOT FOUND OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 WITH candidates AS MATERIALIZED (
  SELECT id,subscriber_id,generation,issuer,subject,client_id,database_principal,request_id,previous_revision,revision,fields,reason,recorded_at FROM (
   SELECT audit_id AS id,subscriber_id,generation,issuer,subject,client_id,database_principal,request_id,NULL::bigint AS previous_revision,0::bigint AS revision,fields,reason,created_at AS recorded_at FROM public.novu_rust_operator_jwt_subscriber_create_requests WHERE p_kind='create' AND environment_id=p_env AND (p_after IS NULL OR audit_id>p_after)
   UNION ALL
   SELECT id,subscriber_id,generation,issuer,subject,client_id,database_principal,NULL::uuid,previous_revision,new_revision,fields,reason,recorded_at FROM public.novu_rust_operator_jwt_subscriber_profile_audit WHERE p_kind='profile' AND environment_id=p_env AND (p_after IS NULL OR id>p_after)
   UNION ALL
   SELECT audit_id,subscriber_id,generation,issuer,subject,client_id,database_principal,request_id,revision,NULL::bigint,ARRAY[]::text[],reason,created_at FROM public.novu_rust_operator_jwt_subscriber_delete_requests WHERE p_kind='delete' AND environment_id=p_env AND (p_after IS NULL OR audit_id>p_after)
  ) a ORDER BY id LIMIT 51
 ), page AS MATERIALIZED (SELECT * FROM candidates ORDER BY id LIMIT 50)
 SELECT jsonb_build_object('data',coalesce((SELECT jsonb_agg(jsonb_build_object('id',id,'subscriberId',subscriber_id,'generation',generation,'issuer',issuer,'subject',subject,'clientId',client_id,'databasePrincipal',database_principal,'requestId',request_id,'previousRevision',previous_revision::text,'revision',revision::text,'fields',fields,'reason',reason,'recordedAt',recorded_at::text) ORDER BY id) FROM page),'[]'::jsonb),'hasMore',(SELECT count(*)>50 FROM candidates),'next',CASE WHEN (SELECT count(*)>50 FROM candidates) THEN (SELECT id::text FROM page ORDER BY id DESC LIMIT 1) ELSE NULL END) INTO v_result;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN v_result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_subscriber_activity_read(text,text,text,text,bigint,text,uuid) FROM PUBLIC;
