-- Explicit trust registration and environment-scoped JWT subject mapping.
CREATE TABLE public.novu_rust_operator_jwt_api_logins (
 database_principal name PRIMARY KEY, principal_oid oid NOT NULL
);
CREATE TABLE public.novu_rust_operator_jwt_identities (
 issuer text NOT NULL CHECK (octet_length(issuer) BETWEEN 1 AND 2048),
 subject text NOT NULL CHECK (octet_length(subject) BETWEEN 1 AND 1024),
 client_id text NOT NULL CHECK (octet_length(client_id) BETWEEN 1 AND 1024),
 environment_id text NOT NULL REFERENCES public.novu_rust_environments(id),
 enabled boolean NOT NULL DEFAULT true,
 subscriber_read boolean NOT NULL DEFAULT false,
 alert_read boolean NOT NULL DEFAULT false,
 alert_retry boolean NOT NULL DEFAULT false,
 alert_accepted boolean NOT NULL DEFAULT false,
 PRIMARY KEY(issuer,subject,client_id,environment_id)
);
REVOKE ALL ON public.novu_rust_operator_jwt_api_logins,public.novu_rust_operator_jwt_identities FROM PUBLIC;

CREATE FUNCTION public.novu_rust_operator_jwt_self(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_actor oid; v_identity public.novu_rust_operator_jwt_identities%ROWTYPE;
BEGIN
 IF p_issuer IS NULL OR p_subject IS NULL OR p_client IS NULL OR p_env IS NULL OR p_exp IS NULL OR p_exp NOT BETWEEN 1 AND 253402300799 THEN
  RAISE EXCEPTION 'invalid operator request' USING ERRCODE='42501';
 END IF;
 SELECT oid INTO v_actor FROM pg_catalog.pg_roles WHERE rolname=session_user AND rolcanlogin;
 PERFORM 1 FROM public.novu_rust_operator_jwt_api_logins a WHERE a.database_principal=session_user AND a.principal_oid=v_actor FOR SHARE OF a;
 IF NOT FOUND THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 SELECT * INTO v_identity FROM public.novu_rust_operator_jwt_identities i WHERE i.issuer=p_issuer AND i.subject=p_subject AND i.client_id=p_client AND i.environment_id=p_env AND i.enabled FOR SHARE OF i;
 IF NOT FOUND OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN jsonb_build_object('issuer',v_identity.issuer,'subject',v_identity.subject,'clientId',v_identity.client_id,'environmentId',v_identity.environment_id,
 'provisionedPermissions',jsonb_build_object('subscriberRead',v_identity.subscriber_read,'alertRead',v_identity.alert_read,'alertRetry',v_identity.alert_retry,'alertAccepted',v_identity.alert_accepted));
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_self(text,text,text,text,bigint) FROM PUBLIC;

CREATE FUNCTION public.novu_rust_operator_jwt_subscribers(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_name text,p_after text,p_before text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_identity jsonb; v_result jsonb;
BEGIN
 IF (p_name IS NOT NULL AND octet_length(p_name)>512) OR (p_after IS NOT NULL AND (octet_length(p_after)>512 OR p_after=''))
 OR (p_before IS NOT NULL AND (octet_length(p_before)>512 OR p_before='')) OR (p_after IS NOT NULL AND p_before IS NOT NULL) THEN
  RAISE EXCEPTION 'invalid request' USING ERRCODE='22023';
 END IF;
 v_identity:=public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 IF (v_identity->'provisionedPermissions'->>'subscriberRead')::boolean IS NOT TRUE THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 WITH filtered AS MATERIALIZED (
   SELECT s.subscriber_id,s.profile FROM public.novu_rust_subscribers s
   WHERE s.environment_id=p_env AND (p_name IS NULL OR position(lower(p_name) in lower(trim(coalesce(s.profile->>'firstName','') || ' ' || coalesce(s.profile->>'lastName',''))))>0)
 ), candidates AS MATERIALIZED (
   SELECT * FROM filtered WHERE (p_after IS NULL OR subscriber_id COLLATE "C">p_after COLLATE "C") AND (p_before IS NULL OR subscriber_id COLLATE "C"<p_before COLLATE "C")
   ORDER BY CASE WHEN p_before IS NULL THEN subscriber_id END COLLATE "C" ASC,CASE WHEN p_before IS NOT NULL THEN subscriber_id END COLLATE "C" DESC LIMIT 20
 ), bounds AS (
   SELECT min(subscriber_id COLLATE "C") AS first_id,max(subscriber_id COLLATE "C") AS last_id FROM candidates
 )
 SELECT jsonb_build_object('data',coalesce((SELECT jsonb_agg(jsonb_build_object('subscriberId',subscriber_id,'firstName',profile->>'firstName','lastName',profile->>'lastName','email',profile->>'email') ORDER BY subscriber_id COLLATE "C") FROM candidates),'[]'::jsonb),
  'previous',CASE WHEN EXISTS(SELECT 1 FROM filtered f WHERE f.subscriber_id COLLATE "C"<b.first_id COLLATE "C") THEN b.first_id ELSE NULL END,
  'next',CASE WHEN EXISTS(SELECT 1 FROM filtered f WHERE f.subscriber_id COLLATE "C">b.last_id COLLATE "C") THEN b.last_id ELSE NULL END) INTO v_result FROM bounds b;
 RETURN v_result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_subscribers(text,text,text,text,bigint,text,text,text) FROM PUBLIC;
