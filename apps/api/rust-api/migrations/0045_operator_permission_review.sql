-- Explicit JWT principals permitted to review permissions and/or audit in one environment.
CREATE TABLE public.novu_rust_operator_jwt_permission_review_grants (
 issuer text NOT NULL,
 subject text NOT NULL,
 client_id text NOT NULL,
 environment_id text NOT NULL,
 identity_read boolean NOT NULL DEFAULT false,
 audit_read boolean NOT NULL DEFAULT false,
 PRIMARY KEY(issuer,subject,client_id,environment_id),
 FOREIGN KEY(issuer,subject,client_id,environment_id)
  REFERENCES public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) ON DELETE CASCADE
);
REVOKE ALL ON public.novu_rust_operator_jwt_permission_review_grants FROM PUBLIC;

CREATE FUNCTION public.novu_rust_operator_jwt_review_access(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_audit boolean)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF p_audit IS NULL THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 PERFORM 1 FROM public.novu_rust_operator_jwt_permission_review_grants g
 WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env
  AND CASE WHEN p_audit THEN g.audit_read ELSE g.identity_read END FOR SHARE OF g;
 IF NOT FOUND OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN
  RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501';
 END IF;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_review_access(text,text,text,text,bigint,boolean) FROM PUBLIC;

CREATE FUNCTION public.novu_rust_operator_jwt_review_identity(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_target_issuer text,p_target_subject text,p_target_client text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_identity public.novu_rust_operator_jwt_identities%ROWTYPE;
BEGIN
 IF p_target_issuer IS NULL OR octet_length(p_target_issuer) NOT BETWEEN 1 AND 2048 OR
    p_target_subject IS NULL OR octet_length(p_target_subject) NOT BETWEEN 1 AND 1024 OR
    p_target_client IS NULL OR octet_length(p_target_client) NOT BETWEEN 1 AND 1024 THEN
  RAISE EXCEPTION 'invalid request' USING ERRCODE='22023';
 END IF;
 PERFORM public.novu_rust_operator_jwt_review_access(p_issuer,p_subject,p_client,p_env,p_exp,false);
 SELECT * INTO v_identity FROM public.novu_rust_operator_jwt_identities i
 WHERE i.environment_id=p_env AND i.issuer=p_target_issuer AND i.subject=p_target_subject AND i.client_id=p_target_client FOR SHARE;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 IF NOT FOUND THEN RETURN pg_catalog.jsonb_build_object('found',false); END IF;
 RETURN pg_catalog.jsonb_build_object('found',true,'identity',pg_catalog.to_jsonb(v_identity));
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_review_identity(text,text,text,text,bigint,text,text,text) FROM PUBLIC;

CREATE FUNCTION public.novu_rust_operator_jwt_review_audit(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_before bigint)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_result jsonb;
BEGIN
 IF p_before IS NOT NULL AND p_before<1 THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_review_access(p_issuer,p_subject,p_client,p_env,p_exp,true);
 WITH candidates AS MATERIALIZED (
  SELECT id,recorded_at,issuer,subject,client_id,actor,actor_oid,reason,previous,current
  FROM public.novu_rust_operator_jwt_permission_audit
  WHERE environment_id=p_env AND (p_before IS NULL OR id<p_before)
  ORDER BY id DESC LIMIT 51
 ), page AS MATERIALIZED (SELECT * FROM candidates ORDER BY id DESC LIMIT 50)
 SELECT pg_catalog.jsonb_build_object('data',coalesce((SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
  'id',id::text,'recordedAt',recorded_at,'issuer',issuer,'subject',subject,'clientId',client_id,
  'actor',actor,'actorOid',actor_oid,'reason',reason,'previous',previous,'current',current
 ) ORDER BY id DESC) FROM page),'[]'::jsonb),
 'next',CASE WHEN (SELECT count(*) FROM candidates)>50 THEN (SELECT min(id)::text FROM page) ELSE NULL END) INTO v_result;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN v_result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_review_audit(text,text,text,text,bigint,bigint) FROM PUBLIC;
