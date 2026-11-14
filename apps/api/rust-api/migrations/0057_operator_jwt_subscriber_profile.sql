-- Separate DBA-provisioned profile permissions; broad environment API keys remain optional.
CREATE TABLE public.novu_rust_operator_jwt_subscriber_profile_grants (
 issuer text NOT NULL,subject text NOT NULL,client_id text NOT NULL,environment_id text NOT NULL,permission text NOT NULL CHECK(permission IN ('read','write')),
 PRIMARY KEY(issuer,subject,client_id,environment_id,permission),
 FOREIGN KEY(issuer,subject,client_id,environment_id) REFERENCES public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) ON DELETE CASCADE
);
CREATE TABLE public.novu_rust_operator_jwt_subscriber_profile_audit (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(),environment_id text NOT NULL,subscriber_id text NOT NULL,generation uuid NOT NULL,previous_revision bigint NOT NULL,new_revision bigint NOT NULL,
 issuer text NOT NULL,subject text NOT NULL,client_id text NOT NULL,database_principal name NOT NULL,fields text[] NOT NULL,reason text NOT NULL,recorded_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
REVOKE ALL ON public.novu_rust_operator_jwt_subscriber_profile_grants,public.novu_rust_operator_jwt_subscriber_profile_audit FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_subscriber_profile_access(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_permission text)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF p_permission NOT IN ('read','write') OR p_permission IS NULL THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 PERFORM 1 FROM public.novu_rust_operator_jwt_subscriber_profile_grants g WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env AND g.permission=p_permission FOR SHARE OF g;
 IF NOT FOUND OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_subscriber_profile_access(text,text,text,text,bigint,text) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_subscriber_profile_read(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_subscriber text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE s public.novu_rust_subscribers%ROWTYPE;
BEGIN
 IF p_subscriber IS NULL OR octet_length(p_subscriber) NOT BETWEEN 1 AND 512 THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_subscriber_profile_access(p_issuer,p_subject,p_client,p_env,p_exp,'read');
 SELECT * INTO s FROM public.novu_rust_subscribers WHERE environment_id=p_env AND subscriber_id=p_subscriber;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 IF NOT FOUND THEN RETURN jsonb_build_object('found',false); END IF;
 RETURN jsonb_build_object('found',true,'subscriberId',s.subscriber_id,'generation',s.management_generation,'revision',s.profile_revision::text,'profile',jsonb_build_object('firstName',s.profile->'firstName','lastName',s.profile->'lastName','email',s.profile->'email','phone',s.profile->'phone'));
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_subscriber_profile_read(text,text,text,text,bigint,text) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_subscriber_profile_write(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_subscriber text,p_generation uuid,p_revision bigint,p_profile jsonb,p_reason text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE s public.novu_rust_subscribers%ROWTYPE; v_revision bigint; v_profile jsonb; v_fields text[];
BEGIN
 IF p_subscriber IS NULL OR octet_length(p_subscriber) NOT BETWEEN 1 AND 512 OR p_generation IS NULL OR p_revision IS NULL OR p_revision<0 OR p_reason IS NULL OR octet_length(btrim(p_reason)) NOT BETWEEN 1 AND 1000 OR p_profile IS NULL OR jsonb_typeof(p_profile)<>'object' THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 SELECT array_agg(key ORDER BY key) INTO v_fields FROM jsonb_object_keys(p_profile) key;
 IF v_fields IS NULL OR NOT v_fields <@ ARRAY['firstName','lastName','email','phone']::text[] OR EXISTS(SELECT 1 FROM jsonb_each(p_profile) e WHERE jsonb_typeof(e.value) NOT IN ('string','null') OR octet_length(e.value::text)>8192) THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_subscriber_profile_access(p_issuer,p_subject,p_client,p_env,p_exp,'write');
 SELECT * INTO s FROM public.novu_rust_subscribers WHERE environment_id=p_env AND subscriber_id=p_subscriber FOR UPDATE;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 IF NOT FOUND OR s.management_generation<>p_generation OR s.profile_revision<>p_revision THEN RAISE EXCEPTION 'stale subscriber' USING ERRCODE='P4090'; END IF;
 UPDATE public.novu_rust_subscribers SET profile=s.profile||p_profile,updated_at=CASE WHEN s.profile IS DISTINCT FROM s.profile||p_profile THEN now() ELSE updated_at END WHERE environment_id=p_env AND subscriber_id=p_subscriber RETURNING profile_revision,profile INTO v_revision,v_profile;
 IF v_revision<>s.profile_revision THEN INSERT INTO public.novu_rust_operator_jwt_subscriber_profile_audit(environment_id,subscriber_id,generation,previous_revision,new_revision,issuer,subject,client_id,database_principal,fields,reason) VALUES(p_env,p_subscriber,p_generation,s.profile_revision,v_revision,p_issuer,p_subject,p_client,session_user,v_fields,btrim(p_reason)); END IF;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN jsonb_build_object('subscriberId',p_subscriber,'generation',p_generation,'revision',v_revision::text,'changed',v_revision<>s.profile_revision);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_subscriber_profile_write(text,text,text,text,bigint,text,uuid,bigint,jsonb,text) FROM PUBLIC;
