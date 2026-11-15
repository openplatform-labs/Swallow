-- Independent operator read and list permissions; worker alert_view is also required.
CREATE TABLE public.novu_rust_operator_jwt_fcm_unrecorded_claim_grants (
 issuer text NOT NULL, subject text NOT NULL, client_id text NOT NULL, environment_id text NOT NULL,
 can_health boolean NOT NULL DEFAULT false, can_list boolean NOT NULL DEFAULT false,
 PRIMARY KEY(issuer,subject,client_id,environment_id),
 FOREIGN KEY(issuer,subject,client_id,environment_id) REFERENCES public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) ON DELETE CASCADE
);
REVOKE ALL ON public.novu_rust_operator_jwt_fcm_unrecorded_claim_grants FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_fcm_unrecorded_claims(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_gap integer,p_grace integer,p_list boolean,p_after uuid)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE permitted boolean; result jsonb;
BEGIN
 IF p_gap IS NULL OR p_gap NOT BETWEEN 2 AND 100000 OR p_grace IS NULL OR p_grace NOT BETWEEN 1 AND 86400 OR p_list IS NULL OR (NOT p_list AND p_after IS NOT NULL) THEN RAISE EXCEPTION 'invalid threshold' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 SELECT CASE WHEN p_list THEN can_list ELSE can_health END INTO permitted
 FROM public.novu_rust_operator_jwt_fcm_unrecorded_claim_grants g
 WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env FOR SHARE OF g;
 IF NOT coalesce(permitted,false) OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 IF p_list THEN
  WITH rows AS MATERIALIZED (SELECT * FROM public.novu_rust_fcm_unrecorded_claim_list(p_env,p_gap,p_grace,p_after)),
  numbered AS (SELECT *,row_number() OVER (ORDER BY job_id) AS rn FROM rows)
  SELECT jsonb_build_object('data',coalesce(jsonb_agg(jsonb_build_object('jobId',job_id,'status',status,'availableAt',available_at,'claims',claims,'recordedAttempts',recorded_attempts,'unrecordedClaims',unrecorded_claims) ORDER BY job_id) FILTER (WHERE rn<=100),'[]'::jsonb),
    'hasMore',count(*)>100,'next',CASE WHEN count(*)>100 THEN max(job_id::text) FILTER (WHERE rn=100) ELSE NULL END)
  INTO result FROM numbered;
 ELSE
  SELECT jsonb_build_object('unrecordedClaimJobs',public.novu_rust_fcm_unrecorded_claim_health(p_env,p_gap,p_grace)) INTO result;
 END IF;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_fcm_unrecorded_claims(text,text,text,text,bigint,integer,integer,boolean,uuid) FROM PUBLIC;
