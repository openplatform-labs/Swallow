-- List access is independent of the existing permission to read one job.
CREATE TABLE public.novu_rust_operator_jwt_resend_jobs_grants (
 issuer text NOT NULL, subject text NOT NULL, client_id text NOT NULL, environment_id text NOT NULL,
 PRIMARY KEY(issuer,subject,client_id,environment_id),
 FOREIGN KEY(issuer,subject,client_id,environment_id)
  REFERENCES public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) ON DELETE CASCADE
);
REVOKE ALL ON public.novu_rust_operator_jwt_resend_jobs_grants FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_resend_jobs(
 p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_after uuid)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_result jsonb;
BEGIN
 PERFORM public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 PERFORM 1 FROM public.novu_rust_operator_jwt_resend_result_grants g
  WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env FOR SHARE OF g;
 IF NOT FOUND OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN
  RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501';
 END IF;
 PERFORM 1 FROM public.novu_rust_operator_jwt_resend_jobs_grants g
  WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env FOR SHARE OF g;
 IF NOT FOUND OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN
  RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501';
 END IF;
 WITH candidates AS MATERIALIZED (
  SELECT j.id FROM public.novu_queue_jobs j WHERE j.environment_id=p_env AND j.kind='experimental_resend_email'
   AND (p_after IS NULL OR j.id>p_after) ORDER BY j.id LIMIT 21
 ), page AS MATERIALIZED (SELECT id FROM candidates ORDER BY id LIMIT 20)
 SELECT jsonb_build_object('data',coalesce((SELECT jsonb_agg(public.novu_rust_operator_jwt_resend_result(p_issuer,p_subject,p_client,p_env,p_exp,id) ORDER BY id) FROM page),'[]'::jsonb),
  'hasMore',(SELECT count(*)>20 FROM candidates),
  'next',CASE WHEN (SELECT count(*)>20 FROM candidates) THEN (SELECT id::text FROM page ORDER BY id DESC LIMIT 1) ELSE NULL END)
 INTO v_result;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN v_result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_resend_jobs(text,text,text,text,bigint,uuid) FROM PUBLIC;
