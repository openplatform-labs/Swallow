-- Filter current Resend attempts without exposing payloads.
CREATE INDEX IF NOT EXISTS novu_rust_resend_attempts_operator_filter_idx
 ON public.novu_rust_resend_attempts(environment_id,state,http_status,job_id);
CREATE FUNCTION public.novu_rust_operator_jwt_resend_jobs_filtered(
 p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_after uuid,p_filter text)
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
 IF p_filter IS NULL OR p_filter NOT IN ('unknown','rejected','rate-limited') THEN RAISE EXCEPTION 'invalid filter' USING ERRCODE='22023'; END IF;
 WITH candidates AS MATERIALIZED (
  SELECT j.id FROM public.novu_queue_jobs j JOIN public.novu_rust_resend_attempts a ON a.job_id=j.id AND a.environment_id=j.environment_id
  WHERE j.environment_id=p_env AND j.kind='experimental_resend_email'
   AND (p_after IS NULL OR j.id>p_after)
   AND ((p_filter='unknown' AND a.state='unknown') OR (p_filter='rejected' AND a.state='rejected') OR (p_filter='rate-limited' AND a.state='rejected' AND a.http_status=429))
   ORDER BY j.id LIMIT 21
 ), page AS MATERIALIZED (SELECT id FROM candidates ORDER BY id LIMIT 20)
 SELECT jsonb_build_object('data',coalesce((SELECT jsonb_agg(public.novu_rust_operator_jwt_resend_result(p_issuer,p_subject,p_client,p_env,p_exp,id) ORDER BY id) FROM page),'[]'::jsonb),
  'hasMore',(SELECT count(*)>20 FROM candidates),
  'next',CASE WHEN (SELECT count(*)>20 FROM candidates) THEN (SELECT id::text FROM page ORDER BY id DESC LIMIT 1) ELSE NULL END)
 INTO v_result;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN v_result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_resend_jobs_filtered(text,text,text,text,bigint,uuid,text) FROM PUBLIC;
