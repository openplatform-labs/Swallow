-- Independent permission for authenticated operators to inspect one Resend job.
CREATE TABLE public.novu_rust_operator_jwt_resend_result_grants (
 issuer text NOT NULL, subject text NOT NULL, client_id text NOT NULL, environment_id text NOT NULL,
 PRIMARY KEY(issuer,subject,client_id,environment_id),
 FOREIGN KEY(issuer,subject,client_id,environment_id)
  REFERENCES public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) ON DELETE CASCADE
);
REVOKE ALL ON public.novu_rust_operator_jwt_resend_result_grants FROM PUBLIC;

CREATE FUNCTION public.novu_rust_operator_jwt_resend_result(
 p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_job uuid)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_result jsonb;
BEGIN
 PERFORM public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 PERFORM 1 FROM public.novu_rust_operator_jwt_resend_result_grants g
  WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env FOR SHARE OF g;
 IF NOT FOUND OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN
  RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501';
 END IF;
 IF p_job IS NULL THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 SELECT jsonb_build_object('jobId',j.id,'jobStatus',j.status,
  'result',CASE WHEN v.conflict OR v.ambiguous THEN 'unknown' ELSE coalesce(j.resend_delivery_result,'unknown') END,
  'providerIdAmbiguous',v.ambiguous,'attemptState',a.state,'providerIdKnown',coalesce(length(a.provider_message_id)>0,false),
  'autoCorrelated',c.job_id IS NOT NULL,'autoCorrelationConflict',v.conflict,'autoCorrelatedAt',c.recorded_at::text,
  'lastCompletedScanAt',(SELECT h.last_completed_at::text FROM public.novu_rust_resend_projection_heartbeat h WHERE h.environment_id=p_env))
 INTO v_result FROM public.novu_queue_jobs j
 LEFT JOIN public.novu_rust_resend_attempts a ON a.environment_id=j.environment_id AND a.job_id=j.id
 LEFT JOIN public.novu_rust_resend_auto_correlations c ON c.environment_id=j.environment_id AND c.job_id=j.id
 CROSS JOIN LATERAL (SELECT
  CASE WHEN c.job_id IS NOT NULL THEN public.novu_rust_resend_auto_evidence_conflicts(j.environment_id,j.id) ELSE false END AS conflict,
  EXISTS(SELECT 1 FROM public.novu_rust_resend_attempts b WHERE b.environment_id=j.environment_id AND b.provider_message_id=a.provider_message_id AND b.job_id<>a.job_id AND a.provider_message_id IS NOT NULL AND a.provider_message_id<>'') AS ambiguous) v
 WHERE j.id=p_job AND j.environment_id=p_env AND j.kind='experimental_resend_email';
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 IF v_result IS NULL THEN RAISE EXCEPTION 'channel job not found' USING ERRCODE='22023'; END IF;
 RETURN v_result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_resend_result(text,text,text,text,bigint,uuid) FROM PUBLIC;
