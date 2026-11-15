-- Expose bounded Slack rate-limit guidance. Requires worker 0170.
CREATE OR REPLACE FUNCTION public.novu_rust_operator_jwt_slack_result(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_job uuid)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE result jsonb;
BEGIN
 PERFORM public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 PERFORM 1 FROM public.novu_rust_operator_jwt_slack_result_grants g WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env FOR SHARE OF g;
 IF NOT FOUND OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 IF p_job IS NULL THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 SELECT jsonb_build_object('jobId',j.id,'jobStatus',j.status,
 'kind',CASE j.kind WHEN 'experimental_slack_chat' THEN 'app' WHEN 'experimental_slack_webhook' THEN 'webhook' WHEN 'experimental_slack_workflow_fanout' THEN 'workflow-fanout' ELSE 'workflow-delivery' END,
 'attemptState',a.state,'attemptId',a.attempt_id,'attemptHttpStatus',a.http_status,'retryAfterSeconds',a.retry_after_seconds,'retryNotBefore',a.retry_not_before::text,'providerIdKnown',coalesce(length(a.provider_message_id)>0,false),
 'hasResolutionHistory',EXISTS(SELECT 1 FROM public.novu_rust_slack_resolutions r WHERE r.environment_id=j.environment_id AND r.job_id=j.id),
 'failureCategory',CASE WHEN j.last_error IS NULL THEN NULL WHEN j.last_error='Slack outcome unknown' THEN 'unknown'
  WHEN j.last_error='Slack rejected request' THEN 'provider-rejected'
  WHEN j.last_error IN ('Slack endpoint changed or deleted','Slack endpoint snapshot changed','Slack endpoint revision missing','Slack endpoint snapshot missing','invalid Slack endpoint snapshot') THEN 'endpoint-review'
  WHEN j.last_error='no Slack endpoints' THEN 'endpoint-missing'
  WHEN j.last_error='Slack integration credentials not configured' THEN 'credentials-missing'
  WHEN j.last_error IN ('Slack workflow step disabled','Slack workflow content changed') THEN 'workflow-policy'
  WHEN j.last_error IN ('subscriber identity changed','subscriber not found','actor identity changed or missing') THEN 'identity-review'
  WHEN j.last_error='chat preference disabled' THEN 'preference'
  WHEN j.last_error IN ('invalid Slack job','invalid Slack request','invalid Slack workflow delivery','invalid Slack workflow job','unsupported Slack template or input') THEN 'invalid-request'
  ELSE 'other' END) INTO result
 FROM public.novu_queue_jobs j LEFT JOIN public.novu_rust_slack_attempts a ON a.environment_id=j.environment_id AND a.job_id=j.id
 WHERE j.id=p_job AND j.environment_id=p_env AND j.kind IN ('experimental_slack_chat','experimental_slack_webhook','experimental_slack_workflow_fanout','experimental_slack_workflow_delivery');
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 IF result IS NULL THEN RAISE EXCEPTION 'channel job not found' USING ERRCODE='22023'; END IF;
 RETURN result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_slack_result(text,text,text,text,bigint,uuid) FROM PUBLIC;
