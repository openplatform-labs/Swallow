-- Stable identity survives state changes and is recorded before retry removes an attempt.
ALTER TABLE public.novu_rust_slack_attempts ADD COLUMN IF NOT EXISTS attempt_id uuid NOT NULL DEFAULT gen_random_uuid();
ALTER TABLE public.novu_rust_slack_resolutions ADD COLUMN IF NOT EXISTS attempt_id uuid;

CREATE OR REPLACE FUNCTION public.novu_rust_list_slack_attention_v2(p_environment text,p_after uuid)
RETURNS TABLE(job_id uuid,kind text,job_status text,attempts integer,http_status integer,category text,attempt_id uuid)
LANGUAGE sql SECURITY DEFINER SET search_path=pg_catalog AS $$
 SELECT s.job_id,s.kind,s.job_status,s.attempts,s.http_status,s.category,a.attempt_id
 FROM public.novu_rust_list_slack_attention(p_environment,p_after) s
 LEFT JOIN public.novu_rust_slack_attempts a ON a.environment_id=p_environment AND a.job_id=s.job_id
 ORDER BY s.job_id
$$;
REVOKE ALL ON FUNCTION public.novu_rust_list_slack_attention_v2(text,uuid) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.novu_rust_resolve_slack_v2(
 p_environment text,p_job uuid,p_attempt uuid,p_decision text,p_operator text,p_reason text
) RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_status text; v_previous text; v_attempt uuid;
BEGIN
 IF p_environment IS NULL OR length(btrim(p_environment))=0 OR p_job IS NULL OR p_attempt IS NULL
    OR p_decision IS NULL OR p_decision NOT IN ('retry','accepted')
    OR p_operator IS NULL OR length(btrim(p_operator))=0 OR octet_length(p_operator)>128
    OR p_reason IS NULL OR length(btrim(p_reason))=0 OR octet_length(p_reason)>512 THEN
   RAISE EXCEPTION 'invalid Slack resolution' USING ERRCODE='22023';
 END IF;
 IF NOT public.novu_rust_lock_operator_grant(p_environment,'slack_' || p_decision) THEN
   RAISE EXCEPTION 'Slack resolution permission denied' USING ERRCODE='42501';
 END IF;
 SELECT j.status,a.state,a.attempt_id INTO v_status,v_previous,v_attempt
 FROM public.novu_queue_jobs j JOIN public.novu_rust_slack_attempts a ON a.job_id=j.id AND a.environment_id=j.environment_id
 WHERE j.id=p_job AND j.environment_id=p_environment AND j.kind IN ('experimental_slack_chat','experimental_slack_webhook','experimental_slack_workflow_delivery')
 FOR UPDATE OF j,a;
 IF NOT FOUND THEN RAISE EXCEPTION 'Slack attempt not found' USING ERRCODE='22023'; END IF;
 IF v_attempt<>p_attempt THEN RAISE EXCEPTION 'Slack attempt changed' USING ERRCODE='22023'; END IF;
 IF v_status<>'failed' OR v_previous NOT IN ('unknown','rejected') OR (p_decision='accepted' AND v_previous<>'unknown') THEN
   RAISE EXCEPTION 'Slack outcome is not eligible for this decision' USING ERRCODE='22023';
 END IF;
 INSERT INTO public.novu_rust_slack_resolutions(id,environment_id,job_id,attempt_id,previous_state,decision,operator_name,reason,database_user,database_session_user)
 VALUES(gen_random_uuid(),p_environment,p_job,v_attempt,v_previous,p_decision,p_operator,p_reason,session_user,session_user);
 IF p_decision='retry' THEN
   DELETE FROM public.novu_rust_slack_attempts WHERE job_id=p_job AND environment_id=p_environment;
   UPDATE public.novu_queue_jobs SET status='pending',available_at=clock_timestamp(),last_error=NULL WHERE id=p_job AND environment_id=p_environment;
 ELSE
   UPDATE public.novu_rust_slack_attempts SET state='accepted',updated_at=clock_timestamp() WHERE job_id=p_job AND environment_id=p_environment;
   UPDATE public.novu_queue_jobs SET status='completed',last_error=NULL WHERE id=p_job AND environment_id=p_environment;
 END IF;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_resolve_slack_v2(text,uuid,uuid,text,text,text) FROM PUBLIC;
