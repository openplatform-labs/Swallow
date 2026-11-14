-- Scoped FCM target reconciliation with the same group semantics as the legacy tool.
CREATE OR REPLACE FUNCTION public.novu_rust_resolve_fcm(p_environment text,p_job uuid,p_target integer,p_decision text,p_operator text,p_reason text)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_kind text; v_status text; v_error text; v_payload jsonb; v_meta jsonb;
  v_http integer; v_history integer; v_size integer; v_end integer; v_count bigint; v_uncertain bigint; v_expected integer;
BEGIN
  IF p_environment IS NULL OR length(p_environment)=0 OR p_job IS NULL OR p_target IS NULL OR p_target<0
    OR p_decision IS NULL OR p_decision NOT IN ('retry','accepted')
    OR p_operator IS NULL OR length(btrim(p_operator))=0 OR octet_length(p_operator)>128
    OR p_reason IS NULL OR length(btrim(p_reason))=0 OR octet_length(p_reason)>512 THEN
    RAISE EXCEPTION 'invalid reconciliation parameters' USING ERRCODE='22023';
  END IF;
  IF NOT public.novu_rust_lock_operator_grant(p_environment,p_decision) THEN
    RAISE EXCEPTION 'reconciliation permission denied' USING ERRCODE='42501';
  END IF;
  SELECT j.kind,j.status,j.last_error,j.payload INTO v_kind,v_status,v_error,v_payload
    FROM public.novu_queue_jobs j WHERE j.environment_id=p_environment AND j.id=p_job FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'job not found' USING ERRCODE='22023'; END IF;
  IF v_status<>'failed' OR v_kind<>'experimental_fcm_push' THEN RAISE EXCEPTION 'job kind or status invalid' USING ERRCODE='22023'; END IF;
  IF p_decision='retry' AND v_error IS DISTINCT FROM 'push delivery outcome unknown' THEN
    RAISE EXCEPTION 'cannot retry push job with a different failure reason' USING ERRCODE='22023';
  END IF;
  SELECT a.http_status INTO v_http FROM public.novu_rust_fcm_attempts a
    WHERE a.environment_id=p_environment AND a.job_id=p_job AND a.target_index=p_target AND a.state='unknown' FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'unknown attempt not found' USING ERRCODE='22023'; END IF;
  v_meta:=v_payload #> ARRAY['prepared_context','target_metadata',p_target::text];
  v_history:=p_target; v_size:=1;
  IF jsonb_typeof(v_meta->'history_index')='number' AND (v_meta->>'history_index') ~ '^-?[0-9]+$' THEN
    IF (v_meta->>'history_index')::numeric BETWEEN -2147483648 AND 2147483647 THEN v_history:=(v_meta->>'history_index')::integer; END IF;
  END IF;
  IF jsonb_typeof(v_meta->'group_size')='number' AND (v_meta->>'group_size') ~ '^-?[0-9]+$' THEN
    IF (v_meta->>'group_size')::numeric BETWEEN 1 AND 2147483647 THEN v_size:=(v_meta->>'group_size')::integer; END IF;
  END IF;
  v_end:=least(2147483647::bigint,v_history::bigint+v_size::bigint)::integer;
  INSERT INTO public.novu_rust_delivery_reconciliations(id,environment_id,job_id,target_index,decision,operator_name,reason,previous_http_status,retry_dispatch_id,database_user,database_session_user)
    VALUES (gen_random_uuid(),p_environment,p_job,p_target,p_decision,p_operator,p_reason,v_http,NULL,session_user,session_user);
  IF p_decision='retry' THEN
    DELETE FROM public.novu_rust_fcm_attempts WHERE environment_id=p_environment AND job_id=p_job AND target_index=p_target;
    UPDATE public.novu_rust_push_messages SET delivery_state=CASE WHEN EXISTS(SELECT 1 FROM public.novu_rust_fcm_attempts WHERE environment_id=p_environment AND job_id=p_job AND target_index>=v_history AND target_index<v_end AND state='unknown') THEN 'unknown' ELSE 'in_progress' END,
      status='sent',error_id=NULL,error_text=NULL WHERE environment_id=p_environment AND push_job_id=p_job AND target_index=v_history;
    UPDATE public.novu_queue_jobs SET status='pending',available_at=now(),last_error=NULL WHERE environment_id=p_environment AND id=p_job;
  ELSE
    UPDATE public.novu_rust_fcm_attempts SET state='accepted',updated_at=clock_timestamp() WHERE environment_id=p_environment AND job_id=p_job AND target_index=p_target;
    SELECT count(*),count(*) FILTER(WHERE state IN ('unknown','dispatching')) INTO v_count,v_uncertain FROM public.novu_rust_fcm_attempts
      WHERE environment_id=p_environment AND job_id=p_job AND target_index>=v_history AND target_index<v_end;
    IF v_uncertain>0 THEN
      UPDATE public.novu_rust_push_messages SET delivery_state='unknown' WHERE environment_id=p_environment AND push_job_id=p_job AND target_index=v_history;
    ELSIF v_count=v_size THEN
      UPDATE public.novu_rust_push_messages SET delivery_state='accepted',status='sent',error_id=NULL,error_text=NULL,identifier=internal_id WHERE environment_id=p_environment AND push_job_id=p_job AND target_index=v_history;
    ELSIF v_count<v_size THEN
      UPDATE public.novu_rust_push_messages SET delivery_state='in_progress' WHERE environment_id=p_environment AND push_job_id=p_job AND target_index=v_history;
    END IF;
    SELECT count(*),count(*) FILTER(WHERE state IN ('unknown','dispatching')) INTO v_count,v_uncertain
      FROM public.novu_rust_fcm_attempts WHERE environment_id=p_environment AND job_id=p_job;
    IF jsonb_typeof(v_payload->'prepared_messages')='array' THEN v_expected:=jsonb_array_length(v_payload->'prepared_messages'); END IF;
    IF v_uncertain=0 AND v_error='push delivery outcome unknown' AND v_expected>0 THEN
      IF v_count=v_expected THEN
        UPDATE public.novu_queue_jobs SET status='completed',last_error=NULL WHERE environment_id=p_environment AND id=p_job;
      ELSIF v_count<v_expected THEN
        UPDATE public.novu_queue_jobs SET status='pending',available_at=now(),last_error=NULL WHERE environment_id=p_environment AND id=p_job;
      END IF;
    END IF;
  END IF;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_resolve_fcm(text,uuid,integer,text,text,text) FROM PUBLIC;
