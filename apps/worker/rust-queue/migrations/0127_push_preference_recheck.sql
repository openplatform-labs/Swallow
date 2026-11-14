-- An environment chooses one preference policy for all current FCM workers.
ALTER TABLE public.novu_rust_push_workflow_worker_config ADD COLUMN IF NOT EXISTS preference_policy text DEFAULT 'snapshot';
ALTER TABLE public.novu_rust_push_workflow_worker_config DROP CONSTRAINT IF EXISTS novu_rust_push_preference_policy_check;
ALTER TABLE public.novu_rust_push_workflow_worker_config ADD CONSTRAINT novu_rust_push_preference_policy_check CHECK (preference_policy IS NULL OR preference_policy IN ('snapshot','recheck'));
CREATE INDEX IF NOT EXISTS novu_rust_push_preference_policy_cancelled_idx ON public.novu_queue_jobs(environment_id,id) WHERE kind='experimental_fcm_push' AND status='cancelled' AND last_error IN ('push preference recheck disabled','push preference context missing');

CREATE OR REPLACE FUNCTION public.novu_rust_channel_policy_revision() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
 IF NEW.kind IN ('experimental_sendgrid_email','experimental_resend_email','experimental_twilio_sms','experimental_fcm_push') AND NEW.status='cancelled'
 AND NEW.last_error IN ('email workflow identity missing','email workflow step unavailable','email workflow step disabled','email workflow step changed','sms workflow identity missing','sms workflow step unavailable','sms workflow step disabled','sms workflow step changed','push workflow identity or snapshot missing','push workflow step unavailable','push workflow step disabled','push workflow step changed','push preference recheck disabled','push preference context missing')
 AND (OLD.kind IS DISTINCT FROM NEW.kind OR OLD.status IS DISTINCT FROM NEW.status OR OLD.attempts IS DISTINCT FROM NEW.attempts OR OLD.last_error IS DISTINCT FROM NEW.last_error) THEN
  NEW.channel_policy_revision:=OLD.channel_policy_revision+1;
 END IF;
 RETURN NEW;
END; $$;

CREATE INDEX IF NOT EXISTS novu_rust_push_policy_cancelled_idx ON public.novu_queue_jobs(environment_id,id)
 WHERE status='cancelled' AND kind='experimental_fcm_push' AND last_error IN ('push workflow identity or snapshot missing','push workflow step unavailable','push workflow step disabled','push workflow step changed','push preference recheck disabled','push preference context missing');

CREATE OR REPLACE FUNCTION public.novu_rust_list_channel_policy_attention(p_environment text,p_channel text,p_after uuid)
RETURNS TABLE(job_id uuid,kind text,reason_code text,attempts integer,created_at text,revision text)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 PERFORM public.novu_rust_assert_alert_view(p_environment);
 IF p_channel IS NULL OR p_channel NOT IN ('email','sms','push','all') THEN RAISE EXCEPTION 'invalid channel' USING ERRCODE='22023'; END IF;
 RETURN QUERY SELECT j.id,j.kind,j.last_error,j.attempts,j.created_at::text,j.channel_policy_revision::text FROM public.novu_queue_jobs j
 WHERE j.environment_id=p_environment AND j.status='cancelled' AND (p_after IS NULL OR j.id>p_after)
 AND ((p_channel IN ('email','all') AND j.kind IN ('experimental_sendgrid_email','experimental_resend_email') AND j.last_error IN ('email workflow identity missing','email workflow step unavailable','email workflow step disabled','email workflow step changed')) OR (p_channel IN ('sms','all') AND j.kind='experimental_twilio_sms' AND j.last_error IN ('sms workflow identity missing','sms workflow step unavailable','sms workflow step disabled','sms workflow step changed')) OR (p_channel IN ('push','all') AND j.kind='experimental_fcm_push' AND j.last_error IN ('push workflow identity or snapshot missing','push workflow step unavailable','push workflow step disabled','push workflow step changed','push preference recheck disabled','push preference context missing')))
 AND NOT EXISTS(SELECT 1 FROM public.novu_rust_channel_policy_acknowledgements a WHERE a.environment_id=j.environment_id AND a.job_id=j.id AND a.revision=j.channel_policy_revision)
 ORDER BY j.id LIMIT 100;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_list_channel_policy_attention(text,text,uuid) FROM PUBLIC;


CREATE OR REPLACE FUNCTION public.novu_rust_count_channel_policy_attention(p_environment text,p_channel text) RETURNS bigint
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE n bigint;
BEGIN
 PERFORM public.novu_rust_assert_alert_view(p_environment);
 IF p_channel IS NULL OR p_channel NOT IN ('email','sms','push','all') THEN RAISE EXCEPTION 'invalid channel' USING ERRCODE='22023'; END IF;
 SELECT count(*) INTO n FROM public.novu_queue_jobs j
 WHERE j.environment_id=p_environment AND j.status='cancelled'
 AND ((p_channel IN ('email','all') AND j.kind IN ('experimental_sendgrid_email','experimental_resend_email') AND j.last_error IN ('email workflow identity missing','email workflow step unavailable','email workflow step disabled','email workflow step changed')) OR (p_channel IN ('sms','all') AND j.kind='experimental_twilio_sms' AND j.last_error IN ('sms workflow identity missing','sms workflow step unavailable','sms workflow step disabled','sms workflow step changed')) OR (p_channel IN ('push','all') AND j.kind='experimental_fcm_push' AND j.last_error IN ('push workflow identity or snapshot missing','push workflow step unavailable','push workflow step disabled','push workflow step changed','push preference recheck disabled','push preference context missing')))
 AND NOT EXISTS(SELECT 1 FROM public.novu_rust_channel_policy_acknowledgements a WHERE a.environment_id=j.environment_id AND a.job_id=j.id AND a.revision=j.channel_policy_revision);
 RETURN n;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_count_channel_policy_attention(text,text) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.novu_rust_ack_channel_policy(p_environment text,p_job uuid,p_expected_revision bigint,p_operator text,p_reason text) RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_revision bigint; v_count integer;
BEGIN
 IF p_environment IS NULL OR length(btrim(p_environment))=0 OR p_job IS NULL OR p_expected_revision IS NULL OR p_expected_revision<0 OR p_operator IS NULL OR length(btrim(p_operator))=0 OR octet_length(p_operator)>128 OR p_reason IS NULL OR length(btrim(p_reason))=0 OR octet_length(p_reason)>512 THEN RAISE EXCEPTION 'invalid policy acknowledgement' USING ERRCODE='22023'; END IF;
 IF NOT public.novu_rust_lock_operator_grant(p_environment,'queue_ack') THEN RAISE EXCEPTION 'queue acknowledgement permission denied' USING ERRCODE='42501'; END IF;
 SELECT j.channel_policy_revision INTO v_revision FROM public.novu_queue_jobs j WHERE j.environment_id=p_environment AND j.id=p_job AND j.status='cancelled'
 AND ((j.kind IN ('experimental_sendgrid_email','experimental_resend_email') AND j.last_error IN ('email workflow identity missing','email workflow step unavailable','email workflow step disabled','email workflow step changed')) OR (j.kind='experimental_twilio_sms' AND j.last_error IN ('sms workflow identity missing','sms workflow step unavailable','sms workflow step disabled','sms workflow step changed')) OR (j.kind='experimental_fcm_push' AND j.last_error IN ('push workflow identity or snapshot missing','push workflow step unavailable','push workflow step disabled','push workflow step changed','push preference recheck disabled','push preference context missing'))) FOR UPDATE OF j;
 IF NOT FOUND THEN RAISE EXCEPTION 'policy cancellation not found' USING ERRCODE='22023'; END IF;
 IF v_revision<>p_expected_revision THEN RAISE EXCEPTION 'policy cancellation changed; review again' USING ERRCODE='40001'; END IF;
 INSERT INTO public.novu_rust_channel_policy_acknowledgements(job_id,environment_id,revision,operator_name,reason) VALUES(p_job,p_environment,v_revision,p_operator,p_reason) ON CONFLICT(job_id,revision) DO NOTHING;
 GET DIAGNOSTICS v_count=ROW_COUNT; RETURN v_count=1;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_ack_channel_policy(text,uuid,bigint,text,text) FROM PUBLIC;

-- Aggregate-only point-in-time evidence for an operator reviewing a partial push.
CREATE OR REPLACE FUNCTION public.novu_rust_push_policy_evidence(p_environment text,p_job uuid,p_revision bigint)
RETURNS TABLE(accepted bigint,rejected bigint,unknown bigint,dispatching bigint,unattempted bigint)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE current_revision bigint; total integer; n bigint; min_target integer; max_target integer;
BEGIN
 PERFORM public.novu_rust_assert_alert_view(p_environment);
 IF p_job IS NULL OR p_revision IS NULL OR p_revision<0 THEN RAISE EXCEPTION 'invalid push policy evidence request' USING ERRCODE='22023'; END IF;
 SELECT j.channel_policy_revision,CASE WHEN jsonb_typeof(j.payload->'prepared_messages')='array' THEN jsonb_array_length(j.payload->'prepared_messages') ELSE NULL END
 INTO current_revision,total FROM public.novu_queue_jobs j WHERE j.environment_id=p_environment AND j.id=p_job AND j.kind='experimental_fcm_push' AND j.status='cancelled' AND j.last_error IN ('push workflow identity or snapshot missing','push workflow step unavailable','push workflow step disabled','push workflow step changed','push preference recheck disabled','push preference context missing') FOR SHARE OF j;
 IF NOT FOUND THEN RAISE EXCEPTION 'push policy cancellation not found' USING ERRCODE='22023'; END IF;
 IF current_revision<>p_revision THEN RAISE EXCEPTION 'policy cancellation changed; review again' USING ERRCODE='40001'; END IF;
 SELECT count(*) FILTER(WHERE a.state='accepted'),count(*) FILTER(WHERE a.state='rejected'),count(*) FILTER(WHERE a.state='unknown'),count(*) FILTER(WHERE a.state='dispatching'),count(*),min(a.target_index),max(a.target_index)
 INTO accepted,rejected,unknown,dispatching,n,min_target,max_target FROM public.novu_rust_fcm_attempts a WHERE a.environment_id=p_environment AND a.job_id=p_job;
 unattempted:=CASE WHEN total IS NOT NULL AND n<=total AND (n=0 OR (min_target>=0 AND max_target<total)) THEN total-n ELSE NULL END;
 RETURN NEXT;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_push_policy_evidence(text,uuid,bigint) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.novu_rust_resolve_cancelled_push(p_environment text,p_job uuid,p_revision bigint,p_target integer,p_decision text,p_operator text,p_reason text)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_revision bigint; v_http integer; v_permission text;
BEGIN
 IF p_environment IS NULL OR length(btrim(p_environment))=0 OR p_job IS NULL OR p_revision IS NULL OR p_revision<0 OR p_target IS NULL OR p_target<0
 OR p_decision IS NULL OR p_decision NOT IN ('accepted','not_accepted') OR p_operator IS NULL OR length(btrim(p_operator))=0 OR octet_length(p_operator)>128
 OR p_reason IS NULL OR length(btrim(p_reason))=0 OR octet_length(p_reason)>512 THEN RAISE EXCEPTION 'invalid cancelled push resolution' USING ERRCODE='22023'; END IF;
 v_permission:=CASE WHEN p_decision='accepted' THEN 'accepted' ELSE 'retry' END;
 IF NOT public.novu_rust_lock_operator_grant(p_environment,v_permission) THEN RAISE EXCEPTION 'resolution permission denied' USING ERRCODE='42501'; END IF;
 SELECT j.channel_policy_revision INTO v_revision FROM public.novu_queue_jobs j WHERE j.environment_id=p_environment AND j.id=p_job AND j.kind='experimental_fcm_push' AND j.status='cancelled'
 AND j.last_error IN ('push workflow identity or snapshot missing','push workflow step unavailable','push workflow step disabled','push workflow step changed','push preference recheck disabled','push preference context missing') FOR UPDATE OF j;
 IF NOT FOUND THEN RAISE EXCEPTION 'push policy cancellation not found' USING ERRCODE='22023'; END IF;
 IF v_revision<>p_revision THEN RAISE EXCEPTION 'policy cancellation changed; review again' USING ERRCODE='40001'; END IF;
 SELECT a.http_status INTO v_http FROM public.novu_rust_fcm_attempts a WHERE a.environment_id=p_environment AND a.job_id=p_job AND a.target_index=p_target AND a.state='unknown' FOR UPDATE OF a;
 IF NOT FOUND THEN RAISE EXCEPTION 'unknown push attempt not found' USING ERRCODE='22023'; END IF;
 INSERT INTO public.novu_rust_cancelled_push_resolutions(id,environment_id,job_id,revision,target_index,decision,operator_name,reason,database_session_user,previous_http_status)
 VALUES(gen_random_uuid(),p_environment,p_job,p_revision,p_target,p_decision,p_operator,p_reason,session_user,v_http);
 UPDATE public.novu_rust_fcm_attempts SET state=CASE WHEN p_decision='accepted' THEN 'accepted' ELSE 'rejected' END,operator_resolution=p_decision,updated_at=clock_timestamp()
 WHERE environment_id=p_environment AND job_id=p_job AND target_index=p_target;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_resolve_cancelled_push(text,uuid,bigint,integer,text,text,text) FROM PUBLIC;
CREATE OR REPLACE FUNCTION public.novu_rust_list_cancelled_push_resolutions(p_environment text,p_job uuid,p_revision bigint,p_after integer)
RETURNS TABLE(target_index integer,decision text,created_at text,revision text)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_revision bigint;
BEGIN
 PERFORM public.novu_rust_assert_alert_view(p_environment);
 IF p_job IS NULL OR p_revision IS NULL OR p_revision<0 OR p_after IS NULL OR p_after< -1 THEN RAISE EXCEPTION 'invalid cancelled push resolution request' USING ERRCODE='22023'; END IF;
 SELECT j.channel_policy_revision INTO v_revision FROM public.novu_queue_jobs j WHERE j.environment_id=p_environment AND j.id=p_job AND j.kind='experimental_fcm_push' AND j.status='cancelled'
 AND j.last_error IN ('push workflow identity or snapshot missing','push workflow step unavailable','push workflow step disabled','push workflow step changed','push preference recheck disabled','push preference context missing') FOR SHARE OF j;
 IF NOT FOUND THEN RAISE EXCEPTION 'push policy cancellation not found' USING ERRCODE='22023'; END IF;
 IF v_revision<>p_revision THEN RAISE EXCEPTION 'policy cancellation changed; review again' USING ERRCODE='40001'; END IF;
 RETURN QUERY SELECT DISTINCT ON (r.target_index) r.target_index,r.decision,r.created_at::text,r.revision::text FROM public.novu_rust_cancelled_push_resolutions r JOIN public.novu_rust_fcm_attempts a ON a.environment_id=r.environment_id AND a.job_id=r.job_id AND a.target_index=r.target_index AND a.operator_resolution=r.decision WHERE r.environment_id=p_environment AND r.job_id=p_job AND r.revision<=p_revision AND r.target_index>p_after ORDER BY r.target_index,r.revision DESC LIMIT 100;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_list_cancelled_push_resolutions(text,uuid,bigint,integer) FROM PUBLIC;
