-- Remove unsafe acknowledgement and replace the listing with revision-bearing rows.
DROP FUNCTION IF EXISTS public.novu_rust_ack_channel_policy(text,uuid,text,text);
DROP FUNCTION IF EXISTS public.novu_rust_list_channel_policy_attention(text,text,uuid);
CREATE OR REPLACE FUNCTION public.novu_rust_list_channel_policy_attention(p_environment text,p_channel text,p_after uuid)
RETURNS TABLE(job_id uuid,kind text,reason_code text,attempts integer,created_at text,revision text)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 PERFORM public.novu_rust_assert_alert_view(p_environment);
 IF p_channel IS NULL OR p_channel NOT IN ('email','sms','all') THEN RAISE EXCEPTION 'invalid channel' USING ERRCODE='22023'; END IF;
 RETURN QUERY SELECT j.id,j.kind,j.last_error,j.attempts,j.created_at::text,j.channel_policy_revision::text FROM public.novu_queue_jobs j
 WHERE j.environment_id=p_environment AND j.status='cancelled' AND (p_after IS NULL OR j.id>p_after)
 AND ((p_channel IN ('email','all') AND j.kind IN ('experimental_sendgrid_email','experimental_resend_email') AND j.last_error IN ('email workflow identity missing','email workflow step unavailable','email workflow step disabled','email workflow step changed')) OR (p_channel IN ('sms','all') AND j.kind='experimental_twilio_sms' AND j.last_error IN ('sms workflow identity missing','sms workflow step unavailable','sms workflow step disabled','sms workflow step changed')))
 AND NOT EXISTS(SELECT 1 FROM public.novu_rust_channel_policy_acknowledgements a WHERE a.environment_id=j.environment_id AND a.job_id=j.id AND a.revision=j.channel_policy_revision)
 ORDER BY j.id LIMIT 100;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_list_channel_policy_attention(text,text,uuid) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.novu_rust_ack_channel_policy(p_environment text,p_job uuid,p_expected_revision bigint,p_operator text,p_reason text) RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_revision bigint; v_count integer;
BEGIN
 IF p_environment IS NULL OR length(btrim(p_environment))=0 OR p_job IS NULL OR p_expected_revision IS NULL OR p_expected_revision<0 OR p_operator IS NULL OR length(btrim(p_operator))=0 OR octet_length(p_operator)>128 OR p_reason IS NULL OR length(btrim(p_reason))=0 OR octet_length(p_reason)>512 THEN RAISE EXCEPTION 'invalid policy acknowledgement' USING ERRCODE='22023'; END IF;
 IF NOT public.novu_rust_lock_operator_grant(p_environment,'queue_ack') THEN RAISE EXCEPTION 'queue acknowledgement permission denied' USING ERRCODE='42501'; END IF;
 SELECT j.channel_policy_revision INTO v_revision FROM public.novu_queue_jobs j WHERE j.environment_id=p_environment AND j.id=p_job AND j.status='cancelled'
 AND ((j.kind IN ('experimental_sendgrid_email','experimental_resend_email') AND j.last_error IN ('email workflow identity missing','email workflow step unavailable','email workflow step disabled','email workflow step changed')) OR (j.kind='experimental_twilio_sms' AND j.last_error IN ('sms workflow identity missing','sms workflow step unavailable','sms workflow step disabled','sms workflow step changed'))) FOR UPDATE OF j;
 IF NOT FOUND THEN RAISE EXCEPTION 'policy cancellation not found' USING ERRCODE='22023'; END IF;
 IF v_revision<>p_expected_revision THEN RAISE EXCEPTION 'policy cancellation changed; review again' USING ERRCODE='40001'; END IF;
 INSERT INTO public.novu_rust_channel_policy_acknowledgements(job_id,environment_id,revision,operator_name,reason) VALUES(p_job,p_environment,v_revision,p_operator,p_reason) ON CONFLICT(job_id,revision) DO NOTHING;
 GET DIAGNOSTICS v_count=ROW_COUNT; RETURN v_count=1;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_ack_channel_policy(text,uuid,bigint,text,text) FROM PUBLIC;
