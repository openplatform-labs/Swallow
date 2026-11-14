-- Acknowledgement tracks each transition into a guarded cancellation.
ALTER TABLE public.novu_queue_jobs ADD COLUMN IF NOT EXISTS channel_policy_revision bigint NOT NULL DEFAULT 0;
CREATE OR REPLACE FUNCTION public.novu_rust_channel_policy_revision() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
 IF NEW.kind IN ('experimental_sendgrid_email','experimental_resend_email','experimental_twilio_sms') AND NEW.status='cancelled'
 AND NEW.last_error IN ('email workflow identity missing','email workflow step unavailable','email workflow step disabled','email workflow step changed','sms workflow identity missing','sms workflow step unavailable','sms workflow step disabled','sms workflow step changed')
 AND (OLD.kind IS DISTINCT FROM NEW.kind OR OLD.status IS DISTINCT FROM NEW.status OR OLD.attempts IS DISTINCT FROM NEW.attempts OR OLD.last_error IS DISTINCT FROM NEW.last_error) THEN
  NEW.channel_policy_revision:=OLD.channel_policy_revision+1;
 END IF;
 RETURN NEW;
END; $$;
DROP TRIGGER IF EXISTS novu_rust_channel_policy_revision_trigger ON public.novu_queue_jobs;
CREATE TRIGGER novu_rust_channel_policy_revision_trigger BEFORE UPDATE OF kind,status,attempts,last_error ON public.novu_queue_jobs FOR EACH ROW EXECUTE FUNCTION public.novu_rust_channel_policy_revision();
CREATE TABLE IF NOT EXISTS public.novu_rust_channel_policy_acknowledgements (
 job_id uuid NOT NULL REFERENCES public.novu_queue_jobs(id) ON DELETE CASCADE,
 environment_id text NOT NULL, revision bigint NOT NULL, operator_name text NOT NULL, reason text NOT NULL,
 database_session_user text NOT NULL DEFAULT session_user, created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(job_id,revision)
);
REVOKE ALL ON public.novu_rust_channel_policy_acknowledgements FROM PUBLIC;
CREATE INDEX IF NOT EXISTS novu_rust_channel_policy_cancelled_idx ON public.novu_queue_jobs(environment_id,id)
 WHERE status='cancelled' AND kind IN ('experimental_sendgrid_email','experimental_resend_email','experimental_twilio_sms')
 AND last_error IN ('email workflow identity missing','email workflow step unavailable','email workflow step disabled','email workflow step changed','sms workflow identity missing','sms workflow step unavailable','sms workflow step disabled','sms workflow step changed');

CREATE OR REPLACE FUNCTION public.novu_rust_list_channel_policy_attention(p_environment text,p_channel text,p_after uuid)
RETURNS TABLE(job_id uuid,kind text,reason_code text,attempts integer,created_at text)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 PERFORM public.novu_rust_assert_alert_view(p_environment);
 IF p_channel IS NULL OR p_channel NOT IN ('email','sms','all') THEN RAISE EXCEPTION 'invalid channel' USING ERRCODE='22023'; END IF;
 RETURN QUERY SELECT j.id,j.kind,j.last_error,j.attempts,j.created_at::text FROM public.novu_queue_jobs j
 WHERE j.environment_id=p_environment AND j.status='cancelled' AND (p_after IS NULL OR j.id>p_after)
 AND ((p_channel IN ('email','all') AND j.kind IN ('experimental_sendgrid_email','experimental_resend_email') AND j.last_error IN ('email workflow identity missing','email workflow step unavailable','email workflow step disabled','email workflow step changed')) OR (p_channel IN ('sms','all') AND j.kind='experimental_twilio_sms' AND j.last_error IN ('sms workflow identity missing','sms workflow step unavailable','sms workflow step disabled','sms workflow step changed')))
 AND NOT EXISTS(SELECT 1 FROM public.novu_rust_channel_policy_acknowledgements a WHERE a.environment_id=j.environment_id AND a.job_id=j.id AND a.revision=j.channel_policy_revision)
 ORDER BY j.id LIMIT 100;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_list_channel_policy_attention(text,text,uuid) FROM PUBLIC;
CREATE OR REPLACE FUNCTION public.novu_rust_count_channel_policy_attention(p_environment text,p_channel text) RETURNS bigint
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE n bigint;
BEGIN
 PERFORM public.novu_rust_assert_alert_view(p_environment);
 IF p_channel IS NULL OR p_channel NOT IN ('email','sms','all') THEN RAISE EXCEPTION 'invalid channel' USING ERRCODE='22023'; END IF;
 SELECT count(*) INTO n FROM public.novu_queue_jobs j
 WHERE j.environment_id=p_environment AND j.status='cancelled'
 AND ((p_channel IN ('email','all') AND j.kind IN ('experimental_sendgrid_email','experimental_resend_email') AND j.last_error IN ('email workflow identity missing','email workflow step unavailable','email workflow step disabled','email workflow step changed')) OR (p_channel IN ('sms','all') AND j.kind='experimental_twilio_sms' AND j.last_error IN ('sms workflow identity missing','sms workflow step unavailable','sms workflow step disabled','sms workflow step changed')))
 AND NOT EXISTS(SELECT 1 FROM public.novu_rust_channel_policy_acknowledgements a WHERE a.environment_id=j.environment_id AND a.job_id=j.id AND a.revision=j.channel_policy_revision);
 RETURN n;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_count_channel_policy_attention(text,text) FROM PUBLIC;
CREATE OR REPLACE FUNCTION public.novu_rust_ack_channel_policy(p_environment text,p_job uuid,p_operator text,p_reason text) RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_revision bigint; v_count integer;
BEGIN
 IF p_environment IS NULL OR length(btrim(p_environment))=0 OR p_job IS NULL OR p_operator IS NULL OR length(btrim(p_operator))=0 OR octet_length(p_operator)>128 OR p_reason IS NULL OR length(btrim(p_reason))=0 OR octet_length(p_reason)>512 THEN RAISE EXCEPTION 'invalid policy acknowledgement' USING ERRCODE='22023'; END IF;
 IF NOT public.novu_rust_lock_operator_grant(p_environment,'queue_ack') THEN RAISE EXCEPTION 'queue acknowledgement permission denied' USING ERRCODE='42501'; END IF;
 SELECT j.channel_policy_revision INTO v_revision FROM public.novu_queue_jobs j WHERE j.environment_id=p_environment AND j.id=p_job AND j.status='cancelled'
 AND ((j.kind IN ('experimental_sendgrid_email','experimental_resend_email') AND j.last_error IN ('email workflow identity missing','email workflow step unavailable','email workflow step disabled','email workflow step changed')) OR (j.kind='experimental_twilio_sms' AND j.last_error IN ('sms workflow identity missing','sms workflow step unavailable','sms workflow step disabled','sms workflow step changed'))) FOR UPDATE OF j;
 IF NOT FOUND THEN RAISE EXCEPTION 'policy cancellation not found' USING ERRCODE='22023'; END IF;
 INSERT INTO public.novu_rust_channel_policy_acknowledgements(job_id,environment_id,revision,operator_name,reason) VALUES(p_job,p_environment,v_revision,p_operator,p_reason) ON CONFLICT(job_id,revision) DO NOTHING;
 GET DIAGNOSTICS v_count=ROW_COUNT; RETURN v_count=1;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_ack_channel_policy(text,uuid,text,text) FROM PUBLIC;
