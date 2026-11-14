-- Operator assertions are distinguished from actual provider responses.
ALTER TABLE public.novu_rust_fcm_attempts ADD COLUMN IF NOT EXISTS operator_resolution text;
ALTER TABLE public.novu_rust_fcm_attempts DROP CONSTRAINT IF EXISTS novu_rust_fcm_operator_resolution_check;
ALTER TABLE public.novu_rust_fcm_attempts ADD CONSTRAINT novu_rust_fcm_operator_resolution_check CHECK (
 operator_resolution IS NULL OR (operator_resolution='accepted' AND state='accepted') OR (operator_resolution='not_accepted' AND state='rejected'));
CREATE TABLE IF NOT EXISTS public.novu_rust_cancelled_push_resolutions (
 id uuid PRIMARY KEY,environment_id text NOT NULL,job_id uuid NOT NULL REFERENCES public.novu_queue_jobs(id),
 revision bigint NOT NULL,target_index integer NOT NULL CHECK(target_index>=0),
 decision text NOT NULL CHECK(decision IN ('accepted','not_accepted')),
 operator_name text NOT NULL,reason text NOT NULL,database_session_user text NOT NULL DEFAULT session_user,
 previous_http_status integer,created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 UNIQUE(job_id,revision,target_index)
);
REVOKE ALL ON public.novu_rust_cancelled_push_resolutions FROM PUBLIC;
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
 AND j.last_error IN ('push workflow identity or snapshot missing','push workflow step unavailable','push workflow step disabled','push workflow step changed') FOR UPDATE OF j;
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
 AND j.last_error IN ('push workflow identity or snapshot missing','push workflow step unavailable','push workflow step disabled','push workflow step changed') FOR SHARE OF j;
 IF NOT FOUND THEN RAISE EXCEPTION 'push policy cancellation not found' USING ERRCODE='22023'; END IF;
 IF v_revision<>p_revision THEN RAISE EXCEPTION 'policy cancellation changed; review again' USING ERRCODE='40001'; END IF;
 RETURN QUERY SELECT DISTINCT ON (r.target_index) r.target_index,r.decision,r.created_at::text,r.revision::text FROM public.novu_rust_cancelled_push_resolutions r JOIN public.novu_rust_fcm_attempts a ON a.environment_id=r.environment_id AND a.job_id=r.job_id AND a.target_index=r.target_index AND a.operator_resolution=r.decision WHERE r.environment_id=p_environment AND r.job_id=p_job AND r.revision<=p_revision AND r.target_index>p_after ORDER BY r.target_index,r.revision DESC LIMIT 100;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_list_cancelled_push_resolutions(text,uuid,bigint,integer) FROM PUBLIC;
