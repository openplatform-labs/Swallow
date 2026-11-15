-- Persistent, version-fenced acknowledgement of completed FCM partial failures.
CREATE TABLE public.novu_rust_fcm_partial_reviews (
 environment_id text NOT NULL,
 job_id uuid NOT NULL REFERENCES public.novu_queue_jobs(id) ON DELETE CASCADE,
 revision text NOT NULL CHECK (revision ~ '^[0-9a-f]{64}$'),
 operator_name text NOT NULL,
 reason text NOT NULL,
 database_principal text NOT NULL,
 reviewed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(job_id,revision)
);
REVOKE ALL ON public.novu_rust_fcm_partial_reviews FROM PUBLIC;
-- Internal fingerprint contains only a hash of outcome metadata, not tokens or provider IDs.
CREATE FUNCTION public.novu_rust_fcm_partial_snapshot(p_env text,p_job uuid)
RETURNS TABLE(accepted bigint,rejected bigint,revision text)
LANGUAGE sql STABLE SET search_path=pg_catalog AS $$
 SELECT count(*) FILTER(WHERE a.state='accepted'),count(*) FILTER(WHERE a.state='rejected'),
 encode(sha256(convert_to(j.attempts::text||'|'||coalesce(string_agg(a.target_index::text||':'||a.state||':'||coalesce(a.http_status::text,'-')||':'||extract(epoch FROM a.updated_at)::text,',' ORDER BY a.target_index),''),'UTF8')),'hex')
 FROM public.novu_queue_jobs j JOIN public.novu_rust_fcm_attempts a ON a.environment_id=j.environment_id AND a.job_id=j.id
 WHERE j.environment_id=p_env AND j.id=p_job AND j.kind='experimental_fcm_push' AND j.status='completed'
 GROUP BY j.attempts;
$$;
REVOKE ALL ON FUNCTION public.novu_rust_fcm_partial_snapshot(text,uuid) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_fcm_partial_unreviewed(p_env text,p_after uuid)
RETURNS TABLE(job_id uuid,accepted bigint,rejected bigint,revision text)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 PERFORM public.novu_rust_assert_alert_view(p_env);
 RETURN QUERY SELECT j.id,s.accepted,s.rejected,s.revision FROM public.novu_queue_jobs j
 CROSS JOIN LATERAL public.novu_rust_fcm_partial_snapshot(j.environment_id,j.id) s
 WHERE j.environment_id=p_env AND j.kind='experimental_fcm_push' AND j.status='completed' AND (p_after IS NULL OR j.id>p_after)
 AND s.accepted>0 AND s.rejected>0 AND NOT EXISTS (SELECT 1 FROM public.novu_rust_fcm_partial_reviews r WHERE r.environment_id=p_env AND r.job_id=j.id AND r.revision=s.revision)
 ORDER BY j.id LIMIT 100;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_fcm_partial_unreviewed(text,uuid) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_fcm_partial_ack(p_env text,p_job uuid,p_revision text,p_operator text,p_reason text)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_accepted bigint;v_rejected bigint;v_revision text;v_count integer;
BEGIN
 IF p_env IS NULL OR length(btrim(p_env))=0 OR p_job IS NULL OR p_revision IS NULL OR p_revision !~ '^[0-9a-f]{64}$' OR p_operator IS NULL OR length(btrim(p_operator))=0 OR octet_length(p_operator)>128 OR p_reason IS NULL OR length(btrim(p_reason))=0 OR octet_length(p_reason)>512 THEN RAISE EXCEPTION 'invalid partial review' USING ERRCODE='22023'; END IF;
 IF NOT public.novu_rust_lock_operator_grant(p_env,'queue_ack') THEN RAISE EXCEPTION 'review permission denied' USING ERRCODE='42501'; END IF;
 PERFORM 1 FROM public.novu_queue_jobs j WHERE j.environment_id=p_env AND j.id=p_job AND j.kind='experimental_fcm_push' AND j.status='completed' FOR UPDATE OF j;
 IF NOT FOUND THEN RAISE EXCEPTION 'completed FCM job not found' USING ERRCODE='22023'; END IF;
 PERFORM 1 FROM public.novu_rust_fcm_attempts a WHERE a.environment_id=p_env AND a.job_id=p_job ORDER BY a.target_index FOR UPDATE OF a;
 SELECT s.accepted,s.rejected,s.revision INTO v_accepted,v_rejected,v_revision FROM public.novu_rust_fcm_partial_snapshot(p_env,p_job) s;
 IF NOT FOUND OR v_accepted=0 OR v_rejected=0 THEN RAISE EXCEPTION 'partial failure not found' USING ERRCODE='22023'; END IF;
 IF v_revision<>p_revision THEN RAISE EXCEPTION 'partial failure changed; review again' USING ERRCODE='40001'; END IF;
 INSERT INTO public.novu_rust_fcm_partial_reviews(environment_id,job_id,revision,operator_name,reason,database_principal) VALUES(p_env,p_job,p_revision,p_operator,p_reason,session_user) ON CONFLICT(job_id,revision) DO NOTHING;
 GET DIAGNOSTICS v_count=ROW_COUNT;RETURN v_count=1;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_fcm_partial_ack(text,uuid,text,text,text) FROM PUBLIC;
