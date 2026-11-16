-- Fence historical ambiguous cancellations until an authorized assessment.
CREATE OR REPLACE FUNCTION public.novu_rust_resend_cancel_reserve(p_env text,p_request uuid,p_job uuid,p_reason text) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_oid oid; v_old public.novu_rust_resend_cancel_requests%ROWTYPE; v_provider text; v_inserted boolean;
BEGIN
 IF p_request IS NULL OR p_job IS NULL OR p_reason IS NULL OR length(btrim(p_reason))=0 OR octet_length(p_reason)>500 OR p_reason ~ '[[:cntrl:]]' THEN RAISE EXCEPTION 'invalid cancellation request' USING ERRCODE='22023'; END IF;
 v_oid:=public.novu_rust_resend_cancel_access(p_env,true);
 IF current_setting('transaction_isolation')<>'read committed' THEN RAISE EXCEPTION 'cancellation requires read committed isolation' USING ERRCODE='55000'; END IF;
 -- Serializes competing operations using the existing queue job lock.
 PERFORM 1 FROM public.novu_queue_jobs WHERE environment_id=p_env AND id=p_job FOR UPDATE;
 SELECT * INTO v_old FROM public.novu_rust_resend_cancel_requests WHERE environment_id=p_env AND request_id=p_request;
 IF FOUND THEN
  IF (v_old.job_id,v_old.reason,v_old.database_principal,v_old.principal_oid) IS DISTINCT FROM (p_job,p_reason,session_user,v_oid) THEN RAISE EXCEPTION 'request conflict' USING ERRCODE='23505'; END IF;
  RETURN jsonb_build_object('requestId',p_request,'state',v_old.state,'fresh',false);
 END IF;
 -- Old 408/409 rows are ambiguous until an authorized operator assesses each.
 IF EXISTS(SELECT 1 FROM public.novu_rust_resend_cancel_requests r WHERE r.environment_id=p_env AND r.job_id=p_job AND r.state='rejected' AND r.http_status IN (408,409)) THEN
  RAISE EXCEPTION 'historical cancellation requires review' USING ERRCODE='55000';
 END IF;
 -- Coordinate with existing provider-ID writers, then re-read their committed evidence.
 SELECT a.provider_message_id INTO v_provider FROM public.novu_rust_resend_attempts a WHERE a.environment_id=p_env AND a.job_id=p_job;
 IF v_provider IS NOT NULL AND v_provider<>'' THEN
  PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('resend-provider/'||length(p_env)::text||'/'||p_env||'/'||v_provider,0));
 END IF;
 SELECT a.provider_message_id INTO v_provider FROM public.novu_queue_jobs j JOIN public.novu_rust_resend_attempts a ON a.environment_id=j.environment_id AND a.job_id=j.id
 WHERE j.environment_id=p_env AND j.id=p_job AND j.kind='experimental_resend_email' AND j.status='completed' AND j.resend_scheduled_at>clock_timestamp()
 AND (j.resend_delivery_result IS NULL OR j.resend_delivery_result='pending') AND a.state='accepted'
 AND a.provider_message_id=v_provider AND a.provider_message_id ~ '^[A-Za-z0-9_-]+$' AND octet_length(a.provider_message_id)<=1024
 AND NOT EXISTS(SELECT 1 FROM public.novu_rust_resend_attempts b WHERE b.environment_id=a.environment_id AND b.provider_message_id=a.provider_message_id AND b.job_id<>a.job_id)
 AND NOT EXISTS(SELECT 1 FROM public.novu_rust_delivery_reconciliations r WHERE r.environment_id=a.environment_id AND r.decision='retry' AND r.target_index IS NULL AND r.retry_provider_message_id=a.provider_message_id)
 AND NOT EXISTS(SELECT 1 FROM public.novu_rust_resend_delivery_events e WHERE e.environment_id=a.environment_id AND e.email_id=a.provider_message_id AND e.event_type IN ('email.sent','email.delivered','email.delivery_delayed','email.opened','email.clicked','email.bounced','email.failed','email.suppressed','email.complained'));
 IF NOT FOUND THEN RAISE EXCEPTION 'scheduled cancellation not eligible' USING ERRCODE='55000'; END IF;
 INSERT INTO public.novu_rust_resend_cancel_requests(environment_id,request_id,job_id,provider_id,database_principal,principal_oid,reason,state)
 VALUES(p_env,p_request,p_job,v_provider,session_user,v_oid,p_reason,'unknown') ON CONFLICT DO NOTHING RETURNING true INTO v_inserted;
 IF NOT coalesce(v_inserted,false) THEN RAISE EXCEPTION 'cancellation request or job conflict' USING ERRCODE='23505'; END IF;
 RETURN jsonb_build_object('requestId',p_request,'state','unknown','fresh',true,'providerId',v_provider);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_resend_cancel_reserve(text,uuid,uuid,text) FROM PUBLIC;
CREATE OR REPLACE FUNCTION public.novu_rust_resend_cancel_resolve(p_env text,p_resolution uuid,p_request uuid,p_decision text,p_reason text) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_oid oid; v_old public.novu_rust_resend_cancel_resolutions%ROWTYPE; v_state text; v_status integer; v_job uuid;
BEGIN
 IF p_resolution IS NULL OR p_request IS NULL OR p_decision IS NULL OR p_decision NOT IN ('confirmed','not_cancelled') OR p_reason IS NULL OR length(btrim(p_reason))=0 OR octet_length(p_reason)>500 OR p_reason ~ '[[:cntrl:]]' THEN RAISE EXCEPTION 'invalid cancellation resolution' USING ERRCODE='22023'; END IF;
 SELECT r.oid INTO v_oid FROM public.novu_rust_resend_cancel_operators g JOIN pg_catalog.pg_roles r ON r.oid=g.principal_oid AND r.rolname=g.database_principal AND r.rolcanlogin
 WHERE g.environment_id=p_env AND g.database_principal=session_user AND g.can_resolve FOR SHARE OF g;
 IF NOT FOUND THEN RAISE EXCEPTION 'cancellation resolution denied' USING ERRCODE='42501'; END IF;
 SELECT job_id INTO v_job FROM public.novu_rust_resend_cancel_requests WHERE environment_id=p_env AND request_id=p_request;
 IF NOT FOUND THEN RAISE EXCEPTION 'cancellation request missing' USING ERRCODE='55000'; END IF;
 -- Same lock order as reservation; serializes review and new requests.
 PERFORM 1 FROM public.novu_queue_jobs WHERE environment_id=p_env AND id=v_job FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'cancellation job missing' USING ERRCODE='55000'; END IF;
 SELECT state,http_status INTO v_state,v_status FROM public.novu_rust_resend_cancel_requests WHERE environment_id=p_env AND request_id=p_request FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'cancellation request missing' USING ERRCODE='55000'; END IF;
 SELECT * INTO v_old FROM public.novu_rust_resend_cancel_resolutions WHERE environment_id=p_env AND resolution_id=p_resolution;
 IF FOUND THEN
  IF (v_old.request_id,v_old.decision,v_old.reason,v_old.database_principal,v_old.principal_oid) IS DISTINCT FROM (p_request,p_decision,p_reason,session_user,v_oid) THEN RAISE EXCEPTION 'resolution conflict' USING ERRCODE='23505'; END IF;
  RETURN jsonb_build_object('resolutionId',p_resolution,'requestId',p_request,'decision',v_old.decision,'replayed',true);
 END IF;
 IF v_state<>'unknown' AND NOT (v_state='rejected' AND COALESCE(v_status IN (408,409),false)) THEN RAISE EXCEPTION 'cancellation is no longer unknown' USING ERRCODE='55000'; END IF;
 INSERT INTO public.novu_rust_resend_cancel_resolutions(environment_id,resolution_id,request_id,database_principal,principal_oid,decision,reason) VALUES(p_env,p_resolution,p_request,session_user,v_oid,p_decision,p_reason);
 UPDATE public.novu_rust_resend_cancel_requests SET state=p_decision,http_status=CASE WHEN v_state='rejected' THEN v_status ELSE NULL END,updated_at=clock_timestamp() WHERE environment_id=p_env AND request_id=p_request;
 RETURN jsonb_build_object('resolutionId',p_resolution,'requestId',p_request,'decision',p_decision,'replayed',false);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_resend_cancel_resolve(text,uuid,uuid,text,text) FROM PUBLIC;
