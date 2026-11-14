-- The request record and decision audit commit together; no provider request is sent here.
CREATE TABLE public.novu_rust_console_decision_requests (
 credential_id uuid NOT NULL, request_id uuid NOT NULL, environment_id text NOT NULL,
 channel text NOT NULL, target_id text NOT NULL, attempt_id uuid NOT NULL,
 decision text NOT NULL, reason text NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(credential_id,request_id)
);
REVOKE ALL ON public.novu_rust_console_decision_requests FROM PUBLIC;

CREATE FUNCTION public.novu_rust_console_resolve_alert_v2(p_hash text,p_request uuid,p_channel text,p_target text,p_attempt uuid,p_decision text,p_reason text)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_env text; v_id uuid; v_expires timestamptz; v_count integer; v_existing public.novu_rust_console_decision_requests%ROWTYPE;
BEGIN
 IF p_hash IS NULL OR p_hash !~ '^[0-9a-f]{64}$' OR p_decision IS NULL OR p_decision NOT IN ('retry','accepted') THEN RAISE EXCEPTION 'invalid credential' USING ERRCODE='42501'; END IF;
 IF p_request IS NULL OR p_channel IS NULL OR p_channel NOT IN ('slack','email') OR p_target IS NULL OR p_target !~ '^[A-Za-z0-9._-]{1,64}$'
 OR p_attempt IS NULL OR p_reason IS NULL OR length(btrim(p_reason))=0 OR octet_length(p_reason)>512 THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 SELECT c.environment_id,c.credential_id,c.expires_at INTO v_env,v_id,v_expires FROM public.novu_rust_console_credentials c
 WHERE c.token_hash=p_hash AND c.enabled AND c.expires_at>clock_timestamp() FOR SHARE OF c;
 IF NOT FOUND OR v_expires<=clock_timestamp() THEN RAISE EXCEPTION 'invalid credential' USING ERRCODE='42501'; END IF;
 PERFORM 1 FROM public.novu_rust_console_alert_decision_grants g WHERE g.credential_id=v_id AND g.decision=p_decision FOR SHARE OF g;
 IF NOT FOUND OR v_expires<=clock_timestamp() THEN RAISE EXCEPTION 'invalid credential' USING ERRCODE='42501'; END IF;
 INSERT INTO public.novu_rust_console_decision_requests(credential_id,request_id,environment_id,channel,target_id,attempt_id,decision,reason)
 VALUES(v_id,p_request,v_env,p_channel,p_target,p_attempt,p_decision,p_reason) ON CONFLICT (credential_id,request_id) DO NOTHING;
 GET DIAGNOSTICS v_count=ROW_COUNT;
 IF v_expires<=clock_timestamp() THEN RAISE EXCEPTION 'invalid credential' USING ERRCODE='42501'; END IF;
 IF v_count=0 THEN
  SELECT * INTO v_existing FROM public.novu_rust_console_decision_requests r WHERE r.credential_id=v_id AND r.request_id=p_request;
  IF NOT FOUND OR v_existing.environment_id IS DISTINCT FROM v_env OR v_existing.channel IS DISTINCT FROM p_channel
   OR v_existing.target_id IS DISTINCT FROM p_target OR v_existing.attempt_id IS DISTINCT FROM p_attempt
   OR v_existing.decision IS DISTINCT FROM p_decision OR v_existing.reason IS DISTINCT FROM p_reason THEN
   RAISE EXCEPTION 'request id conflict' USING ERRCODE='22023';
  END IF;
  RETURN true;
 END IF;
 PERFORM public.novu_rust_console_resolve_alert(p_hash,p_channel,p_target,p_attempt,p_decision,p_reason);
 RETURN false;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_console_resolve_alert_v2(text,uuid,text,text,uuid,text,text) FROM PUBLIC;

CREATE FUNCTION public.novu_rust_console_decision_request(p_hash text,p_request uuid)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_env text; v_id uuid; v_expires timestamptz; v_record public.novu_rust_console_decision_requests%ROWTYPE;
BEGIN
 IF p_hash IS NULL OR p_hash !~ '^[0-9a-f]{64}$' THEN RAISE EXCEPTION 'invalid credential' USING ERRCODE='42501'; END IF;
 IF p_request IS NULL THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 SELECT c.environment_id,c.credential_id,c.expires_at INTO v_env,v_id,v_expires FROM public.novu_rust_console_credentials c
 WHERE c.token_hash=p_hash AND c.enabled AND c.expires_at>clock_timestamp() FOR SHARE OF c;
 IF NOT FOUND OR v_expires<=clock_timestamp() THEN RAISE EXCEPTION 'invalid credential' USING ERRCODE='42501'; END IF;
 PERFORM 1 FROM public.novu_rust_console_alert_decision_grants g WHERE g.credential_id=v_id ORDER BY g.decision FOR SHARE OF g;
 IF NOT FOUND OR v_expires<=clock_timestamp() THEN RAISE EXCEPTION 'invalid credential' USING ERRCODE='42501'; END IF;
 SELECT * INTO v_record FROM public.novu_rust_console_decision_requests r WHERE r.credential_id=v_id AND r.request_id=p_request AND r.environment_id=v_env;
 IF NOT FOUND THEN RETURN jsonb_build_object('found',false); END IF;
 RETURN jsonb_build_object('found',true,'requestId',v_record.request_id,'channel',v_record.channel,'targetId',v_record.target_id,'attemptId',v_record.attempt_id,'decision',v_record.decision,'reason',v_record.reason,'createdAt',v_record.created_at);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_console_decision_request(text,uuid) FROM PUBLIC;
