-- Durable idempotent decisions belong to the issuer/subject/client/environment, not a JWT instance.
CREATE TABLE public.novu_rust_operator_jwt_decision_requests (
 issuer text NOT NULL, subject text NOT NULL, client_id text NOT NULL, environment_id text NOT NULL,
 request_id uuid NOT NULL, channel text NOT NULL, target_id text NOT NULL, attempt_id uuid NOT NULL,
 decision text NOT NULL, reason text NOT NULL, created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(issuer,subject,client_id,environment_id,request_id)
);
CREATE TABLE public.novu_rust_operator_jwt_decision_audit (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, issuer text NOT NULL, subject text NOT NULL, client_id text NOT NULL,
 environment_id text NOT NULL, request_id uuid NOT NULL, channel text NOT NULL, target_id text NOT NULL,
 attempt_id uuid NOT NULL, decision text NOT NULL, reason text NOT NULL, database_session_user name NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
REVOKE ALL ON public.novu_rust_operator_jwt_decision_requests,public.novu_rust_operator_jwt_decision_audit FROM PUBLIC;

CREATE FUNCTION public.novu_rust_operator_jwt_resolve_alert(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_request uuid,p_channel text,p_target text,p_attempt uuid,p_decision text,p_reason text)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_identity jsonb; v_count integer; v_existing public.novu_rust_operator_jwt_decision_requests%ROWTYPE; v_operator text;
BEGIN
 IF p_request IS NULL OR p_channel IS NULL OR p_channel NOT IN ('slack','email') OR p_target IS NULL OR p_target !~ '^[A-Za-z0-9._-]{1,64}$'
 OR p_attempt IS NULL OR p_decision IS NULL OR p_decision NOT IN ('retry','accepted') OR p_reason IS NULL OR length(btrim(p_reason))=0 OR octet_length(p_reason)>512 THEN
  RAISE EXCEPTION 'invalid request' USING ERRCODE='22023';
 END IF;
 v_identity:=public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 IF (v_identity->'provisionedPermissions'->>CASE p_decision WHEN 'retry' THEN 'alertRetry' ELSE 'alertAccepted' END)::boolean IS NOT TRUE THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 IF NOT public.novu_rust_lock_operator_grant(p_env,'alert_' || p_decision) OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 INSERT INTO public.novu_rust_operator_jwt_decision_requests(issuer,subject,client_id,environment_id,request_id,channel,target_id,attempt_id,decision,reason)
 VALUES(p_issuer,p_subject,p_client,p_env,p_request,p_channel,p_target,p_attempt,p_decision,p_reason) ON CONFLICT DO NOTHING;
 GET DIAGNOSTICS v_count=ROW_COUNT;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 IF v_count=0 THEN
  SELECT * INTO v_existing FROM public.novu_rust_operator_jwt_decision_requests r WHERE r.issuer=p_issuer AND r.subject=p_subject AND r.client_id=p_client AND r.environment_id=p_env AND r.request_id=p_request;
  IF NOT FOUND OR v_existing.channel IS DISTINCT FROM p_channel OR v_existing.target_id IS DISTINCT FROM p_target OR v_existing.attempt_id IS DISTINCT FROM p_attempt OR v_existing.decision IS DISTINCT FROM p_decision OR v_existing.reason IS DISTINCT FROM p_reason THEN
   RAISE EXCEPTION 'request id conflict' USING ERRCODE='22023';
  END IF;
  RETURN true;
 END IF;
 PERFORM 1 FROM public.novu_rust_alert_delivery_slots s WHERE s.environment_id=p_env AND s.channel=p_channel AND s.target_id=p_target FOR UPDATE OF s;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 v_operator:='jwt:' || encode(sha256(convert_to(jsonb_build_array(p_issuer,p_subject,p_client,p_env)::text,'UTF8')),'hex');
 PERFORM public.novu_rust_resolve_alert_v2(p_env,p_channel,p_target,p_attempt,p_decision,v_operator,p_reason);
 INSERT INTO public.novu_rust_operator_jwt_decision_audit(issuer,subject,client_id,environment_id,request_id,channel,target_id,attempt_id,decision,reason,database_session_user)
 VALUES(p_issuer,p_subject,p_client,p_env,p_request,p_channel,p_target,p_attempt,p_decision,p_reason,session_user);
 RETURN false;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_resolve_alert(text,text,text,text,bigint,uuid,text,text,uuid,text,text) FROM PUBLIC;

CREATE FUNCTION public.novu_rust_operator_jwt_decision_request(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_request uuid)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_identity jsonb; v_record public.novu_rust_operator_jwt_decision_requests%ROWTYPE;
BEGIN
 IF p_request IS NULL THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 v_identity:=public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 IF (v_identity->'provisionedPermissions'->>'alertRetry')::boolean IS NOT TRUE AND (v_identity->'provisionedPermissions'->>'alertAccepted')::boolean IS NOT TRUE THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 SELECT * INTO v_record FROM public.novu_rust_operator_jwt_decision_requests r WHERE r.issuer=p_issuer AND r.subject=p_subject AND r.client_id=p_client AND r.environment_id=p_env AND r.request_id=p_request;
 IF NOT FOUND THEN RETURN jsonb_build_object('found',false); END IF;
 RETURN jsonb_build_object('found',true,'requestId',v_record.request_id,'channel',v_record.channel,'targetId',v_record.target_id,'attemptId',v_record.attempt_id,'decision',v_record.decision,'reason',v_record.reason,'createdAt',v_record.created_at);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_decision_request(text,text,text,text,bigint,uuid) FROM PUBLIC;
