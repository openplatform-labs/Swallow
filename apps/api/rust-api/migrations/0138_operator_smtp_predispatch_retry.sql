-- Preserve null attempt identity for explicitly approved pre-dispatch retry.
ALTER TABLE public.novu_rust_operator_jwt_smtp_resolution_requests ALTER COLUMN attempt_revision DROP NOT NULL;
CREATE OR REPLACE FUNCTION public.novu_rust_operator_jwt_smtp_resolve(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_request uuid,p_job uuid,p_attempt uuid,p_failure bigint,p_decision text,p_reason text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE n integer; r public.novu_rust_operator_jwt_smtp_resolution_requests%ROWTYPE; v_recorded boolean; v_db_request uuid;
BEGIN
 IF p_request IS NULL OR p_job IS NULL OR (p_attempt IS NULL AND p_decision IS DISTINCT FROM 'retry') OR p_failure IS NULL OR p_failure<0 OR p_reason IS NULL OR octet_length(btrim(p_reason)) NOT BETWEEN 1 AND 512 THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_smtp_resolution_access(p_issuer,p_subject,p_client,p_env,p_exp,p_decision);
 INSERT INTO public.novu_rust_operator_jwt_smtp_resolution_requests(issuer,subject,client_id,environment_id,request_id,job_id,attempt_revision,failure_revision,decision,reason) VALUES(p_issuer,p_subject,p_client,p_env,p_request,p_job,p_attempt,p_failure,p_decision,p_reason) ON CONFLICT DO NOTHING;
 GET DIAGNOSTICS n=ROW_COUNT;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 IF n=0 THEN
  SELECT * INTO r FROM public.novu_rust_operator_jwt_smtp_resolution_requests WHERE issuer=p_issuer AND subject=p_subject AND client_id=p_client AND environment_id=p_env AND request_id=p_request;
  IF NOT FOUND OR (r.job_id,r.attempt_revision,r.failure_revision,r.decision,r.reason) IS DISTINCT FROM (p_job,p_attempt,p_failure,p_decision,p_reason) OR r.recorded IS NULL THEN RAISE EXCEPTION 'request id conflict' USING ERRCODE='P4090'; END IF;
  RETURN jsonb_build_object('recorded',r.recorded,'replayed',true);
 END IF;
 -- Isolate request IDs between JWT identities sharing a database login.
 v_db_request:=substr(encode(sha256(convert_to(jsonb_build_array('smtp-jwt-resolution-v1',p_issuer,p_subject,p_client,p_env,p_request)::text,'UTF8')),'hex'),1,32)::uuid;
 BEGIN
  v_recorded:=public.novu_rust_resolve_smtp(p_env,v_db_request,p_job,p_attempt,p_failure,p_decision,p_reason);
 EXCEPTION WHEN SQLSTATE '22023' THEN RAISE EXCEPTION 'resolution conflict' USING ERRCODE='P4090'; END;
 IF NOT v_recorded THEN RAISE EXCEPTION 'resolution request collision' USING ERRCODE='P4090'; END IF;
 UPDATE public.novu_rust_operator_jwt_smtp_resolution_requests SET recorded=v_recorded WHERE issuer=p_issuer AND subject=p_subject AND client_id=p_client AND environment_id=p_env AND request_id=p_request;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN jsonb_build_object('recorded',v_recorded,'replayed',false);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_smtp_resolve(text,text,text,text,bigint,uuid,uuid,uuid,bigint,text,text) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.novu_rust_smtp_resolution_capture_jwt() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_actor oid; v_request uuid;
BEGIN
 IF OLD.recorded IS NULL AND NEW.recorded IS TRUE THEN
  SELECT oid INTO v_actor FROM pg_catalog.pg_roles WHERE rolname=session_user AND rolcanlogin;
  IF NOT FOUND THEN RAISE EXCEPTION 'SMTP attribution actor unavailable' USING ERRCODE='42501'; END IF;
  v_request:=substr(encode(sha256(convert_to(jsonb_build_array('smtp-jwt-resolution-v1',NEW.issuer,NEW.subject,NEW.client_id,NEW.environment_id,NEW.request_id)::text,'UTF8')),'hex'),1,32)::uuid;
  PERFORM 1 FROM public.novu_rust_smtp_resolutions r WHERE r.environment_id=NEW.environment_id AND r.actor_oid=v_actor AND r.actor=session_user AND r.request_id=v_request AND r.job_id=NEW.job_id AND r.attempt_revision IS NOT DISTINCT FROM NEW.attempt_revision AND r.failure_revision=NEW.failure_revision AND r.decision=NEW.decision AND r.reason=NEW.reason;
  IF NOT FOUND THEN RAISE EXCEPTION 'SMTP attribution resolution unavailable' USING ERRCODE='22023'; END IF;
  INSERT INTO public.novu_rust_smtp_resolution_jwt_attribution(environment_id,actor_oid,database_request_id,issuer,subject,client_id,jwt_request_id)
  VALUES(NEW.environment_id,v_actor,v_request,NEW.issuer,NEW.subject,NEW.client_id,NEW.request_id);
 END IF;
 RETURN NEW;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_smtp_resolution_capture_jwt() FROM PUBLIC;
