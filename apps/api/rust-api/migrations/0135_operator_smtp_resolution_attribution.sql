-- Record attribution in the same transaction as a new JWT decision.
-- Earlier rows cannot be attributed by this mechanism and remain unknown.
CREATE TABLE public.novu_rust_smtp_resolution_jwt_attribution (
 environment_id text NOT NULL, actor_oid oid NOT NULL, database_request_id uuid NOT NULL,
 issuer text NOT NULL, subject text NOT NULL, client_id text NOT NULL, jwt_request_id uuid NOT NULL,
 PRIMARY KEY(environment_id,actor_oid,database_request_id),
 FOREIGN KEY(environment_id,actor_oid,database_request_id) REFERENCES public.novu_rust_smtp_resolutions(environment_id,actor_oid,request_id) ON DELETE CASCADE
);
REVOKE ALL ON public.novu_rust_smtp_resolution_jwt_attribution FROM PUBLIC;
CREATE FUNCTION public.novu_rust_smtp_resolution_capture_jwt() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_actor oid; v_request uuid;
BEGIN
 IF OLD.recorded IS NULL AND NEW.recorded IS TRUE THEN
  SELECT oid INTO v_actor FROM pg_catalog.pg_roles WHERE rolname=session_user AND rolcanlogin;
  IF NOT FOUND THEN RAISE EXCEPTION 'SMTP attribution actor unavailable' USING ERRCODE='42501'; END IF;
  v_request:=substr(encode(sha256(convert_to(jsonb_build_array('smtp-jwt-resolution-v1',NEW.issuer,NEW.subject,NEW.client_id,NEW.environment_id,NEW.request_id)::text,'UTF8')),'hex'),1,32)::uuid;
  PERFORM 1 FROM public.novu_rust_smtp_resolutions r WHERE r.environment_id=NEW.environment_id AND r.actor_oid=v_actor AND r.actor=session_user AND r.request_id=v_request AND r.job_id=NEW.job_id AND r.attempt_revision=NEW.attempt_revision AND r.failure_revision=NEW.failure_revision AND r.decision=NEW.decision AND r.reason=NEW.reason;
  IF NOT FOUND THEN RAISE EXCEPTION 'SMTP attribution resolution unavailable' USING ERRCODE='22023'; END IF;
  INSERT INTO public.novu_rust_smtp_resolution_jwt_attribution(environment_id,actor_oid,database_request_id,issuer,subject,client_id,jwt_request_id)
  VALUES(NEW.environment_id,v_actor,v_request,NEW.issuer,NEW.subject,NEW.client_id,NEW.request_id);
 END IF;
 RETURN NEW;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_smtp_resolution_capture_jwt() FROM PUBLIC;
CREATE TRIGGER novu_rust_smtp_resolution_capture_jwt AFTER UPDATE OF recorded ON public.novu_rust_operator_jwt_smtp_resolution_requests FOR EACH ROW EXECUTE FUNCTION public.novu_rust_smtp_resolution_capture_jwt();

CREATE OR REPLACE FUNCTION public.novu_rust_operator_jwt_smtp_resolution_history(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_job uuid,p_before_actor oid,p_before_request uuid)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_result jsonb; v_data jsonb;
BEGIN
 IF p_job IS NULL OR ((p_before_actor IS NULL)<>(p_before_request IS NULL)) THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 PERFORM 1 FROM public.novu_rust_operator_jwt_smtp_resolution_history_viewers v WHERE v.issuer=p_issuer AND v.subject=p_subject AND v.client_id=p_client AND v.environment_id=p_env FOR SHARE OF v;
 IF NOT FOUND OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 v_result:=public.novu_rust_smtp_resolution_history(p_env,p_job,p_before_actor,p_before_request);
 SELECT coalesce(jsonb_agg(jsonb_set(item,'{failureRevision}',to_jsonb(item->>'failureRevision')) || jsonb_build_object('operator',CASE WHEN a.database_request_id IS NULL THEN NULL ELSE jsonb_build_object('issuer',a.issuer,'subject',a.subject,'clientId',a.client_id,'requestId',a.jwt_request_id) END) ORDER BY ord),'[]'::jsonb)
 INTO v_data FROM jsonb_array_elements(v_result->'data') WITH ORDINALITY AS e(item,ord)
 LEFT JOIN public.novu_rust_smtp_resolution_jwt_attribution a ON a.environment_id=p_env AND a.actor_oid=(item->>'actorOid')::oid AND a.database_request_id=(item->>'requestId')::uuid;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN jsonb_set(v_result,'{data}',v_data);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_smtp_resolution_history(text,text,text,text,bigint,uuid,oid,uuid) FROM PUBLIC;
