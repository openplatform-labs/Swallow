-- Separate opt-in retention for tracking configuration (including user-provided text).
ALTER TABLE public.novu_queue_jobs ADD COLUMN tracking_completed_at timestamptz;
ALTER TABLE public.novu_queue_jobs ADD COLUMN tracking_scrubbed_at timestamptz;
CREATE FUNCTION public.novu_rust_remove_sendgrid_tracking(p_kind text,p_payload jsonb) RETURNS jsonb LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog AS $$
DECLARE result jsonb:=p_payload; step text;
BEGIN
 IF p_kind IN ('experimental_static_in_app','experimental_static_in_app_attachments_v1') THEN
  result:=result #- '{emailProviderOverrides,sendgrid,trackingSettings}' #- '{emailProviderOverrides,sendgrid,_passthrough,body,trackingSettings}';
  IF jsonb_typeof(result->'emailStepOverrides')='object' THEN
   FOR step IN SELECT jsonb_object_keys(result->'emailStepOverrides') LOOP
    result:=result #- ARRAY['emailStepOverrides',step,'providers','sendgrid','trackingSettings'] #- ARRAY['emailStepOverrides',step,'providers','sendgrid','_passthrough','body','trackingSettings'];
   END LOOP;
  END IF;
 ELSIF p_kind='experimental_sendgrid_email' AND p_payload ? 'workflow_context' THEN
  result:=(result-'tracking_settings') #- '{workflow_context,overrides,trackingSettings}' #- '{workflow_context,overrides,_passthrough,body,trackingSettings}';
 END IF;
 RETURN result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_remove_sendgrid_tracking(text,jsonb) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_has_sendgrid_tracking(p_kind text,p_payload jsonb) RETURNS boolean LANGUAGE sql IMMUTABLE SET search_path=pg_catalog AS $$
 SELECT p_payload IS DISTINCT FROM public.novu_rust_remove_sendgrid_tracking(p_kind,p_payload)
$$;
REVOKE ALL ON FUNCTION public.novu_rust_has_sendgrid_tracking(text,jsonb) FROM PUBLIC;
CREATE OR REPLACE FUNCTION public.novu_rust_attachment_retention_guard() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF TG_OP='UPDATE' AND OLD.attachment_scrubbed_at IS NOT NULL AND (NEW.status<>'completed' OR NEW.kind IS DISTINCT FROM OLD.kind OR NEW.attachment_completed_at IS DISTINCT FROM OLD.attachment_completed_at OR NEW.attachment_scrubbed_at IS DISTINCT FROM OLD.attachment_scrubbed_at OR (NEW.payload IS DISTINCT FROM OLD.payload AND NOT (OLD.alternative_scrubbed_at IS NULL AND NEW.alternative_scrubbed_at IS NOT NULL AND NEW.payload=public.novu_rust_remove_email_alternatives(OLD.kind,OLD.payload)) AND NOT (OLD.alternative_scrubbed_at IS NOT NULL AND NEW.alternative_scrubbed_at IS NOT DISTINCT FROM OLD.alternative_scrubbed_at AND NEW.payload=public.novu_rust_remove_alternative_layers(OLD.kind,OLD.payload)) AND NOT (OLD.tracking_scrubbed_at IS NULL AND NEW.tracking_scrubbed_at IS NOT NULL AND NEW.payload=public.novu_rust_remove_sendgrid_tracking(OLD.kind,OLD.payload)))) THEN
  RAISE EXCEPTION 'scrubbed attachment job cannot be replayed' USING ERRCODE='55000';
 END IF;
 IF NEW.status<>'completed' THEN NEW.attachment_completed_at:=NULL;
 ELSIF (TG_OP='INSERT' OR OLD.status<>'completed' OR NEW.attachment_completed_at IS NULL) AND public.novu_rust_has_workflow_attachment(NEW.kind,NEW.payload) THEN NEW.attachment_completed_at:=clock_timestamp(); END IF;
 RETURN NEW;
END; $$;
CREATE OR REPLACE FUNCTION public.novu_rust_alternative_retention_guard() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF TG_OP='UPDATE' AND OLD.alternative_scrubbed_at IS NOT NULL AND (NEW.status<>'completed' OR NEW.kind IS DISTINCT FROM OLD.kind OR NEW.alternative_completed_at IS DISTINCT FROM OLD.alternative_completed_at OR NEW.alternative_scrubbed_at IS DISTINCT FROM OLD.alternative_scrubbed_at OR (NEW.payload IS DISTINCT FROM OLD.payload AND NOT (OLD.attachment_scrubbed_at IS NULL AND NEW.attachment_scrubbed_at IS NOT NULL AND NEW.payload=OLD.payload-CASE WHEN OLD.kind IN ('experimental_static_in_app','experimental_static_in_app_attachments_v1') THEN 'emailAttachments' ELSE 'attachments' END) AND NOT (NEW.payload=public.novu_rust_remove_alternative_layers(OLD.kind,OLD.payload)) AND NOT (OLD.tracking_scrubbed_at IS NULL AND NEW.tracking_scrubbed_at IS NOT NULL AND NEW.payload=public.novu_rust_remove_sendgrid_tracking(OLD.kind,OLD.payload)))) THEN
  RAISE EXCEPTION 'scrubbed alternative job cannot be replayed' USING ERRCODE='55000';
 END IF;
 IF NEW.status<>'completed' THEN NEW.alternative_completed_at:=NULL;
 ELSIF (TG_OP='INSERT' OR OLD.status<>'completed' OR NEW.alternative_completed_at IS NULL) AND public.novu_rust_has_email_alternatives(NEW.kind,NEW.payload) THEN NEW.alternative_completed_at:=clock_timestamp(); END IF;
 RETURN NEW;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_alternative_retention_guard() FROM PUBLIC;
CREATE FUNCTION public.novu_rust_tracking_retention_guard() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF TG_OP='UPDATE' AND OLD.tracking_scrubbed_at IS NOT NULL AND (NEW.status<>'completed' OR NEW.kind IS DISTINCT FROM OLD.kind OR NEW.tracking_completed_at IS DISTINCT FROM OLD.tracking_completed_at OR NEW.tracking_scrubbed_at IS DISTINCT FROM OLD.tracking_scrubbed_at OR (NEW.payload IS DISTINCT FROM OLD.payload AND NOT (OLD.attachment_scrubbed_at IS NULL AND NEW.attachment_scrubbed_at IS NOT NULL AND NEW.payload=OLD.payload-CASE WHEN OLD.kind IN ('experimental_static_in_app','experimental_static_in_app_attachments_v1') THEN 'emailAttachments' ELSE 'attachments' END) AND NOT (OLD.alternative_scrubbed_at IS NULL AND NEW.alternative_scrubbed_at IS NOT NULL AND NEW.payload=public.novu_rust_remove_email_alternatives(OLD.kind,OLD.payload)) AND NOT (OLD.alternative_scrubbed_at IS NOT NULL AND NEW.alternative_scrubbed_at IS NOT DISTINCT FROM OLD.alternative_scrubbed_at AND NEW.payload=public.novu_rust_remove_alternative_layers(OLD.kind,OLD.payload)))) THEN
  RAISE EXCEPTION 'scrubbed tracking job cannot be replayed' USING ERRCODE='55000';
 END IF;
 IF NEW.status<>'completed' THEN NEW.tracking_completed_at:=NULL;
 ELSIF (TG_OP='INSERT' OR OLD.status<>'completed' OR NEW.tracking_completed_at IS NULL) AND public.novu_rust_has_sendgrid_tracking(NEW.kind,NEW.payload) THEN NEW.tracking_completed_at:=clock_timestamp(); END IF;
 RETURN NEW;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_tracking_retention_guard() FROM PUBLIC;
CREATE TRIGGER novu_rust_tracking_retention_guard BEFORE INSERT OR UPDATE OF kind,status,payload,tracking_completed_at,tracking_scrubbed_at,attachment_scrubbed_at,alternative_scrubbed_at ON public.novu_queue_jobs FOR EACH ROW EXECUTE FUNCTION public.novu_rust_tracking_retention_guard();
UPDATE public.novu_queue_jobs SET tracking_completed_at=clock_timestamp() WHERE status='completed' AND public.novu_rust_has_sendgrid_tracking(kind,payload);
CREATE INDEX novu_rust_tracking_retention_idx ON public.novu_queue_jobs(environment_id,tracking_completed_at,id) WHERE status='completed' AND tracking_completed_at IS NOT NULL AND tracking_scrubbed_at IS NULL;
CREATE TABLE public.novu_rust_tracking_retention_audit(id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,environment_id text NOT NULL,job_id uuid NOT NULL,kind text NOT NULL,actor name NOT NULL,actor_oid oid NOT NULL,reason text NOT NULL,created_at timestamptz NOT NULL DEFAULT clock_timestamp());
CREATE INDEX novu_rust_tracking_retention_audit_idx ON public.novu_rust_tracking_retention_audit(environment_id,id);
REVOKE ALL ON public.novu_rust_tracking_retention_audit FROM PUBLIC;
CREATE FUNCTION public.novu_rust_list_tracking_retention(p_env text,p_age integer,p_limit integer) RETURNS TABLE(job_id uuid,kind text,completed_at text) LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF p_age IS NULL OR p_age NOT BETWEEN 0 AND 31536000 OR p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 100 THEN RAISE EXCEPTION 'invalid retention request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_lock_attachment_retention(p_env,false);
 RETURN QUERY SELECT j.id,j.kind,j.tracking_completed_at::text FROM public.novu_queue_jobs j WHERE j.environment_id=p_env AND j.status='completed' AND j.tracking_scrubbed_at IS NULL AND j.tracking_completed_at<=clock_timestamp()-p_age*interval '1 second' AND public.novu_rust_has_sendgrid_tracking(j.kind,j.payload) ORDER BY j.tracking_completed_at,j.id LIMIT p_limit;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_list_tracking_retention(text,integer,integer) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_scrub_tracking_retention(p_env text,p_age integer,p_limit integer,p_reason text) RETURNS TABLE(job_id uuid,kind text) LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_actor oid; v_job record;
BEGIN
 IF p_age IS NULL OR p_age NOT BETWEEN 0 AND 31536000 OR p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 100 OR p_reason IS NULL OR length(btrim(p_reason))=0 OR octet_length(p_reason)>512 THEN RAISE EXCEPTION 'invalid retention request' USING ERRCODE='22023'; END IF;
 v_actor:=public.novu_rust_lock_attachment_retention(p_env,true);
 FOR v_job IN SELECT j.id,j.kind FROM public.novu_queue_jobs j WHERE j.environment_id=p_env AND j.status='completed' AND j.tracking_scrubbed_at IS NULL AND j.tracking_completed_at<=clock_timestamp()-p_age*interval '1 second' AND public.novu_rust_has_sendgrid_tracking(j.kind,j.payload) ORDER BY j.tracking_completed_at,j.id FOR UPDATE OF j SKIP LOCKED LIMIT p_limit LOOP
  UPDATE public.novu_queue_jobs j SET payload=public.novu_rust_remove_sendgrid_tracking(j.kind,j.payload),tracking_scrubbed_at=clock_timestamp() WHERE j.id=v_job.id;
  INSERT INTO public.novu_rust_tracking_retention_audit(environment_id,job_id,kind,actor,actor_oid,reason) VALUES(p_env,v_job.id,v_job.kind,session_user,v_actor,p_reason);
  job_id:=v_job.id;kind:=v_job.kind;RETURN NEXT;
 END LOOP;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_scrub_tracking_retention(text,integer,integer,text) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_tracking_retention_audit(p_env text,p_after bigint) RETURNS TABLE(id text,job_id uuid,kind text,actor text,reason text,created_at text) LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF p_after IS NULL OR p_after<0 THEN RAISE EXCEPTION 'invalid cursor' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_lock_attachment_retention(p_env,false);
 RETURN QUERY SELECT a.id::text,a.job_id,a.kind,a.actor::text,a.reason,a.created_at::text FROM public.novu_rust_tracking_retention_audit a WHERE a.environment_id=p_env AND a.id>p_after ORDER BY a.id LIMIT 100;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_tracking_retention_audit(text,bigint) FROM PUBLIC;
