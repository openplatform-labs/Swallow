-- Remove alternatives from all accepted provider and step layers in parent jobs.
CREATE FUNCTION public.novu_rust_remove_alternative_layers(p_kind text,p_payload jsonb) RETURNS jsonb LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog AS $$
DECLARE result jsonb:=p_payload; provider text; step text;
BEGIN
 IF p_kind NOT IN ('experimental_static_in_app','experimental_static_in_app_attachments_v1') THEN RETURN result; END IF;
 result:=result #- '{emailFlatProviderOverride,alternatives}';
 IF jsonb_typeof(result->'emailProviderOverrides')='object' THEN
  FOR provider IN SELECT jsonb_object_keys(result->'emailProviderOverrides') LOOP
   result:=result #- ARRAY['emailProviderOverrides',provider,'alternatives'];
  END LOOP;
 END IF;
 IF jsonb_typeof(result->'emailStepOverrides')='object' THEN
  FOR step IN SELECT jsonb_object_keys(result->'emailStepOverrides') LOOP
   IF jsonb_typeof(result#>ARRAY['emailStepOverrides',step,'providers'])='object' THEN
    FOR provider IN SELECT jsonb_object_keys(result#>ARRAY['emailStepOverrides',step,'providers']) LOOP
     result:=result #- ARRAY['emailStepOverrides',step,'providers',provider,'alternatives'];
    END LOOP;
   END IF;
  END LOOP;
 END IF;
 RETURN result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_remove_alternative_layers(text,jsonb) FROM PUBLIC;
CREATE OR REPLACE FUNCTION public.novu_rust_remove_email_alternatives(p_kind text,p_payload jsonb) RETURNS jsonb
LANGUAGE sql IMMUTABLE SET search_path=pg_catalog AS $$
 SELECT CASE WHEN p_kind IN ('experimental_static_in_app','experimental_static_in_app_attachments_v1') THEN public.novu_rust_remove_alternative_layers(p_kind,p_payload #- '{emailOverride,alternatives}')
 WHEN p_kind IN ('experimental_resend_email','experimental_sendgrid_email','experimental_smtp_email') THEN (p_payload-'alternatives') #- '{workflow_context,overrides,alternatives}' ELSE p_payload END
$$;
REVOKE ALL ON FUNCTION public.novu_rust_remove_email_alternatives(text,jsonb) FROM PUBLIC;
CREATE OR REPLACE FUNCTION public.novu_rust_has_email_alternatives(p_kind text,p_payload jsonb) RETURNS boolean
LANGUAGE sql IMMUTABLE SET search_path=pg_catalog AS $$
 SELECT CASE WHEN p_kind IN ('experimental_static_in_app','experimental_static_in_app_attachments_v1') THEN
   (CASE WHEN jsonb_typeof(p_payload#>'{emailOverride,alternatives}')='array' THEN jsonb_array_length(p_payload#>'{emailOverride,alternatives}')>0 ELSE false END) OR p_payload IS DISTINCT FROM public.novu_rust_remove_alternative_layers(p_kind,p_payload)
 WHEN p_kind IN ('experimental_resend_email','experimental_sendgrid_email','experimental_smtp_email') AND p_payload ? 'workflow_context' THEN
   CASE WHEN jsonb_typeof(p_payload->'alternatives')='array' THEN jsonb_array_length(p_payload->'alternatives')>0 ELSE false END OR
   CASE WHEN jsonb_typeof(p_payload#>'{workflow_context,overrides,alternatives}')='array' THEN jsonb_array_length(p_payload#>'{workflow_context,overrides,alternatives}')>0 ELSE false END
 ELSE false END
$$;
REVOKE ALL ON FUNCTION public.novu_rust_has_email_alternatives(text,jsonb) FROM PUBLIC;
CREATE OR REPLACE FUNCTION public.novu_rust_attachment_retention_guard() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF TG_OP='UPDATE' AND OLD.attachment_scrubbed_at IS NOT NULL AND (NEW.status<>'completed' OR NEW.kind IS DISTINCT FROM OLD.kind OR NEW.attachment_completed_at IS DISTINCT FROM OLD.attachment_completed_at OR NEW.attachment_scrubbed_at IS DISTINCT FROM OLD.attachment_scrubbed_at OR (NEW.payload IS DISTINCT FROM OLD.payload AND NOT (OLD.alternative_scrubbed_at IS NULL AND NEW.alternative_scrubbed_at IS NOT NULL AND NEW.payload=public.novu_rust_remove_email_alternatives(OLD.kind,OLD.payload)) AND NOT (OLD.alternative_scrubbed_at IS NOT NULL AND NEW.alternative_scrubbed_at IS NOT DISTINCT FROM OLD.alternative_scrubbed_at AND NEW.payload=public.novu_rust_remove_alternative_layers(OLD.kind,OLD.payload)))) THEN
  RAISE EXCEPTION 'scrubbed attachment job cannot be replayed' USING ERRCODE='55000';
 END IF;
 IF NEW.status<>'completed' THEN NEW.attachment_completed_at:=NULL;
 ELSIF (TG_OP='INSERT' OR OLD.status<>'completed' OR NEW.attachment_completed_at IS NULL) AND public.novu_rust_has_workflow_attachment(NEW.kind,NEW.payload) THEN NEW.attachment_completed_at:=clock_timestamp(); END IF;
 RETURN NEW;
END; $$;
CREATE OR REPLACE FUNCTION public.novu_rust_alternative_retention_guard() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF TG_OP='UPDATE' AND OLD.alternative_scrubbed_at IS NOT NULL AND (NEW.status<>'completed' OR NEW.kind IS DISTINCT FROM OLD.kind OR NEW.alternative_completed_at IS DISTINCT FROM OLD.alternative_completed_at OR NEW.alternative_scrubbed_at IS DISTINCT FROM OLD.alternative_scrubbed_at OR (NEW.payload IS DISTINCT FROM OLD.payload AND NOT (OLD.attachment_scrubbed_at IS NULL AND NEW.attachment_scrubbed_at IS NOT NULL AND NEW.payload=OLD.payload-CASE WHEN OLD.kind IN ('experimental_static_in_app','experimental_static_in_app_attachments_v1') THEN 'emailAttachments' ELSE 'attachments' END) AND NOT (NEW.payload=public.novu_rust_remove_alternative_layers(OLD.kind,OLD.payload)))) THEN
  RAISE EXCEPTION 'scrubbed alternative job cannot be replayed' USING ERRCODE='55000';
 END IF;
 IF NEW.status<>'completed' THEN NEW.alternative_completed_at:=NULL;
 ELSIF (TG_OP='INSERT' OR OLD.status<>'completed' OR NEW.alternative_completed_at IS NULL) AND public.novu_rust_has_email_alternatives(NEW.kind,NEW.payload) THEN NEW.alternative_completed_at:=clock_timestamp(); END IF;
 RETURN NEW;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_alternative_retention_guard() FROM PUBLIC;
UPDATE public.novu_queue_jobs SET alternative_completed_at=clock_timestamp() WHERE status='completed' AND alternative_completed_at IS NULL AND public.novu_rust_has_email_alternatives(kind,payload);
CREATE OR REPLACE FUNCTION public.novu_rust_list_alternative_retention(p_env text,p_age integer,p_limit integer) RETURNS TABLE(job_id uuid,kind text,completed_at text) LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF p_age IS NULL OR p_age NOT BETWEEN 0 AND 31536000 OR p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 100 THEN RAISE EXCEPTION 'invalid retention request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_lock_attachment_retention(p_env,false);
 RETURN QUERY SELECT j.id,j.kind,j.alternative_completed_at::text FROM public.novu_queue_jobs j WHERE j.environment_id=p_env AND j.status='completed' AND (j.alternative_scrubbed_at IS NULL OR j.payload IS DISTINCT FROM public.novu_rust_remove_alternative_layers(j.kind,j.payload)) AND j.alternative_completed_at<=clock_timestamp()-p_age*interval '1 second' AND public.novu_rust_has_email_alternatives(j.kind,j.payload) ORDER BY j.alternative_completed_at,j.id LIMIT p_limit;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_list_alternative_retention(text,integer,integer) FROM PUBLIC;
CREATE OR REPLACE FUNCTION public.novu_rust_scrub_alternative_retention(p_env text,p_age integer,p_limit integer,p_reason text) RETURNS TABLE(job_id uuid,kind text) LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_actor oid; v_job record;
BEGIN
 IF p_age IS NULL OR p_age NOT BETWEEN 0 AND 31536000 OR p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 100 OR p_reason IS NULL OR length(btrim(p_reason))=0 OR octet_length(p_reason)>512 THEN RAISE EXCEPTION 'invalid retention request' USING ERRCODE='22023'; END IF;
 v_actor:=public.novu_rust_lock_attachment_retention(p_env,true);
 FOR v_job IN SELECT j.id,j.kind FROM public.novu_queue_jobs j WHERE j.environment_id=p_env AND j.status='completed' AND (j.alternative_scrubbed_at IS NULL OR j.payload IS DISTINCT FROM public.novu_rust_remove_alternative_layers(j.kind,j.payload)) AND j.alternative_completed_at<=clock_timestamp()-p_age*interval '1 second' AND public.novu_rust_has_email_alternatives(j.kind,j.payload) ORDER BY j.alternative_completed_at,j.id FOR UPDATE OF j SKIP LOCKED LIMIT p_limit LOOP
  UPDATE public.novu_queue_jobs j SET payload=public.novu_rust_remove_email_alternatives(j.kind,j.payload),alternative_scrubbed_at=coalesce(j.alternative_scrubbed_at,clock_timestamp()) WHERE j.id=v_job.id;
  INSERT INTO public.novu_rust_alternative_retention_audit(environment_id,job_id,kind,actor,actor_oid,reason) VALUES(p_env,v_job.id,v_job.kind,session_user,v_actor,p_reason);
  job_id:=v_job.id;kind:=v_job.kind;RETURN NEXT;
 END LOOP;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_scrub_alternative_retention(text,integer,integer,text) FROM PUBLIC;
