-- Explicit, environment-scoped cleanup of completed workflow attachment copies.
ALTER TABLE public.novu_queue_jobs ADD COLUMN attachment_completed_at timestamptz;
ALTER TABLE public.novu_queue_jobs ADD COLUMN attachment_scrubbed_at timestamptz;

CREATE FUNCTION public.novu_rust_has_workflow_attachment(p_kind text,p_payload jsonb) RETURNS boolean
LANGUAGE sql IMMUTABLE SET search_path=pg_catalog AS $$
 SELECT CASE
  WHEN p_kind IN ('experimental_static_in_app','experimental_static_in_app_attachments_v1') AND jsonb_typeof(p_payload->'emailAttachments')='array' THEN jsonb_array_length(p_payload->'emailAttachments')>0
  WHEN p_kind IN ('experimental_resend_email','experimental_sendgrid_email') AND p_payload ? 'workflow_context' AND jsonb_typeof(p_payload->'attachments')='array' THEN jsonb_array_length(p_payload->'attachments')>0
  ELSE false END
$$;
REVOKE ALL ON FUNCTION public.novu_rust_has_workflow_attachment(text,jsonb) FROM PUBLIC;

CREATE FUNCTION public.novu_rust_attachment_retention_guard() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF TG_OP='UPDATE' AND OLD.attachment_scrubbed_at IS NOT NULL AND (NEW.status<>'completed' OR NEW.kind IS DISTINCT FROM OLD.kind OR NEW.attachment_completed_at IS DISTINCT FROM OLD.attachment_completed_at OR NEW.attachment_scrubbed_at IS DISTINCT FROM OLD.attachment_scrubbed_at OR NEW.payload IS DISTINCT FROM OLD.payload) THEN
  RAISE EXCEPTION 'scrubbed attachment job cannot be replayed' USING ERRCODE='55000';
 END IF;
 IF NEW.status<>'completed' THEN
  NEW.attachment_completed_at:=NULL;
 ELSIF (TG_OP='INSERT' OR OLD.status<>'completed' OR NEW.attachment_completed_at IS NULL) AND public.novu_rust_has_workflow_attachment(NEW.kind,NEW.payload) THEN
  NEW.attachment_completed_at:=clock_timestamp();
 END IF;
 RETURN NEW;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_attachment_retention_guard() FROM PUBLIC;
CREATE TRIGGER novu_rust_attachment_retention_guard BEFORE INSERT OR UPDATE OF kind,status,payload,attachment_completed_at,attachment_scrubbed_at ON public.novu_queue_jobs
FOR EACH ROW EXECUTE FUNCTION public.novu_rust_attachment_retention_guard();
-- Old completed rows start their retention clock at migration, not job creation.
UPDATE public.novu_queue_jobs SET attachment_completed_at=clock_timestamp()
WHERE status='completed' AND public.novu_rust_has_workflow_attachment(kind,payload);
CREATE INDEX novu_rust_attachment_retention_idx ON public.novu_queue_jobs(environment_id,attachment_completed_at,id)
 WHERE status='completed' AND attachment_completed_at IS NOT NULL AND attachment_scrubbed_at IS NULL;

CREATE TABLE public.novu_rust_attachment_retention_grants (
 environment_id text NOT NULL REFERENCES public.novu_rust_environments(id), database_principal name NOT NULL,
 principal_oid oid NOT NULL, can_view boolean NOT NULL DEFAULT false, can_scrub boolean NOT NULL DEFAULT false,
 PRIMARY KEY(environment_id,database_principal)
);
CREATE TABLE public.novu_rust_attachment_retention_audit (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, environment_id text NOT NULL, job_id uuid NOT NULL,
 kind text NOT NULL, actor name NOT NULL, actor_oid oid NOT NULL, reason text NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX novu_rust_attachment_retention_audit_env_idx ON public.novu_rust_attachment_retention_audit(environment_id,id);
REVOKE ALL ON public.novu_rust_attachment_retention_grants,public.novu_rust_attachment_retention_audit FROM PUBLIC;

CREATE FUNCTION public.novu_rust_lock_attachment_retention(p_env text,p_scrub boolean) RETURNS oid
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_actor oid;
BEGIN
 SELECT oid INTO v_actor FROM pg_catalog.pg_roles WHERE rolname=session_user AND rolcanlogin;
 PERFORM 1 FROM public.novu_rust_attachment_retention_grants g WHERE g.environment_id=p_env AND g.database_principal=session_user AND g.principal_oid=v_actor AND (CASE WHEN p_scrub THEN g.can_scrub ELSE g.can_view END) FOR SHARE OF g;
 IF NOT FOUND THEN RAISE EXCEPTION 'attachment retention permission denied' USING ERRCODE='42501'; END IF;
 RETURN v_actor;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_lock_attachment_retention(text,boolean) FROM PUBLIC;

CREATE FUNCTION public.novu_rust_list_attachment_retention(p_env text,p_age integer,p_limit integer)
RETURNS TABLE(job_id uuid,kind text,completed_at text) LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF p_age IS NULL OR p_age NOT BETWEEN 0 AND 31536000 OR p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 100 THEN RAISE EXCEPTION 'invalid retention request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_lock_attachment_retention(p_env,false);
 RETURN QUERY SELECT j.id,j.kind,j.attachment_completed_at::text FROM public.novu_queue_jobs j
 WHERE j.environment_id=p_env AND j.status='completed' AND j.attachment_scrubbed_at IS NULL AND j.attachment_completed_at<=clock_timestamp()-p_age*interval '1 second' AND public.novu_rust_has_workflow_attachment(j.kind,j.payload)
 ORDER BY j.attachment_completed_at,j.id LIMIT p_limit;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_list_attachment_retention(text,integer,integer) FROM PUBLIC;

CREATE FUNCTION public.novu_rust_scrub_attachment_retention(p_env text,p_age integer,p_limit integer,p_reason text)
RETURNS TABLE(job_id uuid,kind text) LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_actor oid; v_job record;
BEGIN
 IF p_age IS NULL OR p_age NOT BETWEEN 0 AND 31536000 OR p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 100 OR p_reason IS NULL OR length(btrim(p_reason))=0 OR octet_length(p_reason)>512 THEN RAISE EXCEPTION 'invalid retention request' USING ERRCODE='22023'; END IF;
 v_actor:=public.novu_rust_lock_attachment_retention(p_env,true);
 FOR v_job IN SELECT j.id,j.kind FROM public.novu_queue_jobs j WHERE j.environment_id=p_env AND j.status='completed' AND j.attachment_scrubbed_at IS NULL AND j.attachment_completed_at<=clock_timestamp()-p_age*interval '1 second' AND public.novu_rust_has_workflow_attachment(j.kind,j.payload) ORDER BY j.attachment_completed_at,j.id FOR UPDATE OF j SKIP LOCKED LIMIT p_limit LOOP
  UPDATE public.novu_queue_jobs j SET payload=j.payload-CASE WHEN v_job.kind IN ('experimental_static_in_app','experimental_static_in_app_attachments_v1') THEN 'emailAttachments' ELSE 'attachments' END, attachment_scrubbed_at=clock_timestamp() WHERE j.id=v_job.id;
  INSERT INTO public.novu_rust_attachment_retention_audit(environment_id,job_id,kind,actor,actor_oid,reason) VALUES(p_env,v_job.id,v_job.kind,session_user,v_actor,p_reason);
  job_id:=v_job.id; kind:=v_job.kind; RETURN NEXT;
 END LOOP;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_scrub_attachment_retention(text,integer,integer,text) FROM PUBLIC;

CREATE FUNCTION public.novu_rust_attachment_retention_audit(p_env text,p_after bigint)
RETURNS TABLE(id text,job_id uuid,kind text,actor text,reason text,created_at text)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF p_after IS NULL OR p_after<0 THEN RAISE EXCEPTION 'invalid cursor' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_lock_attachment_retention(p_env,false);
 RETURN QUERY SELECT a.id::text,a.job_id,a.kind,a.actor::text,a.reason,a.created_at::text FROM public.novu_rust_attachment_retention_audit a WHERE a.environment_id=p_env AND a.id>p_after ORDER BY a.id LIMIT 100;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_attachment_retention_audit(text,bigint) FROM PUBLIC;
