-- Audit viewers have separate, environment-scoped authority from resolvers.
CREATE TABLE public.novu_rust_smtp_resolution_audit_viewers (
 environment_id text NOT NULL REFERENCES public.novu_rust_environments(id),
 database_principal name NOT NULL, principal_oid oid NOT NULL,
 PRIMARY KEY(environment_id,database_principal)
);
REVOKE ALL ON public.novu_rust_smtp_resolution_audit_viewers FROM PUBLIC;
CREATE INDEX novu_rust_smtp_resolution_job_history_idx ON public.novu_rust_smtp_resolutions(environment_id,job_id,created_at DESC,actor_oid DESC,request_id DESC);

CREATE FUNCTION public.novu_rust_smtp_resolution_history(p_env text,p_job uuid,p_before_actor oid,p_before_request uuid)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_time timestamptz; v_result jsonb;
BEGIN
 IF p_env IS NULL OR length(btrim(p_env))=0 OR p_job IS NULL OR ((p_before_actor IS NULL)<>(p_before_request IS NULL)) THEN
  RAISE EXCEPTION 'invalid SMTP resolution history request' USING ERRCODE='22023'; END IF;
 PERFORM 1 FROM public.novu_rust_smtp_resolution_audit_viewers v JOIN pg_catalog.pg_roles r ON r.oid=v.principal_oid AND r.rolname=v.database_principal AND r.rolcanlogin
 WHERE v.environment_id=p_env AND v.database_principal=session_user FOR SHARE OF v;
 IF NOT FOUND THEN RAISE EXCEPTION 'SMTP resolution history access denied' USING ERRCODE='42501'; END IF;
 IF p_before_actor IS NOT NULL THEN
  SELECT created_at INTO v_time FROM public.novu_rust_smtp_resolutions WHERE environment_id=p_env AND job_id=p_job AND actor_oid=p_before_actor AND request_id=p_before_request;
  IF NOT FOUND THEN RAISE EXCEPTION 'SMTP resolution history cursor unavailable' USING ERRCODE='22023'; END IF;
 END IF;
 WITH candidates AS MATERIALIZED (
  SELECT * FROM public.novu_rust_smtp_resolutions a WHERE a.environment_id=p_env AND a.job_id=p_job
   AND (p_before_actor IS NULL OR (a.created_at,a.actor_oid,a.request_id)<(v_time,p_before_actor,p_before_request))
  ORDER BY a.created_at DESC,a.actor_oid DESC,a.request_id DESC LIMIT 101
 ), page AS MATERIALIZED (SELECT * FROM candidates ORDER BY created_at DESC,actor_oid DESC,request_id DESC LIMIT 100)
 SELECT jsonb_build_object('data',coalesce(jsonb_agg(jsonb_build_object('actor',actor::text,'actorOid',actor_oid::text,'requestId',request_id,'jobId',job_id,'attemptRevision',attempt_revision,'failureRevision',failure_revision,'decision',decision,'reason',reason,'createdAt',to_char(created_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"')) ORDER BY created_at DESC,actor_oid DESC,request_id DESC),'[]'::jsonb),
 'next',CASE WHEN (SELECT count(*) FROM candidates)>100 THEN (SELECT jsonb_build_object('actorOid',actor_oid::text,'requestId',request_id) FROM page ORDER BY created_at,actor_oid,request_id LIMIT 1) ELSE NULL END)
 INTO v_result FROM page;
 RETURN v_result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_smtp_resolution_history(text,uuid,oid,uuid) FROM PUBLIC;
