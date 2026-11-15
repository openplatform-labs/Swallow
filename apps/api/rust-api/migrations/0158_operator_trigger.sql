-- Separate workflow-specific execution permission; existing read/write grants do not imply it.
CREATE TABLE public.novu_rust_operator_jwt_trigger_execute_grants (
 issuer text NOT NULL,subject text NOT NULL,client_id text NOT NULL,environment_id text NOT NULL,workflow_identifier text NOT NULL,
 PRIMARY KEY(issuer,subject,client_id,environment_id,workflow_identifier),
 FOREIGN KEY(issuer,subject,client_id,environment_id) REFERENCES public.novu_rust_operator_jwt_identities(issuer,subject,client_id,environment_id) ON DELETE CASCADE
);
CREATE TABLE public.novu_rust_operator_jwt_trigger_audit (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(),environment_id text NOT NULL,workflow_identifier text NOT NULL,transaction_id text NOT NULL,
 issuer text NOT NULL,subject text NOT NULL,client_id text NOT NULL,database_principal name NOT NULL,recorded_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX novu_rust_operator_jwt_trigger_audit_env_idx ON public.novu_rust_operator_jwt_trigger_audit(environment_id,id);
REVOKE ALL ON public.novu_rust_operator_jwt_trigger_execute_grants,public.novu_rust_operator_jwt_trigger_audit FROM PUBLIC;
CREATE FUNCTION public.novu_rust_operator_jwt_trigger_authorize(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_workflow text)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF p_workflow IS NULL OR octet_length(p_workflow) NOT BETWEEN 1 AND 512 OR p_workflow ~ '[[:cntrl:]]' THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 PERFORM 1 FROM public.novu_rust_operator_jwt_trigger_execute_grants g WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env AND g.workflow_identifier=p_workflow FOR SHARE OF g;
 IF NOT FOUND OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN true;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_trigger_authorize(text,text,text,text,bigint,text) FROM PUBLIC;

-- This function is callable only by the trusted API DB login. Input is prepared
-- by the existing trigger handler; permission, enqueue and audit share a transaction.
CREATE FUNCTION public.novu_rust_operator_jwt_trigger_enqueue(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_workflow text,p_txn text,p_job uuid,p_kind text,p_data jsonb,p_contexts jsonb)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE inserted bigint;
BEGIN
 PERFORM public.novu_rust_operator_jwt_trigger_authorize(p_issuer,p_subject,p_client,p_env,p_exp,p_workflow);
 IF p_txn IS NULL OR octet_length(p_txn) NOT BETWEEN 1 AND 512 OR p_txn ~ '[[:cntrl:]]' OR p_job IS NULL OR p_kind IS NULL OR p_kind NOT IN ('experimental_static_in_app','experimental_static_in_app_attachments_v1') OR jsonb_typeof(p_data) IS DISTINCT FROM 'object' OR p_data->>'workflow' IS DISTINCT FROM p_workflow OR p_data->>'transactionId' IS DISTINCT FROM p_txn OR jsonb_typeof(p_data->'recipients') IS DISTINCT FROM 'array' OR jsonb_typeof(p_data->'topicKeys') IS DISTINCT FROM 'array' OR jsonb_typeof(p_contexts) IS DISTINCT FROM 'array' THEN RAISE EXCEPTION 'invalid request' USING ERRCODE='22023'; END IF;
 WITH accepted AS (INSERT INTO public.novu_rust_trigger_transactions(environment_id,transaction_id) VALUES(p_env,p_txn) ON CONFLICT DO NOTHING RETURNING 1),
 contexts AS (INSERT INTO public.novu_rust_contexts(environment_id,context_key,data) SELECT p_env,entry->>'key',COALESCE(NULLIF(entry->'data','null'::jsonb),'{}'::jsonb) FROM accepted CROSS JOIN jsonb_array_elements(p_contexts) entry ON CONFLICT(environment_id,context_key) DO UPDATE SET data=CASE WHEN EXISTS(SELECT 1 FROM jsonb_array_elements(p_contexts) element WHERE element->>'key'=EXCLUDED.context_key AND element->'data'<>'null'::jsonb) THEN EXCLUDED.data ELSE public.novu_rust_contexts.data END,updated_at=CASE WHEN EXISTS(SELECT 1 FROM jsonb_array_elements(p_contexts) element WHERE element->>'key'=EXCLUDED.context_key AND element->'data'<>'null'::jsonb) AND public.novu_rust_contexts.data IS DISTINCT FROM EXCLUDED.data THEN now() ELSE public.novu_rust_contexts.updated_at END RETURNING 1),
 enqueued AS (INSERT INTO public.novu_queue_jobs(id,environment_id,kind,payload) SELECT p_job,p_env,p_kind,p_data||jsonb_build_object('recipientIdentitySnapshot',(SELECT COALESCE(jsonb_object_agg(r->>'subscriberId',s.internal_id),'{}'::jsonb) FROM jsonb_array_elements(p_data->'recipients') r LEFT JOIN public.novu_rust_subscribers s ON s.environment_id=p_env AND s.subscriber_id=r->>'subscriberId'),'actorIdentitySnapshot',(SELECT s.internal_id FROM public.novu_rust_subscribers s WHERE s.environment_id=p_env AND s.subscriber_id=p_data->'actor'->>'subscriberId'),'topicIdentitySnapshot',(SELECT COALESCE(jsonb_object_agg(t.subscriber_id,s.internal_id),'{}'::jsonb) FROM public.novu_rust_topic_subscribers t JOIN public.novu_rust_subscribers s ON s.environment_id=t.environment_id AND s.subscriber_id=t.subscriber_id WHERE t.environment_id=p_env AND t.topic_key IN (SELECT jsonb_array_elements_text(p_data->'topicKeys')))) FROM accepted RETURNING 1)
 SELECT count(*) INTO inserted FROM enqueued;
 IF inserted=1 THEN INSERT INTO public.novu_rust_operator_jwt_trigger_audit(environment_id,workflow_identifier,transaction_id,issuer,subject,client_id,database_principal) VALUES(p_env,p_workflow,p_txn,p_issuer,p_subject,p_client,session_user); END IF;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN true;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_trigger_enqueue(text,text,text,text,bigint,text,text,uuid,text,jsonb,jsonb) FROM PUBLIC;
