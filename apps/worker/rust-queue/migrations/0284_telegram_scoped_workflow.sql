-- Workflow functions are opt-in separately; existing scoped worker functions retain their old behavior.
CREATE FUNCTION public.novu_rust_telegram_workflow_claim(p_env text,p_dest text,p_token uuid)
RETURNS TABLE(id uuid,kind text,payload jsonb,attempts integer) LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF p_token IS NULL THEN RAISE EXCEPTION 'invalid Telegram lease' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_telegram_worker_access(p_env,p_dest);
 RETURN QUERY WITH candidate AS (SELECT j.id FROM public.novu_queue_jobs j WHERE j.environment_id=p_env AND j.kind IN ('experimental_telegram','experimental_telegram_workflow') AND j.status='pending' AND j.payload->>'destination'=p_dest AND j.available_at<=now() ORDER BY j.available_at,j.created_at,j.id FOR UPDATE SKIP LOCKED LIMIT 1)
 UPDATE public.novu_queue_jobs j SET status='running',attempts=j.attempts+1,lease_token=p_token,lease_until=clock_timestamp()+interval '60 seconds' FROM candidate c WHERE j.id=c.id RETURNING j.id,j.kind,j.payload,j.attempts;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_telegram_workflow_claim(text,text,uuid) FROM PUBLIC;

-- Caller must hold this transaction until preparation commits; all values derive from the leased job.
CREATE FUNCTION public.novu_rust_telegram_workflow_snapshot(p_env text,p_dest text,p_job uuid,p_token uuid,p_allow_recreated boolean) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE c jsonb; profile jsonb; actor jsonb; contexts jsonb; chat text; keys text[]; row_data record; actor_id text; allowed boolean;
BEGIN
 PERFORM public.novu_rust_telegram_worker_access(p_env,p_dest);
 SELECT j.payload->'workflowContext' INTO c FROM public.novu_queue_jobs j WHERE j.id=p_job AND j.environment_id=p_env AND j.kind='experimental_telegram_workflow' AND j.payload->>'destination'=p_dest AND j.status='running' AND j.lease_token=p_token AND j.lease_until>clock_timestamp() FOR UPDATE OF j;
 IF NOT FOUND THEN RETURN NULL; END IF;
 SELECT s.profile,s.internal_id INTO row_data FROM public.novu_rust_subscribers s WHERE s.environment_id=p_env AND s.subscriber_id=c->>'subscriber_id' FOR UPDATE;
 IF NOT FOUND THEN RETURN jsonb_build_object('error','subscriber not found'); END IF;
 IF row_data.internal_id IS DISTINCT FROM c->>'subscriber_internal_id' THEN RETURN jsonb_build_object('error','subscriber identity changed'); END IF;
 profile:=row_data.profile;
 IF jsonb_typeof(profile) IS DISTINCT FROM 'object' THEN RETURN jsonb_build_object('error','invalid subscriber profile'); END IF;
 profile:=profile || jsonb_build_object('subscriberId',c->>'subscriber_id');
 SELECT CASE WHEN COALESCE(w.read_only,false) THEN COALESCE(w.chat_enabled,w.all_enabled,true) ELSE COALESCE((sp.channels->>'chat')::boolean,(p.channels->>'chat')::boolean,w.chat_enabled,w.all_enabled,true) END INTO allowed
 FROM (SELECT 1) singleton LEFT JOIN public.novu_rust_workflow_preferences w ON w.environment_id=p_env AND w.workflow_identifier=c->>'workflow_identifier'
 LEFT JOIN public.novu_rust_subscriber_preferences p ON p.environment_id=p_env AND p.subscriber_id=c->>'subscriber_id'
 LEFT JOIN public.novu_rust_subscriber_workflow_preferences sp ON sp.environment_id=p_env AND sp.subscriber_id=c->>'subscriber_id' AND sp.workflow_identifier=c->>'workflow_identifier';
 IF NOT allowed THEN RETURN jsonb_build_object('error','chat preference disabled'); END IF;
 PERFORM 1 FROM public.novu_rust_workflows w WHERE w.environment_id=p_env AND w.identifier=c->>'workflow_identifier' AND (c->>'workflow_internal_id' IS NULL OR w.inbox_metadata->>'id'=c->>'workflow_internal_id') AND w.active AND w.has_steps AND w.chat_step_active AND w.chat_content=c->>'content_template' FOR SHARE;
 IF NOT FOUND THEN RETURN jsonb_build_object('error','Telegram workflow step changed or disabled'); END IF;
 SELECT COALESCE(array_agg(value ORDER BY ord),'{}'::text[]) INTO keys FROM jsonb_array_elements_text(c->'context_keys') WITH ORDINALITY t(value,ord);
 SELECT e.chat_id INTO chat FROM public.novu_rust_telegram_workflow_endpoints e WHERE e.environment_id=p_env AND e.internal_id=(c->>'endpoint_id')::uuid AND e.revision=(c->>'endpoint_revision')::uuid AND e.subscriber_internal_id=c->>'subscriber_internal_id' AND e.destination_id=p_dest AND e.context_keys @> keys AND e.context_keys <@ keys AND cardinality(e.context_keys)=cardinality(keys) FOR SHARE;
 IF NOT FOUND THEN RETURN jsonb_build_object('error','Telegram endpoint changed or deleted'); END IF;
 actor:=c->'actor'; actor_id:=actor->>'subscriberId';
 IF actor_id IS NOT NULL THEN
  IF actor_id=c->>'subscriber_id' THEN actor:=profile;
  ELSE
   IF NOT p_allow_recreated AND COALESCE(c->>'actor_internal_id','')='' THEN RETURN jsonb_build_object('error','actor identity changed or missing'); END IF;
   SELECT s.profile INTO actor FROM public.novu_rust_subscribers s WHERE s.environment_id=p_env AND s.subscriber_id=actor_id AND (p_allow_recreated OR s.internal_id=c->>'actor_internal_id');
   IF NOT FOUND AND NOT p_allow_recreated THEN RETURN jsonb_build_object('error','actor identity changed or missing'); END IF;
   IF jsonb_typeof(actor)='object' THEN actor:=actor || jsonb_build_object('subscriberId',actor_id); END IF;
  END IF;
 END IF;
 SELECT COALESCE(jsonb_object_agg(split_part(t.context_key,':',1),jsonb_build_object('id',substring(t.context_key from position(':' in t.context_key)+1),'data',t.data)),'{}'::jsonb) INTO contexts FROM public.novu_rust_contexts t WHERE t.environment_id=p_env AND t.context_key=ANY(keys) AND position(':' in t.context_key)>0;
 RETURN jsonb_build_object('profile',profile,'actor',actor,'contexts',contexts,'chatId',chat);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_telegram_workflow_snapshot(text,text,uuid,uuid,boolean) FROM PUBLIC;

CREATE FUNCTION public.novu_rust_telegram_workflow_prepare(p_env text,p_dest text,p_job uuid,p_token uuid,p_error text,p_allow_recreated boolean) RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_count integer; v_snapshot jsonb;
BEGIN
 PERFORM public.novu_rust_telegram_worker_access(p_env,p_dest);
 PERFORM 1 FROM public.novu_queue_jobs j WHERE j.id=p_job AND j.environment_id=p_env AND j.kind='experimental_telegram_workflow' AND j.payload->>'destination'=p_dest AND j.status='running' AND j.lease_token=p_token AND j.lease_until>clock_timestamp() FOR UPDATE OF j;
 IF NOT FOUND THEN RETURN false; END IF;
 IF p_allow_recreated IS NULL THEN RAISE EXCEPTION 'invalid Telegram actor policy' USING ERRCODE='22023'; END IF;
 IF p_error IS NULL THEN
  v_snapshot:=public.novu_rust_telegram_workflow_snapshot(p_env,p_dest,p_job,p_token,p_allow_recreated);
  IF v_snapshot IS NULL THEN RETURN false; END IF;
  p_error:=v_snapshot->>'error';
 END IF;
 IF p_error IS NOT NULL THEN
  IF p_error NOT IN ('invalid Telegram workflow job','subscriber not found','subscriber identity changed','invalid subscriber profile','chat preference disabled','Telegram workflow step changed or disabled','Telegram endpoint changed or deleted','actor identity changed or missing','unsupported Telegram template or input','invalid Telegram request') THEN RAISE EXCEPTION 'invalid Telegram diagnostic' USING ERRCODE='22023'; END IF;
  UPDATE public.novu_queue_jobs SET status=CASE WHEN p_error='chat preference disabled' THEN 'cancelled' ELSE 'failed' END,last_error=p_error,lease_token=NULL,lease_until=NULL WHERE id=p_job;
  RETURN false;
 END IF;
 INSERT INTO public.novu_rust_telegram_attempts(job_id,environment_id,state) VALUES(p_job,p_env,'dispatching') ON CONFLICT DO NOTHING;
 GET DIAGNOSTICS v_count=ROW_COUNT;
 IF v_count<>1 THEN UPDATE public.novu_queue_jobs SET status='failed',last_error='Telegram attempt requires review',lease_token=NULL,lease_until=NULL WHERE id=p_job; RETURN false; END IF;
 IF NOT EXISTS(SELECT 1 FROM public.novu_queue_jobs WHERE id=p_job AND lease_until>clock_timestamp()) THEN RAISE EXCEPTION 'Telegram lease expired during preparation' USING ERRCODE='40001'; END IF;
 RETURN true;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_telegram_workflow_prepare(text,text,uuid,uuid,text,boolean) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.novu_rust_telegram_worker_finish(p_env text,p_dest text,p_job uuid,p_token uuid,p_state text,p_code integer,p_id text,p_timestamp text) RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_count integer;
BEGIN
 IF p_job IS NULL OR p_token IS NULL OR p_state IS NULL OR p_state NOT IN ('accepted','rejected','unknown') OR
    (p_state='accepted' AND (p_id IS NULL OR p_id !~ '^[0-9]{1,20}$' OR p_timestamp IS NULL OR octet_length(p_timestamp)>64 OR p_code IS NOT NULL)) OR
    (p_state='rejected' AND (p_code IS NULL OR p_code NOT BETWEEN 400 AND 499 OR p_code IN (408,409) OR p_id IS NOT NULL OR p_timestamp IS NOT NULL)) OR
    (p_state='unknown' AND (p_code IS NOT NULL OR p_id IS NOT NULL OR p_timestamp IS NOT NULL)) THEN RAISE EXCEPTION 'invalid Telegram result' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_telegram_worker_access(p_env,p_dest);
 PERFORM 1 FROM public.novu_queue_jobs j WHERE j.id=p_job AND j.environment_id=p_env AND j.kind IN ('experimental_telegram','experimental_telegram_workflow') AND j.payload->>'destination'=p_dest AND j.status='running' AND j.lease_token=p_token AND j.lease_until>clock_timestamp() FOR UPDATE OF j;
 IF NOT FOUND THEN RETURN false; END IF;
 UPDATE public.novu_rust_telegram_attempts SET state=p_state,http_status=p_code,provider_message_id=p_id,provider_timestamp=p_timestamp,updated_at=clock_timestamp() WHERE job_id=p_job AND environment_id=p_env AND state='dispatching';
 GET DIAGNOSTICS v_count=ROW_COUNT;
 IF v_count=1 THEN UPDATE public.novu_queue_jobs SET status=CASE WHEN p_state='accepted' THEN 'completed' ELSE 'failed' END,last_error=CASE p_state WHEN 'accepted' THEN NULL WHEN 'rejected' THEN 'Telegram rejected request' ELSE 'Telegram outcome unknown' END,lease_token=NULL,lease_until=NULL WHERE id=p_job; RETURN true; END IF;
 RETURN false;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_telegram_worker_finish(text,text,uuid,uuid,text,integer,text,text) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_telegram_workflow_recover(p_env text,p_dest text) RETURNS integer LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE r record; v_count integer:=0;
BEGIN
 PERFORM public.novu_rust_telegram_worker_access(p_env,p_dest);
 FOR r IN SELECT j.id FROM public.novu_queue_jobs j WHERE j.environment_id=p_env AND j.kind IN ('experimental_telegram','experimental_telegram_workflow') AND j.payload->>'destination'=p_dest AND j.status='running' AND j.lease_until<=clock_timestamp() ORDER BY j.lease_until,j.id FOR UPDATE SKIP LOCKED LIMIT 100 LOOP
  IF EXISTS(SELECT 1 FROM public.novu_rust_telegram_attempts a WHERE a.job_id=r.id AND a.environment_id=p_env) THEN
   UPDATE public.novu_rust_telegram_attempts SET state='unknown',updated_at=clock_timestamp() WHERE job_id=r.id AND environment_id=p_env AND state='dispatching';
   UPDATE public.novu_queue_jobs SET status='failed',last_error='Telegram outcome unknown',lease_token=NULL,lease_until=NULL WHERE id=r.id;
  ELSE UPDATE public.novu_queue_jobs SET status='pending',lease_token=NULL,lease_until=NULL WHERE id=r.id; END IF;
  v_count:=v_count+1;
 END LOOP;
 RETURN v_count;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_telegram_workflow_recover(text,text) FROM PUBLIC;
