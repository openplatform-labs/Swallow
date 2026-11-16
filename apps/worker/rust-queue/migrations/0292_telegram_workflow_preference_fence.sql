-- Serialize workflow preference writes with Telegram workflow reservation.
-- Existing workers must be upgraded before relying on this guard.
CREATE FUNCTION public.novu_rust_workflow_preference_lock_parent() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE old_env text; old_workflow text; new_env text; new_workflow text;
BEGIN
 IF TG_OP<>'INSERT' THEN old_env:=OLD.environment_id; old_workflow:=OLD.workflow_identifier; END IF;
 IF TG_OP<>'DELETE' THEN new_env:=NEW.environment_id; new_workflow:=NEW.workflow_identifier; END IF;
 PERFORM 1 FROM public.novu_rust_workflows w WHERE (w.environment_id,w.identifier) IN ((old_env,old_workflow),(new_env,new_workflow)) ORDER BY w.environment_id,w.identifier FOR UPDATE;
 IF TG_OP='DELETE' THEN RETURN OLD; END IF;
 RETURN NEW;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_workflow_preference_lock_parent() FROM PUBLIC;
CREATE TRIGGER novu_rust_workflow_preference_lock_parent BEFORE INSERT OR UPDATE OR DELETE ON public.novu_rust_workflow_preferences
 FOR EACH ROW EXECUTE FUNCTION public.novu_rust_workflow_preference_lock_parent();

CREATE OR REPLACE FUNCTION public.novu_rust_telegram_workflow_snapshot(p_env text,p_dest text,p_job uuid,p_token uuid,p_allow_recreated boolean) RETURNS jsonb
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
 PERFORM 1 FROM public.novu_rust_workflows w WHERE w.environment_id=p_env AND w.identifier=c->>'workflow_identifier' AND (c->>'workflow_internal_id' IS NULL OR w.inbox_metadata->>'id'=c->>'workflow_internal_id') AND w.active AND w.has_steps AND w.chat_step_active AND w.chat_content=c->>'content_template' FOR SHARE;
 IF NOT FOUND THEN RETURN jsonb_build_object('error','Telegram workflow step changed or disabled'); END IF;
 SELECT CASE WHEN COALESCE(w.read_only,false) THEN COALESCE(w.chat_enabled,w.all_enabled,true) ELSE COALESCE((sp.channels->>'chat')::boolean,(p.channels->>'chat')::boolean,w.chat_enabled,w.all_enabled,true) END INTO allowed
 FROM (SELECT 1) singleton LEFT JOIN public.novu_rust_workflow_preferences w ON w.environment_id=p_env AND w.workflow_identifier=c->>'workflow_identifier'
 LEFT JOIN public.novu_rust_subscriber_preferences p ON p.environment_id=p_env AND p.subscriber_id=c->>'subscriber_id'
 LEFT JOIN public.novu_rust_subscriber_workflow_preferences sp ON sp.environment_id=p_env AND sp.subscriber_id=c->>'subscriber_id' AND sp.workflow_identifier=c->>'workflow_identifier';
 IF NOT allowed THEN RETURN jsonb_build_object('error','chat preference disabled'); END IF;
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
