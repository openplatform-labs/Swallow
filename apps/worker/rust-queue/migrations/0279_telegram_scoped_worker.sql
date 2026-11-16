-- Registrations are explicit and tied to the LOGIN OID and destination.
CREATE TABLE public.novu_rust_telegram_worker_grants (
 environment_id text NOT NULL REFERENCES public.novu_rust_environments(id), destination_id text NOT NULL,
 database_principal name NOT NULL, principal_oid oid NOT NULL,
 PRIMARY KEY(environment_id,destination_id,database_principal)
);
REVOKE ALL ON public.novu_rust_telegram_worker_grants FROM PUBLIC;
CREATE FUNCTION public.novu_rust_telegram_worker_access(p_env text,p_dest text) RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF p_env IS NULL OR length(btrim(p_env))=0 OR p_dest IS NULL OR p_dest !~ '^[A-Za-z0-9._-]{1,64}$' THEN RAISE EXCEPTION 'invalid Telegram worker scope' USING ERRCODE='22023'; END IF;
 PERFORM 1 FROM public.novu_rust_telegram_worker_grants g JOIN pg_catalog.pg_roles r ON r.oid=g.principal_oid AND r.rolname=g.database_principal AND r.rolcanlogin WHERE g.environment_id=p_env AND g.destination_id=p_dest AND g.database_principal=session_user FOR SHARE OF g;
 IF NOT FOUND THEN RAISE EXCEPTION 'Telegram worker access denied' USING ERRCODE='42501'; END IF;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_telegram_worker_access(text,text) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_telegram_worker_register(p_env text,p_dest text,p_hash text) RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF p_hash IS NULL OR p_hash !~ '^[A-Za-z0-9+/]{43}=$' THEN RAISE EXCEPTION 'invalid Telegram binding' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_telegram_worker_access(p_env,p_dest);
 INSERT INTO public.novu_rust_telegram_destinations(environment_id,destination_id,token_hash) VALUES(p_env,p_dest,p_hash) ON CONFLICT(environment_id,destination_id) DO UPDATE SET token_hash=EXCLUDED.token_hash WHERE public.novu_rust_telegram_destinations.token_hash=EXCLUDED.token_hash;
 RETURN FOUND;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_telegram_worker_register(text,text,text) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_telegram_worker_claim(p_env text,p_dest text,p_token uuid) RETURNS TABLE(id uuid,payload jsonb,attempts integer) LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF p_token IS NULL THEN RAISE EXCEPTION 'invalid Telegram lease' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_telegram_worker_access(p_env,p_dest);
 RETURN QUERY WITH candidate AS (SELECT j.id FROM public.novu_queue_jobs j WHERE j.environment_id=p_env AND j.kind='experimental_telegram' AND j.status='pending' AND j.payload->>'destination'=p_dest AND j.available_at<=now() ORDER BY j.available_at,j.created_at,j.id FOR UPDATE SKIP LOCKED LIMIT 1)
 UPDATE public.novu_queue_jobs j SET status='running',attempts=j.attempts+1,lease_token=p_token,lease_until=clock_timestamp()+interval '60 seconds' FROM candidate c WHERE j.id=c.id RETURNING j.id,j.payload,j.attempts;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_telegram_worker_claim(text,text,uuid) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_telegram_worker_prepare(p_env text,p_dest text,p_job uuid,p_token uuid,p_valid boolean) RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_count integer;
BEGIN
 IF p_job IS NULL OR p_token IS NULL OR p_valid IS NULL THEN RAISE EXCEPTION 'invalid Telegram preparation' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_telegram_worker_access(p_env,p_dest);
 PERFORM 1 FROM public.novu_queue_jobs j WHERE j.id=p_job AND j.environment_id=p_env AND j.kind='experimental_telegram' AND j.payload->>'destination'=p_dest AND j.status='running' AND j.lease_token=p_token AND j.lease_until>clock_timestamp() FOR UPDATE OF j;
 IF NOT FOUND THEN RETURN false; END IF;
 IF NOT p_valid THEN UPDATE public.novu_queue_jobs SET status='failed',last_error='invalid Telegram job',lease_token=NULL,lease_until=NULL WHERE id=p_job; RETURN false; END IF;
 INSERT INTO public.novu_rust_telegram_attempts(job_id,environment_id,state) VALUES(p_job,p_env,'dispatching') ON CONFLICT DO NOTHING;
 GET DIAGNOSTICS v_count=ROW_COUNT;
 IF v_count<>1 THEN UPDATE public.novu_queue_jobs SET status='failed',last_error='Telegram attempt requires review',lease_token=NULL,lease_until=NULL WHERE id=p_job; RETURN false; END IF;
 IF NOT EXISTS(SELECT 1 FROM public.novu_queue_jobs WHERE id=p_job AND lease_until>clock_timestamp()) THEN RAISE EXCEPTION 'Telegram lease expired during preparation' USING ERRCODE='40001'; END IF;
 RETURN true;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_telegram_worker_prepare(text,text,uuid,uuid,boolean) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_telegram_worker_finish(p_env text,p_dest text,p_job uuid,p_token uuid,p_state text,p_code integer,p_id text,p_timestamp text) RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_count integer;
BEGIN
 IF p_job IS NULL OR p_token IS NULL OR p_state IS NULL OR p_state NOT IN ('accepted','rejected','unknown') OR
    (p_state='accepted' AND (p_id IS NULL OR p_id !~ '^[0-9]{1,20}$' OR p_timestamp IS NULL OR octet_length(p_timestamp)>64 OR p_code IS NOT NULL)) OR
    (p_state='rejected' AND (p_code IS NULL OR p_code NOT BETWEEN 400 AND 499 OR p_code IN (408,409) OR p_id IS NOT NULL OR p_timestamp IS NOT NULL)) OR
    (p_state='unknown' AND (p_code IS NOT NULL OR p_id IS NOT NULL OR p_timestamp IS NOT NULL)) THEN RAISE EXCEPTION 'invalid Telegram result' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_telegram_worker_access(p_env,p_dest);
 PERFORM 1 FROM public.novu_queue_jobs j WHERE j.id=p_job AND j.environment_id=p_env AND j.kind='experimental_telegram' AND j.payload->>'destination'=p_dest AND j.status='running' AND j.lease_token=p_token AND j.lease_until>clock_timestamp() FOR UPDATE OF j;
 IF NOT FOUND THEN RETURN false; END IF;
 UPDATE public.novu_rust_telegram_attempts SET state=p_state,http_status=p_code,provider_message_id=p_id,provider_timestamp=p_timestamp,updated_at=clock_timestamp() WHERE job_id=p_job AND environment_id=p_env AND state='dispatching';
 GET DIAGNOSTICS v_count=ROW_COUNT;
 IF v_count=1 THEN UPDATE public.novu_queue_jobs SET status=CASE WHEN p_state='accepted' THEN 'completed' ELSE 'failed' END,last_error=CASE p_state WHEN 'accepted' THEN NULL WHEN 'rejected' THEN 'Telegram rejected request' ELSE 'Telegram outcome unknown' END,lease_token=NULL,lease_until=NULL WHERE id=p_job; RETURN true; END IF;
 RETURN false;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_telegram_worker_finish(text,text,uuid,uuid,text,integer,text,text) FROM PUBLIC;
CREATE FUNCTION public.novu_rust_telegram_worker_recover(p_env text,p_dest text) RETURNS integer LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE r record; v_count integer:=0;
BEGIN
 PERFORM public.novu_rust_telegram_worker_access(p_env,p_dest);
 FOR r IN SELECT j.id FROM public.novu_queue_jobs j WHERE j.environment_id=p_env AND j.kind='experimental_telegram' AND j.payload->>'destination'=p_dest AND j.status='running' AND j.lease_until<=clock_timestamp() ORDER BY j.lease_until,j.id FOR UPDATE SKIP LOCKED LIMIT 100 LOOP
  IF EXISTS(SELECT 1 FROM public.novu_rust_telegram_attempts a WHERE a.job_id=r.id AND a.environment_id=p_env) THEN
   UPDATE public.novu_rust_telegram_attempts SET state='unknown',updated_at=clock_timestamp() WHERE job_id=r.id AND environment_id=p_env AND state='dispatching';
   UPDATE public.novu_queue_jobs SET status='failed',last_error='Telegram outcome unknown',lease_token=NULL,lease_until=NULL WHERE id=r.id;
  ELSE UPDATE public.novu_queue_jobs SET status='pending',lease_token=NULL,lease_until=NULL WHERE id=r.id; END IF;
  v_count:=v_count+1;
 END LOOP;
 RETURN v_count;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_telegram_worker_recover(text,text) FROM PUBLIC;
