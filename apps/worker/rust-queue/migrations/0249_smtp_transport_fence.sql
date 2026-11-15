-- Optional cooperative mixed-version fence; NULL leaves legacy behavior.
LOCK TABLE public.novu_rust_environments IN SHARE ROW EXCLUSIVE MODE;
CREATE TABLE public.novu_rust_smtp_transport_policy (
 environment_id text PRIMARY KEY REFERENCES public.novu_rust_environments(id) ON DELETE CASCADE,
 allowed_mode text CHECK (allowed_mode IN ('none','starttls','tls'))
);
REVOKE ALL ON public.novu_rust_smtp_transport_policy FROM PUBLIC;
INSERT INTO public.novu_rust_smtp_transport_policy(environment_id) SELECT id FROM public.novu_rust_environments;
CREATE FUNCTION public.novu_rust_smtp_transport_init() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN INSERT INTO public.novu_rust_smtp_transport_policy(environment_id) VALUES(NEW.id); RETURN NEW; END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_smtp_transport_init() FROM PUBLIC;
CREATE TRIGGER novu_rust_smtp_transport_init_trigger AFTER INSERT ON public.novu_rust_environments FOR EACH ROW EXECUTE FUNCTION public.novu_rust_smtp_transport_init();

CREATE FUNCTION public.novu_rust_smtp_transport_check(p_env text,p_mode text) RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_mode text;
BEGIN
 IF p_env IS NULL OR p_mode IS NULL OR p_mode NOT IN ('none','starttls','tls') THEN RAISE EXCEPTION 'invalid SMTP transport' USING ERRCODE='22023'; END IF;
 SELECT allowed_mode INTO v_mode FROM public.novu_rust_smtp_transport_policy WHERE environment_id=p_env FOR SHARE;
 IF NOT FOUND THEN RAISE EXCEPTION 'SMTP transport policy missing' USING ERRCODE='55000'; END IF;
 RETURN v_mode IS NOT NULL AND v_mode=p_mode;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_smtp_transport_check(text,text) FROM PUBLIC;

CREATE FUNCTION public.novu_rust_smtp_transport_guard(p_env text,p_job uuid,p_lease uuid,p_mode text) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 IF NOT public.novu_rust_smtp_transport_check(p_env,p_mode) THEN RAISE EXCEPTION 'SMTP transport policy mismatch' USING ERRCODE='55000'; END IF;
 PERFORM 1 FROM public.novu_queue_jobs WHERE id=p_job AND environment_id=p_env AND kind='experimental_smtp_email' AND status='running' AND lease_token=p_lease AND lease_until>clock_timestamp();
 IF NOT FOUND THEN RAISE EXCEPTION 'SMTP transport lease lost' USING ERRCODE='55000'; END IF;
 PERFORM pg_catalog.set_config('swallow.smtp_transport_fence',p_job::text||':'||p_lease::text||':'||p_mode,true);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_smtp_transport_guard(text,uuid,uuid,text) FROM PUBLIC;

CREATE FUNCTION public.novu_rust_smtp_transport_fence() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_mode text; v_lease uuid;
BEGIN
 SELECT allowed_mode INTO v_mode FROM public.novu_rust_smtp_transport_policy WHERE environment_id=NEW.environment_id FOR SHARE;
 IF NOT FOUND THEN RAISE EXCEPTION 'SMTP transport policy missing' USING ERRCODE='55000'; END IF;
 IF v_mode IS NOT NULL THEN
  SELECT lease_token INTO v_lease FROM public.novu_queue_jobs WHERE id=NEW.job_id AND environment_id=NEW.environment_id AND kind='experimental_smtp_email' AND status='running' AND lease_until>clock_timestamp();
  IF v_lease IS NULL OR current_setting('swallow.smtp_transport_fence',true) IS DISTINCT FROM NEW.job_id::text||':'||v_lease::text||':'||v_mode THEN
   RAISE EXCEPTION 'SMTP transport requires matching guarded worker' USING ERRCODE='55000';
  END IF;
 END IF;
 RETURN NEW;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_smtp_transport_fence() FROM PUBLIC;
CREATE TRIGGER novu_rust_smtp_transport_fence_trigger BEFORE INSERT ON public.novu_rust_smtp_attempts FOR EACH ROW EXECUTE FUNCTION public.novu_rust_smtp_transport_fence();
