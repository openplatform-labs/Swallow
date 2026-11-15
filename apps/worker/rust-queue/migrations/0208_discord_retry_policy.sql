-- The opt-in policy is scoped to an environment and one registered destination.
CREATE TABLE public.novu_rust_discord_retry_policy (
 environment_id text NOT NULL REFERENCES public.novu_rust_environments(id),
 destination_id text NOT NULL,
 mode text NOT NULL CHECK(mode IN ('off','known','fallback')),
 max_retries integer NOT NULL CHECK(max_retries BETWEEN 0 AND 10),
 fallback_seconds integer NOT NULL CHECK(fallback_seconds BETWEEN 1 AND 86400),
 PRIMARY KEY(environment_id,destination_id)
);
-- Each row is one explicit 429 response that was scheduled for another attempt.
CREATE TABLE public.novu_rust_discord_retry_history (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 job_id uuid NOT NULL REFERENCES public.novu_queue_jobs(id),
 environment_id text NOT NULL,
 destination_id text NOT NULL,
 retry_after_seconds integer,
 scheduled_delay_seconds integer NOT NULL,
 recorded_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX novu_rust_discord_retry_history_job ON public.novu_rust_discord_retry_history(job_id);
REVOKE ALL ON public.novu_rust_discord_retry_policy, public.novu_rust_discord_retry_history FROM PUBLIC;
-- Compatible workers set the marker after locking the live job in the same
-- transaction. Privileged direct SQL or workers deliberately spoofing it can bypass it.
CREATE FUNCTION public.novu_rust_discord_retry_version_fence()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_mode text; v_lease uuid;
BEGIN
 IF NEW.state <> 'dispatching' THEN RETURN NEW; END IF;
 SELECT p.mode INTO v_mode FROM public.novu_queue_jobs j
 JOIN public.novu_rust_discord_retry_policy p ON p.environment_id=j.environment_id AND p.destination_id=j.payload->>'destination'
 WHERE j.id=NEW.job_id AND j.environment_id=NEW.environment_id FOR SHARE OF p;
 IF v_mode IN ('known','fallback') THEN
  SELECT lease_token INTO v_lease FROM public.novu_queue_jobs WHERE id=NEW.job_id AND environment_id=NEW.environment_id AND kind IN ('experimental_discord_webhook','experimental_discord_workflow') AND status='running';
  IF v_lease IS NULL OR current_setting('swallow.discord_retry_fence',true) IS DISTINCT FROM NEW.job_id::text||':'||v_lease::text THEN
   RAISE EXCEPTION 'Discord retry policy requires a compatible worker' USING ERRCODE='55000';
  END IF;
 END IF;
 RETURN NEW;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_discord_retry_version_fence() FROM PUBLIC;
CREATE TRIGGER novu_rust_discord_retry_version_fence BEFORE INSERT ON public.novu_rust_discord_attempts
FOR EACH ROW EXECUTE FUNCTION public.novu_rust_discord_retry_version_fence();
