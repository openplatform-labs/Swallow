-- A row opts a destination in; no row preserves the existing behavior.
CREATE TABLE public.novu_rust_discord_shared_cooldown (
 environment_id text NOT NULL,
 destination_id text NOT NULL,
 not_before timestamptz NOT NULL DEFAULT '-infinity',
 PRIMARY KEY (environment_id,destination_id)
);
REVOKE ALL ON public.novu_rust_discord_shared_cooldown FROM PUBLIC;

-- Refuse older workers when a destination opts in. The row lock also
-- serializes a reservation with a committed 429 cooldown.
CREATE FUNCTION public.novu_rust_discord_cooldown_fence()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_until timestamptz; v_lease uuid;
BEGIN
 IF NEW.state <> 'dispatching' THEN RETURN NEW; END IF;
 SELECT c.not_before INTO v_until FROM public.novu_queue_jobs j
 JOIN public.novu_rust_discord_shared_cooldown c
 ON c.environment_id=j.environment_id AND c.destination_id=j.payload->>'destination'
 WHERE j.id=NEW.job_id AND j.environment_id=NEW.environment_id FOR UPDATE OF c;
 IF FOUND THEN
  SELECT lease_token INTO v_lease FROM public.novu_queue_jobs WHERE id=NEW.job_id AND environment_id=NEW.environment_id AND kind IN ('experimental_discord_webhook','experimental_discord_workflow') AND status='running';
  IF v_lease IS NULL OR current_setting('swallow.discord_cooldown_fence',true) IS DISTINCT FROM NEW.job_id::text||':'||v_lease::text THEN
   RAISE EXCEPTION 'Discord shared cooldown requires a compatible worker' USING ERRCODE='55000';
  END IF;
  IF v_until > clock_timestamp() THEN
   RAISE EXCEPTION 'Discord destination is cooling down' USING ERRCODE='55000';
  END IF;
 END IF;
 RETURN NEW;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_discord_cooldown_fence() FROM PUBLIC;
CREATE TRIGGER novu_rust_discord_cooldown_fence BEFORE INSERT ON public.novu_rust_discord_attempts
FOR EACH ROW EXECUTE FUNCTION public.novu_rust_discord_cooldown_fence();
