-- NULL allows the first upgraded worker to bind the policy for each environment.
-- Legacy workers do not check these columns and must be stopped before relying on them.
ALTER TABLE public.novu_rust_push_workflow_worker_config
 ADD COLUMN multicast_429_policy text CHECK (multicast_429_policy IN ('continue','delay-known','delay-default')),
 ADD COLUMN multicast_503_policy text CHECK (multicast_503_policy IN ('continue','delay-known','delay-minimum'));
