-- Identify the original generated single-column constraint without touching custom checks.
DO $$ DECLARE c record; matched integer := 0; BEGIN
  FOR c IN SELECT conname FROM pg_constraint
    WHERE conrelid='public.novu_rust_push_workflow_worker_config'::regclass
      AND contype='c' AND conkey=ARRAY[(SELECT attnum FROM pg_attribute WHERE attrelid='public.novu_rust_push_workflow_worker_config'::regclass AND attname='multicast_503_policy')]::smallint[]
      AND pg_get_constraintdef(oid) LIKE '%delay-known%'
      AND pg_get_constraintdef(oid) LIKE '%delay-minimum%'
      AND pg_get_constraintdef(oid) LIKE '%continue%'
  LOOP
    matched := matched + 1;
    IF matched > 1 THEN RAISE EXCEPTION 'ambiguous FCM 503 policy constraint'; END IF;
    EXECUTE format('ALTER TABLE public.novu_rust_push_workflow_worker_config DROP CONSTRAINT %I',c.conname);
  END LOOP;
  IF matched <> 1 THEN RAISE EXCEPTION 'missing FCM 503 policy constraint'; END IF;
END $$;
ALTER TABLE public.novu_rust_push_workflow_worker_config ADD CONSTRAINT novu_fcm_multicast_503_policy_check
 CHECK (multicast_503_policy IN ('continue','delay-known','delay-minimum','exponential'));
