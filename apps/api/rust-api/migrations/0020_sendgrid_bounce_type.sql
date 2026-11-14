-- A legacy bounce did not preserve its type and cannot be reclassified safely.
ALTER TABLE public.novu_rust_sendgrid_delivery_events ADD COLUMN IF NOT EXISTS bounce_type text;
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_constraint WHERE conname='novu_rust_sendgrid_bounce_type_check' AND conrelid='public.novu_rust_sendgrid_delivery_events'::regclass) THEN
    ALTER TABLE public.novu_rust_sendgrid_delivery_events ADD CONSTRAINT novu_rust_sendgrid_bounce_type_check CHECK (bounce_type IS NULL OR (event_type='bounce' AND bounce_type IN ('bounce','blocked')));
  END IF;
END $$;
