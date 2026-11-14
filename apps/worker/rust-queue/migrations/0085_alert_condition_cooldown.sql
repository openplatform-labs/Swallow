-- Optional cooldown bypass for newly observed alert conditions.
ALTER TABLE public.novu_rust_alert_delivery_slots ADD COLUMN IF NOT EXISTS last_condition_mask integer;
