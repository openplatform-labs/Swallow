-- Slack decisions are separate from email/SMS/push and operational alert grants.
ALTER TABLE novu_rust_delivery_operator_grants DROP CONSTRAINT IF EXISTS novu_rust_delivery_operator_grants_decision_check;
ALTER TABLE novu_rust_delivery_operator_grants ADD CONSTRAINT novu_rust_delivery_operator_grants_decision_check CHECK (decision IN ('retry','accepted','alert_send','alert_retry','alert_accepted','slack_retry','slack_accepted'));
CREATE OR REPLACE FUNCTION public.novu_rust_lock_operator_grant(p_environment text, p_decision text)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
BEGIN
    IF p_decision NOT IN ('retry','accepted','alert_send','alert_retry','alert_accepted','slack_retry','slack_accepted') THEN RETURN false; END IF;
    PERFORM 1 FROM public.novu_rust_delivery_operator_grants
      WHERE environment_id=p_environment AND database_principal=session_user AND decision=p_decision FOR SHARE;
    RETURN FOUND;
END;
$$;
REVOKE ALL ON FUNCTION public.novu_rust_lock_operator_grant(text,text) FROM PUBLIC;
