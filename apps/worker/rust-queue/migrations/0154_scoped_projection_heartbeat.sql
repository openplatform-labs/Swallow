-- Hold the grant and policy locks until the heartbeat write commits.
CREATE FUNCTION public.novu_rust_scoped_projection_heartbeat(p_environment text, p_provider text, p_full_scan boolean)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
  IF NOT public.novu_rust_check_projection_policy(p_environment,p_provider,p_full_scan) THEN
    RAISE EXCEPTION 'projection heartbeat policy changed' USING ERRCODE='42501';
  END IF;
  IF p_provider='sendgrid' THEN
    INSERT INTO public.novu_rust_sendgrid_projection_heartbeat(environment_id,last_completed_at) VALUES (p_environment,clock_timestamp())
      ON CONFLICT(environment_id) DO UPDATE SET last_completed_at=EXCLUDED.last_completed_at;
  ELSIF p_provider='resend' THEN
    INSERT INTO public.novu_rust_resend_projection_heartbeat(environment_id,last_completed_at) VALUES (p_environment,clock_timestamp())
      ON CONFLICT(environment_id) DO UPDATE SET last_completed_at=EXCLUDED.last_completed_at;
  ELSIF p_provider='twilio' THEN
    INSERT INTO public.novu_rust_twilio_projection_heartbeat(environment_id,last_completed_at) VALUES (p_environment,clock_timestamp())
      ON CONFLICT(environment_id) DO UPDATE SET last_completed_at=EXCLUDED.last_completed_at;
  ELSE
    RAISE EXCEPTION 'invalid provider' USING ERRCODE='22023';
  END IF;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_scoped_projection_heartbeat(text,text,boolean) FROM PUBLIC;
