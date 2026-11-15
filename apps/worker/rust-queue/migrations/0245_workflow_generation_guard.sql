-- Held through the parent job transaction. Only trusted worker logins should
-- receive EXECUTE; the owner needs the workflow row-lock privilege.
CREATE FUNCTION public.novu_rust_lock_workflow_generation(p_environment text,p_workflow text,p_expected text)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE actual text;
BEGIN
 IF p_environment IS NULL OR p_workflow IS NULL OR p_expected IS NULL OR octet_length(p_expected) NOT BETWEEN 1 AND 512 THEN RAISE EXCEPTION 'invalid workflow generation' USING ERRCODE='22023'; END IF;
 SELECT w.inbox_metadata->>'id' INTO actual FROM public.novu_rust_workflows w WHERE w.environment_id=p_environment AND w.identifier=p_workflow FOR SHARE OF w;
 RETURN FOUND AND actual IS NOT DISTINCT FROM p_expected;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_lock_workflow_generation(text,text,text) FROM PUBLIC;
