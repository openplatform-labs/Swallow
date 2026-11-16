-- Existing list grant now also allows pre-dispatch diagnostics.
CREATE FUNCTION public.novu_rust_operator_jwt_telegram_predispatch(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_after uuid)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE result jsonb;
BEGIN
 PERFORM public.novu_rust_operator_jwt_telegram_resolution_access(p_issuer,p_subject,p_client,p_env,p_exp,'list');
 result:=public.novu_rust_telegram_predispatch_list(p_env,p_after);
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 RETURN result;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_telegram_predispatch(text,text,text,text,bigint,uuid) FROM PUBLIC;
