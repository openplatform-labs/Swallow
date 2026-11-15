CREATE FUNCTION public.novu_rust_operator_jwt_discord_failure_lookup(p_issuer text,p_subject text,p_client text,p_env text,p_exp bigint,p_job uuid)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE permitted boolean;v_row record;v_found boolean;
BEGIN
 IF p_job IS NULL THEN RAISE EXCEPTION 'invalid Discord lookup' USING ERRCODE='22023'; END IF;
 PERFORM public.novu_rust_operator_jwt_self(p_issuer,p_subject,p_client,p_env,p_exp);
 SELECT can_list INTO permitted FROM public.novu_rust_operator_jwt_discord_review_grants g WHERE g.issuer=p_issuer AND g.subject=p_subject AND g.client_id=p_client AND g.environment_id=p_env FOR SHARE OF g;
 IF NOT coalesce(permitted,false) OR to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 SELECT * INTO v_row FROM public.novu_rust_discord_failure_lookup(p_env,p_job);v_found:=FOUND;
 IF to_timestamp(p_exp::double precision)<=clock_timestamp() THEN RAISE EXCEPTION 'operator access denied' USING ERRCODE='42501'; END IF;
 IF NOT v_found THEN RETURN jsonb_build_object('found',false); END IF;
 RETURN jsonb_build_object('found',true,'jobId',v_row.job_id,'kind',v_row.kind,'status',v_row.status,'attemptState',v_row.attempt_state,'attemptRevision',v_row.attempt_revision,'failureRevision',v_row.failure_revision::text,'attempts',v_row.attempts,'httpStatus',v_row.http_status,'retryAfterSeconds',v_row.retry_after_seconds,'retryNotBefore',v_row.retry_not_before,'reviewed',v_row.reviewed);
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_operator_jwt_discord_failure_lookup(text,text,text,text,bigint,uuid) FROM PUBLIC;
