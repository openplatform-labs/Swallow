-- Evaluate live evidence without changing cancellation or delivery records.
CREATE FUNCTION public.novu_rust_resend_cancel_evidence_health(p_env text)
RETURNS TABLE(conflicting bigint, ambiguous bigint)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 PERFORM public.novu_rust_assert_alert_view(p_env);
 RETURN QUERY SELECT count(*) FILTER (WHERE x.evidence='conflicting'),count(*) FILTER (WHERE x.evidence='ambiguous')
 FROM public.novu_rust_resend_cancel_requests r
 CROSS JOIN LATERAL (SELECT public.novu_rust_channel_result(r.environment_id,r.job_id,'resend')->>'cancellationEvidence' AS evidence) x
 WHERE r.environment_id=p_env AND r.state='confirmed';
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_resend_cancel_evidence_health(text) FROM PUBLIC;

CREATE FUNCTION public.novu_rust_resend_cancel_evidence_list(p_env text,p_after uuid)
RETURNS TABLE(request_id uuid,job_id uuid,evidence text)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
BEGIN
 PERFORM public.novu_rust_assert_alert_view(p_env);
 RETURN QUERY SELECT r.request_id,r.job_id,x.evidence
 FROM public.novu_rust_resend_cancel_requests r
 CROSS JOIN LATERAL (SELECT public.novu_rust_channel_result(r.environment_id,r.job_id,'resend')->>'cancellationEvidence' AS evidence) x
 WHERE r.environment_id=p_env AND r.state='confirmed' AND (p_after IS NULL OR r.request_id>p_after)
 AND x.evidence IN ('conflicting','ambiguous') ORDER BY r.request_id LIMIT 51;
END; $$;
REVOKE ALL ON FUNCTION public.novu_rust_resend_cancel_evidence_list(text,uuid) FROM PUBLIC;
