-- Preserve audit history and existing function ACLs while allowing SMTP.
ALTER TABLE public.novu_rust_workflow_provider_audit DROP CONSTRAINT IF EXISTS novu_rust_workflow_provider_audit_previous_provider_check;
ALTER TABLE public.novu_rust_workflow_provider_audit DROP CONSTRAINT IF EXISTS novu_rust_workflow_provider_audit_new_provider_check;
ALTER TABLE public.novu_rust_workflow_provider_audit ADD CONSTRAINT novu_rust_workflow_provider_audit_previous_provider_check CHECK (previous_provider IN ('sendgrid','resend','smtp'));
ALTER TABLE public.novu_rust_workflow_provider_audit ADD CONSTRAINT novu_rust_workflow_provider_audit_new_provider_check CHECK (new_provider IN ('sendgrid','resend','smtp'));
CREATE OR REPLACE FUNCTION public.novu_rust_operator_jwt_workflow_value_valid(p_resource text,p_value jsonb)
RETURNS boolean LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog AS $$
DECLARE n integer;
BEGIN
 IF p_value IS NULL OR jsonb_typeof(p_value)<>'object' THEN RETURN false; END IF;
 SELECT count(*) INTO n FROM jsonb_object_keys(p_value);
 IF p_resource='email-provider' THEN
  RETURN n=1 AND COALESCE(jsonb_typeof(p_value->'provider')='string' AND p_value->>'provider' IN ('sendgrid','resend','smtp'),false);
 END IF;
 IF p_resource NOT IN ('email','sms','push','slack') THEN RETURN false; END IF;
 IF NOT (p_value ? 'content' AND p_value ? 'active') OR jsonb_typeof(p_value->'active')<>'boolean' OR jsonb_typeof(p_value->'content') NOT IN ('string','null') OR COALESCE(octet_length(p_value->>'content'),0)>40000 THEN RETURN false; END IF;
 IF p_resource='slack' THEN RETURN n=2; END IF;
 IF n<>3 OR NOT p_value ? 'title' OR jsonb_typeof(p_value->'title') NOT IN ('string','null') OR COALESCE(octet_length(p_value->>'title'),0)>40000 THEN RETURN false; END IF;
 IF p_resource='sms' THEN RETURN jsonb_typeof(p_value->'title')='null'; END IF;
 RETURN (jsonb_typeof(p_value->'title')='null')=(jsonb_typeof(p_value->'content')='null');
END; $$;
