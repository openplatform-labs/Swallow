-- Optional per-organization maximum. A missing row uses Novu's free-tier
-- 24-hour limit; NULL represents an unlimited tier.
CREATE TABLE IF NOT EXISTS novu_rust_organization_snooze_limits (
    organization_id text PRIMARY KEY,
    max_duration_ms bigint CHECK (max_duration_ms >= 0)
);
