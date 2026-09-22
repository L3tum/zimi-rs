-- 017: extend the un-embedded partial index with `id` for the embed claim.
--
-- The embed batch-claim (`src/embed/pipeline.rs`, CLAIM_EMBED_BATCH_SQL)
-- selects, per 64-row batch:
--     WHERE zim_id = … AND embedding IS NULL … ORDER BY id LIMIT $2
-- over the still-un-embedded rows of a ZIM. The 002 index
-- (`idx_articles_unembedded_zim` on `(zim_id) WHERE embedding IS NULL`)
-- narrows the candidate set but carries no `id` order, so every claim
-- against a first-time backfill must scan AND sort the whole un-embedded
-- remainder — O(remaining) per batch, quadratic overall (hours of pure
-- claim overhead on a multi-million-article ZIM).
--
-- `(zim_id, id)` keeps the `zim_id` prefix that still serves
-- `embed::list_embeddable_zims`' `EXISTS (… zim_id … embedding IS NULL)`
-- probe, and its per-`zim_id` order IS the claim's `ORDER BY id`, so the
-- claim becomes an early-terminating index scan: O(batch) per claim
-- instead of O(remaining). The plan shape is pinned by
-- `tests/integration/embed_claim_plan.rs` (uses this index, no `Sort`, no
-- `Seq Scan on articles`).
--
-- Idempotent and applied in one transaction (the harness runs each file as
-- a single raw-string script): the drop + recreate is invisible to other
-- sessions until commit. The index only spans un-embedded rows, so the
-- rebuild is bounded by the backfill itself.
DROP INDEX IF EXISTS idx_articles_unembedded_zim;
CREATE INDEX IF NOT EXISTS idx_articles_unembedded_zim
    ON articles (zim_id, id)
    WHERE embedding IS NULL;
