-- 002: post-initialisation schema fixes.
--
-- Safe to run on any database (uses IF EXISTS / IF NOT EXISTS). Each DDL
-- statement is idempotent, and the whole file is applied as one transaction
-- by the migration runner.

-- Unused: search telemetry was written fire-and-forget and never read.
DROP TABLE IF EXISTS search_history;

-- Unused: the passive qid_cache is superseded by qid_index (built in full at
-- index time). Dropping the table drops its indexes too.
DROP TABLE IF EXISTS qid_cache;

-- Embedding state is tracked per article row (articles.embedding /
-- articles.embed_at), not per-ZIM progress counters.
ALTER TABLE zims DROP COLUMN IF EXISTS embed_status;
ALTER TABLE zims DROP COLUMN IF EXISTS embed_progress;

-- Serves embed::list_embeddable_zims ("which ZIMs still have >=1 un-embedded
-- article") without scanning the embedding column: a partial index over just
-- the un-embedded rows, keyed by zim_id.
CREATE INDEX IF NOT EXISTS idx_articles_unembedded_zim
    ON articles (zim_id)
    WHERE embedding IS NULL;
