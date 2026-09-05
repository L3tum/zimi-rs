-- Partial index to support fast COUNT(*) WHERE embedding IS NOT NULL
-- in vector_index_state(). Turns a full table scan into an index-only scan.
CREATE INDEX IF NOT EXISTS idx_articles_embedded ON articles (id) WHERE embedding IS NOT NULL;
