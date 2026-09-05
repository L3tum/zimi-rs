-- Migrate the articles embedding index from the legacy NON-partial shape
-- (built over all rows, including the large NULL-embedding majority) to the
-- partial shape the runtime builds at build time
-- (src/embed/mod.rs `maybe_build_vector_index`).
--
-- Why partial:
--  * The old non-partial index keeps entries for rows with embedding IS NULL
--    (the common case for most deployments) — a large, useless index that
--    bloats every write on articles.
--  * A non-partial HNSW/IVFFlat index can only be created when the table has
--    no NULLs in the indexed column, so it either silently blocks index
--    creation or indexes garbage.
--
-- The DO block runs in the same transaction as the tracking INSERT (the
-- migration harness batch_executes the whole file), and plain (non-
-- CONCURRENTLY) CREATE INDEX is transaction-safe.

DO $$
DECLARE
    n_vectors BIGINT;
    has_legacy_non_partial BOOLEAN;
BEGIN
    SELECT count(*) INTO n_vectors FROM articles WHERE embedding IS NOT NULL;
    SELECT EXISTS (
        SELECT 1 FROM pg_index i
        JOIN pg_class c ON c.oid = i.indexrelid
        WHERE c.relname = 'idx_articles_embedding' AND i.indpred IS NULL
    ) INTO has_legacy_non_partial;

    -- Drop a legacy NON-partial index (built by older runtimes over all rows).
    IF has_legacy_non_partial THEN
        DROP INDEX idx_articles_embedding;
    END IF;

    -- Recreate the partial shape inline only at HNSW-safe scale. At/above
    -- EMBED_DEFAULT_HNSW_THRESHOLD (1_000_000) let the runtime rebuild with
    -- the strategy it would choose (ivfflat branch of maybe_build_vector_index;
    -- count >= 10k passes the should_spawn_build gate in auto_embed_loop).
    -- Never create HNSW inline for >= 1M vectors (memory/latency regression
    -- vs the ivfflat the runtime would pick, and the rebuild gate is
    -- exists-gated so it would never re-evaluate).
    IF n_vectors < 1000000 THEN
        CREATE INDEX IF NOT EXISTS idx_articles_embedding
            ON articles USING hnsw (embedding vector_cosine_ops)
            WHERE embedding IS NOT NULL;
    END IF;
END $$;

-- P7: `zimservice list --sort` orders by created_at; add the missing index.
CREATE INDEX IF NOT EXISTS idx_downloads_created_at ON downloads (created_at);
