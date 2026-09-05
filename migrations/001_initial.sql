-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS vector;  -- pgvector

-- ZIM archive metadata
CREATE TABLE zims (
    id              SERIAL PRIMARY KEY,
    name            TEXT NOT NULL UNIQUE,       -- filename without .zim
    display_title   TEXT NOT NULL,             -- human-readable from ZIM metadata
    description     TEXT,
    language        TEXT NOT NULL DEFAULT 'en',-- ISO 639-1 code
    creator         TEXT,
    publisher       TEXT,
    date            DATE,                       -- ZIM publication date
    entry_count     BIGINT NOT NULL DEFAULT 0,  -- total entries (includes X/, M/)
    article_count   BIGINT NOT NULL DEFAULT 0,  -- namespace C entries only
    file_path       TEXT NOT NULL,             -- absolute path to .zim file
    file_size       BIGINT NOT NULL,           -- bytes
    uuid            TEXT,                      -- ZIM UUID (content-address)
    category        TEXT,                      -- auto-categorized
    -- Indexing state
    index_status    TEXT NOT NULL DEFAULT 'pending',  -- pending|indexing|ready|error
    index_progress  REAL NOT NULL DEFAULT 0.0,        -- 0.0 to 1.0
    indexed_entries BIGINT NOT NULL DEFAULT 0,
    indexed_at      TIMESTAMPTZ,
    embed_status    TEXT NOT NULL DEFAULT 'none',      -- none|pending|processing|ready
    embed_progress  REAL NOT NULL DEFAULT 0.0,
    -- Per-ZIM settings (managed via UI settings page)
    embed_enabled   BOOLEAN NOT NULL DEFAULT TRUE,  -- allow embeddings for this ZIM
    file_mtime      TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_zims_lang ON zims(language);
CREATE INDEX idx_zims_category ON zims(category);
CREATE INDEX idx_zims_status ON zims(index_status);

-- Articles (the main search table)
-- NOTE: Uses 'simple' text search config for multi-language support.
-- 'simple' = lowercase + tokenization only (no stemming).
-- Tradeoff: "running" won't match "run", but we gain cross-language
-- same-word search and a much smaller GIN index. pg_trgm handles fuzzy.
CREATE TABLE articles (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    zim_id          INT NOT NULL REFERENCES zims(id) ON DELETE CASCADE,
    path            TEXT NOT NULL,             -- ZIM entry path (e.g. "A/Water")
    title           TEXT NOT NULL,             -- article title
    title_lower     TEXT NOT NULL GENERATED ALWAYS AS (lower(title)) STORED,
    content_preview TEXT,                      -- first ~2000 chars of extracted text
    snippet         TEXT NOT NULL DEFAULT '',  -- pre-generated snippet (~300 chars)
    -- Full article content is NOT stored in DB — read from ZIM file via memmap when needed
    -- Full-text search (pre-computed in Rust during indexing)
    search_vector   TSVECTOR NOT NULL,
    -- Language / categorization
    language        TEXT NOT NULL DEFAULT 'en',
    namespace       TEXT NOT NULL DEFAULT 'C', -- ZIM namespace
    -- Embeddings
    embedding       VECTOR(1536),             -- dimension configurable
    embed_model     TEXT,
    embed_at        TIMESTAMPTZ,
    -- Metadata
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (zim_id, path)
);

-- GIN index for full-text search
CREATE INDEX idx_articles_fts ON articles USING GIN(search_vector);
-- GIN index for trigram (fuzzy, prefix, similarity)
CREATE INDEX idx_articles_title_trgm ON articles USING GIN(title_lower gin_trgm_ops);
-- B-tree for exact prefix (fast single-word autocomplete)
CREATE INDEX idx_articles_title_prefix ON articles(title_lower);
-- Composite filter index (for scoped searches)
CREATE INDEX idx_articles_zim_lang ON articles(zim_id, language);
-- Vector index (build ONLY when embedded count < threshold, see notes)
-- For < 1M vectors: HNSW (fast, high recall, ~8GB RAM for 1M×1536)
-- For 1M-10M: IVFFlat (lower RAM, slightly lower recall)
-- For > 10M: don't build — vector search degrades; rely on FTS+trgm
-- CREATE INDEX idx_articles_embedding ON articles USING hnsw (embedding vector_cosine_ops);
-- CREATE INDEX idx_articles_embedding ON articles USING ivfflat (embedding vector_cosine_ops) WITH (lists = 4000);

-- Staging table for COPY-based bulk inserts
CREATE UNLOGGED TABLE articles_staging (
    path            TEXT NOT NULL,
    title           TEXT NOT NULL,
    content_preview TEXT,
    snippet         TEXT NOT NULL DEFAULT '',
    language        TEXT NOT NULL DEFAULT 'en',
    namespace       TEXT NOT NULL DEFAULT 'C',
    zim_id          INT NOT NULL
);

-- Search history
CREATE TABLE search_history (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    query           TEXT NOT NULL,
    zim_ids         INT[],
    result_count    INT,
    elapsed_ms      INT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Collections (user-defined groups of ZIMs)
CREATE TABLE collections (
    id              SERIAL PRIMARY KEY,
    name            TEXT NOT NULL UNIQUE,
    label           TEXT NOT NULL,
    zim_ids         INT[] NOT NULL DEFAULT '{}',
    is_favorite     BOOLEAN NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Auth: either OIDC or a simple admin password (no in-between user management)
-- If AUTH_MODE=oidc: users authenticate via OIDC provider, no local user table needed
-- If AUTH_MODE=password: single shared admin password (in settings table)
-- No user accounts table — access is either open, password-gated, or OIDC-verified

-- Q-ID index (Wikidata cross-language article linking)
-- Maps article paths to their Wikidata Q-ID (e.g., Q1468 = Paris)
-- Enables: "find the French article for this English article" and almanac deep-links
CREATE TABLE qid_index (
    zim_id          INT NOT NULL REFERENCES zims(id) ON DELETE CASCADE,
    path            TEXT NOT NULL,             -- ZIM article path
    qid             BIGINT NOT NULL,           -- Wikidata Q-ID as integer (e.g., 1468 for Q1468)
    PRIMARY KEY (zim_id, path)
);
CREATE INDEX idx_qid_lookup ON qid_index(qid, zim_id);

-- Q-ID passive cache (populated on article view for large ZIMs where full scan is impractical)
CREATE TABLE qid_cache (
    zim_id          INT NOT NULL REFERENCES zims(id) ON DELETE CASCADE,
    path            TEXT NOT NULL,
    qid             BIGINT NOT NULL,
    PRIMARY KEY (zim_id, path)
);
CREATE INDEX idx_qid_cache_lookup ON qid_cache(qid, zim_id);

-- Server settings (key-value, managed via UI settings page)
CREATE TABLE settings (
    key             TEXT PRIMARY KEY,
    value           JSONB NOT NULL,
    description     TEXT,                    -- help text for the UI
    category        TEXT NOT NULL DEFAULT 'general',  -- general|search|torrent|embedding|access
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Downloads tracking
CREATE TABLE downloads (
    id              SERIAL PRIMARY KEY,
    name            TEXT NOT NULL,
    url             TEXT NOT NULL,
    hash            TEXT,                      -- torrent info_hash or NULL for direct
    status          TEXT NOT NULL DEFAULT 'queued',  -- queued|downloading|complete|error|cancelled
    progress        REAL NOT NULL DEFAULT 0.0,
    speed_bps       BIGINT,
    eta_secs        BIGINT,
    file_path       TEXT,
    error           TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
