-- btree for prefix LIKE 'q%' lookups (complements existing idx_articles_title_prefix)
CREATE INDEX IF NOT EXISTS idx_articles_title_lower ON articles (title_lower);

-- GiST trgm index for similarity() > threshold queries
CREATE INDEX IF NOT EXISTS idx_articles_title_gist ON articles USING GIST (title_lower gist_trgm_ops);
