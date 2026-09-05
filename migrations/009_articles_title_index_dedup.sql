-- 001 created idx_articles_title_prefix (btree on title_lower); 008 added
-- the identical idx_articles_title_lower (plus the GiST trgm index, which
-- stays). Two identical btree indexes waste writes and confuse the planner.
DROP INDEX IF EXISTS idx_articles_title_lower;
