-- Drop the unused `settings.description` column.
--
-- It was help text for a settings UI that never rendered it: no code path
-- reads or writes the column — the settings INSERTs use only
-- (key, value, category[, updated_at]) (src/settings/cache.rs) and the
-- settings SELECTs read (key, value) only. Removing the column is a pure
-- dead-weight sweep.

ALTER TABLE settings DROP COLUMN IF EXISTS description;

-- Reviewed and KEPT (documentation only — no DDL in this migration):
--
-- articles.namespace / articles_staging.namespace (001) — still WRITTEN by
-- the production bulk-insert pipeline (the COPY into articles_staging and
-- UPSERT_ARTICLES_FROM_STAGING_SQL in src/zim/index.rs), though never read.
-- Dropping them would force SQL changes across the indexing pipeline for
-- zero runtime benefit; recorded as a future cleanup candidate.
--
-- search_history / qid_cache (001) — already dropped by 002_fixes.sql
-- (search telemetry was never read; the passive qid_cache is superseded by
-- qid_index). Nothing left to drop; their former pkey names are gone with
-- the tables.
