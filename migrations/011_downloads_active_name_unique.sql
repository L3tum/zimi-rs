-- BUG-3: at most one ACTIVE row (queued/downloading) per download `name`.
--
-- A direct download's partial file is named `{name}.part` in the ZIM dir,
-- so two same-name active rows (different URLs) would fight over the same
-- partial file (and later resume from each other's bytes). A partial unique
-- index makes the second insert fail with 23505 (unique_violation),
-- which every insert site already handles (the API maps it to 409; the
-- OPDS/reconcile queue skips it). Terminal rows (complete/error/cancelled)
-- do not block re-downloading the same name.
--
-- A `WHERE` (partial) predicate is only legal on `CREATE INDEX`, never on
-- `ADD CONSTRAINT ... UNIQUE (...)` — so this is a partial UNIQUE INDEX,
-- exactly the 003 `idx_downloads_active_url` precedent. Non-CONCURRENTLY so
-- it stays valid inside the harness's single-transaction apply. The index
-- name is `uq_downloads_active_name` because `error.rs::duplicate_field`
-- maps that name to the `name` field for the 409 response.
CREATE UNIQUE INDEX IF NOT EXISTS uq_downloads_active_name
    ON downloads (name)
    WHERE status IN ('queued', 'downloading');
