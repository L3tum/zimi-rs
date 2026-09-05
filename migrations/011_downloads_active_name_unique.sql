-- BUG-3: at most one ACTIVE row (queued/downloading) per download `name`.
--
-- A direct download's partial file is named `{name}.part` in the ZIM dir,
-- so two same-name active rows (different URLs) would fight over the same
-- partial file (and later resume from each other's bytes). A partial unique
-- constraint makes the second insert fail with 23505 (unique_violation),
-- which every insert site already handles (the API maps it to 409; the
-- OPDS/reconcile queue skips it). Terminal rows (complete/error/cancelled)
-- do not block re-downloading the same name.
--
-- Plain ALTER TABLE (non-CONCURRENTLY): the harness runs each migration file
-- inside ONE transaction, which rejects CONCURRENTLY (see 010 note; 003 is
-- the partial-unique precedent).
ALTER TABLE downloads ADD CONSTRAINT uq_downloads_active_name
    UNIQUE (name) WHERE status IN ('queued', 'downloading');
