-- Prevent duplicate in-flight downloads of the same URL.
--
-- `add_download` is check-then-insert: two concurrent POSTs with the same
-- URL both pass the "is it already queued?" SELECT and both insert, causing
-- two direct downloads of the same file (double bandwidth, double install
-- work, potential rename race). A partial unique index over the *active*
-- statuses makes the second insert fail with 23505 (unique_violation),
-- which the handler maps to 409. Completed/errored/cancelled rows don't
-- block re-downloading the same URL.
CREATE UNIQUE INDEX IF NOT EXISTS idx_downloads_active_url
    ON downloads (url)
    WHERE status IN ('queued', 'downloading');
