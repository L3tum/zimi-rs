-- 013_downloads_status_updated_index.sql
--
-- Composite index supporting the poller's status-scoped scans:
--   * the bounded 10-minute error retry  (status = 'error' AND updated_at < …)
--   * the per-tick early-exit count      (status = … / status IN (…))
-- `status` is the equality predicate in every poller query; `updated_at`
-- follows for the range filter. Low-cardinality, so a plain b-tree on
-- (status, updated_at) is a good fit.
CREATE INDEX IF NOT EXISTS idx_downloads_status_updated
    ON downloads (status, updated_at);
