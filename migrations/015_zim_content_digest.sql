-- SEC: ZIM content integrity, version-keyed digests (2026-09 review:
-- "the ZIM source IS the content trust boundary"; operator rejected pinning
-- to fixed digests, so the digest is always the one the source is CURRENTLY
-- publishing for this name/URL).
--
--   downloads.sha256 — version-keyed, two flavors:
--     * OPDS direct URLs: the SHA-256 the catalog declared on the acquire
--       link (hex `hash` or base64 `digest` attribute, when present) at
--       enqueue time — the publisher's claim, verified at finalize.
--     * completion (both paths): the self-recorded SHA-256 of the installed
--       bytes (direct + torrent — torrents pin their content by the
--       BitTorrent info-hash in `hash`, so the recorded digest is a
--       provenance record + drift baseline, not a claim).
--
--   zims.content_sha256 — SHA-256 of the installed bytes (computed at
--   install time on both paths; NULL only for pre-feature installs).
--   zims.publisher_sha256 — the catalog claim that was VERIFIED at install
--   time (only set when the source declared one; then equal to
--   content_sha256).
--   zims.digest_drift — a NEW download of the SAME identity (same source
--   URL / same torrent info-hash) produced bytes whose observed digest
--   differs from the previously observed digest for that identity. Flag,
--   never reject: a publisher republishing the same URL is a human
--   judgment call (the fetch-time claim mismatch is what auto-rejects).
--   First observation for an identity (or a NULL legacy record) is never
--   drift; a new identity under the same name resets the flag.
--
-- Version-key semantics: a NEW digest published under the same URL is a new
-- version, not a violation — the queue guard re-queues it (see
-- queue_opds_updates), and the old completed row stays as history.
--
-- CHECK constraints pin the format (64 lowercase hex chars, or NULL =
-- "no claim") so a malformed value can never silently disable the
-- mismatch check at finalize time.

ALTER TABLE downloads ADD COLUMN IF NOT EXISTS sha256 TEXT;
ALTER TABLE downloads ADD CONSTRAINT chk_downloads_sha256
    CHECK (sha256 IS NULL OR sha256 ~ '^[0-9a-f]{64}$');
COMMENT ON COLUMN downloads.sha256 IS
    'SHA-256 the publisher declared at enqueue (OPDS direct) or of the installed bytes, self-recorded at completion (direct + torrent); NULL when the source declared nothing or the row predates the feature';

ALTER TABLE zims ADD COLUMN IF NOT EXISTS content_sha256 TEXT;
ALTER TABLE zims ADD CONSTRAINT chk_zims_content_sha256
    CHECK (content_sha256 IS NULL OR content_sha256 ~ '^[0-9a-f]{64}$');
COMMENT ON COLUMN zims.content_sha256 IS
    'SHA-256 of the installed .zim bytes, measured at install time on both paths (NULL only for pre-feature installs)';

ALTER TABLE zims ADD COLUMN IF NOT EXISTS publisher_sha256 TEXT;
ALTER TABLE zims ADD CONSTRAINT chk_zims_publisher_sha256
    CHECK (publisher_sha256 IS NULL OR publisher_sha256 ~ '^[0-9a-f]{64}$');
COMMENT ON COLUMN zims.publisher_sha256 IS
    'catalog-declared SHA-256 verified against the installed bytes at install time (NULL = source declared none — structural check only)';

-- Per-ZIM drift flag (see header): set by the install paths when a new
-- download of the same identity yields different bytes than the identity's
-- previously observed digest. NOT NULL DEFAULT FALSE: pre-feature rows are
-- "not drift" — absence of integrity records is the NULL state above, a
-- different (older) condition, and must not be confused with a cleared flag.
ALTER TABLE zims ADD COLUMN IF NOT EXISTS digest_drift BOOLEAN NOT NULL DEFAULT FALSE;
COMMENT ON COLUMN zims.digest_drift IS
    'same identity (source URL / torrent info-hash) served different bytes than its previously observed digest at a later install — flagged, never auto-rejected';
