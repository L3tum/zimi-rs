-- H2 (2026-09 project-wide review): make torrent-based ZIM acquisition
-- opt-in on EXISTING deployments.
--
-- Fresh installs already pick up the new `SETTING_DEFS` seed default
-- (`torrent.enabled` = false, see `src/settings/defs.rs`); this migration
-- covers the other half: the settings cache only ever INSERTs *missing*
-- keys (`ON CONFLICT DO NOTHING` in `SettingsCache::reload` — existing
-- values are never overwritten), so without this flip an upgraded
-- deployment keeps its seed-era `true` row and the default-on
-- ingestion cluster stays live: auto-seed from any configured OPDS feed,
-- into the 512 GiB `downloads.max_bytes` budget, with digest-less direct
-- downloads accepted unverified (SEC M3a) and served to all readers.
--
-- One-time and unconditional: after this migration every deployment has
-- torrent acquisition OFF until the operator opts back in (settings UI or
-- `PUT /settings`). The qB endpoint configuration (`QBITTORRENT_URL` env /
-- `torrent.url` setting) is untouched — only the enable switch flips, so
-- opting in is a single settings change.
UPDATE settings
   SET value = 'false'::jsonb,
       updated_at = now()
 WHERE key = 'torrent.enabled';
