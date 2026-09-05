-- Seeding visibility: per-torrent ratio, upload rate, and seed count.
-- Adds the data backing the new 'seeding' status value (a free-form value
-- of downloads.status, NOT added to idx_downloads_active_url in 003).
ALTER TABLE downloads ADD COLUMN ratio REAL;
ALTER TABLE downloads ADD COLUMN up_speed_bps BIGINT;
ALTER TABLE downloads ADD COLUMN num_seeds BIGINT;
