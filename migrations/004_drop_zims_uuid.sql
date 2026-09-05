-- Drop the unused `zims.uuid` column.
--
-- `ZimMeta.uuid` was always `None` (never populated from the ZIM file) and was
-- never read for any logic — only written/round-tripped through the `zims`
-- table. Removing the column (and the Rust field + SQL plumbing) is a pure
-- dead-weight sweep.

ALTER TABLE zims DROP COLUMN IF EXISTS uuid;
