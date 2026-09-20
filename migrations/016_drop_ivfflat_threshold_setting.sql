-- Remove the dead `embedding.ivfflat_threshold` setting row (2026-09-18
-- review, Ponytail #1: "dead setting with a misleading UI").
--
-- The key was read in exactly one place — a `tracing::info!` log branch in
-- `maybe_build_vector_index` — and never influenced a decision: the index
-- kind is chosen solely by `embedding.hnsw_threshold` (`index_build_sql`:
-- HNSW below, IVFFlat at/above), so the setting "never suppressed a build"
-- (its own code docs admitted this) while the settings UI advertised it as
-- "Use IVFFlat index above this (less RAM)". The key, its UI field, the log
-- branch, and the `SettingDef` entry are deleted; this migration removes the
-- orphaned row from existing databases (the settings cache only ever
-- INSERTs missing defaults — it never deletes rows, so without this the
-- dead key would linger in `settings` forever on upgraded deployments).

DELETE FROM settings WHERE key = 'embedding.ivfflat_threshold';
