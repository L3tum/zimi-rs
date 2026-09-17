//! Settings auth-service integration tests (the DB-backed arms the unit
//! tests can't reach: `upgrade()`'s *successful* persistence).

use super::common::*;

/// Tests (2026-09 review round 2): `SettingsAuth::upgrade()`'s DB-write
/// **success** path — the unit test pins only the "DB fails, cache still
/// updates" arm (dead pool). Here a live pool must persist the freshly
/// hashed password to the `settings` row AND cache it (the cache update is
/// authoritative for subsequent auth either way).
#[tokio::test]
async fn upgrade_persists_hashed_password_to_settings_row() {
    let (pool, _db_gate) = match pool_or_skip().await {
        Some(p) => p,
        None => return,
    };
    run_migrations(&pool).await.expect("migrations");

    // Snapshot `access.admin_password` before anything touches it (the
    // `SettingsCache::load` below bulk-inserts the `""` default if the key
    // is absent, so the snapshot must precede the load) — restored at the
    // end so the shared dev DB's credential row is left as found (suite
    // convention; see `update_torrent_url_batch_enables_flag_in_same_save`).
    let original: Option<String> = zimservice::db::raw::fetch_scalar_optional(
        &pool,
        "SELECT value::text FROM settings WHERE key = 'access.admin_password'",
        |q| q,
    )
    .await
    .expect("settings row snapshot");

    let settings = SettingsCache::load(pool.clone(), HashMap::new(), HashMap::new())
        .await
        .expect("settings load");

    let legacy = "legacy-pw";
    let hashed = zimservice::settings::hash_admin_password(legacy);

    // Success arm: the UPDATE commits and the stored row carries the hash.
    settings.auth().upgrade(hashed.clone()).await;
    let stored: Option<serde_json::Value> = zimservice::db::raw::fetch_scalar_optional(
        &pool,
        "SELECT value FROM settings WHERE key = 'access.admin_password'",
        |q| q,
    )
    .await
    .expect("settings row query")
    .expect("settings row present");
    assert_eq!(
        stored.as_ref(),
        Some(&serde_json::Value::String(hashed.clone())),
        "upgrade() must persist the hashed password to the settings row"
    );

    // The cache holds the same hash (subsequent auth verifies against it).
    assert!(settings.auth().verify(legacy), "new password verifies");
    assert!(!settings.auth().verify("not-the-password"));

    // Idempotent second upgrade (a restart would re-run the transparent
    // upgrade once; the write must succeed again, not 500).
    settings.auth().upgrade(hashed.clone()).await;
    let stored2: Option<serde_json::Value> = zimservice::db::raw::fetch_scalar_optional(
        &pool,
        "SELECT value FROM settings WHERE key = 'access.admin_password'",
        |q| q,
    )
    .await
    .expect("settings row query")
    .expect("settings row present");
    assert_eq!(stored2.as_ref(), Some(&serde_json::Value::String(hashed)));

    // Restore the row to its pre-test state (suite convention: no test
    // leaves a shared-fixture mutation behind).
    match original {
        Some(v) => {
            zimservice::db::raw::execute(
                &pool,
                "UPDATE settings SET value = $1::jsonb WHERE key = 'access.admin_password'",
                |q| q.bind(v),
            )
            .await
            .expect("settings row restore");
        }
        None => {
            zimservice::db::raw::execute(
                &pool,
                "DELETE FROM settings WHERE key = 'access.admin_password'",
                |q| q,
            )
            .await
            .expect("settings row restore");
        }
    }
}
