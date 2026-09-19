//! Content-integrity (SEC) tests: the 015 digest schema, the
//! `prior_observed_digest_*` identity-history helpers, and the
//! `set_content_sha256` provenance record.
//!
//! Each test runs in a **dedicated temporary database** (the private temp-DB
//! pattern of [`migrations`]) so the shared dev DB is untouched — the fresh
//! apply also proves migration 015 creates the columns/constraints from
//! scratch (Task F), not just idempotently no-ops on an already-migrated
//! database.

use super::common::*;
use super::migrations::{close_and_drop, create_temp_db};
use zimservice::db::raw;

/// Task F (schema): a fresh apply creates the 015 integrity surface —
/// `zims.content_sha256` / `zims.publisher_sha256` (nullable, 64-hex CHECK),
/// `zims.digest_drift` (NOT NULL DEFAULT FALSE), and `downloads.sha256`
/// (nullable, 64-hex CHECK) — and the CHECK constraints reject malformed
/// digests.
#[tokio::test]
async fn digest_schema_applies_fresh_and_enforces_format() {
    let (base_pool, _db_gate) = match pool_or_skip().await {
        Some(p) => p,
        None => return,
    };
    let (pool, name) = match create_temp_db(&base_pool).await {
        Some(t) => t,
        None => return,
    };
    run_migrations(&pool).await.expect("fresh apply");

    // Column shape: digest_drift is NOT NULL with default FALSE.
    let (not_null, default): (bool, Option<String>) = raw::fetch_optional(
        &pool,
        "SELECT is_nullable = 'NO', column_default::text
         FROM information_schema.columns
         WHERE table_name = 'zims' AND column_name = 'digest_drift'",
        |q| q,
    )
    .await
    .expect("probe digest_drift")
    .expect("digest_drift column must exist");
    assert!(not_null, "digest_drift must be NOT NULL");
    assert!(
        default
            .as_deref()
            .is_some_and(|d| d.to_uppercase().contains("FALSE")),
        "digest_drift must default to FALSE, got: {default:?}"
    );

    // The 64-hex CHECK constraints accept a well-formed digest and reject
    // the malformed shapes (uppercase, short, non-hex, empty). Seeding real
    // rows first: a zero-row UPDATE would never fire a CHECK.
    let bad_digests: [&str; 4] = [
        &"A".repeat(64), // uppercase (constraint pins lowercase)
        "abc",           // too short
        "zzz",           // non-hex
        "",              // empty
    ];
    let ok = "1".repeat(64);
    // Seed: one downloads row + one zims row to exercise each table's CHECK.
    raw::execute(
        &pool,
        "INSERT INTO downloads (name, url, status, sha256)
             VALUES ('digcheck', 'http://example.net/digcheck.zim', 'queued', $1)",
        |q| q.bind(&ok),
    )
    .await
    .expect("well-formed digest must be accepted (downloads)");
    raw::execute(
        &pool,
        "INSERT INTO zims (name, display_title, file_path, file_size, file_mtime, content_sha256, publisher_sha256)
             VALUES ('digcheck-zims', 'digcheck', '/nonexistent/digcheck.zim', 10, now(), $1, $2)",
        |q| q.bind(&ok).bind(&ok),
    )
    .await
    .expect("well-formed digest must be accepted (zims)");

    for (table, col, sql) in [
        (
            "downloads",
            "sha256",
            "UPDATE downloads SET sha256 = $1 WHERE name = 'digcheck'",
        ),
        (
            "zims",
            "content_sha256",
            "UPDATE zims SET content_sha256 = $1 WHERE name = 'digcheck-zims'",
        ),
        (
            "zims",
            "publisher_sha256",
            "UPDATE zims SET publisher_sha256 = $1 WHERE name = 'digcheck-zims'",
        ),
    ] {
        for bad in &bad_digests {
            let err = raw::execute(&pool, sql, |q| q.bind(bad))
                .await
                .expect_err("malformed digest must be rejected");
            assert!(
                matches!(err, zimservice::error::Error::Database(_)),
                "malformed digest {bad:?} must fail the CHECK on {table}.{col}, got: {err}"
            );
        }
    }

    // The valid values survived the failed updates (the CHECK rejects the
    // whole statement, not just the bad value).
    let still_ok: Option<String> = raw::fetch_scalar_optional(
        &pool,
        "SELECT sha256 FROM downloads WHERE name = 'digcheck'",
        |q| q,
    )
    .await
    .expect("read back")
    .unwrap();
    assert_eq!(
        still_ok.as_deref(),
        Some(ok.as_str()),
        "rejected updates must not clobber the value"
    );

    raw::execute(
        &pool,
        "DELETE FROM downloads WHERE name = 'digcheck'",
        |q| q,
    )
    .await
    .unwrap();
    raw::execute(
        &pool,
        "DELETE FROM zims WHERE name = 'digcheck-zims'",
        |q| q,
    )
    .await
    .unwrap();

    close_and_drop(&base_pool, &pool, &name).await;
}

/// `prior_observed_digest_by_url` (SEC identity history): returns the most
/// recent SETTLED observation for the identity (source URL), excluding the
/// calling row — unsettled rows are ignored, NULL-legacy rows are ignored,
/// other identities' rows are ignored, and no history is `None`.
#[tokio::test]
async fn prior_observed_digest_by_url_semantics() {
    let (base_pool, _db_gate) = match pool_or_skip().await {
        Some(p) => p,
        None => return,
    };
    let (pool, name) = match create_temp_db(&base_pool).await {
        Some(t) => t,
        None => return,
    };
    run_migrations(&pool).await.expect("fresh apply");
    use zimservice::db::downloads_lifecycle::prior_observed_digest_by_url;

    let d_old = "1".repeat(64);
    let d_new = "2".repeat(64);
    let url = "http://example.net/same.zim";
    let other = "http://example.net/other.zim";

    // (1) No history at all → None (first observation is never drift).
    assert!(
        prior_observed_digest_by_url(&pool, 1, url)
            .await
            .unwrap()
            .is_none(),
        "no settled rows → no previous observation"
    );

    // (2) A NULL-legacy settled row → still None (nothing comparable).
    let legacy: i32 = raw::fetch_scalar_optional(
        &pool,
        "INSERT INTO downloads (name, url, status)
             VALUES ('drift-legacy', $1, 'complete') RETURNING id",
        |q| q.bind(url),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        prior_observed_digest_by_url(&pool, legacy, url)
            .await
            .unwrap()
            .is_none(),
        "a NULL-legacy record is never a previous observation"
    );

    // (3) A settled row with a digest → Some(that digest) for a LATER row on
    // the same identity — newest first.
    let settled: i32 = raw::fetch_scalar_optional(
        &pool,
        "INSERT INTO downloads (name, url, status, sha256)
             VALUES ('drift-old', $1, 'complete', $2) RETURNING id",
        |q| q.bind(url).bind(&d_old),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        prior_observed_digest_by_url(&pool, legacy, url)
            .await
            .unwrap(),
        Some(d_old.clone()),
        "settled observation on the same identity is the previous digest"
    );

    // (4) The newest settled row wins (the identity re-published again;
    // newer id = newer observation).
    let _settled2: i32 = raw::fetch_scalar_optional(
        &pool,
        "INSERT INTO downloads (name, url, status, sha256)
             VALUES ('drift-newer', $1, 'seeding', $2) RETURNING id",
        |q| q.bind(url).bind(&d_new),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        prior_observed_digest_by_url(&pool, legacy, url)
            .await
            .unwrap(),
        Some(d_new.clone()),
        "the most recent settled observation is the one compared against"
    );

    // (5) The calling row is excluded (a re-finalize of itself is not drift).
    assert_eq!(
        prior_observed_digest_by_url(&pool, settled, url)
            .await
            .unwrap(),
        Some(d_new.clone()),
        "excludes the calling row, reports the other settled observation"
    );

    // (6) Unsettled rows are ignored (a stuck `downloading` row is not a
    // published version).
    raw::execute(
        &pool,
        "UPDATE downloads SET sha256 = $2 WHERE id = $1 AND status = 'downloading'",
        |q| q.bind(legacy).bind("9".repeat(64)),
    )
    .await
    .unwrap();
    assert_eq!(
        prior_observed_digest_by_url(&pool, legacy, url)
            .await
            .unwrap(),
        Some(d_new.clone()),
        "an unsettled row must not be treated as a previous observation"
    );
    // (The status update in (6) intentionally left the row 'downloading' —
    // the UPDATE matched zero rows and the legacy row is still complete/NULL.)

    // (7) A different identity's observation is invisible.
    assert!(
        prior_observed_digest_by_url(&pool, legacy, other)
            .await
            .unwrap()
            .is_none(),
        "another URL's observation is a different identity"
    );

    close_and_drop(&base_pool, &pool, &name).await;
}

/// `prior_observed_digest_by_hash` (torrent identity history): the torrent
/// path's twin of the URL semantics — most recent settled observation by
/// info-hash, excluding the calling row.
#[tokio::test]
async fn prior_observed_digest_by_hash_semantics() {
    let (base_pool, _db_gate) = match pool_or_skip().await {
        Some(p) => p,
        None => return,
    };
    let (pool, name) = match create_temp_db(&base_pool).await {
        Some(t) => t,
        None => return,
    };
    run_migrations(&pool).await.expect("fresh apply");
    use zimservice::db::downloads_lifecycle::prior_observed_digest_by_hash;

    let d1 = "3".repeat(64);
    let d2 = "4".repeat(64);
    let hash = "h".repeat(40);
    let url = format!("magnet:?xt=urn:btih:{hash}");

    // No history → None (first observation is never drift).
    assert!(
        prior_observed_digest_by_hash(&pool, 1, &hash)
            .await
            .unwrap()
            .is_none(),
        "no settled rows → no previous observation"
    );

    let id1: i32 = raw::fetch_scalar_optional(
        &pool,
        "INSERT INTO downloads (name, url, status, hash, sha256)
             VALUES ('thash1', $1, 'complete', $2, $3) RETURNING id",
        |q| q.bind(&url).bind(&hash).bind(&d1),
    )
    .await
    .unwrap()
    .unwrap();

    // The calling row is EXCLUDED: a single settled row is not its own
    // history.
    assert!(
        prior_observed_digest_by_hash(&pool, id1, &hash)
            .await
            .unwrap()
            .is_none(),
        "the calling row is excluded from its own history"
    );

    // A second settled completion of the same torrent sees the first one
    // (newest settled observation wins).
    let id2: i32 = raw::fetch_scalar_optional(
        &pool,
        "INSERT INTO downloads (name, url, status, hash, sha256)
             VALUES ('thash2', $1, 'seeding', $2, $3) RETURNING id",
        |q| q.bind(&url).bind(&hash).bind(&d2),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        prior_observed_digest_by_hash(&pool, id2, &hash)
            .await
            .unwrap(),
        Some(d1.clone()),
        "the newer row sees the older settled observation"
    );
    assert_eq!(
        prior_observed_digest_by_hash(&pool, id1, &hash)
            .await
            .unwrap(),
        Some(d2.clone()),
        "the older row sees the newer observation"
    );
    let _ = id2;
    close_and_drop(&base_pool, &pool, &name).await;
}

/// `set_content_sha256` (provenance record): the install path's write —
/// observed digest, verified publisher claim, and the drift flag — lands on
/// the ZIM row.
#[tokio::test]
async fn set_content_sha256_records_integrity_record() {
    let (base_pool, _db_gate) = match pool_or_skip().await {
        Some(p) => p,
        None => return,
    };
    let (pool, name) = match create_temp_db(&base_pool).await {
        Some(t) => t,
        None => return,
    };
    run_migrations(&pool).await.expect("fresh apply");
    let tmp = tempfile::tempdir().expect("tempdir");
    let zims = ZimManager::new(tmp.path().to_path_buf(), pool.clone());

    raw::execute(
        &pool,
        "INSERT INTO zims (name, display_title, file_path, file_size, file_mtime)
             VALUES ($1, 'integrity row', '/nonexistent/x.zim', 10, now())",
        |q| q.bind("integrity-row"),
    )
    .await
    .expect("seed zims row");

    let d = "5".repeat(64);
    let p = "6".repeat(64);
    zims.set_content_sha256("integrity-row", &d, Some(&p), true)
        .await
        .expect("record integrity");

    let (content, publisher, drift): (Option<String>, Option<String>, bool) = raw::fetch_optional(
        &pool,
        "SELECT content_sha256, publisher_sha256, digest_drift FROM zims WHERE name = $1",
        |q| q.bind("integrity-row"),
    )
    .await
    .expect("read back")
    .expect("row exists");
    assert_eq!(
        content.as_deref(),
        Some(d.as_str()),
        "observed digest recorded"
    );
    assert_eq!(
        publisher.as_deref(),
        Some(p.as_str()),
        "verified publisher claim recorded"
    );
    assert!(drift, "drift flag recorded");

    // A later install with no claim and no drift clears the flag but keeps
    // the observed digest (the flag is a current-state, not an append).
    let d2 = "7".repeat(64);
    zims.set_content_sha256("integrity-row", &d2, None, false)
        .await
        .expect("re-record");
    let (content, publisher, drift): (Option<String>, Option<String>, bool) = raw::fetch_optional(
        &pool,
        "SELECT content_sha256, publisher_sha256, digest_drift FROM zims WHERE name = $1",
        |q| q.bind("integrity-row"),
    )
    .await
    .expect("read back")
    .expect("row exists");
    assert_eq!(
        content.as_deref(),
        Some(d2.as_str()),
        "new observed digest replaces the old"
    );
    assert_eq!(
        publisher.as_deref(),
        Some(p.as_str()),
        "no claim → the previous verified claim is kept, not clobbered"
    );
    assert!(
        !drift,
        "drift flag is current-state: cleared by a clean install"
    );

    close_and_drop(&base_pool, &pool, &name).await;
}
