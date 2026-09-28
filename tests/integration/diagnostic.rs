//! `GET /diagnostic` payload-contract integration test (M-diag, 2026-10
//! review).
//!
//! The route is admin-authenticated (it names internal config keys) and
//! returns the operator-facing diagnostics that `/health` intentionally
//! omits. This test pins the DOCUMENTED payload contract — the fields an
//! operator's tooling may rely on — and never the internal structure of
//! the probes that build it:
//!
//! Always present (the contract):
//! - `version` (string, non-empty)
//! - `pool` / `pool_bg` (object: `size`, `idle`, `max_size`, `checked_out`
//!   — non-negative integers, with `checked_out == size - idle`)
//! - `checkout_wait` (object: `count`, `max_us`, `avg_us` — non-negative
//!   integers)
//! - `query_embed_cache_entries` (non-negative integer)
//!
//! Conditional (the contract):
//! - `settings_mismatches` — array of strings, omitted when every stored
//!   value deserializes; the key appears after a wrong-typed value is
//!   written (asserted here via direct SQL + reload)
//! - `pool_read` — pool-shaped object, omitted when no read replica
//! - `vector_index` — object with `embedded_rows` (integer), `index`
//!   (one of `absent`/`present`/`present_invalid`), `count_at` (integer);
//!   `degraded` (string) present only when the index is not `present` and
//!   `embedded_rows >= 10 000`; the whole field omitted when the catalog
//!   probe fails. The ≥10k `degraded` branch is not seeded here (10k real
//!   embeddings are not cheap) — its threshold logic is unit-tested in
//!   `src/embed/vector_index.rs`.
//! - `content_integrity` — object with `zims` (array of rows),
//!   `publisher_verified` / `drift` (integers consistent with the rows);
//!   rows without any integrity record do not appear; omitted when the
//!   probe fails
//! - `notify` — object carrying a `state` field when a listener runs;
//!   omitted when none does (this test's `AppState` shape never spawns one)
//!
//! Two halves: a DB-less half (dead pool → the probe-omitted fields must
//! be absent, proving the omission contract without a database) and a
//! DB-gated half (live pool → the probe-backed fields must be present and
//! shaped). Neither half asserts beyond the documented contract.

use super::common::*;
use std::collections::HashMap;
use zimservice::settings::SettingsCache;

/// The test admin password (in-memory / env-snapshot only — never written
/// to the shared suite DB; the password-mode rows live in the cache).
const TEST_ADMIN_PW: &str = "diag-contract-test-pw";

/// A well-formed 64-char lowercase hex digest (the `zims.content_sha256`
/// CHECK constraint).
const DIGEST: &str = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef";

/// Assert one pool-shaped object (`pool`, `pool_bg`, `pool_read`) honors
/// the documented contract: four non-negative integer fields with
/// `checked_out == size - idle`.
fn assert_pool_shape(obj: &serde_json::Value, label: &str) {
    assert!(obj.is_object(), "{label} must be an object: {obj}");
    for field in ["size", "idle", "max_size", "checked_out"] {
        let v = &obj[field];
        assert!(
            v.is_u64(),
            "{label}.{field} must be a non-negative integer: {v}"
        );
    }
    let size = obj["size"].as_u64().unwrap();
    let idle = obj["idle"].as_u64().unwrap();
    let checked_out = obj["checked_out"].as_u64().unwrap();
    assert_eq!(
        checked_out,
        size - idle,
        "{label}: checked_out must be size - idle ({size} - {idle})"
    );
    assert!(
        idle <= size,
        "{label}: idle ({idle}) cannot exceed size ({size})"
    );
    assert!(
        obj["max_size"].as_u64().unwrap() > 0,
        "{label}.max_size must be a positive integer"
    );
}

/// Assert the always-present contract fields on a `/diagnostic` payload.
fn assert_always_present(payload: &serde_json::Value) {
    // `version` — non-empty string.
    assert!(
        payload["version"].is_string() && !payload["version"].as_str().unwrap().is_empty(),
        "version must be a non-empty string: {}",
        &payload["version"]
    );
    // `pool` / `pool_bg` — always present, pool-shaped.
    assert_pool_shape(&payload["pool"], "pool");
    assert_pool_shape(&payload["pool_bg"], "pool_bg");
    // `checkout_wait` — always present: count / max_us / avg_us integers.
    let cw = &payload["checkout_wait"];
    assert!(cw.is_object(), "checkout_wait must be an object: {cw}");
    for field in ["count", "max_us", "avg_us"] {
        assert!(
            cw[field].is_u64(),
            "checkout_wait.{field} must be a non-negative integer: {}",
            &cw[field]
        );
    }
    // `by_site` — the documented omission rule: absent until the first
    // explicit checkout (a zero-count bucket has no per-site rows).
    if cw["count"].as_u64() == Some(0) {
        assert!(
            cw.get("by_site").is_none_or(serde_json::Value::is_null),
            "checkout_wait.by_site must be omitted until the first explicit \
             checkout: {cw}"
        );
    }
    // If present (count > 0), an array of `{site, count, max_us, avg_us}`.
    if let Some(by_site) = cw.get("by_site") {
        assert!(
            by_site.as_array().is_some_and(|a| {
                a.iter().all(|row| {
                    row["site"].is_string()
                        && row["count"].is_u64()
                        && row["max_us"].is_u64()
                        && row["avg_us"].is_u64()
                })
            }),
            "checkout_wait.by_site must be an array of per-site rows: {by_site}"
        );
    }
    // `query_embed_cache_entries` — always present, non-negative integer.
    assert!(
        payload["query_embed_cache_entries"].is_u64(),
        "query_embed_cache_entries must be a non-negative integer: {}",
        &payload["query_embed_cache_entries"]
    );
    // `settings_mismatches` — if present, an array of strings.
    if let Some(mm) = payload.get("settings_mismatches") {
        assert!(
            mm.as_array()
                .is_some_and(|a| a.iter().all(|v| v.is_string())),
            "settings_mismatches must be an array of strings: {mm}"
        );
    }
}

/// Assert the probe-backed conditional fields honor the contract when
/// present (and the known-absent ones stay absent in this topology).
fn assert_conditional(payload: &serde_json::Value, db_up: bool) {
    // `pool_read` — this test never configures a read replica.
    assert!(
        payload.get("pool_read").is_none_or(|v| v.is_null()),
        "pool_read must be omitted without a read replica: {}",
        &payload["pool_read"]
    );
    // `notify` — this test never spawns a listener (`assemble_state` shape:
    // `notify: None`); an env with a running listener would surface an
    // object carrying `state` — either shape is contract-valid, and this
    // topology deterministically produces the omission.
    match payload.get("notify") {
        None | Some(serde_json::Value::Null) => {}
        Some(v) => assert!(
            v.get("state").is_some_and(|s| s.is_string()),
            "notify, when present, must carry a `state` string: {v}"
        ),
    }
    // `vector_index` — present iff the catalog probe ran (db_up).
    let vi = payload.get("vector_index");
    if db_up {
        assert!(
            vi.is_some_and(|v| v.is_object()),
            "vector_index must be present with a live DB: {vi:?}"
        );
        let vi = vi.unwrap();
        assert!(
            vi["embedded_rows"].is_i64() || vi["embedded_rows"].is_u64(),
            "vector_index.embedded_rows must be an integer: {}",
            &vi["embedded_rows"]
        );
        assert!(
            matches!(
                vi["index"].as_str(),
                Some("absent" | "present" | "present_invalid")
            ),
            "vector_index.index must be absent|present|present_invalid: {}",
            &vi["index"]
        );
        assert!(
            vi["count_at"].is_u64(),
            "vector_index.count_at must be a non-negative integer: {}",
            &vi["count_at"]
        );
        // `degraded` — the documented condition: only when the index is
        // not `present` and `embedded_rows >= 10 000` (the ≥10k branch is
        // not seeded here; see the module docs).
        let embedded = vi["embedded_rows"].as_i64().unwrap_or(0);
        let degraded = vi.get("degraded");
        if embedded < 10_000 {
            assert!(
                degraded.is_none_or(serde_json::Value::is_null),
                "vector_index.degraded must be absent below 10k embedded \
                 rows ({embedded}): {degraded:?}"
            );
        } else {
            assert!(
                degraded.is_none() || degraded.unwrap().is_string() || degraded.unwrap().is_null(),
                "vector_index.degraded, when present, must be a string: {degraded:?}"
            );
        }
    } else {
        assert!(
            vi.is_none_or(serde_json::Value::is_null),
            "vector_index must be omitted when the catalog probe fails: {vi:?}"
        );
    }
    // `content_integrity` — same present/omitted contract; when present,
    // the counts must be consistent with the row array.
    let ci = payload.get("content_integrity");
    if db_up {
        assert!(
            ci.is_some_and(|v| v.is_object()),
            "content_integrity must be present with a live DB: {ci:?}"
        );
        let ci = ci.unwrap();
        let rows = ci["zims"].as_array().expect("content_integrity.zims array");
        for row in rows {
            assert!(
                row["name"].is_string() && row["digest_drift"].is_boolean(),
                "content_integrity.zims row must carry name + digest_drift: {row}"
            );
            if let Some(sha) = row.get("content_sha256") {
                assert!(sha.is_string(), "content_sha256 must be a string: {sha}");
            }
            if let Some(sha) = row.get("publisher_sha256") {
                assert!(sha.is_string(), "publisher_sha256 must be a string: {sha}");
            }
        }
        let drift = rows
            .iter()
            .filter(|r| r["digest_drift"].as_bool() == Some(true))
            .count();
        let verified = rows
            .iter()
            .filter(|r| r.get("publisher_sha256").is_some_and(|v| !v.is_null()))
            .count();
        assert_eq!(
            ci["drift"].as_i64().unwrap_or(-1),
            drift as i64,
            "content_integrity.drift must count the drifted rows"
        );
        assert_eq!(
            ci["publisher_verified"].as_i64().unwrap_or(-1),
            verified as i64,
            "content_integrity.publisher_verified must count the rows with a \
             publisher claim"
        );
    } else {
        assert!(
            ci.is_none_or(serde_json::Value::is_null),
            "content_integrity must be omitted when the probe fails: {ci:?}"
        );
    }
}

/// Build an authenticated (password-mode) `AppState` over `pool` with the
/// test admin password. `db_backed` selects the settings source: `true`
/// loads from the suite DB (with the password/mode re-applied through the
/// env-snapshot seam — the startup path, no process env mutation),
/// `false` uses the in-memory defaults (the dead-pool half). Returns the
/// state plus a settings handle (cheap `Arc` clone) so the test can
/// `reload()` after direct-SQL setting mutations.
async fn diag_state(pool: Pool, db_backed: bool) -> (zimservice::AppState, SettingsCache) {
    let settings = if db_backed {
        let mut env_locked = HashMap::new();
        env_locked.insert("access.mode".into(), "ACCESS_MODE".into());
        env_locked.insert("access.admin_password".into(), "AUTH_PASSWORD".into());
        let mut env_snapshot = HashMap::new();
        env_snapshot.insert("access.mode".into(), "password".into());
        env_snapshot.insert("access.admin_password".into(), TEST_ADMIN_PW.to_string());
        zimservice::settings::SettingsCache::load(pool.clone(), env_locked, env_snapshot)
            .await
            .expect("settings load (env-snapshot password mode)")
    } else {
        let mut map = zimservice::settings::default_settings();
        map.insert("access.mode".into(), serde_json::json!("password"));
        map.insert(
            "access.admin_password".into(),
            serde_json::json!(TEST_ADMIN_PW),
        );
        zimservice::settings::SettingsCache::new_with_map(pool.clone(), map, HashMap::new())
    };
    let zims = zimservice::zim::ZimManager::new(
        std::path::PathBuf::from("/nonexistent-diagnostic"),
        pool.clone(),
    );
    let search = zimservice::search::SearchEngine::new(
        pool.clone(),
        settings.clone(),
        zimservice::health::DegradationTracker::default(),
    );
    (
        assemble_state(pool, settings.clone(), zims, search),
        settings,
    )
}

/// Pull `/diagnostic` with the admin token and parse the JSON payload.
async fn pull_diagnostic(base: &str, client: &reqwest::Client) -> serde_json::Value {
    let ok = client
        .get(format!("{base}/diagnostic"))
        .header("Authorization", format!("Bearer {TEST_ADMIN_PW}"))
        .send()
        .await
        .expect("authenticated /diagnostic request");
    assert_eq!(
        ok.status(),
        200,
        "the admin token must authenticate /diagnostic, got {:?} ({})",
        ok.status(),
        ok.text().await.unwrap_or_default()
    );
    ok.json().await.expect("JSON payload")
}

/// DB-less half: auth gating + the always-present contract fields, with
/// the probe-backed fields omitted (dead pool ⇒ probe failure ⇒ omission).
#[tokio::test]
async fn diagnostic_contract_without_db() {
    // Dead pool: the saturation reads are pure (no connection opened) and
    // the catalog/content probes fail → their fields must be omitted.
    let pool = zimservice::testing::dead_pool();
    let (state, _settings) = diag_state(pool, false).await;
    let (base, _trigger) = boot_server(zimservice::serve::build_router(state)).await;
    let client = reqwest::Client::new();

    // Auth gating: the route is admin-only.
    let anon = client
        .get(format!("{base}/diagnostic"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        anon.status(),
        401,
        "unauthenticated /diagnostic must be 401, got {:?}",
        anon.status()
    );
    let wrong = client
        .get(format!("{base}/diagnostic"))
        .header("Authorization", "Bearer wrong-password")
        .send()
        .await
        .unwrap();
    assert_eq!(
        wrong.status(),
        401,
        "a wrong admin token must be 401, got {:?}",
        wrong.status()
    );

    let payload = pull_diagnostic(&base, &client).await;
    assert_always_present(&payload);
    assert_conditional(&payload, false);
}

/// DB-gated half: with a live pool the probe-backed fields are present and
/// shaped per the contract, and the two dynamic contracts are exercised:
/// a wrong-typed setting surfaces in `settings_mismatches`, and an
/// integrity row surfaces in `content_integrity` (a bare row does not).
#[tokio::test]
async fn diagnostic_contract_with_db() {
    let (pool, _db_gate) = match pool_or_skip().await {
        Some(p) => p,
        None => return,
    };
    run_migrations(&pool).await.expect("migrations");
    let (state, settings) = diag_state(pool.clone(), true).await;
    let (base, _trigger) = boot_server(zimservice::serve::build_router(state)).await;
    let client = reqwest::Client::new();

    // ── Baseline: every contract field, probes live ─────────────────
    let payload = pull_diagnostic(&base, &client).await;
    assert_always_present(&payload);
    assert_conditional(&payload, true);

    // ── settings_mismatches: the wrong-typed-key contract ──────────
    // `search.default_limit` is an Int-typed key; a stored JSON string
    // fails the `json_type` check and must surface (the key silently runs
    // on its default — this is the operator's signal for that).
    zimservice::db::raw::execute(
        &pool,
        "UPDATE settings SET value = '\"not-an-int\"'::jsonb WHERE key = 'search.default_limit'",
        |q| q,
    )
    .await
    .expect("wrong-typed setting write");
    settings.reload().await.expect("settings reload");
    let payload = pull_diagnostic(&base, &client).await;
    // The entries name the key (descriptive form: `"<key>: expected …"`) —
    // the contract is that the key is surfaced, not the entry format.
    assert!(
        payload.get("settings_mismatches").is_some_and(|mm| {
            mm.as_array().is_some_and(|a| {
                a.iter().any(|k| {
                    k.as_str()
                        .is_some_and(|s| s.starts_with("search.default_limit"))
                })
            })
        }),
        "settings_mismatches must name the wrong-typed key after the write: {payload}"
    );
    // Restore (the shared suite DB must not keep a corrupt row) and verify
    // the clean state omits the field again.
    zimservice::db::raw::execute(
        &pool,
        "UPDATE settings SET value = '10'::jsonb WHERE key = 'search.default_limit'",
        |q| q,
    )
    .await
    .expect("setting restore");
    settings.reload().await.expect("settings reload (restore)");
    let payload = pull_diagnostic(&base, &client).await;
    assert!(
        payload
            .get("settings_mismatches")
            .is_none_or(|v| v.is_null()),
        "settings_mismatches must be omitted again after the restore: {}",
        &payload["settings_mismatches"]
    );

    // ── content_integrity: the row-appearance contract ─────────────
    // One row WITH an integrity record (observed digest + publisher
    // claim) and one bare row — only the former may appear.
    zimservice::db::raw::execute(
        &pool,
        "INSERT INTO zims (name, display_title, file_path, file_size, file_mtime,
                          content_sha256, publisher_sha256)
         VALUES ('diag-contract-digest', 'diag', '/nonexistent/diag.zim', 10, now(), $1, $2)",
        |q| q.bind(DIGEST).bind(DIGEST),
    )
    .await
    .expect("integrity row insert");
    zimservice::db::raw::execute(
        &pool,
        "INSERT INTO zims (name, display_title, file_path, file_size, file_mtime,
                          index_status, indexed_entries, article_count)
         VALUES ('diag-contract-bare', 'diag', '/nonexistent/diag-bare.zim', 10, now(),
                 'ready', 0, 0)",
        |q| q,
    )
    .await
    .expect("bare row insert");
    let payload = pull_diagnostic(&base, &client).await;
    let ci = payload
        .get("content_integrity")
        .expect("content_integrity present with a live DB")
        .as_object()
        .expect("content_integrity object");
    let rows = ci
        .get("zims")
        .and_then(|z| z.as_array())
        .expect("zims array");
    let mine = rows
        .iter()
        .find(|r| r["name"] == "diag-contract-digest")
        .expect("the integrity row must appear in content_integrity.zims");
    assert_eq!(mine["content_sha256"], serde_json::json!(DIGEST));
    assert_eq!(mine["publisher_sha256"], serde_json::json!(DIGEST));
    assert_eq!(mine["digest_drift"], serde_json::json!(false));
    assert!(
        !rows.iter().any(|r| r["name"] == "diag-contract-bare"),
        "a row without any integrity record must not appear: {rows:?}"
    );

    // Cleanup (the shared suite DB must not keep the test rows).
    zimservice::db::raw::execute(
        &pool,
        "DELETE FROM zims WHERE name IN ('diag-contract-digest', 'diag-contract-bare')",
        |q| q,
    )
    .await
    .expect("fixture cleanup");
}
