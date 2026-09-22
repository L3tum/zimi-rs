//! Read-only API token integration tests (2026-09-18 review).
//!
//! HTTP-level coverage of the two-level credential model: the admin password
//! keeps full access; the optional `access.read_only_token` authenticates but
//! is scoped to the RAG-read allowlist (403 elsewhere); an unknown token is
//! 401. DB-less: the dead lazy pool is never touched on the auth-gated paths
//! exercised here — a request that gets PAST the auth gate may then fail with
//! a 5xx from the dead DB, which is exactly the signal "auth passed" for this
//! test (only 401/403 mean the gate rejected it).

use super::common::*;

/// Boot a password-mode server with BOTH credentials configured and reads
/// gated, so every route below goes through the full auth decision.
///
/// Returns the base URL **and the shutdown trigger** — the trigger must be
/// held for the whole test: dropping it completes `boot_server`'s
/// graceful-shutdown future and stops the listener (see `boot_server`).
async fn boot_two_level_server() -> (String, tokio::sync::oneshot::Sender<()>) {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect_lazy("postgres://u:p@127.0.0.1:1/nodb")
        .unwrap();
    let mut map = zimservice::settings::default_settings();
    map.insert("access.mode".into(), serde_json::json!("password"));
    map.insert(
        "access.admin_password".into(),
        serde_json::json!("admin-secret-pw"),
    );
    map.insert(
        "access.read_only_token".into(),
        serde_json::json!("ro-secret-token"),
    );
    // Gate reads too, so allowlisted GETs actually exercise the token.
    map.insert(
        "access.require_auth_for_reads".into(),
        serde_json::json!(true),
    );
    let settings = SettingsCache::new_with_map(pool.clone(), map, HashMap::new());
    let zims = ZimManager::new(std::path::PathBuf::from("/nonexistent-ro"), pool.clone());
    let search = SearchEngine::new(
        pool.clone(),
        settings.clone(),
        zimservice::health::DegradationTracker::default(),
    );
    let state = assemble_state(pool, settings, zims, search);
    boot_server(zimservice::serve::build_router(state)).await
}

/// Statuses that mean "the auth gate let it through" (the request then
/// reached a handler that may 5xx on the dead pool or 404 on missing data).
fn gate_passed(status: u16) -> bool {
    status != 401 && status != 403
}

#[tokio::test]
async fn read_only_token_grants_allowlist_only() {
    let (base, _trigger) = boot_two_level_server().await;
    let client = reqwest::Client::new();
    let ro = |b: &str| format!("Bearer {b}");

    // ── Allowlist: the read-only token gets through the gate ─────────────
    // /openapi.json needs no DB — an exact 200 proves both the gate and the
    // handler.
    let resp = client
        .get(format!("{base}/openapi.json"))
        .header("Authorization", ro("ro-secret-token"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "openapi.json must be 200 for the read-only token"
    );

    // /search passes the gate; the dead pool then makes the handler 5xx —
    // anything that is NOT 401/403 proves the allowlist let it in.
    for path in [
        "/search?q=x",
        "/suggest?q=x",
        "/list",
        "/snippet?path=/a",
        "/random",
    ] {
        let resp = client
            .get(format!("{base}{path}"))
            .header("Authorization", ro("ro-secret-token"))
            .send()
            .await
            .unwrap();
        assert!(
            gate_passed(resp.status().as_u16()),
            "GET {path} with the read-only token must pass the gate, got {}",
            resp.status()
        );
    }

    // ── Outside the allowlist: 403 (valid credential, wrong scope) ───────
    for path in ["/settings", "/diagnostic", "/collections", "/downloads"] {
        let resp = client
            .get(format!("{base}{path}"))
            .header("Authorization", ro("ro-secret-token"))
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            403,
            "GET {path} with the read-only token must be 403 (scope), not {} — body: {}",
            resp.status(),
            resp.text().await.unwrap_or_default()
        );
    }

    // Mutating verbs are never in the allowlist, even on allowlisted paths.
    let resp = client
        .post(format!("{base}/search"))
        .header("Authorization", ro("ro-secret-token"))
        .body("")
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        403,
        "POST /search with the read-only token must be 403 (mutating), got {}",
        resp.status()
    );
}

#[tokio::test]
async fn admin_token_keeps_full_access_and_unknown_is_401() {
    let (base, _trigger) = boot_two_level_server().await;
    let client = reqwest::Client::new();

    // Admin Bearer: full access — /settings (secret-bearing) is readable.
    let resp = client
        .get(format!("{base}/settings"))
        .header("Authorization", "Bearer admin-secret-pw")
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "admin Bearer must keep full access to /settings"
    );

    // No token on a gated read: 401.
    let resp = client.get(format!("{base}/settings")).send().await.unwrap();
    assert_eq!(resp.status(), 401, "no token on a gated read must be 401");

    // A token that matches neither credential: 401 (not 403).
    let resp = client
        .get(format!("{base}/settings"))
        .header("Authorization", "Bearer not-a-real-token")
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        401,
        "an unknown token must be 401 (no match), not 403 — got {}",
        resp.status()
    );

    // The read-only token is NOT the admin credential: it must not unlock
    // full access via the admin path (it 403s on /settings, not 200s).
    let resp = client
        .get(format!("{base}/settings"))
        .header("Authorization", "Bearer ro-secret-token")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 403);
}

#[tokio::test]
async fn query_string_form_still_works_for_both_levels() {
    let (base, _trigger) = boot_two_level_server().await;
    let client = reqwest::Client::new();

    // Back-compat: the query-string form authenticates read verbs for BOTH
    // levels (SEC-M3: read verbs only).
    let resp = client
        .get(format!("{base}/openapi.json?access_token=ro-secret-token"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "query-string read-only token must pass the gate"
    );

    let resp = client
        .get(format!("{base}/settings?access_token=admin-secret-pw"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "query-string admin token must keep full read access"
    );

    // A query-string token is never usable to mutate (SEC-M3): the mutating
    // verb ignores the query form entirely → 401 (no Bearer presented).
    let resp = client
        .put(format!("{base}/settings?access_token=admin-secret-pw"))
        .body(r#"{"general.log_level":"info"}"#)
        .header("content-type", "application/json")
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        401,
        "mutating verbs are Bearer-only; the query-string token must not authenticate them"
    );
}

#[tokio::test]
async fn bearer_wins_over_query_when_present() {
    let (base, _trigger) = boot_two_level_server().await;
    let client = reqwest::Client::new();

    // Bearer present: it is THE credential. A lower-level Bearer (read-only)
    // is validated on its own — the higher-level query token beside it does
    // NOT upgrade it.
    let resp = client
        .get(format!("{base}/settings?access_token=admin-secret-pw"))
        .header("Authorization", "Bearer ro-secret-token")
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        403,
        "the Bearer (read-only) must decide the outcome; the admin query \
         token must not upgrade it — got {}",
        resp.status()
    );

    // Conversely, a higher-level Bearer still wins (admin Bearer + RO query
    // token → full access).
    let resp = client
        .get(format!("{base}/settings?access_token=ro-secret-token"))
        .header("Authorization", "Bearer admin-secret-pw")
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "an admin Bearer must keep full access even with a read-only query token present"
    );
}
