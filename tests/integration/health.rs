//! Health domain integration tests.

use super::common::*;

/// M-health-200: with a **live** DB, `/health` returns `200` and reports
/// `db_connected: true` / `status: "ok"` (the 503 path is covered by the dead-pool
/// handler unit test).
#[tokio::test]
async fn health_live_db_returns_200() {
    let (pool, _db_gate) = match pool_or_skip().await {
        Some(p) => p,
        None => return,
    };
    run_migrations(&pool).await.expect("migrations");
    let state = live_state(pool).await;
    let (code, axum::response::Json(body)) =
        zimservice::serve::handlers::health(axum::extract::State(state)).await;
    assert_eq!(code, axum::http::StatusCode::OK);
    assert!(body.db_connected, "live DB must be reachable");
    assert_eq!(body.status, "ok");
}

/// TEST-6: `/health` over a real TCP listener returns 200 (DB-gated: the
/// health probe reports on the live pool).
#[tokio::test]
async fn listener_health_returns_200_over_real_tcp() {
    let (pool, _db_gate) = match pool_or_skip().await {
        Some(p) => p,
        None => return,
    };
    run_migrations(&pool).await.expect("migrations");

    let state = live_state(pool).await;
    let (base, _trigger) = boot_server(zimservice::serve::build_router(state)).await;
    let resp = reqwest::get(format!("{base}/health"))
        .await
        .expect("health GET over real TCP");
    assert_eq!(resp.status(), 200, "/health over real TCP must be 200");
}

/// TEST-6 (WI-18): under a fail-closed config (password mode, empty
/// password) `/health` stays reachable ABOVE the 503 branch — the DB-backed
/// health body proves the request reached the health handler — while every
/// other route fails closed.
#[tokio::test]
async fn listener_fail_closed_config_keeps_health_reachable() {
    let (pool, _db_gate) = match pool_or_skip().await {
        Some(p) => p,
        None => return,
    };
    run_migrations(&pool).await.expect("migrations");

    let mut map = zimservice::settings::default_settings();
    map.insert("access.mode".into(), serde_json::json!("password"));
    map.insert("access.admin_password".into(), serde_json::json!(""));
    let settings = SettingsCache::new_with_map(pool.clone(), map, HashMap::new());
    let zims = ZimManager::new(
        std::path::PathBuf::from("/nonexistent-failclosed"),
        pool.clone(),
    );
    let search = SearchEngine::new(
        pool.clone(),
        settings.clone(),
        zimservice::health::DegradationTracker::default(),
    );
    let state = assemble_state(pool.clone(), settings, zims, search);
    let (base, _trigger) = boot_server(zimservice::serve::build_router(state)).await;

    // `/health` is exempt: 200 with the real health body, not the 503 JSON.
    let health = reqwest::get(format!("{base}/health"))
        .await
        .expect("health GET over real TCP");
    assert_eq!(
        health.status(),
        200,
        "/health must stay reachable under fail-closed"
    );
    let body = health.text().await.unwrap();
    assert!(
        !body.contains("misconfigured"),
        "health body must come from the health handler, not the fail-closed 503: {body}"
    );

    // Every other route fails closed.
    let settings_resp = reqwest::get(format!("{base}/settings"))
        .await
        .expect("settings GET over real TCP");
    assert_eq!(settings_resp.status(), 503, "/settings must fail closed");
    let body = settings_resp.text().await.unwrap();
    assert!(
        body.contains("authentication is misconfigured"),
        "unexpected fail-closed body: {body}"
    );
}
