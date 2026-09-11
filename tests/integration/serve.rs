//! Serve domain integration tests.

use super::common::*;

/// TEST-6 (WI-02): five wrong passwords from one client (one real 127.0.0.1
/// `ConnectInfo`) get 401; the sixth is locked out with 429 + Retry-After.
/// DB-less: the password lives in the in-memory settings cache, so the dead
/// pool is never touched.
#[tokio::test]
async fn listener_five_bad_passwords_lock_out_client() {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect_lazy("postgres://u:p@127.0.0.1:1/nodb")
        .unwrap();
    let mut map = zimservice::settings::default_settings();
    map.insert("access.mode".into(), serde_json::json!("password"));
    map.insert(
        "access.admin_password".into(),
        serde_json::json!("correct-horse-battery"),
    );
    // Reads gated so `GET /settings` goes through the auth path (by default
    // only mutating verbs require the password).
    map.insert(
        "access.require_auth_for_reads".into(),
        serde_json::json!(true),
    );
    let settings = SettingsCache::new_with_map(pool.clone(), map, HashMap::new());
    let zims = ZimManager::new(
        std::path::PathBuf::from("/nonexistent-lockout"),
        pool.clone(),
    );
    let search = SearchEngine::new(
        pool.clone(),
        settings.clone(),
        zimservice::health::DegradationTracker::default(),
    );
    let state = assemble_state(pool.clone(), settings, zims, search);
    let (base, _trigger) = boot_server(zimservice::serve::build_router(state)).await;

    let client = reqwest::Client::new();
    for attempt in 1..=5u32 {
        let resp = client
            .get(format!("{base}/settings"))
            .header("Authorization", "Bearer wrong")
            .send()
            .await
            .unwrap_or_else(|e| panic!("attempt {attempt}: request failed: {e}"));
        assert_eq!(
            resp.status(),
            401,
            "attempt {attempt} of 5 must be 401, got {:?}",
            resp.status()
        );
    }
    let locked = client
        .get(format!("{base}/settings"))
        .header("Authorization", "Bearer wrong")
        .send()
        .await
        .unwrap_or_else(|e| panic!("attempt 6: request failed: {e}"));
    assert_eq!(
        locked.status(),
        429,
        "attempt 6 must be locked out (429), keyed on the real 127.0.0.1 ConnectInfo"
    );
    assert!(
        locked
            .headers()
            .contains_key(axum::http::header::RETRY_AFTER),
        "lockout 429 must carry Retry-After"
    );
}

/// TEST-6: graceful shutdown serves out the in-flight request before stopping
/// the listener, and afterwards refuses new connections. DB-less.
#[tokio::test]
async fn listener_graceful_shutdown_finishes_in_flight() {
    let app = axum::Router::new().route(
        "/slow",
        axum::routing::get(async || {
            tokio::time::sleep(Duration::from_secs(1)).await;
            "ok"
        }),
    );
    let (base, trigger) = boot_server(app).await;
    let client = reqwest::Client::new();

    let (task_client, task_base) = (client.clone(), base.clone());
    let in_flight = tokio::spawn(async move {
        task_client
            .get(format!("{task_base}/slow"))
            .send()
            .await
            .expect("in-flight request accepted")
    });

    // Let the request start, then trigger the graceful shutdown. We poll
    // briefly to ensure the server has accepted the TCP connection and the
    // handler has entered its 1 s sleep (deterministic on slow CI where a
    // fixed 100 ms might be too short for the handshake + HTTP parse). The
    // guard assert below proves the poll window still ends before the
    // handler's 1 s sleep does, so the request is genuinely in flight.
    for _ in 0..20 {
        tokio::time::sleep(Duration::from_millis(10)).await;
        // If the in-flight task has already completed (shouldn't happen in
        // 200 ms), bail early.
        if in_flight.is_finished() {
            break;
        }
    }
    assert!(
        !in_flight.is_finished(),
        "guard: the request must still be in flight when shutdown is triggered; if the /slow handler's 1 s sleep shrank below the 200 ms poll window, this test no longer proves graceful serve-out"
    );
    trigger.send(()).expect("trigger send");

    // axum serves the in-flight request out before stopping the listener.
    let resp = tokio::time::timeout(Duration::from_secs(5), in_flight)
        .await
        .expect("in-flight request must finish within 5 s of the shutdown trigger")
        .expect("in-flight task must not panic");
    assert_eq!(resp.status(), 200);

    // The listener no longer accepts new connections.
    let refused = client.get(format!("{base}/slow")).send().await;
    assert!(
        refused.is_err(),
        "closed listener must refuse new connections"
    );
}
