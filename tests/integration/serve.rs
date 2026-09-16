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
///
/// Barrier-based (no wall-clock polling): the `/slow` handler first signals
/// "I am now in flight" via a `tokio::sync::Notify` and then blocks on a
/// second `Notify` release barrier before returning 200. The test (1) fires
/// the request (registering the in-flight `notified()` waiter first, so the
/// handler's `notify_waiters` can never land unobserved), (2) awaits the
/// in-flight signal (5 s bound — a deadlock guard, not a scheduler),
/// (3) triggers the graceful shutdown, (4) releases the handler, (5) asserts
/// the in-flight request completed with 200, (6) asserts a new connection is
/// refused. The critical ordering — request in flight BEFORE shutdown, served
/// out AFTER the shutdown trigger — is event-driven, so a stalled runner
/// cannot flip it. A `notify_one` that lands before the handler registers
/// `notified()` is safe: `Notify` stores one permit.
#[tokio::test]
async fn listener_graceful_shutdown_finishes_in_flight() {
    let in_flight = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let app = axum::Router::new().route(
        "/slow",
        axum::routing::get({
            let in_flight = in_flight.clone();
            let release = release.clone();
            move || {
                let in_flight = in_flight.clone();
                let release = release.clone();
                async move {
                    // Barrier: signal "handler entered, request is in
                    // flight", then block until the test releases us.
                    in_flight.notify_waiters();
                    release.notified().await;
                    "ok"
                }
            }
        }),
    );
    let (base, trigger) = boot_server(app).await;
    let client = reqwest::Client::new();

    // Register the in-flight waiter BEFORE firing the request: the handler
    // only runs after the server task has accepted the connection, so this
    // guarantees the handler's `notify_waiters` finds a registered waiter.
    let in_flight_wait = in_flight.notified();
    let (task_client, task_base) = (client.clone(), base.clone());
    let in_flight_req = tokio::spawn(async move {
        task_client
            .get(format!("{task_base}/slow"))
            .send()
            .await
            .expect("in-flight request accepted")
    });

    // Wait for the in-flight signal from the handler. The 5 s bound is only a
    // deadlock guard (a hang means a hard failure); it does not schedule the
    // critical event — the handler proves it is in flight itself.
    tokio::time::timeout(Duration::from_secs(5), in_flight_wait)
        .await
        .expect("handler must signal in-flight within 5 s (deadlock guard)");

    // The request is now provably inside the handler (awaiting release).
    // Trigger the graceful shutdown: the listener must stop accepting new
    // connections while this in-flight request is still outstanding.
    trigger.send(()).expect("trigger send");

    // axum serves the in-flight request out before stopping.
    release.notify_one();
    let resp = tokio::time::timeout(Duration::from_secs(5), in_flight_req)
        .await
        .expect("in-flight request must finish within 5 s of the release")
        .expect("in-flight task must not panic");
    assert_eq!(resp.status(), 200);

    // The listener no longer accepts new connections.
    let refused = client.get(format!("{base}/slow")).send().await;
    assert!(
        refused.is_err(),
        "closed listener must refuse new connections"
    );
}
