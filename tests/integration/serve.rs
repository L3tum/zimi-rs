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

/// TEST-7 (Tests Major #3): a ~50-request concurrent fan-out against a real
/// listener hits the service-wide rate limiter: the burst budget is
/// admitted (valid credentials → 200) and the rest is throttled with
/// 429 + Retry-After + `"rate limit exceeded"`. DB-less: the password
/// lives in the in-memory settings cache, so the dead pool is never
/// touched — which is why it is stored salted-hashed (see the inline note
/// at the `access.admin_password` insert): a legacy-plaintext value would
/// route every admitted request through the transparent upgrade's pool
/// write, and the dead pool's 30 s acquire timeout would stretch the
/// fan-out's wall-clock window until the rps=1 refill alone admits past
/// the window below (observed full-suite flake: ok=49). `rps=1` keeps the
/// refill during the fan-out's arrival window far under a token, so
/// admissions stay pinned near `burst` (the exact admission count is
/// pinned at the limiter unit level by
/// `concurrency_admits_exactly_burst`; this pins the HTTP wiring).
#[tokio::test]
async fn listener_burst_fanout_rate_limited() {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect_lazy("postgres://u:p@127.0.0.1:1/nodb")
        .unwrap();
    let mut map = zimservice::settings::default_settings();
    map.insert("access.mode".into(), serde_json::json!("password"));
    // Salted-argon2id form, NOT legacy plaintext: a plaintext value makes
    // every Full-auth request take the transparent-upgrade path (see
    // `SettingsAuth::upgrade`), which awaits a pool write. Against this
    // test's dead pool each such write burns the full 30 s acquire timeout,
    // stretching the 50-request fan-out's wall-clock window until the rps=1
    // token refill alone admits well past the (1..=8) window below — the
    // full-suite flake (ok=49). Hashed, the upgrade path is never taken and
    // the dead pool is never touched.
    map.insert(
        "access.admin_password".into(),
        serde_json::json!(zimservice::settings::hash_admin_password(
            "correct-horse-battery"
        )),
    );
    // Reads gated so `GET /settings` goes through the auth path (by default
    // only mutating verbs require the password).
    map.insert(
        "access.require_auth_for_reads".into(),
        serde_json::json!(true),
    );
    map.insert("access.rate_limit_rps".into(), serde_json::json!(1));
    map.insert("access.rate_limit_burst".into(), serde_json::json!(3));
    let settings = SettingsCache::new_with_map(pool.clone(), map, HashMap::new());
    let zims = ZimManager::new(std::path::PathBuf::from("/nonexistent-burst"), pool.clone());
    let search = SearchEngine::new(
        pool.clone(),
        settings.clone(),
        zimservice::health::DegradationTracker::default(),
    );
    let state = assemble_state(pool.clone(), settings, zims, search);
    let (base, _trigger) = boot_server(zimservice::serve::build_router(state)).await;

    const N: usize = 50;
    let client = reqwest::Client::new();
    let mut handles = Vec::with_capacity(N);
    for _ in 0..N {
        let client = client.clone();
        let base = base.clone();
        handles.push(tokio::spawn(async move {
            let resp = client
                .get(format!("{base}/settings"))
                .header("Authorization", "Bearer correct-horse-battery")
                .send()
                .await
                .expect("fan-out request must complete");
            (resp.status(), resp.text().await.unwrap_or_default())
        }));
    }

    let mut ok = 0usize;
    let mut throttled = 0usize;
    for h in handles {
        let (status, body) = h.await.expect("fan-out task must not panic");
        match status.as_u16() {
            200 => ok += 1,
            429 => {
                throttled += 1;
                let v: serde_json::Value = serde_json::from_str(&body).expect("429 body is JSON");
                assert_eq!(
                    v.get("error").and_then(|e| e.as_str()),
                    Some("rate limit exceeded"),
                    "429 must be the rate limiter's, not the lockout's"
                );
            }
            other => panic!("expected 200 or 429, got {other} (body: {body})"),
        }
    }
    assert_eq!(ok + throttled, N);
    assert!(
        (1..=8).contains(&ok),
        "burst=3 at rps=1 admits ~3 (refill margin), got {ok}"
    );
    assert!(throttled >= 1, "a 50-fan-out against burst=3 must throttle");
}

/// TEST-7 (Tests Major #3): five CONCURRENT auth failures from one source
/// IP (one real 127.0.0.1 `ConnectInfo`) engage the per-IP lockout — the
/// sixth request, which would otherwise reach the password check, is
/// rejected with 429 + Retry-After + the lockout body. The rate limiter is
/// set to its ceiling (effectively disabled) so the two 429 sources cannot
/// be confused. DB-less (in-memory settings cache).
#[tokio::test]
async fn listener_burst_fanout_lockout_after_five_concurrent_failures() {
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
    map.insert(
        "access.require_auth_for_reads".into(),
        serde_json::json!(true),
    );
    // Ceiling values: the limiter is effectively disabled for 6 requests.
    map.insert("access.rate_limit_rps".into(), serde_json::json!(1_000_000));
    map.insert(
        "access.rate_limit_burst".into(),
        serde_json::json!(1_000_000),
    );
    let settings = SettingsCache::new_with_map(pool.clone(), map, HashMap::new());
    let zims = ZimManager::new(
        std::path::PathBuf::from("/nonexistent-lockout-burst"),
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
    // Five concurrent wrong tokens → five 401s, each recorded as a failure
    // for the same source IP.
    let mut handles = Vec::new();
    for _ in 0..5 {
        let client = client.clone();
        let base = base.clone();
        handles.push(tokio::spawn(async move {
            client
                .get(format!("{base}/settings"))
                .header("Authorization", "Bearer wrong")
                .send()
                .await
                .expect("lockout fan-out request must complete")
                .status()
        }));
    }
    for h in handles {
        let status = h.await.expect("lockout fan-out task must not panic");
        assert_eq!(status.as_u16(), 401, "each of the first 5 must be 401");
    }
    // The sixth from the same IP is locked out before any password check.
    let locked = client
        .get(format!("{base}/settings"))
        .header("Authorization", "Bearer wrong")
        .send()
        .await
        .expect("attempt 6 must complete");
    assert_eq!(
        locked.status(),
        429,
        "attempt 6 from the same IP must be locked out"
    );
    assert!(
        locked
            .headers()
            .contains_key(axum::http::header::RETRY_AFTER),
        "lockout 429 must carry Retry-After"
    );
    let v: serde_json::Value = locked.json().await.expect("lockout 429 body is JSON");
    assert_eq!(
        v.get("error").and_then(|e| e.as_str()),
        Some("too many failed authentication attempts"),
        "the 429 must be the lockout's, not the rate limiter's"
    );
}

/// TEST-7 (Tests Major #3c): with the pool's only connection held, a
/// DB-backed request surfaces the pool-exhaustion 503
/// (`PoolTimedOut` → `SERVICE_UNAVAILABLE`) instead of hanging. The shared
/// suite pool (via `pool_or_skip`, which holds the DbExclusiveGuard) is
/// used for the skip decision + its URL only; the server under test is
/// built against a PRIVATE 1-connection pool whose sole connection this
/// test holds — no suite fixture is touched by the private pool (its
/// checkouts time out before any query runs, and the held connection is
/// idle), so the guard's serialization purpose is preserved.
#[tokio::test]
async fn listener_pool_exhausted_returns_503() {
    let Some((_shared, _db_gate)) = pool_or_skip().await else {
        return;
    };
    let url = std::env::var("DATABASE_URL").unwrap_or_else(|_| DEFAULT_URL.to_string());
    // Private 1-connection pool against the same DB; the test holds the
    // sole connection for the duration, so every server-side checkout
    // waits out its 1 s `acquire_timeout` and fails with `PoolTimedOut`.
    let exhausted = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(1))
        .connect(&url)
        .await
        .expect("private pool must connect (the shared pool just proved the DB is up)");
    let held = exhausted.acquire().await.expect("hold the sole connection");

    let settings = SettingsCache::new_with_map(
        exhausted.clone(),
        zimservice::settings::default_settings(),
        HashMap::new(),
    );
    let zims = ZimManager::new(
        std::path::PathBuf::from("/nonexistent-pool503"),
        exhausted.clone(),
    );
    let search = SearchEngine::new(
        exhausted.clone(),
        settings.clone(),
        zimservice::health::DegradationTracker::default(),
    );
    let state = assemble_state(exhausted.clone(), settings, zims, search);
    let (base, _trigger) = boot_server(zimservice::serve::build_router(state)).await;

    let client = reqwest::Client::new();
    let resp = tokio::time::timeout(
        Duration::from_secs(15),
        client.get(format!("{base}/search?q=test")).send(),
    )
    .await
    .expect("exhausted-pool request must answer (503), not hang")
    .expect("request must complete");
    assert_eq!(
        resp.status(),
        503,
        "an exhausted pool must surface 503 (PoolTimedOut), not hang or 200"
    );
    drop(held);
}
