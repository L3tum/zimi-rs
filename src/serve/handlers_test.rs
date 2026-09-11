//! Handler unit tests that run without a live database (DB-backed coverage
//! lives in `tests/integration.rs`). Uses `.unwrap()` freely behind the inner
//! `allow`; see the crate-level test conventions.
#![allow(clippy::unwrap_used)]

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    use crate::serve::build_router;
    use crate::settings::{
        KEY_ACCESS_ADMIN_PASSWORD, KEY_ACCESS_MODE, KEY_ACCESS_RATE_LIMIT_BURST,
        KEY_ACCESS_RATE_LIMIT_RPS, KEY_ACCESS_REQUIRE_AUTH_FOR_READS,
        KEY_DOWNLOADS_ALLOW_PRIVATE_NETWORKS, KEY_EMBEDDING_ENDPOINT, KEY_SEARCH_MAX_LIMIT,
        KEY_TORRENT_URL,
    };
    use crate::AppState;

    fn test_state() -> AppState {
        crate::testing::test_state()
    }

    async fn body_text(resp: axum::response::Response) -> String {
        let (_, body) = resp.into_parts();
        let bytes = body.collect().await.unwrap().to_bytes();
        String::from_utf8_lossy(&bytes).to_string()
    }

    // ── Health & info ────────────────────────────────────────────────────────

    #[tokio::test]
    async fn health_db_down_returns_503() {
        // `test_state()` points at a dead pool, so `probe_db` is false → the
        // handler returns 503 and reports `db_connected: false` (this also
        // proves the liveness probe reports false for an unreachable DB).
        let app = build_router(test_state());
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        let v: serde_json::Value = serde_json::from_str(&body_text(resp).await).unwrap();
        assert_eq!(v["db_connected"], false);
        assert_eq!(v["status"], "degraded");
    }

    /// M1: `/health` carries the multi-instance mode as an additive field so
    /// a monitor polling several instances can see which one is running with
    /// the single-instance guards disabled. The value is the process-level
    /// env opt-out, so the assertion is self-consistent under either state
    /// (a developer's export must not break the test).
    #[tokio::test]
    async fn health_reports_multi_instance_flag() {
        let app = build_router(test_state());
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let v: serde_json::Value = serde_json::from_str(&body_text(resp).await).unwrap();
        assert_eq!(
            v["multi_instance"],
            crate::startup::multi_instance_allowed(),
            "the /health flag must mirror the process-level opt-out"
        );
    }

    /// M-A: `/health` carries an additive `index` signal — `building` is
    /// `true` only while a `CREATE INDEX CONCURRENTLY` is in flight (the flag
    /// is the `AppState::index_building` field held by the build path, so
    /// the test sets it directly; no DB needed). All pre-existing fields
    /// stay, so this also guards the additive-only contract.
    #[tokio::test]
    async fn health_reports_index_build_flag() {
        let state = test_state();
        let app = build_router(state.clone());

        // Idle: `index.building` is present and false.
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let v: serde_json::Value = serde_json::from_str(&body_text(resp).await).unwrap();
        assert_eq!(v["index"]["building"], false, "idle /health: {v:?}");
        assert_eq!(v["db_connected"], false, "existing fields must stay: {v:?}");
        assert_eq!(v["status"], "degraded");

        // Simulate an in-flight build: the same flag the build path holds.
        // Fresh router: `oneshot` consumes the previous one.
        state
            .index_building
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let app = build_router(state.clone());
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let v: serde_json::Value = serde_json::from_str(&body_text(resp).await).unwrap();
        assert_eq!(v["index"]["building"], true, "building /health: {v:?}");
    }

    /// Security High #2: `/health` is the unauthenticated, rate-limit-exempt
    /// LB probe, so it must never disclose *which* settings rows are corrupt
    /// (that names internal config keys) — the detail lives on the
    /// authenticated `/diagnostic` route.
    #[tokio::test]
    async fn health_never_exposes_settings_mismatches() {
        let state = test_state();
        let app = build_router(state.clone());

        // Corrupt a row: /health must not gain a `settings_mismatches` field.
        let mut m = std::collections::BTreeMap::new();
        m.insert(
            KEY_DOWNLOADS_ALLOW_PRIVATE_NETWORKS.to_string(),
            "expected a boolean, got a string".to_string(),
        );
        state.settings.set_type_mismatches(m);

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let v: serde_json::Value = serde_json::from_str(&body_text(resp).await).unwrap();
        assert!(
            v.get("settings_mismatches").is_none(),
            "the open /health probe must not name corrupt settings rows: {v:?}"
        );
    }

    /// ARCH Major #3 / Security High #2: `/diagnostic` reports the keys whose
    /// stored value fails its `json_type` check, but only to a valid admin
    /// token — unauthenticated callers get 401 with no diagnostic content.
    #[tokio::test]
    async fn diagnostic_requires_auth_and_reports_mismatches() {
        let state = password_state(false);
        let app = build_router(state.clone());

        // Unauthenticated: 401, and the body must not leak the diagnostic.
        let resp = app
            .clone()
            .oneshot(get_with_token("/diagnostic", None))
            .await
            .unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::UNAUTHORIZED);
        let text = body_text(resp).await;
        assert!(
            !text.contains(KEY_DOWNLOADS_ALLOW_PRIVATE_NETWORKS),
            "the 401 body must not name the corrupt key: {text:?}"
        );

        // Corrupt a row: still hidden without a token…
        let mut m = std::collections::BTreeMap::new();
        m.insert(
            KEY_DOWNLOADS_ALLOW_PRIVATE_NETWORKS.to_string(),
            "expected a boolean, got a string".to_string(),
        );
        state.settings.set_type_mismatches(m);
        let resp = app
            .clone()
            .oneshot(get_with_token("/diagnostic", None))
            .await
            .unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::UNAUTHORIZED);
        let text = body_text(resp).await;
        assert!(
            !text.contains(KEY_DOWNLOADS_ALLOW_PRIVATE_NETWORKS),
            "the 401 body must not name the corrupt key: {text:?}"
        );

        // …but named for a valid admin token.
        let resp = app
            .oneshot(get_with_token("/diagnostic", Some("pw")))
            .await
            .unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::OK);
        let v: serde_json::Value = serde_json::from_str(&body_text(resp).await).unwrap();
        assert_eq!(
            v["settings_mismatches"],
            serde_json::json!([format!(
                "{KEY_DOWNLOADS_ALLOW_PRIVATE_NETWORKS}: expected a boolean, got a string"
            )])
        );
        assert_eq!(v["version"], env!("CARGO_PKG_VERSION"));
    }

    /// A clean state omits `settings_mismatches` on the authenticated
    /// `/diagnostic` (the field is skipped when nothing is corrupt).
    #[tokio::test]
    async fn diagnostic_omits_mismatches_when_clean() {
        let app = build_router(password_state(false));
        let resp = app
            .oneshot(get_with_token("/diagnostic", Some("pw")))
            .await
            .unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::OK);
        let v: serde_json::Value = serde_json::from_str(&body_text(resp).await).unwrap();
        assert!(
            v.get("settings_mismatches").is_none(),
            "a clean state must omit the field: {v:?}"
        );
    }

    /// AppState like `test_state()` (dead pool) but with the rate limiter
    /// tightened to `rps=1, burst=1` so the second request of the same burst
    /// is throttled. A fresh `RateLimiterHandle` starts with `burst` tokens.
    fn rate_limited_state() -> AppState {
        let mut values = crate::settings::default_settings();
        values.insert(KEY_ACCESS_RATE_LIMIT_RPS.into(), serde_json::json!(1));
        values.insert(KEY_ACCESS_RATE_LIMIT_BURST.into(), serde_json::json!(1));
        crate::testing::test_state_with_settings(values)
    }

    #[tokio::test]
    async fn rate_limit_429_retry_after_and_health_exempt() {
        // burst=1: the first `/list` is admitted (dead pool → 503); the second
        // is throttled → 429 with a `Retry-After` header of ≥ 1s.
        let app = build_router(rate_limited_state());

        let first = app
            .clone()
            .oneshot(Request::builder().uri("/list").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(
            first.status(),
            StatusCode::OK,
            "first request admitted (empty in-memory list → 200)"
        );

        let second = app
            .clone()
            .oneshot(Request::builder().uri("/list").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(
            second.status(),
            StatusCode::TOO_MANY_REQUESTS,
            "second request of the burst is throttled"
        );
        let retry_after = second
            .headers()
            .get(axum::http::header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        let secs: u64 = retry_after.parse().unwrap_or(0);
        assert!(
            secs >= 1,
            "Retry-After must be a delta-seconds value ≥ 1, got: {retry_after:?}"
        );

        // `/health` is exempt: three more requests in the same window must all
        // reach the handler (dead pool → 503), never 429.
        for _ in 0..3 {
            let h = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri("/health")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(
                h.status(),
                StatusCode::SERVICE_UNAVAILABLE,
                "/health is rate-limit-exempt (dead pool → 503, not 429)"
            );
        }
    }

    #[tokio::test]
    async fn list_returns_200_empty() {
        let app = build_router(test_state());
        let resp = app
            .oneshot(Request::builder().uri("/list").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    // ── Downloads ────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn downloads_list_db_unavailable_returns_503() {
        // Dead pool → list_downloads queries DB → 503 Service Unavailable.
        let app = build_router(test_state());
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/downloads")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn add_download_rejects_non_zim_without_qb() {
        // A non-.zim URL requires qBittorrent; without it configured → 400.
        let app = build_router(test_state());
        let body = serde_json::json!({ "name": "test", "url": "https://example.com/file.zip" });
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/downloads")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn add_download_rejects_empty_url() {
        let app = build_router(test_state());
        let body = serde_json::json!({ "name": "test", "url": "" });
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/downloads")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn delete_download_db_unavailable_returns_503() {
        // Dead pool → cancel_download queries DB → 503.
        let app = build_router(test_state());
        let resp = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/downloads/99999")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    // ── Settings ─────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn get_settings_unauthenticated_redacts_endpoints() {
        // Set password mode + a password so that an unauthenticated GET
        // triggers topology redaction (open-mode callers are redacted too —
        // see `get_settings_open_mode_redacts_topology`).
        let mut state = test_state();
        let mut settings_map = crate::settings::default_settings();
        settings_map.insert(KEY_ACCESS_MODE.into(), serde_json::json!("password"));
        settings_map.insert(
            KEY_ACCESS_ADMIN_PASSWORD.into(),
            serde_json::json!("testpass"),
        );
        settings_map.insert(
            KEY_EMBEDDING_ENDPOINT.into(),
            serde_json::json!("http://10.0.0.1:8080/v1"),
        );
        settings_map.insert(
            KEY_TORRENT_URL.into(),
            serde_json::json!("http://10.0.0.2:8080"),
        );
        state.settings = crate::settings::SettingsCache::new_with_map(
            state.db.clone(),
            settings_map,
            std::collections::HashMap::new(),
        );

        let app = build_router(state);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/settings")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let text = body_text(resp).await;
        // Unauthenticated: endpoints must be redacted.
        assert!(
            !text.contains("10.0.0.1"),
            "embedding.endpoint should be redacted, got: {text}"
        );
        assert!(
            !text.contains("10.0.0.2"),
            "torrent.url should be redacted, got: {text}"
        );
    }

    #[tokio::test]
    async fn get_settings_open_mode_redacts_topology() {
        // BUG-10: in open mode there is no shared secret, so every caller is
        // unauthenticated → topology values must be redacted (the old
        // `if mode == "open" { true }` override leaked them).
        let mut settings_map = crate::settings::default_settings();
        assert_eq!(
            settings_map.get(KEY_ACCESS_MODE),
            Some(&serde_json::json!("open")),
            "default mode must be open"
        );
        settings_map.insert(
            KEY_EMBEDDING_ENDPOINT.into(),
            serde_json::json!("http://10.1.1.1:8080/v1"),
        );
        settings_map.insert(
            KEY_TORRENT_URL.into(),
            serde_json::json!("http://10.1.1.2:8080"),
        );
        let mut state = test_state();
        state.settings = crate::settings::SettingsCache::new_with_map(
            state.db.clone(),
            settings_map,
            std::collections::HashMap::new(),
        );

        let app = build_router(state);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/settings")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let text = body_text(resp).await;
        assert!(
            !text.contains("10.1.1.1"),
            "open mode: embedding.endpoint should be redacted, got: {text}"
        );
        assert!(
            !text.contains("10.1.1.2"),
            "open mode: torrent.url should be redacted, got: {text}"
        );
    }

    // ── /list file_path redaction (WP3.7) ─────────────────────────────────────

    /// AppState like `test_state()` but with one ZIM (`demo`) seeded in the
    /// in-memory cache via a real temp dir + `scan()`, so `file_path` is a
    /// non-trivial absolute path (`<tmpdir>/demo.zim`). `settings_map` lets
    /// each test pick open vs password mode.
    async fn list_state_with_zim(
        settings_map: std::collections::HashMap<String, serde_json::Value>,
    ) -> AppState {
        let mut state = crate::testing::test_state_with_settings(settings_map);
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("demo.zim"), b"ZIM\x00").unwrap();
        let zims = crate::zim::ZimManager::new(dir.path().to_path_buf(), state.db.clone());
        zims.scan().await.unwrap();
        assert!(
            zims.list().iter().any(|z| z.name == "demo"),
            "scan should have seeded the demo ZIM"
        );
        state.zims = zims;
        state
    }

    #[tokio::test]
    async fn list_password_mode_unauthenticated_redacts_file_path() {
        let mut m = crate::settings::default_settings();
        m.insert(KEY_ACCESS_MODE.into(), serde_json::json!("password"));
        m.insert(
            KEY_ACCESS_ADMIN_PASSWORD.into(),
            serde_json::json!("testpass"),
        );
        let state = list_state_with_zim(m).await;
        let app = build_router(state);
        let resp = app
            .oneshot(Request::builder().uri("/list").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let text = body_text(resp).await;
        assert!(
            text.contains("\"demo\""),
            "zim should be listed, got: {text}"
        );
        assert!(
            !text.contains("demo.zim"),
            "unauthenticated file_path should be redacted, got: {text}"
        );
        assert!(
            text.contains("[redacted]"),
            "expected [redacted], got: {text}"
        );
    }

    #[tokio::test]
    async fn list_password_mode_authenticated_shows_file_path() {
        let mut m = crate::settings::default_settings();
        m.insert(KEY_ACCESS_MODE.into(), serde_json::json!("password"));
        m.insert(
            KEY_ACCESS_ADMIN_PASSWORD.into(),
            serde_json::json!("testpass"),
        );
        let state = list_state_with_zim(m).await;
        let app = build_router(state);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/list")
                    .header(axum::http::header::AUTHORIZATION, "Bearer testpass")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let text = body_text(resp).await;
        assert!(
            text.contains("demo.zim"),
            "authenticated caller should see file_path, got: {text}"
        );
        assert!(
            !text.contains("[redacted]"),
            "no redaction when authenticated, got: {text}"
        );
    }

    #[tokio::test]
    async fn list_open_mode_redacts_file_path() {
        // Open mode has no shared secret → every caller is unauthenticated
        // (settings_authed is false) → file_path redacted.
        let m = crate::settings::default_settings(); // mode = open
        let state = list_state_with_zim(m).await;
        let app = build_router(state);
        let resp = app
            .oneshot(Request::builder().uri("/list").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let text = body_text(resp).await;
        assert!(
            text.contains("[redacted]"),
            "open mode redacts file_path, got: {text}"
        );
        assert!(
            !text.contains("demo.zim"),
            "open mode must not leak file_path, got: {text}"
        );
    }

    // ── Per-ZIM settings 404 ─────────────────────────────────────────────────

    #[tokio::test]
    async fn put_zim_settings_db_unavailable_returns_503() {
        // Dead pool → existence check queries DB → 503. The body uses a valid
        // key/type so the up-front validation (BUG-16b) passes and the call
        // actually reaches the DB (→ 503, not a 400).
        let app = build_router(test_state());
        let body = serde_json::json!({ "embed_enabled": true });
        let resp = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/settings/zim/nonexistent-zim-xyz")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn put_zim_settings_type_mismatch_returns_400() {
        // BUG-16b: `embed_enabled` must be a boolean — a string 400s before
        // any DB touch (holds on the dead pool too).
        let app = build_router(test_state());
        let body = serde_json::json!({ "embed_enabled": "yes" });
        let resp = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/settings/zim/some-zim")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let text = body_text(resp).await;
        assert!(text.contains("embed_enabled"), "body: {text}");
    }

    #[tokio::test]
    async fn put_zim_settings_unknown_key_returns_400() {
        // BUG-16b: an unknown key 400s before any DB touch.
        let app = build_router(test_state());
        let body = serde_json::json!({ "is_favorite": true });
        let resp = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/settings/zim/some-zim")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let text = body_text(resp).await;
        assert!(text.contains("is_favorite"), "body: {text}");
    }

    #[tokio::test]
    async fn put_zim_settings_non_object_body_returns_400() {
        // A non-object body (array/scalar/null) is not a settings update —
        // 400 before any DB touch (the old `if let Some(obj)` skipped
        // validation entirely and returned a fake `ok:true` no-op).
        let app = build_router(test_state());
        for body in [
            serde_json::json!([1, 2]),
            serde_json::json!("some-zim"),
            serde_json::json!(null),
        ] {
            let resp = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("PUT")
                        .uri("/settings/zim/some-zim")
                        .header("content-type", "application/json")
                        .body(Body::from(serde_json::to_string(&body).unwrap()))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(
                resp.status(),
                StatusCode::BAD_REQUEST,
                "body {body} must 400"
            );
            let text = body_text(resp).await;
            assert!(text.contains("expected a JSON object"), "body: {text}");
        }
    }

    #[tokio::test]
    async fn put_collection_empty_label_returns_400() {
        // BUG-16a: a whitespace-only label 400s before any DB touch.
        let app = build_router(test_state());
        let body = serde_json::json!({ "label": "   " });
        let resp = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/collections/1")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let text = body_text(resp).await;
        assert!(text.contains("label cannot be empty"), "body: {text}");
    }

    #[tokio::test]
    async fn auth_middleware_fail_closed_body() {
        // BUG-16d: password mode + empty `admin_password` → fail-closed 503 on
        // a non-health route, with the misconfiguration message in the body.
        let state = fail_closed_state();
        let app = build_router(state);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/settings")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        let text = body_text(resp).await;
        assert!(
            text.contains("authentication is misconfigured"),
            "body: {text}"
        );
    }

    #[tokio::test]
    async fn put_settings_unauthenticated_sensitive_key_returns_403() {
        // Open mode = unauthenticated writer. A write to a security-sensitive
        // key is rejected with 403 **before** any DB round-trip (a DB error
        // would surface as 503, masking the auth decision) — so this holds on
        // the dead pool too.
        let app = build_router(test_state());
        let body = serde_json::json!({ KEY_TORRENT_URL: "http://127.0.0.1:9" });
        let resp = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/settings")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
        let text = body_text(resp).await;
        assert!(text.contains("security-sensitive"), "body: {text}");
    }

    #[tokio::test]
    async fn put_settings_unauthenticated_non_sensitive_key_not_403() {
        // A non-sensitive write in open mode is allowed past the auth gate; on
        // the dead pool it reaches the DB and surfaces 503 (not 403).
        let app = build_router(test_state());
        let body = serde_json::json!({ KEY_SEARCH_MAX_LIMIT: 500 });
        let resp = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/settings")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn read_article_bad_zim_returns_404() {
        let app = build_router(test_state());
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/read?zim=no-such-zim&path=A/foo")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    // ── Content/search dead-pool 503s (DB point-lookups) ────────────────────

    #[tokio::test]
    async fn snippet_dead_pool_returns_503() {
        // `/snippet` reads the DB for the article's snippet → dead pool → 503.
        let app = build_router(test_state());
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/snippet?zim=x&path=y")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn suggest_dead_pool_returns_503() {
        // `/suggest` runs an ILIKE over the DB → dead pool → 503 (the required
        // `q` param must be present or it 400s before the DB is touched).
        let app = build_router(test_state());
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/suggest?q=x")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn interlanguage_dead_pool_returns_503() {
        // `/interlanguage` resolves Q-IDs in the DB → dead pool → 503.
        let app = build_router(test_state());
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/interlanguage?zim=x&path=y")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn chunks_bad_zim_returns_404() {
        // `/chunks` opens the ZIM from the in-memory handle cache *before* any
        // DB access, so a missing ZIM 404s even on a dead pool.
        let app = build_router(test_state());
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/chunks?zim=no-such-zim&path=A/foo")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    // ── Search limit clamping ────────────────────────────────────────────────

    #[tokio::test]
    async fn search_limit_clamped_no_panic() {
        // limit=99999 is clamped to max_limit before reaching the engine.
        // H3 delta: a dead pool now surfaces as 503 (pool-get failure
        // propagates) instead of soft-failing to an empty 200 — the
        // accepted contract change from P3. Invariant here: no panic, no
        // 4xx, and the clamped-limit path (no i32 overflow) is exercised.
        let app = build_router(test_state());
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/search?q=test&limit=99999")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "dead pool → 503, no panic/overflow"
        );
    }

    // ── CORS preflight ───────────────────────────────────────────────────────

    #[tokio::test]
    async fn cors_preflight_disallowed_origin_no_acao() {
        let app = build_router(test_state());
        let resp = app
            .oneshot(
                Request::builder()
                    .method("OPTIONS")
                    .uri("/search")
                    .header("origin", "https://evil.example")
                    .header("access-control-request-method", "GET")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        // With empty CORS origins, no Access-Control-Allow-Origin header
        // should be present.
        assert!(
            resp.headers().get("access-control-allow-origin").is_none(),
            "expected no ACAO header for disallowed origin, got: {:?}",
            resp.headers().get("access-control-allow-origin")
        );
    }

    // ── Search q-length cap (WP3.6) ───────────────────────────────────────

    #[tokio::test]
    async fn search_q_over_cap_returns_400() {
        // 501 chars (one over the 500 cap) → 400 with the cap message, before
        // any DB work (the dead pool is never touched).
        let app = build_router(test_state());
        let q = "a".repeat(501);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri(format!("/search?q={q}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let body = body_text(resp).await;
        assert!(
            body.contains("exceeds 500 characters"),
            "expected cap message, got: {body}"
        );
    }

    #[tokio::test]
    async fn search_q_at_cap_not_rejected_by_cap() {
        // Exactly 500 chars is allowed past the cap. It then proceeds to the
        // (dead) DB, so the response is a 503 DB error — not the cap's 400.
        // Pinning the status makes the claim explicit: we reached the DB
        // layer (503) rather than being rejected at the cap (400).
        let app = build_router(test_state());
        let q = "a".repeat(500);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri(format!("/search?q={q}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "a 500-char query must reach the (dead) DB → 503, not trip the cap → 400"
        );
        let body = body_text(resp).await;
        assert!(
            !body.contains("exceeds 500 characters"),
            "500-char query must not trip the cap, got: {body}"
        );
    }

    #[tokio::test]
    async fn suggest_q_over_cap_returns_400() {
        let app = build_router(test_state());
        let q = "b".repeat(501);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri(format!("/suggest?q={q}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let body = body_text(resp).await;
        assert!(body.contains("exceeds 500 characters"), "got: {body}");
    }

    // ── Range: zero-length → 416 ─────────────────────────────────────────────

    #[tokio::test]
    async fn raw_content_bad_zim_returns_404() {
        let app = build_router(test_state());
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/w/no-such-zim/A/foo.html")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    // `/w` raw content is served cross-origin so the web UI's plain-anchor
    // article links (new-tab navigation) and third-party readers can open
    // `/w/…` without a same-origin requirement. With no origins configured
    // (the test default), the global CORS layer emits no headers — which is
    // correct: cross-origin *simple* GETs need no CORS headers at all. For
    // preflight (custom headers / non-simple methods), operators add the
    // reader's origin to `general.cors_origins` and the global layer handles it.
    // SEC-L1: the old route-level `CorsLayer(Any)` was removed because it
    // conflicted with the global layer (duplicate
    // `Access-Control-Allow-Origin` headers).
    #[tokio::test]
    async fn raw_content_no_cors_headers_when_no_origins_configured() {
        let app = build_router(test_state());
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/w/no-such-zim/A/foo.html")
                    .header(axum::http::header::ORIGIN, "http://example.com")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        // With no origins configured, no CORS headers are emitted. This is
        // intentional: plain cross-origin GETs need no CORS headers.
        assert!(
            resp.headers()
                .get(axum::http::header::ACCESS_CONTROL_ALLOW_ORIGIN)
                .is_none(),
            "no CORS header expected when no origins are configured"
        );
    }

    // ── Web UI ─────────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn web_index_returns_200_html() {
        let app = build_router(test_state());
        let resp = app
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(resp
            .headers()
            .get(axum::http::header::CONTENT_TYPE)
            .unwrap()
            .to_str()
            .unwrap()
            .starts_with("text/html"));
        let text = body_text(resp).await;
        assert!(
            text.contains("Library — zimservice"),
            "index page title missing: {text}"
        );
        // The served page is cache-busted: the common.js tag carries the
        // crate-version stamp (see `stamp_assets` in handlers/web.rs).
        assert!(
            text.contains(&format!(
                "<script src=\"/common.js?v={}\">",
                env!("CARGO_PKG_VERSION")
            )),
            "index page must load common.js"
        );
    }

    #[tokio::test]
    async fn web_search_returns_200_html() {
        let app = build_router(test_state());
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/search.html")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(resp
            .headers()
            .get(axum::http::header::CONTENT_TYPE)
            .unwrap()
            .to_str()
            .unwrap()
            .starts_with("text/html"));
        let text = body_text(resp).await;
        assert!(
            text.contains("Search — zimservice"),
            "search page title missing: {text}"
        );
        assert!(
            text.contains(&format!(
                "<script src=\"/common.js?v={}\">",
                env!("CARGO_PKG_VERSION")
            )),
            "search page must load common.js"
        );
    }

    #[tokio::test]
    async fn web_settings_returns_200_html() {
        let app = build_router(test_state());
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/settings.html")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(resp
            .headers()
            .get(axum::http::header::CONTENT_TYPE)
            .unwrap()
            .to_str()
            .unwrap()
            .starts_with("text/html"));
        let text = body_text(resp).await;
        assert!(
            text.contains("Settings — zimservice"),
            "settings page title missing: {text}"
        );
        assert!(
            text.contains(&format!(
                "<script src=\"/common.js?v={}\">",
                env!("CARGO_PKG_VERSION")
            )),
            "settings page must load common.js"
        );
    }

    #[tokio::test]
    async fn web_common_js_returns_200_js() {
        let app = build_router(test_state());
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/common.js")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers()
                .get(axum::http::header::CONTENT_TYPE)
                .unwrap()
                .to_str()
                .unwrap(),
            "application/javascript"
        );
        let text = body_text(resp).await;
        // Every shared helper the pages depend on must be present.
        for marker in [
            "function esc",
            "function apiFetch",
            "function toast",
            "function fmtBytes",
            "function fmtNum",
            "function fmtEta",
        ] {
            assert!(text.contains(marker), "common.js missing {marker}");
        }
        // Regression guard for the duplicated-header bug: the file must
        // define `esc` exactly once (the bug duplicated lines 1–5, leaving
        // the first `esc` body unclosed and hiding every other function).
        assert_eq!(
            text.matches("function esc(s)").count(),
            1,
            "common.js must define `function esc(s)` exactly once"
        );
        // No duplication of the `'use strict';` header either.
        assert_eq!(text.matches("'use strict';").count(), 1);
    }

    #[tokio::test]
    async fn web_common_js_has_cache_control() {
        // Locks in the deploy-time-only cache policy (requires Step 5).
        let app = build_router(test_state());
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/common.js")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers()
                .get(axum::http::header::CACHE_CONTROL)
                .unwrap()
                .to_str()
                .unwrap(),
            "public, max-age=300"
        );
    }

    /// A password-mode state with a known password; `require_reads` controls
    /// the M1 read-gating flag. (Plaintext password → fast verify, no 100k
    /// hash in tests.)
    fn password_state(require_reads: bool) -> AppState {
        let mut values = crate::settings::default_settings();
        values.insert(KEY_ACCESS_MODE.into(), serde_json::json!("password"));
        values.insert(KEY_ACCESS_ADMIN_PASSWORD.into(), serde_json::json!("pw"));
        values.insert(
            KEY_ACCESS_REQUIRE_AUTH_FOR_READS.into(),
            serde_json::json!(require_reads),
        );
        crate::testing::test_state_with_settings(values)
    }

    /// AppState like `password_state` but with an empty
    /// `access.admin_password` — the fail-closed 503 configuration (BUG-2).
    fn fail_closed_state() -> AppState {
        let mut values = crate::settings::default_settings();
        values.insert(KEY_ACCESS_MODE.into(), serde_json::json!("password"));
        values.insert(KEY_ACCESS_ADMIN_PASSWORD.into(), serde_json::json!(""));
        crate::testing::test_state_with_settings(values)
    }

    // ── BUG-2: /health exempt from the fail-closed 503 ───────────────────

    #[tokio::test]
    async fn auth_middleware_health_exempt_in_fail_closed() {
        let app = build_router(fail_closed_state());
        // /health must NOT hit the middleware's misconfigured-auth 503 — it
        // reaches the health handler, which (dead pool) reports its own
        // degraded 503. The 200-"ok" case needs a live pool (WI-50 listener).
        let health = app
            .clone()
            .oneshot(get_with_token("/health", None))
            .await
            .unwrap();
        assert_eq!(health.status(), StatusCode::SERVICE_UNAVAILABLE);
        let hv: serde_json::Value = serde_json::from_str(&body_text(health).await).unwrap();
        assert_eq!(hv["status"], "degraded");
        assert_eq!(hv["db_connected"], false);
        // Every other route keeps the fail-closed 503.
        let settings = app
            .clone()
            .oneshot(get_with_token("/settings", None))
            .await
            .unwrap();
        assert_eq!(settings.status(), StatusCode::SERVICE_UNAVAILABLE);
        let sv: serde_json::Value = serde_json::from_str(&body_text(settings).await).unwrap();
        assert_eq!(
            sv["error"], "authentication is misconfigured",
            "non-health routes must keep the fail-closed 503 body"
        );
    }

    /// Build a router request with an optional Bearer token.
    fn get_with_token(uri: &str, token: Option<&str>) -> Request<Body> {
        let mut b = Request::builder().uri(uri);
        if let Some(t) = token {
            b = b.header("Authorization", format!("Bearer {t}"));
        }
        b.body(Body::empty()).unwrap()
    }

    // ── M1: read-gating (access.require_auth_for_reads) ──────────────────

    #[tokio::test]
    async fn m1_flag_on_read_without_token_is_401() {
        let app = build_router(password_state(true));
        let resp = app
            .oneshot(get_with_token("/settings", None))
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::UNAUTHORIZED,
            "flag on + no token → 401"
        );
    }

    #[tokio::test]
    async fn m1_flag_on_read_with_token_is_200() {
        let app = build_router(password_state(true));
        let resp = app
            .oneshot(get_with_token("/settings", Some("pw")))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "flag on + valid token → 200");
    }

    #[tokio::test]
    async fn m1_flag_on_read_with_wrong_token_is_401() {
        let app = build_router(password_state(true));
        let resp = app
            .oneshot(get_with_token("/settings", Some("nope")))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn m1_flag_off_read_stays_open() {
        // Default (flag off): password mode gates only mutations; GETs open.
        let app = build_router(password_state(false));
        let resp = app
            .oneshot(get_with_token("/settings", None))
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "flag off + no token → 200 (reads stay open)"
        );
    }

    #[tokio::test]
    async fn m1_flag_on_write_still_gated() {
        // Flag on must not weaken write gating: PUT without token → 401.
        let app = build_router(password_state(true));
        let req = Request::builder()
            .method("PUT")
            .uri("/settings")
            .header("Content-Type", "application/json")
            .body(Body::from("{}"))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // ── SEC-M3: `?access_token=` restricted to read verbs ──────────────────

    #[tokio::test]
    async fn put_settings_query_token_rejected() {
        // A query-string token on a mutating verb must be ignored by the
        // middleware gate (Bearer-only for writes) → 401, not a write.
        let app = build_router(password_state(false));
        let req = Request::builder()
            .method("PUT")
            .uri("/settings?access_token=pw")
            .header("Content-Type", "application/json")
            .body(Body::from(r#"{"downloads.max_bytes": 1073741824}"#))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::UNAUTHORIZED,
            "query token on a mutating verb → 401"
        );
    }

    #[tokio::test]
    async fn put_settings_bearer_token_passes_auth() {
        // The Bearer path stays intact: auth passes the gate, and the dead
        // pool then 503s the write (proving we got past the 401 gate).
        let app = build_router(password_state(false));
        let req = Request::builder()
            .method("PUT")
            .uri("/settings")
            .header("Authorization", "Bearer pw")
            .header("Content-Type", "application/json")
            .body(Body::from(r#"{"downloads.max_bytes": 1073741824}"#))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "Bearer auth must pass the gate; the dead pool then 503s"
        );
    }

    #[tokio::test]
    async fn get_settings_query_token_still_accepted() {
        // Read verbs still honor the query-string form (no regression).
        let app = build_router(password_state(false));
        let resp = app
            .oneshot(get_with_token("/settings?access_token=pw", None))
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "read with a query token stays 200"
        );
    }

    // ── P2: chunk clamp (B8), download-name fragment strip (B9), DB-fallback
    //    truncated (B12) ─────────────────────────────────────────────────────

    use crate::content::{clamp_chunk_params, read_response_truncated};
    use crate::serve::handlers::default_download_name;

    #[test]
    fn clamp_chunk_params_bounds_size_and_overlap() {
        assert_eq!(clamp_chunk_params(0, 5), (10, 5)); // floor; overlap fits under size/2
        assert_eq!(clamp_chunk_params(1000, 200), (1000, 200)); // in-range unchanged
        assert_eq!(clamp_chunk_params(10, 999), (10, 5)); // overlap capped to size/2
        assert_eq!(clamp_chunk_params(5, 100), (10, 5)); // size floored first
        assert_eq!(clamp_chunk_params(1_000_000, 0), (100_000, 0)); // ceiling
        assert_eq!(
            clamp_chunk_params(usize::MAX, usize::MAX),
            (100_000, 50_000)
        ); // no panic; overlap capped at size/2
        assert_eq!(clamp_chunk_params(100_000, 100_000), (100_000, 50_000)); // overlap ≤ size/2
        assert_eq!(clamp_chunk_params(1_000_000, 999_999), (100_000, 50_000)); // size ceiling first
    }

    #[test]
    fn clamp_chunk_params_overlap_capped_at_half_size() {
        // The DoS shape from B8: near-equal size/overlap used to allow
        // ~len/(size-overlap) windows; with overlap ≤ size/2 every window
        // advances by at least size/2.
        assert_eq!(clamp_chunk_params(100_000, 99_999), (100_000, 50_000));
        assert_eq!(clamp_chunk_params(1_000, 999), (1_000, 500));
        assert_eq!(clamp_chunk_params(100_000, 50_000), (100_000, 50_000)); // boundary stays
        assert_eq!(clamp_chunk_params(100_000, 49_999), (100_000, 49_999)); // under cap stays
    }

    #[test]
    fn default_download_name_strips_query_and_fragment() {
        assert_eq!(default_download_name("https://h/a.zim"), "a.zim");
        assert_eq!(default_download_name("https://h/a.zim?q=1"), "a.zim");
        assert_eq!(default_download_name("https://h/a#frag"), "a"); // B9: fragment stripped
        assert_eq!(default_download_name("https://h/a?q=1#f"), "a"); // both stripped
        assert_eq!(default_download_name("https://h/a?q=1#f/sub"), "sub"); // fragment in a later seg
        assert_eq!(default_download_name("https://h/"), ""); // empty last seg
    }

    #[test]
    fn read_response_truncated_db_preview_cap() {
        // Live ZIM read: truncation only from max_len.
        assert!(!read_response_truncated("zim", 500, 8000));
        assert!(read_response_truncated("zim", 9000, 8000));
        // DB fallback: a preview that hit the 2000-char cap is truncated even
        // when max_len is larger than the preview.
        assert!(read_response_truncated(
            "db",
            crate::zim::index::PREVIEW_CHARS,
            8000
        ));
        assert!(!read_response_truncated(
            "db",
            crate::zim::index::PREVIEW_CHARS - 1,
            8000
        ));
        // max_len still applies on top.
        assert!(read_response_truncated("db", 100, 50));
    }

    #[tokio::test]
    async fn router_serves_all_registered_api_paths() {
        // Two-sided pinning, side 2 (ARCH-4): every registered route that
        // cannot 404 by handler logic on a dead pool must actually be served
        // by `build_router` (dead pool → 503/200; unregistered → 404). The 7
        // ZIM-scoped paths (`/read`, `/chunks`, `/snippet`, `/interlanguage`,
        // `/w/{…}`, `/random`, `/settings/zim/{name}`) are excluded — they 404
        // via the empty `ZimManager`/handler logic before any DB round-trip.
        let app = build_router(test_state());
        let routes: [(axum::http::Method, &str); 14] = [
            (axum::http::Method::GET, "/health"),
            (axum::http::Method::GET, "/list"),
            (axum::http::Method::GET, "/search"),
            (axum::http::Method::GET, "/suggest"),
            (axum::http::Method::GET, "/settings"),
            (axum::http::Method::GET, "/downloads"),
            (axum::http::Method::DELETE, "/downloads/1"),
            (axum::http::Method::GET, "/"),
            (axum::http::Method::GET, "/search.html"),
            (axum::http::Method::GET, "/settings.html"),
            (axum::http::Method::GET, "/common.js"),
            (axum::http::Method::GET, "/openapi.json"),
            (axum::http::Method::GET, "/collections"),
            (axum::http::Method::PUT, "/collections/1"),
        ];
        for (method, path) in routes {
            let resp = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(method)
                        .uri(path)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_ne!(
                resp.status(),
                StatusCode::NOT_FOUND,
                "registered route {path} must be reachable (not 404); got {}",
                resp.status()
            );
        }
    }

    // ── Downloads: BUG-4 URL length cap ────────────────────────────────────

    #[tokio::test]
    async fn create_download_url_too_long_rejected() {
        // >2048 bytes → 400 pre-DB (the dead pool guarantees no row is created).
        let app = build_router(test_state());
        let body = serde_json::json!({
            "url": format!("http://testhost/x.{}", "a".repeat(2040)),
        });
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/downloads")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let text = body_text(resp).await;
        assert!(
            text.contains("url is too long"),
            "expected length error, got: {text}"
        );
    }

    #[tokio::test]
    async fn create_download_url_at_limit_not_length_rejected() {
        // Exactly 2048 bytes passes the length guard; the dead pool then 503s
        // (proving the 400 length path did not fire).
        let app = build_router(test_state());
        let body = serde_json::json!({
            "url": format!("http://testhost/x.{}.zim", "a".repeat(2048 - 22)),
            // Explicit short name: name validation now runs before the DB
            // touch, so it must pass for the request to reach the dead pool
            // (503) and isolate the URL-length guard this test is about.
            "name": "a.zim",
        });
        // "http://testhost/x." + ".zim" is 22 chars → total exactly 2048;
        // a .zim URL skips the qBittorrent branch so the dead pool 503s.
        assert_eq!(body["url"].as_str().unwrap().len(), 2048);
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/downloads")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_ne!(
            resp.status(),
            StatusCode::BAD_REQUEST,
            "a 2048-byte url must pass the length guard (dead pool → 503 expected)"
        );
    }
}
