//! Raw domain integration tests.

use super::common::*;

/// `GET /w/{zim}/{path}` honors RFC 7233 byte ranges: a satisfiable range
/// returns 206 with a `Content-Range` header; an out-of-range one returns 416.
///
/// The ZIM handle cache is **in-memory**, so the test builds its own `AppState`
/// whose `ZimManager` points at `tests/fixtures` (a committed one-article
/// `tiny.zim`) and calls `resync()` to populate the cache — the plain
/// `live_state()` (a `/nonexistent-handler` dir) would 404 on the ZIM miss
/// before any DB access.
///
/// `tests/fixtures/tiny.zim` is the `nons/small.zim` fixture from the openZIM
/// testing suite (libzim's test corpus, BSD-2-Clause licensed):
/// https://github.com/openzim/zim-testing-suite/blob/main/data/nons/small.zim
/// It is format v6.1 with 16 entries. `main.html` (UserContent namespace) is
/// its only article and is **207 bytes** long; the ranges below are chosen
/// against that total.
#[tokio::test]
async fn raw_content_range_206() {
    let (pool, _db_gate) = match pool_or_skip().await {
        Some(p) => p,
        None => return,
    };
    run_migrations(&pool).await.expect("migrations");
    const ZIM: &str = "tiny";
    const PATH: &str = "main.html";

    // Start from a clean slate for the fixture's zims row (resync will upsert it).
    zimservice::db::raw::execute(&pool, "DELETE FROM zims WHERE name = $1", |q| q.bind(ZIM))
        .await
        .unwrap();

    let zims = ZimManager::new(std::path::PathBuf::from(FIXTURES_DIR), pool.clone());
    zims.resync().await.expect("resync");
    assert!(
        zims.get(ZIM).is_some(),
        "resync must cache the committed fixture"
    );

    let settings = SettingsCache::load(pool.clone(), HashMap::new(), HashMap::new())
        .await
        .expect("settings load");
    let search = SearchEngine::new(
        pool.clone(),
        settings.clone(),
        zimservice::health::DegradationTracker::default(),
    );
    let state = assemble_state(pool.clone(), settings, zims, search);

    use zimservice::serve::handlers::{raw_content, RAW_CONTENT_CSP};

    // (a) Satisfiable range → 206 with the correct Content-Range.
    let mut h = axum::http::HeaderMap::new();
    h.insert(axum::http::header::RANGE, "bytes=0-10".parse().unwrap());
    let resp = raw_content(
        axum::extract::State(state.clone()),
        h,
        axum::extract::Path((ZIM.to_string(), PATH.to_string())),
    )
    .await
    .expect("raw_content 206");
    let status = resp.status();
    assert_eq!(
        status,
        axum::http::StatusCode::PARTIAL_CONTENT,
        "satisfiable range → 206"
    );
    assert_eq!(
        resp.headers()
            .get(axum::http::header::CONTENT_RANGE)
            .and_then(|v| v.to_str().ok()),
        Some("bytes 0-10/207"),
        "Content-Range must name the entry's total length"
    );
    // SEC-M2: `text/html` raw content must carry the sandboxing CSP + DENY.
    assert_eq!(
        resp.headers()
            .get(axum::http::header::CONTENT_SECURITY_POLICY)
            .and_then(|v| v.to_str().ok()),
        Some(RAW_CONTENT_CSP),
        "text/html 206 must carry the sandbox CSP"
    );
    assert_eq!(
        resp.headers()
            .get(axum::http::header::X_FRAME_OPTIONS)
            .and_then(|v| v.to_str().ok()),
        Some("DENY"),
        "text/html 206 must refuse framing"
    );
    // PERF-7: a satisfiable 206 response carries the file-level ETag so
    // clients can revalidate without re-fetching the body.
    assert!(
        resp.headers().get(axum::http::header::ETAG).is_some(),
        "206 must carry a file-level ETag"
    );
    let bytes = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();
    assert_eq!(
        bytes.len(),
        11,
        "206 body must be exactly the requested 11 bytes"
    );

    // (a1) PERF-1: a mid-range 206 body must be exactly the slice length
    // (end - start + 1), proving the served body is the slice, not a
    // full-body leak. `bytes=100-150` → 51 bytes of a 207-byte entry.
    let mut h_mid = axum::http::HeaderMap::new();
    h_mid.insert(axum::http::header::RANGE, "bytes=100-150".parse().unwrap());
    let resp = raw_content(
        axum::extract::State(state.clone()),
        h_mid,
        axum::extract::Path((ZIM.to_string(), PATH.to_string())),
    )
    .await
    .expect("raw_content 206 (mid-range)");
    assert_eq!(
        resp.status(),
        axum::http::StatusCode::PARTIAL_CONTENT,
        "mid-range → 206"
    );
    assert_eq!(
        resp.headers()
            .get(axum::http::header::CONTENT_RANGE)
            .and_then(|v| v.to_str().ok()),
        Some("bytes 100-150/207"),
        "mid-range Content-Range must name 100-150/207"
    );
    let mid_bytes = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();
    assert_eq!(
        mid_bytes.len(),
        51,
        "mid-range 206 body must be exactly 51 bytes"
    );

    // (a2) No `Range` header → 200 full body, with the same sandbox headers.
    let h_no = axum::http::HeaderMap::new();
    let resp = raw_content(
        axum::extract::State(state.clone()),
        h_no,
        axum::extract::Path((ZIM.to_string(), PATH.to_string())),
    )
    .await
    .expect("raw_content 200");
    assert_eq!(resp.status(), axum::http::StatusCode::OK, "no Range → 200");
    assert_eq!(
        resp.headers()
            .get(axum::http::header::CONTENT_SECURITY_POLICY)
            .and_then(|v| v.to_str().ok()),
        Some(RAW_CONTENT_CSP),
        "text/html 200 must carry the sandbox CSP"
    );
    assert_eq!(
        resp.headers()
            .get(axum::http::header::X_FRAME_OPTIONS)
            .and_then(|v| v.to_str().ok()),
        Some("DENY"),
        "text/html 200 must refuse framing"
    );

    // (b) Out-of-range start (>= total) → 416.
    let mut h2 = axum::http::HeaderMap::new();
    h2.insert(axum::http::header::RANGE, "bytes=500-".parse().unwrap());
    let resp = raw_content(
        axum::extract::State(state.clone()),
        h2,
        axum::extract::Path((ZIM.to_string(), PATH.to_string())),
    )
    .await
    .expect("raw_content 416");
    assert_eq!(
        resp.status(),
        axum::http::StatusCode::RANGE_NOT_SATISFIABLE,
        "out-of-range start → 416"
    );

    // (b2) BUG-15: a start offset that overflows u64 (RFC 7233 §3.1) is beyond
    // EOF → 416 with `Content-Range: bytes */207` (not a 500, not a 200).
    let mut h3 = axum::http::HeaderMap::new();
    h3.insert(
        axum::http::header::RANGE,
        "bytes=18446744073709551616-".parse().unwrap(),
    );
    let resp = raw_content(
        axum::extract::State(state.clone()),
        h3,
        axum::extract::Path((ZIM.to_string(), PATH.to_string())),
    )
    .await
    .expect("raw_content 416 (overflow start)");
    assert_eq!(
        resp.status(),
        axum::http::StatusCode::RANGE_NOT_SATISFIABLE,
        "overflow start → 416"
    );
    assert_eq!(
        resp.headers()
            .get(axum::http::header::CONTENT_RANGE)
            .and_then(|v| v.to_str().ok()),
        Some("bytes */207"),
        "overflow start → Content-Range: bytes */207"
    );

    // Cleanup: drop the fixture's zims row so the harness stays idempotent.
    zimservice::db::raw::execute(&pool, "DELETE FROM zims WHERE name = $1", |q| q.bind(ZIM))
        .await
        .unwrap();
}

/// W6.1: a matching `If-None-Match` revalidates to 304 *before* any entry
/// lookup — so even a path that does not exist in the ZIM returns 304 (a real
/// lookup would 404). The stat+304 moved into the blocking read (off the async
/// worker). DB-gated.
#[tokio::test]
async fn read_raw_entry_not_modified_before_entry_lookup() {
    let (pool, _db_gate) = match pool_or_skip().await {
        Some(p) => p,
        None => return,
    };
    run_migrations(&pool).await.expect("migrations");
    const ZIM: &str = "tiny";
    const PATH: &str = "main.html";

    zimservice::db::raw::execute(&pool, "DELETE FROM zims WHERE name = $1", |q| q.bind(ZIM))
        .await
        .unwrap();
    let zims = ZimManager::new(std::path::PathBuf::from(FIXTURES_DIR), pool.clone());
    zims.resync().await.expect("resync");
    let settings = SettingsCache::load(pool.clone(), HashMap::new(), HashMap::new())
        .await
        .expect("settings load");
    let search = SearchEngine::new(
        pool.clone(),
        settings.clone(),
        zimservice::health::DegradationTracker::default(),
    );
    let state = assemble_state(pool.clone(), settings, zims, search);
    use zimservice::serve::handlers::raw_content;

    // (1) A real 200 → capture the ETag.
    let h0 = axum::http::HeaderMap::new();
    let resp = raw_content(
        axum::extract::State(state.clone()),
        h0,
        axum::extract::Path((ZIM.to_string(), PATH.to_string())),
    )
    .await
    .expect("raw_content 200");
    assert_eq!(resp.status(), axum::http::StatusCode::OK);
    let etag = resp
        .headers()
        .get(axum::http::header::ETAG)
        .and_then(|v| v.to_str().ok())
        .expect("200 carries an ETag")
        .to_string();

    // (2) Matching If-None-Match on a NON-EXISTENT path → 304 (not 404): the
    // revalidation short-circuits before the entry lookup.
    let mut h1 = axum::http::HeaderMap::new();
    h1.insert(axum::http::header::IF_NONE_MATCH, etag.parse().unwrap());
    let resp = raw_content(
        axum::extract::State(state.clone()),
        h1,
        axum::extract::Path((ZIM.to_string(), "no/such/path.html".to_string())),
    )
    .await
    .expect("raw_content 304");
    assert_eq!(
        resp.status(),
        axum::http::StatusCode::NOT_MODIFIED,
        "matching If-None-Match revalidates to 304 before the entry lookup"
    );

    zimservice::db::raw::execute(&pool, "DELETE FROM zims WHERE name = $1", |q| q.bind(ZIM))
        .await
        .unwrap();
}

/// PERF-7 (WI-39): ETag + If-None-Match revalidation on `/w` raw content.
/// (a) GET → 200 + ETag present (capture); (b) matching If-None-Match → 304,
/// ETag echoed, empty body; (c) `Range` + matching If-None-Match → 304 with
/// no body; (d) a bogus tag → 200 (revalidation miss). DB-gated.
#[tokio::test]
async fn raw_content_etag_304() {
    let (pool, _db_gate) = match pool_or_skip().await {
        Some(p) => p,
        None => return,
    };
    run_migrations(&pool).await.expect("migrations");
    const ZIM: &str = "tiny";
    const PATH: &str = "main.html";

    // Start from a clean slate for the fixture's zims row (resync will upsert it).
    zimservice::db::raw::execute(&pool, "DELETE FROM zims WHERE name = $1", |q| q.bind(ZIM))
        .await
        .unwrap();

    let zims = ZimManager::new(std::path::PathBuf::from(FIXTURES_DIR), pool.clone());
    zims.resync().await.expect("resync");
    assert!(
        zims.get(ZIM).is_some(),
        "resync must cache the committed fixture"
    );

    let settings = SettingsCache::load(pool.clone(), HashMap::new(), HashMap::new())
        .await
        .expect("settings load");
    let search = SearchEngine::new(
        pool.clone(),
        settings.clone(),
        zimservice::health::DegradationTracker::default(),
    );
    let state = assemble_state(pool.clone(), settings, zims, search);

    use zimservice::serve::handlers::raw_content;

    // (a) Plain GET → 200 + a file-level ETag.
    let h = axum::http::HeaderMap::new();
    let resp = raw_content(
        axum::extract::State(state.clone()),
        h,
        axum::extract::Path((ZIM.to_string(), PATH.to_string())),
    )
    .await
    .expect("raw_content 200");
    assert_eq!(resp.status(), axum::http::StatusCode::OK, "GET → 200");
    let etag = resp
        .headers()
        .get(axum::http::header::ETAG)
        .and_then(|v| v.to_str().ok())
        .expect("200 must carry an ETag")
        .to_string();
    assert!(etag.starts_with('"'), "strong tag is quoted: {etag}");

    // (b) Matching If-None-Match → 304, ETag echoed, empty body.
    let mut h_inm = axum::http::HeaderMap::new();
    h_inm.insert(axum::http::header::IF_NONE_MATCH, etag.parse().unwrap());
    let resp = raw_content(
        axum::extract::State(state.clone()),
        h_inm,
        axum::extract::Path((ZIM.to_string(), PATH.to_string())),
    )
    .await
    .expect("raw_content 304");
    assert_eq!(
        resp.status(),
        axum::http::StatusCode::NOT_MODIFIED,
        "matching If-None-Match → 304"
    );
    assert_eq!(
        resp.headers()
            .get(axum::http::header::ETAG)
            .and_then(|v| v.to_str().ok()),
        Some(etag.as_str()),
        "304 must echo the ETag"
    );
    let body = axum::body::to_bytes(resp.into_body(), 4096).await.unwrap();
    assert!(body.is_empty(), "304 body must be empty");

    // (c) Range + matching If-None-Match → 304 with no body (entity-level
    // revalidation, RFC 7233).
    let mut h_range_inm = axum::http::HeaderMap::new();
    h_range_inm.insert(axum::http::header::RANGE, "bytes=0-10".parse().unwrap());
    h_range_inm.insert(axum::http::header::IF_NONE_MATCH, etag.parse().unwrap());
    let resp = raw_content(
        axum::extract::State(state.clone()),
        h_range_inm,
        axum::extract::Path((ZIM.to_string(), PATH.to_string())),
    )
    .await
    .expect("raw_content 304 (ranged)");
    assert_eq!(
        resp.status(),
        axum::http::StatusCode::NOT_MODIFIED,
        "ranged matching If-None-Match → 304 (no 206)"
    );

    // (d) A bogus tag → 200 (revalidation miss, body served).
    let mut h_bogus = axum::http::HeaderMap::new();
    h_bogus.insert(
        axum::http::header::IF_NONE_MATCH,
        "\"0-0\"".parse().unwrap(),
    );
    let resp = raw_content(
        axum::extract::State(state.clone()),
        h_bogus,
        axum::extract::Path((ZIM.to_string(), PATH.to_string())),
    )
    .await
    .expect("raw_content 200 (bogus tag)");
    assert_eq!(
        resp.status(),
        axum::http::StatusCode::OK,
        "mismatching If-None-Match → 200"
    );

    // Cleanup: drop the fixture's zims row so the harness stays idempotent.
    zimservice::db::raw::execute(&pool, "DELETE FROM zims WHERE name = $1", |q| q.bind(ZIM))
        .await
        .unwrap();
}
