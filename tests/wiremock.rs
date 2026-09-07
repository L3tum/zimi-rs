#![allow(clippy::unwrap_used)]

//! Step 5.2 (T2): qBittorrent client tests backed by wiremock.
//!
//! Covers:
//! - `TorrentInfo` serde round-trip for realistic 4.x / 5.x payload shapes
//!   (field mapping + unknown-field tolerance).
//! - `QbitClient::auth` success/failure.
//! - `QbitClient::add_torrent` any-200 success rule (B5) and error responses.
//! - `QbitClient::delete` / `set_ratio_limit` request contracts (TEST#4).
//! - `get_torrents("all")` filter contract (pins P1-B2: the poller must fetch
//!   *all* torrents, not just `active`, so paused ones aren't mass-errored).
//!
//! NOTE: test code may use `.unwrap()` (crate-level `#![allow(clippy::unwrap_used)]`,
//! matching `tests/integration/main.rs`) — but `expect` with a message or match
//! patterns stay preferred where intent is worth stating, consistent with the
//! rest of the file. `clippy --all-targets -- -D warnings` is a CI gate.

//!
//! Wiremock serves plain HTTP on 127.0.0.1; `QbitClient::new` permits
//! loopback hosts via its SSRF guard, so no real qBittorrent instance is
//! needed.

use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

use zimservice::error::{Error, TorrentKind};
use zimservice::torrent::{connect_qbit, QbitClient, QbitClientCache, TorrentInfo};

/// A realistic qBittorrent 4.x-style payload: includes legacy fields and
/// extras that the struct does not model.
const QB_4X_PAYLOAD: &str = r#"[
    {
        "hash": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
        "name": "wikipedia_en_all_maxi_2024-01.zim",
        "state": "downloading",
        "progress": 0.42,
        "dlspeed": 123456,
        "upspeed": 1024,
        "ratio": 0.0,
        "category": "/wikipedia",
        "save_path": "/downloads",
        "content_path": "/downloads/wikipedia_en_all_maxi_2024-01.zim",
        "size": 10737418240,
        "downloaded": 4509765658,
        "num_seeds": 3,
        "num_leechs": 7,
        "ratio_limit": 2.0,
        "seeding_time": 60,
        "time_since_activity": 5
    }
]"#;

/// A qBittorrent 5.x-style payload: new fields appear, some fields absent.
const QB_5X_PAYLOAD: &str = r#"[
    {
        "hash": "b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3",
        "name": "stackexchange_cs_all_2024-06.zim",
        "state": "uploading",
        "progress": 1.0,
        "dlspeed": 0,
        "upspeed": 512,
        "ratio": 1.5,
        "size": 2147483648,
        "downloaded": 2147483648,
        "new_fields_5x": {"something": true},
        "last_activity": 1700000000
    }
]"#;

#[test]
fn torrent_info_deserializes_qb4x_payload() {
    let v: Vec<TorrentInfo> = serde_json::from_str(QB_4X_PAYLOAD).expect("parse 4.x");
    assert_eq!(v.len(), 1);
    let t = &v[0];
    assert_eq!(t.hash, "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2");
    assert_eq!(t.name, "wikipedia_en_all_maxi_2024-01.zim");
    assert_eq!(t.state, "downloading");
    assert!((t.progress - 0.42).abs() < f64::EPSILON);
    assert_eq!(t.dlspeed, 123456);
    assert_eq!(t.upspeed, 1024);
    assert!((t.ratio - 0.0).abs() < f64::EPSILON);
    assert_eq!(t.category.as_deref(), Some("/wikipedia"));
    assert_eq!(t.save_path.as_deref(), Some("/downloads"));
    assert_eq!(
        t.content_path.as_deref(),
        Some("/downloads/wikipedia_en_all_maxi_2024-01.zim")
    );
    assert_eq!(t.size, 10737418240);
    assert_eq!(t.downloaded, 4509765658);
    assert_eq!(t.num_seeds, 3);
    assert!(t.is_active_download());
    assert!(!t.is_complete());
}

#[test]
fn torrent_info_deserializes_qb5x_payload_with_unknown_fields() {
    // Unknown fields (num_leechs, ratio_limit, new_fields_5x, ...) must be
    // tolerated, and absent Option fields must default to None.
    let v: Vec<TorrentInfo> = serde_json::from_str(QB_5X_PAYLOAD).expect("parse 5.x");
    assert_eq!(v.len(), 1);
    let t = &v[0];
    assert_eq!(t.hash, "b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3");
    assert_eq!(t.name, "stackexchange_cs_all_2024-06.zim");
    assert_eq!(t.state, "uploading");
    assert_eq!(t.progress, 1.0);
    assert_eq!(t.dlspeed, 0);
    assert_eq!(t.upspeed, 512);
    assert!((t.ratio - 1.5).abs() < f64::EPSILON);
    assert!(t.category.is_none());
    assert!(t.save_path.is_none());
    assert!(t.content_path.is_none());
    assert!(t.err_str.is_none());
    assert_eq!(t.num_seeds, 0); // default when absent
    assert!(t.is_complete());
    assert!(!t.is_active_download());
}

#[test]
fn torrent_info_defaults_when_fields_absent() {
    // Omitting every optional-stat field must parse with zero defaults, and
    // the three state predicates must all be false (state "" matches none).
    let v: Vec<TorrentInfo> =
        serde_json::from_str(r#"[{"hash":"h1","name":"n1"}]"#).expect("parse minimal");
    assert_eq!(v.len(), 1);
    let t = &v[0];
    assert_eq!(t.hash, "h1");
    assert_eq!(t.name, "n1");
    assert_eq!(t.progress, 0.0);
    assert_eq!(t.state, "");
    assert_eq!(t.dlspeed, 0);
    assert_eq!(t.upspeed, 0);
    assert_eq!(t.ratio, 0.0);
    assert_eq!(t.size, 0);
    assert_eq!(t.downloaded, 0);
    assert!(!t.is_complete());
    assert!(!t.is_active_download());
    assert!(!t.is_fatal());
}

#[test]
fn torrent_info_requires_hash_and_name() {
    // Omitting `hash` (structural key for by_hash) must fail.
    assert!(
        serde_json::from_str::<Vec<TorrentInfo>>(r#"[{"name":"n1"}]"#).is_err(),
        "missing hash must not parse"
    );
    // Omitting `name` (structural key for by_name) must also fail.
    assert!(
        serde_json::from_str::<Vec<TorrentInfo>>(r#"[{"hash":"h1"}]"#).is_err(),
        "missing name must not parse"
    );
}

#[tokio::test]
async fn qbit_auth_success() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v2/login"))
        .respond_with(ResponseTemplate::new(200).set_body_string("OK"))
        .expect(1)
        .mount(&server)
        .await;

    let mut client =
        QbitClient::new(&server.uri(), "user", "pass", false, None).expect("build client");
    client.auth().await.expect("auth should succeed");
}

#[tokio::test]
async fn qbit_auth_failure() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v2/login"))
        .respond_with(ResponseTemplate::new(403).set_body_string("Fails"))
        .expect(1)
        .mount(&server)
        .await;

    let mut client =
        QbitClient::new(&server.uri(), "user", "wrong", false, None).expect("build client");
    let err = client.auth().await.expect_err("auth should fail on 403");
    let msg = format!("{err}");
    assert!(msg.contains("403"), "error should mention status: {msg}");
}

#[tokio::test]
async fn qbit_add_torrent_success_any_200_rule() {
    // B5: any successful (2xx) response counts as success; the response body
    // (the torrent name qB returns) is the value, NOT required to start with
    // "ok".
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v2/torrents/add"))
        .respond_with(ResponseTemplate::new(200).set_body_string("My Torrent Name"))
        .expect(1)
        .mount(&server)
        .await;

    let client = QbitClient::new(&server.uri(), "user", "pass", false, None).expect("build client");
    let result = client
        .add_torrent("https://example.com/file.zim", "cat", "/save")
        .await
        .expect("add_torrent should succeed on 200");
    assert_eq!(result, "My Torrent Name");
}

#[tokio::test]
async fn qbit_add_torrent_error_response() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v2/torrents/add"))
        .respond_with(ResponseTemplate::new(500).set_body_string("boom"))
        .expect(1)
        .mount(&server)
        .await;

    let client = QbitClient::new(&server.uri(), "user", "pass", false, None).expect("build client");
    let err = client
        .add_torrent("https://example.com/file.zim", "cat", "/save")
        .await
        .expect_err("add_torrent should fail on 500");
    let msg = format!("{err}");
    assert!(msg.contains("500"), "error should mention status: {msg}");
}

#[tokio::test]
async fn qbit_get_torrents_roundtrip() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v2/torrents/info"))
        // Pins the contract: `filter` is the raw qBittorrent filter value
        // ("active"), not a "filter=active" query fragment.
        .and(query_param("filter", "active"))
        .respond_with(ResponseTemplate::new(200).set_body_string(QB_5X_PAYLOAD))
        .expect(1)
        .mount(&server)
        .await;

    let client = QbitClient::new(&server.uri(), "user", "pass", false, None).expect("build client");
    let torrents = client.get_torrents("active").await.expect("get_torrents");
    assert_eq!(torrents.len(), 1);
    assert_eq!(torrents[0].hash, "b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3");
    assert!(torrents[0].is_complete());
}

#[tokio::test]
async fn qbit_get_torrents_all_filter() {
    // P1-B2 contract: the poller's tick/reconcile fetch with `filter=all`
    // (superset that includes paused torrents). If the code regresses to
    // `active`, this matcher fails.
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v2/torrents/info"))
        .and(query_param("filter", "all"))
        .respond_with(ResponseTemplate::new(200).set_body_string(QB_5X_PAYLOAD))
        .expect(1)
        .mount(&server)
        .await;

    let client = QbitClient::new(&server.uri(), "user", "pass", false, None).expect("build client");
    let torrents = client.get_torrents("all").await.expect("get_torrents");
    assert_eq!(torrents.len(), 1);
    assert_eq!(torrents[0].hash, "b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3");
}

#[tokio::test]
async fn qbit_get_torrents_session_expired_403() {
    let server = MockServer::start().await;
    // Login succeeds (valid session), then the info fetch returns 403 — the
    // session expired between login and the fetch.
    Mock::given(method("POST"))
        .and(path("/api/v2/login"))
        .respond_with(ResponseTemplate::new(200).set_body_string("Ok."))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v2/torrents/info"))
        .respond_with(ResponseTemplate::new(403).set_body_string("forbidden"))
        .expect(1)
        .mount(&server)
        .await;

    let mut client =
        QbitClient::new(&server.uri(), "user", "pass", false, None).expect("build client");
    client.auth().await.expect("login ok");
    let err = client
        .get_torrents("all")
        .await
        .expect_err("403 must surface as an error, not a JSON parse");
    assert!(
        matches!(
            &err,
            Error::Torrent {
                kind: TorrentKind::SessionExpired,
                ..
            }
        ),
        "403 should be classified as a session expiry, got: {err:?}"
    );
    assert!(
        err.to_string().contains("403"),
        "message should name the status, got: {err}"
    );
}

#[tokio::test]
async fn qbit_get_torrents_500_is_other() {
    // A non-auth upstream failure is `Other`, never `SessionExpired` (it
    // must not trigger a pointless re-login).
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v2/login"))
        .respond_with(ResponseTemplate::new(200).set_body_string("Ok."))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v2/torrents/info"))
        .respond_with(ResponseTemplate::new(500).set_body_string("boom"))
        .expect(1)
        .mount(&server)
        .await;

    let mut client =
        QbitClient::new(&server.uri(), "user", "pass", false, None).expect("build client");
    client.auth().await.expect("login ok");
    let err = client.get_torrents("all").await.expect_err("500 must fail");
    assert!(
        matches!(
            &err,
            Error::Torrent {
                kind: TorrentKind::Other,
                ..
            }
        ),
        "500 should be classified as Other, got: {err:?}"
    );
    assert!(
        err.to_string().contains("500"),
        "message should name the status, got: {err}"
    );
}

#[tokio::test]
async fn qbit_version_ok() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v2/login"))
        .respond_with(ResponseTemplate::new(200).set_body_string("Ok."))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v2/app/version"))
        .respond_with(ResponseTemplate::new(200).set_body_string("R5.0.0\n"))
        .expect(1)
        .mount(&server)
        .await;

    let mut client =
        QbitClient::new(&server.uri(), "user", "pass", false, None).expect("build client");
    client.auth().await.expect("login ok");
    let v = client.version().await.expect("version ok");
    assert_eq!(v, "R5.0.0", "version body is trimmed");
}

#[tokio::test]
async fn qbit_version_auth_failure() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v2/login"))
        .respond_with(ResponseTemplate::new(200).set_body_string("Ok."))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v2/app/version"))
        .respond_with(ResponseTemplate::new(401).set_body_string("unauthorized"))
        .expect(1)
        .mount(&server)
        .await;

    let mut client =
        QbitClient::new(&server.uri(), "user", "pass", false, None).expect("build client");
    client.auth().await.expect("login ok");
    let err = client
        .version()
        .await
        .expect_err("401 must surface as an error");
    assert!(
        matches!(
            &err,
            Error::Torrent {
                kind: TorrentKind::SessionExpired,
                ..
            }
        ),
        "401 should be classified as a session expiry, got: {err:?}"
    );
    assert!(
        err.to_string().contains("401"),
        "message should name the status, got: {err}"
    );
}

#[tokio::test]
async fn qbit_cache_ensure_reauths_after_invalidate() {
    let server = MockServer::start().await;
    // Exactly two logins: the initial ensure and the post-invalidate ensure.
    // The middle (cached) ensure must not re-login.
    Mock::given(method("POST"))
        .and(path("/api/v2/login"))
        .respond_with(ResponseTemplate::new(200).set_body_string("Ok."))
        .expect(2)
        .mount(&server)
        .await;

    let base = server.uri();
    let base_ref: &str = &base; // Copyable &str; `base` outlives every closure.
    let cache = QbitClientCache::new();

    // Initial ensure → build → login #1.
    cache
        .ensure("fp", move || connect_qbit(base_ref, "user", "pass", false))
        .await
        .expect("first ensure should connect");

    // Same fingerprint → cache hit, no new login.
    let _ = cache
        .ensure("fp", move || connect_qbit(base_ref, "user", "pass", false))
        .await;

    // Invalidate → next ensure must re-login (#2).
    cache.invalidate();
    cache
        .ensure("fp", move || connect_qbit(base_ref, "user", "pass", false))
        .await
        .expect("re-auth ensure should connect");

    // wiremock's expect(2) asserts exactly two logins fired on drop.
}

#[tokio::test]
async fn qbit_client_pins_named_host() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v2/torrents/info"))
        .and(query_param("filter", "all"))
        .respond_with(ResponseTemplate::new(200).set_body_string(QB_5X_PAYLOAD))
        .expect(1)
        .mount(&server)
        .await;
    // Pin the nonexistent `testhost` at the MockServer socket: the fetch of
    // http://testhost:{port} succeeds only if the client carries the pin.
    let client = QbitClient::new(
        &format!("http://testhost:{}", server.address().port()),
        "user",
        "pass",
        false,
        Some(("testhost".into(), *server.address())),
    )
    .expect("build client");
    let torrents = client
        .get_torrents("all")
        .await
        .expect("get_torrents through pin");
    assert_eq!(torrents.len(), 1);
}

#[tokio::test]
async fn qbit_client_redirect_to_metadata_blocked() {
    let server = MockServer::start().await;
    // Initial info hop is pinned local; the 302 redirects to the link-local
    // metadata IP, which the guarded redirect policy must refuse.
    Mock::given(method("POST"))
        .and(path("/api/v2/login"))
        .respond_with(ResponseTemplate::new(200).set_body_string("Ok."))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v2/torrents/info"))
        .respond_with(
            ResponseTemplate::new(302).append_header("Location", "http://169.254.169.254/"),
        )
        .expect(1)
        .mount(&server)
        .await;

    let base = format!("http://testhost:{}", server.address().port());
    let mut client = QbitClient::new(
        &base,
        "user",
        "pass",
        false,
        Some(("testhost".into(), *server.address())),
    )
    .expect("build client");
    client.auth().await.expect("login ok");
    let err = client
        .get_torrents("all")
        .await
        .expect_err("redirect to metadata must be refused");
    // The policy's message lives on the error's source chain (reqwest wraps
    // it as a generic "error following redirect" kind).
    let mut chain = Vec::new();
    let mut cur: Option<&dyn std::error::Error> = Some(&err);
    while let Some(e) = cur {
        chain.push(e.to_string());
        cur = e.source();
    }
    let chain_text = chain.join(" | ");
    assert!(
        chain_text.contains("redirect target rejected"),
        "expected redirect refusal, got: {chain_text}"
    );
}

#[tokio::test]
async fn qbit_delete_sends_hash_and_delete_files() {
    let server = MockServer::start().await;
    // delete_files = true
    Mock::given(method("POST"))
        .and(path("/api/v2/torrents/delete"))
        .and(query_param("hashes", "hash-true"))
        .and(query_param("deleteFiles", "true"))
        .respond_with(ResponseTemplate::new(200).set_body_string(""))
        .expect(1)
        .mount(&server)
        .await;
    // delete_files = false
    Mock::given(method("POST"))
        .and(path("/api/v2/torrents/delete"))
        .and(query_param("hashes", "hash-false"))
        .and(query_param("deleteFiles", "false"))
        .respond_with(ResponseTemplate::new(200).set_body_string(""))
        .expect(1)
        .mount(&server)
        .await;

    let client = QbitClient::new(&server.uri(), "user", "pass", false, None).expect("build client");
    client
        .delete("hash-true", true)
        .await
        .expect("delete with files should succeed");
    client
        .delete("hash-false", false)
        .await
        .expect("delete without files should succeed");
}

#[tokio::test]
async fn qbit_set_ratio_limit_posts_form() {
    // Pins the form body: `hashes=<h>` and `limit` formatted as `{:.2}`
    // (two decimal places) — the qBittorrent Web API expects a form POST.
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v2/torrents/setRatioLimit"))
        .and(wiremock::matchers::body_string("hashes=abc123&limit=2.00"))
        .respond_with(ResponseTemplate::new(200).set_body_string(""))
        .expect(1)
        .mount(&server)
        .await;

    let client = QbitClient::new(&server.uri(), "user", "pass", false, None).expect("build client");
    client
        .set_ratio_limit("abc123", 2.0)
        .await
        .expect("set_ratio_limit should succeed");
}

// ── B5.5: Embed client tests ────────────────────────────────────────────
// `EmbedConfig` is constructed directly (all fields pub) with the endpoint at
// the MockServer; `EmbedClient::new(config, None)` needs no settings cache
// (loopback is admitted for the embed client).

fn embed_config(server: &MockServer, api_key: &str) -> zimservice::embed::EmbedConfig {
    zimservice::embed::EmbedConfig {
        endpoint: server.uri(),
        api_key: api_key.to_string(),
        model: "test-model".into(),
        dimension: 4,
        batch_size: 10,
        max_concurrency: 1,
        timeout_secs: 5,
    }
}

const EMBED_OK_BODY: &str = r#"{"data":
    [{"index":1,"embedding":[0.1,0.1,0.1,0.1]},
     {"index":0,"embedding":[0.2,0.2,0.2,0.2]},
     {"index":2,"embedding":[0.3,0.3,0.3,0.3]}]}"#;

#[tokio::test]
async fn embed_returns_vectors_in_index_order() {
    use wiremock::matchers::body_json;
    let server = MockServer::start().await;
    // Provider replies out of order; the client must restore index order.
    Mock::given(method("POST"))
        .and(path("/embeddings"))
        .and(body_json(
            serde_json::json!({"model":"test-model","input":["a","b","c"]}),
        ))
        .respond_with(ResponseTemplate::new(200).set_body_string(EMBED_OK_BODY))
        .expect(1)
        .mount(&server)
        .await;

    let client = zimservice::embed::EmbedClient::new(embed_config(&server, ""), None)
        .expect("build embed client");
    let v = client
        .embed(&["a".into(), "b".into(), "c".into()])
        .await
        .expect("embed");
    assert_eq!(v.len(), 3);
    assert_eq!(v[0], vec![0.2f32; 4], "index 0 comes first");
    assert_eq!(v[1], vec![0.1f32; 4], "index 1 second");
    assert_eq!(v[2], vec![0.3f32; 4], "index 2 third");
}

#[tokio::test]
async fn embed_wrong_count_returned_as_is() {
    // Documents the guard's location: `embed()` performs **no** count check —
    // a short response is returned as-is. The count guard is downstream and
    // DB-backed (`embeddings.len() != ids.len()` in `run_pipeline`). A wrong
    // per-vector *length* likewise isn't caught here; it fails later at the
    // Postgres `::vector` cast.
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/embeddings"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(r#"{"data":[{"index":0,"embedding":[0.1,0.1,0.1,0.1]}]}"#),
        )
        .expect(1)
        .mount(&server)
        .await;

    let client = zimservice::embed::EmbedClient::new(embed_config(&server, ""), None)
        .expect("build embed client");
    let v = client
        .embed(&["a".into(), "b".into()])
        .await
        .expect("client returns whatever the provider sent");
    assert_eq!(v.len(), 1, "count mismatch is not caught by the client");
}

#[tokio::test]
async fn embed_client_sends_bearer_when_api_key_set() {
    use wiremock::matchers::header;
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/embeddings"))
        .and(header("Authorization", "Bearer secret-key"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(r#"{"data":[{"index":0,"embedding":[0.1,0.1,0.1,0.1]}]}"#),
        )
        .expect(1) // 404 if the header is missing/wrong → request fails
        .mount(&server)
        .await;

    let client = zimservice::embed::EmbedClient::new(embed_config(&server, "secret-key"), None)
        .expect("build embed client");
    let v = client
        .embed(&["a".into()])
        .await
        .expect("bearer auth must match the mock");
    assert_eq!(v.len(), 1);
}

#[tokio::test]
async fn embed_client_omits_bearer_when_api_key_empty() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/embeddings"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(r#"{"data":[{"index":0,"embedding":[0.1,0.1,0.1,0.1]}]}"#),
        )
        .expect(1)
        .mount(&server)
        .await;

    let client = zimservice::embed::EmbedClient::new(embed_config(&server, ""), None)
        .expect("build embed client");
    let _ = client.embed(&["a".into()]).await.expect("embed");

    let reqs = server.received_requests().await.expect("requests recorded");
    assert_eq!(reqs.len(), 1);
    let has_auth = reqs[0]
        .headers
        .iter()
        .any(|(n, _)| n.as_str() == "authorization");
    assert!(!has_auth, "no Authorization header when api_key is empty");
}

#[tokio::test]
async fn embed_client_maps_http_errors() {
    // 401 and 500 with non-JSON bodies: reqwest's `.json()` maps both to
    // `Error::Http` (status or parse failure) — the client must surface Err,
    // never a bogus success.
    for status in [401, 500] {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/embeddings"))
            .respond_with(ResponseTemplate::new(status).set_body_string("not json"))
            .expect(1)
            .mount(&server)
            .await;

        let client = zimservice::embed::EmbedClient::new(embed_config(&server, ""), None)
            .expect("build embed client");
        let err = client.embed(&["a".into()]).await.expect_err("must be Err");
        let _ = format!("{err}");
    }
}
