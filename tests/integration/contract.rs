//! JS↔Rust response-contract tests (Tests Major #5): the jsdom UI tests
//! (tests/web/*.test.mjs) run the page scripts against canned JSON payloads;
//! those payloads are loaded from `tests/web/fixtures/<endpoint>.json` — the
//! SAME files this module asserts the LIVE handler responses against. A
//! handler field rename now breaks both halves in one change: the
//! live-response check below fails, and the UI half renders the renamed
//! field as garbage (its existing assertions catch that).
//!
//! DB-gated like the rest of the suite ([`common::pool_or_skip`]): both
//! handler paths under contract read Postgres state — `/list` after a
//! `resync` of the fixture dir, `/search` over the seeded articles.

use super::common::*;
use serde_json::Value;

/// Shared contract fixtures — the SAME files the jsdom tests load
/// (tests/web/index.test.mjs, tests/web/search.test.mjs). Resolved against
/// the crate root, the cwd under `cargo test`.
const LIST_FIXTURE: &str = "tests/web/fixtures/list.json";
const SEARCH_FIXTURE: &str = "tests/web/fixtures/search.json";

/// Parse a contract fixture (panics with a readable message on I/O or JSON
/// errors — a missing fixture is a build-tree breakage, not a test failure).
fn load_fixture(path: &str) -> Value {
    let raw = std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("read contract fixture {path}: {e}"));
    serde_json::from_str(&raw)
        .unwrap_or_else(|e| panic!("contract fixture {path} is not valid JSON: {e}"))
}

/// Assert that the live response satisfies the contract. The fixture is a
/// contract SUBSET — extra live fields are allowed, but every field the UI
/// reads (every fixture key) must exist with the fixture's JSON type; a
/// renamed field shows up as a missing key and fails. A `null` live value
/// is accepted where the fixture carries a sample: the handler may
/// legitimately omit an optional value (e.g. a ZIM without `category`),
/// and the UI code handles null/absent via `||` / ternaries.
fn assert_conforms(live: &Value, contract: &Value, path: &str) {
    match (contract, live) {
        (Value::Object(c), Value::Object(l)) => {
            for (key, cv) in c {
                let field_path = format!("{path}.{key}");
                assert!(
                    l.contains_key(key),
                    "contract field {field_path} is missing from the live \
                     response — renamed? the UI reads it (tests/web/*.test.mjs)"
                );
                assert_conforms(&l[key], cv, &field_path);
            }
        }
        (Value::Array(c), Value::Array(l)) => {
            assert!(
                !c.is_empty(),
                "contract array {path} is empty in the fixture — it must carry \
                 one sample element"
            );
            assert!(
                !l.is_empty(),
                "contract array {path} is empty in the live response — the \
                 seeded fixture must produce at least one element"
            );
            let elem = &c[0];
            for (i, lv) in l.iter().enumerate() {
                assert_conforms(lv, elem, &format!("{path}[{i}]"));
            }
        }
        (_, lv) => {
            assert!(
                lv.is_null() || json_type(lv) == json_type(contract),
                "contract field {path} is a {} in the fixture but the live \
                 response carries {}",
                type_name(contract),
                type_name(lv)
            );
        }
    }
}

fn json_type(v: &Value) -> u8 {
    match v {
        Value::Null => 0,
        Value::Bool(_) => 1,
        Value::Number(_) => 2,
        Value::String(_) => 3,
        Value::Array(_) => 4,
        Value::Object(_) => 5,
    }
}

fn type_name(v: &Value) -> &'static str {
    match json_type(v) {
        0 => "null",
        1 => "bool",
        2 => "number",
        3 => "string",
        4 => "array",
        _ => "object",
    }
}

/// DB-gated: the live `GET /list` response satisfies the shared contract
/// (tests/web/fixtures/list.json — the same file the jsdom tests load).
#[tokio::test]
async fn list_response_conforms_to_the_js_contract() {
    let Some((pool, _gate)) = pool_or_skip().await else {
        return;
    };
    // Open mode, in-memory: the contract is about the response shape, so the
    // shared suite DB's settings rows (other tests may flip `access.mode`)
    // must not matter.
    let settings = SettingsCache::new_with_map(
        pool.clone(),
        zimservice::settings::default_settings(),
        HashMap::new(),
    );
    let zims = ZimManager::new(std::path::PathBuf::from(FIXTURES_DIR), pool.clone());
    zims.resync().await.expect("resync the fixture dir");
    assert!(
        zims.get("tiny").is_some(),
        "resync must cache the committed fixture"
    );
    let search = SearchEngine::new(
        pool.clone(),
        settings.clone(),
        zimservice::health::DegradationTracker::default(),
    );
    let state = assemble_state(pool, settings, zims, search);
    let (base, _trigger) = boot_server(zimservice::serve::build_router(state)).await;

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{base}/list"))
        .send()
        .await
        .expect("GET /list must complete");
    assert_eq!(resp.status(), 200, "GET /list must be 200");
    let live: Value = resp.json().await.expect("/list body is JSON");
    assert_conforms(&live, &load_fixture(LIST_FIXTURE), "list");
}

/// DB-gated: the live `GET /search` response (over the seeded articles)
/// satisfies the shared contract (tests/web/fixtures/search.json — the same
/// file the jsdom tests load).
#[tokio::test]
async fn search_response_conforms_to_the_js_contract() {
    let Some((pool, _gate)) = pool_or_skip().await else {
        return;
    };
    let engine = seed_search_fixture(&pool).await;
    let settings = SettingsCache::new_with_map(
        pool.clone(),
        zimservice::settings::default_settings(),
        HashMap::new(),
    );
    let zims = ZimManager::new(
        std::path::PathBuf::from("/nonexistent-contract"),
        pool.clone(),
    );
    let state = assemble_state(pool.clone(), settings, zims, engine);
    let (base, _trigger) = boot_server(zimservice::serve::build_router(state)).await;

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{base}/search?q=alpine&highlight=true"))
        .send()
        .await
        .expect("GET /search must complete");
    assert_eq!(resp.status(), 200, "GET /search must be 200");
    let live: Value = resp.json().await.expect("/search body is JSON");
    assert_conforms(&live, &load_fixture(SEARCH_FIXTURE), "search");
}
