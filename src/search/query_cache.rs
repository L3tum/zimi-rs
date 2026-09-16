//! Bounded FIFO cache of normalised search query → embedding vector.
//!
//! Used **only** on the interactive search path (not the batch embedding
//! pipeline, which embeds many distinct article texts per cycle).
//!
//! FIFO, not LRU (review: Ponytail SIMPLIFY): single consumer, 256 entries —
//! the eviction policy is unmeasurable at this size, and FIFO removes the
//! O(n) reorder work (`VecDeque::position` + `remove` on every get/insert)
//! the LRU paid on the hot search path.
//!
//! Key format: `{model}|{dimension}|{trimmed_query}` — including the model
//! and dimension means a settings change automatically produces cache
//! misses (different key) without any explicit invalidation. The trade-off:
//! stale entries for the old model/dimension remain until FIFO-evicted,
//! but they are never returned for the new key. Worst-case extra memory is
//! bounded by the cache capacity (256 entries × ~3 KB = ~768 KB).
//!
//! Key normalisation: the query is trimmed (leading/trailing whitespace);
//! case is preserved (embedding models are typically case-sensitive).
//!
//! In-process, protected by a `std::sync::Mutex` (short critical section,
//! no `await` while held — tokio-friendly).

use std::collections::{HashMap, VecDeque};

/// Maximum entries in the search-path query embedding cache.
///
/// At the default 768-dimension each entry is ~3 KB, so 256 entries
/// ≈ 768 KB worst-case — negligible for a server process.
pub const QUERY_EMBED_CACHE_CAP: usize = 256;

/// Bounded FIFO cache: normalised query key → embedding vector.
///
/// `get` is a plain lookup (NO promotion — a re-read does not extend an
/// entry's life); `insert` of a NEW key evicts the oldest-INSERTED entry
/// at capacity, and re-`insert` of an EXISTING key updates the value in
/// place without moving it in FIFO order.
///
/// Thread-safe via `std::sync::Mutex` (callers lock externally; the
/// methods here take `&mut self`).
pub struct QueryEmbedCache {
    cap: usize,
    map: HashMap<String, Vec<f32>>,
    /// FIFO insertion order: front = oldest (eviction candidate), back =
    /// most recently inserted.
    order: VecDeque<String>,
}

impl QueryEmbedCache {
    /// Create a cache with the given capacity (at least 1).
    #[must_use]
    pub fn new(cap: usize) -> Self {
        Self {
            cap: cap.max(1),
            map: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    /// Look up a cached vector. Plain lookup — no FIFO reordering (a
    /// re-read does NOT protect the entry from eviction). Returns `None`
    /// on miss.
    pub fn get(&mut self, key: &str) -> Option<&Vec<f32>> {
        self.map.get(key)
    }

    /// Insert (or update) a vector under `key`. A NEW key at capacity
    /// evicts the oldest-INSERTED entry; re-inserting an EXISTING key
    /// updates the value in place without changing its FIFO position.
    pub fn insert(&mut self, key: String, vec: Vec<f32>) {
        if self.map.insert(key.clone(), vec).is_some() {
            return; // existing key: value updated in place, order untouched
        }
        // New key: evict the oldest-INSERTED entry if the cache is now
        // over capacity (the insert above already landed, so over-capacity
        // is `> cap`, not `>= cap`).
        if self.map.len() > self.cap {
            if let Some(evicted) = self.order.pop_front() {
                self.map.remove(&evicted);
            }
        }
        self.order.push_back(key);
    }

    /// Number of entries currently cached. Reported by `/diagnostic`.
    #[must_use]
    pub fn len(&self) -> usize {
        self.map.len()
    }
}

impl Default for QueryEmbedCache {
    fn default() -> Self {
        Self::new(QUERY_EMBED_CACHE_CAP)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn vec(n: usize) -> Vec<f32> {
        vec![1.0; n]
    }

    #[test]
    fn miss_on_empty() {
        let mut c = QueryEmbedCache::new(10);
        assert!(c.get("nope").is_none());
    }

    #[test]
    fn hit_after_insert() {
        let mut c = QueryEmbedCache::new(10);
        c.insert("q1".into(), vec(768));
        assert_eq!(c.get("q1").unwrap().len(), 768);
    }

    #[test]
    fn eviction_at_capacity() {
        let mut c = QueryEmbedCache::new(3);
        c.insert("a".into(), vec(1));
        c.insert("b".into(), vec(2));
        c.insert("c".into(), vec(3));
        // Full: a,b,c in insertion order (a is the eviction candidate).
        c.insert("d".into(), vec(4));
        assert!(c.get("a").is_none(), "oldest-inserted 'a' evicted");
        assert!(c.get("b").is_some());
        assert!(c.get("c").is_some());
        assert!(c.get("d").is_some());
        assert_eq!(c.len(), 3);
    }

    #[test]
    fn fifo_no_promotion_on_get() {
        let mut c = QueryEmbedCache::new(2);
        c.insert("x".into(), vec(1));
        c.insert("y".into(), vec(2));
        // A re-read does NOT protect an entry (no LRU promotion): 'x'
        // remains the oldest-INSERTED.
        assert!(c.get("x").is_some());
        // Insert 'z' → evicts 'x' (the oldest-INSERTED), not 'y'.
        c.insert("z".into(), vec(3));
        assert!(
            c.get("x").is_none(),
            "'x' should be evicted even though it was re-read"
        );
        assert!(c.get("y").is_some(), "'y' (inserted second) survives");
        assert!(c.get("z").is_some());
    }

    #[test]
    fn update_existing_key_no_growth() {
        let mut c = QueryEmbedCache::new(2);
        c.insert("a".into(), vec(1));
        c.insert("a".into(), vec(2));
        assert_eq!(c.len(), 1, "re-insert should not grow");
        assert_eq!(c.get("a").unwrap().len(), 2, "value updated");
        // A re-insert must NOT refresh the FIFO position: fill the cache,
        // then the next NEW key still evicts 'a' (the oldest-INSERTED).
        c.insert("b".into(), vec(3));
        assert_eq!(c.len(), 2);
        c.insert("c".into(), vec(4));
        assert!(
            c.get("a").is_none(),
            "re-inserted 'a' stays at the FIFO back"
        );
        assert!(c.get("b").is_some());
        assert!(c.get("c").is_some());
    }

    #[test]
    fn key_includes_model_and_dimension() {
        let mut c = QueryEmbedCache::new(10);
        // Same query text but different model/dimension → distinct keys.
        c.insert("model-a|768|hello".into(), vec(768));
        c.insert("model-a|1024|hello".into(), vec(1024));
        c.insert("model-b|768|hello".into(), vec(768));
        assert_eq!(c.len(), 3);
        assert_eq!(c.get("model-a|768|hello").unwrap().len(), 768);
        assert_eq!(c.get("model-a|1024|hello").unwrap().len(), 1024);
        assert_eq!(c.get("model-b|768|hello").unwrap().len(), 768);
    }

    #[test]
    fn min_capacity_is_one() {
        let mut c = QueryEmbedCache::new(0);
        // A cache with cap 0 is clamped to 1: inserting 2 items leaves only 1.
        c.insert("a".into(), vec(1));
        c.insert("b".into(), vec(1));
        assert_eq!(c.len(), 1, "cap-0 cache should hold at most 1 entry");
    }

    #[test]
    fn default_capacity() {
        assert_eq!(QUERY_EMBED_CACHE_CAP, 256);
        let c = QueryEmbedCache::default();
        assert_eq!(c.len(), 0, "a fresh default cache is empty");
    }
}
