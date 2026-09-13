//! Bounded LRU cache of normalised search query → embedding vector.
//!
//! Used **only** on the interactive search path (not the batch embedding
//! pipeline, which embeds many distinct article texts per cycle).
//!
//! Key format: `{model}|{dimension}|{trimmed_query}` — including the model
//! and dimension means a settings change automatically produces cache
//! misses (different key) without any explicit invalidation. The trade-off:
//! stale entries for the old model/dimension remain until LRU-evicted,
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

/// Bounded LRU cache: normalised query key → embedding vector.
///
/// Thread-safe via `std::sync::Mutex` (callers lock externally; the
/// methods here take `&mut self`).
pub struct QueryEmbedCache {
    cap: usize,
    map: HashMap<String, Vec<f32>>,
    /// LRU order: front = most recently used, back = eviction candidate.
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

    /// Look up a cached vector. On hit, promotes the entry to MRU position.
    /// Returns `None` on miss.
    pub fn get(&mut self, key: &str) -> Option<&Vec<f32>> {
        if self.map.contains_key(key) {
            // Move to MRU (front).
            if let Some(pos) = self.order.iter().position(|k| k == key) {
                self.order.remove(pos);
            }
            self.order.push_front(key.to_string());
            self.map.get(key)
        } else {
            None
        }
    }

    /// Insert (or update) a vector under `key`. Evicts the LRU entry if at
    /// capacity.
    pub fn insert(&mut self, key: String, vec: Vec<f32>) {
        if self.map.contains_key(&key) {
            // Update in place: remove old LRU position, re-insert at MRU.
            if let Some(pos) = self.order.iter().position(|k| k == &key) {
                self.order.remove(pos);
            }
            self.map.insert(key.clone(), vec);
            self.order.push_front(key);
            return;
        }
        // Evict LRU if at capacity.
        if self.map.len() >= self.cap {
            if let Some(evicted) = self.order.pop_back() {
                self.map.remove(&evicted);
            }
        }
        self.map.insert(key.clone(), vec);
        self.order.push_front(key);
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
        // Full: a,b,c (a is LRU).
        c.insert("d".into(), vec(4));
        assert!(c.get("a").is_none(), "LRU entry 'a' evicted");
        assert!(c.get("b").is_some());
        assert!(c.get("c").is_some());
        assert!(c.get("d").is_some());
        assert_eq!(c.len(), 3);
    }

    #[test]
    fn lru_order_promoted_on_get() {
        let mut c = QueryEmbedCache::new(2);
        c.insert("x".into(), vec(1));
        c.insert("y".into(), vec(2));
        // Access 'x' → 'x' is now MRU, 'y' is LRU.
        assert!(c.get("x").is_some());
        // Insert 'z' → evicts 'y' (the true LRU).
        c.insert("z".into(), vec(3));
        assert!(
            c.get("y").is_none(),
            "'y' should be evicted (was LRU before access)"
        );
        assert!(c.get("x").is_some(), "'x' was promoted and survives");
        assert!(c.get("z").is_some());
    }

    #[test]
    fn update_existing_key_no_growth() {
        let mut c = QueryEmbedCache::new(2);
        c.insert("a".into(), vec(1));
        c.insert("a".into(), vec(2));
        assert_eq!(c.len(), 1, "re-insert should not grow");
        assert_eq!(c.get("a").unwrap().len(), 2, "value updated");
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
