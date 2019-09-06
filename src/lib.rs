#![deny(missing_docs)]
//! `KvStore` is a simple key/value store.

use std::collections::HashMap;

/// `KvStore` stores string key/value pairs.
///
/// Key/value string pairs are stored in a `HashMap` in memory but not persisted to disk.
///
/// Example:
///
/// ```rust
/// # use kvs::KvStore;
/// let mut store = KvStore::new();
/// store.set("key".to_owned(), "value".to_owned());
/// let val = store.get("key".to_owned());
/// assert_eq!(val, Some("value".to_owned()));
/// ```
#[derive(Default)]
pub struct KvStore {
    store: HashMap<String, String>,
}

impl KvStore {
    /// Create a `KvStore`.
    pub fn new() -> Self {
        KvStore {
            store: HashMap::new(),
        }
    }

    /// Sets the value of a string key to a string.
    ///
    /// If the key already exists, the existing value will be overwritten.
    pub fn set(&mut self, key: String, val: String) {
        self.store.insert(key, val);
    }

    // Gets the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    pub fn get(&self, key: String) -> Option<String> {
        self.store.get(&key).cloned()
    }

    /// Removes a given string key.
    pub fn remove(&mut self, key: String) {
        self.store.remove(&key);
    }
}
