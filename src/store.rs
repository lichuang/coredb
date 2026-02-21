use std::collections::HashMap;
use std::sync::RwLock;

/// In-memory key-value store
pub struct Store {
    data: RwLock<HashMap<String, Vec<u8>>>,
}

impl Store {
    /// Create a new empty store
    pub fn new() -> Self {
        Self {
            data: RwLock::new(HashMap::new()),
        }
    }

    /// Set a key to the given value
    pub fn set(&self, key: String, value: Vec<u8>) -> Result<(), String> {
        let mut data = self.data.write().map_err(|_| "Lock poisoned")?;
        data.insert(key, value);
        Ok(())
    }

    /// Get the value for a key (not used yet, but will be needed for GET)
    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>, String> {
        let data = self.data.read().map_err(|_| "Lock poisoned")?;
        Ok(data.get(key).cloned())
    }
}

impl Default for Store {
    fn default() -> Self {
        Self::new()
    }
}
