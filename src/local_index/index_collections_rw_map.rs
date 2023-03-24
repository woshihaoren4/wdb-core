use crate::core::IndexCollections;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use wd_tools::PFSome;

#[derive(Debug, Default)]
pub struct IndexCollRWMap {
    inner: Arc<RwLock<HashMap<u64, (u64, u64)>>>,
}

#[async_trait::async_trait]
impl IndexCollections for IndexCollRWMap {
    async fn push(&self, key: u64, value: u64, offset: u64) {
        let mut w = self.inner.write().await;
        w.insert(key, (value, offset));
    }

    async fn find(&self, key: &u64) -> Option<u64> {
        let w = self.inner.read().await;
        let (v, _) = w.get(key)?;
        v.clone().some()
    }

    async fn update_index(&self, key: u64, offset: u64) {
        let mut w = self.inner.write().await;
        let a = match w.get_mut(&key) {
            None => return,
            Some(s) => s,
        };
        *a = (a.0.clone(), offset)
    }

    async fn find_index(&self, key: &u64) -> Option<u64> {
        let w = self.inner.read().await;
        let a = w.get(&key)?;
        a.1.clone().some()
    }
}
