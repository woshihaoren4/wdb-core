use crate::core::NodeCache;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use wd_tools::PFSome;

#[derive(Debug, Default)]
pub struct RWLockCache {
    inner: RwLock<HashMap<u64, Arc<Vec<u8>>>>,
}

#[async_trait::async_trait]
impl NodeCache for RWLockCache {
    async fn get(&self, offset: u64) -> Option<Arc<Vec<u8>>> {
        let map = self.inner.read().await;
        let value = map.get(&offset)?;
        value.clone().some()
    }

    async fn set(&self, offset: u64, value: Arc<Vec<u8>>) {
        if value.is_empty() {
            return;
        }
        let mut map = self.inner.write().await;
        map.insert(offset, value);
    }

    async fn reset(&self) {
        let mut map = self.inner.write().await;
        map.clear();
    }
}
