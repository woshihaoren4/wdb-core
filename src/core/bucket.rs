use crate::core::{BucketDataBase, BucketIndex};
use std::sync::Arc;
use wd_tools::PFOk;

pub struct Bucket {
    db: Arc<dyn BucketDataBase>,
    index: Arc<dyn BucketIndex>,
}

impl Bucket {
    pub fn new(db: Arc<dyn BucketDataBase>,index: Arc<dyn BucketIndex>)->Self{
        Self{db,index}
    }

    pub async fn set_raw(&self, key: u64, value: Vec<u8>) -> anyhow::Result<u64> {
        let offset = self.db.set(key, value.as_slice()).await?;
        self.index.push(key, offset);
        offset.ok()
    }
    pub async fn find_raw(&self, key: &u64) -> anyhow::Result<Vec<u8>> {
        let offset = match self.index.find(key).await {
            None => return Vec::new().ok(),
            Some(s) => s,
        };
        let data = self.db.get(offset).await?;
        return data.ok()
    }
}
