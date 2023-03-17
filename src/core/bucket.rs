use std::sync::Arc;
use crate::core::{BucketDataBase, BucketIndex};
use wd_tools::{PFArc, PFOk};

pub struct Bucket{
    db: Arc<dyn BucketDataBase>,
    index: Arc<dyn BucketIndex>
}

impl Bucket {
    pub async fn set_raw(&self,key:u64,value:Vec<u8>)->anyhow::Result<u64>{
        let offset = self.db.set(key, value.as_slice()).await?;
        // self.wal.append(key,offset).await?;
        self.index.push(key,offset);offset.ok()
    }
    pub async fn find_raw(&self,key:u64)->anyhow::Result<Vec<(u64,Arc<Vec<u8>>)>>{
        let offset = match self.index.find(key).await {
            None => return Vec::new().ok(),
            Some(s) => {s}
        };
        let mut res = vec![];
        for i in offset.into_iter(){
            let data = self.db.get(i).await?;
            res.push((i,data))
        }
        return res.ok()
    }
}