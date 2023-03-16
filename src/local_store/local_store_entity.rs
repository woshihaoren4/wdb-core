use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::io::{Error, ErrorKind, Read, Write};
use std::path::Path;
use std::str::FromStr;
use std::sync::{Arc};
use std::sync::atomic::{AtomicU32, Ordering};
use tokio::sync::{Mutex, RwLock};
use wd_tools::{PFArc, PFErr, PFOk};
use wd_tools::sync::LessLock;
use crate::common::{WDBError, WDBResult};
use crate::core::{Block, BucketDataBase, DataBaseBlockManager, NodeCache};

pub struct LocalStoreEntity{
    block_manager : Arc<dyn DataBaseBlockManager>,
    cache : Arc<dyn NodeCache>,
    blocks: LessLock<HashMap<u32,Arc<dyn Block>>>,
    sn : AtomicU32,
    size: u32,
}

impl LocalStoreEntity {
    pub async fn new(block_manager : Arc<dyn DataBaseBlockManager>,cache : Arc<dyn NodeCache>)->anyhow::Result<LocalStoreEntity> {
        let size = block_manager.block_size();
        let block_list = block_manager.init_block().await?;
        let mut blocks = HashMap::new();
        let mut sn = 0;
        if block_list.is_empty() {
            let first_block = block_manager.create_block(0).await?;
            blocks.insert(0,first_block);
        }else {
            for (i,block) in block_list.into_iter(){
                let len = block.size().await?;
                if len != 0 {
                    sn = i;
                }
                blocks.insert(i,block);
            }
        }
        LocalStoreEntity::load_block_cache(blocks.get(&sn).unwrap().clone(),cache.clone()).await;
        LocalStoreEntity{block_manager,cache,
            blocks : LessLock::new(blocks),
            sn: AtomicU32::new(sn),
            size}.ok()
    }
    pub async fn load_block_cache(block:Arc<dyn Block>,cache : Arc<dyn NodeCache>){
        cache.reset().await;
        let recv = block.traversal().await;
        while !recv.is_closed() {
           let result = match recv.recv().await {
                Ok(o) => {
                    o
                }
                Err(_) => {
                    continue
                }
            };
            match result {
                Ok((offset,_key,value)) => {
                    cache.set(offset,value.arc()).await;
                },
                Err(e) => {
                    if let WDBError::BlockAbnormal(ref i) = e{
                        wd_log::log_error_ln!("db file({}) damage position[{}]，try skip the block other part",block.path(),i);
                        break
                    }else{
                        wd_log::log_panic!("load_block_cache unknown error:{}",e);
                    }
                }
            }
        }
    }
}


#[async_trait::async_trait]
impl BucketDataBase for LocalStoreEntity
{
    async fn set(&self, key: u64, mut value: Vec<u8>) -> anyhow::Result<u64> {
        let block = self.blocks.share().get(&self.sn.load(Ordering::Relaxed)).expect("LocalStoreEntity.set").clone();
        let value = Arc::new(value);
        //todo 处理超过块最大值的错误
        let offset = block.append(key, value.clone()).await?;
        //todo 添加缓存
        self.cache.set(offset,value).await;
        offset.ok()
    }

    async fn get(&self, offset: u64) -> anyhow::Result<Arc<Vec<u8>>> {
        let sn = (offset / self.size as u64) as u32;
        let now_sn = self.sn.load(Ordering::Relaxed);
        if sn > now_sn {
            return Ok(Arc::new(Vec::new()))
        }
        let offset = offset % self.size as u64;
        if sn == now_sn {
            if let Some(s) = self.cache.get(offset).await{
                return s.ok()
            }
        }
        //如果是最新快则直接返回
        if sn == self.sn.load(Ordering::Relaxed) {
            return Ok(Arc::new(Vec::new()))
        }
        let block = match self.blocks.share().get(&sn){
            None => return Ok(Arc::new(Vec::new())),
            Some(s) => s.clone(),
        };
        let (_,v) = block.get(offset).await?;v.ok()
    }
}

#[cfg(test)]
mod test{
    use std::sync::Arc;
    use wd_tools::PFArc;
    use crate::core::BucketDataBase;
    use crate::local_store::{FileBlockManage, LocalStoreEntity, NodeValeCodec, RWLockCache};

    #[tokio::test]
    async fn test_local_store_entity(){
        let manager = FileBlockManage::new(".".into(), 1024 * 1024, Arc::new(NodeValeCodec)).arc();
        let cache = RWLockCache::default().arc();
        let store = LocalStoreEntity::new(manager,cache).await.expect("LocalStoreEntity.new failed");

        let key = 666;
        let value = b"wdb very nb".to_vec();

        let offset = store.set(key, value.clone()).await.expect("LocalStoreEntity.set failed");

        let info = store.get(31).await.expect("LocalStoreEntity.get 31 failed");
        println!("31--->{}",String::from_utf8_lossy(info.as_slice()));
        let info = store.get(offset).await.expect("LocalStoreEntity.get failed");

        println!("{}--->{}",offset,String::from_utf8_lossy(info.as_slice()));
        assert_eq!(value.as_slice(),info.as_slice(),"test_local_store_entity failed")
    }
}