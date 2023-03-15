use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::io::{Error, ErrorKind, Read, Write};
use std::path::Path;
use std::str::FromStr;
use std::sync::{Arc};
use std::sync::atomic::AtomicU32;
use tokio::sync::{Mutex, RwLock};
use wd_tools::{PFArc, PFErr, PFOk};
use wd_tools::sync::LessLock;
use crate::common::{WDBError, WDBResult};
use crate::core::{Block, BucketDataBase, DataBaseBlockManager};

pub struct LocalStoreEntity{
    block_manager : Arc<dyn DataBaseBlockManager>,
    last_block_cache : Arc<RwLock<HashMap<u64,Vec<u8>>>>,
    blocks: LessLock<HashMap<u32,Arc<dyn Block>>>,
    sn : AtomicU32,
    size: u32,
}

impl LocalStoreEntity {
    pub async fn new(block_manager : Arc<dyn DataBaseBlockManager>)->anyhow::Result<LocalStoreEntity> {
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
        let last_block_cache = LocalStoreEntity::load_block_cache(blocks.get(&sn).unwrap().clone()).await;
        LocalStoreEntity{block_manager,last_block_cache,
            blocks : LessLock::new(blocks),
            sn: AtomicU32::new(sn),
            size}.ok()
    }
    pub async fn load_block_cache(block:Arc<dyn Block>)-> Arc<RwLock<HashMap<u64,Vec<u8>>>>{
        let recv = block.traversal().await;
        let mut map = HashMap::new();
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
                    map.insert(offset,value);
                },
                Err(e) => {
                    if WDBError::BlockAbnormal(i) = e{
                        wd_log::log_panic!("db file damage，try excision necrosis")
                        //todo 切除受损文件
                    }else{
                        wd_log::log_panic!("load_block_cache unknown error:{}",e);
                    }
                }
            }
        }
        return Arc::new(RwLock::new(map))
    }
}


#[async_trait::async_trait]
impl BucketDataBase for LocalStoreEntity
{
    async fn set(&self, key: u64, mut value: Vec<u8>) -> anyhow::Result<u64> {
        todo!()
    }

    async fn find(&self, offset: u64) -> anyhow::Result<Arc<Vec<u8>>> {
        todo!()
    }
}

#[cfg(test)]
mod test{
    use crate::core::BucketDataBase;
    use crate::local_store::LocalStoreEntity;

    #[tokio::test]
    async fn test_local_store_entity(){
        let  lse = LocalStoreEntity::new("./".into(),1024*1024).expect("set failed");

    }
}