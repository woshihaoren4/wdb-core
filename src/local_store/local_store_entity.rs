use crate::common::{WDBError, WDBResult};
use crate::core::{Block, BucketDataBase, DataBaseBlockManager, NodeCache};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use wd_tools::sync::LessLock;
use wd_tools::{PFArc, PFErr, PFOk};

pub struct LocalStoreEntity {
    block_manager: Arc<dyn DataBaseBlockManager>,
    blocks: LessLock<HashMap<u32, Arc<dyn Block>>>,
    sn: Arc<AtomicU32>,
    size: u32,
}

impl LocalStoreEntity {
    pub async fn new(
        block_manager: Arc<dyn DataBaseBlockManager>,
    ) -> anyhow::Result<LocalStoreEntity> {
        let size = block_manager.block_size();
        let block_list = block_manager.init_block().await?;
        let mut blocks = HashMap::new();
        let mut sn = 0;
        if block_list.is_empty() {
            let first_block = block_manager.create_block(0).await?;
            blocks.insert(0, first_block);
        } else {
            for (i, block) in block_list.into_iter() {
                let len = block.size().await;
                if len != 0 {
                    sn = i;
                }
                blocks.insert(i, block);
            }
        }
        // LocalStoreEntity::load_block_cache(blocks.get(&sn).unwrap().clone(),cache.clone()).await;
        LocalStoreEntity {
            block_manager,
            blocks: LessLock::new(blocks),
            sn: AtomicU32::new(sn).arc(),
            size,
        }
        .ok()
    }
    // async fn load_block_cache(block:Arc<dyn Block>,cache : Arc<dyn NodeCache>){
    //     cache.reset().await;
    //     let recv = block.traversal().await;
    //     while !recv.is_closed() || !recv.is_empty() {
    //        let result = match recv.recv().await {
    //             Ok(o) => {
    //                 o
    //             }
    //             Err(_) => {
    //                 continue
    //             }
    //         };
    //         match result {
    //             Ok((offset,_key,value)) => {
    //                 cache.set(offset,value.arc()).await;
    //             },
    //             Err(e) => {
    //                 if let WDBError::BlockAbnormal(ref i) = e{
    //                     wd_log::log_error_ln!("db file({}) damage position[{}]，try skip the block other part",block.path(),i);
    //                     break
    //                 }else{
    //                     wd_log::log_panic!("load_block_cache unknown error:{}",e);
    //                 }
    //             }
    //         }
    //     }
    // }
    async fn extension_block(&self, old_sn: u32) -> anyhow::Result<()> {
        if old_sn < self.sn.load(Ordering::Relaxed) {
            return ().ok();
        }
        let sn = self.sn.clone();
        let mg = self.block_manager.clone();
        let result = self
            .blocks
            .async_update(|mut x| async move {
                if x.get(&(old_sn + 1)).is_some() {
                    return anyhow::anyhow!("success").err();
                }
                let block = mg.create_block(old_sn + 1).await?;
                x.insert(old_sn + 1, block);
                return x.ok();
            })
            .await;
        if let Err(err) = result {
            return if err.to_string().eq("success") {
                ().ok()
            } else {
                err.err()
            };
        }
        // self.cache.reset().await;  //缓存重置在写之前
        sn.fetch_add(1, Ordering::Relaxed);
        return ().ok();
    }
    async fn append_to_block(&self, sn: u32, key: u64, value: &[u8]) -> WDBResult<u64> {
        let block = match self.blocks.share().get(&sn) {
            None => return WDBError::BlockNonexistence(sn).err(),
            Some(s) => s.clone(),
        };
        block.append(key, value).await
    }
}

#[async_trait::async_trait]
impl BucketDataBase for LocalStoreEntity {
    async fn set(&self, key: u64, value: &[u8]) -> anyhow::Result<u64> {
        // let value = value.arc();
        loop {
            let sn = self.sn.load(Ordering::Relaxed);

            let result = self.append_to_block(sn, key, value).await;
            let mut offset = match result {
                Ok(o) => o,
                Err(err) => {
                    if let WDBError::BlockFulled = err {
                        self.extension_block(sn).await?;
                        continue;
                    }
                    return anyhow::anyhow!("{}", err).err();
                }
            };
            //添加缓存
            offset += sn as u64 * self.size as u64;
            // self.cache.set(offset,value.to_vec().arc()).await;
            return offset.ok();
        }
    }

    async fn get(&self, offset: u64) -> anyhow::Result<Vec<u8>> {
        let sn = (offset / self.size as u64) as u32;
        let now_sn = self.sn.load(Ordering::Relaxed);
        if sn > now_sn {
            return Ok(Vec::new());
        }
        let offset = offset % self.size as u64;
        // if sn == now_sn {
        //     if let Some(s) = self.cache.get(offset).await{
        //         return s.as_slice().to_vec().ok()
        //     }
        // }
        //如果是最新快则直接返回
        // if sn == self.sn.load(Ordering::Relaxed) {
        //     return Ok(Vec::new())
        // }
        let block = match self.blocks.share().get(&sn) {
            None => return Ok(Vec::new()),
            Some(s) => s.clone(),
        };
        let (_, v) = block.get(offset).await?;
        v.ok()
    }
}

#[cfg(test)]
mod test {
    use crate::core::BucketDataBase;
    use crate::local_store::{FileBlockManage, LocalStoreEntity, NodeValeCodec, RWLockCache};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use wd_tools::PFArc;

    #[tokio::test]
    async fn test_local_store_entity() {
        let manager =
            FileBlockManage::new("database".into(), 1024 * 1024, Arc::new(NodeValeCodec)).arc();
        // let cache = RWLockCache::default().arc();
        let store = LocalStoreEntity::new(manager)
            .await
            .expect("LocalStoreEntity.new failed");

        let key = 666;
        let value = b"wdb very nb".as_slice();

        let offset = store
            .set(key, value.clone())
            .await
            .expect("LocalStoreEntity.set failed");

        let info = store
            .get(offset)
            .await
            .expect("LocalStoreEntity.get failed");

        println!("{}--->{}", offset, String::from_utf8_lossy(info.as_slice()));
        assert_eq!(value, info.as_slice(), "test_local_store_entity failed");
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_local_store_extension_block() {
        let manager = FileBlockManage::new(
            "./database".into(),
            1024 * 1024 * 8,
            Arc::new(NodeValeCodec),
        )
        .arc();
        // let cache = RWLockCache::default().arc();
        let store = LocalStoreEntity::new(manager)
            .await
            .expect("LocalStoreEntity.new failed");

        let data = "男儿何不带吴钩，收取关山五十州，请君暂上凌烟阁，若个书生万户侯".as_bytes();

        let start = std::time::Instant::now();
        for i in 0..10_0000 {
            store
                .set(i, data)
                .await
                .expect("node local store insert failed");
            if i % 100000 == 0 {
                println!("插入进度 --->{}", i);
            }
        }
        let t = start.elapsed();
        println!("insert over, use time: {}ms", t.as_millis());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_local_store_mut_threads() {
        let manager =
            FileBlockManage::new("./database".into(), 1024 * 1024, Arc::new(NodeValeCodec)).arc();
        // let cache = RWLockCache::default().arc();
        let store = LocalStoreEntity::new(manager)
            .await
            .expect("LocalStoreEntity.new failed")
            .arc();

        let data = "男儿何不带吴钩，收取关山五十州，请君暂上凌烟阁，若个书生万户侯".as_bytes();

        let count = AtomicUsize::new(10_000).arc();
        let start = std::time::Instant::now();
        for _ in 0..10 {
            let data = data.clone();
            let store = store.clone();
            let count = count.clone();
            tokio::spawn(async move {
                for i in 0..1000 {
                    store
                        .set(i, data)
                        .await
                        .expect("node local store insert failed");
                    count.fetch_sub(1, Ordering::Relaxed);
                }
            });
        }
        while count.load(Ordering::Relaxed) != 0 {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        let t = start.elapsed();
        println!("insert over, use time: {}ms", t.as_millis());
    }
}
