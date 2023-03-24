use crate::common::WDBResult;
use crate::core::{Block, Codec, DataBaseBlockManager};
use crate::local_store::FileBlock;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use wd_tools::{PFArc, PFOk, PFSome};

pub struct FileBlockManage {
    size: u32,
    dir: String,
    codec: Arc<dyn Codec>,
    blocks: Arc<RwLock<HashMap<u32, Arc<dyn Block>>>>,
    sn: AtomicU32,
}

impl FileBlockManage {
    pub fn new(dir: String, size: u32, codec: Arc<dyn Codec>) -> Self {
        Self {
            size,
            dir,
            codec,
            blocks: Default::default(),
            sn: Default::default(),
        }
    }
    pub fn search_dir(dir: String) -> WDBResult<Vec<u32>> {
        let mut list = vec![];
        let paths = std::fs::read_dir(dir.as_str())?;
        for i in paths {
            let p = i.expect("FileBlockManage.search_dir:").path();
            if !p.is_file() {
                continue;
            }
            if let Some(s) = p.extension() {
                if s != "wdb" {
                    continue;
                }
            } else {
                continue;
            }
            let name = p
                .file_name()
                .expect("FileBlockManage.search_dir.file_name:")
                .to_str()
                .unwrap();
            if name.len() < 5 {
                wd_log::log_error_ln!("unknown wdb file:{:?}", p.to_str());
                continue;
            }
            let len = name.len();
            let sn = u32::from_str(&name[..len - 4]).expect("unknown file name");
            list.push(sn);
        }
        list.sort();
        list.ok()
    }
    pub fn path(&self, sn: u32) -> String {
        format!("{}/{}.wdb", self.dir, sn)
    }

    async fn insert_block(&self, sn: u32, block: Arc<dyn Block>) {
        let mut w = self.blocks.write().await;
        w.insert(sn, block);
    }
    async fn get_block(&self, sn: u32) -> Option<Arc<dyn Block>> {
        let r = self.blocks.read().await;
        let b = r.get(&sn)?;
        b.clone().some()
    }
}

#[async_trait::async_trait]
impl DataBaseBlockManager for FileBlockManage {
    async fn init_block(&self) -> WDBResult<Vec<(u32, Arc<dyn Block>)>> {
        let mut files = FileBlockManage::search_dir(self.dir.clone())?;
        files.sort();
        let len = files.len();
        if len > 1 {
            self.sn.store(files[len - 1], Ordering::Relaxed);
        }

        let mut list = vec![];
        for i in files.into_iter() {
            let block = self.create_block(i).await?;
            list.push((i, block));
        }
        list.ok()
    }

    async fn create_block(&self, block_sn: u32) -> WDBResult<Arc<dyn Block>> {
        let sn = self.sn.load(Ordering::Relaxed);
        let block = FileBlock::new(
            self.path(block_sn),
            self.codec.clone(),
            self.size,
            block_sn >= sn,
        )
        .await?;
        let block = block.arc();
        self.insert_block(block_sn, block.clone()).await;
        if block_sn > sn {
            self.sn.store(block_sn, Ordering::Relaxed);
        }
        Ok(block)
    }

    async fn get(&self, sn: u32) -> Option<Arc<dyn Block>> {
        self.get_block(sn).await
    }

    fn block_size(&self) -> u32 {
        self.size
    }
}
