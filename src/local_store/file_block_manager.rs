use std::str::FromStr;
use std::sync::Arc;
use wd_tools::{PFArc, PFOk};
use crate::common::WDBResult;
use crate::core::{Block, Codec, DataBaseBlockManager};
use crate::local_store::FileBlock;

pub struct FileBlockManage{
    size: u32,
    dir: String,
    codec: Arc<dyn Codec>
}

impl FileBlockManage{
    pub fn new(dir:String,size:u32,codec: Arc<dyn Codec>)->Self{
        Self{size,dir,codec}
    }
    pub fn search_dir(dir:String)->WDBResult<Vec<u32>>{
        let mut list = vec![];
        let paths = std::fs::read_dir(dir.as_str())?;
        for i in paths {
            let p = i.expect("FileBlockManage.search_dir:").path();
            if !p.is_file() {
                continue
            }
            if let Some(s) = p.extension() {
                if s != "wdb" {
                    continue
                }
            }else{
                continue
            }
            let name = p.file_name().expect("FileBlockManage.search_dir.file_name:").to_str().unwrap();
            if name.len() < 5 {
                wd_log::log_error_ln!("unknown wdb file:{:?}",p.to_str());
                continue
            }
            let len = name.len();
            let sn = u32::from_str(&name[..len - 4]).expect("unknown file name");
            list.push(sn);
        }
        list.sort();
        list.ok()
    }
    pub fn path(&self,sn:u32)->String{
        format!("{}/{}.wdb", self.dir, sn)
    }
}

#[async_trait::async_trait]
impl DataBaseBlockManager for FileBlockManage {
    async fn init_block(&self) -> WDBResult<Vec<(u32, Arc<dyn Block>)>> {
        let files = FileBlockManage::search_dir(self.dir.clone())?;
        let mut list = vec![];
        for i in files.into_iter(){
            let block = self.create_block(i).await?;
            list.push((i,block));
        }
        list.ok()
    }

    async fn create_block(&self, block_sn: u32) -> WDBResult<Arc<dyn Block>> {
       let block = FileBlock::new(self.path(block_sn), self.codec.clone(),self.size).await?;Ok(Arc::new(block))
    }

    fn block_size(&self) -> u32 {
        self.size
    }
}