use std::os::fd::AsRawFd;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use wd_tools::{PFErr, PFOk};
use crate::common;
use crate::common::MemoryFileReadOnly;
use crate::core::{BucketIndex, IndexCollections, DataBaseBlockManager, IndexPersistenceStrategy};

pub struct LocalFileIndex{
    inner:Arc<dyn IndexCollections<(u64,u64)>>, //index offset,value
    // bm: Arc<dyn DataBaseBlockManager>
}

impl LocalFileIndex {
    pub async fn new(dir:String,inner:Arc<dyn IndexCollections<(u64,u64)>>,bm: Arc<dyn DataBaseBlockManager>,ps:Arc<dyn IndexPersistenceStrategy>)->anyhow::Result<Self>{
        let (sn,file) = LocalFileIndex::load_index_from_file(format!("{}/wdb_index.cache",dir.as_str()),inner.clone()).await?;
        //开启异步写入索引固化
        Self{inner}.ok()
    }

    async fn load_index_from_file(path:String,index:Arc<dyn IndexCollections<(u64,u64)>>)->anyhow::Result<(u32,File)>{
        let mut file = tokio::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path.as_str()).await?;
        let len = file.metadata().await?.len();
        if len == 0 { //需要初始化
            file.write(&[255, 255, 255, 255]).await?;
            return (0,file).ok()
        }
        //获取sn编号
        let mf = common::MemoryFileReadOnly::new(file.as_raw_fd(), len as usize)?;

        if mf.len < 4 {
            return anyhow::anyhow!("index file[{}] header len failed",path).err()
        }
        let sn = u32::from_le_bytes([mf.data[0], mf.data[1], mf.data[2], mf.data[3]]);

        if mf.len == 4 {
            return (sn,file).ok()
        }

        //检查索引文件是否损毁
        if mf.len - 4 % 16 != 0 {
            //todo 需要尝试恢复文件
            return anyhow::anyhow!("index file[{}] check exception",path).err();
        }
        //并发解析索引信息
        LocalFileIndex::parse_index_from_mf(4,&mf.data[4..],index).await;

        (sn,file).ok()
    }
    async fn parse_index_from_mf(offset:u64,buf:&[u8],index:Arc<dyn IndexCollections<(u64,u64)>>){
        let mut i = 0;
        let len = buf.len() as u64;
        while i + 16 < len {
            let key = u64::from_be_bytes([buf[0],buf[2],buf[3],buf[4],buf[5],buf[6],buf[7],buf[8]]);
            let value = u64::from_be_bytes([buf[9],buf[10],buf[11],buf[12],buf[13],buf[14],buf[15],buf[16]]);
            let index_offset = i+offset;
            index.push(key,(index_offset,value)).await;
            i+=16;
        }
    }
    async fn cache_block_to_file(start:u32,inner:Arc<dyn IndexCollections<(u64,u64)>>,bm: Arc<dyn DataBaseBlockManager>,ps:Arc<dyn IndexPersistenceStrategy>){
        todo!()
    }
}

#[cfg(test)]
mod test{
    #[test]
    fn test(){
        let max = u32::MAX;
        let slice = max.to_be_bytes();
        println!("{:?}",slice);
    }
}