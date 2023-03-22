use std::collections::HashMap;
use std::sync::Arc;
use async_channel::Receiver;
use tokio::io;
use crate::common::WDBResult;

//数据区
#[async_trait::async_trait]
pub trait BucketDataBase{
    async fn set(&self,key:u64,value:&[u8])->anyhow::Result<u64>; //插入后返回偏移量
    async fn get(&self, offset:u64) ->anyhow::Result<Arc<Vec<u8>>>;
}

//索引
#[async_trait::async_trait]
pub trait BucketIndex{
    async fn push(&self,key:u64,offset:u64);
    async fn find(&self,key:u64)->Option<Vec<u64>>;
}

//索引在内存中存放的容器，需要不同的数据结构实现
#[async_trait::async_trait]
pub trait IndexCollections:Send+Sync{
    async fn push(&self,key:u64,value:u64);
    async fn find(&self,key:&u64)->Option<Vec<u64>>;
    async fn update_index(&self,key:u64,offset:u64); //更新索引位置
    async fn find_index(&self,key:&u64)->Option<u64>; //查询索引的位置
}

pub enum IndexModuleKind{
    KV,
    TIME,
    LOG
}

pub trait IndexModule:Send+Sync {
    fn module(&self)->IndexModuleKind {  //1:kv库 2:时序库 3:日志库
        return IndexModuleKind::KV
    }

    fn status(&self)->bool{
        return true;
    }
    /// status: 当前已固化的块
    /// end: 可固化的块
    fn persistence(&self,start:u32,end:u32) ->Option<u64>{
        return if end > start {
            None
        }else {
            Some(60)
        }
    }
    fn error_handler(&self,err:io::Error)->bool{
        return false
    }
}

//编解码器
// #[async_trait::async_trait]
pub trait Codec:Send+Sync{
    fn encode(&self,key:u64,value:&[u8])->Vec<u8>;
    fn decode(&self,data:Vec<u8>)->WDBResult<(u64,Vec<u8>)>;  //返回key value
}

//区块
#[async_trait::async_trait]
pub trait Block:Send+Sync{
    async fn append(&self,key:u64,value:&[u8])->WDBResult<u64>;  //返回偏移量
    async fn get(&self,offset:u64)->WDBResult<(u64,Arc<Vec<u8>>)>;
    async fn traversal(&self)-> Receiver<WDBResult<(u64,u64,Vec<u8>)>>;
    async fn size(&self)->WDBResult<u64>;
    // async fn restore(&self, position:u64) ->anyhow::Result<()>;  //恢复数据
    fn path(&self)->String;
    fn status(&self)->u8;  //1:可用 2：固化
}
//区块管理器
#[async_trait::async_trait]
pub trait DataBaseBlockManager:Send+Sync{
    async fn init_block(&self)->WDBResult<Vec<(u32,Arc<dyn Block>)>>;
    async fn create_block(&self,block_sn:u32)->WDBResult<Arc<dyn Block>>;
    async fn get(&self, sn:u32) -> Option<Arc<dyn Block>>;

    fn block_size(&self)->u32;
}

//node缓存
#[async_trait::async_trait]
pub trait NodeCache:Send+Sync{
    async fn get(&self, offset: u64) -> Option<Arc<Vec<u8>>>;
    async fn set(&self, offset: u64, value:Arc<Vec<u8>>);
    async fn reset(&self);
}

