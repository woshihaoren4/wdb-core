use std::fs::File;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use wd_tools::{PFArc, PFErr, PFOk};
use crate::common::{WDBError, WDBResult};
use crate::core::{Bucket, BucketDataBase, BucketIndex, Codec, DataBaseBlockManager, IndexCollections, IndexModule, IndexModuleKind};
use crate::local_index::{IndexCollRWMap, IndexModuleImpl, LocalFileIndex};
use crate::local_store::{FileBlockManage, LocalStoreEntity, NodeValeCodec};

pub struct WDBuilder{
    dir:String,
    name:String,
    block_size:u32,
    db_module:IndexModuleKind,
    block_manager: Option<Arc<dyn DataBaseBlockManager>>,
    db: Option<Arc<dyn BucketDataBase>>,
    index: Option<Arc<dyn BucketIndex>>,
    codec: Arc<dyn Codec>,
    index_coll: Arc<dyn IndexCollections>,
    index_module: Arc<dyn IndexModule>,
}

impl<T> From<T> for WDBuilder
where T: AsRef<str>
{
    fn from(path: T) -> Self {
        let dir = path.as_ref().to_string();
        let name = "default".into();
        let block_size = 1024*1024*1024u32; //1g
        let db_module = IndexModuleKind::KV;
        let codec = NodeValeCodec.arc();
        // let block_manager = FileBlockManage::new(format!("{}/{}",dir,name),block_size,codec.clone()).arc();
        let block_manager = None;
        let db = None;
        let index = None;
        let index_coll = IndexCollRWMap::default().arc();
        let index_module = IndexModuleImpl{}.arc();
        Self{
            dir,
            name,
            block_size,
            db_module,
            block_manager,
            db,
            index,
            codec,
            index_coll,
            index_module,
        }
    }
}
macro_rules! build {
    ($(($name:tt:$ty:ty)),*) => {
        impl WDBuilder {
            $(
                pub fn $name(mut self,val:$ty) ->Self{
                    self.$name = val;self
                }
            )*
        }
    };
}
macro_rules! build_arc {
    ($(($name:tt:$ty:ident)),*) => {
        impl WDBuilder {
            $(
                pub fn $name<T:$ty + 'static>(mut self,val:T) ->Self{
                    self.$name =Arc::new(val);;self
                }
            )*
        }
    };
}
macro_rules! build_opt {
    ($(($name:tt:$ty:ident)),*) => {
        impl WDBuilder {
            $(
                pub fn $name<T:$ty + 'static>(mut self,val:T) ->Self{
                    self.$name = Some(Arc::new(val));self
                }
            )*
        }
    };
}

impl WDBuilder{
    pub fn new()->Self{
        WDBuilder::from(".")
    }
    pub fn map<F:FnOnce(WDBuilder)->WDBuilder>(self,function:F)->Self{
        function(self)
    }
    pub async fn map_async<Fut,F>(self,function:F)->Self
    where Fut:Future<Output=Self> + Send,
    F: FnOnce(Self)->Fut + Send
    {
        function(self).await
    }
    pub async fn build(&self)->WDBResult<Bucket>{
        if !wd_tools::fs::exist(self.dir.as_str(),wd_tools::fs::FileType::DIR).await? {
            return WDBError::Unknown(anyhow::anyhow!("dir[{}] not exist",self.dir)).err()
        }
        let path = format!("{}/{}",self.dir,self.name);
        if !wd_tools::fs::exist(path.as_str(),wd_tools::fs::FileType::DIR).await? {
            tokio::fs::create_dir(path.as_str()).await?;
        }

        let bm = if let Some(ref s) = self.block_manager {
            s.clone()
        }else{
            FileBlockManage::new(path.clone(), self.block_size, self.codec.clone()).arc()
        };

        let lse = if let Some(ref s) = self.db{
            s.clone()
        }else {
            LocalStoreEntity::new(bm.clone()).await?.arc()
        };

        let index = match self.index {
            None => {
                LocalFileIndex::new(path,self.index_coll.clone(),bm.clone(),self.index_module.clone()).await?.arc()
            }
            Some(ref s) => {s.clone()}
        };
        Bucket::new(lse,index).ok()
    }
}
build!((dir:String),(name:String),(block_size:u32),(db_module:IndexModuleKind));
build_arc!((codec:Codec),(index_coll:IndexCollections),(index_module:IndexModule));
build_opt!((block_manager:DataBaseBlockManager),(db:BucketDataBase),(index:BucketIndex));


#[cfg(test)]
mod test{
    use crate::db::WDBuilder;

    #[tokio::test]
    pub async fn build_test(){
        let bucket = WDBuilder::new()
            .dir("./database".into())
            .name("test".into())
            .block_size(1024 * 1024 * 32)
            .map(|x| {
                wd_log::log_debug_ln!("path--->{}/{}",x.dir,x.name);
                x
            })
            .build()
            .await
            .expect("build wdb error:");

        bucket.set(1,b"hello world").await.expect("insert failed");
        let value = bucket.find(&1).await.expect("find error");
        assert_eq!(b"hello world",value.as_slice(),"test one");

        bucket.set(2,b"wdb every nb").await.expect("insert failed");
        let value = bucket.find(&2).await.expect("find error");
        assert_eq!(b"wdb every nb",value.as_slice(),"test one");

        tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
    }
}