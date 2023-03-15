use std::io;
use std::io::SeekFrom;
use std::sync::Arc;
use async_channel::{Receiver, Sender};
use tokio::fs::File;
use tokio::sync::{Mutex, RwLock};
use wd_tools::{PFArc, PFErr, PFOk};
use crate::common::{WDBError, WDBResult};
use crate::core::{Block, Codec};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

pub struct FileBlock{
    size: u32,
    codec : Arc<dyn Codec>,
    file: Arc<Mutex<File>>
}

impl FileBlock {
    pub async  fn new(path:String , codec : Arc<dyn Codec>,size:u32)->io::Result<Self>{
        let file = tokio::fs::OpenOptions::default()
            .read(true)
            .append(true)
            .create(true)
            .open(path).await?;
        let file = Mutex::new(file).arc();
        FileBlock{size,codec,file}.ok()
    }
    pub async fn scan(size:u32,codec: Arc<dyn Codec>,file: Arc<Mutex<File>>,sender:Sender<WDBResult<(u64, u64, Vec<u8>)>>){
        let mut file =file.lock().await;
        let mut i = 0 ;
        loop{
            let mut len_buf = vec![0;4];
            match file.read(&mut len_buf).await{
                Ok(o) => {
                    if o == 0 {
                        sender.clone();
                        return;
                    }else if o != 4 {
                        sender.send(WDBError::BlockAbnormal(i).err()).await.expect("FileBlock.scan send failed");
                        return;
                    }
                    o
                }
                Err(e) => {
                    wd_log::log_error_ln!("BlockAbnormal {}",e);
                    sender.send(WDBError::BlockAbnormal(i).err()).await.expect("FileBlock.scan send failed");
                    return;
                }
            };
            let val_len = u32::from_le_bytes([len_buf[0], len_buf[1], len_buf[2], len_buf[3]]);
            if val_len > size {

                wd_log::log_error_ln!("BlockAbnormal node len({}) > max size({})",val_len,size);
                sender.send(WDBError::BlockAbnormal(i).err()).await.expect("FileBlock.scan send failed");
                return;
            }
            if val_len < 8 {
                wd_log::log_error_ln!("BlockAbnormal node len({}) < min size(8)",val_len);
                sender.send(WDBError::BlockAbnormal(i).err()).await.expect("FileBlock.scan send failed");
                return;
            }
            let mut buf = vec![0;val_len as usize];
            let len = match file.read(&mut buf).await{
                Ok(o) => {
                    if o != val_len as usize {
                        sender.send(WDBError::BlockAbnormal(i).err()).await.expect("FileBlock.scan send failed");
                        return;
                    }
                    o
                }
                Err(e) => {
                    wd_log::log_error_ln!("BlockAbnormal {}",e);
                    sender.send(WDBError::BlockAbnormal(i).err()).await.expect("FileBlock.scan send failed");
                    return;
                }
            };
            if len == 8 {
                let key = u64::from_le_bytes([buf[0],buf[1],buf[2],buf[3],buf[4],buf[5],buf[6],buf[7]]);
                sender.send((i,key,Vec::new()).ok()).await.expect("FileBlock.scan send failed");
            }else{
                match codec.decode(buf){
                    Ok((k,v)) => {
                        sender.send((i,k,v).ok()).await.expect("FileBlock.scan send failed");
                    }
                    Err(e) => {
                        wd_log::log_error_ln!("BlockAbnormal decode error:{}",e);
                        sender.send(WDBError::BlockAbnormal(i).err()).await.expect("FileBlock.scan send failed");
                    }
                };
            }
            i += val_len as u64 + 4;
        }
    }
}

#[async_trait::async_trait]
impl Block for FileBlock{
    async fn append(&self,key:u64, value: Arc<Vec<u8>>) -> WDBResult<u64> {
        let buf = self.codec.encode(key,value.clone());

        let mut file = self.file.lock().await;

        let offset = file.metadata().await?.len();
        if let Err(e) = file.write_all(buf.as_slice()).await {
            return WDBError::from(e).err()
        }
        offset.ok()
    }

    async fn get(&self, offset: u64) -> WDBResult<(u64,Vec<u8>)> {
        let mut file = self.file.lock().await;
        file.seek(SeekFrom::Start(offset)).await?;
        let len = file.read_u32_le().await?;
        let mut buf = vec![0; len as usize];
        file.read_exact(&mut buf).await?;
        self.codec.decode(buf)
    }

    async fn traversal(&self) -> Receiver<WDBResult<(u64, u64, Vec<u8>)>> {
        let (sender,receiver) = async_channel::bounded(1024);
        tokio::spawn(FileBlock::scan(self.size,self.codec.clone(),self.file.clone(),sender));
        return receiver;
    }

    async fn size(&self) -> WDBResult<u64> {
        let mut file = self.file.lock().await;
        file.metadata().await?.len().ok()
    }
}

#[cfg(test)]
mod test{
    use std::sync::Arc;
    use async_channel::RecvError;
    use wd_tools::PFArc;
    use crate::common::WDBResult;
    use crate::core::Block;
    use crate::local_store::{FileBlock, NodeValeCodec};

    #[tokio::test]
    async fn test_file_block(){
        let block = FileBlock::new("./0.wdb".into(), Arc::new(NodeValeCodec),1024*1024).await.expect("FileBlock.new failed");
        let data = Vec::from("hello world").arc();
        let offset = block.append(456,data.clone()).await.expect("FileBlock.append failed");
        println!("offset-->{}",offset);
        let (key,value) = block.get(offset).await.expect("FileBlock.get failed");
        println!("key->{},value->[{}]",key,String::from_utf8_lossy(value.as_slice()).to_string());
        assert_eq!(456,key,"test_file_block key failed");
        assert_eq!(data.as_slice(),value.as_slice(),"test_file_block value failed");
    }
    #[tokio::test]
    async fn test_file_block_scan(){
        let block = FileBlock::new("./0.wdb".into(), Arc::new(NodeValeCodec),1024*1024).await.expect("FileBlock.new failed");
        let receiver = block.traversal().await;
        while !receiver.is_closed() {
            let result = receiver.recv().await;
            let (offset,key,value) = match result {
                Ok(o)=>o,
                Err(e)=>{
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                    continue
                }
            }.expect("from file parse error");
            println!("offset[{}] key[{}] ---> {}",offset,key,String::from_utf8_lossy(value.as_slice()));
        }
        println!("test ove")
    }

}