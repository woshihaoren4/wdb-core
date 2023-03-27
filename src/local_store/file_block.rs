use crate::common::{MemoryFileReadOnly, WDBError, WDBResult};
use crate::core::{Block, Codec};
use crate::local_store::RWLockCache;
use async_channel::{Receiver, Sender};
use nix::libc::scanf;
use std::collections::HashMap;
use std::io::SeekFrom;
use std::ops::Deref;
use std::os::fd::AsRawFd;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::{Mutex, RwLock};
use wd_tools::sync::NullLock;
use wd_tools::{PFArc, PFErr, PFOk};

pub struct FileBlock {
    status: Arc<AtomicU8>,
    path: String,
    size: u32,
    codec: Arc<dyn Codec>,
    file: Arc<NullLock<File>>,
    mf: Arc<NullLock<MemoryFileReadOnly<'static>>>,
    buffer: Sender<Vec<u8>>,
    offset: Arc<AtomicUsize>,
    lock: Mutex<()>,
    cache: Arc<RwLock<HashMap<u64, (u64, Vec<u8>)>>>,
}

impl FileBlock {
    pub async fn new(
        path: String,
        codec: Arc<dyn Codec>,
        size: u32,
        allow_writer: bool,
    ) -> anyhow::Result<FileBlock> {
        let file_raw = tokio::fs::OpenOptions::default()
            .read(true)
            .append(true)
            .create(true)
            .open(path.as_str())
            .await?;
        let offset = file_raw.metadata().await?.len() as u64;

        let mf = NullLock::new().arc();
        let file = NullLock::new().arc();
        let status = AtomicU8::new(1).arc(); //默认可写
        let mut cache = HashMap::new();

        //文件自检 如果确定文件没有问题可以跳过自检
        let amf = MemoryFileReadOnly::new(file_raw.as_raw_fd(), offset as usize)?;
        // if check && offset != 0{
        //     FileBlock::check_file(offset as usize, codec.clone(), &amf)?;
        // }
        mf.init(amf).await;
        if !allow_writer {
            //文件已满
            status.store(2, Ordering::Relaxed);
        } else {
            if offset != 0 {
                let (sender, receiver) = async_channel::bounded(1024);
                tokio::spawn(FileBlock::scan(codec.clone(), mf.clone(), sender));
                while !receiver.is_empty() || !receiver.is_closed() {
                    //todo 文件可能受损,应该尝试修复
                    let (offset, key, value) = receiver.recv().await??;
                    cache.insert(offset, (key, value));
                }
            }
            mf.reset().await;

            file.init(file_raw).await;
        }

        let offset = AtomicUsize::new(offset as usize).arc();
        //开启缓存模式
        let buffer = FileBlock::start_cache_schedule(
            offset.clone(),
            status.clone(),
            mf.clone(),
            file.clone(),
            1024 * 1024 * 4,
            1024 * 16,
        )
        .await;

        let lock = Mutex::new(());
        let cache = RwLock::new(cache).arc();
        FileBlock {
            status,
            path,
            size,
            codec,
            file,
            mf,
            buffer,
            offset,
            lock,
            cache,
        }
        .ok()
    }
    // pub fn check_file(size:usize, codec: Arc<dyn Codec>,mf: &MemoryFileReadOnly<'static>)->WDBResult<()>{
    //     let mut offset = 0usize;
    //     while offset < size {
    //         let len_buf = mf.range(offset,offset+4);
    //         let val_len = u32::from_le_bytes([len_buf[0], len_buf[1], len_buf[2], len_buf[3]]) as usize;
    //         if val_len > size || val_len < 8 {
    //             return WDBError::BlockAbnormal(offset as u64).err()
    //         }
    //         codec.check(mf.range(offset+4,offset+4+val_len))?;
    //         offset += 4 + val_len;
    //     }
    //     ().ok()
    // }

    pub async fn scan(
        codec: Arc<dyn Codec>,
        mf: Arc<NullLock<MemoryFileReadOnly<'static>>>,
        sender: Sender<WDBResult<(u64, u64, Vec<u8>)>>,
    ) {
        let size = match mf.map(|x| x.len).await {
            None => {
                sender.close();
                return;
            }
            Some(s) => s,
        };
        let mut i = 0;
        while i < size {
            let result = mf
                .map::<anyhow::Result<u32>, _>(|x| {
                    if i + 4 >= size {
                        return anyhow::anyhow!("header len failed").err();
                    }
                    let len_buf = x.range(i, i + 4);
                    let len = u32::from_le_bytes([len_buf[0], len_buf[1], len_buf[2], len_buf[3]]);
                    Ok(len)
                })
                .await
                .unwrap();

            let val_len = match result {
                Ok(o) => o as usize,
                Err(e) => {
                    wd_log::log_error_ln!("BlockAbnormal {}", e);
                    sender
                        .send(WDBError::BlockAbnormal(i as u64).err())
                        .await
                        .unwrap();
                    break;
                }
            };
            if val_len > size {
                wd_log::log_error_ln!("BlockAbnormal node len({}) > max size({})", val_len, size);
                sender
                    .send(WDBError::BlockAbnormal(i as u64).err())
                    .await
                    .unwrap();
                break;
            }
            if val_len < 8 {
                wd_log::log_error_ln!("BlockAbnormal node len({}) < min size(8)", val_len);
                sender
                    .send(WDBError::BlockAbnormal(i as u64).err())
                    .await
                    .unwrap();
                break;
            }

            let result = mf
                .map::<anyhow::Result<Vec<u8>>, _>(|x| {
                    if i + 4 + val_len > size {
                        return anyhow::anyhow!("value len failed").err();
                    }
                    x.range(i + 4, i + 4 + val_len).to_vec().ok()
                })
                .await
                .unwrap();

            let buf = match result {
                Ok(o) => o,
                Err(e) => {
                    wd_log::log_error_ln!("BlockAbnormal: {}", e);
                    sender
                        .send(WDBError::BlockAbnormal(i as u64).err())
                        .await
                        .unwrap();
                    break;
                }
            };
            let len = buf.len();
            if len == 8 {
                let key = u64::from_le_bytes([
                    buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
                ]);
                sender.send((i as u64, key, Vec::new()).ok()).await.unwrap();
            } else {
                match codec.decode(buf) {
                    Ok((k, v)) => {
                        sender.send((i as u64, k, v).ok()).await.unwrap();
                    }
                    Err(e) => {
                        wd_log::log_error_ln!("BlockAbnormal decode error:{}", e);
                        sender
                            .send(WDBError::BlockAbnormal(i as u64).err())
                            .await
                            .unwrap();
                    }
                };
            }
            i += val_len + 4;
        }

        sender.close();
    }

    pub async fn real_time_append(&self, key: u64, value: &[u8]) -> WDBResult<u64> {
        let buf = self.codec.encode(key, value);

        let mut file = self.file.map_raw().write().await;
        let file = file.as_mut().unwrap();

        let offset = file.metadata().await?.len();
        if offset >= self.size as u64 {
            return WDBError::BlockFulled.err();
        }

        if let Err(e) = file.write_all(buf.as_slice()).await {
            return WDBError::from(e).err();
        }
        offset.ok()
    }
    pub async fn buffer_append(&self, key: u64, value: &[u8]) -> WDBResult<u64> {
        let buf = self.codec.encode(key, value);
        let len = buf.len() as u32;
        let _ = self.lock.lock().await;
        let offset = self.offset.load(Ordering::Relaxed);

        if offset >= self.size as usize {
            return WDBError::BlockFulled.err();
        }

        let new_offset = offset + len as usize;
        self.buffer
            .send(buf)
            .await
            .expect("FileBlock.buffer_append failed");
        self.offset.store(new_offset, Ordering::Relaxed);

        if new_offset >= self.size as usize {
            self.buffer.close();
        }

        return (offset as u64).ok();
    }

    pub async fn start_cache_schedule(
        offset: Arc<AtomicUsize>,
        status: Arc<AtomicU8>,
        mf: Arc<NullLock<MemoryFileReadOnly<'static>>>,
        file: Arc<NullLock<File>>,
        buf_max: u32,
        chan_max: usize,
    ) -> Sender<Vec<u8>> {
        let buf_max = buf_max as usize;
        let mut buf = vec![];
        let (sender, receiver) = async_channel::bounded(chan_max);
        tokio::spawn(async move {
            while !receiver.is_closed() || !receiver.is_empty() || !buf.is_empty() {
                if receiver.is_empty() {
                    tokio::time::sleep(std::time::Duration::from_millis(3)).await;
                    continue;
                }

                while buf.len() < buf_max && !receiver.is_empty() {
                    let mut data = match receiver.recv().await {
                        Ok(o) => o,
                        Err(_) => break,
                    };
                    buf.append(&mut data);
                }
                let raw = file.map_raw();
                let mut w = raw.write().await;
                if w.is_none() {
                    wd_log::log_error_ln!("cache_schedule file inner is nil");
                    continue;
                }
                let f = w.as_mut().unwrap();

                if let Err(e) = f.write_all(buf.as_slice()).await {
                    wd_log::log_error_ln!("cache_schedule write file failed {}", e);
                } else {
                    buf = vec![];
                }
            }
            //转变为文件映射
            for i in 1..u64::MAX {
                let raw = file.map_raw();
                let mut w = raw.read().await;
                if let Some(s) = w.as_ref() {
                    let len = offset.load(Ordering::Relaxed);
                    let mfr = match MemoryFileReadOnly::new(s.as_raw_fd(), len) {
                        Ok(o) => o,
                        Err(e) => {
                            wd_log::log_error_ln!("file mmap failed:{}", e);
                            tokio::time::sleep(tokio::time::Duration::from_secs(i * i)).await;
                            continue;
                        }
                    };
                    mf.init(mfr).await;
                }
                status.store(2, Ordering::Relaxed);
                break;
            }
        });
        return sender;
    }
}

#[async_trait::async_trait]
impl Block for FileBlock {
    async fn append(&self, key: u64, value: &[u8]) -> WDBResult<u64> {
        // let offset = self.real_time_append(key, value).await?;
        let offset = self.buffer_append(key, value).await?;

        //加入到缓存中
        let mut w = self.cache.write().await;
        w.insert(offset, (key, value.to_vec()));
        offset.ok()
    }

    async fn get(&self, offset: u64) -> WDBResult<(u64, Vec<u8>)> {
        if self.status.load(Ordering::Relaxed) == 1 {
            //尝试从缓存拿
            let x = self.cache.read().await;
            if let Some(s) = x.get(&offset) {
                return s.clone().ok();
            }
            drop(x);
        }
        if self.status.load(Ordering::Relaxed) == 2 {
            //尝试从内存中取值
            let offset = offset as usize;
            let size = self.size;
            let codec = self.codec.clone();
            let value = self
                .mf
                .map::<anyhow::Result<(u64, Vec<u8>)>, _>(|x| {
                    let len_buf = x.range(offset, offset + 4);
                    let len = u32::from_le_bytes([len_buf[0], len_buf[2], len_buf[3], len_buf[4]]);
                    if len < 8 || len > size {
                        return anyhow::anyhow!("value is too lenght").err();
                    }
                    let val = x.range(offset + 4, offset + 4 + len as usize).to_vec();
                    let res = codec.decode(val)?;
                    res.ok()
                })
                .await;
            return match value {
                None => WDBError::from(anyhow::anyhow!("FileBlock.mf is nil")).err(),
                Some(x) => {
                    let a = x?;
                    a.ok()
                }
            };
        }
        return WDBError::NotFound.err();
    }

    async fn traversal(&self) -> Receiver<WDBResult<(u64, u64, Vec<u8>)>> {
        let (sender, receiver) = async_channel::bounded(1024);
        let len = self.size().await;
        let status = self.status.clone();
        let mf = self.mf.clone();
        let cache = self.cache.clone();
        let codec = self.codec.clone();
        tokio::spawn(async move {
            let r = cache.read().await;
            if !r.is_empty() {
                for (offset, (key, val)) in r.deref().iter() {
                    sender
                        .send((offset.clone(), key.clone(), val.clone()).ok())
                        .await
                        .unwrap();
                }
                return;
            }
            if status.load(Ordering::Relaxed) == 2 {
                FileBlock::scan(codec, mf, sender).await;
            } else {
                sender.close();
            }
        });
        return receiver;
    }

    async fn size(&self) -> usize {
        self.offset.load(Ordering::Relaxed)
    }

    fn path(&self) -> String {
        self.path.clone()
    }

    fn status(&self) -> u8 {
        self.status.load(Ordering::Relaxed)
    }

    // async fn restore(&self,_position:u64) -> anyhow::Result<()> {
    //     return anyhow::anyhow!("unsupported restore").err()
    // }
}

impl Drop for FileBlock {
    fn drop(&mut self) {
        self.buffer.close();
    }
}

#[cfg(test)]
mod test {
    use crate::core::Block;
    use crate::local_store::{FileBlock, NodeValeCodec};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_file_block() {
        let block = FileBlock::new(
            "./database/0.wdb".into(),
            Arc::new(NodeValeCodec),
            1024 * 1024,
            true,
        )
        .await
        .expect("FileBlock.new failed");
        let data = b"hello world";
        let offset = block
            .append(456, &data[..])
            .await
            .expect("FileBlock.append failed");
        println!("offset-->{}", offset);
        let (key, value) = block.get(offset).await.expect("FileBlock.get failed");
        println!(
            "key->{},value->[{}]",
            key,
            String::from_utf8_lossy(value.as_slice()).to_string()
        );
        assert_eq!(456, key, "test_file_block key failed");
        assert_eq!(
            data.as_slice(),
            value.as_slice(),
            "test_file_block value failed"
        );
    }
    #[tokio::test]
    async fn test_file_block_scan() {
        let block = FileBlock::new(
            "./database/0.wdb".into(),
            Arc::new(NodeValeCodec),
            1024 * 1024,
            true,
        )
        .await
        .expect("FileBlock.new failed");
        let receiver = block.traversal().await;
        while !receiver.is_empty() || !receiver.is_closed() {
            let result = receiver.recv().await;
            let result = match result {
                Ok(o) => o,
                Err(e) => {
                    // wd_log::log_error_ln!("recover error:{}",e);
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                    continue;
                }
            };
            let (offset, key, value) = match result {
                Ok(o) => o,
                Err(e) => {
                    wd_log::log_error_ln!("FileBlock.scan parse failed:{}", e);
                    break;
                }
            };
            println!(
                "offset[{}] key[{}] ---> {}",
                offset,
                key,
                String::from_utf8_lossy(value.as_slice())
            );
        }
        println!("test ove");
    }
}
