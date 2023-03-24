use crate::common;
use crate::common::WDBError;
use crate::core::{BucketIndex, DataBaseBlockManager, IndexCollections, IndexModule, IndexModuleKind};
use std::io;
use std::io::SeekFrom;
use std::os::fd::AsRawFd;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use wd_tools::{PFErr, PFOk};

pub struct LocalFileIndex {
    inner: Arc<dyn IndexCollections>, //index offset,value
                                      // bm: Arc<dyn DataBaseBlockManager>
}

impl LocalFileIndex {
    pub async fn new(
        dir: String,
        inner: Arc<dyn IndexCollections>,
        bm: Arc<dyn DataBaseBlockManager>,
        ps: Arc<dyn IndexModule>,
    ) -> anyhow::Result<Self> {
        let (sn, file) = LocalFileIndex::load_index_from_file(
            format!("{}/wdb_index.cache", dir.as_str()),
            inner.clone(),
        )
        .await?;
        //开启异步写入索引固化
        tokio::spawn(LocalFileIndex::cache_block_to_file(
            sn,
            file,
            inner.clone(),
            bm,
            ps,
        ));
        Self { inner }.ok()
    }

    async fn load_index_from_file(
        path: String,
        index: Arc<dyn IndexCollections>,
    ) -> anyhow::Result<(u32, File)> {
        let mut file = tokio::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path.as_str())
            .await?;
        let len = file.metadata().await?.len();
        if len == 0 {
            //需要初始化
            file.write(&[255, 255, 255, 255]).await?;
            return (u32::MAX, file).ok();
        }
        //获取sn编号
        let mf = common::MemoryFileReadOnly::new(file.as_raw_fd(), len as usize)?;

        if mf.len < 4 {
            return anyhow::anyhow!("index file[{}] header len failed", path).err();
        }
        let sn = u32::from_le_bytes([mf.data[0], mf.data[1], mf.data[2], mf.data[3]]);

        if mf.len == 4 {
            return (sn, file).ok();
        }

        //检查索引文件是否损毁
        if (mf.len - 4) % 16 != 0 {
            //todo 需要尝试恢复文件
            return anyhow::anyhow!("index file[{}] check exception", path).err();
        }
        //并发解析索引信息
        LocalFileIndex::parse_index_from_mf(4, &mf.data[4..], index).await;

        (sn, file).ok()
    }
    async fn parse_index_from_mf(offset: u64, buf: &[u8], index: Arc<dyn IndexCollections>) {
        let mut i = 0;
        let len = buf.len() as u64;
        while i + 16 < len {
            let key = u64::from_be_bytes([
                buf[0], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7], buf[8],
            ]);
            let value = u64::from_be_bytes([
                buf[9], buf[10], buf[11], buf[12], buf[13], buf[14], buf[15], buf[16],
            ]);
            let index_offset = i + offset;
            index.push(key, value, index_offset).await;
            // index.update_index(key,index_offset).await;

            i += 16;
        }
    }
    async fn cache_block_to_file(
        start: u32,
        mut file: File,
        index: Arc<dyn IndexCollections>,
        bm: Arc<dyn DataBaseBlockManager>,
        ps: Arc<dyn IndexModule>,
    ) {
        let size = bm.block_size() as u64;
        //判断是否有需要固化的内容
        let mut sn = start;
        if start == u32::MAX {
            sn = 0;
        } else {
            sn += 1;
        }

        'lp: while ps.status() {
            let block = bm.get(sn).await;

            if block.is_none() {
                if let Some(wait) = ps.persistence(sn, sn) {
                    tokio::time::sleep(std::time::Duration::from_secs(wait)).await;
                    continue;
                }
            }
            let block = block.unwrap();

            if block.status() != 2 {
                if let Some(wait) = ps.persistence(sn, sn) {
                    tokio::time::sleep(std::time::Duration::from_secs(wait)).await;
                    continue;
                }
            }
            //开始固化
            let receiver = block.traversal().await;

            while !receiver.is_closed() || !receiver.is_empty() {
                let data = if let Ok(o) = receiver.recv().await {
                    o
                } else {
                    continue;
                };
                if let Err(ref e) = data {
                    if let WDBError::BlockAbnormal(_) = e {
                        //block abnormal
                        wd_log::log_error_ln!("block abnormal:{}", e);
                        break;
                    }
                }
                let (mut offset, k, _v) = data.unwrap();
                offset += size * sn as u64;

                let result = match ps.module() {
                    IndexModuleKind::KV | IndexModuleKind::FILE | IndexModuleKind::TIME => {
                        if let Some(index_offset) = index.find_index(&k).await {
                            LocalFileIndex::write(&mut file, index_offset, k, offset, false).await
                        } else {
                            LocalFileIndex::write(&mut file, 0, k, offset, true).await
                        }
                    }
                    IndexModuleKind::LOG => {
                        //不需要处理
                        break;
                    }
                };
                if let Err(e) = result {
                    if !ps.error_handler(e) {
                        continue 'lp;
                    }
                }
            }
            receiver.close();
            if let Err(_e) = LocalFileIndex::write_block(&mut file, sn).await {
                wd_log::log_error_ln!("cache_block_to_file seek fa")
            } else {
                wd_log::log_debug_ln!("scan block[{}] to index file", sn);
                sn += 1;
            }
        }
    }
    async fn write(
        file: &mut File,
        start_pos: u64,
        key: u64,
        value: u64,
        append: bool,
    ) -> io::Result<()> {
        let mut buf = key.to_be_bytes().to_vec();
        let mut val = value.to_be_bytes().to_vec();
        buf.append(&mut val);
        if append {
            file.seek(SeekFrom::End(0)).await?;
        } else {
            file.seek(SeekFrom::Start(start_pos)).await?;
        }
        file.write_all(buf.as_slice()).await
    }
    async fn write_block(file: &mut File, sn: u32) -> io::Result<()> {
        let buf = sn.to_be_bytes().to_vec();
        file.seek(SeekFrom::Start(0)).await?;
        file.write_all(buf.as_slice()).await
    }
}

#[async_trait::async_trait]
impl BucketIndex for LocalFileIndex{
    async fn push(&self, key: u64, offset: u64) {
        todo!()
    }

    async fn find(&self, key: u64) -> Option<Vec<u64>> {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use crate::core::DataBaseBlockManager;
    use crate::local_index::{IndexCollRWMap, IndexModuleImpl, LocalFileIndex};
    use crate::local_store::{FileBlockManage, NodeValeCodec};
    use std::sync::Arc;
    use wd_tools::PFArc;

    #[tokio::test]
    async fn test_index() {
        let im = IndexModuleImpl {}.arc();
        let ic = IndexCollRWMap::default().arc();
        println!("start new block manager");
        let manager = FileBlockManage::new(
            "./database".into(),
            1024 * 1024 * 8,
            Arc::new(NodeValeCodec),
        )
        .arc();
        println!("manager init block");
        let _ = manager
            .init_block()
            .await
            .expect("block manager init block failed");

        println!("start create index");
        let index = LocalFileIndex::new("./database".into(), ic, manager, im)
            .await
            .expect("index new failed");
        println!("index create over");
        tokio::time::sleep(tokio::time::Duration::from_secs(3600)).await
    }
}
