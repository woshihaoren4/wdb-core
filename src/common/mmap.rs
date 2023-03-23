use std::num::NonZeroUsize;
use std::os::fd::{RawFd};
use std::os::raw::c_void;
use nix::sys::mman::{MapFlags, MmapAdvise, ProtFlags};
use wd_tools::{ PFOk};

#[derive(Debug,Default)]
pub struct MemoryFileReadOnly<'a>{
    pub len:usize,
    raw_fd:usize,
    addr: usize,
    pub data: &'a [u8],
}

impl MemoryFileReadOnly<'_> {
    pub fn new(raw_fd:RawFd,len:usize)->anyhow::Result<Self>{

        if len == 0 {
            return Self{
                len,
                raw_fd: 0,
                addr: 0,
                data: &[],
            }.ok()
        }

        let non_zero_usize = NonZeroUsize::new(len).unwrap();
        unsafe {
            let addr = nix::sys::mman::mmap(None,
                                              non_zero_usize,
                                              ProtFlags::PROT_READ,
                                              MapFlags::MAP_SHARED,
                                              raw_fd,
                                              0)?;

            // nix::sys::mman::madvise(addr,len,MmapAdvise::MADV_RANDOM)?;

            let data: &[u8] = std::slice::from_raw_parts(addr as *const u8, len);
            return Self{
                len,
                raw_fd: raw_fd as usize,
                addr: addr as usize,
                data}.ok()
        }
    }
    pub fn preload(&mut self)->anyhow::Result<()>{
        unsafe {
            nix::sys::mman::madvise(self.addr as *mut c_void,self.len,MmapAdvise::MADV_RANDOM)?;
        }
        Ok(())
    }
    pub fn range(&self, left:usize, mut right:usize) ->&[u8]{
        if left>=right{
            return &[]
        }
        if left > self.len{
            return &[]
        }
        if right > self.len{
            right = self.len;
        }
        return &self.data[left..right]
    }
}

impl Drop for MemoryFileReadOnly<'_>{
    fn drop(&mut self) {
        if self.len != 0 {
            unsafe {
                if let Err(e) = nix::sys::mman::munmap(self.addr as *mut c_void,self.len){
                    wd_log::log_error_ln!("MemoryFileReadOnly.drop error:{}",e);
                }
            }
        }
    }
}


#[cfg(test)]
mod test{
    use std::os::fd::AsRawFd;
    use crate::common::MemoryFileReadOnly;

    #[tokio::test]
    async fn test_mmap(){
        let file = tokio::fs::File::open("./database/0.wdb").await.expect("open file failed");
        let len = file.metadata().await.expect("file metadata len failed").len() as usize;
        println!("len -->{}",len);
        let mut mf = MemoryFileReadOnly::new(file.as_raw_fd(), len).expect("file mmap failed");
        mf.preload().expect("preload file to memory failed");

        let s = mf.range(12, 105);
        assert_eq!(s,"男儿何不带吴钩，收取关山五十州，请君暂上凌烟阁，若个书生万户侯".as_bytes(),"mmap test failed");
        println!("{}",String::from_utf8_lossy(s));

    }
}