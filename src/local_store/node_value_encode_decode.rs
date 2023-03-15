use std::sync::Arc;
use wd_tools::{MD5, PFBox, PFErr, PFOk};
use crate::common::{WDBError, WDBResult};
use crate::core::Codec;

#[derive(Debug)]
pub struct NodeValeCodec;

impl NodeValeCodec{
    pub fn simple_md5(mut src:&[u8]) ->Vec<u8>{
        let mut src = src.md5();
        let len = src.len() - 1;
        if len < 8 {
            return src
        }
        for i in 0..8 {
            src[i] ^= src[len-i]
        }
        while src.len() > 8 {
            src.pop();
        }
        return src
    }
}


// #[async_trait::async_trait]
impl Codec for NodeValeCodec {
     fn encode(&self, key: u64, mut value: Arc<Vec<u8>>) -> Vec<u8> {
        let mut key_buf = key.to_le_bytes().to_vec();
        let mut sign_buf = if value.is_empty(){
            Vec::new()
        }else{
            NodeValeCodec::simple_md5(value.as_slice())
        };
        let len = (key_buf.len() + sign_buf.len() + value.len()) as u32;
        let mut buf = len.to_le_bytes().to_vec();
        let mut value_buf = value.as_ref().clone();
        buf.append(&mut key_buf);
        buf.append(&mut value_buf);
        buf.append(&mut sign_buf);
        return buf
    }

     fn decode(&self, mut data: Vec<u8>) -> WDBResult<(u64, Vec<u8>)> {
        let len = data.len();
        if len < 8 {
            return WDBError::DecodeNodeValueLengthError(8).err()
        }
        let key = u64::from_le_bytes([data[0],data[1],data[2],data[3],data[4],data[5],data[6],data[7]]);
        if len == 8 {
            return (key,Vec::new()).ok()
        };
        if len <= 16 {
            return WDBError::DecodeNodeValueLengthError(17).err()
        }
        let sing = NodeValeCodec::simple_md5(&data[8..len-8]);
        if sing != &data[len-8..] {
            return WDBError::DecodeNodeValueCheckFailed.err()
        }
        // for _ in 0..8 {
        //     data.pop();
        //     data.remove(0);
        // }
        return (key,data[8..len-8].to_vec()).ok()
    }
}

#[cfg(test)]
mod test{
    use wd_tools::PFArc;
    use crate::core::Codec;
    use super::NodeValeCodec;
    #[test]
    fn test_encode_decode(){
        let key = 123u64;
        let value = b"hello world".to_vec().arc();

        let mut data = NodeValeCodec.encode(key, value.clone());
        let value_len = u32::from_le_bytes([data[0],data[1],data[2],data[3]]);
        assert_eq!(4+8+11+8,data.len(),"len is error");
        assert_eq!(8+11+8,value_len,"value len is error");
        data.remove(0);
        data.remove(0);
        data.remove(0);
        data.remove(0);
        let (k,v) = NodeValeCodec.decode(data).expect("decode failed");
        assert_eq!(k,key,"decode key failed");
        assert_eq!(v.as_slice(),value.as_slice(),"decode value failed")
    }
}