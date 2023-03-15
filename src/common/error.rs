use thiserror::Error;

#[derive(Debug,Error)]
pub enum WDBError{
    #[error("decode node value unexpect length({0})")]
    DecodeNodeValueLengthError(u32),
    #[error("decode node value check failed, data may be tampered with")]
    DecodeNodeValueCheckFailed,
    #[error("io error")]
    IO(#[from] std::io::Error),
    #[error("block at({0}) abnormal")]
    BlockAbnormal(u64)
}

pub type WDBResult<T> = Result<T,WDBError>;