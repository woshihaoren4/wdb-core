use thiserror::Error;

#[derive(Debug, Error)]
pub enum WDBError {
    #[error("decode node value unexpect length({0})")]
    DecodeNodeValueLengthError(u32),
    #[error("decode node value check failed, data may be tampered with")]
    DecodeNodeValueCheckFailed,
    #[error("io error")]
    IO(#[from] std::io::Error),
    #[error("block at({0}) abnormal")]
    BlockAbnormal(u64),
    #[error("the block is fulled")]
    BlockFulled,
    #[error("block[{0}] nonexistence")]
    BlockNonexistence(u32),
    #[error("unknown error")]
    Unknown(#[from] anyhow::Error),
    #[error("not found,The data file may be corrupted")]
    NotFound,
}

pub type WDBResult<T> = Result<T, WDBError>;
