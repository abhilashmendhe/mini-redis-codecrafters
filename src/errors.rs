use thiserror::Error;
use tokio::task::JoinError;

#[derive(Debug, Error)]
pub enum RedisErrors {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("ParseIntError: {}", 0)]
    ParseInt(#[from] std::num::ParseIntError),

    #[error("SystemTimeError: {}", 0)]
    SystemTimeError(#[from] std::time::SystemTimeError),

    #[error("TokioThreadJoinError: {}", 0)]
    TokioThreadJoinError(#[from] JoinError)
}