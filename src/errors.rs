use thiserror::Error;

#[derive(Debug, Error)]
pub enum RedisErrors {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("ParseIntError: {}", 0)]
    ParseInt(#[from] std::num::ParseIntError)
}