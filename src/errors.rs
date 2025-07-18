use std::net::SocketAddr;

use thiserror::Error;
use tokio::{sync::mpsc::error::SendError, task::JoinError};

#[derive(Debug, Error)]
pub enum RedisErrors {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("ParseIntError: {}", 0)]
    ParseInt(#[from] std::num::ParseIntError),

    #[error("ParseFloatError: {}", 0)]
    ParseFloat(#[from] std::num::ParseFloatError),

    #[error("SystemTimeError: {}", 0)]
    SystemTimeError(#[from] std::time::SystemTimeError),

    #[error("TokioThreadJoinError: {}", 0)]
    TokioThreadJoinError(#[from] JoinError),

    #[error("SendError to channel: {}", 0)]
    SendErrorToChannel(#[from] SendError<(SocketAddr, Vec<u8>)>),

    #[error("HandshakeInvalidReply: {}", 0)]
    HandhshakeInvalidReply(String),
}