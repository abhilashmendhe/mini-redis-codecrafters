use std::time::{SystemTime, UNIX_EPOCH};

use crate::errors::RedisErrors;

pub mod errors;
pub mod run_node;

mod handle_client;
mod rdb_persistence;
mod redis_server_info;
mod replication;
mod connection_handling;
mod master_or_slave;
mod parse_redis_bytes_file;
mod kv_lists;

// #[allow(unused)]
mod streams;
mod transactions;
mod basics;


pub async fn get_current_unix_time() -> Result<u128, RedisErrors> {
    let start = SystemTime::now();
    let since_the_epoch = start
        .duration_since(UNIX_EPOCH)?;
    Ok(since_the_epoch.as_millis())
}