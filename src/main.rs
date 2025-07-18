// #![allow(unused)]

mod errors;
mod handle_client;
mod rdb_persistence;
mod redis_server_info;
mod replication;
mod connection_handling;
mod master_or_slave;
mod parse_redis_bytes_file;
mod run_node;
mod kv_lists;
mod streams;
mod transactions;
mod basics;

use crate::errors::RedisErrors;

#[tokio::main]
async fn main() -> Result<(), RedisErrors> {

    run_node::run_redis_node().await?;
    Ok(())
}