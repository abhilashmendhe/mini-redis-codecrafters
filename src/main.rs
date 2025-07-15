#![allow(unused)]

mod errors;
mod handle_client;
mod redis_key_value_struct;
mod rdb_persistence;
mod redis_server_info;
mod replication;
mod connection_handling;
mod master_or_slave;
mod parse_redis_bytes_file;
mod run_node;

use crate::errors::RedisErrors;

#[tokio::main]
async fn main() -> Result<(), RedisErrors> {

    let bytes = [42, 51, 13, 10, 36, 51, 13, 10, 83, 69, 84, 13, 10, 36, 51, 13, 10, 102, 111, 111, 13, 10, 36, 51, 13, 10, 49, 50, 51, 13, 10, 42, 51, 13, 10, 36, 51, 13, 10, 83, 69, 84, 13, 10, 36, 51, 13, 10, 98, 97, 114, 13, 10, 36, 51, 13, 10, 52, 53, 54, 13, 10, 42, 51, 13, 10, 36, 51, 13, 10, 83, 69, 84, 13, 10, 36, 51, 13, 10, 98, 97, 122, 13, 10, 36, 51, 13, 10, 55, 56, 57, 13, 10];
    // let bytes = b"+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0".to_vec();
    // let bytes = b"+Ok\r\n".to_vec();
    // let bytes = b"*2\r\n$3\r\nget\r\n$3\r\nfoo\r\n*3\r\n$3\r\nset\r\n$3\r\nfoo\r\n$4\r\nbar1\r\n*2\r\n$2\r\nhi\r\n$4\r\nwowo\r\n".to_vec();
    // ".to_vec();
    // let cmds = parse_redis_bytes_file::parse_recv_bytes(&bytes).await?;
    
    // println!("{:?}", cmds);
    run_node::run_redis_node().await?;
    // let multi_cmds = parse_redis_bytes_file::parse_multi_commands(&bytes).await;
    // println!("{:#?}",multi_cmds);
    Ok(())
}