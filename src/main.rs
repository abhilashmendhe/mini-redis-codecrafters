mod errors;
mod handle_client;
mod redis_key_value_struct;
mod rdb_persist;

use std::{env::args, sync::Arc};

use tokio::net::TcpListener;

use crate::{errors::RedisErrors, handle_client::handle_client, rdb_persist::init_rdb, redis_key_value_struct::{clean_map, init_map}};

#[tokio::main]
async fn main() -> Result<(), RedisErrors> {

    // 1. Read args for RDB persistence and check if folder and file(.rdb) exists
    // if does not then create it
    let args = args().collect::<Vec<String>>();
    // println!("{:?}", args);

    let rdb = init_rdb(args)?;

    let map = init_map();
    let map2 = Arc::clone(&map);
    tokio::spawn(async move {
        let _ = clean_map(map2).await;
    });
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    
    loop {
        let (stream, _sock_addr) = listener.accept().await?; 
        let map1 = Arc::clone(&map);    
        let rdb1 = Arc::clone(&rdb);
        tokio::spawn(async move {
            handle_client(stream, map1, rdb1).await
        });
        
    }

    // Ok(())
    
}
