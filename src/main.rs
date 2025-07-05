mod errors;
mod handle_client;
mod redis_key_value_struct;

use std::sync::Arc;

use tokio::net::TcpListener;

use crate::{errors::RedisErrors, handle_client::handle_client, redis_key_value_struct::init_map};

#[tokio::main]
async fn main() -> Result<(), RedisErrors> {

    let map = init_map();

    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    
    loop {
        let (stream, _sock_addr) = listener.accept().await?; 
        let map = Arc::clone(&map);          
        tokio::spawn(async move {
            handle_client(stream, map).await
        });
    }
}
