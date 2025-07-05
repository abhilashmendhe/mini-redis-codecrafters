mod errors;
use tokio::net::{TcpListener, TcpStream};

use crate::errors::RedisErrors;

#[tokio::main]
async fn main() -> Result<(), RedisErrors> {

    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    
    loop {
        let (stream, sock_addr) = listener.accept().await?;     
    }
}

async fn handle_client(mut stream: TcpStream) {

    let mut buffer = [0u8; 1024];

    
}
