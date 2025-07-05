mod errors;
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream}};

use crate::errors::RedisErrors;

#[tokio::main]
async fn main() -> Result<(), RedisErrors> {

    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    
    loop {
        let (stream, _sock_addr) = listener.accept().await?;     
        
        tokio::spawn(async move {
            handle_client(stream).await
        });
    }
}

async fn handle_client(mut stream: TcpStream) -> Result<(), RedisErrors> {

    let mut buffer = [0u8; 1024];
    println!("accepted new connection");
    loop {
        match stream.read(&mut buffer).await {
            Ok(n) => {
                // println!("Read {} bytes!", n);
                let recv_msg = String::from_utf8_lossy(&buffer[..n]);
                // println!("Msg: {}", recv_msg);
            },
            Err(e) => {
                eprintln!("Read error: {}", e);
            }
        }

        stream.write_all(b"+PONG\r\n").await?;
    }
    // Ok(())
}

