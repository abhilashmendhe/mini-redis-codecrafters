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

    // let mut buffer = [0u8; 1024];
    println!("accepted new connection");
    
}


// use std::net::TcpListener;
// fn main() {
//     let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
//     for stream in listener.incoming() {
//         match stream {
//             Ok(_stream) => {
//                 println!("accepted new connection");
//             }
//             Err(e) => {
//                 println!("error: {}", e);
//             }
//         }
//     }
// }