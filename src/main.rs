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
            Ok(0) => {
                // println!("Client disconnected");
                return Ok(());
            },
            Ok(n) => {
                let _recv_msg = String::from_utf8_lossy(&buffer[..n]);
                // println!("{}", _recv_msg);
                // println!("{:?}",buffer);
                let cmds = parse_recv_bytes(&buffer[..n]).await?;
                // println!("{:?}", cmds[1]);
                if cmds[0] == String::from("ping") {
                    stream.write_all(b"+PONG\r\n").await?;
                }
                else if cmds[0] == String::from("echo") {
                    stream.write_all(format!("+{}\r\n",cmds[1]).as_bytes()).await?;
                }
            },
            Err(e) => {
                eprintln!("Read error: {}", e);
            }
        }
    }
    // Ok(())
}

async fn parse_recv_bytes(buffer: &[u8]) -> Result<Vec<String>, RedisErrors>{
    
    // 1. parse the first line that tells how many args were passed
    let mut ind = 0;
    while ind < buffer.len() {
        if buffer[ind] == 13 && buffer[ind + 1] == 10 {
            break;
        }
        ind+=1;
    }

    let _num_args = String::from_utf8_lossy(&buffer[1..ind]);
    
    ind += 2;
    let mut prev_ind = ind;
    
    let mut cmds = vec![];
    // 2. parse the remaining content
    while ind < buffer.len() - 1 {
        if buffer[ind] == 13 && buffer[ind + 1] == 10 {
            let arg_size = String::from_utf8_lossy(&buffer[prev_ind+1..ind])
                            .parse::<usize>()?;
            let cmd_arg = String::from_utf8_lossy(&buffer[(ind+2)..(ind+2+arg_size)]);
            cmds.push(cmd_arg.to_lowercase());
            // println!("val ->{}", String::from_utf8_lossy(&buffer[(ind+2)..(ind+2+arg_size)]));            
            ind = ind + 2 + arg_size ;
            prev_ind = ind + 2;
        }
        ind += 1;
    }
    // println!("{:?}", cmds);

    Ok(cmds)
}

