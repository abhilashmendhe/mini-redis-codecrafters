
use std::{collections::HashMap, sync::Arc};

use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream, sync::Mutex};

use crate::{errors::RedisErrors, redis_key_value_struct::{get, insert, ValueStruct}};

pub async fn handle_client(
    mut stream: TcpStream, 
    map: Arc<Mutex<HashMap<String, ValueStruct>>>
) -> Result<(), RedisErrors> {

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
                
                if cmds[0].to_lowercase() == String::from("ping") {

                    stream.write_all(b"+PONG\r\n").await?;
                } else if cmds[0].to_lowercase() == String::from("echo") {

                    stream.write_all(format!("+{}\r\n",cmds[1]).as_bytes()).await?;
                } else if cmds[0] == String::from("GET") {

                    let key = cmds[1].as_str();
                    let value_struct = get(key.to_string(), map.clone()).await;

                    if let Some(vs) = value_struct {
                        let value = vs.value();
                        let value_len = value.len();
                        let stream_write_fmt = format!("${}\r\n{}\r\n", value_len, value);
                        stream.write_all(stream_write_fmt.as_bytes()).await?;
                    } else {
                        stream.write_all(b"$-1\r\n").await?;
                    }
                } else if cmds[0] == String::from("SET") {
                    
                    let key = cmds[1].as_str();
                    let value = cmds[2].as_str();
                    let value_struct = ValueStruct::new(
                        value.to_string(), 
                        0, 
                        0, 
                        0, 
                        0
                    );
                    insert(key.to_string(), value_struct, map.clone()).await;
                    stream.write_all(b"+OK\r\n").await?;
                }
            },
            Err(e) => {
                eprintln!("Read error: {}", e);
            }
        }
    }
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
            cmds.push(cmd_arg.to_string());
            // println!("val ->{}", String::from_utf8_lossy(&buffer[(ind+2)..(ind+2+arg_size)]));            
            ind = ind + 2 + arg_size ;
            prev_ind = ind + 2;
        }
        ind += 1;
    }
    Ok(cmds)
}

