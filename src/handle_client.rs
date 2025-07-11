
use std::{collections::HashMap, sync::Arc, time::{SystemTime, UNIX_EPOCH}};

use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream, sync::Mutex};

use crate::{errors::RedisErrors, redis_key_value_struct::{get, insert, ValueStruct}, redis_server_info::ServerInfo, replication::replica_info::ReplicaInfo};
use crate::rdb_persistence::rdb_persist::RDB;

pub async fn handle_client(
    mut stream: TcpStream, 
    map: Arc<Mutex<HashMap<String, ValueStruct>>>,
    rdb: Arc<Mutex<RDB>>,
    server_info: Arc<Mutex<ServerInfo>>,
    replica_info: Arc<Mutex<ReplicaInfo>>
) -> Result<(), RedisErrors> {

    let mut buffer = [0u8; 1024];
    // println!("accepted new connection");
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
                    
                    // println!("cmds vec len: {}",cmds.len());
                    let mut value_struct = ValueStruct::new(
                        value.to_string(), 
                        None, 
                        None, 
                    );

                    if cmds.len() == 5 {
                        let px = cmds[4].parse::<u128>()?;
                        let now = SystemTime::now()
                                .duration_since(UNIX_EPOCH)?;
                        let now_ms = now.as_millis() + px as u128;
                        value_struct.set_px(Some(px));
                        value_struct.set_pxat(Some(now_ms));
                    }
                    // println!("{:?}", value_struct);
                    insert(key.to_string(), value_struct, map.clone()).await;
                    stream.write_all(b"+OK\r\n").await?;
                } else if cmds[0] == String::from("CONFIG") {
                    if cmds[1] == String::from("GET") {
                        if cmds[2] == String::from("dir") {
                            // println!("wants to read dir");
                            let rdb_gaurd = rdb.lock().await;
                            let dirpath = rdb_gaurd.dirpath().await;
                            // println!("{}", dirpath);
                            let stream_write_fmt = format!("*2\r\n$3\r\ndir\r\n${}\r\n{}\r\n",&dirpath.len(), dirpath);
                            stream.write_all(stream_write_fmt.as_bytes()).await?;
                        } else if cmds[2] == String::from("dbfilename") {
                            // println!("wants to read dbfilename");
                            let rdb_gaurd = rdb.lock().await;
                            let rdb_filepath = rdb_gaurd.rdb_filepath().await;
                            // println!("{}", rdb_filepath);
                            let stream_write_fmt = format!("*2\r\n$10\r\ndbfilename\r\n${}\r\n{}\r\n",&rdb_filepath.len(), rdb_filepath);
                            stream.write_all(stream_write_fmt.as_bytes()).await?;
                        }
                    } else if cmds[1] == String::from("SET") { // This is the CONFIG SET (not setting the kv)

                    }
                } else if cmds[0] == String::from("KEYS") {
                    if cmds[1] == String::from("*") {
                        // Get all keys
                        let map_gaurd = map.lock().await;
                        let mut count = 0;
                        let mut key_str = String::new();
                        for (key, _value_struct) in map_gaurd.iter() {
                            key_str.push_str(format!("${}\r\n{}\r\n",key.len(),key).as_str());
                            count += 1;
                        }
                        let mut full_str = format!("*{}\r\n",count);
                        full_str.push_str(&key_str);
                        // stream.write_all("+Everything\r\n".as_bytes()).await?;
                        stream.write_all(full_str.as_bytes()).await?;
                    }
                } else if cmds[0].eq("INFO") {

                    let mut s = String::new();
                    if cmds[1].eq("replication") {
                        // println!("Asking info about replication");
                        let replica_info_gaurd = replica_info.lock().await;
                        // println!("{}",replica_info_gaurd.to_string());
                        s.push_str(&replica_info_gaurd.to_string());
                        stream.write_all(s.as_bytes()).await?;
                        // stream.write_all("$33\r\nrole:master\r\nmaster_repl_offset:0\r\n".as_bytes()).await?;
                
                    } else if cmds[1].eq("server") {
                        let server_info_gaurd = server_info.lock().await;
                        // println!("{}",server_info_gaurd);
                        s.push_str(&server_info_gaurd.to_string());
                        stream.write_all(s.as_bytes()).await?;
                    }

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

