
use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::{SystemTime, UNIX_EPOCH}};

use tokio::{io::{AsyncReadExt, AsyncWriteExt}, sync::Mutex};

use crate::{connection_handling::{RecvChannelT, SharedConnectionHashMapT}, errors::RedisErrors, redis_key_value_struct::{get, insert, ValueStruct}, redis_server_info::ServerInfo, replication::replica_info::ReplicaInfo};
use crate::rdb_persistence::rdb_persist::RDB;

pub async fn read_handler(
    mut reader: tokio::net::tcp::OwnedReadHalf,
    sock_addr: SocketAddr,
    connections: SharedConnectionHashMapT,
    kv_map: Arc<Mutex<HashMap<String, ValueStruct>>>,
    rdb: Arc<Mutex<RDB>>,
    server_info: Arc<Mutex<ServerInfo>>,
    replica_info: Arc<Mutex<ReplicaInfo>>
) -> Result<(), RedisErrors> {

    println!("Accepted new connection from: {}",sock_addr);
    let mut role = String::new();
    {
        let replica_info_gaurd = replica_info.lock().await;
        role.push_str(&replica_info_gaurd.role());
    }
    let mut buffer = [0u8; 1024];
    loop {
        match reader.read(&mut buffer).await {
            Ok(0) => {
                println!("0 bytes received");
                let mut clients_ports = vec![];
                {
                    let conn_gaurd = connections.lock().await;
                    for (k, (_tx, _flag)) in conn_gaurd.iter() {
                        // println!("key: {}, flag: {}", k, _flag);
                        if !*_flag {
                            clients_ports.push(*k);
                        }
                    }
                }
                for port in clients_ports {
                    println!("Client {}:{}, disconnected....", sock_addr.ip(), sock_addr.port());
                    connections.lock().await.remove(&port);
                }
                println!();
                return Ok(());
            },
            Ok(n) => {

                let _recv_msg = String::from_utf8_lossy(&buffer[..n]);
                let cmds = parse_recv_bytes(&buffer[..n]).await?;
                println!("cmds: {:?}",cmds);
                if cmds[0].eq("PING") {
                    if let Some((client_tx, _flag)) = connections.lock().await.get(&sock_addr.port()) {
                        client_tx.send((sock_addr, b"+PONG\r\n".to_vec()))?;
                    }
                } else if cmds[0].eq("ECHO") {
                    if let Some((client_tx, _flag)) = connections.lock().await.get(&sock_addr.port()) {
                        client_tx.send((sock_addr, format!("+{}\r\n",cmds[1]).as_bytes().to_vec()))?;
                    }
                } else if cmds[0].eq("BYE") {
                    println!("Bye received from replica: {}",sock_addr);
                    connections.lock().await.remove(&sock_addr.port());
                } else if cmds[0] == String::from("GET") {

                    let key = cmds[1].as_str();
                    let value_struct = get(key.to_string(), kv_map.clone()).await;

                    if let Some(vs) = value_struct {
                        let value = vs.value();
                        let value_len = value.len();
                        let stream_write_fmt = format!("${}\r\n{}\r\n", value_len, value);
                        if let Some((client_tx, _flag)) = connections.lock().await.get(&sock_addr.port()) {
                            client_tx.send((sock_addr, stream_write_fmt.as_bytes().to_vec()))?;
                        }
                    } else {
                        if let Some((client_tx, _flag)) = connections.lock().await.get(&sock_addr.port()) {
                            client_tx.send((sock_addr, b"$-1\r\n".to_vec()))?;
                        }
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
                    insert(key.to_string(), value_struct, kv_map.clone()).await;
                    if let Some((client_tx, _flag)) = connections.lock().await.get(&sock_addr.port()) {
                        client_tx.send((sock_addr, b"+OK\r\n".to_vec()))?;
                    }

                    {
                        let mut conn_gaurd = connections.lock().await;
                        for (_k, (_tx, _flag)) in conn_gaurd.iter_mut() {
                            
                            if *_flag {
                                println!("key: {}, flag: {}", _k, _flag);
                                _tx.send((sock_addr, buffer[..n].to_vec()))?;
                            } 
                        }
                    }
        
                } else if cmds[0] == String::from("CONFIG") {
                    if cmds[1] == String::from("GET") {
                        if cmds[2] == String::from("dir") {
                            // println!("wants to read dir");
                            let rdb_gaurd = rdb.lock().await;
                            let dirpath = rdb_gaurd.dirpath().await;
                            // println!("{}", dirpath);
                            let stream_write_fmt = format!("*2\r\n$3\r\ndir\r\n${}\r\n{}\r\n",&dirpath.len(), dirpath);
                            if let Some((client_tx, _flag)) = connections.lock().await.get(&sock_addr.port()) {
                                client_tx.send((sock_addr, stream_write_fmt.as_bytes().to_vec()))?;
                            }
                        } else if cmds[2] == String::from("dbfilename") {
                            // println!("wants to read dbfilename");
                            let rdb_gaurd = rdb.lock().await;
                            let rdb_filepath = rdb_gaurd.rdb_filepath().await;
                            // println!("{}", rdb_filepath);
                            let stream_write_fmt = format!("*2\r\n$10\r\ndbfilename\r\n${}\r\n{}\r\n",&rdb_filepath.len(), rdb_filepath);
                            if let Some((client_tx, _flag)) = connections.lock().await.get(&sock_addr.port()) {
                                client_tx.send((sock_addr, stream_write_fmt.as_bytes().to_vec()))?;
                            }
                        }
                    } else if cmds[1] == String::from("SET") { // This is the CONFIG SET (not setting the kv)

                    }
                } else if cmds[0] == String::from("KEYS") {
                    if cmds[1] == String::from("*") {
                        // Get all keys
                        let map_gaurd = kv_map.lock().await;
                        let mut count = 0;
                        let mut key_str = String::new();
                        for (key, _value_struct) in map_gaurd.iter() {
                            key_str.push_str(format!("${}\r\n{}\r\n",key.len(),key).as_str());
                            count += 1;
                        }
                        let mut full_str = format!("*{}\r\n",count);
                        full_str.push_str(&key_str);
                        // stream.write_all("+Everything\r\n".as_bytes()).await?;
                        if let Some((client_tx, _flag)) = connections.lock().await.get(&sock_addr.port()) {
                            client_tx.send((sock_addr, full_str.as_bytes().to_vec()))?;
                        }
                    }
                } else if cmds[0].eq("INFO") {

                    let mut s = String::new();
                    if cmds[1].eq("replication") {
                        // println!("Asking info about replication");
                        let replica_info_gaurd = replica_info.lock().await;
                        // println!("{}",replica_info_gaurd.to_string());
                        s.push_str(&replica_info_gaurd.to_string());
                        
                        if let Some((client_tx, _flag)) = connections.lock().await.get(&sock_addr.port()) {
                            client_tx.send((sock_addr, s.as_bytes().to_vec()))?;
                        }
                        // stream.write_all("$33\r\nrole:master\r\nmaster_repl_offset:0\r\n".as_bytes()).await?;
                
                    } else if cmds[1].eq("server") {
                        let server_info_gaurd = server_info.lock().await;
                        // println!("{}",server_info_gaurd);
                        s.push_str(&server_info_gaurd.to_string());
                        
                        if let Some((client_tx, _flag)) = connections.lock().await.get(&sock_addr.port()) {
                            client_tx.send((sock_addr, s.as_bytes().to_vec()))?;
                        }
                    }

                } else if cmds[0].eq("REPLCONF") {
                    if cmds[1].eq("listening-port") {
                        println!("Received 2nd handshake for Replconf!");
                        let _port = cmds[2].parse::<u16>()?;
                        let _ip = sock_addr.ip().to_string();
                        
                        replica_info.lock().await.add_slave(_ip, _port);
                        if let Some((client_tx, _flag)) = connections.lock().await.get_mut(&sock_addr.port()) {
                            *_flag = true;
                            client_tx.send((sock_addr, b"+OK\r\n".to_vec()))?;
                        }
                    } else if cmds[1].eq("capa") {
                        println!("Received 3nd handshake for Replconf psync!");
                        if let Some((client_tx, _flag)) = connections.lock().await.get(&sock_addr.port()) {
                            client_tx.send((sock_addr, b"+OK\r\n".to_vec()))?;
                        }
                    }

                } else if cmds[0].eq("PSYNC") {
                    println!("Receieved final handshake for PSYNC ?");
                    
                    // if let Some(client_tx) = connections.lock().await.get(&sock_addr.port()) {
                    //     client_tx.send((sock_addr, b"+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n".to_vec()))?;
                    // }
                    let hex_str = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
                    let contents = (0..hex_str.len())
                        .step_by(2)
                        .map(|i| u8::from_str_radix(&hex_str[i..i + 2], 16).unwrap())
                        .collect::<Vec<_>>();

                    let header = format!("${}\r\n", contents.len());
                    if let Some((client_tx, _flag)) = connections.lock().await.get(&sock_addr.port()) {
                        client_tx.send((sock_addr, b"+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n".to_vec()))?;
                        client_tx.send((sock_addr, header.as_bytes().to_vec()))?;
                        client_tx.send((sock_addr, contents))?;
                    }
                
                }

            },
            Err(e) => {
                eprintln!("Read error: {}", e);
                connections.lock().await.remove(&sock_addr.port());
                return Err(RedisErrors::Io(e));
            }
        }
    }
}

pub async fn write_handler(
    mut writer: tokio::net::tcp::OwnedWriteHalf,
    mut rx: RecvChannelT,
    connections: SharedConnectionHashMapT
) {
    // println!("Write handler!");
    while let Some(recv_bytes) = rx.recv().await {
        let (_sock_addr, reply_bytes) = recv_bytes; 
        println!("In write_handler - {}",_sock_addr);
        match writer.write_all(&reply_bytes).await {
            Ok(_) => {
                println!("Writing data back to client: {}", _sock_addr);
            },
            Err(e) => {
                println!("Error in write_handler: {e}");
                if e.kind() == std::io::ErrorKind::BrokenPipe {
                    connections.lock().await.remove(&_sock_addr.port());
                }
                break;
            },
        }
    }
}

pub async fn parse_recv_bytes(buffer: &[u8]) -> Result<Vec<String>, RedisErrors>{
    
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

