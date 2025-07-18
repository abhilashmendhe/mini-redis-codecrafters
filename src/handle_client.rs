
use std::{collections::{HashMap, HashSet, VecDeque}, net::SocketAddr, sync::Arc, time::{SystemTime, UNIX_EPOCH}};

use tokio::{io::{AsyncReadExt, AsyncWriteExt}, sync::{oneshot, Mutex, Notify}};

use crate::{connection_handling::{RecvChannelT, SharedConnectionHashMapT}, errors::RedisErrors, kv_lists::list_ops::{blpop, llen, lpop, lrange, push}, parse_redis_bytes_file::parse_recv_bytes, redis_key_value_struct::{get, insert, Value, ValueStruct}, redis_server_info::ServerInfo, replication::{propagate_cmds::propagate_master_commands, replica_info::ReplicaInfo, replication_ops::{psync_ops, replconf_ops, wait_repl}}, streams::stream_ops::type_ops, transactions::transac_ops::incr_ops};
use crate::rdb_persistence::rdb_persist::RDB;

pub async fn read_handler(
    mut reader: tokio::net::tcp::OwnedReadHalf,
    sock_addr: SocketAddr,
    connections: SharedConnectionHashMapT,
    kv_map: Arc<Mutex<HashMap<String, ValueStruct>>>,
    rdb: Arc<Mutex<RDB>>,
    server_info: Arc<Mutex<ServerInfo>>,
    replica_info: Arc<Mutex<ReplicaInfo>>,
    slave_ack_set: Arc<Mutex<HashSet<u16>>>,
    slave_ack_count: Arc<Mutex<usize>>,
    notify_replica: Arc<Notify>,        
    blpop_clients_queue: Arc<Mutex<VecDeque<(SocketAddr,oneshot::Sender<(String,SocketAddr)>)>>>,
    command_init_for_replica: Arc<Mutex<bool>>,
    store_commands: Arc<Mutex<VecDeque<Vec<u8>>>>
) -> Result<(), RedisErrors> {

    
    let mut role = String::new();
    {
        let replica_info_gaurd = replica_info.lock().await;
        role.push_str(&replica_info_gaurd.role());
    }
    println!("I am {}!. Accepted new connection from: {}", role,sock_addr);
    let mut buffer = [0u8; 1024];
    let connections1 = Arc::clone(&connections);
    // blpop_clients_queue: Arc<Mutex<VecDeque<(u16,oneshot::Sender<String>)>>>
    let blpop_clients_queue1 = Arc::clone(&blpop_clients_queue);
    loop {
        match reader.read(&mut buffer).await {
            Ok(0) => {
                println!("0 bytes received");
                let mut clients_ports = vec![];
                let connections2 = Arc::clone(&connections1);
                {
                    let conn_gaurd = connections2.lock().await;
                    for (k, (_tx, _flag)) in conn_gaurd.iter() {
                        // println!("key: {}, flag: {}", k, _flag);
                        if !*_flag {
                            clients_ports.push(*k);
                        }
                    }
                }
                for port in clients_ports {
                    println!("Client {}:{}, disconnected....", sock_addr.ip(), sock_addr.port());
                    connections2.lock().await.remove(&port);
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
                        let value = {
                            match vs.value() {
                                crate::redis_key_value_struct::Value::STRING(s) => s,
                                crate::redis_key_value_struct::Value::NUMBER(num) => num.to_string(),
                                crate::redis_key_value_struct::Value::LIST(_) => "".to_string(),
                                crate::redis_key_value_struct::Value::STREAM(_) => "".to_string(),
                            }
                        };
                        let value_len = value.len();

                        let stream_write_fmt = format!("${}\r\n{}\r\n", value_len, value);
                        println!("stream_write_fmt:{}",stream_write_fmt);
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
                    let value = match value.parse::<i64>() {
                        Ok(num) => Value::NUMBER(num),
                        Err(_) => Value::STRING(value.to_string()),
                    };
                    let mut value_struct = ValueStruct::new(
                        // value.to_string(), 
                        value,
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
                    // {
                    //     let mut slave_ack_set_gaurd = slave_ack_set.lock().await;
                    //     slave_ack_set_gaurd.clear();
                    // }
                    {
                        *command_init_for_replica.lock().await = true;
                    }
                    let slave_count = {
                        // println!("Replicas Set: {:?}",slave_ack_set.lock().await);
                        slave_ack_set.lock().await.len()
                    };
                    // println!("Slave count: {}", slave_count);
                    if slave_count > 0 {
                        // Don't propagate the commands, store commands(Vec<u8>) in a vector or queue..
                        store_commands.lock().await.push_back(buffer[..n].to_vec());
                        // println!("{:?}", store_commands.lock().await);
                        let mut store_commands_gaurd = store_commands.lock().await;
                        let store_commands_len = store_commands_gaurd.len();
                        // println!("store_commands_len: {}", store_commands_len);
                        if store_commands_len >= 3 {
                            while let Some(buffer) = store_commands_gaurd.pop_front() {
                                let connections1 = Arc::clone(&connections);
                                propagate_master_commands(
                                    sock_addr, 
                                    connections1, 
                                    buffer
                                ).await?;
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
                    replconf_ops(
                        &cmds, 
                        sock_addr, 
                        Arc::clone(&connections), 
                        Arc::clone(&replica_info), 
                        Arc::clone(&slave_ack_count), 
                        Arc::clone(&notify_replica), 
                        Arc::clone(&slave_ack_set)
                    ).await?;

                } else if cmds[0].eq("PSYNC") {
                    psync_ops(
                        sock_addr, 
                        Arc::clone(&connections), 
                        Arc::clone(&slave_ack_set)
                    ).await?;
                
                } else if cmds[0].eq("WAIT") {
                    
                    wait_repl(
                        &cmds, 
                        sock_addr, 
                        Arc::clone(&connections), 
                        Arc::clone(&replica_info), 
                        Arc::clone(&slave_ack_count), 
                        Arc::clone(&notify_replica), 
                        Arc::clone(&command_init_for_replica), 
                        Arc::clone(&store_commands)
                    ).await?;

                } else if cmds[0].eq("RPUSH") || cmds[0].eq("LPUSH") {
                    push(&cmds, 
                        sock_addr, 
                        Arc::clone(&kv_map), 
                        Arc::clone(&connections1),
                        Arc::clone(&blpop_clients_queue1)
                    ).await?;
                } 
                else if cmds[0].eq("LRANGE") {

                    lrange(&cmds, 
                        sock_addr, 
                        Arc::clone(&kv_map), 
                        Arc::clone(&connections1)
                    ).await?;

                } else if cmds[0].eq("LLEN") {

                    llen(&cmds, 
                        sock_addr, 
                        Arc::clone(&kv_map), 
                        Arc::clone(&connections1)
                    ).await?;
                } else if cmds[0].eq("LPOP") {

                    lpop(&cmds, 
                        sock_addr, 
                        Arc::clone(&kv_map), 
                        Arc::clone(&connections1)
                    ).await?;
                } else if cmds[0].eq("BLPOP") { 
                    blpop(&cmds, 
                        sock_addr, 
                        Arc::clone(&kv_map), 
                        Arc::clone(&connections1),
                        Arc::clone(&blpop_clients_queue1)
                    ).await?;
                } else if cmds[0].eq("TYPE") {
                    type_ops(&cmds, 
                        sock_addr, 
                        Arc::clone(&connections), 
                        Arc::clone(&kv_map)).await?;   
                } else if cmds[0].eq("XADD") {
                    
                } else if cmds[0].eq("INCR") {
                    incr_ops(
                        &cmds, 
                        sock_addr, 
                        Arc::clone(&kv_map), 
                        Arc::clone(&connections)).await?;
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