
use std::{collections::{HashMap, HashSet, VecDeque}, net::SocketAddr, sync::Arc, time::{SystemTime, UNIX_EPOCH}};

use tokio::{io::{AsyncReadExt, AsyncWriteExt}, sync::{oneshot, Mutex, Notify}};

use crate::{connection_handling::{RecvChannelT, SharedConnectionHashMapT}, errors::RedisErrors, kv_lists::list_ops::{llen, lpop, lrange, push}, parse_redis_bytes_file::parse_recv_bytes, redis_key_value_struct::{get, insert, Value, ValueStruct}, redis_server_info::ServerInfo, replication::{propagate_cmds::{propagate_master_commands, propagate_replconf_getack}, replica_info::ReplicaInfo}};
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
                    } else if cmds[1].eq("ACK") {
                        // println!("Received ACK from replica!");
                        // println!("Store reply here...");
                        
                        // Increase reply count
                        {
                            *slave_ack_count.lock().await += 1;
                            if *slave_ack_count.lock().await >= slave_ack_set.lock().await.len() {
                                notify_replica.notify_waiters();
                            }
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
                        let mut vv = b"+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n".to_vec();
                        vv.extend_from_slice(header.as_bytes());
                        vv.extend_from_slice(&contents);
                        // ##############################
                        // vv.extend_from_slice(b"*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n");
                        // ##############################
                        client_tx.send((sock_addr, vv))?;
                    }
                    {
                        let mut slave_ack_set_gaurd = slave_ack_set.lock().await;
                        slave_ack_set_gaurd.insert(sock_addr.port());
                        println!("{:?}", slave_ack_set_gaurd);
                    }
                
                } else if cmds[0].eq("WAIT") {
                    
                    
                    let conn_slaves = {
                        replica_info.lock().await.connected_slaves()
                    };
                    let mut store_commands_len = 0;
                    if conn_slaves > 0 {
                        // Propagate commands
                        let vecdeque_len = {
                            let mut store_commands_gaurd = store_commands.lock().await;
                            let store_commands_len = store_commands_gaurd.len();
                            println!("store_commands_len: {}", store_commands_len);
                            while let Some(buffer) = store_commands_gaurd.pop_front() {
                                let connections1 = Arc::clone(&connections);
                                propagate_master_commands(
                                    sock_addr, 
                                    connections1, 
                                    buffer
                                ).await?;
                            }
                            store_commands_len
                        };
                        store_commands_len = vecdeque_len;
                        // Propagate "REPLCONF GETACK *" to replicas     
                        if vecdeque_len > 0 {              
                            let connections1 = Arc::clone(&connections);
                            propagate_replconf_getack(sock_addr, connections1).await?;
                            // Sleep for the desired amount of time...
                            let timeout_millis = cmds[2].parse::<u64>()?;
                            
                            let t1 = tokio::spawn(
                            tokio::time::sleep(std::time::Duration::from_millis(timeout_millis))
                            );
                            let t2 = tokio::spawn({
                                let count = slave_ack_count.clone();
                                let notify_replica = notify_replica.clone();
                                async move {
                                    loop {
                                        {
                                            let val = *count.lock().await;
                                            if val > 0 {
                                                return val;
                                            }
                                        }
                                        notify_replica.notified().await;
                                    }
                                }
                            });
                            tokio::select! {
                                _ = t1 => {
                                    println!("Timeout occurred first!");
                                }
                                res = t2 => {
                                    println!("Ack count detected: {}", res.unwrap());
                                }
                            }
                        }
                    };
                        // store_commands_len = store_commands_len;
                    println!("\nDone sleeping for WAIT!");

                    // // Propagate "REPLCONF GETACK *" to replicas                   
                    // let connections1 = Arc::clone(&connections);
                    // propagate_replconf_getack(sock_addr, connections1).await?;
                    let mut reply_ack_slave_count = 0;
                    if store_commands_len > 0 {
                        let replyackslavecount = {
                            while *slave_ack_count.lock().await <= 0 {}
                            println!("what");
                            *slave_ack_count.lock().await
                        };
                        reply_ack_slave_count = replyackslavecount;
                    }
                    println!("Reply slave count: {}",reply_ack_slave_count);
                    if let Some((client_tx, _flag)) = connections.lock().await.get(&sock_addr.port()) {
                        let connected_slaves = {
                            if *command_init_for_replica.lock().await {
                                reply_ack_slave_count
                            } else {
                                conn_slaves as usize
                            }
                        };
                        
                        let form = format!(":{}\r\n", connected_slaves);
                        client_tx.send((sock_addr,form.as_bytes().to_vec()))?;
                    }
                    {   *slave_ack_count.lock().await = 0;   }
                } else if cmds[0].eq("TYPE") {
                    let v_type = {
                        let key = &cmds[1];
                        if let Some(v) = kv_map.lock().await.get(key) {
                            let value = v.value();
                            let v_type = match value {
                                Value::STRING(_) => "+string\r\n",
                                Value::NUMBER(_) => "+number\r\n",
                                Value::LIST(_) => "+list\r\n",
                                Value::STREAM(_) => "+stream\r\n",
                            };
                            v_type
                        } else {
                            "+none\r\n"
                        }
                    };

                    if let Some((client_tx, _flag)) = connections.lock().await.get(&sock_addr.port()) {
                        let form = format!("{}",v_type);
                        client_tx.send((sock_addr, form.as_bytes().to_vec()))?;
                    }
                     
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

                    println!("In BLPOP");
                    let listkey = cmds[1].to_string();
                    let _seconds = cmds[2].parse::<u64>()?;
                    
                    let mut form = String::from("");
                    
                    let (tx, rx) = tokio::sync::oneshot::channel();
                    let blpop_clients_queue2 = Arc::clone(&blpop_clients_queue1);
                    
                    {
                        let mut kv_map_gaurd = kv_map.lock().await;
                        if let Some(value_struct) = kv_map_gaurd.get_mut(&listkey) {
                            // value_struct
                            match value_struct.mut_value() {
                                Value::STRING(_) => {},
                                Value::NUMBER(_) => {},
                                Value::LIST(items) => {
                                    // println!("Items: {:?}", items);
                                    if let Some(item) = items.pop_front() {
                                        // let _ = waiter.send(item);
                                        form.push_str("*2\r\n");
                                        form.push('$');
                                        form.push_str(&listkey.len().to_string());
                                        form.push_str("\r\n");
                                        form.push_str(&listkey);
                                        form.push_str("\r\n");
                                        form.push('$');
                                        form.push_str(&item.len().to_string());
                                        form.push_str("\r\n");
                                        form.push_str(&item);
                                        form.push_str("\r\n");
                                        blpop_clients_queue2.lock().await.push_back((sock_addr, tx));
                                    } else {
                                        blpop_clients_queue2.lock().await.push_back((sock_addr, tx));
                                    }
                                },
                                Value::STREAM(_) => {},
                            }
                        } else {
                            blpop_clients_queue2.lock().await.push_back((sock_addr, tx));
                        }
                    }
                    {
                        println!("Before wait: {:?}",blpop_clients_queue2.lock().await);
                    }
                    println!("Before wait!");
                    if form.len() <=0 {
                         
                        println!("Wating for the poppped elem"); 
                        match rx.await {
                            Ok((item, sock_addr)) => {
                                
                                form.push_str("*2\r\n");
                                form.push('$');
                                form.push_str(&listkey.len().to_string());
                                form.push_str("\r\n");
                                form.push_str(&listkey);
                                form.push_str("\r\n");
                                form.push('$');
                                form.push_str(&item.len().to_string());
                                form.push_str("\r\n");
                                form.push_str(&item);
                                form.push_str("\r\n");
                                if let Some((client_tx, _flag)) = connections.lock().await.get(&sock_addr.port()) {
                                // println!("{}", form);
                                    client_tx.send((sock_addr, form.as_bytes().to_vec()))?;
                                }
                                form.clear();
                                form.push_str("$-1\r\n");
                            },
                            Err(e) => {
                                println!("recverr: {}",  e);
                                form.push_str("$-1\r\n");
                            }, // Sender dropped
                        }
                    } else 
                    {
                        println!("After wait: {:?}",blpop_clients_queue2.lock().await);
                    }
                    println!("After wait!");
                    {
                        let mut blpop_clients_queue2_gaurd = blpop_clients_queue2.lock().await;
                        if let Some((sock_addr, _)) = blpop_clients_queue2_gaurd.pop_front() {
                            if let Some((client_tx, _flag)) = connections.lock().await.get(&sock_addr.port()) {
                                println!("{}", form);
                                client_tx.send((sock_addr, form.as_bytes().to_vec()))?;
                            }
                        }
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