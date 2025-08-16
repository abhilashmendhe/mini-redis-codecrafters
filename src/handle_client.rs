
use std::{collections::{HashSet, VecDeque}, net::SocketAddr, sync::Arc};

use tokio::{io::{AsyncReadExt, AsyncWriteExt}, sync::{oneshot, Mutex, Notify}};

use crate::{basics::{all_types::{SharedMapT, SharedRDBStructT}, basic_ops::{get, get_pattern_match_keys, set}}, connection_handling::{RecvChannelT, SharedConnectionHashMapT}, errors::RedisErrors, kv_lists::list_ops::{blpop, llen, lpop, lrange, push}, parse_redis_bytes_file::try_parse_resp, pub_sub::{pub_sub_ds::{subscribe, unsubscribe, SharedPubSubType}, publish_cmd::publish}, redis_server_info::ServerInfo, replication::{propagate_cmds::propagate_master_commands, replica_info::ReplicaInfo, replication_ops::{psync_ops, replconf_ops, wait_repl}}, sorted_sets::zadd_ops::zadd, streams::stream_ops::{type_ops, xadd, xrange, xread}, transactions::{append_commands::{append_transaction_to_commands, get_command_trans_len}, transac_ops::{discard_multi, exec_multi, incr_ops, multi}}};

use crate::transactions::commands::CommandTransactions;

pub async fn read_handler(
    mut reader: tokio::net::tcp::OwnedReadHalf,
    sock_addr: SocketAddr,
    connections: SharedConnectionHashMapT,
    kv_map: SharedMapT,
    rdb: SharedRDBStructT,
    server_info: Arc<Mutex<ServerInfo>>,
    replica_info: Arc<Mutex<ReplicaInfo>>,
    slave_ack_set: Arc<Mutex<HashSet<u16>>>,
    slave_ack_count: Arc<Mutex<usize>>,
    notify_replica: Arc<Notify>,        
    blpop_clients_queue: Arc<Mutex<VecDeque<(SocketAddr,oneshot::Sender<(String,SocketAddr)>)>>>,
    command_init_for_replica: Arc<Mutex<bool>>,
    store_commands: Arc<Mutex<VecDeque<Vec<u8>>>>,
    xread_clients_queue: Arc<Mutex<VecDeque<SocketAddr>>>,
    pub_sub_map: SharedPubSubType
    // multi_command_map: Arc<Mutex<HashMap<u16, String>>>
) -> Result<(), RedisErrors> {

    
    let mut role = String::new();
    {
        let replica_info_gaurd = replica_info.lock().await;
        role.push_str(&replica_info_gaurd.role());
    }
    println!("I am {}!. Accepted new connection from: {}", role,sock_addr);
    // let mut buffer = [0u8; 1024];

    let mut buffer = Vec::new();
    let mut read_buf = [0u8; 1024];
    let connections1 = Arc::clone(&connections);
    let blpop_clients_queue1 = Arc::clone(&blpop_clients_queue);
    // let xread_clients_queue1 = Arc::clone(&xread_clients_queue);
    loop {  
        match reader.read(&mut read_buf).await {
            Ok(0) => {
                println!("0 bytes received");
                let mut clients_ports = vec![];
                let mut client_ps_ch = vec![];
                let connections2 = Arc::clone(&connections1);
                {
                    let mut conn_gaurd = connections2.lock().await;
                    for (k, conn_struct) in conn_gaurd.iter_mut() {
                        // println!("key: {}, flag: {}", k, _flag);
                        let _flag = conn_struct.flag;
                        if !_flag {
                            clients_ports.push(*k);
                        }
                        // Remove all the channels subs from the client connection
                        if conn_struct.is_pub_sub {
                            let chs = conn_struct.mut_get_pub_sub_ch();
                            while let Some(ch) =  chs.pop_first() {
                                client_ps_ch.push(ch);
                            }
                        }
                        conn_struct.is_pub_sub = false;
                    }
                }
                for ch in client_ps_ch {
                    {
                        let mut ps_map = pub_sub_map.lock().await;
                        let hs_empty = if let Some(ps_hs) = ps_map.get_mut(&ch) {
                            ps_hs.remove(&sock_addr.port());
                            let hs_empty = if ps_hs.len() == 0 {
                                true
                            } else {
                                false
                            };
                            hs_empty
                        } else { false };
                        if hs_empty {
                            ps_map.remove(&ch);
                        }
                    }
                }
                // for port in clients_ports {
                //     println!("Client {}:{}, disconnected....", sock_addr.ip(), sock_addr.port());
                //     connections2.lock().await.remove(&port);
                // }
                println!();
                return Ok(());
            },
            Ok(n) => {

                buffer.extend_from_slice(&read_buf[..n]);
                
                let _recv_msg = String::from_utf8_lossy(&read_buf[..n]);

                // println!("({}) --->  Recv: {}",sock_addr,_recv_msg);
                let ps_flag = {
                    let conn_gaurd = connections1.lock().await;
                    if let Some(cs) = conn_gaurd.get(&sock_addr.port()) {
                        cs.is_pub_sub
                    } else {
                        false
                    }
                };
                // println!("Is pub_sub on for {}: {}",sock_addr.port(),ps_flag);
                while let Some((cmds, _end)) = try_parse_resp(&buffer).await {
                    buffer.clear();
                    // println!("{:?}",cmds);
                    // send_to_client(&connections, &sock_addr, b"+OK\r\n").await?;
                    // tokio::time::sleep(Duration::from_secs(4)).await;
                    
                    // let cmds = parse_recv_bytes(&buffer[..n]).await?;
                    
                    if cmds[0].eq("PING") || cmds[0].eq("ping") {
                        let form = if !ps_flag {
                            format!("+PONG\r\n")
                        } else {
                            format!("*2\r\n$4\r\npong\r\n$0\r\n\r\n")
                        };
                        send_to_client(&connections, &sock_addr, form.as_bytes()).await?;
                    } else if cmds[0].eq("ECHO") {

                        let form = if !ps_flag {
                            format!("+{}\r\n",cmds[1])                            
                        } else {
                            format!("-ERR Can't execute 'echo': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context\r\n")
                        };
                        send_to_client(&connections, &sock_addr, form.as_bytes()).await?;
                    } else if cmds[0].eq("BYE") {

                        println!("Bye received from replica: {}",sock_addr);
                        connections.lock().await.remove(&sock_addr.port());
                    } else if cmds[0] == String::from("GET") {
                        let form = if !ps_flag {
                            let key = cmds[1].to_string();
                            let command_transc_len = get_command_trans_len(sock_addr, Arc::clone(&connections)).await;
                            let form = if command_transc_len > 0 {
                                let form = append_transaction_to_commands(CommandTransactions::Get { key }, 
                                    sock_addr, 
                                    Arc::clone(&connections)).await;
                                form
                            } else {
                                let form = get(key, kv_map.clone()).await;
                                form
                            };
                            form
                        } else {
                            format!("-ERR Can't execute 'get': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context\r\n")
                        };
                        send_to_client(&connections, &sock_addr, form.as_bytes()).await?;
                    } else if cmds[0] == String::from("SET") {
                        let form = if !ps_flag {
                        let key = cmds[1].to_string();
                        let value = cmds[2].to_string();
                        let px = if cmds.len() == 5 {
                            Some(cmds[4].to_string())
                        } else {
                            None
                        };
                        let command_transc_len = get_command_trans_len(sock_addr, Arc::clone(&connections)).await;
                        let form = if command_transc_len > 0 {
                            let form = append_transaction_to_commands(CommandTransactions::Set { key, value, px }, 
                                sock_addr, 
                                Arc::clone(&connections)).await;
                            form
                        } else {
                            let form = set(key, value, px, Arc::clone(&kv_map)).await?;
                            form
                        };
                        form
                        } else {
                            format!("-ERR Can't execute 'set': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context\r\n")
                        };
                        send_to_client(&connections, &sock_addr, form.as_bytes()).await?;

                        // Below code to propagate commands to the replicas.
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
                                let form = format!("*2\r\n$3\r\ndir\r\n${}\r\n{}\r\n",&dirpath.len(), dirpath);
                                send_to_client(&connections, &sock_addr, form.as_bytes()).await?;

                            } else if cmds[2] == String::from("dbfilename") {
                                // println!("wants to read dbfilename");
                                let rdb_gaurd = rdb.lock().await;
                                let rdb_filepath = rdb_gaurd.rdb_filepath().await;
                                // println!("{}", rdb_filepath);
                                let form = format!("*2\r\n$10\r\ndbfilename\r\n${}\r\n{}\r\n",&rdb_filepath.len(), rdb_filepath);
                                send_to_client(&connections, &sock_addr, form.as_bytes()).await?;
                            }
                        } else if cmds[1] == String::from("SET") { // This is the CONFIG SET (not setting the kv)

                        }
                    } else if cmds[0] == String::from("KEYS") {

                        let pattern = cmds[1].to_string();
                        let form = get_pattern_match_keys(pattern, 
                                        Arc::clone(&kv_map)).await;
                        send_to_client(&connections, &sock_addr, form.as_bytes()).await?;
                        
                    } else if cmds[0].eq("INFO") {

                        let mut form = String::new();
                        if cmds[1].eq("replication") {

                            let replica_info_gaurd = replica_info.lock().await;
                            form.push_str(&replica_info_gaurd.to_string());
                            send_to_client(&connections, &sock_addr, form.as_bytes()).await?;

                        } else if cmds[1].eq("server") {

                            let server_info_gaurd = server_info.lock().await;
                            form.push_str(&server_info_gaurd.to_string());
                            send_to_client(&connections, &sock_addr, form.as_bytes()).await?;
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

                        let push_side = cmds[0].to_string();
                        let list_key = cmds[1].to_string();
                        let values = cmds[2..].to_vec();
                        let form = push(push_side,
                            list_key,
                            values,
                            Arc::clone(&kv_map), 
                            Arc::clone(&blpop_clients_queue1)
                        ).await?;
                        send_to_client(&connections, &sock_addr, form.as_bytes()).await?;
                    } 
                    else if cmds[0].eq("LRANGE") {

                        let list_key = cmds[1].to_string();
                        let start = cmds[2].to_string();
                        let end = cmds[3].to_string();
                        let form = lrange(list_key,
                            start,
                            end,
                            Arc::clone(&kv_map), 
                        ).await?;
                        send_to_client(&connections, &sock_addr, form.as_bytes()).await?;
                    } else if cmds[0].eq("LLEN") {

                        let list_key = cmds[1].to_string();
                        let form = llen(list_key, 
                            Arc::clone(&kv_map), 
                        ).await?;
                        send_to_client(&connections, &sock_addr, form.as_bytes()).await?;
                    } else if cmds[0].eq("LPOP") {

                        let form = lpop(&cmds, 
                            Arc::clone(&kv_map), 
                        ).await?;
                        send_to_client(&connections, &sock_addr, form.as_bytes()).await?;
                    } else if cmds[0].eq("BLPOP") { 

                        blpop(&cmds, 
                            sock_addr, 
                            Arc::clone(&kv_map), 
                            Arc::clone(&connections1),
                            Arc::clone(&blpop_clients_queue1)
                        ).await?;
                        // send_to_client(&connections, &sock_addr, form.as_bytes()).await?;
                    } else if cmds[0].eq("TYPE") {

                        let key = &cmds[1];
                        let form = type_ops(key.to_string(),  
                            Arc::clone(&kv_map)).await?;   
                        send_to_client(&connections, &sock_addr, form.as_bytes()).await?;
                    } else if cmds[0].eq("INCR") {

                        let key = cmds[1].to_string();
                
                        let command_transc_len = get_command_trans_len(sock_addr, Arc::clone(&connections)).await;
                        let form = if command_transc_len > 0 {
                            let form = append_transaction_to_commands(CommandTransactions::Incr { key }, 
                                sock_addr, 
                                Arc::clone(&connections)).await;
                            form
                        } else {
                            
                            let form = incr_ops(
                                key,
                                Arc::clone(&kv_map)).await?;
                            form
                        };
                        send_to_client(&connections, &sock_addr, form.as_bytes()).await?;
                    } else if cmds[0].eq("MULTI") {

                        let form = multi(
                            sock_addr,
                            Arc::clone(&connections1)).await?;
                        
                        send_to_client(&connections, &sock_addr, form.as_bytes()).await?;
                    } else if cmds[0].eq("EXEC") {

                        let form = exec_multi(
                            sock_addr,
                            Arc::clone(&kv_map),
                            Arc::clone(&connections1)).await?;
                        
                        send_to_client(&connections, &sock_addr, form.as_bytes()).await?; 
                    } else if cmds[0].eq("DISCARD") {
                        
                        let form = discard_multi(
                            sock_addr,
                            Arc::clone(&connections1)).await?;
                        
                        send_to_client(&connections, &sock_addr, form.as_bytes()).await?;
                    } else if cmds[0].eq("XADD") {

                        let stream_key = cmds[1].to_string();
                        let stream_id = cmds[2].to_string();
                        let pair_values = cmds[3..].to_vec();
                        let form = xadd(stream_key, 
                            stream_id, 
                            pair_values,
                        Arc::clone(&kv_map)).await?;
                        
                        send_to_client(&connections, &sock_addr, form.as_bytes()).await?;
                    } else if cmds[0].eq("XRANGE") {

                        let stream_key = cmds[1].to_string();
                        let start = cmds[2].to_string();
                        let end = cmds[3].to_string();
                        let form = xrange(stream_key, 
                            start,
                            end,
                            Arc::clone(&kv_map)).await?;
                        
                        send_to_client(&connections, &sock_addr, form.as_bytes()).await?;
                    } else if cmds[0].eq("XREAD") {
                        println!("{:?}",cmds);
                        if cmds[1].eq("block") {
                            xread_clients_queue.lock().await.push_back(sock_addr);
                        }
                        let streams_with_id = &cmds[1..].to_vec();
                        let form = xread(
                            streams_with_id, 
                            Arc::clone(&kv_map)).await?;
                        if cmds[1].eq("block") {
                            // println!("returning form to block client: {}",form);
                            let mut sock_addr= sock_addr;
                            let mut xread_clients_queue_gaurd = xread_clients_queue.lock().await;
                            if let Some(queue_sock_addr) = xread_clients_queue_gaurd.pop_front() {
                                // println!("returning to {}",queue_sock_addr);
                                // send_to_client(&connections, &queue_sock_addr, form.as_bytes()).await?;
                                sock_addr = queue_sock_addr;
                            }
                            std::mem::drop(xread_clients_queue_gaurd);
                            // println!("Dropped xread_clients_queue_gaurd");
                            send_to_client(&connections, &sock_addr, form.as_bytes()).await?;            
                        } else {
                            send_to_client(&connections, &sock_addr, form.as_bytes()).await?;
                        }
                    } else if cmds[0].eq("SUBSCRIBE") {

                        let form = subscribe(
                            &cmds, 
                            sock_addr, 
                            connections.clone(), 
                            pub_sub_map.clone()).await?;
                        send_to_client(&connections, &sock_addr, form.as_bytes()).await?;
                    } else if cmds[0].eq("UNSUBSCRIBE") {

                        let form = unsubscribe(
                            &cmds, 
                            sock_addr, 
                            connections.clone(), 
                            pub_sub_map.clone()).await?;
                        send_to_client(&connections, &sock_addr, form.as_bytes()).await?;
                    } else if cmds[0].eq("PUBLISH") {

                        let form = publish(
                            &cmds, 
                            sock_addr, 
                            connections.clone(), 
                            pub_sub_map.clone()).await?;
                        send_to_client(&connections, &sock_addr, form.as_bytes()).await?;
                    } else if cmds[0].eq("ZADD") {
                        if cmds.len() < 4 {
                            let form = "-ERR wrong number of arguments for 'zadd' command\r\n";
                            send_to_client(&connections, &sock_addr, form.as_bytes()).await?;    
                        } else {

                            let key = &cmds[1];
                            let set_values = &cmds[2..].to_vec();
                            if set_values.len() % 2 != 0 {
                                send_to_client(&connections, &sock_addr, "-ERR syntax error\r\n".as_bytes()).await?;
                            } else {
                                let form = zadd(
                                    key,
                                    set_values,
                                    Arc::clone(&kv_map)
                                ).await?;
                                send_to_client(&connections, &sock_addr, form.as_bytes()).await?;
                            }
                        }
                    }
                    else {
                        send_to_client(&connections, &sock_addr, b"+OK\r\n").await?;
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

pub async fn send_to_client(
    connections: &SharedConnectionHashMapT,
    sock_addr: &SocketAddr,
    msg: &[u8],
) -> Result<(), RedisErrors> {
    println!("Sending data back to client ({})", sock_addr);
    if let Some(conn_struct) = connections.lock().await.get(&sock_addr.port()) {
        let client_tx = conn_struct.tx_sender.clone();
        client_tx.send((*sock_addr, msg.to_vec()))?;
    }
    Ok(())
}

pub async fn write_handler(
    mut writer: tokio::net::tcp::OwnedWriteHalf,
    mut rx: RecvChannelT,
    connections: SharedConnectionHashMapT
) {
    println!("Write handler!");
    while let Some(recv_bytes) = rx.recv().await {
        let (_sock_addr, reply_bytes) = recv_bytes; 
        // println!("In write_handler - {}",_sock_addr);
        match writer.write_all(&reply_bytes).await {
            Ok(_) => {
                // println!("Writing data back to client: {}", _sock_addr);
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

