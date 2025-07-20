
use std::{collections::{HashMap, HashSet, VecDeque}, net::SocketAddr, sync::Arc};

use tokio::{io::{AsyncReadExt, AsyncWriteExt}, sync::{oneshot, Mutex, Notify}};

use crate::{basics::{basic_ops::{get, set}, kv_ds::ValueStruct}, connection_handling::{RecvChannelT, SharedConnectionHashMapT}, errors::RedisErrors, kv_lists::list_ops::{blpop, llen, lpop, lrange, push}, parse_redis_bytes_file::try_parse_resp, redis_server_info::ServerInfo, replication::{propagate_cmds::propagate_master_commands, replica_info::ReplicaInfo, replication_ops::{psync_ops, replconf_ops, wait_repl}}, streams::stream_ops::type_ops, transactions::transac_ops::{exec_multi, incr_ops, multi}};
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
    store_commands: Arc<Mutex<VecDeque<Vec<u8>>>>,
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
    loop {  
        match reader.read(&mut read_buf).await {
            Ok(0) => {
                println!("0 bytes received");
                let mut clients_ports = vec![];
                let connections2 = Arc::clone(&connections1);
                {
                    let conn_gaurd = connections2.lock().await;
                    for (k, conn_struct) in conn_gaurd.iter() {
                        // println!("key: {}, flag: {}", k, _flag);
                        let _flag = conn_struct.flag;
                        if !_flag {
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

                buffer.extend_from_slice(&read_buf[..n]);
                
                let _recv_msg = String::from_utf8_lossy(&read_buf[..n]);

                // println!("({}) --->  Recv: {}",sock_addr,_recv_msg);
                
                while let Some((cmds, _end)) = try_parse_resp(&buffer).await {
                    buffer.clear();

                    // println!("{:?}",cmds);
                    // send_to_client(&connections, &sock_addr, b"+OK\r\n").await?;
                    // tokio::time::sleep(Duration::from_secs(4)).await;
                    
                    // let cmds = parse_recv_bytes(&buffer[..n]).await?;
                    
                    if cmds[0].eq("PING") {

                        let form = format!("+PONG\r\n");
                        send_to_client(&connections, &sock_addr, form.as_bytes()).await?;
                    } else if cmds[0].eq("ECHO") {

                        let form = format!("+{}\r\n",cmds[1]);
                        send_to_client(&connections, &sock_addr, form.as_bytes()).await?;
                    } else if cmds[0].eq("BYE") {

                        println!("Bye received from replica: {}",sock_addr);
                        connections.lock().await.remove(&sock_addr.port());
                    } else if cmds[0] == String::from("GET") {

                        let form = get(&cmds, kv_map.clone()).await;
                        send_to_client(&connections, &sock_addr, form.as_bytes()).await?;
                    } else if cmds[0] == String::from("SET") {

                        let form = set(&cmds, Arc::clone(&kv_map)).await?;
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
                            send_to_client(&connections, &sock_addr, full_str.as_bytes()).await?;
                        }
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

                        let form = push(&cmds,
                            Arc::clone(&kv_map), 
                            Arc::clone(&blpop_clients_queue1)
                        ).await?;
                        send_to_client(&connections, &sock_addr, form.as_bytes()).await?;
                    } 
                    else if cmds[0].eq("LRANGE") {

                        let form = lrange(&cmds, 
                            Arc::clone(&kv_map), 
                        ).await?;
                        send_to_client(&connections, &sock_addr, form.as_bytes()).await?;
                    } else if cmds[0].eq("LLEN") {

                        let form = llen(&cmds, 
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

                        let form = type_ops(&cmds,  
                            Arc::clone(&kv_map)).await?;   
                        send_to_client(&connections, &sock_addr, form.as_bytes()).await?;
                    } else if cmds[0].eq("XADD") {
                        
                    } else if cmds[0].eq("INCR") {

                        let form = incr_ops(
                            &cmds, 
                            Arc::clone(&kv_map)).await?;
                        send_to_client(&connections, &sock_addr, form.as_bytes()).await?;

                    } else if cmds[0].eq("MULTI") {

                        let form = multi(
                            &cmds,
                            sock_addr, 
                            Arc::clone(&kv_map),
                        Arc::clone(&connections1)).await?;
                        
                        send_to_client(&connections, &sock_addr, form.as_bytes()).await?;
                    } else if cmds[0].eq("EXEC") {
                        let form = exec_multi(
                            &cmds,
                            sock_addr, 
                            Arc::clone(&kv_map),
                            Arc::clone(&connections1)).await?;
                        
                        send_to_client(&connections, &sock_addr, form.as_bytes()).await?; 
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

