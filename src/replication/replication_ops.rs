use std::{collections::{HashSet, VecDeque}, net::SocketAddr, sync::Arc};

use tokio::sync::{Mutex, Notify};

use crate::{connection_handling::SharedConnectionHashMapT, errors::RedisErrors, replication::{propagate_cmds::{propagate_master_commands, propagate_replconf_getack}, replica_info::ReplicaInfo}};

pub async fn replconf_ops(
    cmds: &Vec<String>,
    sock_addr: SocketAddr,
    connections: SharedConnectionHashMapT,
    replica_info: Arc<Mutex<ReplicaInfo>>,
    slave_ack_count: Arc<Mutex<usize>>,
    notify_replica: Arc<Notify>,
    slave_ack_set: Arc<Mutex<HashSet<u16>>>,
) -> Result<(), RedisErrors> {

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
    Ok(())
}

pub async fn psync_ops(
    sock_addr: SocketAddr,
    connections: SharedConnectionHashMapT,
    slave_ack_set: Arc<Mutex<HashSet<u16>>>,
) -> Result<(), RedisErrors> {

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
    Ok(())
}

pub async fn wait_repl(
    cmds: &Vec<String>,
    sock_addr: SocketAddr,
    connections: SharedConnectionHashMapT,
    replica_info: Arc<Mutex<ReplicaInfo>>,
    slave_ack_count: Arc<Mutex<usize>>,
    notify_replica: Arc<Notify>,        
    command_init_for_replica: Arc<Mutex<bool>>,
    store_commands: Arc<Mutex<VecDeque<Vec<u8>>>>
) -> Result<(), RedisErrors> {
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
    Ok(())
}
