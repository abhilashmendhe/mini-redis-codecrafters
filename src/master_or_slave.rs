use std::net::SocketAddr;
use std::sync::Arc;
use std::collections::HashMap;
use tokio::net::TcpListener;
use tokio::signal;
use tokio::sync::{mpsc, Mutex};

use crate::connection_handling::SharedConnectionHashMapT;
use crate::errors::RedisErrors;
use crate::handle_client::{read_handler, write_handler};
use crate::rdb_persistence::rdb_persist::RDB;
use crate::redis_key_value_struct::ValueStruct;
use crate::redis_server_info::ServerInfo;
use crate::replication::replica_info::ReplicaInfo;

pub async fn run_master(
    connections: SharedConnectionHashMapT,
    kv_map: Arc<Mutex<HashMap<String, ValueStruct>>>,
    rdb: Arc<Mutex<RDB>>,
    server_info: Arc<Mutex<ServerInfo>>,
    replica_info: Arc<Mutex<ReplicaInfo>>
) -> Result<(), RedisErrors> {

    let server_info_gaurd = server_info.lock().await;
    let ip_port = format!("{}:{}",server_info_gaurd.listener_info.bind_ipv4(),server_info_gaurd.listener_info.port());
    let listener = TcpListener::bind(ip_port).await?;
    std::mem::drop(server_info_gaurd);

    loop {
        tokio::select! {
            res_acc = listener.accept() => {
                match res_acc {
                    Ok((stream, sock_addr)) => {

                        // channel to send/recv commands
                        let (tx, rx) = mpsc::unbounded_channel::<(SocketAddr, Vec<u8>)>();
                        
                        // insert client info to the HashMap
                        {
                            let mut conns = connections.lock().await;
                            conns.insert(sock_addr.port(), (tx.clone(), false));
                        }

                        // split the stream
                        let (reader, writer) = stream.into_split();

                        let kv_map1 = Arc::clone(&kv_map);
                        let rdb1 = Arc::clone(&rdb);
                        let server_info1 = Arc::clone(&server_info);
                        let replica_info1 = Arc::clone(&replica_info);
                        let connections1 = Arc::clone(&connections);

                        // Spawn thread for reader task
                        tokio::spawn(async move {
                            read_handler(
                                reader, 
                                sock_addr, 
                                connections1, 
                                kv_map1, 
                                rdb1, 
                                server_info1, 
                                replica_info1
                            ).await
                        });

                        // Spawn thread for writer task
                        let connections2 = Arc::clone(&connections);
                        tokio::spawn(async move {
                            write_handler(writer, rx, connections2).await
                        });
                    },
                    Err(e) => {
                        eprintln!("Accept error: {}", e);
                    },
                }
            }
            _  = signal::ctrl_c() => {
                println!("\nCtrl-c command received!");
                break;
            }
        }
    }

    Ok(())
}

// pub async fn run_slave(
//     connections: SharedConnectionHashMapT,
//     kv_map: Arc<Mutex<HashMap<String, ValueStruct>>>,
//     rdb: Arc<Mutex<RDB>>,
//     server_info: Arc<Mutex<ServerInfo>>,
//     replica_info: Arc<Mutex<ReplicaInfo>>
// ) -> Result<(), RedisErrors> {

//     let connections1 = Arc::clone(&connections);
//     let kv_map1 = Arc::clone(&kv_map);
//     let rdb1 = Arc::clone(&rdb);
//     let server_info1 = Arc::clone(&server_info);
//     let replica_info1 = Arc::clone(&replica_info);

//     let connections2 = Arc::clone(&connections);
//     let kv_map2 = Arc::clone(&kv_map);
//     let rdb2 = Arc::clone(&rdb);
//     let server_info2 = Arc::clone(&server_info);
//     let replica_info2 = Arc::clone(&replica_info);
//     tokio::spawn(async move {
//         let _ = run_master(connections1, kv_map1, rdb1, server_info1, replica_info1).await;
//     });
//     // tokio::spawn(async move {
//     let v = handshake(connections, kv_map, rdb, server_info, replica_info).await;
    
//     // });
//     println!("After handshake!:{:?}",v);
//     // tokio::spawn(async move {
//     let v = run_master(connections2, kv_map2, rdb2, server_info2, replica_info2).await;
//     println!("Ran run_master again after handshake closed!");
//     // }); 
//     Ok(())
// }