use std::net::SocketAddr;
use std::sync::Arc;
use std::collections::{HashMap, HashSet, VecDeque};
use tokio::net::TcpListener;
use tokio::signal;
use tokio::sync::{mpsc, oneshot, Mutex, Notify};

use crate::basics::kv_ds::ValueStruct;
// use crate::connection_handling::_periodic_ack_slave;
use crate::connection_handling::{ConnectionStruct, SharedConnectionHashMapT};
use crate::errors::RedisErrors;
use crate::handle_client::{read_handler, write_handler};
use crate::rdb_persistence::rdb_persist::RDB;
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

    let slave_ack_set: Arc<Mutex<HashSet<u16>>> = Arc::new(Mutex::new(HashSet::new()));
    let command_init_for_slave = Arc::new(Mutex::new(false));
    let store_commands: Arc<Mutex<VecDeque<Vec<u8>>>> = Arc::new(Mutex::new(VecDeque::new()));
    let slave_ack_count: Arc<Mutex<usize>> = Arc::new(Mutex::new(0));
    let notify_replica = Arc::new(Notify::new());
    let blpop_clients_queue: Arc<Mutex<VecDeque<(SocketAddr,oneshot::Sender<(String,SocketAddr)>)>>> = Arc::new(Mutex::new(VecDeque::new()));
    // let multi_command_map = Arc::new(Mutex::new(HashMap::new()));
    let xread_clients_queue: Arc<Mutex<VecDeque<SocketAddr>>> = Arc::new(Mutex::new(VecDeque::new()));
    loop {
        tokio::select! {
            res_acc = listener.accept() => {
                match res_acc {
                    Ok((stream, sock_addr)) => {

                        // channel to send/recv commands
                        let (tx, rx) = mpsc::unbounded_channel::<(SocketAddr, Vec<u8>)>();
                        
                        // insert client info to the HashMap
                        {
                            let conn_struct = ConnectionStruct::new(tx.clone(), false, VecDeque::new());
                            let mut conns = connections.lock().await;
                            conns.insert(sock_addr.port(), conn_struct);
                        }

                        // split the stream
                        let (reader, writer) = stream.into_split();

                        // let writer1 = Arc::new(Mutex::new(&writer));
                        let kv_map1 = Arc::clone(&kv_map);
                        let rdb1 = Arc::clone(&rdb);
                        let server_info1 = Arc::clone(&server_info);
                        let replica_info1 = Arc::clone(&replica_info);
                        let connections1 = Arc::clone(&connections);
                        let slave_ack_set1 = Arc::clone(&slave_ack_set);
                        let slave_ack_count1 = Arc::clone(&slave_ack_count);
                        let notify_replica1 = Arc::clone(&notify_replica);
                        let command_init_for_replica1 = Arc::clone(&command_init_for_slave);
                        let store_commands1 = Arc::clone(&store_commands);
                        let blpop_clients_queue1 = Arc::clone(&blpop_clients_queue);
                        let xread_clients_queue1 = Arc::clone(&xread_clients_queue);
                        // let multi_command_map1 = Arc::clone(&multi_command_map);
                        // Spawn thread for reader task
                        tokio::spawn(async move {
                            read_handler(
                                reader, 
                                sock_addr, 
                                connections1, 
                                kv_map1, 
                                rdb1, 
                                server_info1, 
                                replica_info1,
                                slave_ack_set1,
                                slave_ack_count1,
                                notify_replica1,
                                blpop_clients_queue1,
                                command_init_for_replica1,
                                store_commands1,
                                xread_clients_queue1
                                // multi_command_map1
                            ).await
                        });

                        // Spawn thread for writer task
                        let connections2 = Arc::clone(&connections);

                        tokio::spawn(async move {
                            write_handler(writer, rx, connections2).await
                        });

                        // let connections3 = Arc::clone(&connections);
                        // tokio::spawn(async move {
                        //     match _periodic_ack_slave(connections3, sock_addr).await {
                        //         Ok(_ack) => todo!(),
                        //         Err(_err) => println!("{:?}",_err),
                        //     };
                        // });
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
