// #![allow(unused)]
mod errors;
mod handle_client;
mod redis_key_value_struct;
mod rdb_persistence;
mod redis_server_info;
mod replication;
mod connection_handling;
mod master_or_slave;

use std::{env::args, net::SocketAddr, sync::Arc};

use tokio::{net::TcpListener, signal, sync::mpsc};

use crate::{connection_handling::{init_connection_channel, read_connections_simul}, errors::RedisErrors, handle_client::{read_handler, write_handler}, master_or_slave::{run_master, run_slave}, redis_key_value_struct::{clean_map, init_map}, redis_server_info::init_sever_info, replication::{handshake_proto::handshake, replica_info::init_replica_info}};

use crate::rdb_persistence::{rdb_persist::{init_rdb, save}, read_rdb::read_rdb_file};


#[tokio::main]
async fn main() -> Result<(), RedisErrors> {

    // 1. Read args for RDB persistence and check if folder and file(.rdb) exists
    let args = args().collect::<Vec<String>>();

    let connections = init_connection_channel().await;

    let server_info = init_sever_info(&args).await?;

    let replica_info = init_replica_info(&args).await?;

    let rdb = init_rdb(&args)?;
    
    let map = init_map();

    let map1 = Arc::clone(&map);
    match read_rdb_file(rdb.clone(), map1).await {
        Ok(_) => {},
        Err(_e) => {
            save(rdb.clone()).await?;
        },
    }

    // // Read keys and values periodically to check if client is inserted on not
    // // The below function call is temporary. Should not run on prod servers.
    // let connections1 = Arc::clone(&connections);
    // tokio::spawn(async move {
    //     read_connections_simul(connections1).await;
    // });

    // Regular clean-up of keys in redis
    let map2 = Arc::clone(&map);
    tokio::spawn(async move {
        let _ = clean_map(map2).await;
    });

    
    
    let replica_info1 = Arc::clone(&replica_info);
    let replica_info_gaurd = replica_info1.lock().await;
    let rdb1 = Arc::clone(&rdb);
    let role = replica_info_gaurd.role();
    std::mem::drop(replica_info_gaurd);

    if role.eq("master") {
        println!("Call master code async.");
        run_master(connections, map, rdb1, server_info, replica_info).await?;
    } else {
        println!("Call slave code async.");
        run_slave(connections, map, rdb1, server_info, replica_info).await?;
    }

    println!("Gracefully shutting down redis server!");
    save(rdb.clone()).await?; 
    Ok(())
    
}