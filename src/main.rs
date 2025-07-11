// #![allow(unused)]
mod errors;
mod handle_client;
mod redis_key_value_struct;
mod rdb_persistence;
mod redis_server_info;
mod replication;

use std::{env::args, sync::Arc};

use tokio::{net::TcpListener, signal};

use crate::{errors::RedisErrors, handle_client::handle_client, redis_key_value_struct::{clean_map, init_map}, redis_server_info::init_sever_info, replication::replica_info::init_replica_info};

use crate::rdb_persistence::{rdb_persist::{init_rdb, save}, read_rdb::read_rdb_file};

#[tokio::main]
async fn main() -> Result<(), RedisErrors> {

    // 1. Read args for RDB persistence and check if folder and file(.rdb) exists
    let args = args().collect::<Vec<String>>();

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

    // Regular clean-up of keys in redis
    let map2 = Arc::clone(&map);
    tokio::spawn(async move {
        let _ = clean_map(map2).await;
    });

    // Create a tcp redis-server on port ::6379
    let server_info_gaurd = server_info.lock().await;
    let ip_port = format!("{}:{}",server_info_gaurd.listener_info.bind_ipv4(),server_info_gaurd.listener_info.port());
    let listener = TcpListener::bind(ip_port).await?;
    std::mem::drop(server_info_gaurd); 
    
    loop {
        tokio::select! {
            res_acc = listener.accept() => {
                match res_acc {
                    Ok((stream, _sock_addr)) => {
                        let map1 = Arc::clone(&map);
                        let rdb1 = Arc::clone(&rdb);
                        let server_info1 = Arc::clone(&server_info);
                        let replica_info1 = Arc::clone(&replica_info);
                        tokio::spawn(async move {
                            handle_client(stream, map1, rdb1, server_info1, replica_info1).await
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

    println!("Gracefully shutting down redis server!");
    // save(rdb.clone()).await?; 
    Ok(())
    
}

/*
// // read_rdb_file("./all_dumps/dump_one_key.rdb");
    // println!("");
    // // read_rdb_file("./all_dumps/dump20.rdb");
    // // println!("");
    // read_rdb_file("./dumps/dump100.rdb");
    // // println!("");
    // read_rdb_file("./dumps/grape.rdb");
    // println!();
    // read_rdb_file("./dumps/pear.rdb");
    // println!();
    // read_rdb_file("./dumps/strawberry.rdb");
    // // read_rdb_file("./dumps/dump_ints2.rdb");
    // println!();
    // // read_rdb_file("./dumps/dump_with_dbindex1.rdb");
*/