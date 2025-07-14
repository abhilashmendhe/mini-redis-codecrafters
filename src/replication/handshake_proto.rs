use std::{collections::HashMap, net::{IpAddr, Ipv4Addr, SocketAddr}, sync::Arc, time::{SystemTime, UNIX_EPOCH}};

use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream, signal, sync::Mutex};

use crate::{connection_handling::SharedConnectionHashMapT, errors::RedisErrors, handle_client::{parse_recv_bytes, read_handler}, rdb_persistence::rdb_persist::RDB, redis_key_value_struct::{insert, ValueStruct}, redis_server_info::ServerInfo, replication::{replica_info::ReplicaInfo, slave_replica_info}};

pub async fn handshake(
    connections: SharedConnectionHashMapT,
    kv_map: Arc<Mutex<HashMap<String, ValueStruct>>>,
    rdb: Arc<Mutex<RDB>>,
    server_info: Arc<Mutex<ServerInfo>>, 
    replica_info: Arc<Mutex<ReplicaInfo>>
) -> Result<(), RedisErrors> {
    
    let mut replica_info_gaurd = replica_info.lock().await;
    let role = replica_info_gaurd.role();
    let slave_replica_info = replica_info_gaurd.slave_info();
    std::mem::drop(replica_info_gaurd);
    if role.eq("slave") {
        if let Some(slave_replica_info) = slave_replica_info {
            let master_host = slave_replica_info.master_host();
            let master_port = slave_replica_info.master_port();
            let master_addr = format!("{}:{}",master_host,master_port);
            let server_info1 = Arc::clone(&server_info);
            let stream = first_handshake(server_info1, master_addr.as_str()).await?;    
            // let sock_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), master_port);
            
            // let (mut reader, _) = stream.into_split();
            
            // let server_task = tokio::spawn(async move{
            //     let mut buf = [0 as u8; 1024];
            //     while let Ok(n) = reader.read(&mut buf).await {
            //         if n != 0 {

            //         match parse_recv_bytes(&mut buf[..n]).await {
            //             Ok(cmds) => {
            //                 println!("{:?}",cmds);
            //                 if cmds[0] == String::from("SET") {

            //                     let key = cmds[1].as_str();
            //                     let value = cmds[2].as_str();
                                
            //                     // println!("cmds vec len: {}",cmds.len());
            //                     let mut value_struct = ValueStruct::new(
            //                         value.to_string(), 
            //                         None, 
            //                         None, 
            //                     );

            //                     if cmds.len() == 5 {
            //                         let px = cmds[4].parse::<u128>().unwrap();
            //                         let now = SystemTime::now()
            //                                 .duration_since(UNIX_EPOCH).unwrap();
            //                         let now_ms = now.as_millis() + px as u128;
            //                         value_struct.set_px(Some(px));
            //                         value_struct.set_pxat(Some(now_ms));
            //                     }
            //                     insert(key.to_string(), value_struct, kv_map.clone()).await;
            //                 }
            //             },
            //             Err(e) => {
            //                 println!("Err: {e}");   
            //                 break;
            //             },
            //         }
            //         }

            //     }
            //     println!("Server connection closed");
            // });
            // server_task.await?;

        let (mut reader, mut writer) = stream.into_split();

        let reader_task = tokio::spawn(async move {
            let mut buf = [0u8; 1024];
            loop {
                match reader.read(&mut buf).await {
                    Ok(0) => {
                        println!("Server closed connection");
                        break;
                    }
                    Ok(n) => {
                        // println!("From server: {}", String::from_utf8_lossy(&buf[..n]));
                        if let Ok(cmds) = parse_recv_bytes(&mut buf).await {
                            // println!("{:?}",cmds);
                            if cmds[0] == String::from("SET") {

                                let key = cmds[1].as_str();
                                let value = cmds[2].as_str();
                                
                                // println!("cmds vec len: {}",cmds.len());
                                let mut value_struct = ValueStruct::new(
                                    value.to_string(), 
                                    None, 
                                    None, 
                                );

                                if cmds.len() == 5 {
                                    let px = cmds[4].parse::<u128>().unwrap();
                                    let now = SystemTime::now()
                                            .duration_since(UNIX_EPOCH).unwrap();
                                    let now_ms = now.as_millis() + px as u128;
                                    value_struct.set_px(Some(px));
                                    value_struct.set_pxat(Some(now_ms));
                                }
                                insert(key.to_string(), value_struct, kv_map.clone()).await;
                            }
                        }
                        
                    }
                    Err(e) => {
                        eprintln!("Error reading from server: {}", e);
                        break;
                    }
                }
            }
        });
        reader_task.await?;
        println!("Closing the handshake stream!");
        // Wait for Ctrl+C or for the reader task to finish
        // tokio::select! {
        //     // _ = signal::ctrl_c() => {
        //     //     println!("Ctrl+C pressed. Sending shutdown message...");

        //     //     // Write a shutdown message to the server
        //     //     // writer.write_all(b"*1\r\n$3\r\nBYE\r\n").await?;
        //     //     // writer.shutdown().await?;
        //     //     println!("Shutdown message sent.");
        //     // }

        //     _ = reader_task => {
        //         println!("Reader task completed (server disconnected?)");
        //     }        
       
        } else {
            return Ok(());
        } 
    } 

    Ok(())
}

async fn first_handshake(server_info: Arc<Mutex<ServerInfo>>, master_addr: &str) -> Result<TcpStream, RedisErrors> {
    let mut stream = TcpStream::connect(&master_addr).await?;
        
    let _ = stream.write_all("*1\r\n$4\r\nPING\r\n".as_bytes()).await?;
    let _ = stream.flush().await;
    // read the incoming message from master server
    let mut buf = [0 as u8; 1024];
    let len = stream.read(&mut buf).await?;
    let message = String::from_utf8_lossy(&buf[..len]);
    println!("First handshake Received: {}", message);
    if message.eq("+PONG\r\n") {
        // second_handshake(_slave_replica_info, master_addr).await?;
        second_handshake(server_info, &mut stream).await?;
    }

    Ok(stream)
}

async fn second_handshake(server_info: Arc<Mutex<ServerInfo>>, stream: &mut TcpStream) -> Result<(), RedisErrors> {
    // let s = format!("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n",);
    let mut s = String::new();
    {
        let rep_info_gaurd = server_info.lock().await;
        let form = format!("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n{}\r\n", rep_info_gaurd.tcp_port);
        s.push_str(&form);
        
    }
    let _ = stream.write(s.as_bytes()).await?;

    // let _ = stream.write("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n".as_bytes()).await?;
    let _ = stream.flush().await;
    // read the incoming message from master server
    let mut buf = [0 as u8; 1024];
    let len = stream.read(&mut buf).await?;
    let message = String::from_utf8_lossy(&buf[..len]);
    println!("Second handshake Received: {}", message);
    if message.eq("+OK\r\n") {
        third_handshake(server_info, stream).await?;
    }
    Ok(())
}

async fn third_handshake(server_info: Arc<Mutex<ServerInfo>>, stream: &mut TcpStream) -> Result<(), RedisErrors> {
    
    let _ = stream.write("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n".as_bytes()).await?;
    let _ = stream.flush().await;
    // read the incoming message from master server
    let mut buf = [0 as u8; 1024];
    let len = stream.read(&mut buf).await?;
    let message = String::from_utf8_lossy(&buf[..len]);
    println!("Third handshake Received: {}", message);
    if message.eq("+OK\r\n") {
        final_handshake(server_info, stream).await?;
    }
    Ok(())
}

async fn final_handshake(_server_info: Arc<Mutex<ServerInfo>>, stream: &mut TcpStream) -> Result<(), RedisErrors> {
    
    let _ = stream.write("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n".as_bytes()).await?;
    let _ = stream.flush().await;
    // read the incoming message from master server
    let mut buf = [0 as u8; 1024];
    let len = stream.read(&mut buf).await?;
    let message = String::from_utf8_lossy(&buf[..len]);
    println!("Final handshake Received: {}", message);
    
    // get file content
    let mut buf = [0 as u8; 1024];
    let len = stream.read(&mut buf).await?;
    let message = String::from_utf8_lossy(&buf[..len]);
    println!("After final handshake -> Got the file content: {}", message);
    
    Ok(())
}