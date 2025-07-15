use std::{collections::HashMap, sync::Arc, time::{SystemTime, UNIX_EPOCH}};

use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream, sync::Mutex};

use crate::{
    connection_handling::SharedConnectionHashMapT, 
    errors::RedisErrors, 
    parse_redis_bytes_file::parse_multi_commands, 
    rdb_persistence::rdb_persist::RDB, 
    redis_key_value_struct::{insert, ValueStruct}, 
    redis_server_info::ServerInfo, 
    replication::replica_info::ReplicaInfo
};


pub async fn handshake(
    _connections: SharedConnectionHashMapT,
    kv_map: Arc<Mutex<HashMap<String, ValueStruct>>>,
    _rdb: Arc<Mutex<RDB>>,
    server_info: Arc<Mutex<ServerInfo>>, 
    replica_info: Arc<Mutex<ReplicaInfo>>
) -> Result<(), RedisErrors> {

    let recv_bytes_count = Arc::new(Mutex::new(0_usize));
    let recv_flag         = Arc::new(Mutex::new(false));
    loop {
        {
            // If role is master break.. Master can't perform handshake..
            let replica_info_guard = replica_info.lock().await;
            let role = replica_info_guard.role();
            if role != "slave" {
                println!("Handshake ->>> Master can't perform handshake...");
                break;
            }
        }

        // Get master info
        let (master_host, master_port) = {
            let mut replica_info_guard = replica_info.lock().await;
            match replica_info_guard.slave_info() {
                Some(slave_replica_info) => (
                    slave_replica_info.master_host(),
                    slave_replica_info.master_port()
                ),
                None => {
                    println!("Handshake ->>> No master info configured, sleeping...");
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    continue;
                }
            }
        };

        // Get master address:port to start handshake....
        let master_addr = format!("{}:{}", master_host, master_port);
        println!("Handshake ->>> Trying to connect to master at {}", master_addr);

        let stream = TcpStream::connect(&master_addr).await?;
        
        // Try to connect and do handshake
        let server_info1 = Arc::clone(&server_info);
        match full_handshake(stream, server_info1).await {
            Ok(stream) => {
                println!("Handshake ->>> Connected and handshake complete");

                // Now spawn reader task to listen for master's replication stream
                let kv_map_clone = Arc::clone(&kv_map);
                let recv_bytes_count1 = Arc::clone(&recv_bytes_count);
                let recv_flag1 = Arc::clone(&recv_flag);
                let reader_task = tokio::spawn(async move {
                    let (mut reader, mut writer) = stream.into_split();
                    let mut buf = [0u8; 1024];
                    loop {
                        match reader.read(&mut buf).await {
                            Ok(0) => {
                                println!("Handshake ->>> Master closed connection");
                                break;
                            }
                            Ok(n) => {
                                {
                                    if *recv_flag1.lock().await {
                                        println!("No. of bytes: {}", n);
                                    }
                                // println!("\n{}", String::from_utf8_lossy(&buf[..n]));
                                }
                                {
                                    if *recv_flag1.lock().await {
                                        *recv_bytes_count1.lock().await += n;
                                    }
                                }
                                if let Ok(commands) = parse_multi_commands(&mut buf[..n]).await {
                                    println!("Handshake ->>> Received: {:?}", commands);
                                    for cmd in commands {
                                        if cmd.get(1).map(|s| s.as_str()) == Some("SET") {
                                            let key = cmd[3].as_str();
                                            let value = cmd[5].as_str();
                                            let mut value_struct = ValueStruct::new(value.to_string(), None, None);
                                            if cmd.len() >= 7 {
                                                let px = cmd[9].parse::<u128>().unwrap();
                                                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
                                                let now_ms = now.as_millis() + px;
                                                value_struct.set_px(Some(px));
                                                value_struct.set_pxat(Some(now_ms));
                                            }
                                            insert(key.to_string(), value_struct, kv_map_clone.clone()).await;
                                        } else if cmd.get(1).map(|s| s.as_str()) == Some("REPLCONF") {
                                            let _ack = cmd[3].as_str();
                                            let _commands = cmd[5].as_str();
                                            let mut recv_byte_counts = 0;
                                            {
                                                *recv_flag1.lock().await = true;
                                            }
                                            {
                                                recv_byte_counts += *recv_bytes_count1.lock().await;
                                            }
                                            let repl_ack = format!("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$1\r\n{}\r\n",recv_byte_counts);
                                            // println!("Writing ack back to master\n:{}",repl_ack);
                                            let _ = writer.write(repl_ack.as_bytes()).await;
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                eprintln!("Handshake ->>> Error reading from master: {}", e);
                                break;
                            }
                        }
                    }
                });

                // Wait for reader_task to finish (i.e. until master connection closes)
                reader_task.await.ok();
                println!("Handshake ->>> Connection to master lost, will retry");
            }
            Err(e) => {
                eprintln!("Handshake ->>> Handshake error: {}", e);
            }
        }

        // Wait before retrying
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }

    Ok(())
}

async fn full_handshake(mut stream: TcpStream, server_info: Arc<Mutex<ServerInfo>>) -> Result<TcpStream, RedisErrors> {
    let mut buffer = [0 as u8; 1024];

    println!("Started First Handhshake");
        let _ = stream.write_all("*1\r\n$4\r\nPING\r\n".as_bytes()).await?;

        // 1st handshake
        let buf_size = stream.read(&mut buffer).await?; // recv `+PONG`
        if &buffer[..buf_size] != b"+PONG\r\n" {
            // println!("Master reply failed for first handshake. Didn't receive +PONG");
            return Err(
                RedisErrors::HandhshakeInvalidReply("Master reply failed for first handshake. Didn't receive +PONG".to_string())
            );
            // break;
        }

        // 2nd handshake
        println!("Started Second Handhshake");
        let mut replconf_port_str = String::new();
        {
            let rep_info_gaurd = server_info.lock().await;
            let form = format!("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n{}\r\n", rep_info_gaurd.tcp_port);
            replconf_port_str.push_str(&form);
            
        }
        let _ = stream.write(replconf_port_str.as_bytes()).await?; // write back `REPLCONF listening-port "port"` info to master
        let buf_size = stream.read(&mut buffer).await?; // recv `+OK`
        if &buffer[..buf_size] != b"+OK\r\n" {
            // println!("Master reply failed for second handshake. Didn't receive +OK");    
            return Err(
                RedisErrors::HandhshakeInvalidReply("Master reply failed for second handshake. Didn't receive +OK".to_string())
            );      
        }

        // 3rd handshake
        println!("Started Third Handhshake");
        let _ = stream.write(b"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n").await?; // write back `REPLCONF capa psync2` info to master
        let buf_size = stream.read(&mut buffer).await?; // recv `+OK`
        if &buffer[..buf_size] != b"+OK\r\n" {
            // println!("Master reply failed for third handshake. Didn't receive +OK");    
            // break;
            return Err(
                RedisErrors::HandhshakeInvalidReply("Master reply failed for third handshake. Didn't receive +OK".to_string())
            );
        }

        // 4th(final) handshake
        println!("Started Final Handshake");
        let _ = stream.write("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n".as_bytes()).await?; // write back `PSYNC ?`
        let buf_size = stream.read(&mut buffer).await?; // recv `PSYNC and rdb file content`
        println!("{}", String::from_utf8_lossy(&buffer[..buf_size]));
        Ok(stream)
}
