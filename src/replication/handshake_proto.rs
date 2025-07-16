use std::{collections::HashMap, sync::Arc};

use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{tcp::{OwnedReadHalf, OwnedWriteHalf}, TcpStream}, sync::Mutex};

use crate::{
    connection_handling::SharedConnectionHashMapT, 
    errors::RedisErrors, 
    parse_redis_bytes_file::parse_multi_commands, 
    rdb_persistence::rdb_persist::RDB, 
    redis_key_value_struct::{SharedMapT, ValueStruct}, 
    redis_server_info::ServerInfo, 
    replication::{handle_master_client::master_reader_handle, replica_info::ReplicaInfo}
};


pub async fn handshake(
    _connections: SharedConnectionHashMapT,
    kv_map: Arc<Mutex<HashMap<String, ValueStruct>>>,
    _rdb: Arc<Mutex<RDB>>,
    server_info: Arc<Mutex<ServerInfo>>, 
    replica_info: Arc<Mutex<ReplicaInfo>>
) -> Result<(), RedisErrors> {

    let recv_bytes_count = Arc::new(Mutex::new(0_usize));
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
        let (reader, writer) = stream.into_split();
        let reader = Arc::new(Mutex::new(reader));
        let writer = Arc::new(Mutex::new(writer));

        // Try to connect and do handshake
        let server_info1 = Arc::clone(&server_info);
        let server_info2 = Arc::clone(&server_info);
        let reader1 = Arc::clone(&reader);
        let writer1 = Arc::clone(&writer);
        let kv_map1 = Arc::clone(&kv_map);
        let recv_bytes_count1 = Arc::clone(&recv_bytes_count);
        let recv_bytes_count3 = Arc::clone(&recv_bytes_count);
        full_handshake(
            reader1, 
            writer1, 
            server_info1,
            kv_map1,
            recv_bytes_count1,
        ).await?;
        
        println!("Handshake ->>> Connected and handshake complete");
        let kv_map1 = Arc::clone(&kv_map);
        let recv_bytes_count1= Arc::clone(&recv_bytes_count);
        let reader_task = tokio::spawn(async move {
        let mut buf = [0u8; 1024];
        let reader2 = Arc::clone(&reader);
        let kv_map1 = Arc::clone(&kv_map1);
        loop {
            let writer2 = Arc::clone(&writer);
            let kv_map2 = Arc::clone(&kv_map1);
            let recv_bytes_count2 = Arc::clone(&recv_bytes_count1);
            match reader2.lock().await.read(&mut buf).await {
                Ok(0) => {
                    println!("Handshake ->>> Master closed connection");
                    break;
                },
                Ok(n) => {
                    // {
                    //     let port = server_info2.lock().await.tcp_port;
                    //     if port > 6380 {
                    //         tokio::time::sleep(std::time::Duration::from_millis(5000)).await;
                    //     }
                    // }
                    if let Ok(commands) = parse_multi_commands(&mut buf[..n]).await {
                        println!("Handshake ->>> Received: {:?}", commands);
                            
                            master_reader_handle(
                                commands, 
                                writer2, 
                                kv_map2, 
                                recv_bytes_count2,
                            ).await;
                    }
                    {
                        *recv_bytes_count3.lock().await += n;
                    }
                },
                Err(e) => {
                    eprintln!("Handshake ->>> Error reading from master: {}", e);
                    break;
                },
            }
        }
    });
    reader_task.await.ok();

        println!("Handshake ->>> Connection to master lost, will retry");
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }

    Ok(())
}

async fn full_handshake(
    reader: Arc<Mutex<OwnedReadHalf>>,
    writer: Arc<Mutex<OwnedWriteHalf>>,
    server_info: Arc<Mutex<ServerInfo>>,
    kv_map: SharedMapT,
    recv_bytes_count: Arc<Mutex<usize>>,
) -> Result<(), RedisErrors> {

    let mut buffer = [0 as u8; 1024];
    
    println!("Started First Handhshake");
    {
        let mut writer_gaurd = writer.lock().await;
        let _ = writer_gaurd.write_all(b"*1\r\n$4\r\nPING\r\n").await?;
    }

    // 1st handshake
    let buf_size = {
        let mut reader_gaurd = reader.lock().await;
        let buf_size = reader_gaurd.read(&mut buffer).await?; // recv `+PONG`
        buf_size
    };

    if &buffer[..buf_size] != b"+PONG\r\n" {
        // println!("Master reply failed for first handshake. Didn't receive +PONG");
        return Err(
            RedisErrors::HandhshakeInvalidReply("Master reply failed for first handshake. Didn't receive +PONG".to_string())
        );
    }

    // 2nd handshake
    println!("Started Second Handhshake");
    {
        let rep_info_gaurd = server_info.lock().await;
        let form = format!("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n{}\r\n", rep_info_gaurd.tcp_port);
        
        let mut writer_gaurd = writer.lock().await;
        writer_gaurd.write(form.as_bytes()).await?;
    }
    
    let buf_size = {
        let mut reader_gaurd = reader.lock().await;
        let buf_size = reader_gaurd.read(&mut buffer).await?; // recv `+PONG`
        buf_size
    };
    if &buffer[..buf_size] != b"+OK\r\n" {
        // println!("Master reply failed for second handshake. Didn't receive +OK");    
        return Err(
            RedisErrors::HandhshakeInvalidReply("Master reply failed for second handshake. Didn't receive +OK".to_string())
        );      
    }

    // 3rd handshake
    println!("Started Third Handhshake");
    {
        let mut writer_gaurd = writer.lock().await;
        let _ = writer_gaurd.write_all(b"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n").await?;
    }
    let buf_size = {
        let mut reader_gaurd = reader.lock().await;
        let buf_size = reader_gaurd.read(&mut buffer).await?; // recv `+PONG`
        buf_size
    };

    if &buffer[..buf_size] != b"+OK\r\n" {
        // println!("Master reply failed for third handshake. Didn't receive +OK");    
        // break;
        return Err(
            RedisErrors::HandhshakeInvalidReply("Master reply failed for third handshake. Didn't receive +OK".to_string())
        );
    }

    // 4th(final) handshake
    println!("Started Final Handshake");
    {
        let mut writer_gaurd = writer.lock().await;
        let _ = writer_gaurd.write_all(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n").await?;
    }
    let buf_size = {
        let mut reader_gaurd = reader.lock().await;
        let buf_size = reader_gaurd.read(&mut buffer).await?; // recv `+PONG`
        buf_size
    };
    println!("{}\n", String::from_utf8_lossy(&buffer[..buf_size]));
    println!();
    let commands = parse_multi_commands(&buffer[..buf_size]).await?;
    master_reader_handle(
        commands, 
        writer, 
        kv_map, 
        recv_bytes_count, 
    ).await;
    Ok(())
}
