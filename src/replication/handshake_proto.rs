use std::sync::Arc;

use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream, sync::Mutex};

use crate::{errors::RedisErrors, replication::{replica_info::ReplicaInfo, slave_replica_info::SlaveReplicaInfo}};

pub async fn handshake(replica_info: Arc<Mutex<ReplicaInfo>>) -> Result<(), RedisErrors> {
    
    let replica_info_gaurd = replica_info.lock().await;
    match &*replica_info_gaurd {
        ReplicaInfo::MASTER(_) => {},
        ReplicaInfo::SLAVE(slave_replica_info) => {

            let master_host = slave_replica_info.master_host();
            let master_port = slave_replica_info.master_port();
            let master_addr = format!("{}:{}",master_host,master_port);
            first_handshake(slave_replica_info, master_addr.as_str()).await?;            
        },
    }
    Ok(())
}

async fn first_handshake(_slave_replica_info: &SlaveReplicaInfo, master_addr: &str) -> Result<(), RedisErrors> {
    if let Ok(mut stream) = TcpStream::connect(&master_addr).await {
        // println!("Sending a PING to master server!");
        
        let _ = stream.write_all("*1\r\n$4\r\nPING\r\n".as_bytes()).await?;
        let _ = stream.flush().await;
        // read the incoming message from master server
        let mut buf = [0 as u8; 1024];
        let len = stream.read(&mut buf).await?;
        let message = String::from_utf8_lossy(&buf[..len]);
        println!("First handshake Received: {}", message);
        if message.eq("+PONG\r\n") {
            // second_handshake(_slave_replica_info, master_addr).await?;
            second_handshake(_slave_replica_info, &mut stream).await?;
        }
    } else {
        eprintln!("Failed to connect to {}", &master_addr);
    }
    Ok(())
}

async fn second_handshake(_slave_replica_info: &SlaveReplicaInfo, stream: &mut TcpStream) -> Result<(), RedisErrors> {
    let _ = stream.write("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n".as_bytes()).await?;
    let _ = stream.flush().await;
    // read the incoming message from master server
    let mut buf = [0 as u8; 1024];
    let len = stream.read(&mut buf).await?;
    let message = String::from_utf8_lossy(&buf[..len]);
    println!("Second handshake Received: {}", message);
    if message.eq("+OK\r\n") {
        third_handshake(_slave_replica_info, stream).await?;
    }
    Ok(())
}

async fn third_handshake(_slave_replica_info: &SlaveReplicaInfo, stream: &mut TcpStream) -> Result<(), RedisErrors> {
    
    let _ = stream.write("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n".as_bytes()).await?;
    let _ = stream.flush().await;
    // read the incoming message from master server
    let mut buf = [0 as u8; 1024];
    let len = stream.read(&mut buf).await?;
    let message = String::from_utf8_lossy(&buf[..len]);
    println!("Third handshake Received: {}", message);
    if message.eq("+OK\r\n") {
        final_handshake(_slave_replica_info, stream).await?;
    }
    Ok(())
}

async fn final_handshake(_slave_replica_info: &SlaveReplicaInfo, stream: &mut TcpStream) -> Result<(), RedisErrors> {
    
    let _ = stream.write("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n".as_bytes()).await?;
    let _ = stream.flush().await;
    // read the incoming message from master server
    let mut buf = [0 as u8; 1024];
    let len = stream.read(&mut buf).await?;
    let message = String::from_utf8_lossy(&buf[..len]);
    println!("Final handshake Received: {}", message);
    
    Ok(())
}