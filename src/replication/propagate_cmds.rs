use std::{
    net::SocketAddr, 
};


use crate::{
    connection_handling::SharedConnectionHashMapT, 
    errors::RedisErrors, 
};

pub async fn propagate_master_commands(
    sock_addr: SocketAddr,
    connections: SharedConnectionHashMapT,
    buffer: Vec<u8>
) -> Result<(), RedisErrors> {

    {
        let mut conn_gaurd = connections.lock().await;
        for (_k, conn_struct) in conn_gaurd.iter_mut() {
            let _flag = conn_struct.flag;
            let _client_tx = conn_struct.tx_sender.clone();
            if _flag {
                println!("key: {}, flag: {}", _k, _flag);
                _client_tx.send((sock_addr, buffer.clone()))?;
            } 
        }
    }
    Ok(())
}

pub async fn propagate_replconf_getack(
    sock_addr: SocketAddr,
    connections: SharedConnectionHashMapT,
) -> Result<(), RedisErrors> {

    {
        let mut conn_gaurd = connections.lock().await;
        for (_k, conn_struct) in conn_gaurd.iter_mut() {
            let _flag = conn_struct.flag;
            let _client_tx = conn_struct.tx_sender.clone();
            if _flag {
                _client_tx.send((sock_addr, b"*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n".to_vec()))?;
            } 
        }
    }
    println!("Propagated REPLCONF GETACK *");
    Ok(())
}