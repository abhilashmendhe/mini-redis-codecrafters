use std::net::SocketAddr;

use crate::{connection_handling::SharedConnectionHashMapT, transactions::commands::CommandTransactions};


pub async fn get_command_trans_len(
    sock_addr: SocketAddr,
    connections: SharedConnectionHashMapT
) -> usize {
    
    let conn_gaurd = connections.lock().await;
    if let Some(conn_struct) = conn_gaurd.get(&sock_addr.port()) {
        conn_struct.get_command_vec().len()
    } else {
        0
    }
}

pub async fn append_transaction_to_commands(
    command_trans: CommandTransactions,
    sock_addr: SocketAddr,
    connections: SharedConnectionHashMapT) -> String {
        
    let mut conn_gaurd = connections.lock().await;
    if let Some(conn_struct) = conn_gaurd.get_mut(&sock_addr.port()) {
        let commands_trans_vec = conn_struct.mut_get_command_vec();
        // commands_trans_vec.push_back(key.to_string());
        commands_trans_vec.push_back(command_trans);
    }
    "+QUEUED\r\n".to_string()
}