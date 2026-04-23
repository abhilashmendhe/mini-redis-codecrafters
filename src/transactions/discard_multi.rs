use std::net::SocketAddr;

use crate::{connection_handling::SharedConnectionHashMapT, errors::RedisErrors};

pub async fn discard_multi(
    sock_addr: SocketAddr,
    connections: SharedConnectionHashMapT,
) -> Result<String, RedisErrors> {
    let mut form = String::new();
    {
        let client_port = sock_addr.port();
        let mut connections_gaurd = connections.lock().await;
        if let Some(conn_struct) = connections_gaurd.get_mut(&client_port) {
            let commands_transac = conn_struct.mut_get_command_vec();
            // println!("Lenth of command vec: {}",commands_transac.len());
            // println!("Command vec: {:?}", commands_transac);
            if commands_transac.len() == 0 {
                form.push_str("-ERR DISCARD without MULTI\r\n");
            } else {
                form.push_str("+OK\r\n");
                commands_transac.clear();
            }
        }
    }
    Ok(form)
}
