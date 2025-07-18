use std::net::SocketAddr;

use crate::{connection_handling::SharedConnectionHashMapT, errors::RedisErrors, redis_key_value_struct::{SharedMapT}};

pub async fn incr_ops(
    cmds: &Vec<String>,
    sock_addr: SocketAddr, 
    kv_map: SharedMapT,
    connections: SharedConnectionHashMapT
) -> Result<(), RedisErrors> {

    let key = &cmds[1];
    let mut form = String::new();
    if let Some(value_struct)  = kv_map.lock().await.get_mut(key) {
        match value_struct.mut_value() {
            crate::redis_key_value_struct::Value::STRING(_) => {},
            crate::redis_key_value_struct::Value::NUMBER(num) => {
                *num += 1;
                form.push(':');
                form.push_str(&*num.to_string());
                form.push_str("\r\n");
            },
            crate::redis_key_value_struct::Value::LIST(_) => {},
            crate::redis_key_value_struct::Value::STREAM(_) => {},
        }
    }
    if let Some((client_tx, _flag)) = connections.lock().await.get(&sock_addr.port()) {
        client_tx.send((sock_addr, form.as_bytes().to_vec()))?;
    }
    Ok(())
}