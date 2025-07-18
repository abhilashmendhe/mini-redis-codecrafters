use std::net::SocketAddr;

use crate::{basics::{ all_types::SharedMapT, kv_ds::Value}, connection_handling::SharedConnectionHashMapT, errors::RedisErrors};


pub async fn type_ops(
    cmds: &Vec<String>,
    sock_addr: SocketAddr,
    connections: SharedConnectionHashMapT,
    kv_map: SharedMapT
) -> Result<(), RedisErrors> {
    let v_type = {
    let key = &cmds[1];
    if let Some(v) = kv_map.lock().await.get(key) {
            let value = v.value();
            let v_type = match value {
                Value::STRING(_) => "+string\r\n",
                Value::NUMBER(_) => "+number\r\n",
                Value::LIST(_) => "+list\r\n",
                Value::STREAM(_) => "+stream\r\n",
            };
            v_type
        } else {
            "+none\r\n"
        }
    };

    if let Some((client_tx, _flag)) = connections.lock().await.get(&sock_addr.port()) {
        let form = format!("{}",v_type);
        client_tx.send((sock_addr, form.as_bytes().to_vec()))?;
    }
    Ok(())
}