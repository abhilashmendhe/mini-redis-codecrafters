use std::{net::SocketAddr, time::{SystemTime, UNIX_EPOCH}};

use crate::{connection_handling::SharedConnectionHashMapT, errors::RedisErrors, redis_key_value_struct::{insert, SharedMapT, ValueStruct}};

pub async fn incr_ops(
    cmds: &Vec<String>,
    sock_addr: SocketAddr, 
    kv_map: SharedMapT,
    connections: SharedConnectionHashMapT
) -> Result<(), RedisErrors> {

    let key = &cmds[1];
    let mut form = String::new();
    let f = {
        if let Some(value_struct)  = kv_map.lock().await.get_mut(key) {
            match value_struct.mut_value() {
                crate::redis_key_value_struct::Value::STRING(_) => {
                    form.push_str("-ERR value is not an integer or out of range\r\n");
                },
                crate::redis_key_value_struct::Value::NUMBER(num) => {
                    *num += 1;
                    form.push(':');
                    form.push_str(&*num.to_string());
                    form.push_str("\r\n");
                },
                crate::redis_key_value_struct::Value::LIST(_) => {},
                crate::redis_key_value_struct::Value::STREAM(_) => {},
            }
            false
        } else {
            true
        }
    };
    if f {
        let mut value_struct = ValueStruct::new(
            // value.to_string(), 
            crate::redis_key_value_struct::Value::NUMBER(1),
            None, 
            None, 
        );

        if cmds.len() == 5 {
            let px = cmds[4].parse::<u128>()?;
            let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)?;
            let now_ms = now.as_millis() + px as u128;
            value_struct.set_px(Some(px));
            value_struct.set_pxat(Some(now_ms));
        }
        insert(key.to_string(), value_struct, kv_map).await;
        form.push(':');
        form.push('1');
        form.push_str("\r\n");
    }
    if let Some((client_tx, _flag)) = connections.lock().await.get(&sock_addr.port()) {
        client_tx.send((sock_addr, form.as_bytes().to_vec()))?;
    }
    Ok(())
}