use std::{net::SocketAddr, time::{SystemTime, UNIX_EPOCH}};

use crate::{basics::{all_types::SharedMapT, basic_ops::insert, kv_ds::{Value, ValueStruct}}, connection_handling::SharedConnectionHashMapT, errors::RedisErrors};

pub async fn incr_ops(
    cmds: &Vec<String>,
    kv_map: SharedMapT,
) -> Result<String, RedisErrors> {

    let key = &cmds[1];
    let mut form = String::new();
    let f = {
        if let Some(value_struct)  = kv_map.lock().await.get_mut(key) {
            match value_struct.mut_value() {
                Value::STRING(_) => {
                    form.push_str("-ERR value is not an integer or out of range\r\n");
                },
                Value::NUMBER(num) => {
                    *num += 1;
                    form.push(':');
                    form.push_str(&*num.to_string());
                    form.push_str("\r\n");
                },
                Value::LIST(_) => {},
                Value::STREAM(_) => {},
            }
            false
        } else {
            true
        }
    };
    if f {
        let mut value_struct = ValueStruct::new(
            // value.to_string(), 
            Value::NUMBER(1),
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
    
    Ok(form)
}


pub async fn multi(
    cmds: &Vec<String>,
    kv_map: SharedMapT,
) -> Result<String, RedisErrors> {

    let form = "+OK\r\n".to_string();
    Ok(form)
}