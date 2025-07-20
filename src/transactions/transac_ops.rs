use std::{ collections::VecDeque, net::SocketAddr, time::{SystemTime, UNIX_EPOCH}};

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
    sock_addr: SocketAddr,
    _kv_map: SharedMapT,
    connections: SharedConnectionHashMapT
) -> Result<String, RedisErrors> {

    let key = &cmds[0];
    {
        let client_port = sock_addr.port();
        let mut connections_gaurd = connections.lock().await;
        if let Some(conn_struct) = connections_gaurd.get_mut(&client_port) {
            // conn_struct.command_trans.clone()
            let commands_trans_vec = conn_struct.mut_get_command_vec();
            commands_trans_vec.push_back(key.to_string());
        }
    }
    
    let form = "+OK\r\n".to_string();
    Ok(form)
}

pub async fn exec_multi(
    cmds: &Vec<String>,
    sock_addr: SocketAddr,
    _kv_map: SharedMapT,
    connections: SharedConnectionHashMapT
) -> Result<String, RedisErrors> {
    
    let mut form = String::new();
    {
        let client_port = sock_addr.port();
        let mut connections_gaurd = connections.lock().await;
        if let Some(conn_struct) = connections_gaurd.get_mut(&client_port) {
            let commands_transac = conn_struct.get_command_vec();
            if commands_transac.len() == 0 {
                form.push_str("-ERR EXEC without MULTI\r\n");
            } else {
                form.push_str("+OK\r\n");
            }
        }
    }

    Ok(form)
}