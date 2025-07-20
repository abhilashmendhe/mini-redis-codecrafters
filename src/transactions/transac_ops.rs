use std::net::SocketAddr;

use crate::{basics::{all_types::SharedMapT, basic_ops::insert, kv_ds::{Value, ValueStruct}}, connection_handling::SharedConnectionHashMapT, errors::RedisErrors, transactions::commands::handle_transaction_commands};

pub async fn incr_ops(
    key: String,
    kv_map: SharedMapT,
) -> Result<String, RedisErrors> {

    let mut form = String::new();
    let f = {
        if let Some(value_struct)  = kv_map.lock().await.get_mut(&key) {
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
        let value_struct = ValueStruct::new(
            // value.to_string(), 
            Value::NUMBER(1),
            None, 
            None, 
        );

        // if let Some(px) = px {
        //     let px = px.parse::<u128>()?;
        //     let now = SystemTime::now()
        //             .duration_since(UNIX_EPOCH)?;
        //     let now_ms = now.as_millis() + px as u128;
        //     value_struct.set_px(Some(px));
        //     value_struct.set_pxat(Some(now_ms));
        // }
        insert(key.to_string(), value_struct, kv_map).await;
        form.push(':');
        form.push('1');
        form.push_str("\r\n");
    }
    
    Ok(form)
}


pub async fn multi(
    sock_addr: SocketAddr,
    connections: SharedConnectionHashMapT
) -> Result<String, RedisErrors> {
    {
        let client_port = sock_addr.port();
        let mut connections_gaurd = connections.lock().await;
        if let Some(conn_struct) = connections_gaurd.get_mut(&client_port) {
            // conn_struct.command_trans.clone()
            let commands_trans_vec = conn_struct.mut_get_command_vec();
            // commands_trans_vec.push_back(key.to_string());
            commands_trans_vec.push_back(super::commands::CommandTransactions::Multi);
        }
    }
    
    let form = "+OK\r\n".to_string();
    Ok(form)
}

pub async fn exec_multi(
    sock_addr: SocketAddr,
    kv_map: SharedMapT,
    connections: SharedConnectionHashMapT
) -> Result<String, RedisErrors> {
    
    let mut form = String::new();
    {
        let client_port = sock_addr.port();
        let mut connections_gaurd = connections.lock().await;
        if let Some(conn_struct) = connections_gaurd.get_mut(&client_port) {
            let commands_transac = conn_struct.mut_get_command_vec();
            println!("Lenth of command vec: {}",commands_transac.len());
            println!("Command vec: {:?}", commands_transac);
            if commands_transac.len() == 0 {
                form.push_str("-ERR EXEC without MULTI\r\n");
            } else if commands_transac.len() == 1 {
                form.push_str("*0\r\n");
                commands_transac.clear();
            } else {

                let _ = commands_transac.pop_front(); // Remove `MULTI`
                let cmd_len = commands_transac.len();
                form.push('*');
                form.push_str(&cmd_len.to_string());
                form.push_str("\r\n");

                while let Some(command) = commands_transac.pop_front() {
                    
                    let ret_str = handle_transaction_commands(command, kv_map.clone()).await?;
                    form.push_str(&ret_str);
                }
                // form.push_str("+OK\r\n");
                // commands_transac.clear();
            }
        }
    }

    Ok(form)
}

pub async fn discard_multi(
    sock_addr: SocketAddr,
    connections: SharedConnectionHashMapT
) -> Result<String, RedisErrors> {
    
    let mut form = String::new();
    {
        let client_port = sock_addr.port();
        let mut connections_gaurd = connections.lock().await;
        if let Some(conn_struct) = connections_gaurd.get_mut(&client_port) {
            let commands_transac = conn_struct.mut_get_command_vec();
            println!("Lenth of command vec: {}",commands_transac.len());
            println!("Command vec: {:?}", commands_transac);
            if commands_transac.len() == 0 {
                form.push_str("-ERR EXEC without MULTI\r\n");
            } else {
                form.push_str("+OK\r\n");
                commands_transac.clear();
            }
        }
    }
    Ok(form)
}