use std::{collections::VecDeque, net::SocketAddr};

use crate::{
    basics::all_types::SharedMapT, connection_handling::SharedConnectionHashMapT, errors::RedisErrors, optimistic_lock::unwatch::unwatch, transactions::commands::{CommandTransactions, handle_transaction_commands}
};

pub async fn exec_multi(
    sock_addr: SocketAddr,
    kv_map: SharedMapT,
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

                // call func `check_commmand_trans`
                match check_handle_transactons(commands_transac, kv_map.clone()).await {
                    Ok(fo) => {
                        form.push_str(&fo);
                    }
                    Err(_err) => {
                        // println!("{:?}",_err);
                        commands_transac.clear();
                        // let _value = unwatch(sock_addr, connections.clone(), kv_map.clone()).await?;
                        println!("Also need to clear watch");
                        return Ok("*-1\r\n".to_string());
                    }
                }
            }
        }
    }
    // println!("In exec multi");
    // println!("{}",form);
    Ok(form)
}

async fn check_handle_transactons(
    commands_transac: &mut VecDeque<CommandTransactions>,
    kv_map: SharedMapT,
) -> Result<String, RedisErrors> {
    let mut form = String::new();
    // 1. Create a stack to store the multi ops
    let mut command_stack = VecDeque::new();

    while let Some(command) = commands_transac.pop_front() {
        // 2. Extract command and check if key exists
        match command {
            CommandTransactions::Set { key, value, px } => {
                let kv_map_gaurd = kv_map.lock().await;
                if let Some(value) = kv_map_gaurd.get(&key) {
                    if value.clone().watchers().len() > 0 {
                        return Err(RedisErrors::WatcherPresent);
                    }
                }
                command_stack.push_front(CommandTransactions::Set { key, value, px });
            }
            CommandTransactions::Incr { key } => {
                let kv_map_gaurd = kv_map.lock().await;
                if let Some(value) = kv_map_gaurd.get(&key) {
                    if value.clone().watchers().len() > 0 {
                        return Err(RedisErrors::WatcherPresent);
                    }
                }
                command_stack.push_front(CommandTransactions::Incr { key });
            }
            CommandTransactions::WATCH => todo!(),
            CommandTransactions::UNWATCH => return Err(RedisErrors::WatcherPresent),
            _ => {}
        }
    }

    while let Some(command) = command_stack.pop_back() {
        let ret_str = handle_transaction_commands(command, kv_map.clone()).await?;
        // if ret_str.eq("-ABORT\r\n") {
        //     commands_transac.clear();
        //     // println!("Cleared commands trans. Aborted because of WATCH inside MULTI.");
        //     return Ok(ret_str);
        // }
        form.push_str(&ret_str);
    }
    Ok(form)
}
