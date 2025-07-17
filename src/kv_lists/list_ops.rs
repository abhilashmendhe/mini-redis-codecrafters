use std::{collections::VecDeque, net::SocketAddr};

use crate::{
    connection_handling::SharedConnectionHashMapT, 
    errors::RedisErrors, 
    kv_lists::list_ops_utils::{compute_index, fetch_list}, 
    redis_key_value_struct::{insert, SharedMapT, Value, ValueStruct}
};


pub async fn push(
    cmds: &Vec<String>,
    sock_addr: SocketAddr,
    kv_map: SharedMapT,
    connections: SharedConnectionHashMapT,
) -> Result<(), RedisErrors> {
    let push_side = &cmds[0];
    let listkey = &cmds[1];
    let value_struct = {
        let mut kv_map_gaurd = kv_map.lock().await;

        let value_struct = match kv_map_gaurd.get_mut(listkey) {
            Some(value_struct) => {
                match &mut value_struct.value {
                    Value::STRING(_) => todo!(),
                    Value::NUMBER(_) => todo!(),
                    Value::LIST(list_items) => {
                        for item in &cmds[2..] {
                            if push_side.eq("RPUSH") {
                                list_items.push_back(item.to_string());
                            } else if push_side.eq("LPUSH") {
                                list_items.push_front(item.to_string());
                            }
                        }
                    },
                    Value::STREAM(_) => todo!(),
                }
                value_struct.to_owned()
            },
            None => {
                let mut list_items = VecDeque::new();
                
                // items.push_back("haha".to_string());
                for item in &cmds[2..] {
                    if push_side.eq("RPUSH") {
                        list_items.push_back(item.to_string());
                    } else if push_side.eq("LPUSH") {
                        list_items.push_front(item.to_string());
                    }
                }
                println!("{:?}",list_items);
                let value_struct = ValueStruct::new(
                    Value::LIST(list_items),
                    None, 
                    None, 
                );
                // if cmds.len() == 5 {
                //     let px = cmds[4].parse::<u128>()?;
                //     let now = SystemTime::now()
                //             .duration_since(UNIX_EPOCH)?;
                //     let now_ms = now.as_millis() + px as u128;
                //     value_struct.set_px(Some(px));
                //     value_struct.set_pxat(Some(now_ms));
                // }
                value_struct
            },
        };
        value_struct
    };
    let list_len = value_struct.value_len();
    insert(listkey.to_string(), value_struct, kv_map.clone()).await;
    println!("done inserting");
    if let Some((client_tx, _flag)) = connections.lock().await.get(&sock_addr.port()) {
        let form = format!(":{}\r\n",list_len);
        client_tx.send((sock_addr, form.as_bytes().to_vec()))?;
    }
    Ok(())
}


pub async fn lrange(
    cmds: &Vec<String>,
    sock_addr: SocketAddr,
    kv_map: SharedMapT,
    connections: SharedConnectionHashMapT
) -> Result<(), RedisErrors> {
    let listkey = &cmds[1];
    let list_items = fetch_list(&listkey, kv_map).await;
    // println!("List itesm -> {:?}",&list_items);

    let list_len = list_items.len();
    let start = compute_index(&cmds, list_len as i64).await?;
    let stop = compute_index(&cmds, list_len as i64).await?;

    println!("Start : {}, and end : {}", start, stop);
    let mut form = String::new();
    if start >= list_items.len() as i64 || start > stop {
        form.push_str(&format!("*0\r\n"));
    } else {
        let stop = {
            if stop > list_items.len() as i64 {
                list_items.len() as  i64
            } else {
                stop
            }
        };
        let mut repl_str = String::new();
        let mut count = 0;
        for (ind, item) in list_items.iter().enumerate() {
            if ind as i64 >= start && ind as i64 <= stop {
                println!("list_items[{}]={}",ind, item);
                repl_str.push('$');
                repl_str.push_str(&item.len().to_string());
                repl_str.push_str("\r\n");
                repl_str.push_str(item);
                repl_str.push_str("\r\n");
                count += 1;
            }
        }
        form.push_str(&format!("*{}\r\n{}", count, repl_str));
    }
    // println!("{}",form);
    if let Some((client_tx, _flag)) = connections.lock().await.get(&sock_addr.port()) {
        client_tx.send((sock_addr, form.as_bytes().to_vec()))?;
    }
    Ok(())
}

pub async fn llen(
    cmds: &Vec<String>,
    sock_addr: SocketAddr,
    kv_map: SharedMapT,
    connections: SharedConnectionHashMapT
) -> Result<(), RedisErrors> {
    let listkey = &cmds[1];
    let list_items = fetch_list(listkey, kv_map).await;
    let list_len = list_items.len();
    let form = format!(":{}\r\n",list_len);
    if let Some((client_tx, _flag)) = connections.lock().await.get(&sock_addr.port()) {
        client_tx.send((sock_addr, form.as_bytes().to_vec()))?;
    }
    Ok(())
}