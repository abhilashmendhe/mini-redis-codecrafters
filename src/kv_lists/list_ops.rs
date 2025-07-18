use std::{collections::VecDeque, net::SocketAddr, sync::{Arc}};

use tokio::sync::{oneshot, Mutex};

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
    blpop_clients_queue: Arc<Mutex<VecDeque<(SocketAddr,oneshot::Sender<(String,SocketAddr)>)>>>
) -> Result<(), RedisErrors> {
    let push_side = &cmds[0];
    let listkey = &cmds[1];
    let mut list_len = 0;
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
                        list_len += 1;
                    }
                    if let Some((sock_addr, waiter)) = blpop_clients_queue.lock().await.pop_front() {
                        if let Some(item) = list_items.pop_front() {
                            let _ = waiter.send((item, sock_addr));
                        }
                    }
                    list_len = {
                        if list_len > list_items.len() {
                            list_len
                        } else {
                            list_items.len()
                        }
                    };
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
                list_len += 1;
            }
            if let Some((sock_addr, waiter)) = blpop_clients_queue.lock().await.pop_front() {
                if let Some(item) = list_items.pop_front() {
                    let _ = waiter.send((item,sock_addr));
                }
            }
            list_len = {
                if list_len > list_items.len() {
                    list_len
                } else {
                    list_items.len()
                }
            };
            // println!("{:?}",list_items);
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
    insert(listkey.to_string(), value_struct, kv_map.clone()).await;
    // let new_list_len = {
    //     if list_len > 
    // }
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
    let start = compute_index(&cmds[2], list_len as i64).await?;
    let stop = compute_index(&cmds[3], list_len as i64).await?;

    // println!("Start : {}, and end : {}", start, stop);
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
                // println!("list_items[{}]={}",ind, item);
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

pub async fn lpop(
    cmds: &Vec<String>,
    sock_addr: SocketAddr,
    kv_map: SharedMapT,
    connections: SharedConnectionHashMapT
) -> Result<(), RedisErrors> {

    let listkey = &cmds[1];
    let mut num_items = 0;
    let cmds_len = cmds.len();
    if cmds_len > 2 {
        num_items += cmds[2].parse::<usize>()?;
    }
    let mut form = String::new();
    {
        let mut kv_map_gaurd = kv_map.lock().await;
        match kv_map_gaurd.get_mut(listkey) {
            Some(value_struct) => {
                match value_struct.mut_value() {
                    Value::STRING(_) => todo!(),
                    Value::NUMBER(_) => todo!(),
                    Value::LIST(items) => {
                        if cmds_len > 2 {
                            let mut nform = String::new();
                            let mut count = 0;
                            while num_items > 0 {
                                if items.len() <= 0 {
                                    break;
                                }
                                if let Some(k) = items.pop_front() {
                                    nform.push('$');
                                    nform.push_str(&k.len().to_string());
                                    nform.push_str("\r\n");
                                    nform.push_str(&k);
                                    nform.push_str("\r\n");   
                                }
                                count += 1;
                                num_items -= 1;
                            }
                            form.push('*');
                            form.push_str(&count.to_string());
                            form.push_str("\r\n");
                            form.push_str(&nform);
                        } else {
                            if let Some(k) = items.pop_front() {
                                form.push('$');
                                form.push_str(&k.len().to_string());
                                form.push_str("\r\n");
                                form.push_str(&k);
                                form.push_str("\r\n");   
                            } else {
                                form.push_str(":0\r\n");
                            }
                        }
                    },
                    Value::STREAM(_) => todo!(),
                }
            },
            None => {
                form.push_str(":0\r\n");
            },
        }
    }
    if let Some((client_tx, _flag)) = connections.lock().await.get(&sock_addr.port()) {
        client_tx.send((sock_addr, form.as_bytes().to_vec()))?;
    }
    Ok(())    
}

pub async fn blpop(
    cmds: &Vec<String>,
    sock_addr: SocketAddr,
    kv_map: SharedMapT,
    connections: SharedConnectionHashMapT,
    blpop_clients_queue: Arc<Mutex<VecDeque<(SocketAddr,oneshot::Sender<(String,SocketAddr)>)>>>
) -> Result<(), RedisErrors> {

    println!("In BLPOP");
    let listkey = cmds[1].to_string();
    let _seconds = cmds[2].parse::<u64>()?;
    
    let mut form = String::from("");
    
    let (tx, rx) = tokio::sync::oneshot::channel();
    let blpop_clients_queue2 = Arc::clone(&blpop_clients_queue);
    
    {
        let mut kv_map_gaurd = kv_map.lock().await;
        if let Some(value_struct) = kv_map_gaurd.get_mut(&listkey) {
            // value_struct
            match value_struct.mut_value() {
                Value::STRING(_) => {},
                Value::NUMBER(_) => {},
                Value::LIST(items) => {
                    // println!("Items: {:?}", items);
                    if let Some(item) = items.pop_front() {
                        // let _ = waiter.send(item);
                        form.push_str("*2\r\n");
                        form.push('$');
                        form.push_str(&listkey.len().to_string());
                        form.push_str("\r\n");
                        form.push_str(&listkey);
                        form.push_str("\r\n");
                        form.push('$');
                        form.push_str(&item.len().to_string());
                        form.push_str("\r\n");
                        form.push_str(&item);
                        form.push_str("\r\n");
                        blpop_clients_queue2.lock().await.push_back((sock_addr, tx));
                    } else {
                        blpop_clients_queue2.lock().await.push_back((sock_addr, tx));
                    }
                },
                Value::STREAM(_) => {},
            }
        } else {
            blpop_clients_queue2.lock().await.push_back((sock_addr, tx));
        }
    }
    {
        println!("Before wait: {:?}",blpop_clients_queue2.lock().await);
    }
    println!("Before wait!");
    if form.len() <=0 {
            
        println!("Wating for the poppped elem"); 
        match rx.await {
            Ok((item, sock_addr)) => {
                
                form.push_str("*2\r\n");
                form.push('$');
                form.push_str(&listkey.len().to_string());
                form.push_str("\r\n");
                form.push_str(&listkey);
                form.push_str("\r\n");
                form.push('$');
                form.push_str(&item.len().to_string());
                form.push_str("\r\n");
                form.push_str(&item);
                form.push_str("\r\n");
                if let Some((client_tx, _flag)) = connections.lock().await.get(&sock_addr.port()) {
                // println!("{}", form);
                    client_tx.send((sock_addr, form.as_bytes().to_vec()))?;
                }
                form.clear();
                form.push_str("$-1\r\n");
            },
            Err(e) => {
                println!("recverr: {}",  e);
                form.push_str("$-1\r\n");
            }, // Sender dropped
        }
    } else 
    // {
    //     println!("After wait: {:?}",blpop_clients_queue2.lock().await);
    // }
    // println!("After wait!");
    {
        let mut blpop_clients_queue2_gaurd = blpop_clients_queue2.lock().await;
        if let Some((sock_addr, _)) = blpop_clients_queue2_gaurd.pop_front() {
            if let Some((client_tx, _flag)) = connections.lock().await.get(&sock_addr.port()) {
                println!("{}", form);
                client_tx.send((sock_addr, form.as_bytes().to_vec()))?;
            }
        }
    }
    Ok(())
}