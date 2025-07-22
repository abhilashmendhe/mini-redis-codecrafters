use std::{collections::VecDeque, net::SocketAddr, sync::Arc};

use tokio::sync::{oneshot::{self, Sender}, Mutex};

use crate::{basics::{ all_types::SharedMapT, kv_ds::Value}, errors::RedisErrors};

pub async fn fetch_list(
    listkey: &String, 
    kv_map: SharedMapT
) -> VecDeque<String> {
    let kv_map_gaurd = kv_map.lock().await;
    let list_items = match kv_map_gaurd.get(listkey) {
        Some(value_struct) => {
            match &value_struct.value {
                Value::LIST(items) => items.to_owned(),
                _ => todo!(),
            }
        },
        None => {
            VecDeque::new()
        },
    }; 
    list_items
}

pub async fn compute_index(
    index: &String,
    list_len: i64
) -> Result<i64, RedisErrors> {

    let in_list_len = list_len as i64;
        // let v = ((cmds[3].parse::<i64>()? % in_list_len) + in_list_len) % in_list_len;
        let v = index.parse::<i64>()?;
        let v = {
            if v < 0 {
                in_list_len + v
            } else {
                v
            }
        };
        if v < 0 {
            Ok(0)
        } else {
            Ok(v)
        }
}

pub async fn blpop_ops(
    listkey: &String,
    sock_addr: SocketAddr,
    kv_map: SharedMapT,
    blpop_clients_queue: Arc<Mutex<VecDeque<(SocketAddr,oneshot::Sender<(String,SocketAddr)>)>>>,
    tx: Sender<(String, SocketAddr)>
) -> String {
    
    // Step 1:
    let mut form = String::new();
    let mut kv_map_gaurd = kv_map.lock().await;
    if let Some(value_struct) = kv_map_gaurd.get_mut(listkey) {
        // value_struct
        if let Value::LIST(items) = value_struct.mut_value() {
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
                    blpop_clients_queue.lock().await.push_back((sock_addr, tx));
                } else {
                    blpop_clients_queue.lock().await.push_back((sock_addr, tx));
                }
        }
    } else {
        blpop_clients_queue.lock().await.push_back((sock_addr, tx));
    }
    
    form
}