use std::collections::VecDeque;

use crate::{errors::RedisErrors, redis_key_value_struct::{SharedMapT, Value}};

pub async fn fetch_list(
    listkey: &String, 
    kv_map: SharedMapT
) -> VecDeque<String> {
    let kv_map_gaurd = kv_map.lock().await;
    let list_items = match kv_map_gaurd.get(listkey) {
        Some(value_struct) => {
            match &value_struct.value {
                Value::STRING(_) => todo!(),
                Value::NUMBER(_) => todo!(),
                Value::LIST(items) => items.to_owned(),
                Value::STREAM(_) => todo!(),
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