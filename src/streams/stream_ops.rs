use std::collections::{LinkedList, VecDeque};

use crate::{basics::{ all_types::SharedMapT, basic_ops::insert, kv_ds::{Value, ValueStruct}}, errors::RedisErrors, streams::stream_struct::{get_stream_value, StreamStruct}};

pub async fn type_ops(
    key: String,
    kv_map: SharedMapT
) -> Result<String, RedisErrors> {
    
    let value_type_form = {
    
    if let Some(v) = kv_map.lock().await.get(&key) {
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

    Ok(value_type_form.to_string())
}

pub async fn xadd(
    stream_key: String,
    stream_id: String,
    pair_values_vec: Vec<String>,
    kv_map: SharedMapT
) -> Result<String, RedisErrors> {
    
    if stream_id.eq("0-0") {
        return Ok(String::from("-ERR The ID specified in XADD must be greater than 0-0\r\n"));
    }
    
    let form = format!("${}\r\n{}\r\n", stream_id.len(), stream_id);
    let mut i = 0;
    let mut pairs_values = VecDeque::new();

    while i < pair_values_vec.len() {
        let k = pair_values_vec[i].to_string();
        let v = get_stream_value(&pair_values_vec[i+1]).await;
        pairs_values.push_back((k, v));
        i += 2;
    }

    
    {
        let mut kv_map_gaurd = kv_map.lock().await;
        if let Some(value_struct) = kv_map_gaurd.get_mut(&stream_key) {
            
            if let Value::STREAM(stream_items) = value_struct.mut_value() {
                if let Some(stream_struct) = stream_items.back() {
                    // strea
                    let milli_sec_time = stream_struct.id.milli_sec_time;
                    let seq_number = stream_struct.id.seq_number;
                    
                    let new_stream_struct = StreamStruct::new(stream_id, pairs_values)?;
                    let new_milli_sec_time = new_stream_struct.id.milli_sec_time;
                    let new_seq_number = new_stream_struct.id.seq_number;

                    if new_milli_sec_time < milli_sec_time {
                        return Ok(String::from("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"));
                    } else {
                        if new_seq_number <= seq_number {
                            return Ok(String::from("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"));
                        }
                    }
                    stream_items.push_back(new_stream_struct);
                }
            }

        } else {
            std::mem::drop(kv_map_gaurd);
            let mut stream_linked_list = LinkedList::new();
            let stream_struct = StreamStruct::new(stream_id, pairs_values)?;
            stream_linked_list.push_back(stream_struct);
            let kv_value = Value::STREAM(stream_linked_list);
            let value_struct = ValueStruct::new(kv_value, None, None);
            // println!("{:?}",value_struct.clone());
            insert(stream_key, value_struct, kv_map.clone()).await;   
        }
    }
    Ok(form)
}