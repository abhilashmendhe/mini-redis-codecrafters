use std::collections::LinkedList;

use crate::{basics::{ all_types::SharedMapT, basic_ops::insert, kv_ds::{Value, ValueStruct}}, errors::RedisErrors, streams::stream_struct::{extract_stream_data, extract_stream_id, StreamStruct}};

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
    
    let mut form = String::new();
    let (epoch, seq_num) = extract_stream_id(stream_id).await?;
    if epoch == 0 && seq_num == 0 {
        return Ok(String::from("-ERR The ID specified in XADD must be greater than 0-0\r\n"));
    }
    
    let pair_values = extract_stream_data(pair_values_vec).await;

    let mut sid = String::new();
    {
        let mut kv_map_gaurd = kv_map.lock().await;
        if let Some(value_struct) = kv_map_gaurd.get_mut(&stream_key) {
            
            if let Value::STREAM(stream_list) = value_struct.mut_value() {
                if let Some(stream_struct) = stream_list.back() {
                    let last_epoch = stream_struct.epoch;
                    let last_seq_num = stream_struct.seq_number;
                    
                    if epoch < last_epoch {
                        return Ok(String::from("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"));
                    }

                    let new_seq_num = {
                        if epoch == last_epoch { 
                            if seq_num < 0 { last_seq_num + 1 } else { seq_num as usize }
                        } else { 0 }
                    }; 
                    
                    if epoch == last_epoch {
                        if new_seq_num <= last_seq_num {
                            return Ok(String::from("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"));
                        }
                    }
                    let stream_struct = StreamStruct::new(epoch, new_seq_num, pair_values);
                    stream_list.push_back(stream_struct);
                    sid = format!("{}-{}", epoch, new_seq_num);
                }
            }

        } else {
            std::mem::drop(kv_map_gaurd);
            let mut stream_list = LinkedList::new();
            let new_seq_num = if seq_num < 0 { 0 as usize } else { seq_num as usize };
            let new_seq_num = if epoch == 0 { 1 } else { new_seq_num };

            let stream_struct = StreamStruct::new(epoch, new_seq_num, pair_values);
            stream_list.push_back(stream_struct);
            sid = format!("{}-{}", epoch, new_seq_num);
            let stream_value = Value::STREAM(stream_list);
            let value_struct = ValueStruct::new(stream_value, None, None);
            insert(stream_key, value_struct, kv_map.clone()).await;    
        }
    }
    form.push('$');
    form.push_str(&sid.len().to_string());
    form.push_str("\r\n");
    form.push_str(&sid);
    form.push_str("\r\n");
    Ok(form)
}


pub async fn xrange(
    stream_key: String,
    start_id: String,
    end_id: String,
    kv_map: SharedMapT
) -> Result<String, RedisErrors> {

    let (s_epoch, s_seq_num) = extract_stream_id(start_id).await?;
    let (e_epoch, e_seq_num) = extract_stream_id(end_id).await?;
    let mut form = String::new();
    {
        let kv_map_gaurd = kv_map.lock().await;
        if let Some(vs) = kv_map_gaurd.get(&stream_key) {
            if let Value::STREAM(stream_list) = vs.value() {
                let mut count = 0;
                let mut temp_form = String::new();
                for ss in stream_list {
                    
                    if s_epoch <= ss.epoch && e_epoch >= ss.epoch {
                        if s_seq_num == -1 && e_seq_num == -1 {
                            // println!("{}", ss);
                            count += 1;
                            temp_form.push_str(&ss.to_string());
                        } else {
                            if s_seq_num as usize <= ss.seq_number && e_seq_num as usize >= ss.seq_number {
                                // println!("Check seq_num: {}",ss);
                                count += 1;
                                temp_form.push_str(&ss.to_string());
                            }
                        }
                    }    
                }
                form.push_str(&format!("*{}\r\n",count));
                form.push_str(&temp_form);
            }
        }
    }
    println!("form:\n{}",form);
    Ok(form)
}