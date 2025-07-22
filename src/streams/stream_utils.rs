use std::{collections::VecDeque};

use crate::{basics::{all_types::SharedMapT, kv_ds::Value}, errors::RedisErrors, get_current_unix_time, streams::stream_struct::StreamValue};

pub async fn get_stream_value(s: &str) -> StreamValue {
    // match stream
    if let Ok(int_val) = s.parse::<i64>() {
        StreamValue::NumInt(int_val)
    } else if let Ok(float_val) = s.parse::<f64>() {
        StreamValue::NumFloat(float_val)
    } else {
        StreamValue::String(s.to_string())
    }
}

pub async fn extract_stream_data(
    stream_data: Vec<String>
) -> VecDeque<(String, StreamValue)> {
    
    let mut i = 0;
    let mut pairs_values = VecDeque::new();

    while i < stream_data.len() {
        let k = stream_data[i].to_string();
        let v = get_stream_value(&stream_data[i+1]).await;
        pairs_values.push_back((k, v));
        i += 2;
    }
    pairs_values
}

pub async fn extract_stream_id(
    stream_id: String
) -> Result<(u128, isize), RedisErrors> {
    
    if stream_id.eq("*") {
        let epoch = get_current_unix_time().await?;
        Ok((epoch, 0))
    } else if stream_id.eq("-") {
        Ok((0, 0))
    } else if stream_id.eq("+") {
        Ok((0, -5))
    } else {

        let mut id_spl = stream_id.split("-");
        
        let epoch = if let Some(unix_time) = id_spl.next() {
            unix_time.parse::<u128>()?
        } else { 0 };

        let seq_number = 
            if let Some(seq_num) = id_spl.next() {
                if seq_num.eq("*") {
                    -1
                } else {
                    seq_num.parse::<isize>()?
                }
            } else { -1 };
        
        Ok((epoch, seq_number))
    }
}

pub async fn extract_stream_read_data_block(
    streams_ids: Vec<String>,
    kv_map: SharedMapT,
    // tx: Arc<>
) -> Result<Option<String>, RedisErrors> {

    let mut form = String::new();
    let mut ind = 0;
    let mid = streams_ids.len() / 2;
    // println!("searching for stream_read_data_block");
    // println!("{:?}",streams_ids);
    let mut temp1 = String::new();
    temp1.push_str(&format!("*{}\r\n",mid));
    let mut found_data = false;
    while ind < mid {
        let stream_key = &streams_ids[ind];
        let stream_id = &streams_ids[ind+mid];

        let (epoch, seq_num) = extract_stream_id(stream_id.to_string()).await?;
        
        {
            let kv_map_gaurd = kv_map.lock().await;
            if let Some(vs) = kv_map_gaurd.get(stream_key) {

                let mut c2 = 0;
                let mut temp2 = String::new();
                temp1.push_str("*2\r\n");
                temp1.push_str(&format!("${}\r\n{}\r\n",stream_key.len().to_string(), stream_key));
                if let Value::STREAM(stream_list) = vs.value() {
                    
                    // println!("List is present!");
                    for ss in stream_list {
                        if (ss.epoch == epoch && ss.seq_number > seq_num as usize) || (ss.epoch > epoch) {
                            c2 += 1;
                            temp2.push_str(&ss.to_string());
                            // return Ok(Some("+FOUNDATA\r\n".to_string()));
                        } 
                    }

                }
                if c2 > 0 {
                    found_data = true;
                    temp1.push_str(&format!("*{}\r\n", c2));
                    temp1.push_str(&temp2);
                }
                
            } else {
                return Ok(None);
            }
        }
        ind += 1;
    }
    if found_data {
        form.push_str(&temp1);
        return Ok(Some(form));
    } else {
        form.push_str("$-1\r\n");
        return Ok(None);
    }
    // println!("returning Ok(None)");
    // Ok(None)
    // Ok(Some(form))
}

pub async fn extract_stream_read_data(
    streams_ids: Vec<String>,
    kv_map: SharedMapT
) -> Result<String, RedisErrors> {

    let mut form = String::new();
    let st_ids_size = streams_ids.len();
    let mut ind = 0;
    let mid = st_ids_size / 2;

    let mut temp1 = String::new();
    
    temp1.push_str(&format!("*{}\r\n",mid));
    
    while ind < mid {
        
        let stream_key = &streams_ids[ind];
        let stream_id = &streams_ids[ind+mid];
        // println!("stream_key: {}, stream_id: {}", stream_key, stream_id);
        let (epoch, seq_num) = extract_stream_id(stream_id.to_string()).await?;
        
        {
            let kv_map_gaurd = kv_map.lock().await;
            if let Some(vs) = kv_map_gaurd.get(stream_key) {
                // println!("Found: {}",stream_key);
                let mut c2 = 0;
                let mut temp2 = String::new();
                temp1.push_str("*2\r\n");
                temp1.push_str(&format!("${}\r\n{}\r\n",stream_key.len().to_string(), stream_key));
                if let Value::STREAM(stream_list) = vs.value() {
                    for ss in stream_list {
                        
                        if epoch == ss.epoch {
                            if ss.seq_number > seq_num as usize {
                                // println!("{}",ss);
                                c2 += 1;
                                temp2.push_str(&ss.to_string());
                            }
                        } else if ss.epoch > epoch {
                            // println!("{}",ss);
                            c2 += 1;
                            temp2.push_str(&ss.to_string());
                        }
                    }
                }
                
                temp1.push_str(&format!("*{}\r\n", c2));
                temp1.push_str(&temp2);
            } else {
                temp1.push_str("$-1\r\n");
            }
        }
        // println!();
        ind += 1;
    }
    if temp1.contains("*0\r\n") || temp1.contains("$-1\r\n") {
        form.push_str("$-1\r\n");
    } else {
        form.push_str(&temp1);
    }
    Ok(form)
}
