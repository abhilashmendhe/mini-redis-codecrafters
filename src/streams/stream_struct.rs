use std::{collections::VecDeque, fmt::Display};

use crate::{errors::RedisErrors, get_current_unix_time};

#[derive(Debug, Clone)]
#[allow(unused)]
pub enum StreamValue{
    String(String),
    NumInt(i64),
    NumFloat(f64)
}

#[derive(Debug, Clone)]
#[allow(unused)]
pub struct StreamStruct {
    pub epoch: u128,
    pub seq_number: usize,
    pub pairs_values: VecDeque<(String, StreamValue)>
}

impl StreamStruct {
    pub fn new(epoch: u128, seq_number: usize, pairs_values: VecDeque<(String, StreamValue)>) -> Self {
        Self { epoch, seq_number, pairs_values }
    }
}

impl Display for StreamStruct {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {

        let mut form = String::new();

        let size_id_paris = "*2\r\n";
        form.push_str(&size_id_paris);

        let id = format!("{}-{}", self.epoch, self.seq_number);
        let form_id = format!("${}\r\n{}\r\n",id.len(), id);
        form.push_str(&form_id);

        let pair_size = self.pairs_values.len() * 2;
        let form_pair_size = format!("*{}\r\n", pair_size);
        form.push_str(&form_pair_size);

        let mut pairs = String::new();
        for (key, value) in &self.pairs_values {
            // pairs.push_str(string);
            // let key = pv.0
            let form_key = format!("${}\r\n{}\r\n", key.len(), key);
            let value = match value {
                StreamValue::String(s) => s,
                StreamValue::NumInt(ni) => &ni.to_string(),
                StreamValue::NumFloat(nf) => &nf.to_string(),
            };
            let form_value = format!("${}\r\n{}\r\n",value.len(), value);
            pairs.push_str(&form_key);
            pairs.push_str(&form_value);
        }
        form.push_str(&pairs);
        write!(f, "{}", form)
    }
}

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