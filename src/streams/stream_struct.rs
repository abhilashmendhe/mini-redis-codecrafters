use std::collections::VecDeque;

use crate::errors::RedisErrors;

#[derive(Debug, Clone)]
#[allow(unused)]
pub enum StreamValue{
    String(String),
    NumInt(i64),
    NumFloat(f64)
}

#[derive(Debug, Clone)]
#[allow(unused)]
pub struct StreamId {
    pub milli_sec_time: usize,
    pub seq_number: usize
}

#[derive(Debug, Clone)]
#[allow(unused)]
pub struct StreamStruct {
    pub id: StreamId,
    pub pairs_values: VecDeque<(String, StreamValue)>
}

impl StreamStruct {
    pub fn new(id: String, pairs_values: VecDeque<(String, StreamValue)>) -> Result<StreamStruct, RedisErrors> {
        let mut id_spl = id.split("-");
        let milli_sec_time = id_spl.next().unwrap().parse::<usize>()?;
        let seq_number = id_spl.next().unwrap().parse::<usize>()?;
        let id = StreamId { milli_sec_time, seq_number };
        Ok(Self {
            id,
            pairs_values
        })
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