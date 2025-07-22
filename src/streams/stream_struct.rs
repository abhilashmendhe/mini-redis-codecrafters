use std::{collections::VecDeque, fmt::Display};

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
