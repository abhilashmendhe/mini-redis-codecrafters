use crate::{basics::all_types::SharedMapT, errors::RedisErrors};

pub async fn get_str_len(key: &str, kv: SharedMapT) -> Result<String, RedisErrors> {

    let kv_gaurd = { kv.lock().await };
    let vl = if let Some(vs) = kv_gaurd.get(key) {
        vs.value_len()
    } else {
        0
    };
    Ok(format!(":{}\r\n", vl))
}