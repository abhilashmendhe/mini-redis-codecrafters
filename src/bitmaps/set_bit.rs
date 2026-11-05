use crate::{basics::{all_types::SharedMapT, kv_ds::Value}, errors::RedisErrors};

pub async fn set_bit(
    key: &str,
    offset: u64,
    bit_value: u8,
    kv_ds: SharedMapT,
) -> Result<String, RedisErrors> {
    let send_output = String::from(":0\r\n");

    let mut kv_ds_gaurd = { kv_ds.lock().await };
    if let Some(value) = kv_ds_gaurd.get(key) {
        if let Value::BITMAP(bm) = &value.value() {

        }
    } else {
        // Create a new key and insert the value...
        
    }
    Ok(send_output)
}
