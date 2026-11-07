use crate::{
    basics::{all_types::SharedMapT, kv_ds::Value},
    errors::RedisErrors,
};

pub async fn get_bit(key: &str, offset: usize, kv_ds: SharedMapT) -> Result<String, RedisErrors> {
    let kv_ds_gaurd = { kv_ds.lock().await };
    let output_bit = if let Some(value) = kv_ds_gaurd.get(key) {
        if let Value::STRING_BITMAP(str_bitm) = value.value() {
            let bm = str_bitm.bitmap();
            let ret_bit_val = bm.get_offset_bit(offset);
            ret_bit_val.to_string()
        } else {
            "0".to_string()
        }
    } else {
        "0".to_string()
    };
    Ok(format!(":{}\r\n", output_bit))
}
