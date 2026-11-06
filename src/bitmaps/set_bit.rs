use crate::{
    basics::{
        all_types::SharedMapT,
        kv_ds::{Value, ValueStruct},
    },
    bitmaps::bm_struct::BitMap,
    errors::RedisErrors,
};

pub async fn set_bit(
    key: &str,
    offset: usize,
    bit_value: u8,
    kv_ds: SharedMapT,
    px: Option<u128>,
    pxat: Option<u128>,
) -> Result<String, RedisErrors> {
    let mut kv_ds_gaurd = { kv_ds.lock().await };
    let output_bit = if let Some(value) = kv_ds_gaurd.get_mut(key) {
        if let Value::BITMAP(bm) = value.mut_value() {
            let ret_bit_val = bm.set_offset_bit(offset, bit_value);
            ret_bit_val.to_string()
        } else {
            "0".to_string()
        }
    } else {
        // Create a new key and insert the value...
        let mut bitmap = BitMap::new();
        bitmap.set_offset_bit(offset, bit_value);
        kv_ds_gaurd.insert(
            key.to_string(),
            ValueStruct::new(Value::BITMAP(bitmap), px, pxat),
        );
        "0".to_string()
    };
    Ok(format!(":{}\r\n", output_bit))
}
