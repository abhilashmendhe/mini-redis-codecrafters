use crate::{
    basics::{
        all_types::SharedMapT,
        kv_ds::{Value, ValueStruct},
        string_and_bitmap::StringAndBitmap,
    },
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

    // 1. insert into bitmap
    let output_bit = if let Some(value) = kv_ds_gaurd.get_mut(key) {
        if let Value::STRING_BITMAP(str_bitm) = value.mut_value() {
            let bm = str_bitm.mut_bitmap();
            let ret_bit_val = bm.set_offset_bit(offset, bit_value);
            let oss = bm
                .offsets
                .iter()
                .map(|v| v.reverse_bits())
                .take(bm.max_ind + 1)
                .collect::<Vec<u8>>();
            str_bitm.set_value(String::from_utf8_lossy(&oss).as_ref());
            ret_bit_val.to_string()
        } else {
            "0".to_string()
        }
    } else {
        // Create a new key and insert the value...
        let mut str_bitm = StringAndBitmap::default();
        let bitmap = str_bitm.mut_bitmap();
        bitmap.set_offset_bit(offset, bit_value);
        let oss = bitmap
            .offsets
            .iter()
            .map(|v| v.reverse_bits())
            .take(bitmap.max_ind + 1)
            .collect::<Vec<u8>>();
        str_bitm.set_value(String::from_utf8_lossy(&oss).as_ref());
        kv_ds_gaurd.insert(
            key.to_string(),
            ValueStruct::new(Value::STRING_BITMAP(str_bitm), px, pxat),
        );
        "0".to_string()
    };
    Ok(format!(":{}\r\n", output_bit))
}
