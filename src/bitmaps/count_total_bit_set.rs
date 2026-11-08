use crate::{
    basics::{all_types::SharedMapT, kv_ds::Value},
    errors::RedisErrors,
};

pub async fn count_total_set_bit(
    key: &str,
    l: Option<usize>,
    r: Option<usize>,
    kv: SharedMapT,
) -> Result<String, RedisErrors> {
    let kv_gaurd = { kv.lock().await };
    let vl = if let Some(vs) = kv_gaurd.get(key) {
        // vs.value_len()
        if let Value::STRING_BITMAP(str_bitm) = vs.value() {
            str_bitm.get_bit_counts(l, r)
        } else {
            0
        }
    } else {
        0
    };
    Ok(format!(":{}\r\n", vl))
}
