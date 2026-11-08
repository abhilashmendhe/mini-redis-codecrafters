use std::sync::Arc;

use tokio::sync::Mutex;

use crate::{
    basics::{
        all_types::SharedMapT,
        basic_ops::set,
        kv_ds::{Value, ValueStruct},
        string_and_bitmap::StringAndBitmap,
    },
    errors::RedisErrors,
    rdb_persistence::rdb_persist::RDB,
};

pub async fn bitops_and(
    new_key: &str,
    k1: &str,
    k2: &str,
    kv: SharedMapT,
    rdb: Option<Arc<Mutex<RDB>>>,
) -> Result<String, RedisErrors> {
    let mut kv_gaurd = { kv.lock().await };

    let vs1 = kv_gaurd.get(k1);
    let vs2 = kv_gaurd.get(k2);

    if vs1.is_none() && vs2.is_none() {
        return Ok(":0\r\n".to_string());
    } else if vs1.is_none() || vs2.is_none() {
        set(
            new_key.to_string(),
            "\x00".to_string(),
            None,
            kv.clone(),
            rdb,
        )
        .await?;
        return Ok(":1\r\n".to_string());
    }

    // now unwrap here
    let vs1 = vs1.unwrap();
    let vs2 = vs2.unwrap();
    let mut count = 0;
    if let Value::STRING_BITMAP(str_bit1) = vs1.value() {
        if let Value::STRING_BITMAP(str_bit2) = vs2.value() {
            let bitmap1 = str_bit1.bitmap();
            let bitmap2 = str_bit2.bitmap();
            let (new_value, new_bitmap, c) = bitmap1.bitop_and(&bitmap2);
            let mut sb = StringAndBitmap::default();
            sb.set_bitmap(new_bitmap);
            sb.set_value(&new_value);
            kv_gaurd.insert(
                new_key.to_string(),
                ValueStruct::new(Value::STRING_BITMAP(sb), None, None),
            );
            count = c;
        }
    }
    // Ok(":1\r\n".to_string())
    Ok(format!(":{}\r\n", count))
}

pub async fn bitops_or(
    new_key: &str,
    k1: &str,
    k2: &str,
    kv: SharedMapT,
    rdb: Option<Arc<Mutex<RDB>>>,
) -> Result<String, RedisErrors> {
    let mut kv_gaurd = { kv.lock().await };

    let vs1 = kv_gaurd.get(k1);
    let vs2 = kv_gaurd.get(k2);

    if vs1.is_none() && vs2.is_none() {
        return Ok(":0\r\n".to_string());
    } else if vs1.is_none() {
        if let Value::STRING_BITMAP(str_bit) = vs2.unwrap().value() {
            set(new_key.to_string(), str_bit.value(), None, kv.clone(), rdb).await?;
            return Ok(":1\r\n".to_string());
        }
    } else if vs2.is_none() {
        if let Value::STRING_BITMAP(str_bit) = vs1.unwrap().value() {
            set(new_key.to_string(), str_bit.value(), None, kv.clone(), rdb).await?;
            return Ok(":1\r\n".to_string());
        }
    }

    // now unwrap here
    let vs1 = vs1.unwrap();
    let vs2 = vs2.unwrap();
    let mut count = 0;
    if let Value::STRING_BITMAP(str_bit1) = vs1.value() {
        if let Value::STRING_BITMAP(str_bit2) = vs2.value() {
            let bitmap1 = str_bit1.bitmap();
            let bitmap2 = str_bit2.bitmap();
            let (new_value, new_bitmap, c) = bitmap1.bitop_or(&bitmap2);
            let mut sb = StringAndBitmap::default();
            sb.set_bitmap(new_bitmap);
            sb.set_value(&new_value);
            kv_gaurd.insert(
                new_key.to_string(),
                ValueStruct::new(Value::STRING_BITMAP(sb), None, None),
            );
            count = c;
        }
    }
    Ok(format!(":{}\r\n", count))
}
