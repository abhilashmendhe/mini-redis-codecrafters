use std::{collections::HashMap, sync::Arc, time::{Duration, SystemTime, UNIX_EPOCH}};

use tokio::sync::Mutex;

use crate::{basics::{all_types::SharedMapT, kv_ds::{Value, ValueStruct}}, errors::RedisErrors};


pub fn init_map() -> SharedMapT {
    Arc::new(Mutex::new(HashMap::new()))
}

pub async fn clean_map(map: SharedMapT) {
    loop {
        tokio::time::sleep(Duration::from_millis(1500)).await;
        let mut map_guard = map.lock().await;
        let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap();
        map_guard.retain(|_, v| {
            
            match v.pxat() {
                Some(expiry) => now.as_millis() <= expiry,
                None => true,
            }
        });
    }
}


pub async fn insert(key: String, value: ValueStruct, map: SharedMapT) {
    map.lock().await.insert(key, value);
}

pub async fn set(
    key: String,
    value: String,
    px: Option<String>,
    kv_map: SharedMapT
) -> Result<String, RedisErrors> {

    // println!("cmds vec len: {}",cmds.len());
    let value = match value.parse::<i64>() {
        Ok(num) => Value::NUMBER(num),
        Err(_) => Value::STRING(value.to_string()),
    };
    let mut value_struct = ValueStruct::new(
        // value.to_string(), 
        value,
        None, 
        None, 
    );

    if let Some(px) = px {
        let px = px.parse::<u128>()?;
        let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)?;
        let now_ms = now.as_millis() + px as u128;
        value_struct.set_px(Some(px));
        value_struct.set_pxat(Some(now_ms));
    }
    insert(key.to_string(), value_struct, kv_map.clone()).await;
    Ok("+OK\r\n".to_string())
}

pub async fn get(key: String, kv_map: SharedMapT) -> String {

    let mut form = String::new();
    let mut m_gaurd = kv_map.lock().await;
    let value = m_gaurd.get(&key).cloned();

    if let Some(value_struct) = value {
        let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap();

        if let Some(pxat) = value_struct.pxat() {
            if now.as_millis() > pxat {
                m_gaurd.remove(&key);
                return "$-1\r\n".to_string();
            }
        }
        // Some(value_struct)
        match value_struct.value() {
            Value::STRING(s) => {
                form.push('$');
                form.push_str(&s.len().to_string());
                form.push_str("\r\n");
                form.push_str(&s);
                form.push_str("\r\n");
            },
            Value::NUMBER(num) => {
                let num_str = num.to_string();
                let num_len = ((num as f64).log10().floor() + 1 as f64) as i64;
                form.push('$');
                form.push_str(&num_len.to_string());
                form.push_str("\r\n");
                form.push_str(&num_str);
                form.push_str("\r\n");
            },
            _ => {}
        }
        form
    } else {
        "$-1\r\n".to_string()
    }
}

pub async fn get_pattern_match_keys(pattern: String, kv_map: SharedMapT) -> String {

    let mut form = String::new();
    if pattern.eq("*") {
        let kv_map_gaurd = kv_map.lock().await;
        let mut count = 0;
        let mut key_str = String::new();
        for (key, _value_struct) in kv_map_gaurd.iter() {
            key_str.push_str(format!("${}\r\n{}\r\n",key.len(),key).as_str());
            count += 1;
        }
        form = format!("*{}\r\n",count);
        form.push_str(&key_str);
    }
    form
}