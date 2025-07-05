use std::{collections::HashMap, sync::Arc};

use tokio::sync::Mutex;

#[derive(Debug, Clone)]
pub struct ValueStruct {
    value: String,
    ex: usize,    // seconds
    px: usize,    // milliseconds
    exat: usize,  // timestamp-seconds
    pxat: usize   // timestamp-milliseconds
}

impl ValueStruct {
    pub fn new(value: String, ex: usize, px: usize, exat: usize, pxat: usize) -> Self {
        ValueStruct { 
            value, 
            ex, 
            px, 
            exat, pxat
        }
    }
    pub fn value(&self) -> String {
        self.value.clone()
    }
}

pub type SharedMapT = Arc<Mutex<HashMap<String, ValueStruct>>>;

pub fn init_map() -> SharedMapT {
    Arc::new(Mutex::new(HashMap::new()))
}

pub async fn insert(key: String, value: ValueStruct, map: SharedMapT) {
    let mut m_gaurd = map.lock().await;
    m_gaurd.insert(key, value);
}

pub async fn get(key: String, map: SharedMapT) -> Option<ValueStruct> {
    let m_gaurd = map.lock().await;
    let value = m_gaurd.get(&key).cloned();
    if value.is_none() {
        None
    } else {
        value
    }
}

