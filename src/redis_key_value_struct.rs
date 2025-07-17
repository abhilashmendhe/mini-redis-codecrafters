#![allow(unused)]

use std::{collections::{HashMap, VecDeque}, sync::Arc, time::{Duration, SystemTime, UNIX_EPOCH}};

use tokio::sync::Mutex;

#[derive(Debug, Clone)]
pub enum Value {
    STRING(String),
    NUMBER(i64),
    LIST(VecDeque<String>),
    STREAM(Vec<String>)
}
#[derive(Debug, Clone)]
pub struct ValueStruct {
    pub value: Value,
    px: Option<u128>,    // milliseconds
    pxat: Option<u128>,   // timestamp-milliseconds
    saved: bool
}

impl ValueStruct {
    pub fn new(value: Value, px: Option<u128>, pxat: Option<u128>) -> Self {
        
        ValueStruct { 
            value, 
            px, 
            pxat,
            saved: false
        }
    }
    pub fn value(&self) -> Value {
        self.value.clone()
    }
    pub fn mut_value(&mut self) -> &mut Value {
        &mut self.value
    }
    pub fn value_len(&self) -> usize {
        match &self.value {
            Value::STRING(s) => s.len(),
            Value::NUMBER(num) => num.to_string().len(),
            Value::LIST(items) => items.len(),
            Value::STREAM(items) => items.len(),
        }
    }
    pub fn set_px(&mut self, px: Option<u128>) {
        self.px = px;
    }
    pub fn set_pxat(&mut self, pxat: Option<u128>) {
        self.pxat = pxat;
    }
    pub fn px(&self) -> Option<u128> {
        self.px
    }
    pub fn pxat(&self) -> Option<u128> {
        self.pxat
    }
}

pub type SharedMapT = Arc<Mutex<HashMap<String, ValueStruct>>>;

pub fn init_map() -> SharedMapT {
    Arc::new(Mutex::new(HashMap::new()))
}

pub async fn insert(key: String, value: ValueStruct, map: SharedMapT) {
    // let mut m_gaurd = map.lock().await;
    // m_gaurd.insert(key, value);
    map.lock().await.insert(key, value);
}

pub async fn get(key: String, map: SharedMapT) -> Option<ValueStruct> {
    let mut m_gaurd = map.lock().await;
    let value = m_gaurd.get(&key).cloned();

    if let Some(vs) = value {
        let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap();
        // println!("in get method - {:?}", vs);
        if let Some(pxat) = vs.pxat() {
            if now.as_millis() > pxat {
                m_gaurd.remove(&key);
                return None;
            }
        }
        Some(vs)
    } else {
        None
    }
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
