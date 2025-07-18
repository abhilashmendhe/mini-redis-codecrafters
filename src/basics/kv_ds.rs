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
