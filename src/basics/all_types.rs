use std::{collections::HashMap, sync::Arc};

use tokio::sync::Mutex;

use crate::{basics::kv_ds::ValueStruct, rdb_persistence::rdb_persist::RDB};

pub type SharedMapT = Arc<Mutex<HashMap<String, ValueStruct>>>;
pub type SharedRDBStructT = Arc<Mutex<RDB>>;