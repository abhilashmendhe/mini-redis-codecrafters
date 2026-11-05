// use std::sync::Arc;

// use tokio::sync::Mutex;

// use crate::{errors::RedisErrors, rdb_persistence::rdb_persist::RDB};

// pub fn read_aof(rdb: Arc<Mutex<RDB>>) -> Result<(), RedisErrors> {
//     let aof_manifest_path = format!("{}/{}/{}.manifest", dirpath, appenddirname, appendfilename);
//     let file = std::fs::read_to_string(manifest_file_path)?;
//     println!("{:?}", file);
//     Ok(())
// }
