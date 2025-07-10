use std::{fs, sync::Arc};

use tokio::sync::Mutex;

use crate::errors::RedisErrors;

#[derive(Debug)]
pub struct RDB {
    dirpath: String,
    rdb_filepath: String
}

type SharedRDBStructT = Arc<Mutex<RDB>>;

pub fn init_rdb(args: Vec<String>) -> Result<SharedRDBStructT, RedisErrors> {
    let mut dir_path = "".to_string();
    let mut dbfile_path = "".to_string();
    if args.len() > 4 {
        if args[1] == "--dir".to_string() {
            dir_path = args[2].clone();
        }
        if args[3] == "--dbfilename".to_string() {
            dbfile_path = format!("{}/{}",dir_path, &args[4]);
        }
    }
    Ok(Arc::new(Mutex::new(RDB::new(dir_path, dbfile_path))))
}

impl RDB {
    pub fn new(dirpath: String, rdb_filepath: String) -> Self {
        RDB {
            dirpath,
            rdb_filepath
        }
    }
    pub async fn dirpath(&self) -> String {
        (&self.dirpath).to_string()
    }
    pub async fn rdb_filepath(&self) -> String {
        (&self.rdb_filepath).to_string()
    }
    pub fn _set_dirpath(&mut self, dirpath: String) {
        self.dirpath = dirpath;
    }
    pub fn _set_rdb_filepath(&mut self, rdb_filepath: String) {
        self.rdb_filepath = rdb_filepath;
    }
}

pub async fn save(rdb: Arc<Mutex<RDB>>) -> Result<(), RedisErrors> {
    // println!("Problem in save!");
    // 1. Check if the directory path exists; if not create the directory path
    let rdb_m_gaurd = rdb.lock().await;
    if rdb_m_gaurd.dirpath().await.is_empty() {
        return Ok(());
    }
    if !fs::exists(rdb_m_gaurd.dirpath().await)? {
        fs::create_dir(rdb_m_gaurd.dirpath().await)?;
    }
    // 1.1 Header and meta data in bytes
    let mut header_meta_bytes = _decode_hex_str(
        String::from("524544495330303131FA0972656469732D76657206362E302E3136")
    );
    // 1.2 End of file in bytes
    let end_bytes = _decode_hex_str(String::from("FF893bb74ef80f7719"));
    
    // 1.3 Fetch/Read all key-pairs from map and turn them into bytes

    // 1.4 Concat key-pairs bytes

    // 1.5 Concat end bytes
    header_meta_bytes.extend(end_bytes);

    // 2. Save the .rdb file inside the directory path
    fs::write(rdb_m_gaurd.rdb_filepath().await, header_meta_bytes)?;
    
    Ok(())
}

fn _decode_hex_str(st: String) -> Vec<u8> {
    (0..st.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&st[i..i + 2], 16).unwrap())
        .collect::<Vec<_>>()
}