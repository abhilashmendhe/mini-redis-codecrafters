use std::{fs, sync::Arc};

use tokio::sync::Mutex;

use crate::{basics::all_types::SharedRDBStructT, errors::RedisErrors};

#[derive(Debug)]
pub struct RDB {
    dirpath: String,
    rdb_filepath: String,
    appendonly: String,
    appenddirname: String,
    appendfilename: String,
    appendfsync: String,
}

pub fn init_rdb(args: &[String]) -> Result<SharedRDBStructT, RedisErrors> {
    let current_path = std::env::current_dir()?;
    // println!("init_rdb fn -> {:?}",current_path);
    let mut dirpath = current_path.to_string_lossy().to_string();
    // println!("init_rdb fn dirpath -> {}", dirpath);
    let mut rdb_filepath = format!("{}/dump.rdb", dirpath);
    let mut appendonly = "no".to_string();
    let mut appenddirname = "appendonlydir".to_string();
    let mut appendfilename = "appendonly.aof".to_string();
    let mut appendfsync = "everysec".to_string();

    for arg in args {
        match arg.as_str() {
            "--dir" => dirpath = arg.to_string(),
            "--dbfilename" => rdb_filepath = format!("{}/{}", dirpath, &arg),
            "--appendonly" => appendonly = arg.to_string(),
            "--appenddirname" => appenddirname = arg.to_string(),
            "--appendfilename" => appendfilename = arg.to_string(),
            "--appendfsync" => appendfsync = arg.to_string(),
            _ => {}
        }
    }
    // println!("Safely out of for loop init_rdb");
    // if args.len() > 4 {
    //     if args[1] == "--dir".to_string() {
    //         dirpath = args[2].clone();
    //     }
    //     if args[3] == "--dbfilename".to_string() {
    //         rdb_filepath = format!("{}/{}", dirpath, &args[4]);
    //     }
    // }
    let rdb = RDB::new(
        dirpath,
        rdb_filepath,
        appendonly,
        appenddirname,
        appendfilename,
        appendfsync,
    );
    Ok(Arc::new(Mutex::new(rdb)))
}

impl RDB {
    pub fn new(
        dirpath: String,
        rdb_filepath: String,
        appendonly: String,
        appenddirname: String,
        appendfilename: String,
        appendfsync: String,
    ) -> Self {
        RDB {
            dirpath,
            rdb_filepath,
            appendonly,
            appenddirname,
            appendfilename,
            appendfsync,
        }
    }
    pub async fn dirpath(&self) -> String {
        (&self.dirpath).to_string()
    }
    pub async fn rdb_filepath(&self) -> String {
        (&self.rdb_filepath).to_string()
    }
    pub async fn appendonly(&self) -> String {
        (&self.appendonly).to_string()
    }
    pub async fn appenddirname(&self) -> String {
        (&self.appenddirname).to_string()
    }
    pub async fn appendfilename(&self) -> String {
        (&self.appendfilename).to_string()
    }
    pub async fn appendfsync(&self) -> String {
        (&self.appendfsync).to_string()
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
        // println!("{:?}",rdb_m_gaurd.dirpath().await);
        fs::create_dir(rdb_m_gaurd.dirpath().await)?;
    }
    println!("fn save(rdb) --> Done creating dir");
    // 1.1 Header and meta data in bytes
    let mut header_meta_bytes = decode_hex_str(String::from(
        "524544495330303131FA0972656469732D76657206362E302E3136",
    ));
    // 1.2 End of file in bytes
    let end_bytes = decode_hex_str(String::from("FF893bb74ef80f7719"));

    // 1.3 Fetch/Read all key-pairs from map and turn them into bytes

    // 1.4 Concat key-pairs bytes

    // 1.5 Concat end bytes
    header_meta_bytes.extend(end_bytes);

    // 2. Save the .rdb file inside the directory path
    // println!("save(rdb) --> {}",rdb_m_gaurd.rdb_filepath().await);
    fs::write(rdb_m_gaurd.rdb_filepath().await, header_meta_bytes)?;

    Ok(())
}

fn decode_hex_str(st: String) -> Vec<u8> {
    (0..st.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&st[i..i + 2], 16).unwrap())
        .collect::<Vec<_>>()
}
