use std::{fs, sync::Arc};

use tokio::sync::Mutex;

use crate::errors::RedisErrors;

pub struct RDB {
    dirpath: String,
    rdb_filepath: String
}

type SharedRDBStructT = Arc<Mutex<RDB>>;

pub fn init_rdb(args: Vec<String>) -> Result<SharedRDBStructT, RedisErrors> {
    let mut dir_path = "".to_string();
    let mut dbfile_path = "".to_string();
    if args.len() > 1 {
        if args[1] == "--dir".to_string() {
            dir_path = args[2].clone();

            if !fs::exists(&dir_path)? {
                // println!("File does not exists");
                fs::create_dir(&dir_path)?;
            }
            if args[3] == "--dbfilename".to_string() {
                dbfile_path = format!("{}/{}",dir_path, &args[4]);
                if !fs::exists(&dbfile_path)? {
                    // println!("Creating file: {}", dbfile_path);
                    fs::write(&dbfile_path, "")?;
                }
            }
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
    pub async fn _set_dirpath(&mut self, dirpath: String) {
        self.dirpath = dirpath;
    }
    pub async fn _set_rdb_filepath(&mut self, rdb_filepath: String) {
        self.rdb_filepath = rdb_filepath;
    }
}
