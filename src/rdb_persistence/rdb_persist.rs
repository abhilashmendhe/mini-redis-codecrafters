use std::{fs::{self, OpenOptions}, io::BufWriter, sync::Arc};
use std::io::Write;
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
    active_appendfile_path: String
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
    let mut active_appendfile_path = "".to_string();
    // println!("init_rdb ---> {:?}",args);
    if args.len() > 1 {
        let mut i = 1;
        while i < args.len() {
            let arg = args[i].as_str();
            if i+1 >= args.len() {
                return Err(RedisErrors::ArgsErr);
            }
            let arg_val = args[i+1].as_str();
            // println!("dir: {dirpath}, appendonly: {appendonly}, appenddirname: {appenddirname}, appendfilename: {appendfilename}, appendfsync: {appendfsync}");
            match arg {
                "--dir" => {
                    dirpath = arg_val.to_string();
                    // create new dirpath
                     let _ = std::fs::create_dir(&dirpath);
                },
                "--dbfilename" => { 
                    rdb_filepath = format!("{}/{}", dirpath, arg_val); 
                    let _ = std::fs::File::create(&rdb_filepath);
                },
                "--appendonly" => appendonly = arg_val.to_string(),
                "--appenddirname" => {
                    appenddirname = arg_val.to_string();
                    if appendonly.eq("yes") {
                        let _ = std::fs::create_dir(format!("{}/{}",dirpath,appenddirname));
                    }
                },
                "--appendfilename" => {
                    appendfilename = arg_val.to_string();
                    // println!("appendfilename: {}", appendfilename);
                    if appendonly.eq("yes") {
                        let mut max_n = 0;
                        let read_dir_ex = format!("{}/{}",dirpath,appenddirname);
                        let dir = std::fs::read_dir(read_dir_ex)?;
                        for f in dir {
                            let dir_entry = f?;
                            if let Some(file) = dir_entry.file_name().to_str() {
                                let num = file.split('.')
                                    .nth(2)            // "100" in grape.aof.100.incr.aof
                                    .and_then(|s| s.parse::<u32>().ok());
                                if let Some(n) = num {
                                    if n > max_n {
                                        max_n = n;
                                    }
                                }
                            }
                        }
                        let seq = max_n + 1;
                        let nextfile = format!("{}.{}.incr.aof",appendfilename,seq);
                        // println!("next file: {}",nextfile);
                        let _ = std::fs::write(format!("{}/{}/{}",dirpath,appenddirname,nextfile), "");

                        // now create or add into .manifest file in the same appenddirname
                        let manifest_file_name = format!("{}/{}/{}.manifest",dirpath, appenddirname, appendfilename);
                        let manifest_file = match OpenOptions::new()
                            .append(true)
                            .create(true)
                            .open(&manifest_file_name)
                        {
                            Ok(f) => f,
                            Err(e) => {
                                eprintln!("Failed to open file: {}", e);
                                return Err(RedisErrors::Io(e));
                            }
                        };
                        let mut writer = BufWriter::new(manifest_file);
                        // println!("Prints manifest file: {}",manifest_file_name);
                        
                        writeln!(
                            writer,
                            "file {} seq {} type i",
                            nextfile,
                            seq
                        )?;
                        println!("active_appendfile_path: {}/{}/{}", dirpath,appenddirname,nextfile);
                        active_appendfile_path = format!("{}/{}/{}", dirpath,appenddirname,nextfile);
                        // println!("Absolute path: {:?}", std::fs::canonicalize(&manifest_file_name));
                    }
                },
                "--appendfsync" => appendfsync = arg_val.to_string(),
                _ => {}
            }
            i += 2;
        }
    }
    let rdb = RDB::new(
        dirpath,
        rdb_filepath,
        appendonly,
        appenddirname,
        appendfilename,
        appendfsync,
        active_appendfile_path
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
        active_appendfile_path: String
    ) -> Self {
        RDB {
            dirpath,
            rdb_filepath,
            appendonly,
            appenddirname,
            appendfilename,
            appendfsync,
            active_appendfile_path
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
    pub async fn active_appendfile_path(&self) -> String {
        (&self.active_appendfile_path).to_string()
    }
    pub fn _set_dirpath(&mut self, dirpath: String) {
        self.dirpath = dirpath;
    }
    pub fn _set_rdb_filepath(&mut self, rdb_filepath: String) {
        self.rdb_filepath = rdb_filepath;
    }
}

pub async fn append_to_active_appendonly_file(
    command: String,
    rdb: Arc<Mutex<RDB>>,
) -> Result<(), RedisErrors> {
    let rdb_g = rdb.lock().await;
    let appendonly = rdb_g.appendonly().await;
    if appendonly.eq("no") {
        return Ok(());
    }
    let active_appendonly_file = rdb_g.active_appendfile_path().await;
    let open_aaf = match OpenOptions::new()
        .append(true)
        .create(true)
        .open(&active_appendonly_file)
    {
        Ok(f) => f,
        Err(e) => {
            eprintln!("Failed to open file: {}", e);
            return Err(RedisErrors::Io(e));
        }
    };
    let mut writer = BufWriter::new(open_aaf);
    // println!("Prints manifest file: {}",manifest_file_name);
    
    write!(
        writer,
        "{}",
        command
    )?;
    Ok(())
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
