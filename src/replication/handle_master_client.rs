use std::{sync::Arc, time::{SystemTime, UNIX_EPOCH}};

use tokio::{io::AsyncWriteExt, net::tcp::OwnedWriteHalf, sync::Mutex};

use crate::redis_key_value_struct::{insert, SharedMapT, ValueStruct};

pub async fn master_reader_handle(
    commands: Vec<Vec<String>>,
    writer: Arc<Mutex<OwnedWriteHalf>>,
    kv_map: SharedMapT,
    recv_bytes_count: Arc<Mutex<usize>>,
    // recv_bytes_flag: Arc<Mutex<bool>>
    no_bytes: usize
) {
    
    
    for (_ind, cmd) in commands.iter().enumerate() {
        println!("In master_reader_handle - {:?}", cmd);
       
        if cmd.get(1).map(|s| s.as_str()) == Some("SET") {
            let key = cmd[3].as_str();
            let value = cmd[5].as_str();
            let mut value_struct = ValueStruct::new(value.to_string(), None, None);
            if cmd.len() >= 7 {
                let px = cmd[9].parse::<u128>().unwrap();
                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
                let now_ms = now.as_millis() + px;
                value_struct.set_px(Some(px));
                value_struct.set_pxat(Some(now_ms));
            }
            insert(key.to_string(), value_struct, kv_map.clone()).await;
            
        } else if cmd.get(1).map(|s| s.as_str()) == Some("REPLCONF") {
            let _ack = cmd[3].as_str();
            let _commands = cmd[5].as_str();
            let mut recv_byte_counts = 0;
            {
                recv_byte_counts += *recv_bytes_count.lock().await;
            }
            let recv_byte_cnt_str = format!("{}", recv_byte_counts);
            let repl_ack = format!("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${}\r\n{}\r\n",recv_byte_cnt_str.len(),recv_byte_counts);
            // println!("Writing ack back to master\n:{}",repl_ack);
            {
                let mut writer_gaurd = writer.lock().await;
                let _ = writer_gaurd.write(repl_ack.as_bytes()).await;
            }
        }
    }
}