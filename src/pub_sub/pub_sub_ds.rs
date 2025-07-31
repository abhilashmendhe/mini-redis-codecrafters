use std::{collections::{HashMap, HashSet}, net::SocketAddr, sync::Arc};

use tokio::sync::Mutex;

use crate::{connection_handling::SharedConnectionHashMapT, errors::RedisErrors};

pub type SharedPubSubType = Arc<Mutex<HashMap<String, HashSet<u16>>>>;

pub async fn unsubscribe(
    cmds: &Vec<String>,
    sock_addr: SocketAddr,
    connections: SharedConnectionHashMapT,
    pub_sub_map: SharedPubSubType,
) -> Result<String, RedisErrors> {

    // println!("Channels subs to --->>>");
    let mut form = String::new();
    for ch in cmds[1..].iter() {
        // println!("{}",ch);
    
        // First  - remove from the connection
        let pub_sub_len = remove_ch_from_connection(ch, sock_addr, connections.clone()).await;

        // Second - remove from the pub_sub_map
        remove_from_pub_sub_map(ch, sock_addr, pub_sub_map.clone()).await;

        // Create response string format
        pub_sub_format(&mut form, ch, pub_sub_len, "unsubscribe".to_string()).await;
    }

    Ok(form)
}

async fn remove_ch_from_connection(
    ch: &String,
    sock_addr: SocketAddr,
    connections: SharedConnectionHashMapT
) -> usize {
    let mut conn_gaurd = connections.lock().await;
    if let Some(conn_struct) = conn_gaurd.get_mut(&sock_addr.port()) {
        
        // // enable pub_sub flag
        // conn_struct.is_pub_sub = false;
        
        // remove from pub-sub vec
        let pub_sub_len = {
            let pub_sub_ch = conn_struct.mut_get_pub_sub_ch();
            pub_sub_ch.remove(&ch.to_string());
            pub_sub_ch.len()
        };

        if pub_sub_len == 0 {
            conn_struct.set_pub_sub_flag(false);
        }
        pub_sub_len
    } else {
        0
    }
}

async fn remove_from_pub_sub_map(
    ch: &String,
    sock_addr: SocketAddr,
    pub_sub_map: SharedPubSubType,
) {
    
    let mut pub_sub_map_gaurd = pub_sub_map.lock().await;
    match pub_sub_map_gaurd.get_mut(ch) {
        Some(hs) => {
            hs.remove(&sock_addr.port());
        },
        _ => {},
    }    
}

pub async fn subscribe(
    cmds: &Vec<String>,
    sock_addr: SocketAddr,
    connections: SharedConnectionHashMapT,
    pub_sub_map: SharedPubSubType,
) -> Result<String, RedisErrors> {

    // println!("Channels subs to --->>>");
    let mut form = String::new();
    for ch in cmds[1..].iter() {
        // println!("{}",ch);
    
        // First  - insert in the connections
        let pub_sub_len = insert_into_connection(ch, sock_addr, connections.clone()).await;

        // Second - insert in the pub_sub_map
        insert_into_pub_sub_map(ch, sock_addr, pub_sub_map.clone()).await;

        // Create response string format
        pub_sub_format(&mut form, ch, pub_sub_len, "subscribe".to_string()).await;
    }

    Ok(form)
}

async fn insert_into_connection(
    ch: &String,
    sock_addr: SocketAddr,
    connections: SharedConnectionHashMapT
) -> usize {
    let mut conn_gaurd = connections.lock().await;
    if let Some(conn_struct) = conn_gaurd.get_mut(&sock_addr.port()) {
        
        // enable pub_sub flag
        conn_struct.is_pub_sub = true;
        
        // push to pub-sub vec
        let pub_sub_ch = conn_struct.mut_get_pub_sub_ch();
        pub_sub_ch.insert(ch.to_string());
        pub_sub_ch.len()
        
    } else {
        0
    }
}

async fn insert_into_pub_sub_map(
    ch: &String,
    sock_addr: SocketAddr,
    pub_sub_map: SharedPubSubType,
) {
    
    let mut pub_sub_map_gaurd = pub_sub_map.lock().await;
    match pub_sub_map_gaurd.get_mut(ch) {
        Some(hs) => {
            hs.insert(sock_addr.port());
        },
        None => {
            let mut hs = HashSet::new();
            hs.insert(sock_addr.port());
            let _ = pub_sub_map_gaurd.insert(ch.to_string(), hs);
        },
    }    
}

async fn pub_sub_format(form: &mut String, ch: &String, ps_len: usize, tp: String) {
    form.push('*');
    form.push('3');
    form.push_str("\r\n");
    form.push('$');
    form.push_str(&tp.len().to_string());
    form.push_str("\r\n");
    form.push_str(&tp);
    form.push_str("\r\n");
    form.push('$');
    form.push_str(&ch.len().to_string());
    form.push_str("\r\n");
    form.push_str(ch);
    form.push_str("\r\n");
    form.push(':');
    form.push_str(&ps_len.to_string());
    form.push_str("\r\n");
}
