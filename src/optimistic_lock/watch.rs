use std::net::SocketAddr;

use crate::{
    basics::{all_types::SharedMapT, kv_ds::ValueStruct},
    connection_handling::SharedConnectionHashMapT,
    errors::RedisErrors,
};

pub async fn watch(
    keys: &[String],
    sock_addr: SocketAddr,
    connections: SharedConnectionHashMapT,
    kv_map: SharedMapT,
) -> Result<String, RedisErrors> {
    for key in keys {
        // 1. Create a connection gaurd
        let mut conn_gaurd = connections.lock().await;
        if let Some(conn_struct) = conn_gaurd.get_mut(&sock_addr.port()) {
            let c_watch_keys = conn_struct.get_mut_watch_keys();

            // 2. Check if key is in kv_map
            let mut kv_gaurd = kv_map.lock().await;
            let val = if let Some(val) = kv_gaurd.get_mut(key) {
                let v_watchers = val.mut_watchers();
                v_watchers.insert(sock_addr.port());
                val.clone()
            } else {
                let mut value_struct =
                    ValueStruct::new(crate::basics::kv_ds::Value::NOT_AVAILABLE, None, None);
                let v_watchers = value_struct.mut_watchers();
                v_watchers.insert(sock_addr.port());
                kv_gaurd.insert(key.to_string(), value_struct.clone());
                value_struct
            };

            c_watch_keys.insert(key.to_string(), val.clone());
        }
    }

    // // ------  print info
    // let mut conn_gaurd = connections.lock().await;
    // if let Some(conn_struct) = conn_gaurd.get_mut(&sock_addr.port()) {
    //     println!("Conn watch keys: {:?}",conn_struct.get_mut_watch_keys());
    // }
    // for key in keys {

    //     let mut kv_gaurd = kv_map.lock().await;
    //     if let Some(val) = kv_gaurd.get_mut(key) {
    //         println!("key: {} -- Value watchers: {:?}",key ,val.mut_watchers());
    //     }
    // }
    // // ------  print info
    Ok("+OK\r\n".to_string())
}
