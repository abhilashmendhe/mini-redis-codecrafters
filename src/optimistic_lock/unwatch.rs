use std::net::SocketAddr;

use crate::{
    basics::all_types::SharedMapT, connection_handling::SharedConnectionHashMapT,
    errors::RedisErrors,
};

pub async fn unwatch(
    sock_addr: SocketAddr,
    connections: SharedConnectionHashMapT,
    kv_map: SharedMapT,
) -> Result<String, RedisErrors> {
    let mut conn_gaurd = connections.lock().await;
    if let Some(conn_struct) = conn_gaurd.get_mut(&sock_addr.port()) {
        let cs_watch_keys = conn_struct.get_mut_watch_keys();
        for (key, cs_val_struct) in &mut *cs_watch_keys {
            let mut kv_gaurd = kv_map.lock().await;
            if let Some(val_struct) = kv_gaurd.get_mut(key) {
                // val_struct.
                // println!("kv_map: {:?}",val_struct);
                val_struct.mut_watchers().remove(&sock_addr.port());
            }
            // println!("conn_val_struct: {:?}",_val_s);
            cs_val_struct.mut_watchers().remove(&sock_addr.port());
        }
        // cs_watch_keys.clear();
    }

    Ok("+OK\r\n".to_string())
}
