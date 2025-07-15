use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};

use tokio::{sync::{mpsc, Mutex}};

pub type ClientId = u16;
pub type SenderChannelT = mpsc::UnboundedSender<(SocketAddr, Vec<u8>)>;
pub type RecvChannelT = mpsc::UnboundedReceiver<(SocketAddr, Vec<u8>)>;
pub type SharedConnectionHashMapT = Arc<Mutex<HashMap<ClientId, (SenderChannelT, bool)>>>;

pub async fn init_connection_channel() -> SharedConnectionHashMapT {
    Arc::new(Mutex::new(HashMap::new()))
}

pub async fn _read_connections_simul(conns: SharedConnectionHashMapT) {
    loop {
        tokio::time::sleep(Duration::from_millis(2000)).await;
        // let conns1 = Arc::clone(&conns);
        let conn_gaurd = conns.lock().await;
        
        for (k, _v) in conn_gaurd.iter() {
            println!("key: {}, flag: {}", k, _v.1);
        }
        std::mem::drop(conn_gaurd);
        println!();
    }
}