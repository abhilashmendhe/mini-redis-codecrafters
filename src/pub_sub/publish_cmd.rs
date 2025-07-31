use std::net::SocketAddr;

use crate::{connection_handling::SharedConnectionHashMapT, pub_sub::pub_sub_ds::SharedPubSubType};

pub async fn publish(
    cmds: &Vec<String>,
    _sock_addr: SocketAddr,
    connections: SharedConnectionHashMapT,
    pub_sub_map: SharedPubSubType,
) -> String {
    let chan = &cmds[1];
    let msg = &cmds[2];
    println!("publishing msg: {}",msg);
    let hs_len = if let Some(hs) = pub_sub_map.lock().await.get(chan) {
        println!("{:?}",hs);
        {
            let conn_gaurd = connections.lock().await;
            for port in hs {
                if let Some(cs) = conn_gaurd.get(port) {
                    let client_tx = cs.tx_sender.clone();
                    let s = client_tx.send((_sock_addr, "+Cool pub/sub\r\n".as_bytes().to_vec()));
                }
            }
        }
        hs.len()
    } else {
        0
    };

    format!(":{}\r\n",hs_len)
}