use std::net::SocketAddr;

use crate::{connection_handling::SharedConnectionHashMapT, errors::RedisErrors, pub_sub::pub_sub_ds::SharedPubSubType};

pub async fn publish(
    cmds: &Vec<String>,
    _sock_addr: SocketAddr,
    connections: SharedConnectionHashMapT,
    pub_sub_map: SharedPubSubType,
) -> Result<String, RedisErrors> {
    let chan = &cmds[1];
    let msg = &cmds[2];
    // println!("publishing msg: {}",msg);
    let hs_len = if let Some(hs) = pub_sub_map.lock().await.get(chan) {
        // println!("{:?}",hs);
        {
            let conn_gaurd = connections.lock().await;
            for port in hs {
                if let Some(cs) = conn_gaurd.get(port) {
                    let client_tx = cs.tx_sender.clone();
                    let pub_form = pub_format(chan, msg).await;
                    client_tx.send((_sock_addr, pub_form))?;
                }
            }
        }
        hs.len()
    } else {
        0
    };

    Ok(format!(":{}\r\n",hs_len))
}
async fn pub_format(ch_name: &String, msg: &String) -> Vec<u8> {

    let mut form = String::new();
    form.push('*');
    form.push('3');
    form.push_str("\r\n");
    
    form.push('$');
    form.push('7');
    form.push_str("\r\n");
    form.push_str("message");
    form.push_str("\r\n");

    form.push('$');
    form.push_str(&ch_name.len().to_string());
    form.push_str("\r\n");
    form.push_str(&ch_name);
    form.push_str("\r\n");

    form.push('$');
    form.push_str(&msg.len().to_string());
    form.push_str("\r\n");
    form.push_str(&msg);
    form.push_str("\r\n");
    
    form.as_bytes().to_vec()
}