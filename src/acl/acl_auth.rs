use std::{collections::HashSet, net::SocketAddr, sync::Arc};

use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

use crate::{acl::Acl, connection_handling::SharedConnectionHashMapT};

pub async fn auth(
    _user: &String,
    password: &String,
    acl_t: Arc<Mutex<Acl>>,
    conn: SharedConnectionHashMapT,
    sock_addr: SocketAddr,
) -> String {
    // 1. convert pass to sha256
    let hasher = Sha256::digest(password.as_bytes());
    let sha256_pass: String = hasher.iter().map(|b| format!("{:02x}", b)).collect();

    // 2. get acl pass set
    let acl_pass_set = {
        let acl_t_guard = acl_t.lock().await;
        acl_t_guard
            .passwords
            .iter()
            .cloned()
            .collect::<HashSet<String>>()
    };

    if !acl_pass_set.contains(&sha256_pass) {
        return "-WRONGPASS invalid username-password pair or user is disabled.\r\n".to_string();
    }
    {
        let mut conn_gaurd = conn.lock().await;
        if let Some(conn_st) = conn_gaurd.get_mut(&sock_addr.port()) {
            conn_st.acl_auth = true;
        }
    }
    "+OK\r\n".to_string()
}
