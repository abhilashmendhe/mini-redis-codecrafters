use std::{collections::HashSet, sync::Arc};

use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

use crate::acl::Acl;

pub async fn acl_set_user(
    passwords: &Vec<String>,
    acl_t: Arc<Mutex<Acl>>
) -> String {
    let acl_pass_set = {
        let acl_t_guard = acl_t.lock().await;
        acl_t_guard.passwords.iter().cloned().collect::<HashSet<String>>()
    };
    {
        let mut acl_t_gaurd = acl_t.lock().await;
        
        let acl_t_mut_passwords = acl_t_gaurd.mut_passwords();

        let mut should_set_nopass_false = true;
        for pass in passwords {
            if !pass.starts_with('>') {
                return format!("-ERR Error in ACL SETUSER modifier '{}': Syntax error\r\n", pass);
            }
            if acl_pass_set.contains(&pass[1..]) {
                continue;
            }
            // create sha-256 pass 
            let hasher = Sha256::digest(&pass[1..].as_bytes());
            let sha256_pass:String = hasher.iter()
                                    .map(|b| format!("{:02x}", b))
                                    .collect();
            acl_t_mut_passwords.push(sha256_pass);
            should_set_nopass_false = false;
        }
        acl_t_gaurd.set_acl_flags_nopass(should_set_nopass_false);
    }
    "+OK\r\n".to_string()
}