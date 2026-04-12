use std::sync::Arc;

use tokio::sync::Mutex;

use crate::acl::Acl;

pub async fn acl_get_user(
    _user_name: &str,
    acl_t: Arc<Mutex<Acl>>
) -> String {
    acl_t.lock().await.to_string()
}