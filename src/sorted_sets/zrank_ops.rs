use crate::basics::all_types::SharedMapT;

pub async fn zrank(
    key: &str,
    v2: &str,
    kv_ds: SharedMapT
) -> String {

    let mut form = String::new();
    {
        let kv_gaurd = kv_ds.lock().await;
        if let Some(vs) = kv_gaurd.get(key) {
            match vs.value() {
                crate::basics::kv_ds::Value::SORTED_SET(btree_set) => {
                    let mut flag = false;
                    let mut rank = 0;
                    for ssv in btree_set {
                        if ssv.v2 == v2 {
                            flag = true;
                            break;
                        }
                        rank += 1;
                    }                    
                    if flag {
                        form.push(':');
                        form.push_str(&rank.to_string());
                        form.push_str("\r\n");
                    } else {
                        form.push_str("$-1\r\n");
                    }
                },
                _ => {}
            }
        } else {
            form.push_str("$-1\r\n");
        }
        
    }
    form
}