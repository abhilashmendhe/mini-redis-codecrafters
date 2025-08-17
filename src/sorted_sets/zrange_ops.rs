use crate::{basics::all_types::SharedMapT, errors::RedisErrors, kv_lists::list_ops_utils::compute_index};

pub async fn zrange(
    key: &str,
    start: &String,
    stop: &String,
    kv_ds: SharedMapT
) -> Result<String, RedisErrors> {
    
    let mut form = String::new();

    {
        let kv_gaurd = kv_ds.lock().await;
        if let Some(vs) = kv_gaurd.get(key) {
            match vs.value() {
                crate::basics::kv_ds::Value::SORTED_SET(btree_set) => {
                    
                    let sorted_set_len = btree_set.len();
                    let start = compute_index(start, sorted_set_len as i64).await?;
                    let stop = compute_index(stop, sorted_set_len as i64).await?;
                    if start >= sorted_set_len as i64 || start > stop {
                        form.push_str(&format!("*0\r\n"));
                    } else {
                        let stop = {
                            if stop > sorted_set_len as i64 {
                                sorted_set_len as  i64
                            } else {
                                stop
                            }
                        };
                        let mut repl_str = String::new();
                        let mut count = 0;
                        
                        for (ind, item) in btree_set.iter().enumerate() {
                            if ind as i64 >= start && ind as i64 <= stop {
                                repl_str.push('$');
                                repl_str.push_str(&item.v2.len().to_string());
                                repl_str.push_str("\r\n");
                                repl_str.push_str(&item.v2);
                                repl_str.push_str("\r\n");
                                count += 1;
                            }
                        }
                        form.push_str(&format!("*{}\r\n{}", count, repl_str));
                    }

                },
                _ => {}
            }
        } else {
            form.push_str("*0\r\n");
        }
    }
    Ok(form)
}

