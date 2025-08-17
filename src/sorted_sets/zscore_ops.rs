use crate::basics::all_types::SharedMapT;

pub async fn zscore(
    key: &str,
    v2: &str,
    kv_ds: SharedMapT
) -> String {

    let mut form = String::new();
    form.push_str("$-1\r\n");
    {
        let kv_gaurd = kv_ds.lock().await;
        if let Some(vs) = kv_gaurd.get(key) {
            match vs.value() {
                crate::basics::kv_ds::Value::SORTED_SET(btree_set) => {

                    for ssv in btree_set {
                        if ssv.v2 == v2 {
                            form.clear();
                            let v1_string = ssv.v1.to_string();
                            form.push('$');
                            form.push_str(&v1_string.len().to_string());
                            form.push_str("\r\n");
                            form.push_str(&v1_string);
                            form.push_str("\r\n");
                            
                            break;
                        }
                    }
                },
                _ => {}
            };
        }
    }
    form
}