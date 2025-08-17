use crate::basics::all_types::SharedMapT;

pub async fn zcard(
    key: &str,
    kv_ds: SharedMapT
) -> String{
    let mut form = String::new();

    {
        let kv_gaurd = kv_ds.lock().await;
        if let Some(vs) = kv_gaurd.get(key) {
            match vs.value() {
                crate::basics::kv_ds::Value::SORTED_SET(btree_set) => {
                    form.push(':');
                    form.push_str(&btree_set.len().to_string());
                    form.push_str("\r\n");
                },
                _=>{}
            }
        } else {
            form.push_str(":0\r\n");
        }
    }

    form
}