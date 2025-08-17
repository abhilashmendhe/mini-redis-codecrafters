use crate::basics::all_types::SharedMapT;

pub async fn zrem(
    key: &str,
    v2: &str,
    kv_ds: SharedMapT
) -> String {

    let mut form = String::new();
    form.push_str(":0\r\n");
    {
        let mut kv_gaurd = kv_ds.lock().await;
        if let Some(vs) = kv_gaurd.get_mut(key) {
            match vs.mut_value() {
                crate::basics::kv_ds::Value::SORTED_SET(btree_set) => {

                    if let Some(ssv) = btree_set
                                                .iter()
                                                .find(|ssv| ssv.v2 == v2).cloned() {
                        btree_set.remove(&ssv);
                        form.clear();
                        form.push_str(":1\r\n");
                    }
                },
                _ => {}
            };
        }
    }
    form
}