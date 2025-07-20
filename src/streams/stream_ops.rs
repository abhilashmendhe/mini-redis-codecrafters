use crate::{basics::{ all_types::SharedMapT, kv_ds::Value}, errors::RedisErrors};

pub async fn type_ops(
    cmds: &Vec<String>,
    kv_map: SharedMapT
) -> Result<String, RedisErrors> {
    let value_type_form = {
    let key = &cmds[1];
    if let Some(v) = kv_map.lock().await.get(key) {
            let value = v.value();
            let v_type = match value {
                Value::STRING(_) => "+string\r\n",
                Value::NUMBER(_) => "+number\r\n",
                Value::LIST(_) => "+list\r\n",
                Value::STREAM(_) => "+stream\r\n",
            };
            v_type
        } else {
            "+none\r\n"
        }
    };

    Ok(value_type_form.to_string())
}