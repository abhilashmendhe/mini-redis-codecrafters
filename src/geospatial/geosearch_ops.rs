use crate::{
    basics::all_types::SharedMapT,
    errors::RedisErrors,
    geospatial::{decode_coords::decode_coords, geodist_ops::havenstine},
};

pub async fn geosearch(
    key: &str,
    set_values: &Vec<String>,
    kv_ds: SharedMapT,
) -> Result<String, RedisErrors> {
    let lon = &set_values[1];
    let lat = &set_values[2];
    let dist = match &set_values[4].trim().parse::<f64>() {
        Ok(dist) => *dist,
        Err(_) => return Ok("-ERR need numeric radius\r\n".to_string()),
    };

    let _d_type = &set_values[5];
    let mut output = "*-1\r\n".to_string();
    let mut count = 0;
    {
        let kv_guard = kv_ds.lock().await;
        if let Some(vs) = kv_guard.get(key) {
            match vs.value() {
                crate::basics::kv_ds::Value::SORTED_SET(btree_set) => {
                    let mut sub_output = String::new();
                    for bt in btree_set {
                        let z_score = bt.v1;
                        let (lat1, lon1) = decode_coords(z_score as u64);
                        let lat2 = lat.parse::<f64>()?;
                        let lon2 = lon.parse::<f64>()?;
                        let calc_dist = havenstine(lat1, lon1, lat2, lon2);
                        // println!("{} -> {}", bt.v2, calc_dist);
                        if calc_dist <= dist {
                            sub_output.push_str(&format!("${}\r\n{}\r\n", bt.v2.len(), bt.v2));
                            count += 1;
                        }
                    }
                    output = format!("*{}\r\n{}", count, sub_output);
                }
                _ => {}
            }
        }
    }

    Ok(output)
}
