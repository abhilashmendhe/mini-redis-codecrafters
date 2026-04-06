use crate::{basics::all_types::SharedMapT, errors::RedisErrors, sorted_sets::zadd_ops::zadd};

pub async fn geoadd(
    key: &str,
    set_values: &Vec<String>,
    kv_ds: SharedMapT
) -> Result<String, RedisErrors> {

    let set_size = set_values.len();
    let t_return = set_size / 3;

    // Check if longitude and latitude are valid
    let mut t_index = 0;
    while t_index < t_return {
        let longitude = set_values[t_index * 3].parse::<f32>()?;
        let latitude  = set_values[(t_index * 3) + 1].parse::<f32>()?;
        let location = set_values[(t_index * 3) + 2].clone();
        if (longitude < -180.0 || longitude > 180.0) || (latitude < -85.05112878 || latitude > 85.05112878) {
            return Ok(format!("-ERR invalid longitude,latitude pair {},{}\r\n", longitude, latitude));
        }
        
        // Now add it into sorted sets
        let new_set_values = vec!["0".to_string(), location];
        let _ = zadd(key, &new_set_values, kv_ds.clone()).await?;
        
        t_index += 1;
    }
    

    Ok(format!(":{}\r\n",t_return))
}