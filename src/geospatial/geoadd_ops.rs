use crate::{basics::all_types::SharedMapT, errors::RedisErrors};

pub async fn geoadd(
    key: &str,
    set_values: &Vec<String>,
    kv_ds: SharedMapT
) -> Result<String, RedisErrors> {

    let set_size = set_values.len();
    let t_return = set_size / 3;

    let mut t_index = 0;
    while t_index < t_return {
        let longitude = set_values[t_index * 3].parse::<f32>()?;
        let latitude  = set_values[(t_index * 3) + 1].parse::<f32>()?;
        if (longitude < -180.0 || longitude > 180.0) || (latitude < -85.05112878 || latitude > 85.05112878) {
            return Ok(format!("-ERR invalid longitude,latitude pair {},{}\r\n", longitude, latitude));
        }
        t_index += 1;
    }
    
    Ok(format!(":{}\r\n",t_return))
}