use crate::{
    basics::all_types::SharedMapT, errors::RedisErrors, geospatial::encode_coords::encode_coords,
    sorted_sets::zadd_ops::zadd,
};

pub async fn geoadd(
    key: &str,
    set_values: &Vec<String>,
    kv_ds: SharedMapT,
) -> Result<String, RedisErrors> {
    let set_size = set_values.len();
    let t_return = set_size / 3;
    let mut act_return = 0;

    // Check if longitude and latitude are valid
    let mut t_index = 0;
    while t_index < t_return {
        let longitude = set_values[t_index * 3].parse::<f64>()?;
        let latitude = set_values[(t_index * 3) + 1].parse::<f64>()?;
        let location = set_values[(t_index * 3) + 2].clone();
        if (longitude < -180.0 || longitude > 180.0)
            || (latitude < -85.05112878 || latitude > 85.05112878)
        {
            return Ok(format!(
                "-ERR invalid longitude,latitude pair {},{}\r\n",
                longitude, latitude
            ));
        }

        // Before adding into sorted sets, first calculate the scores
        let score = encode_coords(longitude, latitude);
        // Now add it into sorted sets
        let new_set_values = vec![score.to_string(), location];
        let ret_val = zadd(key, &new_set_values, kv_ds.clone()).await?;
        let ret_val_trim = &ret_val[1..(ret_val.len() - 2)];
        act_return += ret_val_trim.parse::<usize>()?;
        t_index += 1;
    }

    Ok(format!(":{}\r\n", act_return))
}
