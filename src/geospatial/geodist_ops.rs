use crate::{basics::all_types::SharedMapT, errors::RedisErrors, geospatial::decode_coords::decode_coords, sorted_sets::zscore_ops::zscore};

pub async fn geodist_ops(
    key: &str,
    loc_values: &Vec<String>,
    kv_ds: SharedMapT
) -> Result<String, RedisErrors> {

    let nilstr = "*-1\r\n";

    let loc1 = &loc_values[0];
    let loc2 = &loc_values[1];
    

    // 1. Read the zscore
    let zsc1 = zscore(key, loc1, kv_ds.clone()).await;
    let zsc1_spl = zsc1.split("\r\n").collect::<Vec<&str>>();
    let zsc1_str = zsc1_spl[1];
    if zsc1_str.len() == 0 {
        return Ok(nilstr.to_string());
    }

    let zsc2 = zscore(key, loc2, kv_ds.clone()).await;
    let zsc2_spl = zsc2.split("\r\n").collect::<Vec<&str>>();
    let zsc2_str = zsc2_spl[1];
    if zsc2_str.len() == 0 {
        return Ok(nilstr.to_string());
    }

    let zsc1_fl = zsc1_str.parse::<u64>()?; 
    let zsc2_fl = zsc2_str.parse::<u64>()?;

    // 2. Decode zscore and get lat, long
    let (lat1, lon1) = decode_coords(zsc1_fl);
    let (lat2, lon2) = decode_coords(zsc2_fl);
    
    // 3. Run havenstine dist cal
    let g_dist = havenstine(lat1, lon1, lat2, lon2).to_string();

    Ok(format!("${}\r\n{}\r\n", g_dist.len(), g_dist))
}

pub fn havenstine(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {

    const RADIUS: f64 = 6372797.560856;

    let d_lat = (lat2 - lat1).to_radians();
    let d_lon = (lon2 - lon1).to_radians();
    let lat1 = lat1.to_radians();
    let lat2= lat2.to_radians();

    let d_half_lat_sin = (d_lat / 2.0).sin();
    let d_half_lon_sin = (d_lon / 2.0).sin();
    let a = (d_half_lat_sin * d_half_lat_sin) + (lat1.cos() * lat2.cos() * (d_half_lon_sin * d_half_lon_sin));
    let c = 2.0 * a.sqrt().asin();

    ((RADIUS * c) * 10000.0).round() / 10000.0
}