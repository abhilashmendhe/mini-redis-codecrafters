use crate::{
    basics::all_types::SharedMapT, errors::RedisErrors, geospatial::decode_coords::decode_coords,
    sorted_sets::zscore_ops::zscore,
};

pub async fn geopos(
    key: &str,
    set_values: &Vec<String>,
    kv_ds: SharedMapT,
) -> Result<String, RedisErrors> {
    let nilstr = "*-1\r\n";
    let mut count = 0;
    let mut output = String::new();

    for loc in set_values {
        let g_score = zscore(key, loc, kv_ds.clone()).await;
        let g_score_spl = g_score.split("\r\n").collect::<Vec<&str>>();
        let g_score_str = g_score_spl[1];
        if g_score_str.len() == 0 {
            output.push_str(nilstr);
        } else {
            let g_score_fl = g_score_spl[1].parse::<u64>()?;
            let (lat, long) = decode_coords(g_score_fl);
            let lat_str = lat.to_string();
            let long_str = long.to_string();
            output.push_str(&format!(
                "*2\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                long_str.len(),
                long_str,
                lat_str.len(),
                lat_str
            ));
        }
        count += 1;
    }
    let final_output = format!("*{}\r\n{}", count, output);
    // println!("{}", final_output);
    Ok(final_output)
}
