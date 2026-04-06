use crate::geospatial::encode_coords::*;

pub fn decode_coords(geo_score: u64) -> (f64, f64) {

    let longitude = geo_score >> 1;
    let latitude  = geo_score;

    // # Compact both latitude and longitude back to 32-bit integers
    let grid_latitude_number = compact_int64_to_int32(latitude);
    let grid_longitude_number = compact_int64_to_int32(longitude);

    convert_grid_numbers_to_coordinates(grid_latitude_number, grid_longitude_number)
}

fn compact_int64_to_int32(v: u64) -> u64 {
    // # Keep only the bits in even positions
    let mut v = v & 0x5555555555555555;

    // # Before masking: w1   v1  ...   w2   v16  ... w31  v31  w32  v32
    // # After masking: 0   v1  ...   0   v16  ... 0  v31  0  v32

    // # Where w1, w2,..w31 are the digits from longitude if we're compacting latitude, or digits from latitude if we're compacting longitude
    // # So, we mask them out and only keep the relevant bits that we wish to compact

    // # ------
    // # Reverse the spreading process by shifting and masking
    v = (v | (v >> 1)) & 0x3333333333333333;
    v = (v | (v >> 2)) & 0x0F0F0F0F0F0F0F0F;
    v = (v | (v >> 4)) & 0x00FF00FF00FF00FF;
    v = (v | (v >> 8)) & 0x0000FFFF0000FFFF;
    v = (v | (v >> 16)) & 0x00000000FFFFFFFF;

    // # Before compacting: 0   v1  ...   0   v16  ... 0  v31  0  v32
    // # After compacting: v1  v2  ...  v31  v32
    // # -----
    
    return v;
}

fn convert_grid_numbers_to_coordinates(grid_latitude_number: u64, grid_longitude_number: u64) -> (f64, f64) {
    // # Calculate the grid boundaries
    let grid_latitude_min = MIN_LATITUDE + LATITUDE_RANGE * (grid_latitude_number as f64 / (2_f64.powf(26.0)));
    let grid_latitude_max = MIN_LATITUDE + LATITUDE_RANGE * ((grid_latitude_number + 1) as f64  / (2_f64.powf(26.0)));
    let grid_longitude_min = MIN_LONGITUDE + LONGITUDE_RANGE * (grid_longitude_number as f64 / (2_f64.powf(26.0)));
    let grid_longitude_max = MIN_LONGITUDE + LONGITUDE_RANGE * ((grid_longitude_number + 1) as f64  / (2_f64.powf(26.0)));
    
    // # Calculate the center point of the grid cell
    let latitude = (grid_latitude_min + grid_latitude_max) as f64 / 2.0;
    let longitude = (grid_longitude_min + grid_longitude_max) as f64 / 2.0;
    return (latitude, longitude);
}