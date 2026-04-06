#[allow(unused)]
pub const MIN_LATITUDE :f64 = -85.05112878;
pub const MAX_LATITUDE  :f64= 85.05112878;
pub const MIN_LONGITUDE :f64 = -180.0;
pub const MAX_LONGITUDE :f64 = 180.0;

pub const LATITUDE_RANGE  :f64 = MAX_LATITUDE - MIN_LATITUDE;
pub const LONGITUDE_RANGE :f64  = MAX_LONGITUDE - MIN_LONGITUDE;

pub fn encode_coords(longitude: f64, latitude: f64) -> f64 {

    // 1. Normalizing
    let normalized_latitude = (2_f64.powf(26.0) * (latitude - MIN_LATITUDE) / LATITUDE_RANGE) as u32;
    let normalized_longitude = (2_f64.powf(26.0) * (longitude - MIN_LONGITUDE) / LONGITUDE_RANGE) as u32;

    // println!("normalized longitude: {}", normalized_longitude);
    // println!("normalized latitude: {}", normalized_latitude);
    // 
    let score = interleave(normalized_latitude, normalized_longitude);
    // println!("score: {}", score);
    score as f64
}

fn spread_int32_to_int64(v: u32) -> u64 {

    // # Ensure only lower 32 bits are non-zero.
    let mut v = v as u64 & 0xFFFFFFFF;

    // # Bitwise operations to spread 32 bits into 64 bits with zeros in-between
    v = (v | (v << 16)) & 0x0000FFFF0000FFFF;
    v = (v | (v << 8))  & 0x00FF00FF00FF00FF;
    v = (v | (v << 4))  & 0x0F0F0F0F0F0F0F0F;
    v = (v | (v << 2))  & 0x3333333333333333;
    v = (v | (v << 1))  & 0x5555555555555555;

    return v;
}

fn interleave(x: u32, y: u32) -> u64 {
    // # First, the values are spread from 32-bit to 64-bit integers.
    // # This is done by inserting 32 zero bits in-between.
    // #
    // # Before spread: x1  x2  ...  x31  x32
    // # After spread:  0   x1  ...   0   x16  ... 0  x31  0  x32
    let x = spread_int32_to_int64(x);
    let y = spread_int32_to_int64(y);

    // # The y value is then shifted 1 bit to the left.
    // # Before shift: 0   y1   0   y2 ... 0   y31   0   y32
    // # After shift:  y1   0   y2 ... 0   y31   0   y32   0
    let y_shifted = y << 1;

    // # Next, x and y_shifted are combined using a bitwise OR.
    // #
    // # Before bitwise OR (x): 0   x1   0   x2   ...  0   x31    0   x32
    // # Before bitwise OR (y): y1  0    y2  0    ...  y31  0    y32   0
    // # After bitwise OR     : y1  x2   y2  x2   ...  y31  x31  y32  x32
    return x | y_shifted;
}